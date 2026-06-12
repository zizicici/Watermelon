import XCTest
@testable import Watermelon

final class RepoFormatRouterTests: XCTestCase {
    private let basePath = "/photos"
    private let createdAt = "2026-06-08T00:00:00Z"
    private let createdBy = "writer-1b4e28ba"

    private var versionPath: String { RepoLayoutLite.versionPath(basePath: basePath) }
    private var repoDir: String { RepoLayoutLite.repoDirectoryPath(basePath: basePath) }

    private func router(_ client: InMemoryRemoteStorageClient) -> RepoFormatRouter {
        RepoFormatRouter(client: client, basePath: basePath)
    }

    private func canonicalVersionBytes() throws -> Data {
        try VersionManifestLite.encode(
            VersionManifestLite.makeManifest(createdAt: createdAt, createdBy: createdBy)
        )
    }

    private func versionBytes(
        formatVersion: Int?,
        layout: String?,
        minAppVersion: String? = "1.5.0"
    ) throws -> Data {
        try VersionManifestLite.encode(WatermelonRemoteVersionManifest(
            formatVersion: formatVersion,
            layout: layout,
            minAppVersion: minAppVersion,
            createdAt: createdAt,
            createdBy: createdBy
        ))
    }

    private func v1ManifestPath(year: Int, month: Int) -> String {
        RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d/%@", year, month, MonthManifestStore.manifestFileName)
        )
    }

    private func classify(_ client: InMemoryRemoteStorageClient) async -> Result<RepoFormatDecision, Error> {
        do { return .success(try await router(client).classify()) }
        catch { return .failure(error) }
    }

    // MARK: - Current

    func testCommittedV2LiteVersionIsCurrent() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try canonicalVersionBytes())

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .current)
    }

    func testCurrentShortCircuitsAndDoesNotScanV1() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try canonicalVersionBytes())
        // A stray V1 manifest must be ignored once a committed version exists.
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        let listed = await client.listedPaths

        XCTAssertEqual(decision, .current)
        XCTAssertFalse(listed.contains("/photos/2024"), "current must not descend into V1 year dirs")
    }

    func testCurrentVersionWithDevMarkerReturnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try canonicalVersionBytes())
        await client.seedDirectory("\(repoDir)/commits")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .unsupported())
    }

    func testCurrentRepoIgnoresVersionScratchSiblings() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try canonicalVersionBytes())
        await client.seedFile(path: "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.tmp")
        await client.seedFile(path: "\(repoDir)/version_22222222-2222-2222-2222-222222222222.json.bak")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .current)
    }

    // MARK: - V1 migrate

    func testMissingVersionWithV1ManifestsReturnsV1Migrate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))
        await client.seedFile(path: v1ManifestPath(year: 2023, month: 12))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .v1Migrate)
    }

    func testHalfCreatedRepoWithV1ManifestsReturnsV1Migrate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(repoDir)
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .v1Migrate)
    }

    // MARK: - Damaged

    func testMonthSqliteWithoutVersionReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1))
        await client.seedFile(path: monthPath)

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testMonthSqliteWithRecoverableVersionBackupReturnsMalformedVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1))
        let backupPath = "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.bak"
        await client.seedFile(path: monthPath)
        await client.seedFile(path: backupPath, data: try canonicalVersionBytes())

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .malformedVersion)
    }

    func testMonthSqliteWithRecoverableVersionTempReturnsMalformedVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1))
        let tempPath = "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.tmp"
        await client.seedFile(path: monthPath)
        await client.seedFile(path: tempPath, data: try canonicalVersionBytes())

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .malformedVersion)
    }

    func testUncommittedRepoWithUnknownChildReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/version_leftover.json.tmp", data: Data([0x01]))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testUncommittedRepoWithOnlyVersionScratchReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.tmp")
        await client.seedFile(path: "\(repoDir)/version_22222222-2222-2222-2222-222222222222.json.bak")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    // MARK: - Malformed version (recoverable, not generic damaged)

    func testMalformedVersionReturnsMalformedVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data("not json".utf8))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .malformedVersion)
    }

    func testEmptyVersionFileReturnsMalformedVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data())

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .malformedVersion)
    }

    func testVersionWithoutFormatVersionReturnsMalformedVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try versionBytes(formatVersion: nil, layout: "lite-month-sqlite"))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .malformedVersion)
    }

    func testCurrentFormatVersionWithoutLayoutReturnsMalformedVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try versionBytes(formatVersion: 2, layout: nil))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .malformedVersion)
    }

    func testCurrentFormatVersionWithoutMinAppVersionReturnsMalformedVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: versionPath,
            data: try versionBytes(formatVersion: 2, layout: "lite-month-sqlite", minAppVersion: nil)
        )

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .malformedVersion)
    }

    func testMalformedVersionWithV1ManifestsReturnsV1Migrate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data("not json".utf8))
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .v1Migrate)
    }

    func testMalformedVersionWithLiteMonthAndV1ManifestDoesNotMigrateV1() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data("not json".utf8))
        await client.seedFile(path: RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1)))
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .malformedVersion)
    }

    func testMalformedVersionWithDevMarkerReturnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data("not json".utf8))
        await client.seedDirectory("\(repoDir)/commits")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .unsupported())
    }

    // MARK: - Fresh

    func testAbsentBasePathReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()   // nothing seeded
        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testEmptyBasePathReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(basePath)
        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testHalfCreatedRepoWithoutMarkersReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(repoDir)   // .watermelon exists, nothing inside
        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testEmptyMonthsDirWithoutVersionReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testNonYearSiblingDirsDoNotTriggerV1Migrate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("/photos/exports")
        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    // MARK: - Unsupported

    func testLayoutMismatchReturnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try versionBytes(formatVersion: 2, layout: "crdt-commit-log"))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .unsupported())
    }

    func testFutureFormatVersionReturnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try versionBytes(formatVersion: 3, layout: "lite-month-sqlite"))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .unsupported())
    }

    func testFutureFormatVersionCarriesRequiredMinAppVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: versionPath,
            data: try versionBytes(formatVersion: 3, layout: "lite-month-sqlite", minAppVersion: "9.9.9")
        )

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .unsupported(minAppVersion: "9.9.9"))
    }

    func testCommitsMarkerReturnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(repoDir)/commits")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .unsupported())
    }

    func testFutureVersionWithDevMarkerCarriesRequiredMinAppVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: versionPath,
            data: try versionBytes(formatVersion: 3, layout: "lite-month-sqlite", minAppVersion: "9.9.9")
        )
        await client.seedDirectory("\(repoDir)/commits")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .unsupported(minAppVersion: "9.9.9"))
    }

    func testSnapshotsMarkerReturnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("\(repoDir)/snapshots")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .unsupported())
    }

    // MARK: - Probe faults never read as fresh

    func testBaseListTransientFaultThrowsAndNeverFresh() async {
        for fault in [RemoteErrorFixtures.retryable, RemoteErrorFixtures.terminal] {
            let client = InMemoryRemoteStorageClient()
            await client.enqueueListError(fault)

            switch await classify(client) {
            case .success(let decision):
                XCTFail("transient probe must not resolve, got \(decision)")
            case .failure(let error):
                XCTAssertNotNil(error as? RepoFormatRouterError)
            }
        }
    }

    func testBaseListRetryableFaultThrowsProbeFault() async {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)

        switch await classify(client) {
        case .success(let decision):
            XCTFail("expected a probe fault, got \(decision)")
        case .failure(let error):
            XCTAssertEqual(error as? RepoFormatRouterError, .probeFault(.retryable))
        }
    }

    func testVersionDownloadTransientFaultThrows() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(repoDir)
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        switch await classify(client) {
        case .success(let decision):
            XCTFail("expected a probe fault, got \(decision)")
        case .failure(let error):
            XCTAssertEqual(error as? RepoFormatRouterError, .probeFault(.retryable))
        }
    }

    func testV1ScanTransientFaultThrows() async {
        let client = InMemoryRemoteStorageClient()
        let yearEntry = RemoteStorageEntry(
            path: "/photos/2024", name: "2024", isDirectory: true,
            size: 0, creationDate: nil, modificationDate: nil
        )
        await client.enqueueListResult([yearEntry])                 // base list surfaces a year dir
        await client.enqueueListError(RemoteErrorFixtures.retryable) // year-dir list faults

        switch await classify(client) {
        case .success(let decision):
            XCTFail("interrupted V1 scan must not resolve, got \(decision)")
        case .failure(let error):
            XCTAssertEqual(error as? RepoFormatRouterError, .probeFault(.retryable))
        }
    }
}

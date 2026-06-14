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
        try versionBytes(
            formatVersion: formatVersion,
            layout: layout,
            minAppVersion: minAppVersion,
            createdAt: createdAt,
            createdBy: createdBy
        )
    }

    private func versionBytes(
        formatVersion: Int?,
        layout: String?,
        minAppVersion: String?,
        createdAt: String?,
        createdBy: String?
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

    func testDirectoryValuedMonthPathWithoutVersionReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 3))
        await client.seedDirectory(monthPath)   // a directory occupies the canonical month-manifest slot

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged,
                       "a directory at <YYYY-MM>.sqlite is damaged control state, not fresh space")
    }

    // A non-directory object occupying the reserved `.watermelon` marker path (reachable on S3-compatible
    // flat-key stores, where an object key and a same-stem child prefix can coexist) is foreign control state,
    // not empty space. With no directory marker it must route .damaged, never .fresh — otherwise a write path
    // could initialize a Lite repo under the reserved path already occupied by an object.
    func testNonDirectoryWatermelonMarkerObjectReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: repoDir)   // an object keyed exactly ".watermelon", no child prefix

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged,
                       "a non-directory object at the reserved .watermelon path is foreign control state, not fresh")
    }

    // The reserved-marker object fails closed even ahead of V1 evidence: a stray `.watermelon` object plus
    // legacy V1 manifests is contradictory foreign control state, so it must not route .v1Migrate (which would
    // commit a Lite marker under the occupied reserved path).
    func testNonDirectoryWatermelonMarkerObjectWithV1ManifestsReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: repoDir)
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged,
                       "a reserved-marker object must fail closed even when V1 manifests are also present")
    }

    // S3 flat-key can surface BOTH a non-directory `.watermelon` object and a same-stem `.watermelon/` prefix
    // (e.g. because `.watermelon/locks/...` exists). When no version is committed, the prefix flipping
    // `repoDirPresent` to true must NOT let the marker-object conflict route `.fresh` and commit version.json
    // over the occupied reserved path.
    func testCoexistingWatermelonObjectAndPrefixWithoutVersionReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListResult([
            RemoteStorageEntry(path: repoDir, name: ".watermelon", isDirectory: false, size: 10, creationDate: nil, modificationDate: nil),
            RemoteStorageEntry(path: repoDir, name: ".watermelon", isDirectory: true, size: 0, creationDate: nil, modificationDate: nil)
        ])

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged,
                       "a reserved-marker object must fail closed even when a .watermelon/ prefix coexists but no version is committed")
    }

    // Non-regression: a validly committed current Lite repo is still trusted even if a stray reserved-marker
    // object coexists with the real `.watermelon/` prefix — the committed version is the format commit point.
    func testCoexistingWatermelonObjectAndPrefixWithCommittedVersionStaysCurrent() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try canonicalVersionBytes())
        await client.enqueueListResult([
            RemoteStorageEntry(path: repoDir, name: ".watermelon", isDirectory: false, size: 10, creationDate: nil, modificationDate: nil),
            RemoteStorageEntry(path: repoDir, name: ".watermelon", isDirectory: true, size: 0, creationDate: nil, modificationDate: nil)
        ])

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .current,
                       "a committed current version is still trusted even if a stray reserved-marker object coexists")
    }

    // A directory occupying a canonical V1 manifest slot (YYYY/MM/.watermelon_manifest.sqlite/) is damaged
    // control state, not empty space: it must not route .fresh, which would let a write commit a Lite version
    // marker over unresolved V1 state and bypass the strict migration scan. With no .watermelon and no
    // readable V1 manifest, it routes .damaged.
    func testDirectoryValuedV1ManifestCandidateWithoutVersionReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(v1ManifestPath(year: 2024, month: 2))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged,
                       "a directory-only V1 manifest candidate is damaged control state, not fresh space")
    }

    // Same shape under an otherwise-empty uncommitted .watermelon tree (the classifyUncommittedRepo
    // fallthrough): still .damaged, never .fresh.
    func testDirectoryValuedV1CandidateUnderUncommittedRepoReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(repoDir)
        await client.seedDirectory(v1ManifestPath(year: 2024, month: 2))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged,
                       "a directory-only V1 candidate under an uncommitted repo must fail closed as damaged")
    }

    // A readable V1 manifest file with a directory-valued sibling still routes .v1Migrate (the readable
    // manifest is decisive); the directory is then caught by the strict migration scan.
    func testReadableV1ManifestWithDirectorySiblingReturnsV1Migrate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))
        await client.seedDirectory(v1ManifestPath(year: 2024, month: 2))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .v1Migrate,
                       "a readable V1 manifest is decisive; the directory sibling is handled by strict migration")
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

    // An interrupted V1→Lite migration leaves Lite month sqlite + a recoverable current version scratch while
    // the legacy V1 manifests are still present. It must route .v1Migrate (so migration re-validates V1 source
    // drift), never .malformedVersion (which would commit version.json over possibly-stale months and then
    // permanently stop scanning V1).
    func testMonthSqliteWithRecoverableScratchAndV1ManifestReturnsV1Migrate() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1))
        let tempPath = "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.tmp"
        await client.seedFile(path: monthPath)
        await client.seedFile(path: tempPath, data: try canonicalVersionBytes())
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(
            decision, .v1Migrate,
            "an interrupted migration (Lite months + recoverable scratch + live V1) must re-validate via .v1Migrate, not bless months via .malformedVersion"
        )
    }

    // The interrupted-migration shape (Lite month + recoverable current version scratch) must still fail closed
    // when V1 evidence is only a directory occupying a manifest slot — committing version.json would bury that
    // unresolved/damaged V1 control state. Mirrors the directory-candidate fail-closed handling on the
    // no-scratch path; the recoverable scratch must not bypass it.
    func testMonthSqliteWithRecoverableScratchAndDirectoryOnlyV1CandidateReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1))
        let tempPath = "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.tmp"
        await client.seedFile(path: monthPath)
        await client.seedFile(path: tempPath, data: try canonicalVersionBytes())
        await client.seedDirectory(v1ManifestPath(year: 2024, month: 2))   // directory at a canonical V1 manifest slot

        let decision = try await router(client).classify()
        XCTAssertEqual(
            decision, .damaged,
            "a directory-only V1 candidate must fail closed even when a recoverable version scratch is present"
        )
    }

    // The malformedVersion recovery route also commits version.json: an unknown `.watermelon` child must fail
    // closed before it, otherwise recovery republishes the version commit point over foreign control state.
    func testMonthSqliteWithRecoverableScratchAndUnknownChildReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1))
        let tempPath = "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.tmp"
        await client.seedFile(path: monthPath)
        await client.seedFile(path: tempPath, data: try canonicalVersionBytes())
        await client.seedFile(path: "\(repoDir)/foreign-control.bin", data: Data([0x01]))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged,
                       "an unknown .watermelon child must fail closed even when a recoverable version scratch is present")
    }

    func testUncommittedRepoWithUnknownChildReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/version_leftover.json.tmp", data: Data([0x01]))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    // A valid V1 manifest must NOT override foreign control state: an unknown child under the reserved
    // `.watermelon` directory fails closed (.damaged) rather than routing .v1Migrate, which would let
    // version.json commit over the unresolved child.
    func testUncommittedRepoWithUnknownChildAndV1ManifestsReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/foreign-control.bin", data: Data([0x01]))
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged,
                       "an unknown .watermelon child must fail closed even when valid V1 manifests are present")
    }

    func testUncommittedRepoWithOnlyVersionScratchReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.tmp")
        await client.seedFile(path: "\(repoDir)/version_22222222-2222-2222-2222-222222222222.json.bak")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    // MARK: - Malformed canonical version (fail closed)

    func testMalformedVersionReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data("not json".utf8))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    // A *damaged* canonical version fails closed as .damaged even alongside a Lite month sqlite and a
    // recoverable current version scratch. Recovery (.malformedVersion) is reserved for a *missing*
    // canonical; a corrupt one is never routed to the version-commit recovery path.
    func testDamagedCanonicalWithMonthSqliteAndRecoverableScratchReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthPath = RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1))
        let scratchPath = "\(repoDir)/version_11111111-1111-1111-1111-111111111111.json.tmp"
        await client.seedFile(path: versionPath, data: Data("not json".utf8))
        await client.seedFile(path: monthPath)
        await client.seedFile(path: scratchPath, data: try canonicalVersionBytes())

        let decision = try await router(client).classify()
        XCTAssertEqual(
            decision, .damaged,
            "a damaged canonical version must fail closed even with a month sqlite and a recoverable scratch"
        )
    }

    func testEmptyVersionFileReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data())

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testVersionWithoutFormatVersionReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try versionBytes(formatVersion: nil, layout: "lite-month-sqlite"))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testCurrentFormatVersionWithoutLayoutReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: try versionBytes(formatVersion: 2, layout: nil))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testCurrentFormatVersionWithoutMinAppVersionReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: versionPath,
            data: try versionBytes(formatVersion: 2, layout: "lite-month-sqlite", minAppVersion: nil)
        )

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testCurrentFormatVersionWithoutCreatedAtReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: versionPath,
            data: try versionBytes(
                formatVersion: 2,
                layout: "lite-month-sqlite",
                minAppVersion: "1.5.0",
                createdAt: nil,
                createdBy: createdBy
            )
        )

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testCurrentFormatVersionWithoutCreatedByReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: versionPath,
            data: try versionBytes(
                formatVersion: 2,
                layout: "lite-month-sqlite",
                minAppVersion: "1.5.0",
                createdAt: createdAt,
                createdBy: nil
            )
        )

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testCurrentFormatVersionWithEmptyCreatedByReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: versionPath,
            data: try versionBytes(
                formatVersion: 2,
                layout: "lite-month-sqlite",
                minAppVersion: "1.5.0",
                createdAt: createdAt,
                createdBy: ""
            )
        )

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testMalformedVersionWithV1ManifestsReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data("not json".utf8))
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testMalformedVersionWithLiteMonthAndV1ManifestReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: versionPath, data: Data("not json".utf8))
        await client.seedFile(path: RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: 2024, month: 1)))
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
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

    // MARK: - OS noise files (CRA-P02-1 regression)

    func testUncommittedRepoWithDSStoreReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/.DS_Store")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testUncommittedRepoWithAppleDoubleFileReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/._DS_Store")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testMonthsDirAppleDoubleSqliteDoesNotTriggerDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))/._2024-03.sqlite")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testMonthsDirMalformedSqliteReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))/2024-13.sqlite")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testMonthsDirForeignSqliteReturnsDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))/copy-2024-03.sqlite")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testUncommittedRepoWithThumbsDbReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/Thumbs.db")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testUncommittedRepoWithDesktopIniReturnsFresh() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/desktop.ini")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .fresh)
    }

    func testUncommittedRepoWithTrulyUnknownFileStillDamaged() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/foreign_writer_marker")

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .damaged)
    }

    func testUncommittedRepoWithNoisePlusV1ManifestsReturnsV1Migrate() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "\(repoDir)/.DS_Store")
        await client.seedFile(path: v1ManifestPath(year: 2024, month: 1))

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .v1Migrate)
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

    func testCurrentFormatWithFutureMinAppVersionReturnsCurrent() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(
            path: versionPath,
            data: try versionBytes(formatVersion: 2, layout: "lite-month-sqlite", minAppVersion: "9.9.9")
        )

        let decision = try await router(client).classify()
        XCTAssertEqual(decision, .current)
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

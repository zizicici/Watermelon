import XCTest
@testable import Watermelon

// P07 (V1ToLiteMigration): foreground V1→Lite migration. Covers the per-month copy/validate/publish,
// idempotent + interrupted reruns, the TOCTOU re-read, the ownership fail-closed gates, background
// skip, the old-client upgrade path, and one true real-SQLite run on a disk-backed remote.
final class V1ToLiteMigrationTests: XCTestCase {
    private let basePath = "/photos"

    private func newWriterID() -> String { UUID().uuidString.lowercased() }

    private func liteMonthPath(_ year: Int, _ month: Int) -> String {
        MonthManifestStore.ManifestLayout.lite.manifestAbsolutePath(basePath: basePath, year: year, month: month)
    }

    private func v1ManifestPath(_ year: Int, _ month: Int) -> String {
        MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(basePath: basePath, year: year, month: month)
    }

    private func versionPath() -> String { RepoLayoutLite.versionPath(basePath: basePath) }

    // A genuine V1 month manifest (real MonthManifestStore SQLite), so a copy actually validates.
    private func seedRealV1Month(
        client: any RemoteStorageClientProtocol,
        year: Int,
        month: Int,
        fileName: String = "a.jpg",
        contentHash: Data = Data([0xAB])
    ) async throws {
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, layout: .v1
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: contentHash, fileName: fileName)
        )
        _ = try await store.flushToRemote()
    }

    private func makeProfile(writerID: String?) -> ServerProfileRecord {
        ServerProfileRecord(
            id: 1,
            name: "server",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "host.local",
            port: 445,
            shareName: "share",
            basePath: basePath,
            username: "user",
            domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: writerID
        )
    }

    // MARK: - Per-month copy (atomic temp → validate → move)

    func testPerMonthCopyIsAtomicViaValidatedTempThenMove() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let uploadsBefore = await client.uploadedPaths

        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")

        let uploads = Array((await client.uploadedPaths).dropFirst(uploadsBefore.count))
        let tempUploads = uploads.filter { $0.hasPrefix("/photos/.watermelon/months/") && $0.hasSuffix(".tmp") }
        XCTAssertEqual(tempUploads.count, 1, "exactly one temp copy per month, never a direct final write")
        XCTAssertFalse(uploads.contains(liteMonthPath(2024, 3)), "must not upload directly to the final month path")

        let moves = await client.movedPaths
        XCTAssertTrue(
            moves.contains { $0.from == tempUploads[0] && $0.to == liteMonthPath(2024, 3) },
            "publish must rename the validated temp onto the final month path"
        )
        let finalData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(finalData, "final month present after publish")
        let tempData = await client.fileData(path: tempUploads[0])
        XCTAssertNil(tempData, "temp must not linger after publish")
    }

    func testInvalidCopyDoesNotPublishFinalOrCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        // Present but corrupt: a non-SQLite blob at the V1 manifest path.
        await client.seedFile(path: v1ManifestPath(2024, 3), data: Data([0x01, 0x02, 0x03]))

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a copy that fails quick_check must abort the migration")
        } catch let error as V1ToLiteMigration.Failure {
            XCTAssertEqual(error, .monthManifestUnreadable(month: "2024-03"))
        }

        let finalData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNil(finalData, "no final month published for an invalid copy")
        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when a month fails")
        let temps = (await client.uploadedPaths).filter { $0.hasPrefix("/photos/.watermelon/months/") }
        for temp in temps {
            let tempData = await client.fileData(path: temp)
            XCTAssertNil(tempData, "temp copy must be cleaned up on failure")
        }
    }

    // R02 (Codex High): a non-not-found metadata fault on one candidate month manifest must abort
    // enumeration, not silently drop the month and then commit an incomplete `.current` repo.
    func testEnumerationFailsClosedOnNonNotFoundMetadataFault() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3, fileName: "a.jpg", contentHash: Data([0xAB]))
        try await seedRealV1Month(client: client, year: 2024, month: 5, fileName: "b.jpg", contentHash: Data([0xCD]))
        // Script a retryable (non-not-found) metadata fault on the second month's manifest probe.
        await client.failMetadata(
            forPathSuffix: "2024/05/\(MonthManifestStore.manifestFileName)",
            error: RemoteErrorFixtures.retryable
        )

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
            XCTFail("a non-not-found metadata fault during enumeration must fail closed")
        } catch {
            XCTAssertEqual(RemoteFaultLite.classify(error), .retryable, "the metadata fault must surface, not read as absence")
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit when enumeration hit a non-not-found fault")
        let month3 = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNil(month3, "no month may be published once enumeration fails closed")
        let month5 = await client.fileData(path: liteMonthPath(2024, 5))
        XCTAssertNil(month5, "the faulted month must not be published")
        let decision = try await RepoFormatRouter(client: client, basePath: basePath).classify()
        XCTAssertEqual(decision, .v1Migrate, "no incomplete .current route may be produced")
    }

    // MARK: - Idempotency / resume

    func testIdempotentRerunRoutesCurrentWithoutRecopy() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let writerID = newWriterID()

        let plan1 = try await LiteRepoGateway.prepareForegroundWrite(client: client, basePath: basePath, writerID: writerID)
        await plan1.session.stopAndRelease()
        let afterFirst = await client.uploadedPaths

        let plan2 = try await LiteRepoGateway.prepareForegroundWrite(client: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan2.layout, .lite)
        await plan2.session.stopAndRelease()

        let rerun = Array((await client.uploadedPaths).dropFirst(afterFirst.count))
        XCTAssertFalse(rerun.contains { $0.hasPrefix("/photos/.watermelon/months/") },
                       "an idempotent rerun (.current) must not re-copy month manifests")
        XCTAssertFalse(rerun.contains(versionPath()),
                       "an idempotent rerun (.current) must not rewrite version.json")
    }

    func testInterruptedBeforeVersionResumesAndFinishes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let writerID = newWriterID()

        // Faithful "interrupted after month copy, before version commit" state: run the migration
        // copy+commit directly (which leaves the V1 manifest in place — only the P08 gateway path cleans
        // it), then drop version.json. The repo still holds the V1 manifest + the Lite month, so the
        // route re-reads as .v1Migrate and resumes.
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: writerID)
        let committedAfterFirst = await client.fileData(path: versionPath())
        XCTAssertNotNil(committedAfterFirst)

        try await client.delete(path: versionPath())
        let beforeResume = await client.uploadedPaths

        let plan2 = try await LiteRepoGateway.prepareForegroundWrite(client: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan2.layout, .lite)
        await plan2.session.stopAndRelease()

        let versionAfterResume = await client.fileData(path: versionPath())
        XCTAssertNotNil(versionAfterResume, "resume recommits version.json")
        let monthAfterResume = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(monthAfterResume, "the migrated month survives the resume")
        let resume = Array((await client.uploadedPaths).dropFirst(beforeResume.count))
        XCTAssertFalse(resume.contains { $0.hasPrefix("/photos/.watermelon/months/") },
                       "resume must skip the already-valid Lite month rather than re-copy it")
    }

    func testExistingValidFinalSkippedWithinMigration() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
        // Drop version.json so a rerun still classifies as .v1Migrate while the Lite month is already valid.
        try await client.delete(path: versionPath())
        let uploadsBefore = await client.uploadedPaths

        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")

        let rerun = Array((await client.uploadedPaths).dropFirst(uploadsBefore.count))
        XCTAssertFalse(rerun.contains { $0.hasPrefix("/photos/.watermelon/months/") && $0.hasSuffix(".tmp") },
                       "an already-valid final month must be skipped, not re-copied")
        // version.json is committed crash-aware now: temp upload under .watermelon, then publish-by-move.
        XCTAssertTrue(rerun.contains { $0.hasPrefix("/photos/.watermelon/") && $0.hasSuffix(".json.tmp") },
                       "rerun still re-commits version.json via its temp publish")
    }

    // MARK: - Resource-path preservation

    func testResourcePathsPreservedAndDataBytesNotMoved() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        await client.seedFile(path: "\(basePath)/2024/03/IMG_0001.JPG", data: Data([0xDE, 0xAD]))
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0x01]), fileName: "IMG_0001.JPG")
        )
        _ = try await store.flushToRemote()
        let writerID = newWriterID()
        let movesBefore = await client.movedPaths

        let plan = try await LiteRepoGateway.prepareForegroundWrite(client: client, basePath: basePath, writerID: writerID)
        await plan.session.stopAndRelease()

        let dataFile = await client.fileData(path: "\(basePath)/2024/03/IMG_0001.JPG")
        XCTAssertNotNil(dataFile, "data resource path preserved")
        let v1Manifest = await client.fileData(path: v1ManifestPath(2024, 3))
        XCTAssertNil(v1Manifest, "old V1 manifest cleaned after migration commit (P08)")

        let migrationMoves = Array((await client.movedPaths).dropFirst(movesBefore.count))
        XCTAssertTrue(migrationMoves.contains { $0.to.hasPrefix("/photos/.watermelon/months/") },
                      "migration must publish at least one Lite month manifest")
        for move in migrationMoves {
            // Month-manifest publishes and the crash-aware version.json publish are the only moves; data
            // bytes under <YYYY>/<MM> are never moved, so every move stays under .watermelon/.
            XCTAssertTrue(move.from.hasPrefix("/photos/.watermelon/"), "only Lite metadata temps move, got \(move.from)")
            XCTAssertTrue(move.to.hasPrefix("/photos/.watermelon/"), "only publishes into Lite metadata, got \(move.to)")
        }
    }

    // MARK: - TOCTOU re-read

    func testPostLockRouteReReadHandlesTOCTOU() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let writerID = newWriterID()
        // TOCTOU: by the time we hold the lock a concurrent writer committed a Lite version.json. The
        // post-lock re-read performs the only version download, so this scripts that read as .current.
        let committed = VersionManifestLite.makeManifest(createdAt: "x", createdBy: "other")
        await client.enqueueDownloadData(try VersionManifestLite.encode(committed))
        let uploadsBefore = await client.uploadedPaths

        let plan = try await LiteRepoGateway.prepareForegroundWrite(client: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan.layout, .lite)
        await plan.session.stopAndRelease()

        let uploads = Array((await client.uploadedPaths).dropFirst(uploadsBefore.count))
        XCTAssertFalse(uploads.contains { $0.hasPrefix("/photos/.watermelon/months/") },
                       "post-lock .current must skip month migration copies")
        XCTAssertFalse(uploads.contains(versionPath()),
                       "post-lock .current must not rewrite version.json")
    }

    // MARK: - Ownership fail-closed

    func testOwnershipLossBeforePublishFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath, assertOwnership: { false })
                .run(createdAt: "t", createdBy: "id")
            XCTFail("lost ownership before publish must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let publishedMonth = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNil(publishedMonth, "no month published when ownership is lost before publish")
        let committedVersion = await client.fileData(path: versionPath())
        XCTAssertNil(committedVersion, "no version commit when ownership is lost")
    }

    func testOwnershipLossBeforeVersionCommitFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        // Pre-publish the valid Lite month so every month is skipped and only the commit gate remains.
        try await V1ToLiteMigration(client: client, basePath: basePath).run(createdAt: "t", createdBy: "id")
        try await client.delete(path: versionPath())

        do {
            try await V1ToLiteMigration(client: client, basePath: basePath, assertOwnership: { false })
                .run(createdAt: "t", createdBy: "id")
            XCTFail("lost ownership before the version commit must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let versionData = await client.fileData(path: versionPath())
        XCTAssertNil(versionData, "version.json must not commit after ownership loss")
        let monthData = await client.fileData(path: liteMonthPath(2024, 3))
        XCTAssertNotNil(monthData, "the previously-migrated month remains")
    }

    // MARK: - Background skip / old-client upgrade path

    func testBackgroundV1MigrateSkipsWithoutMigration() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: v1ManifestPath(2024, 3), data: Data([0x01]))

        let outcome = await LiteRepoGateway.prepareBackgroundWrite(
            client: client, basePath: basePath, writerID: newWriterID()
        )
        guard case .skip = outcome else { return XCTFail("background .v1Migrate must skip") }
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "background must not migrate or write anything for a V1 tree")
    }

    func testMigratedRepoTriggersOldClientUpgradePath() async throws {
        let client = InMemoryRemoteStorageClient()
        try await seedRealV1Month(client: client, year: 2024, month: 3)
        let writerID = newWriterID()
        let plan = try await LiteRepoGateway.prepareForegroundWrite(client: client, basePath: basePath, writerID: writerID)
        await plan.session.stopAndRelease()

        // An old (flag-off) build runs the V1 compatibility verifier, which refuses a .watermelon repo
        // and surfaces the minimum app version → upgrade-required path.
        do {
            try await RemoteFormatCompatibilityService().verify(client: client, profile: makeProfile(writerID: nil))
            XCTFail("a migrated repo must trip the old-client upgrade-required path")
        } catch let error as BackupCompatibilityError {
            guard case .remoteFormatUnsupported(let minVersion) = error else {
                return XCTFail("unexpected compatibility error: \(error)")
            }
            XCTAssertEqual(minVersion, VersionManifestLite.minAppVersion)
        }
    }

    // MARK: - One true real-SQLite migration on a disk-backed remote

    func testTrueV1RepositoryMigrationSucceedsOnDiskRemote() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-v1lite-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let client = DiskBackedRemoteStorageClient(rootURL: root)
        let writerID = newWriterID()

        // Real V1 month: actual MonthManifestStore SQLite content plus a data resource under YYYY/MM.
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .v1
        )
        // Stage the source outside `root`: DiskBackedRemoteStorageClient maps remote paths back into
        // `root`, so a source living at the remote's on-disk location would collide with its own upload.
        let dataURL = FileManager.default.temporaryDirectory.appendingPathComponent("seed_\(UUID().uuidString).JPG")
        try Data([0xDE, 0xAD, 0xBE, 0xEF]).write(to: dataURL)
        defer { try? FileManager.default.removeItem(at: dataURL) }
        try await client.upload(
            localURL: dataURL,
            remotePath: "\(basePath)/2024/03/IMG_0001.JPG",
            respectTaskCancellation: false,
            onProgress: nil
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0x01, 0x02]), fileName: "IMG_0001.JPG")
        )
        _ = try await store.flushToRemote()

        let plan = try await LiteRepoGateway.prepareForegroundWrite(client: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan.layout, .lite)
        await plan.session.stopAndRelease()

        let fm = FileManager.default
        func exists(_ rel: String) -> Bool { fm.fileExists(atPath: root.appendingPathComponent(rel).path) }
        XCTAssertTrue(exists("photos/.watermelon/version.json"), "version.json committed")
        XCTAssertTrue(exists("photos/.watermelon/months/2024-03.sqlite"), "month relocated to the Lite path")
        XCTAssertFalse(exists("photos/2024/03/.watermelon_manifest.sqlite"), "old V1 manifest cleaned after migration commit (P08)")
        XCTAssertTrue(exists("photos/2024/03/IMG_0001.JPG"), "data resource untouched")

        // The relocated Lite manifest is a valid manifest carrying the real resource.
        let migrated = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: basePath, year: 2024, month: 3, layout: .lite, pushSchemaUpgrade: false
        )
        let liteStore = try XCTUnwrap(migrated)
        XCTAssertNotNil(liteStore.findByFileName("IMG_0001.JPG"), "Lite manifest preserves the migrated resource")

        // A subsequent foreground prepare now routes as .current and re-copies nothing.
        let plan2 = try await LiteRepoGateway.prepareForegroundWrite(client: client, basePath: basePath, writerID: writerID)
        XCTAssertEqual(plan2.layout, .lite)
        await plan2.session.stopAndRelease()
    }
}

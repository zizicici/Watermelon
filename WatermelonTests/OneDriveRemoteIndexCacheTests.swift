import GRDB
import XCTest
@testable import Watermelon

final class OneDriveRemoteIndexCacheTests: XCTestCase {
    private let basePath = "/photos"
    private var cacheDirectory: URL!

    override func setUpWithError() throws {
        cacheDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WM-OneDriveRemoteIndexCache-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: cacheDirectory, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        if let cacheDirectory { try? FileManager.default.removeItem(at: cacheDirectory) }
        cacheDirectory = nil
    }

    func testOneDriveSecondSyncUsesPersistedManifestSnapshotWithoutDownloadingUnchangedMonth() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        let modifiedAt = Date(timeIntervalSince1970: 1_700_000_000)
        try await seedLiteMonth(client, month: month, hashByte: 0xAB, modifiedAt: modifiedAt)

        let diskCache = RemoteManifestSnapshotDiskCache(directory: cacheDirectory)
        let profile = try oneDriveProfile()

        _ = try await RemoteIndexSyncService(diskCache: diskCache)
            .syncIndex(client: client, profile: profile, layout: .lite)
        let downloadsAfterFirstSync = await client.downloadAttemptPaths
        XCTAssertEqual(downloadsAfterFirstSync.filter { $0 == liteMonthPath(month) }.count, 1)

        let secondService = RemoteIndexSyncService(diskCache: diskCache)
        _ = try await secondService.syncIndex(client: client, profile: profile, layout: .lite)

        let downloadsAfterSecondSync = await client.downloadAttemptPaths
        XCTAssertEqual(downloadsAfterSecondSync, downloadsAfterFirstSync)
        let snapshot = secondService.fullSnapshot()
        XCTAssertEqual(snapshot.resources.map(\.fileName), ["f171.jpg"])
    }

    func testOneDriveDigestChangeDownloadsChangedManifestInsteadOfHydratingStaleSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        try await seedLiteMonth(
            client,
            month: month,
            hashByte: 0xAB,
            modifiedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )

        let diskCache = RemoteManifestSnapshotDiskCache(directory: cacheDirectory)
        let profile = try oneDriveProfile()
        _ = try await RemoteIndexSyncService(diskCache: diskCache)
            .syncIndex(client: client, profile: profile, layout: .lite)
        let downloadsAfterFirstSync = await client.downloadAttemptPaths

        try await seedLiteMonth(
            client,
            month: month,
            hashByte: 0xCD,
            modifiedAt: Date(timeIntervalSince1970: 1_700_000_100)
        )

        let secondService = RemoteIndexSyncService(diskCache: diskCache)
        _ = try await secondService.syncIndex(client: client, profile: profile, layout: .lite)

        let downloadsAfterSecondSync = await client.downloadAttemptPaths
        XCTAssertEqual(downloadsAfterSecondSync.filter { $0 == liteMonthPath(month) }.count, 2)
        XCTAssertGreaterThan(downloadsAfterSecondSync.count, downloadsAfterFirstSync.count)
        let snapshot = secondService.fullSnapshot()
        XCTAssertEqual(snapshot.resources.map(\.fileName), ["f205.jpg"])
    }

    func testOneDriveManifestDownloadTransientFailureRetriesDuringSync() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        try await seedLiteMonth(
            client,
            month: month,
            hashByte: 0xAB,
            modifiedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        let service = RemoteIndexSyncService(diskCache: RemoteManifestSnapshotDiskCache(directory: cacheDirectory))
        _ = try await service.syncIndex(client: client, profile: try oneDriveProfile(), layout: .lite)

        let downloads = await client.downloadAttemptPaths
        XCTAssertEqual(downloads.filter { $0 == liteMonthPath(month) }.count, 2)
        XCTAssertEqual(service.fullSnapshot().resources.map(\.fileName), ["f171.jpg"])
    }

    func testNonOneDriveManifestDownloadTransientFailureDoesNotRetryDuringSync() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        try await seedLiteMonth(
            client,
            month: month,
            hashByte: 0xAB,
            modifiedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        do {
            _ = try await RemoteIndexSyncService(diskCache: RemoteManifestSnapshotDiskCache(directory: cacheDirectory))
                .syncIndex(client: client, profile: webDAVProfile(), layout: .lite)
            XCTFail("Expected sync to keep non-OneDrive manifest download behavior")
        } catch {
            let downloads = await client.downloadAttemptPaths
            XCTAssertEqual(downloads.filter { $0 == liteMonthPath(month) }.count, 1)
            XCTAssertEqual((error as NSError).domain, "RemoteIndexSyncService")
            XCTAssertEqual((error as NSError).code, -21)
        }
    }

    func testOneDriveScopedSyncDoesNotOverwriteFullPersistedManifestSnapshot() async throws {
        let client = InMemoryRemoteStorageClient()
        let older = LibraryMonthKey(year: 2024, month: 2)
        let newer = LibraryMonthKey(year: 2024, month: 3)
        try await seedLiteMonth(
            client,
            month: older,
            hashByte: 0xAA,
            modifiedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        try await seedLiteMonth(
            client,
            month: newer,
            hashByte: 0xBB,
            modifiedAt: Date(timeIntervalSince1970: 1_700_000_100)
        )

        let diskCache = RemoteManifestSnapshotDiskCache(directory: cacheDirectory)
        let profile = try oneDriveProfile()
        _ = try await RemoteIndexSyncService(diskCache: diskCache)
            .syncIndex(client: client, profile: profile, layout: .lite)
        let downloadsAfterFullSync = await client.downloadAttemptPaths
        XCTAssertEqual(downloadsAfterFullSync.filter { $0 == liteMonthPath(older) }.count, 1)
        XCTAssertEqual(downloadsAfterFullSync.filter { $0 == liteMonthPath(newer) }.count, 1)

        _ = try await RemoteIndexSyncService(diskCache: diskCache)
            .syncIndex(
                client: client,
                profile: profile,
                layout: .lite,
                monthFilter: [newer]
            )
        let downloadsAfterScopedSync = await client.downloadAttemptPaths
        XCTAssertEqual(downloadsAfterScopedSync.filter { $0 == liteMonthPath(newer) }.count, 2)

        _ = try await RemoteIndexSyncService(diskCache: diskCache)
            .syncIndex(client: client, profile: profile, layout: .lite)
        let downloadsAfterSecondFullSync = await client.downloadAttemptPaths
        XCTAssertEqual(downloadsAfterSecondFullSync, downloadsAfterScopedSync)
    }

    func testTrustedSeededLoadDoesNotListOneDriveDataDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        await client.seedFile(
            path: liteMonthPath(month),
            data: Data([0x01]),
            modificationDate: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let resource = TestFixtures.remoteResource(
            year: month.year,
            month: month.month,
            contentHash: Data([0xAB]),
            fileName: "seeded.jpg"
        )
        let seed = MonthManifestStore.Seed(
            resources: [resource],
            assets: [],
            assetResourceLinks: [],
            resourceListingPolicy: .trustManifestResources
        )

        let store = try await MonthManifestStore.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            seed: seed,
            layout: .lite,
            assertOwnership: .uniform({})
        )

        XCTAssertEqual(store.findByFileName("seeded.jpg")?.fileName, "seeded.jpg")
        let listedPaths = await client.listedPaths
        XCTAssertFalse(listedPaths.contains("\(basePath)/2024/03"))
    }

    // OneDrive's collision check can't download a colliding remote file to compare it, and a sub-10MB upload
    // transmits the whole body before the 409 lands — so the seeded load must see remote-only names up front.
    func testSeededLoadSeesRemoteOnlyResourceSoTheCollisionCheckCanPredictIt() async throws {
        let client = InMemoryRemoteStorageClient()
        let month = LibraryMonthKey(year: 2024, month: 3)
        await client.seedFile(
            path: liteMonthPath(month),
            data: Data([0x01]),
            modificationDate: Date(timeIntervalSince1970: 1_700_000_000)
        )
        // An interrupted run uploaded this and never recorded it, so it exists remotely but not in the seed.
        await client.seedFile(path: "\(basePath)/2024/03/orphan.jpg", data: Data([0x09]))
        let seed = MonthManifestStore.Seed(
            resources: [
                TestFixtures.remoteResource(
                    year: month.year,
                    month: month.month,
                    contentHash: Data([0xAB]),
                    fileName: "seeded.jpg"
                )
            ],
            assets: [],
            assetResourceLinks: []
        )

        let store = try await MonthManifestStore.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            seed: seed,
            layout: .lite,
            assertOwnership: .uniform({})
        )

        XCTAssertTrue(store.existingFileNames().contains("orphan.jpg"), "the remote-only orphan must be visible")
        XCTAssertTrue(store.existingCollisionKeys().contains("orphan.jpg".lowercased()))
        let listedPaths = await client.listedPaths
        XCTAssertTrue(listedPaths.contains("\(basePath)/2024/03"), "the data directory must be listed")
    }

    // OneDrive used to be the one backend seeded with `.trustManifestResources`; pin that the run's seed
    // builder no longer forks per backend, or the collision blind spot above comes straight back.
    func testMonthSeedLookupVerifiesTheRemoteDirectoryForOneDrive() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-seed-policy-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        let remoteIndexService = RemoteIndexSyncService()
        let month = LibraryMonthKey(year: 2024, month: 3)
        remoteIndexService.replaceCachedMonth(
            month,
            resources: [
                TestFixtures.remoteResource(
                    year: month.year,
                    month: month.month,
                    contentHash: Data([0xAB]),
                    fileName: "seeded.jpg"
                )
            ],
            assets: [],
            links: [],
            expectedProfileKey: nil
        )
        let service = BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(databaseManager: database),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: database),
            remoteIndexService: remoteIndexService,
            databaseManager: database
        )

        let lookup = service.makeMonthSeedLookup(
            from: RemoteIndexSyncDigest(resourceCount: 1, assetCount: 0, linkCount: 0),
            eventStream: BackupEventStream()
        )

        let seed = try XCTUnwrap(lookup?.seed(for: MonthKey(year: month.year, month: month.month)))
        guard case .verifyRemoteDirectory = seed.resourceListingPolicy else {
            return XCTFail("every backend, OneDrive included, must verify the remote directory")
        }
    }

    private func seedLiteMonth(
        _ client: InMemoryRemoteStorageClient,
        month: LibraryMonthKey,
        hashByte: UInt8,
        modifiedAt: Date
    ) async throws {
        await client.seedFile(
            path: liteMonthPath(month),
            data: try makeManifestData(month: month, hashByte: hashByte),
            modificationDate: modifiedAt
        )
    }

    private func makeManifestData(month: LibraryMonthKey, hashByte: UInt8) throws -> Data {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WM-OneDriveManifest-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let url = directory.appendingPathComponent("month.sqlite")
        let queue = try DatabaseQueue(path: url.path)
        try MonthManifestStore.migrate(queue)
        let store = MonthManifestStore(
            client: InMemoryRemoteStorageClient(),
            basePath: basePath,
            year: month.year,
            month: month.month,
            localManifestURL: url,
            dbQueue: queue,
            remoteFilesByName: [:],
            dirty: false,
            layout: .lite
        )
        let hash = Data([hashByte])
        let fingerprint = Data([hashByte, 0x01])
        let resource = TestFixtures.remoteResource(
            year: month.year,
            month: month.month,
            contentHash: hash,
            fileName: "f\(hashByte).jpg"
        )
        try store.upsertResource(resource)
        try store.upsertAsset(
            TestFixtures.remoteAsset(year: month.year, month: month.month, fingerprint: fingerprint),
            links: [
                TestFixtures.remoteLink(
                    year: month.year,
                    month: month.month,
                    assetFingerprint: fingerprint,
                    resourceHash: hash
                )
            ]
        )
        try queue.close()
        return try Data(contentsOf: url)
    }

    private func liteMonthPath(_ month: LibraryMonthKey) -> String {
        RepoLayoutLite.monthPath(basePath: basePath, month: month)
    }

    private func oneDriveProfile() throws -> ServerProfileRecord {
        let params = OneDriveConnectionParams(
            driveID: "drive",
            rootItemID: "root",
            displayRootPath: "OneDrive/Apps/Watermelon"
        )
        return ServerProfileRecord(
            id: 42,
            name: "OneDrive",
            storageType: StorageType.onedrive.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(params),
            sortOrder: 0,
            host: "graph.microsoft.com",
            port: 443,
            shareName: "root",
            basePath: basePath,
            username: "account@example.com",
            domain: nil,
            credentialRef: "credential",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: nil
        )
    }

    private func webDAVProfile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: 43,
            name: "WebDAV",
            storageType: StorageType.webdav.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "example.com",
            port: 443,
            shareName: "",
            basePath: basePath,
            username: "account@example.com",
            domain: nil,
            credentialRef: "credential",
            backgroundBackupEnabled: false,
            createdAt: Date(),
            updatedAt: Date(),
            writerID: nil
        )
    }
}

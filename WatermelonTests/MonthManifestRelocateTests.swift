import XCTest
import GRDB
@testable import Watermelon

// Step 5 (P05-MonthManifestRelocate): explicit manifest layout, dormant Lite relocation,
// layout-gated discovery, and hardened flush (export + quick_check + read-back verify).
final class MonthManifestRelocateTests: XCTestCase {
    private let basePath = "/photos"
    private let year = 2024
    private let month = 3

    private var v1Layout: MonthManifestStore.ManifestLayout { .v1 }
    private var liteLayout: MonthManifestStore.ManifestLayout { .lite }

    // MARK: - Layout paths

    func testV1ManifestPathMatchesLegacyLayout() {
        XCTAssertEqual(
            v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month),
            "/photos/2024/03/.watermelon_manifest.sqlite"
        )
        XCTAssertEqual(
            v1Layout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month),
            "/photos/2024/03"
        )
    }

    func testLiteManifestPathUnderWatermelonMonths() {
        XCTAssertEqual(
            liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month),
            "/photos/.watermelon/months/2024-03.sqlite"
        )
        XCTAssertEqual(
            liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month),
            "/photos/.watermelon/months"
        )
    }

    func testResourceRemotePathStaysYearMonthFilenameForBothLayouts() {
        let resource = TestFixtures.remoteResource(
            year: year, month: month, contentHash: Data([0x01]), fileName: "IMG_0001.JPG"
        )
        // Data/resource path is layout-independent and must stay YYYY/MM/filename.
        XCTAssertEqual(resource.remoteRelativePath, "2024/03/IMG_0001.JPG")
    }

    // MARK: - Flush hardening

    func testLiteFlushRelocatesManifestAndKeepsDataPaths() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        XCTAssertTrue(store.dirty)

        let flushed = try await store.flushToRemote()
        XCTAssertTrue(flushed)
        XCTAssertFalse(store.dirty)

        // Manifest lives at the Lite path; nothing landed at the V1 path.
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let v1Path = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let liteData = await client.fileData(path: litePath)
        let v1Data = await client.fileData(path: v1Path)
        XCTAssertNotNil(liteData)
        XCTAssertNil(v1Data)

        // The manifest directory was created and the temp upload lived under it.
        let created = await client.createdDirectories
        XCTAssertTrue(created.contains("/photos/.watermelon/months"))
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.allSatisfy { $0.hasPrefix("/photos/.watermelon/months/") })

        try assertPersistedManifestValid(liteData, expectedResourceCount: 1)
        XCTAssertEqual(store.monthRelativePath, "2024/03")
    }

    func testV1FlushKeepsLegacyManifestPath() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )

        _ = try await store.flushToRemote()

        let v1Path = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let v1Data = await client.fileData(path: v1Path)
        XCTAssertNotNil(v1Data)
        let liteData = await client.fileData(path: liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month))
        XCTAssertNil(liteData)
        try assertPersistedManifestValid(v1Data, expectedResourceCount: 1)
    }

    func testFlushReadBackMismatchThrowsAndKeepsDirty() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        // The only download flush performs is its read-back verification: force it to return
        // bytes that differ from what was uploaded.
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))

        do {
            _ = try await store.flushToRemote()
            XCTFail("flush should fail when the read-back bytes differ from the uploaded manifest")
        } catch {
            let ns = error as NSError
            XCTAssertEqual(ns.domain, "MonthManifestStore")
            XCTAssertEqual(ns.code, -36)
        }
        XCTAssertTrue(store.dirty, "a read-back mismatch must keep the manifest dirty for retry")
    }

    // MARK: - Relocation via loadOrCreate

    func testLoadOrCreateLiteRelocatesFreshManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, layout: .lite
        )

        XCTAssertEqual(store.monthRelativePath, "2024/03")
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let v1Path = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let liteData = await client.fileData(path: litePath)
        let v1Data = await client.fileData(path: v1Path)
        XCTAssertNotNil(liteData, "fresh Lite month should flush its empty manifest to the Lite path")
        XCTAssertNil(v1Data)

        let created = await client.createdDirectories
        XCTAssertTrue(created.contains("/photos/2024/03"), "data dir is still created/listed")
        XCTAssertTrue(created.contains("/photos/.watermelon/months"))
    }

    // MARK: - Layout-gated discovery

    func testV1DiscoveryFindsLegacyManifestsAndIgnoresLite() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "/photos/2024/03/.watermelon_manifest.sqlite", data: Data([0x01]))
        await client.seedFile(path: "/photos/2024/05/.watermelon_manifest.sqlite", data: Data([0x01]))
        let service = RemoteIndexSyncService()

        let v1 = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .v1)
        XCTAssertEqual(Set(v1.keys), [LibraryMonthKey(year: 2024, month: 3), LibraryMonthKey(year: 2024, month: 5)])

        // No .watermelon/months directory exists → Lite discovery sees nothing.
        let lite = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
        XCTAssertTrue(lite.isEmpty)
    }

    func testLiteDiscoveryFindsRelocatedManifestsAndIgnoresV1() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "/photos/.watermelon/months/2024-03.sqlite", data: Data([0x01]))
        await client.seedFile(path: "/photos/.watermelon/months/2024-11.sqlite", data: Data([0x01]))
        let service = RemoteIndexSyncService()

        let lite = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
        XCTAssertEqual(Set(lite.keys), [LibraryMonthKey(year: 2024, month: 3), LibraryMonthKey(year: 2024, month: 11)])

        let v1 = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .v1)
        XCTAssertTrue(v1.isEmpty)
    }

    func testLiteDiscoveryMissingMonthsDirectoryMeansNoMonths() async throws {
        let client = InMemoryRemoteStorageClient()   // nothing seeded
        let service = RemoteIndexSyncService()

        let lite = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
        XCTAssertTrue(lite.isEmpty)
    }

    func testLiteDiscoveryListErrorSurfaces() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)   // non-notFound transport fault
        let service = RemoteIndexSyncService()

        do {
            _ = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
            XCTFail("a non-notFound Lite list error must surface, not read as zero months")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
    }

    // MARK: - Helpers

    private func makeStore(
        client: RemoteStorageClientProtocol,
        layout: MonthManifestStore.ManifestLayout
    ) throws -> MonthManifestStore {
        let localURL = MonthManifestStore.makeLocalManifestURL(year: year, month: month)
        let queue = try DatabaseQueue(path: localURL.path)
        try MonthManifestStore.migrate(queue)
        return MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: queue,
            remoteFilesByName: [:],
            dirty: false,
            layout: layout
        )
    }

    private func assertPersistedManifestValid(_ data: Data?, expectedResourceCount: Int) throws {
        let data = try XCTUnwrap(data)
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("verify_\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: url) }
        try data.write(to: url)
        let queue = try DatabaseQueue(path: url.path)
        defer { try? queue.close() }
        let check = try queue.read { try String.fetchAll($0, sql: "PRAGMA quick_check") }
        XCTAssertEqual(check, ["ok"])
        let count = try queue.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM resources") }
        XCTAssertEqual(count, expectedResourceCount)
    }
}

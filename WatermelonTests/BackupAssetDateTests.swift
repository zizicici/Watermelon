import GRDB
import XCTest
@testable import Watermelon

final class BackupAssetDateTests: XCTestCase {
    private func makeStore(client: InMemoryRemoteStorageClient) throws -> MonthManifestStore {
        let url = MonthManifestStore.makeLocalManifestURL(year: 2026, month: 1)
        let queue = try DatabaseQueue(path: url.path)
        try MonthManifestStore.migrate(queue)
        return MonthManifestStore(client: client, basePath: "/photos", year: 2026, month: 1,
            localManifestURL: url, dbQueue: queue, remoteFilesByName: [:], dirty: false,
            layout: .lite, liteWriteOwnership: .uniform({}))
    }

    private func seed(_ store: MonthManifestStore) throws -> RemoteManifestAsset {
        let resource = TestFixtures.remoteResource(year: 2026, month: 1, contentHash: Data([1]), fileName: "photo.jpg")
        _ = try store.upsertResource(resource)
        let asset = RemoteManifestAsset(year: 2026, month: 1, assetFingerprint: Data([2]),
            creationDateMs: 1_000, backedUpAtMs: 2_000, resourceCount: 1, totalFileSizeBytes: resource.fileSize)
        try store.upsertAsset(asset, links: [TestFixtures.remoteLink(year: 2026, month: 1,
            assetFingerprint: asset.assetFingerprint, resourceHash: resource.contentHash, role: 1)])
        return asset
    }

    func testDateUpdatePreservesMediaAndPersistsOnce() async throws {
        let client = InMemoryRemoteStorageClient(), store = try makeStore(client: client)
        let asset = try seed(store)
        _ = try await store.flushToRemote()
        let before = store.unsortedSnapshot()
        let newDate = Date(millisecondsSinceEpoch: 3_000)
        let updated = try XCTUnwrap(store.updateAssetCreationDate(newDate, for: asset.assetFingerprint))
        XCTAssertEqual(updated.creationDateMs, 3_000)
        XCTAssertEqual(updated.backedUpAtMs, asset.backedUpAtMs)
        XCTAssertEqual(updated.totalFileSizeBytes, asset.totalFileSizeBytes)
        XCTAssertEqual(updated.resourceCount, asset.resourceCount)
        XCTAssertEqual(updated.assetFingerprint, asset.assetFingerprint)
        XCTAssertEqual(store.unsortedSnapshot().resources, before.resources)
        XCTAssertEqual(store.unsortedSnapshot().links, before.links)
        XCTAssertTrue(store.dirty)
        _ = try await store.flushToRemote()
        XCTAssertFalse(store.dirty)
        let published = await client.fileData(path: store.manifestAbsolutePath)
        let data = try XCTUnwrap(published)
        let readURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".sqlite")
        defer { try? FileManager.default.removeItem(at: readURL) }
        try data.write(to: readURL)
        let persistedDate = try await DatabaseQueue(path: readURL.path).read {
            try Int64.fetchOne($0, sql: "SELECT creationDateMs FROM assets WHERE assetFingerprint = ?", arguments: [asset.assetFingerprint])
        }
        XCTAssertEqual(persistedDate, 3_000)
        let uploads = await client.uploadedPaths.count
        XCTAssertNil(try store.updateAssetCreationDate(newDate, for: asset.assetFingerprint))
        XCTAssertFalse(store.dirty)
        let flushed = try await store.flushToRemote()
        XCTAssertFalse(flushed)
        let afterUploads = await client.uploadedPaths.count
        XCTAssertEqual(afterUploads, uploads)
    }

    func testDateUpdateRemainsPendingAfterFailedFlush() async throws {
        let client = InMemoryRemoteStorageClient(), store = try makeStore(client: client)
        let asset = try seed(store)
        _ = try await store.flushToRemote()
        _ = try store.updateAssetCreationDate(Date(millisecondsSinceEpoch: 3_000), for: asset.assetFingerprint)
        await client.enqueueUploadError(RemoteErrorFixtures.terminal)
        do {
            _ = try await store.flushToRemote()
            XCTFail("Expected upload failure")
        } catch {}
        XCTAssertTrue(store.dirty)
        _ = try await store.flushToRemote()
        XCTAssertFalse(store.dirty)
        XCTAssertEqual(store.assetsByFingerprint[asset.assetFingerprint]?.creationDateMs, 3_000)
    }

    func testDateOnlySuccessParticipatesInFlushRecoveryAndProgress() {
        let result = AssetProcessResult(status: .success, reason: AssetProcessor.assetDateUpdatedReason,
            displayName: "photo.jpg", assetFingerprint: Data([1]), timing: AssetProcessTiming(),
            totalFileSizeBytes: 1_024, uploadedFileSizeBytes: 0)
        XCTAssertTrue(BackupParallelExecutor.resultDirtiedMonthManifest(status: result.status, reason: result.reason))
        XCTAssertTrue(BackupParallelExecutor.shouldEmitResultCredit(result))
        XCTAssertEqual(result.uploadedFileSizeBytes, 0)
    }
}

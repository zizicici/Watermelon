import GRDB
import XCTest
@testable import Watermelon

// Verify-path reconcile (reconcileMonth) prunes only MEANINGLESS records (phantom / all-missing / config-only)
// and KEEPS partial-but-has-media + fingerprint-divergent records — they hold valid media the user can restore
// as a new asset. Guards against the earlier strict behavior that deleted any incomplete record (data loss).
final class MonthManifestReconcileTests: XCTestCase {
    private let basePath = "/photos"
    private let year = 2024
    private let month = 3

    private func makeStore() throws -> MonthManifestStore {
        let localURL = MonthManifestStore.makeLocalManifestURL(year: year, month: month)
        try? FileManager.default.removeItem(at: localURL)
        let queue = try DatabaseQueue(path: localURL.path)
        try MonthManifestStore.migrate(queue)
        return MonthManifestStore(
            client: InMemoryRemoteStorageClient(), basePath: basePath, year: year, month: month,
            localManifestURL: localURL, dbQueue: queue, remoteFilesByName: [:], dirty: false,
            layout: .lite, liteWriteOwnership: {}
        )
    }

    private func resource(_ name: String, _ hash: Data) -> RemoteManifestResource {
        RemoteManifestResource(year: year, month: month, fileName: name, contentHash: hash, fileSize: 100, resourceType: 1, creationDateMs: nil, backedUpAtMs: 0)
    }
    private func asset(_ fp: Data, count: Int) -> RemoteManifestAsset {
        RemoteManifestAsset(year: year, month: month, assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: 0, resourceCount: count, totalFileSizeBytes: 100)
    }
    private func link(_ fp: Data, _ hash: Data, role: Int, slot: Int = 0) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fp, resourceHash: hash, role: role, slot: slot)
    }

    func testReconcileKeepsPartialButHasMedia() throws {
        // A Live Photo whose paired video went missing but whose photo survives is still restorable → KEEP.
        let store = try makeStore()
        let hPhoto = Data([1]); let hClip = Data([2])
        let fp = Data([0xA])
        _ = try store.upsertResource(resource("p", hPhoto))
        _ = try store.upsertResource(resource("v", hClip))
        try store.upsertAsset(asset(fp, count: 2), links: [
            link(fp, hPhoto, role: ResourceTypeCode.photo),
            link(fp, hClip, role: ResourceTypeCode.pairedVideo)
        ])

        _ = try store.reconcileMonth(missingHashes: [hClip])   // the paired clip is gone from the remote

        XCTAssertTrue(store.containsAssetFingerprint(fp), "partial record still has real media (the photo) → kept")
        let snap = store.unsortedSnapshot()
        XCTAssertFalse(snap.resources.contains { $0.contentHash == hClip }, "the missing clip resource is pruned")
        XCTAssertTrue(snap.resources.contains { $0.contentHash == hPhoto }, "the surviving photo resource stays")
    }

    func testReconcileDeletesConfigOnly() throws {
        // Only an adjustment sidecar resolves → no real media → MEANINGLESS → delete.
        let store = try makeStore()
        let hMeta = Data([7])
        let fp = Data([0xB])
        _ = try store.upsertResource(resource("m", hMeta))
        try store.upsertAsset(asset(fp, count: 1), links: [link(fp, hMeta, role: ResourceTypeCode.adjustmentData)])

        _ = try store.reconcileMonth()

        XCTAssertFalse(store.containsAssetFingerprint(fp), "a config-only record has no real media → pruned")
        XCTAssertFalse(store.unsortedSnapshot().resources.contains { $0.contentHash == hMeta },
                       "its now-orphaned sidecar resource row is deleted too (else the remote file leaks)")
    }

    func testReconcileDeletesPhantomWhenAllResourcesMissing() throws {
        let store = try makeStore()
        let hPhoto = Data([1])
        let fp = Data([0xC])
        _ = try store.upsertResource(resource("p", hPhoto))
        try store.upsertAsset(asset(fp, count: 1), links: [link(fp, hPhoto, role: ResourceTypeCode.photo)])

        _ = try store.reconcileMonth(missingHashes: [hPhoto])   // its only resource is gone

        XCTAssertFalse(store.containsAssetFingerprint(fp), "no resolvable media left → pruned")
    }

    func testCleanupMissingResourcesReclaimsConfigOnlyResource() throws {
        // cleanupMissingResources shares the hasBackedUpMedia rule and now also reclaims the pruned asset's
        // now-orphaned resource row (else the leftover scanner keeps protecting the remote sidecar file).
        let store = try makeStore()
        let hMeta = Data([7])
        let fp = Data([0xE])
        _ = try store.upsertResource(resource("m", hMeta))
        try store.upsertAsset(asset(fp, count: 1), links: [link(fp, hMeta, role: ResourceTypeCode.adjustmentData)])

        _ = try store.cleanupMissingResources(missingHashes: [])

        XCTAssertFalse(store.containsAssetFingerprint(fp), "config-only asset has no displayable media → pruned")
        XCTAssertFalse(store.unsortedSnapshot().resources.contains { $0.contentHash == hMeta },
                       "its orphaned sidecar resource row is reclaimed too")
    }

    func testReconcileKeepsCompletePhoto() throws {
        let store = try makeStore()
        let hPhoto = Data([1])
        let fp = Data([0xD])
        _ = try store.upsertResource(resource("p", hPhoto))
        try store.upsertAsset(asset(fp, count: 1), links: [link(fp, hPhoto, role: ResourceTypeCode.photo)])

        _ = try store.reconcileMonth()

        XCTAssertTrue(store.containsAssetFingerprint(fp), "a complete record is untouched")
    }
}

import GRDB
import XCTest
@testable import Watermelon

// Unit coverage for MonthManifestStore.removeAsset — the ref-counted single-asset deletion primitive that
// backs the media browser's "Delete from Backup". A resource shared by another asset must survive; a
// resource exclusively owned by the removed asset must be reported orphaned (so its file can be deleted).
final class MonthManifestRemoveAssetTests: XCTestCase {
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
    private func link(_ fp: Data, _ hash: Data, role: Int, slot: Int) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(year: year, month: month, assetFingerprint: fp, resourceHash: hash, role: role, slot: slot)
    }

    func testRemoveAssetKeepsSharedResourceAndOrphansExclusive() throws {
        let store = try makeStore()
        let h1 = Data([1]); let h2 = Data([2])
        let fpA = Data([0xA]); let fpB = Data([0xB])
        _ = try store.upsertResource(resource("r1", h1))
        _ = try store.upsertResource(resource("r2", h2))
        // A owns r1 (shared with B) + r2 (exclusive); B owns only r1.
        try store.upsertAsset(asset(fpA, count: 2), links: [link(fpA, h1, role: 1, slot: 0), link(fpA, h2, role: 2, slot: 0)])
        try store.upsertAsset(asset(fpB, count: 1), links: [link(fpB, h1, role: 1, slot: 0)])

        let orphans = try store.removeAsset(fingerprint: fpA)
        XCTAssertEqual(orphans, ["r2"], "only the resource A exclusively owned should be reported orphaned")

        let snap = store.unsortedSnapshot()
        XCTAssertFalse(store.containsAssetFingerprint(fpA), "A is removed")
        XCTAssertTrue(store.containsAssetFingerprint(fpB), "B survives")
        XCTAssertTrue(snap.resources.contains { $0.fileName == "r1" }, "shared resource survives (B still links it)")
        XCTAssertFalse(snap.resources.contains { $0.fileName == "r2" }, "exclusive resource is removed from the manifest")
        XCTAssertEqual(snap.assets.count, 1)
    }

    func testRemoveMissingAssetIsNoOp() throws {
        let store = try makeStore()
        XCTAssertEqual(try store.removeAsset(fingerprint: Data([9])), [])
    }

    // A shared resource kept alive by another asset must not leave the snapshot that browser / seed consumers
    // read in an inconsistent state: no dangling link referencing the removed asset, and the surviving asset's
    // links all resolve to present resources with a matching resourceCount.
    func testSurvivingAssetSnapshotStaysConsistentAfterSharerRemoved() throws {
        let store = try makeStore()
        let h1 = Data([1]); let h2 = Data([2])
        let fpA = Data([0xA]); let fpB = Data([0xB])
        _ = try store.upsertResource(resource("r1", h1))
        _ = try store.upsertResource(resource("r2", h2))
        try store.upsertAsset(asset(fpA, count: 2), links: [link(fpA, h1, role: 1, slot: 0), link(fpA, h2, role: 2, slot: 0)])
        try store.upsertAsset(asset(fpB, count: 1), links: [link(fpB, h1, role: 1, slot: 0)])

        _ = try store.removeAsset(fingerprint: fpA)
        let snap = store.unsortedSnapshot()

        // The removed asset leaves no link-only row for a consumer to trip over.
        XCTAssertFalse(snap.links.contains { $0.assetFingerprint == fpA }, "removed asset leaves no dangling link row")

        // Every link in the snapshot resolves to a resource — no orphan link leaks to consumers.
        let resourceHashes = Set(snap.resources.map { $0.contentHash })
        XCTAssertTrue(snap.links.allSatisfy { resourceHashes.contains($0.resourceHash) }, "every surviving link resolves to a resource")

        // The surviving asset's view is complete and self-consistent.
        let bLinks = snap.links.filter { $0.assetFingerprint == fpB }
        XCTAssertEqual(bLinks.count, 1, "B keeps its single link")
        XCTAssertEqual(snap.assets.first { $0.assetFingerprint == fpB }?.resourceCount, bLinks.count, "B's resourceCount matches its links")
    }
}

import XCTest
@testable import Watermelon

final class BackupAdjustmentUpdateTests: XCTestCase {
    private let fingerprint = Data(repeating: 1, count: 32)
    private var asset: RemoteManifestAsset {
        .init(year: 2026, month: 1, assetFingerprint: fingerprint, creationDateMs: 100,
            backedUpAtMs: 200, resourceCount: 3, totalFileSizeBytes: 300)
    }

    private func link(_ role: Int, _ hash: UInt8, slot: Int = 0) -> RemoteAssetResourceLink {
        .init(year: 2026, month: 1, assetFingerprint: fingerprint,
            resourceHash: Data(repeating: hash, count: 32), role: role, slot: slot)
    }

    private func plan(_ local: [RemoteAssetResourceLink], _ remote: [RemoteAssetResourceLink]) -> [RemoteAssetResourceLink]? {
        BackupAssetResourcePlanner.updatedAdjustmentLinks(
            localResources: local.map { ($0.role, $0.slot, $0.resourceHash) },
            remoteAsset: asset, remoteLinks: remote)
    }

    func testDescriptionReplacementKeepsMediaAndIdentity() throws {
        let media = [link(1, 10), link(5, 20), link(8, 30), link(11, 40), link(12, 50)]
        let remote = media + [link(7, 60)]
        let local = media + [link(7, 70)]
        let updated = try XCTUnwrap(plan(local, remote))
        XCTAssertEqual(Set(updated), Set(local))
        XCTAssertEqual(Set(updated.filter { $0.role != 7 }), Set(media))
        XCTAssertEqual(Set(updated.map(\.assetFingerprint)), [fingerprint])
        XCTAssertEqual(BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: remote.map { ($0.role, $0.slot, $0.resourceHash) }),
            BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: updated.map { ($0.role, $0.slot, $0.resourceHash) }))
    }

    func testAddsDescriptionAndUsesRoleAndSlotRatherThanHashAlone() throws {
        let media = [link(1, 10)]
        let local = media + [link(7, 10)]
        XCTAssertEqual(Set(try XCTUnwrap(plan(local, media))), Set(local))
        let remote = media + [link(7, 10, slot: 1)]
        XCTAssertEqual(Set(try XCTUnwrap(plan(local, remote))), Set(local))
    }

    func testAbsentLocalDescriptionPreservesRemoteDescription() {
        let media = [link(1, 10)]
        XCTAssertNil(plan(media, media + [link(7, 20)]))
        XCTAssertNil(plan(media, media))
    }

    func testIdenticalDescriptionsIgnoreOrderingAndNeedNoWrite() {
        let links = [link(1, 10), link(7, 20), link(7, 30, slot: 1)]
        XCTAssertNil(plan(links.reversed(), links))
    }

    func testLocalDescriptionSnapshotReplacesRemoteDescriptionSet() throws {
        let media = [link(1, 10)]
        let local = media + [link(7, 20)]
        let remote = local + [link(7, 30, slot: 1)]
        XCTAssertEqual(Set(try XCTUnwrap(plan(local, remote))), Set(local))
    }

    func testResourceUpdateParticipatesInCheckpointRecoveryAndProgressEvenWithoutUpload() {
        for bytes in [Int64(0), 100] {
            let result = AssetProcessResult(status: .success, reason: AssetProcessor.assetResourcesUpdatedReason,
                displayName: "photo.jpg", assetFingerprint: fingerprint, timing: AssetProcessTiming(),
                totalFileSizeBytes: 10_000, uploadedFileSizeBytes: bytes)
            XCTAssertTrue(BackupParallelExecutor.resultDirtiedMonthManifest(status: result.status, reason: result.reason))
            XCTAssertTrue(BackupParallelExecutor.shouldEmitResultCredit(result))
            let credit = BackupParallelExecutor.estimatedAssetTransferState(assetLocalIdentifier: "asset",
                displayName: result.displayName, totalBytes: result.totalFileSizeBytes, workerID: 1, assetPosition: 1, totalAssets: 1)
            XCTAssertEqual(credit?.countsTowardTransferSpeed, false)
        }
    }
}

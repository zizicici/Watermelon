import XCTest
@testable import Watermelon

/// Verify/download/Home/health all consume this. The case-coverage matrix here
/// is the contract — adding a new IntegrityState case must come with a test row.
final class RemoteAssetIntegrityClassifierTests: XCTestCase {
    private func link(role: Int, slot: Int = 0, hash: Data, fp: Data) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(
            year: 2026, month: 1,
            assetFingerprint: fp, resourceHash: hash,
            role: role, slot: slot, logicalName: "x"
        )
    }

    private func computedFP(role: Int = ResourceTypeCode.photo, slot: Int = 0, hash: Data) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: role, slot: slot, contentHash: hash)]
        )
    }

    func testHealthy_singleResourceAvailable() {
        let h = TestFixtures.fingerprint(0xAA)
        let fp = computedFP(hash: h)
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fp,
            links: [link(role: ResourceTypeCode.photo, hash: h, fp: fp)],
            isResourceAvailable: { _ in true }
        )
        XCTAssertEqual(state, .healthy)
        XCTAssertTrue(state.allowsRestore)
        XCTAssertFalse(state.allowsCleanup)
    }

    func testPhantom_emptyLinks() {
        let fp = TestFixtures.fingerprint(0x10)
        let emptyLinks: [AssetIntegrityLink] = []
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fp,
            links: emptyLinks,
            isResourceAvailable: { _ in true }
        )
        XCTAssertEqual(state, .phantom)
        XCTAssertTrue(state.allowsCleanup, "phantom is auto-cleanup eligible")
    }

    func testFullyMissing_allResourcesGone() {
        let h = TestFixtures.fingerprint(0xBB)
        let fp = computedFP(hash: h)
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fp,
            links: [link(role: ResourceTypeCode.photo, hash: h, fp: fp)],
            isResourceAvailable: { _ in false }
        )
        XCTAssertEqual(state, .fullyMissing)
        XCTAssertTrue(state.allowsCleanup)
    }

    func testMetadataOnlyLeft_onlyAdjustmentRolesAvailable() {
        let h = TestFixtures.fingerprint(0xCC)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.adjustmentData, slot: 0, contentHash: h)]
        )
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fp,
            links: [link(role: ResourceTypeCode.adjustmentData, hash: h, fp: fp)],
            isResourceAvailable: { _ in true }
        )
        XCTAssertEqual(state, .metadataOnlyLeft)
        XCTAssertTrue(state.allowsCleanup, "edit-history alone is unrestorable; cleanup OK")
    }

    func testFingerprintMismatch_storedDoesNotMatchRecomputed() {
        let h = TestFixtures.fingerprint(0xDD)
        let storedFp = TestFixtures.fingerprint(0x11) // arbitrary, NOT the recomputed one
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: storedFp,
            links: [link(role: ResourceTypeCode.photo, hash: h, fp: storedFp)],
            isResourceAvailable: { _ in true }
        )
        guard case .fingerprintMismatch = state else {
            XCTFail("expected fingerprintMismatch, got \(state)")
            return
        }
        XCTAssertFalse(state.allowsCleanup,
                       "mismatch may be tampering, not loss; auto-tombstone would destroy recoverable asset")
        XCTAssertFalse(state.allowsRestore,
                       "if fp doesn't recompute, restore can't trust this asset")
    }

    func testPartiallyMissing_someResourcesAvailable() {
        let h1 = TestFixtures.fingerprint(0xE1)
        let h2 = TestFixtures.fingerprint(0xE2)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [
                (role: ResourceTypeCode.photo, slot: 0, contentHash: h1),
                (role: ResourceTypeCode.video, slot: 0, contentHash: h2)
            ]
        )
        let links = [
            link(role: ResourceTypeCode.photo, hash: h1, fp: fp),
            link(role: ResourceTypeCode.video, hash: h2, fp: fp)
        ]
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fp,
            links: links,
            isResourceAvailable: { $0 == h1 } // only photo present
        )
        guard case .partiallyMissing(let missing) = state else {
            XCTFail("expected partiallyMissing, got \(state)")
            return
        }
        XCTAssertEqual(missing, [h2])
        XCTAssertFalse(state.allowsRestore,
                       "partial loss blocks restore; full-fingerprint post-save verify can't pass on a subset")
        XCTAssertFalse(state.allowsCleanup,
                       "partial loss may be transient; do not auto-tombstone")
    }

    /// Priority: fullyMissing wins over fingerprintMismatch — when nothing is
    /// physically present we can't act on the asset anyway, so the most useful
    /// diagnosis is "all gone, cleanup safe" rather than "tampering, blocked".
    func testFullyMissing_takesPriorityOverFingerprintMismatch() {
        let h = TestFixtures.fingerprint(0xFF)
        let storedFp = TestFixtures.fingerprint(0x99) // doesn't recompute to h's fp
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: storedFp,
            links: [link(role: ResourceTypeCode.photo, hash: h, fp: storedFp)],
            isResourceAvailable: { _ in false }
        )
        XCTAssertEqual(state, .fullyMissing,
                       "no resources available → cleanup is safe regardless of fp truth")
    }

    /// fp mismatch wins over partiallyMissing / metadataOnly — when resources DO
    /// exist, the link set's (role, slot) mapping can't be trusted; restore could
    /// reconstruct wrong content. Must surface the mismatch instead of "go ahead".
    func testFingerprintMismatch_winsWhenResourcesArePresent() {
        let h = TestFixtures.fingerprint(0xFE)
        let storedFp = TestFixtures.fingerprint(0x88) // doesn't recompute to h's fp
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: storedFp,
            links: [link(role: ResourceTypeCode.photo, hash: h, fp: storedFp)],
            isResourceAvailable: { _ in true } // resource present, so mismatch is actionable
        )
        guard case .fingerprintMismatch = state else {
            XCTFail("expected fingerprintMismatch when resources exist, got \(state)")
            return
        }
    }
}

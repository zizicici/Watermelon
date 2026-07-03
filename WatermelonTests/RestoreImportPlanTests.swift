import XCTest
@testable import Watermelon

// RestoreImportPlan guarantees PHAssetCreationRequest gets a VALID request for any resolvable subset: a primary
// of the right kind and no cross-kind or orphaned paired-clip adjuncts. Complete records pass through untouched
// (no restore-path regression); incomplete subsets are rebuilt into the minimal valid asset.
final class RestoreImportPlanTests: XCTestCase {
    private func inst(_ role: Int, _ hash: Data, slot: Int = 0) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(role: role, slot: slot, resourceHash: hash, fileName: "f\(role)", fileSize: 100, remoteRelativePath: "2024/01/f\(role)", creationDateMs: nil)
    }
    private func roles(_ out: [RemoteAssetResourceInstance]) -> [Int] { out.map(\.role) }

    // MARK: - Complete records pass through unchanged (the common path is untouched)

    func testCompletePhotoPassesThrough() {
        let ins = [inst(ResourceTypeCode.photo, Data([1])), inst(ResourceTypeCode.fullSizePhoto, Data([2])), inst(ResourceTypeCode.adjustmentData, Data([3]))]
        XCTAssertEqual(RestoreImportPlan.normalize(ins), ins)
    }

    func testCompleteVideoPassesThrough() {
        let ins = [inst(ResourceTypeCode.video, Data([1])), inst(ResourceTypeCode.fullSizeVideo, Data([2])), inst(ResourceTypeCode.adjustmentData, Data([3]))]
        XCTAssertEqual(RestoreImportPlan.normalize(ins), ins)
    }

    func testCompleteLivePhotoPassesThrough() {
        let ins = [inst(ResourceTypeCode.photo, Data([1])), inst(ResourceTypeCode.pairedVideo, Data([2])),
                   inst(ResourceTypeCode.fullSizePhoto, Data([3])), inst(ResourceTypeCode.fullSizePairedVideo, Data([4])),
                   inst(ResourceTypeCode.adjustmentData, Data([5]))]
        XCTAssertEqual(RestoreImportPlan.normalize(ins), ins)
    }

    func testCompleteVideoWithAudioKeepsAudio() {
        // Regression: a complete video with a separate .audio resource must pass through WITH its audio — else it
        // restores to a different fingerprint (a spurious new asset), which isn't the accepted incomplete case.
        let ins = [inst(ResourceTypeCode.video, Data([1])), inst(ResourceTypeCode.audio, Data([2]))]
        XCTAssertEqual(RestoreImportPlan.normalize(ins), ins)
    }

    func testCompleteRecordKeepsUnmodeledRole() {
        // Full fidelity for any role the planner doesn't model (a future PHAssetResourceType): the pass-through is
        // a denylist (drop only cross-kind / orphan-clip roles), so an unknown role is never silently dropped.
        let futureRole = 99
        let ins = [inst(ResourceTypeCode.video, Data([1])), inst(futureRole, Data([2]))]
        XCTAssertEqual(RestoreImportPlan.normalize(ins), ins)
    }

    func testCompleteLivePhotoWithAudioKeepsAudio() {
        // The Live branch rejects nothing, so a Live Photo carrying a separate audio resource keeps it (else it
        // would restore to a different fingerprint = a spurious new asset).
        let ins = [inst(ResourceTypeCode.photo, Data([1])), inst(ResourceTypeCode.pairedVideo, Data([2])), inst(ResourceTypeCode.audio, Data([3]))]
        XCTAssertEqual(RestoreImportPlan.normalize(ins), ins)
    }

    // MARK: - Invalid adjuncts are dropped (a request PhotoKit would reject)

    func testBareVideoDropsOrphanPairedClip() {
        // A .video primary must not carry a Live clip (that would be an invalid Live Photo request).
        let ins = [inst(ResourceTypeCode.video, Data([1])), inst(ResourceTypeCode.pairedVideo, Data([2]))]
        XCTAssertEqual(roles(RestoreImportPlan.normalize(ins)), [ResourceTypeCode.video], "the orphaned paired clip is dropped")
    }

    func testPhotoDropsDerivedPairedRoleWithoutCanonicalClip() {
        // A derived paired role (adjustment-base / full-size) without the canonical .pairedVideo isn't a real clip,
        // so the asset is a plain photo, not a broken Live Photo.
        let ins = [inst(ResourceTypeCode.photo, Data([1])), inst(ResourceTypeCode.adjustmentBasePairedVideo, Data([2]))]
        XCTAssertEqual(roles(RestoreImportPlan.normalize(ins)), [ResourceTypeCode.photo], "derived-only paired role dropped; stays a photo")
    }

    func testPhotoDropsCrossKindVideo() {
        let ins = [inst(ResourceTypeCode.photo, Data([1])), inst(ResourceTypeCode.fullSizeVideo, Data([2]))]
        XCTAssertEqual(roles(RestoreImportPlan.normalize(ins)), [ResourceTypeCode.photo], "a still doesn't carry a stray video")
    }

    func testPhotoDropsCanonicalCrossKindVideoRole() {
        // Same drop for the canonical .video role (2), not just .fullSizeVideo — a still can't carry any video-side role.
        let ins = [inst(ResourceTypeCode.photo, Data([1])), inst(ResourceTypeCode.video, Data([2]))]
        XCTAssertEqual(roles(RestoreImportPlan.normalize(ins)), [ResourceTypeCode.photo])
    }

    // MARK: - Incomplete subsets get a promoted primary (minimal valid asset)

    func testPairedVideoOnlyRestoresAsVideo() {
        // The reported case: a Live Photo that lost its still, leaving only the paired clip.
        let ins = [inst(ResourceTypeCode.pairedVideo, Data([3]))]
        let out = RestoreImportPlan.normalize(ins)
        XCTAssertEqual(roles(out), [ResourceTypeCode.video], "a lone Live clip restores as a standalone video")
        XCTAssertEqual(out.first?.resourceHash, Data([3]), "same file, promoted role")
        XCTAssertEqual(out.first?.slot, 0)
    }

    func testMultiplePairedOnlyKeepsSingleVideo() {
        let ins = [inst(ResourceTypeCode.pairedVideo, Data([1])), inst(ResourceTypeCode.fullSizePairedVideo, Data([2]))]
        let out = RestoreImportPlan.normalize(ins)
        XCTAssertEqual(roles(out), [ResourceTypeCode.video], "only the best clip becomes the .video primary; extras dropped")
        XCTAssertEqual(out.first?.resourceHash, Data([1]))
        XCTAssertEqual(out.first?.slot, 0)
    }

    func testPhotoMissingPrimaryPromotesFullSize() {
        let ins = [inst(ResourceTypeCode.fullSizePhoto, Data([2]), slot: 3), inst(ResourceTypeCode.alternatePhoto, Data([4]))]
        let out = RestoreImportPlan.normalize(ins)
        XCTAssertEqual(roles(out), [ResourceTypeCode.photo], "full-size promoted to the .photo primary; other side-resources dropped in the damaged case")
        XCTAssertEqual(out.first?.resourceHash, Data([2]))
        XCTAssertEqual(out.first?.slot, 0, "promoted primary is normalized to slot 0")
    }

    func testLiveMissingStillPromotesPhotoAndKeepsClip() {
        let ins = [inst(ResourceTypeCode.fullSizePhoto, Data([1]), slot: 2), inst(ResourceTypeCode.pairedVideo, Data([2]))]
        let out = RestoreImportPlan.normalize(ins)
        XCTAssertEqual(roles(out), [ResourceTypeCode.photo, ResourceTypeCode.pairedVideo], "promote the still, keep the clip → valid Live Photo")
        XCTAssertEqual(out.first?.slot, 0, "promoted still is normalized to slot 0")
    }

    func testVideoMissingPrimaryPromotesFullSize() {
        let ins = [inst(ResourceTypeCode.fullSizeVideo, Data([2]))]
        let out = RestoreImportPlan.normalize(ins)
        XCTAssertEqual(roles(out), [ResourceTypeCode.video])
        XCTAssertEqual(out.first?.slot, 0)
    }

    func testEmptyPassesThrough() {
        XCTAssertEqual(RestoreImportPlan.normalize([]), [])
    }
}

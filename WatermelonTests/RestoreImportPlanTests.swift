import Photos
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

    func testEditedVideoWithStillPreservesCompleteResourceSet() {
        for extraRoles in [[], [16]] {
            let resourceRoles = [ResourceTypeCode.video, ResourceTypeCode.fullSizePhoto,
                                 ResourceTypeCode.fullSizeVideo, ResourceTypeCode.adjustmentData] + extraRoles
            let instances = resourceRoles.map { inst($0, Data(repeating: UInt8($0), count: 32)) }
            XCTAssertTrue(PHAssetCreationRequest.supportsAssetResourceTypes(resourceRoles.map { NSNumber(value: $0) }))
            XCTAssertEqual(RestoreImportPlan.normalize(instances), instances)
            let reversed = Array(instances.reversed())
            XCTAssertEqual(RestoreImportPlan.normalize(reversed), reversed)
        }
    }

    func testEditedVideoWithStillKeepsDownloadedFilesAndFingerprint() {
        for extraRoles in [[], [16]] {
            let resourceRoles = [ResourceTypeCode.video, ResourceTypeCode.fullSizePhoto,
                                 ResourceTypeCode.fullSizeVideo, ResourceTypeCode.adjustmentData] + extraRoles
            let instances = resourceRoles.map { inst($0, Data(repeating: UInt8($0), count: 32)) }
            let downloaded = instances.map { ($0, URL(fileURLWithPath: "/tmp/restore-role-\($0.role)")) }
            let accepted = RestoreService.acceptedDownloadedResources(from: downloaded)

            XCTAssertEqual(accepted.map(\.0), instances)
            XCTAssertEqual(accepted.map(\.1), downloaded.map(\.1))
            let expectedFingerprint = BackupAssetResourcePlanner.assetFingerprint(
                resourceRoleSlotHashes: instances.map { (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash) }
            )
            let importedFingerprint = BackupAssetResourcePlanner.assetFingerprint(
                resourceRoleSlotHashes: accepted.map { (role: $0.0.role, slot: $0.0.slot, contentHash: $0.0.resourceHash) }
            )
            XCTAssertEqual(importedFingerprint, expectedFingerprint)
        }
    }

    func testCompleteLivePhotoPassesThrough() {
        let ins = [inst(ResourceTypeCode.photo, Data([1])), inst(ResourceTypeCode.pairedVideo, Data([2])),
                   inst(ResourceTypeCode.fullSizePhoto, Data([3])), inst(ResourceTypeCode.fullSizePairedVideo, Data([4])),
                   inst(ResourceTypeCode.adjustmentData, Data([5]))]
        XCTAssertEqual(RestoreImportPlan.normalize(ins), ins)
    }

    func testVideoWithAudioKeepsAudioOnlyWhenSupported() {
        let ins = [inst(ResourceTypeCode.video, Data([1])), inst(ResourceTypeCode.audio, Data([2]))]
        let supported = PHAssetCreationRequest.supportsAssetResourceTypes(ins.map { NSNumber(value: $0.role) })
        let result = RestoreImportPlan.normalize(ins)
        XCTAssertEqual(result, supported ? ins : ins.filter { $0.role != ResourceTypeCode.audio })
        XCTAssertTrue(PHAssetCreationRequest.supportsAssetResourceTypes(result.map { NSNumber(value: $0.role) }))
    }

    func testFutureRolePassesThroughWhenPhotoKitAcceptsIt() {
        let video = inst(ResourceTypeCode.video, Data([1]))
        let ins = [video, inst(99, Data([2]))]
        let supported = PHAssetCreationRequest.supportsAssetResourceTypes(ins.map { NSNumber(value: $0.role) })
        XCTAssertEqual(RestoreImportPlan.normalize(ins), supported ? ins : [video])
    }

    func testLivePhotoWithAudioKeepsAudioOnlyWhenSupported() {
        let ins = [inst(ResourceTypeCode.photo, Data([1])), inst(ResourceTypeCode.pairedVideo, Data([2])), inst(ResourceTypeCode.audio, Data([3]))]
        let supported = PHAssetCreationRequest.supportsAssetResourceTypes(ins.map { NSNumber(value: $0.role) })
        let result = RestoreImportPlan.normalize(ins)
        XCTAssertEqual(result, supported ? ins : ins.filter { $0.role != ResourceTypeCode.audio })
        XCTAssertTrue(PHAssetCreationRequest.supportsAssetResourceTypes(result.map { NSNumber(value: $0.role) }))
    }

    // MARK: - Invalid adjuncts are dropped (a request PhotoKit would reject)

    func testBareVideoDropsOrphanPairedClip() {
        // A .video primary must not carry a Live clip (that would be an invalid Live Photo request).
        let ins = [inst(ResourceTypeCode.video, Data([1])), inst(ResourceTypeCode.pairedVideo, Data([2]))]
        XCTAssertEqual(roles(RestoreImportPlan.normalize(ins)), [ResourceTypeCode.video], "the orphaned paired clip is dropped")
    }

    func testVideoPrimaryWithUnusableStillRemainsVideo() {
        let video = inst(ResourceTypeCode.video, Data([1]))
        let still = inst(ResourceTypeCode.fullSizePhoto, Data([2]))
        XCTAssertEqual(RestoreImportPlan.normalize([video, still]), [video])
    }

    func testVideoPrimaryWithStillAndOrphanClipRemainsVideo() {
        let video = inst(ResourceTypeCode.video, Data([1]))
        let ins = [inst(ResourceTypeCode.fullSizePhoto, Data([2])),
                   inst(ResourceTypeCode.pairedVideo, Data([3])), video]
        XCTAssertEqual(RestoreImportPlan.normalize(ins), [video])
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

    func testMissingOriginalVideoRecoversFullSizeVideoInsteadOfStill() {
        let instances = [inst(5, Data([5])), inst(6, Data([6])), inst(7, Data([7]))]
        let result = RestoreImportPlan.normalize(instances)
        XCTAssertEqual(result.map(\.role), [2])
        XCTAssertEqual(result.first?.resourceHash, Data([6]))
        XCTAssertEqual(result.first?.remoteRelativePath, instances[1].remoteRelativePath)
    }

    func testMissingAdjustmentRecoversSupportedPrimaryOnly() {
        for roles in [[2, 5, 6], [1, 5]] {
            let instances = roles.map { inst($0, Data([UInt8($0)])) }
            let result = RestoreImportPlan.normalize(instances)
            XCTAssertEqual(result, [instances[0]])
            XCTAssertTrue(PHAssetCreationRequest.supportsAssetResourceTypes(result.map { NSNumber(value: $0.role) }))
        }
    }

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

    // MARK: - Plan → file mapping keys on path, not the (possibly empty) hash

    func testLegacyNoHashMapsDistinctFilesByPath() {
        // A legacy manifest carries no resourceHash; the download loop still gives each resource its own file.
        // The plan→file map must key on remoteRelativePath — keying on the empty hash would collapse both
        // resources of a multi-resource asset onto the first URL (the reported half-fix).
        let photo = RemoteAssetResourceInstance(role: ResourceTypeCode.photo, slot: 0, resourceHash: Data(), fileName: "IMG.HEIC", fileSize: 100, remoteRelativePath: "2024/01/IMG.HEIC", creationDateMs: nil)
        let clip = RemoteAssetResourceInstance(role: ResourceTypeCode.pairedVideo, slot: 0, resourceHash: Data(), fileName: "IMG.MOV", fileSize: 200, remoteRelativePath: "2024/01/IMG.MOV", creationDateMs: nil)
        let photoURL = URL(fileURLWithPath: "/tmp/a.heic")
        let clipURL = URL(fileURLWithPath: "/tmp/b.mov")
        let accepted = RestoreService.acceptedDownloadedResources(from: [(photo, photoURL), (clip, clipURL)])
        XCTAssertEqual(accepted.count, 2, "photo + clip both survive")
        let urlForRole = Dictionary(uniqueKeysWithValues: accepted.map { ($0.0.role, $0.1) })
        XCTAssertEqual(urlForRole[ResourceTypeCode.photo], photoURL)
        XCTAssertEqual(urlForRole[ResourceTypeCode.pairedVideo], clipURL, "the clip maps to its OWN file, not the photo's (empty-hash collapse regression)")
    }
}

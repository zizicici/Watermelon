import XCTest
@testable import Watermelon

final class InboxTransferResourcePolicyTests: XCTestCase {
    func testFreeAccessAllowsThreeItemsAndRejectsFour() {
        let policy = InboxTransferAccessPolicy(isPro: false)
        XCTAssertTrue(policy.allows(itemCount: 3))
        XCTAssertFalse(policy.allows(itemCount: 4))
        XCTAssertNotNil(policy.buttonSubtitle)
    }

    func testProAccessHasNoItemLimitOrButtonSubtitle() {
        let policy = InboxTransferAccessPolicy(isPro: true)
        XCTAssertTrue(policy.allows(itemCount: 10_000))
        XCTAssertNil(policy.buttonSubtitle)
    }

    func testDefaultOptionsKeepLiveVideoAndOriginalEditedMedia() {
        let options = InboxTransferOptions.defaultOption
        XCTAssertTrue(options.includesLivePhotoVideo)
        XCTAssertTrue(options.usesOriginalEditedPhoto)
        XCTAssertTrue(options.usesOriginalEditedVideo)
        XCTAssertFalse(options.removesLocationMetadata)
    }

    func testBinarySettingsUpdateOnlyTheirTransferOption() throws {
        let savedOptions = InboxTransferOptions.storedValue
        defer { InboxTransferOptions.store(savedOptions) }

        InboxTransferOptions.store(.defaultOption)
        try InboxTransferOriginalPhotoSetting.setCurrent(.disable)
        var options = InboxTransferOptions.storedValue
        XCTAssertTrue(options.includesLivePhotoVideo)
        XCTAssertFalse(options.usesOriginalEditedPhoto)
        XCTAssertTrue(options.usesOriginalEditedVideo)
        XCTAssertFalse(options.removesLocationMetadata)

        try InboxTransferRemoveLocationSetting.setCurrent(.enable)
        options = InboxTransferOptions.storedValue
        XCTAssertTrue(options.includesLivePhotoVideo)
        XCTAssertFalse(options.usesOriginalEditedPhoto)
        XCTAssertTrue(options.usesOriginalEditedVideo)
        XCTAssertTrue(options.removesLocationMetadata)
    }

    func testLivePhotoDefaultsSelectOriginalStillAndPairedVideo() {
        let candidates = makeCandidates([
            ResourceTypeCode.photo,
            ResourceTypeCode.alternatePhoto,
            ResourceTypeCode.fullSizePhoto,
            ResourceTypeCode.pairedVideo,
            ResourceTypeCode.fullSizePairedVideo,
            ResourceTypeCode.adjustmentData,
        ])
        XCTAssertEqual(
            selectedRoles(candidates, kind: .image(isLivePhoto: true), options: .defaultOption),
            [ResourceTypeCode.photo, ResourceTypeCode.alternatePhoto, ResourceTypeCode.pairedVideo]
        )
    }

    func testLivePhotoCanExcludePairedVideo() {
        let options = InboxTransferOptions.defaultOption.updating(
            .includeLivePhotoVideo,
            isEnabled: false
        )
        let candidates = makeCandidates([
            ResourceTypeCode.photo,
            ResourceTypeCode.pairedVideo,
        ])
        XCTAssertEqual(
            selectedRoles(candidates, kind: .image(isLivePhoto: true), options: options),
            [ResourceTypeCode.photo]
        )
    }

    func testEditedLivePhotoCanSelectRenderedStillAndMotionVideo() {
        var options = InboxTransferOptions.defaultOption.updating(
            .useOriginalEditedPhoto,
            isEnabled: false
        )
        options = options.updating(.useOriginalEditedVideo, isEnabled: false)
        let candidates = makeCandidates([
            ResourceTypeCode.photo,
            ResourceTypeCode.fullSizePhoto,
            ResourceTypeCode.pairedVideo,
            ResourceTypeCode.fullSizePairedVideo,
        ])
        XCTAssertEqual(
            selectedRoles(candidates, kind: .image(isLivePhoto: true), options: options),
            [ResourceTypeCode.fullSizePhoto, ResourceTypeCode.fullSizePairedVideo]
        )
    }

    func testEditedVideoSelectionSwitchesBetweenOriginalAndRenderedResource() {
        let candidates = makeCandidates([
            ResourceTypeCode.video,
            ResourceTypeCode.fullSizeVideo,
            ResourceTypeCode.adjustmentData,
        ])
        XCTAssertEqual(
            selectedRoles(candidates, kind: .video, options: .defaultOption),
            [ResourceTypeCode.video]
        )
        let rendered = InboxTransferOptions.defaultOption.updating(
            .useOriginalEditedVideo,
            isEnabled: false
        )
        XCTAssertEqual(
            selectedRoles(candidates, kind: .video, options: rendered),
            [ResourceTypeCode.fullSizeVideo]
        )
    }

    private func makeCandidates(_ roles: [Int]) -> [InboxTransferResourceCandidate] {
        roles.enumerated().map {
            InboxTransferResourceCandidate(identifier: $0.offset, role: $0.element)
        }
    }

    private func selectedRoles(
        _ candidates: [InboxTransferResourceCandidate],
        kind: InboxTransferAssetKind,
        options: InboxTransferOptions
    ) -> [Int] {
        let selected = Set(InboxTransferResourcePolicy.select(
            candidates,
            kind: kind,
            options: options
        ))
        return candidates.compactMap { selected.contains($0.identifier) ? $0.role : nil }
    }
}

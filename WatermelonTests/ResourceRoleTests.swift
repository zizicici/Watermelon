import XCTest
@testable import Watermelon

// The single role model. The key invariant: any role the classifier counts as a "photo side" is also in the
// photo-side picker priority — so a Live Photo classified via one can always resolve its still via the other
// (the adjustmentBasePhoto drift). Same for the video side.
final class ResourceRoleTests: XCTestCase {
    func testClassifierAndPickerAgreeOnEveryPhotoSideRole() {
        for role in ResourceRole.photoSidePriority {
            XCTAssertTrue(ResourceRole.isPhotoSide(role), "photoSidePriority role \(role) must count as photo-side")
        }
        // Every role the classifier treats as photo-side must be pickable.
        for role in [ResourceTypeCode.photo, ResourceTypeCode.alternatePhoto, ResourceTypeCode.fullSizePhoto,
                     ResourceTypeCode.adjustmentBasePhoto, ResourceTypeCode.photoProxy] {
            XCTAssertTrue(ResourceRole.isPhotoSide(role))
            XCTAssertTrue(ResourceRole.photoSidePriority.contains(role), "classifier photo-side role \(role) must be in the picker")
        }
    }

    func testAdjustmentBasePhotoLiveResolvesAStill() {
        // The reported regression: adjustmentBasePhoto + pairedVideo was classified Live but had no photo path.
        let roles = [ResourceTypeCode.adjustmentBasePhoto, ResourceTypeCode.pairedVideo]
        let (isLive, isVideo) = ResourceRole.classify(roles: roles)
        XCTAssertTrue(isLive)
        XCTAssertFalse(isVideo)
        XCTAssertNotNil(ResourceRole.photoSidePriority.first { roles.contains($0) }, "Live still must be resolvable")
    }

    func testClassifyKinds() {
        assertClassify([ResourceTypeCode.photo], live: false, video: false)
        assertClassify([ResourceTypeCode.video], live: false, video: true)
        assertClassify([ResourceTypeCode.photo, ResourceTypeCode.pairedVideo], live: true, video: false)
        // Paired video with no photo side is a video, not a Live Photo.
        assertClassify([ResourceTypeCode.pairedVideo], live: false, video: true)
        assertClassify([ResourceTypeCode.fullSizePhoto], live: false, video: false)
    }

    func testPredicatesMatchLegacySets() {
        XCTAssertEqual(Set(ResourceRole.photoSidePriority), [1, 4, 5, 8, 19])
        XCTAssertEqual(Set(ResourceRole.videoSidePriority), [2, 6, 9, 10, 11, 12])
        XCTAssertEqual(Set(ResourceRole.pairedVideoRoles), [9, 10, 11])
    }

    private func assertClassify(_ roles: [Int], live: Bool, video: Bool, file: StaticString = #filePath, line: UInt = #line) {
        let result = ResourceRole.classify(roles: roles)
        XCTAssertEqual(result.isLivePhoto, live, "isLivePhoto for \(roles)", file: file, line: line)
        XCTAssertEqual(result.isVideo, video, "isVideo for \(roles)", file: file, line: line)
    }
}

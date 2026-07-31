import XCTest
@testable import WatermelonMac

final class MacRemoteThumbnailFetchPolicyTests: XCTestCase {
    func testFetchesOnlyUncachedRemoteItemInCurrentSession() {
        XCTAssertTrue(
            MacRemoteThumbnailFetchPolicy.shouldFetch(
                hasLocalAsset: false,
                hasFingerprint: true,
                hasCachedData: false,
                sessionIsCurrent: true
            )
        )
    }

    func testDoesNotFetchLocalItem() {
        XCTAssertFalse(
            MacRemoteThumbnailFetchPolicy.shouldFetch(
                hasLocalAsset: true,
                hasFingerprint: true,
                hasCachedData: false,
                sessionIsCurrent: true
            )
        )
    }

    func testDoesNotFetchCachedOrStaleRemoteItem() {
        XCTAssertFalse(
            MacRemoteThumbnailFetchPolicy.shouldFetch(
                hasLocalAsset: false,
                hasFingerprint: true,
                hasCachedData: true,
                sessionIsCurrent: true
            )
        )
        XCTAssertFalse(
            MacRemoteThumbnailFetchPolicy.shouldFetch(
                hasLocalAsset: false,
                hasFingerprint: true,
                hasCachedData: false,
                sessionIsCurrent: false
            )
        )
    }
}

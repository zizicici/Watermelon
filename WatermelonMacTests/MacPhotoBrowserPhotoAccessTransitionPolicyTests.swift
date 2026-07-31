import XCTest
@testable import WatermelonMac

final class MacPhotoBrowserPhotoAccessTransitionPolicyTests:
    XCTestCase
{
    func testOnlyReadableSemanticsInvalidateLocalProjection() {
        XCTAssertFalse(invalidates(.authorized, .authorized))
        XCTAssertTrue(invalidates(.authorized, .limited))
        XCTAssertTrue(invalidates(.limited, .authorized))
        XCTAssertTrue(invalidates(.authorized, .denied))
        XCTAssertTrue(invalidates(.denied, .authorized))
        XCTAssertFalse(invalidates(.denied, .restricted))
        XCTAssertFalse(invalidates(.notDetermined, .denied))
    }

    private func invalidates(
        _ previous: PhotoLibraryAccessState,
        _ current: PhotoLibraryAccessState
    ) -> Bool {
        MacPhotoBrowserPhotoAccessTransitionPolicy
            .invalidatesLocalProjection(
                previous: previous,
                current: current
            )
    }
}

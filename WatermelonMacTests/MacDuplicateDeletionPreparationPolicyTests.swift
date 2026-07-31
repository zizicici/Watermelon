import XCTest
@testable import WatermelonMac

final class MacPhotoLibraryDeletionPreparationPolicyTests:
    XCTestCase
{
    func testCancellationWinsBeforePhotoLibraryMutation() {
        XCTAssertEqual(
            MacPhotoLibraryDeletionPreparationPolicy.disposition(
                isCancelled: true,
                isStillValid: true
            ),
            .cancel
        )
        XCTAssertEqual(
            MacPhotoLibraryDeletionPreparationPolicy.disposition(
                isCancelled: true,
                isStillValid: false
            ),
            .cancel
        )
    }

    func testCurrentSelectionCanProceed() {
        XCTAssertEqual(
            MacPhotoLibraryDeletionPreparationPolicy.disposition(
                isCancelled: false,
                isStillValid: true
            ),
            .proceed
        )
    }

    func testChangedSelectionIsRejected() {
        XCTAssertEqual(
            MacPhotoLibraryDeletionPreparationPolicy.disposition(
                isCancelled: false,
                isStillValid: false
            ),
            .stale
        )
    }

    func testCommitGateRejectsCancelledTask() {
        XCTAssertThrowsError(
            try MacPhotoLibraryDeletionPreparationPolicy
                .ensureCommitAllowed(isCancelled: true)
        ) {
            XCTAssertTrue($0 is CancellationError)
        }
        XCTAssertNoThrow(
            try MacPhotoLibraryDeletionPreparationPolicy
                .ensureCommitAllowed(isCancelled: false)
        )
    }
}

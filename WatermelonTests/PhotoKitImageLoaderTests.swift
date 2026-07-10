import CoreImage
import Photos
import XCTest
@testable import Watermelon

final class PhotoKitImageLoaderTests: XCTestCase {
    func testAcceptsValidPhotoKitImage() {
        XCTAssertNotNil(PhotoKitImageLoader.acceptedImage(
            image(),
            info: nil,
            networkAccessAllowed: false
        ))
    }

    func testRejectsCancelledErrorAndDegradedResults() {
        let source = image()
        XCTAssertNil(PhotoKitImageLoader.acceptedImage(
            source,
            info: [PHImageCancelledKey: true],
            networkAccessAllowed: false
        ))
        XCTAssertNil(PhotoKitImageLoader.acceptedImage(
            source,
            info: [PHImageErrorKey: NSError(domain: "test", code: 1)],
            networkAccessAllowed: false
        ))
        XCTAssertNil(PhotoKitImageLoader.acceptedImage(
            source,
            info: [PHImageResultIsDegradedKey: true],
            networkAccessAllowed: false
        ))
    }

    func testRejectsCloudOnlyResultWhenNetworkIsDisabled() {
        let source = image()
        XCTAssertNil(PhotoKitImageLoader.acceptedImage(
            source,
            info: [PHImageResultIsInCloudKey: true],
            networkAccessAllowed: false
        ))
        XCTAssertNotNil(PhotoKitImageLoader.acceptedImage(
            source,
            info: [PHImageResultIsInCloudKey: true],
            networkAccessAllowed: true
        ))
    }

    func testAcceptsImageWithCIImageBacking() {
        let ciImage = CIImage(color: CIColor(red: 1, green: 0, blue: 0))
            .cropped(to: CGRect(x: 0, y: 0, width: 20, height: 20))
        let source = UIImage(ciImage: ciImage)

        XCTAssertNotNil(PhotoKitImageLoader.acceptedImage(
            source,
            info: nil,
            networkAccessAllowed: false
        ))
    }

    func testCancellationCompletesContinuationWithoutPhotoKitCallback() async {
        let state = PhotoKitRequestState<UIImage>(imageManager: PHImageManager())
        let result: UIImage? = await withCheckedContinuation { continuation in
            XCTAssertTrue(state.bind(continuation))
            state.cancel()
        }

        XCTAssertNil(result)
    }

    func testTimeoutCompletesContinuationWithoutPhotoKitCallback() async {
        let state = PhotoKitRequestState<UIImage>(imageManager: PHImageManager())
        let result: UIImage? = await withCheckedContinuation { continuation in
            XCTAssertTrue(state.bind(continuation))
            state.scheduleTimeout(nanoseconds: 1_000_000)
        }

        XCTAssertNil(result)
    }

    func testCancellationInvokesProvidedRequestCanceller() async {
        let recorder = RequestCancellationRecorder()
        let state = PhotoKitRequestState<UIImage>(cancelRequest: { recorder.record($0) })
        let result: UIImage? = await withCheckedContinuation { continuation in
            XCTAssertTrue(state.bind(continuation))
            state.attach(42)
            state.cancel()
        }

        XCTAssertNil(result)
        XCTAssertEqual(recorder.requestIDs, [42])
    }

    func testBackupSidecarUsesLongTimeoutWithoutNetworkAccess() {
        XCTAssertEqual(
            PhotoKitImageLoader.timeoutNanoseconds(
                networkAccessAllowed: false,
                policy: .interactive
            ),
            15 * 1_000_000_000
        )
        XCTAssertEqual(
            PhotoKitImageLoader.timeoutNanoseconds(
                networkAccessAllowed: false,
                policy: .backupSidecar
            ),
            180 * 1_000_000_000
        )
    }

    private func image() -> UIImage {
        UIGraphicsImageRenderer(size: CGSize(width: 20, height: 20)).image { context in
            UIColor.red.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 20, height: 20))
        }
    }

    private final class RequestCancellationRecorder: @unchecked Sendable {
        private let lock = NSLock()
        private var recordedRequestIDs: [PHImageRequestID] = []

        var requestIDs: [PHImageRequestID] {
            lock.withLock { recordedRequestIDs }
        }

        func record(_ requestID: PHImageRequestID) {
            lock.withLock { recordedRequestIDs.append(requestID) }
        }
    }
}

import Foundation
import Photos
import UIKit

// Renders one small JPEG thumbnail from a PHAsset (photo, video poster frame, or Live Photo still)
// via a single PHImageManager request. Never throws — any failure returns nil so callers stay
// best-effort. `.highQualityFormat` guarantees a single result callback, so there is no degraded
// pre-pass to wait on and the continuation always resumes exactly once.
struct ThumbnailRenderer: Sendable {
    static let defaultMaxPixel = 512

    func renderThumbnailJPEG(
        for asset: PHAsset,
        maxPixel: Int = ThumbnailRenderer.defaultMaxPixel,
        allowNetworkAccess: Bool,
        compressionQuality: CGFloat = 0.8
    ) async -> Data? {
        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = allowNetworkAccess
        options.isSynchronous = false

        let image: UIImage? = await withCheckedContinuation { continuation in
            let resumed = ResumeGuard()
            PHImageManager.default().requestImage(
                for: asset,
                targetSize: CGSize(width: maxPixel, height: maxPixel),
                contentMode: .aspectFill,
                options: options
            ) { image, _ in
                if resumed.tryResume() {
                    continuation.resume(returning: image)
                }
            }
        }

        guard let image else { return nil }
        return image.jpegData(compressionQuality: compressionQuality)
    }

    private final class ResumeGuard: @unchecked Sendable {
        private let lock = NSLock()
        private var done = false
        func tryResume() -> Bool {
            lock.withLock {
                if done { return false }
                done = true
                return true
            }
        }
    }
}

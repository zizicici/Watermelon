import Foundation
import Photos
import UIKit

// Renders one small JPEG thumbnail from a PHAsset (photo, video poster frame, or Live Photo still)
// via a single PHImageManager request. Never throws — any failure returns nil so callers stay
// best-effort. `.highQualityFormat` guarantees a single result callback, so there is no degraded
// pre-pass to wait on and the continuation always resumes exactly once.
struct ThumbnailRenderer: Sendable {
    static let defaultMaxPixel = ThumbnailSizing.maximumLongSide

    func renderThumbnailJPEG(
        for asset: PHAsset,
        maxPixel: Int = ThumbnailRenderer.defaultMaxPixel,
        allowNetworkAccess: Bool,
        compressionQuality: CGFloat = ThumbnailSizing.jpegCompressionQuality
    ) async -> Data? {
        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.isNetworkAccessAllowed = allowNetworkAccess
        options.isSynchronous = false

        guard let targetLongSide = ThumbnailSizing.targetLongSide(
            originalWidth: asset.pixelWidth,
            originalHeight: asset.pixelHeight,
            cap: maxPixel
        ) else { return nil }

        let image: UIImage? = await withCheckedContinuation { continuation in
            let resumed = ResumeGuard()
            PHImageManager.default().requestImage(
                for: asset,
                targetSize: CGSize(width: targetLongSide, height: targetLongSide),
                contentMode: .aspectFit,
                options: options
            ) { image, _ in
                if resumed.tryResume() {
                    continuation.resume(returning: image)
                }
            }
        }

        guard let image,
              let fitted = ThumbnailSizing.fittedImage(image, maximumLongSide: targetLongSide) else { return nil }
        return fitted.jpegData(compressionQuality: compressionQuality)
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

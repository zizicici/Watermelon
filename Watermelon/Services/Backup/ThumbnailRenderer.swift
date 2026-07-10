import Foundation
import Photos
import UIKit

// Encodes the app's canonical PhotoKit thumbnail as a best-effort JPEG sidecar.
struct ThumbnailRenderer: Sendable {
    func renderThumbnailJPEG(
        for asset: PHAsset,
        allowNetworkAccess: Bool,
        compressionQuality: CGFloat = ThumbnailSizing.jpegCompressionQuality
    ) async -> Data? {
        guard let image = await PhotoKitImageLoader.thumbnail(
            for: asset,
            allowNetworkAccess: allowNetworkAccess,
            timeoutPolicy: .backupSidecar
        ) else { return nil }
        return ThumbnailSizing.jpegData(from: image, compressionQuality: compressionQuality)
    }
}

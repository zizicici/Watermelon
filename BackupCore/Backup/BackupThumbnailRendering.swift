import Foundation
@preconcurrency import Photos

protocol BackupThumbnailRendering: Sendable {
    func renderThumbnailJPEG(
        for asset: PHAsset,
        allowNetworkAccess: Bool
    ) async -> Data?
}

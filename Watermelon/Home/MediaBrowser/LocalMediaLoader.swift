import Photos
import UIKit

// Loads media directly from on-device PHAssets (the user's own library; network is allowed for full
// media so iCloud-optimized originals can be fetched). Shared by LocalMediaSource and the merged
// source's local-first path.
enum LocalMediaLoader {
    static func thumbnail(localIdentifier: String) async -> UIImage? {
        await PhotoKitImageLoader.thumbnail(localIdentifier: localIdentifier)
    }

    static func photoImage(localIdentifier: String, maxPixel: Int) async -> UIImage? {
        guard let asset = fetchAsset(localIdentifier) else { return nil }
        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.isNetworkAccessAllowed = true
        options.isSynchronous = false
        options.version = .current
        return await PhotoKitImageLoader.requestImage(
            for: asset,
            targetSize: CGSize(width: maxPixel, height: maxPixel),
            contentMode: .aspectFit,
            options: options
        )
    }

    static func video(localIdentifier: String) async -> MaterializedVideo? {
        guard let asset = fetchAsset(localIdentifier) else { return nil }
        let options = PHVideoRequestOptions()
        options.isNetworkAccessAllowed = true
        options.deliveryMode = .highQualityFormat
        options.version = .current
        let avAsset = await PhotoKitImageLoader.requestVideoAsset(for: asset, options: options)
        // Only a URL-backed (non-composited) asset yields a directly playable file URL.
        guard let urlAsset = avAsset as? AVURLAsset else { return nil }
        return MaterializedVideo(url: urlAsset.url, isTemporary: false)
    }

    static func livePhoto(localIdentifier: String, targetSize: CGSize) async -> PHLivePhoto? {
        guard let asset = fetchAsset(localIdentifier) else { return nil }
        let options = PHLivePhotoRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.isNetworkAccessAllowed = true
        return await PhotoKitImageLoader.requestLivePhoto(
            for: asset,
            targetSize: targetSize,
            contentMode: .aspectFit,
            options: options
        )
    }

    private static func fetchAsset(_ localIdentifier: String) -> PHAsset? {
        PHAsset.fetchAssets(withLocalIdentifiers: [localIdentifier], options: nil).firstObject
    }
}

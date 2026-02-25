import Foundation
import Photos
import UIKit

final class PhotoLibraryService {
    private let imageManager = PHCachingImageManager()
    private let resourceManager = PHAssetResourceManager.default()

    func requestAuthorization() async -> PHAuthorizationStatus {
        await withCheckedContinuation { continuation in
            PHPhotoLibrary.requestAuthorization(for: .readWrite) { status in
                continuation.resume(returning: status)
            }
        }
    }

    func authorizationStatus() -> PHAuthorizationStatus {
        PHPhotoLibrary.authorizationStatus(for: .readWrite)
    }

    func fetchAssetsResult(ascendingByCreationDate: Bool = false) -> PHFetchResult<PHAsset> {
        let options = PHFetchOptions()
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: ascendingByCreationDate)]
        return PHAsset.fetchAssets(with: options)
    }

    func fetchAssets() -> [PHAsset] {
        let fetchResult = fetchAssetsResult()
        var result: [PHAsset] = []
        result.reserveCapacity(fetchResult.count)
        fetchResult.enumerateObjects { asset, _, _ in
            result.append(asset)
        }
        return result
    }

    func exportResourceToTempFile(_ resource: PHAssetResource) async throws -> URL {
        let ext = (resource.originalFilename as NSString).pathExtension
        let temp = FileManager.default.temporaryDirectory
        let filename = UUID().uuidString + (ext.isEmpty ? "" : ".\(ext)")
        let url = temp.appendingPathComponent(filename)
        try? FileManager.default.removeItem(at: url)

        let options = PHAssetResourceRequestOptions()
        options.isNetworkAccessAllowed = true

        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            resourceManager.writeData(for: resource, toFile: url, options: options) { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: ())
                }
            }
        }

        return url
    }

    func requestThumbnail(for asset: PHAsset, targetSize: CGSize, completion: @escaping (UIImage?) -> Void) {
        let options = PHImageRequestOptions()
        options.deliveryMode = .fastFormat
        options.resizeMode = .fast
        options.isNetworkAccessAllowed = true

        imageManager.requestImage(
            for: asset,
            targetSize: targetSize,
            contentMode: .aspectFill,
            options: options
        ) { image, _ in
            completion(image)
        }
    }

    static func mediaTypeName(for asset: PHAsset) -> String {
        switch asset.mediaType {
        case .image:
            return "image"
        case .video:
            return "video"
        case .audio:
            return "audio"
        default:
            return "unknown"
        }
    }

    static func isLivePhoto(_ asset: PHAsset) -> Bool {
        asset.mediaSubtypes.contains(.photoLive)
    }

    static func locationJSON(for asset: PHAsset) -> String? {
        guard let location = asset.location else { return nil }
        let dict: [String: Double] = [
            "latitude": location.coordinate.latitude,
            "longitude": location.coordinate.longitude,
            "altitude": location.altitude
        ]
        guard let data = try? JSONSerialization.data(withJSONObject: dict, options: []),
              let string = String(data: data, encoding: .utf8) else {
            return nil
        }
        return string
    }

    static func resourceFileSize(_ resource: PHAssetResource) -> Int64 {
        if let size = resource.value(forKey: "fileSize") as? CLong {
            return Int64(size)
        }
        if let size = resource.value(forKey: "fileSize") as? Int64 {
            return size
        }
        return 0
    }

    static func resourceTypeName(_ type: PHAssetResourceType) -> String {
        switch type {
        case .photo:
            return "photo"
        case .video:
            return "video"
        case .audio:
            return "audio"
        case .alternatePhoto:
            return "alternatePhoto"
        case .fullSizePhoto:
            return "fullSizePhoto"
        case .fullSizeVideo:
            return "fullSizeVideo"
        case .pairedVideo:
            return "pairedVideo"
        case .adjustmentData:
            return "adjustmentData"
        case .adjustmentBasePhoto:
            return "adjustmentBasePhoto"
        case .photoProxy:
            return "photoProxy"
        default:
            return "other_\(type.rawValue)"
        }
    }
}

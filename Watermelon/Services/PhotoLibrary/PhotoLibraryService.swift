import Foundation
import Photos
import UIKit

final class PhotoLibraryService: @unchecked Sendable {
    private let imageManager = PHCachingImageManager()
    private let resourceManager = PHAssetResourceManager.default()

    private final class ExportRequestState {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Void, Error>?
        private var requestID: PHAssetResourceDataRequestID?
        private var completed = false

        func bind(continuation: CheckedContinuation<Void, Error>, requestID: PHAssetResourceDataRequestID) -> Bool {
            lock.lock()
            defer { lock.unlock() }
            guard !completed else {
                return false
            }
            self.continuation = continuation
            self.requestID = requestID
            return true
        }

        func complete(_ result: Result<Void, Error>) {
            let continuation: CheckedContinuation<Void, Error>?
            lock.lock()
            guard !completed else {
                lock.unlock()
                return
            }
            completed = true
            continuation = self.continuation
            self.continuation = nil
            requestID = nil
            lock.unlock()

            guard let continuation else { return }
            switch result {
            case .success:
                continuation.resume(returning: ())
            case .failure(let error):
                continuation.resume(throwing: error)
            }
        }

        func cancelRequest(using manager: PHAssetResourceManager) {
            let requestID: PHAssetResourceDataRequestID?
            lock.lock()
            requestID = self.requestID
            lock.unlock()

            if let requestID {
                manager.cancelDataRequest(requestID)
            }
        }

        func cancel(using manager: PHAssetResourceManager) {
            cancelRequest(using: manager)
            complete(.failure(CancellationError()))
        }
    }

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

    func exportResourceToTempFile(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController? = nil
    ) async throws -> URL {
        let ext = (resource.originalFilename as NSString).pathExtension
        let temp = FileManager.default.temporaryDirectory
        let filename = UUID().uuidString + (ext.isEmpty ? "" : ".\(ext)")
        let url = temp.appendingPathComponent(filename)
        try? FileManager.default.removeItem(at: url)
        guard FileManager.default.createFile(atPath: url.path, contents: nil) else {
            throw NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileWriteUnknownError,
                userInfo: [NSLocalizedDescriptionKey: "Failed to create temporary export file."]
            )
        }

        let fileHandle = try FileHandle(forWritingTo: url)
        defer {
            try? fileHandle.close()
        }

        let options = PHAssetResourceRequestOptions()
        options.isNetworkAccessAllowed = true

        let resourceManager = self.resourceManager
        let state = ExportRequestState()
        let cancellationHandlerID = cancellationController?.addCancellationHandler {
            state.cancel(using: resourceManager)
        }
        defer {
            if let cancellationHandlerID {
                cancellationController?.removeCancellationHandler(cancellationHandlerID)
            }
        }
        try cancellationController?.throwIfCancelled()

        do {
            try await withTaskCancellationHandler(operation: {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    let requestID = resourceManager.requestData(
                        for: resource,
                        options: options,
                        dataReceivedHandler: { data in
                            autoreleasepool {
                                guard !data.isEmpty else { return }
                                do {
                                    try fileHandle.write(contentsOf: data)
                                } catch {
                                    state.cancelRequest(using: resourceManager)
                                    state.complete(.failure(error))
                                }
                            }
                        },
                        completionHandler: { error in
                            if let error {
                                state.complete(.failure(error))
                            } else {
                                state.complete(.success(()))
                            }
                        }
                    )

                    if !state.bind(continuation: continuation, requestID: requestID) {
                        continuation.resume(throwing: CancellationError())
                        return
                    }

                    if Task.isCancelled {
                        state.cancel(using: resourceManager)
                    }
                }
            }, onCancel: {
                state.cancel(using: resourceManager)
            })
        } catch {
            try? FileManager.default.removeItem(at: url)
            throw error
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

    static func resourceTypeCode(_ type: PHAssetResourceType) -> Int {
        switch type {
        case .photo:
            return ResourceTypeCode.photo
        case .video:
            return ResourceTypeCode.video
        case .audio:
            return ResourceTypeCode.audio
        case .alternatePhoto:
            return ResourceTypeCode.alternatePhoto
        case .fullSizePhoto:
            return ResourceTypeCode.fullSizePhoto
        case .fullSizeVideo:
            return ResourceTypeCode.fullSizeVideo
        case .pairedVideo:
            return ResourceTypeCode.pairedVideo
        case .adjustmentData:
            return ResourceTypeCode.adjustmentData
        case .adjustmentBasePhoto:
            return ResourceTypeCode.adjustmentBasePhoto
        case .photoProxy:
            return ResourceTypeCode.photoProxy
        default:
            return ResourceTypeCode.unknown
        }
    }

    static func resourceTypeName(from code: Int) -> String {
        switch code {
        case ResourceTypeCode.photo:
            return "photo"
        case ResourceTypeCode.video:
            return "video"
        case ResourceTypeCode.audio:
            return "audio"
        case ResourceTypeCode.alternatePhoto:
            return "alternatePhoto"
        case ResourceTypeCode.fullSizePhoto:
            return "fullSizePhoto"
        case ResourceTypeCode.fullSizeVideo:
            return "fullSizeVideo"
        case ResourceTypeCode.pairedVideo:
            return "pairedVideo"
        case ResourceTypeCode.adjustmentData:
            return "adjustmentData"
        case ResourceTypeCode.adjustmentBasePhoto:
            return "adjustmentBasePhoto"
        case ResourceTypeCode.photoProxy:
            return "photoProxy"
        default:
            return "unknown"
        }
    }
}

extension PhotoLibraryService: PhotoLibraryServiceProtocol {}

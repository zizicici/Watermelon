import AVFoundation
import Photos
import UIKit
import os.log

enum PhotoKitImageLoader {
    enum ThumbnailTimeoutPolicy: Sendable {
        case interactive
        case backupSidecar
    }

    private static let log = Logger(subsystem: "com.zizicici.watermelon", category: "PhotoKitImageLoader")
    private static let localRequestTimeoutNanoseconds: UInt64 = 15 * 1_000_000_000
    private static let networkRequestTimeoutNanoseconds: UInt64 = 180 * 1_000_000_000
    private static let localThumbnailManager = ImageManagerBox(PHCachingImageManager())

    private final class ImageManagerBox: @unchecked Sendable {
        let value: PHImageManager

        init(_ value: PHImageManager) {
            self.value = value
        }
    }

    static func thumbnail(localIdentifier: String) async -> UIImage? {
        guard let asset = PHAsset.fetchAssets(
            withLocalIdentifiers: [localIdentifier],
            options: nil
        ).firstObject else { return nil }
        return await thumbnail(
            for: asset,
            allowNetworkAccess: false,
            imageManager: localThumbnailManager.value
        )
    }

    static func thumbnail(
        for asset: PHAsset,
        allowNetworkAccess: Bool,
        timeoutPolicy: ThumbnailTimeoutPolicy = .interactive,
        imageManager: PHImageManager = .default()
    ) async -> UIImage? {
        guard let targetLongSide = ThumbnailSizing.targetLongSide(
            originalWidth: asset.pixelWidth,
            originalHeight: asset.pixelHeight
        ) else { return nil }

        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.isNetworkAccessAllowed = allowNetworkAccess
        options.isSynchronous = false
        guard let image = await requestImage(
            for: asset,
            targetSize: CGSize(width: targetLongSide, height: targetLongSide),
            contentMode: .aspectFit,
            options: options,
            imageManager: imageManager,
            requestTimeoutNanoseconds: timeoutNanoseconds(
                networkAccessAllowed: allowNetworkAccess,
                policy: timeoutPolicy
            )
        ) else { return nil }
        return ThumbnailSizing.fittedImage(image, maximumLongSide: targetLongSide)
    }

    static func requestImage(
        for asset: PHAsset,
        targetSize: CGSize,
        contentMode: PHImageContentMode,
        options: PHImageRequestOptions,
        imageManager: PHImageManager = .default(),
        requestTimeoutNanoseconds: UInt64? = nil
    ) async -> UIImage? {
        let state = PhotoKitRequestState<UIImage>(imageManager: imageManager)
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                guard !Task.isCancelled else {
                    state.cancel()
                    return
                }

                let requestID = imageManager.requestImage(
                    for: asset,
                    targetSize: targetSize,
                    contentMode: contentMode,
                    options: options
                ) { image, info in
                    if let error = info?[PHImageErrorKey] as? Error {
                        log.error(
                            "PhotoKit image failed: asset=\(asset.localIdentifier, privacy: .private(mask: .hash)), type=\(asset.mediaType.rawValue, privacy: .public), pixels=\(asset.pixelWidth, privacy: .public)x\(asset.pixelHeight, privacy: .public), error=\(error.localizedDescription, privacy: .public)"
                        )
                    }
                    if flag(PHImageCancelledKey, in: info) || info?[PHImageErrorKey] != nil {
                        state.complete(nil)
                        return
                    }
                    if flag(PHImageResultIsDegradedKey, in: info) { return }
                    let accepted = acceptedImage(
                        image,
                        info: info,
                        networkAccessAllowed: options.isNetworkAccessAllowed
                    )
                    if image != nil, accepted == nil,
                       !flag(PHImageCancelledKey, in: info),
                       info?[PHImageErrorKey] == nil,
                       !flag(PHImageResultIsInCloudKey, in: info),
                       !flag(PHImageResultIsDegradedKey, in: info) {
                        log.error(
                            "PhotoKit returned unsafe image: asset=\(asset.localIdentifier, privacy: .private(mask: .hash)), type=\(asset.mediaType.rawValue, privacy: .public), pixels=\(asset.pixelWidth, privacy: .public)x\(asset.pixelHeight, privacy: .public)"
                        )
                    }
                    state.complete(accepted)
                }
                state.attach(requestID)
                if let requestTimeoutNanoseconds {
                    state.scheduleTimeout(nanoseconds: requestTimeoutNanoseconds) {
                        log.error(
                            "PhotoKit image timed out: asset=\(asset.localIdentifier, privacy: .private(mask: .hash)), type=\(asset.mediaType.rawValue, privacy: .public), pixels=\(asset.pixelWidth, privacy: .public)x\(asset.pixelHeight, privacy: .public)"
                        )
                    }
                }
            }
        } onCancel: {
            state.cancel()
        }
    }

    static func requestVideoAsset(
        for asset: PHAsset,
        options: PHVideoRequestOptions,
        imageManager: PHImageManager = .default()
    ) async -> AVAsset? {
        let state = PhotoKitRequestState<AVAsset>(imageManager: imageManager)
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                guard !Task.isCancelled else {
                    state.cancel()
                    return
                }

                let requestID = imageManager.requestAVAsset(forVideo: asset, options: options) { avAsset, _, _ in
                    state.complete(avAsset)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    static func requestImageData(
        for asset: PHAsset,
        options: PHImageRequestOptions,
        imageManager: PHImageManager = .default()
    ) async -> Data? {
        let state = PhotoKitRequestState<Data>(imageManager: imageManager)
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                guard !Task.isCancelled else {
                    state.cancel()
                    return
                }

                let requestID = imageManager.requestImageDataAndOrientation(
                    for: asset,
                    options: options
                ) { data, _, _, info in
                    guard !flag(PHImageCancelledKey, in: info),
                          info?[PHImageErrorKey] == nil,
                          options.isNetworkAccessAllowed || !flag(PHImageResultIsInCloudKey, in: info)
                    else {
                        state.complete(nil)
                        return
                    }
                    state.complete(data)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    static func requestLivePhoto(
        for asset: PHAsset,
        targetSize: CGSize,
        contentMode: PHImageContentMode,
        options: PHLivePhotoRequestOptions,
        imageManager: PHImageManager = .default()
    ) async -> PHLivePhoto? {
        let state = PhotoKitRequestState<PHLivePhoto>(imageManager: imageManager)
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                guard !Task.isCancelled else {
                    state.cancel()
                    return
                }

                let requestID = imageManager.requestLivePhoto(
                    for: asset,
                    targetSize: targetSize,
                    contentMode: contentMode,
                    options: options
                ) { livePhoto, info in
                    guard !flag(PHImageCancelledKey, in: info), info?[PHImageErrorKey] == nil else {
                        state.complete(nil)
                        return
                    }
                    if flag(PHImageResultIsDegradedKey, in: info) { return }
                    state.complete(livePhoto)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    static func requestLivePhoto(
        resourceFileURLs: [URL],
        placeholderImage: UIImage?,
        targetSize: CGSize,
        contentMode: PHImageContentMode
    ) async -> PHLivePhoto? {
        let state = PhotoKitRequestState<PHLivePhoto>(cancelRequest: { requestID in
            PHLivePhoto.cancelRequest(withRequestID: requestID)
        })
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                guard !Task.isCancelled else {
                    state.cancel()
                    return
                }

                let requestID = PHLivePhoto.request(
                    withResourceFileURLs: resourceFileURLs,
                    placeholderImage: placeholderImage,
                    targetSize: targetSize,
                    contentMode: contentMode
                ) { livePhoto, info in
                    guard !flag(PHLivePhotoInfoCancelledKey, in: info),
                          info[PHLivePhotoInfoErrorKey] == nil else {
                        state.complete(nil)
                        return
                    }
                    if flag(PHLivePhotoInfoIsDegradedKey, in: info) { return }
                    state.complete(livePhoto)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
    }

    static func acceptedImage(
        _ image: UIImage?,
        info: [AnyHashable: Any]?,
        networkAccessAllowed: Bool
    ) -> UIImage? {
        guard !flag(PHImageCancelledKey, in: info),
              info?[PHImageErrorKey] == nil,
              !flag(PHImageResultIsDegradedKey, in: info),
              networkAccessAllowed || !flag(PHImageResultIsInCloudKey, in: info),
              let image,
              ThumbnailSizing.isSafeForRendering(image)
        else { return nil }
        return image
    }

    private static func flag(_ key: String, in info: [AnyHashable: Any]?) -> Bool {
        (info?[key] as? NSNumber)?.boolValue == true
    }

    static func timeoutNanoseconds(
        networkAccessAllowed: Bool,
        policy: ThumbnailTimeoutPolicy
    ) -> UInt64 {
        if networkAccessAllowed {
            return networkRequestTimeoutNanoseconds
        }
        switch policy {
        case .interactive: return localRequestTimeoutNanoseconds
        case .backupSidecar: return networkRequestTimeoutNanoseconds
        }
    }
}

final class PhotoKitRequestState<Value>: @unchecked Sendable {
    private let cancelRequest: (PHImageRequestID) -> Void
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Value?, Never>?
    private var requestID = PHInvalidImageRequestID
    private var timeoutTask: Task<Void, Never>?
    private var completed = false

    init(imageManager: PHImageManager) {
        self.cancelRequest = { requestID in
            imageManager.cancelImageRequest(requestID)
        }
    }

    init(cancelRequest: @escaping (PHImageRequestID) -> Void) {
        self.cancelRequest = cancelRequest
    }

    func bind(_ continuation: CheckedContinuation<Value?, Never>) -> Bool {
        lock.withLock {
            guard !completed else { return false }
            self.continuation = continuation
            return true
        }
    }

    func attach(_ requestID: PHImageRequestID) {
        let shouldCancel = lock.withLock {
            guard !completed else { return true }
            self.requestID = requestID
            return false
        }
        if shouldCancel {
            cancelRequest(requestID)
        }
    }

    func scheduleTimeout(
        nanoseconds: UInt64,
        onTimeout: @escaping @Sendable () -> Void = {}
    ) {
        let task = Task<Void, Never> { [weak self] in
            do {
                try await Task.sleep(nanoseconds: nanoseconds)
            } catch {
                return
            }
            if self?.cancel() == true {
                onTimeout()
            }
        }
        let shouldCancel = lock.withLock {
            guard !completed else { return true }
            timeoutTask?.cancel()
            timeoutTask = task
            return false
        }
        if shouldCancel { task.cancel() }
    }

    func complete(_ value: Value?) {
        let captured = lock.withLock { () -> (CheckedContinuation<Value?, Never>?, Task<Void, Never>?)? in
            guard !completed else { return nil }
            completed = true
            requestID = PHInvalidImageRequestID
            let captured = (continuation, timeoutTask)
            continuation = nil
            timeoutTask = nil
            return captured
        }
        captured?.1?.cancel()
        captured?.0?.resume(returning: value)
    }

    @discardableResult
    func cancel() -> Bool {
        let captured = lock.withLock { () -> (CheckedContinuation<Value?, Never>?, PHImageRequestID, Task<Void, Never>?)? in
            guard !completed else { return nil }
            completed = true
            let captured = (continuation, requestID, timeoutTask)
            continuation = nil
            requestID = PHInvalidImageRequestID
            timeoutTask = nil
            return captured
        }
        guard let captured else { return false }
        captured.2?.cancel()
        if captured.1 != PHInvalidImageRequestID {
            cancelRequest(captured.1)
        }
        captured.0?.resume(returning: nil)
        return true
    }
}

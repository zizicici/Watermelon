import AppKit
import ImageIO
@preconcurrency import Photos

struct MacThumbnailRenderer:
    BackupThumbnailRendering,
    Sendable
{
    private static let maximumLongSide = 400
    private static let compressionQuality: CGFloat = 0.6

    nonisolated func renderThumbnailJPEG(
        for asset: PHAsset,
        allowNetworkAccess: Bool
    ) async -> Data? {
        let longSide = min(
            max(asset.pixelWidth, asset.pixelHeight),
            Self.maximumLongSide
        )
        guard longSide > 0 else { return nil }

        let options = PHImageRequestOptions()
        options.deliveryMode = .highQualityFormat
        options.resizeMode = .exact
        options.isNetworkAccessAllowed = allowNetworkAccess
        options.isSynchronous = false
        options.version = .current

        let manager = PHImageManager.default()
        let state = MacPhotosRequestState<NSImage>(
            manager: manager
        )
        let image = await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                guard state.bind(continuation) else {
                    continuation.resume(returning: nil)
                    return
                }
                guard !Task.isCancelled else {
                    state.cancel()
                    return
                }
                let requestID = manager.requestImage(
                    for: asset,
                    targetSize: CGSize(
                        width: longSide,
                        height: longSide
                    ),
                    contentMode: .aspectFit,
                    options: options
                ) { image, info in
                    if Self.flag(
                        PHImageResultIsDegradedKey,
                        in: info
                    ) {
                        return
                    }
                    guard !Self.flag(
                        PHImageCancelledKey,
                        in: info
                    ),
                    info?[PHImageErrorKey] == nil,
                    allowNetworkAccess
                        || !Self.flag(
                            PHImageResultIsInCloudKey,
                            in: info
                        ) else {
                        state.complete(nil)
                        return
                    }
                    state.complete(image)
                }
                state.attach(requestID)
            }
        } onCancel: {
            state.cancel()
        }
        guard let image else { return nil }
        return Self.jpegData(from: image)
    }

    private nonisolated static func flag(
        _ key: String,
        in info: [AnyHashable: Any]?
    ) -> Bool {
        (info?[key] as? NSNumber)?.boolValue == true
    }

    private nonisolated static func jpegData(
        from image: NSImage
    ) -> Data? {
        var proposedRect = CGRect(
            origin: .zero,
            size: image.size
        )
        guard let source = image.cgImage(
            forProposedRect: &proposedRect,
            context: nil,
            hints: nil
        ) else {
            return nil
        }
        let width = source.width
        let height = source.height
        guard width > 0, height > 0 else { return nil }

        let colorSpace = CGColorSpaceCreateDeviceRGB()
        let bitmapInfo = CGBitmapInfo.byteOrder32Big.rawValue
            | CGImageAlphaInfo.noneSkipLast.rawValue
        guard let context = CGContext(
            data: nil,
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: 0,
            space: colorSpace,
            bitmapInfo: bitmapInfo
        ) else {
            return nil
        }
        context.setFillColor(NSColor.white.cgColor)
        context.fill(
            CGRect(x: 0, y: 0, width: width, height: height)
        )
        context.interpolationQuality = .high
        context.draw(
            source,
            in: CGRect(
                x: 0,
                y: 0,
                width: width,
                height: height
            )
        )
        guard let opaqueImage = context.makeImage() else {
            return nil
        }

        let data = NSMutableData()
        guard let destination =
            CGImageDestinationCreateWithData(
                data,
                "public.jpeg" as CFString,
                1,
                nil
            ) else {
            return nil
        }
        CGImageDestinationAddImage(
            destination,
            opaqueImage,
            [
                kCGImageDestinationLossyCompressionQuality:
                    Self.compressionQuality
            ] as CFDictionary
        )
        guard CGImageDestinationFinalize(destination) else {
            return nil
        }
        return data as Data
    }
}

nonisolated final class MacPhotosRequestState<Value>:
    @unchecked Sendable
{
    private let cancelRequest: (PHImageRequestID) -> Void
    private let lock = NSLock()
    private var continuation:
        CheckedContinuation<Value?, Never>?
    private var requestID = PHInvalidImageRequestID
    private var completed = false

    init(manager: PHImageManager) {
        cancelRequest = { requestID in
            manager.cancelImageRequest(requestID)
        }
    }

    init(
        cancelRequest:
            @escaping @Sendable (PHImageRequestID) -> Void
    ) {
        self.cancelRequest = cancelRequest
    }

    func bind(
        _ continuation: CheckedContinuation<Value?, Never>
    ) -> Bool {
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

    func complete(_ value: sending Value?) {
        let captured = lock.withLock {
            () -> CheckedContinuation<Value?, Never>? in
            guard !completed else { return nil }
            completed = true
            requestID = PHInvalidImageRequestID
            let captured = continuation
            continuation = nil
            return captured
        }
        captured?.resume(returning: value)
    }

    func cancel() {
        let id = lock.withLock { requestID }
        if id != PHInvalidImageRequestID {
            cancelRequest(id)
        }
        complete(nil)
    }
}

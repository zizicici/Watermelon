import CryptoKit
import Foundation
import Photos
import UIKit

struct ExportedResourceFile: Sendable {
    let fileURL: URL
    let contentHash: Data
    let fileSize: Int64
}

final class PhotoLibraryService: @unchecked Sendable {
    private let imageManager = PHCachingImageManager()
    private let resourceManager = PHAssetResourceManager.default()

    private final class ExportRequestState {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Void, Error>?
        private var requestID: PHAssetResourceDataRequestID?
        private var completed = false

        func bind(continuation: CheckedContinuation<Void, Error>, requestID: PHAssetResourceDataRequestID) -> Bool {
            lock.withLock {
                guard !completed else { return false }
                self.continuation = continuation
                self.requestID = requestID
                return true
            }
        }

        func complete(_ result: Result<Void, Error>) {
            let captured: CheckedContinuation<Void, Error>? = lock.withLock {
                guard !completed else { return nil }
                completed = true
                let c = self.continuation
                self.continuation = nil
                requestID = nil
                return c
            }

            guard let captured else { return }
            switch result {
            case .success:
                captured.resume(returning: ())
            case .failure(let error):
                captured.resume(throwing: error)
            }
        }

        func cancelRequest(using manager: PHAssetResourceManager) {
            let id: PHAssetResourceDataRequestID? = lock.withLock { self.requestID }

            if let id {
                manager.cancelDataRequest(id)
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

    func exportResourceToTempFile(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController? = nil
    ) async throws -> URL {
        let exported = try await exportResourceToTempFileAndDigest(
            resource,
            cancellationController: cancellationController
        )
        return exported.fileURL
    }

    func exportResourceToTempFileAndDigest(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController? = nil
    ) async throws -> ExportedResourceFile {
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
        let digestState = ExportDigestState()

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
                                    digestState.update(with: data)
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

        let fileSizeFromDisk = (try? FileManager.default.attributesOfItem(atPath: url.path)[.size] as? NSNumber)?
            .int64Value ?? 0
        return ExportedResourceFile(
            fileURL: url,
            contentHash: digestState.finalizeDigest(),
            fileSize: max(digestState.totalBytes, fileSizeFromDisk)
        )
    }

    static func isLivePhoto(_ asset: PHAsset) -> Bool {
        asset.mediaSubtypes.contains(.photoLive)
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
        case .fullSizePairedVideo:
            return "fullSizePairedVideo"
        case .adjustmentBasePairedVideo:
            return "adjustmentBasePairedVideo"
        case .adjustmentBaseVideo:
            return "adjustmentBaseVideo"
        case .photoProxy:
            return "photoProxy"
        default:
            return "other_\(type.rawValue)"
        }
    }

    static func resourceTypeCode(_ type: PHAssetResourceType) -> Int {
        type.rawValue
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
        case ResourceTypeCode.fullSizePairedVideo:
            return "fullSizePairedVideo"
        case ResourceTypeCode.adjustmentBasePairedVideo:
            return "adjustmentBasePairedVideo"
        case ResourceTypeCode.adjustmentBaseVideo:
            return "adjustmentBaseVideo"
        case ResourceTypeCode.photoProxy:
            return "photoProxy"
        default:
            return "unknown"
        }
    }
}

private final class ExportDigestState {
    private let lock = NSLock()
    private var hasher = SHA256()
    private(set) var totalBytes: Int64 = 0

    func update(with data: Data) {
        lock.withLock {
            hasher.update(data: data)
            totalBytes += Int64(data.count)
        }
    }

    func finalizeDigest() -> Data {
        lock.withLock { Data(hasher.finalize()) }
    }
}

extension PhotoLibraryService: PhotoLibraryServiceProtocol {}

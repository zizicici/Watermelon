import Foundation
import Photos
import UIKit

struct ExportedResourceFile: Sendable {
    let fileURL: URL
    let contentHash: Data
    let fileSize: Int64
}

enum PhotoLibraryQuery: Equatable, Sendable {
    case allAssets
    case albums(Set<String>)
}

final class PhotoLibraryService: @unchecked Sendable {
    private let imageManager = PHCachingImageManager()
    private let resourceManager = PHAssetResourceManager.default()

    private final class ExportRequestState: @unchecked Sendable {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Void, Error>?
        private var completed = false

        func bind(continuation: CheckedContinuation<Void, Error>) -> Bool {
            lock.withLock {
                guard !completed else { return false }
                self.continuation = continuation
                return true
            }
        }

        func complete(_ result: Result<Void, Error>) {
            let captured: CheckedContinuation<Void, Error>? = lock.withLock {
                guard !completed else { return nil }
                completed = true
                let c = self.continuation
                self.continuation = nil
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

        func cancel() {
            complete(.failure(CancellationError()))
        }
    }

    private final class AvailabilityProbeRequestState {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Bool, Error>?
        private var requestID: PHAssetResourceDataRequestID?
        private var completed = false

        func bind(continuation: CheckedContinuation<Bool, Error>) -> Bool {
            lock.withLock {
                guard !completed else { return false }
                self.continuation = continuation
                return true
            }
        }

        func attachRequestID(_ requestID: PHAssetResourceDataRequestID, using manager: PHAssetResourceManager) {
            let shouldCancel: Bool = lock.withLock {
                if completed { return true }
                self.requestID = requestID
                return false
            }
            if shouldCancel {
                manager.cancelDataRequest(requestID)
            }
        }

        func complete(_ result: Result<Bool, Error>) {
            let captured: CheckedContinuation<Bool, Error>? = lock.withLock {
                guard !completed else { return nil }
                completed = true
                let c = self.continuation
                self.continuation = nil
                requestID = nil
                return c
            }

            guard let captured else { return }
            switch result {
            case .success(let value):
                captured.resume(returning: value)
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

        func markAvailable(using manager: PHAssetResourceManager) {
            let state: (continuation: CheckedContinuation<Bool, Error>?, requestID: PHAssetResourceDataRequestID?)? = lock.withLock {
                guard !completed else { return nil }
                completed = true
                let captured = continuation
                let capturedRequestID = requestID
                continuation = nil
                requestID = nil
                return (captured, capturedRequestID)
            }

            guard let state else { return }
            if let requestID = state.requestID {
                manager.cancelDataRequest(requestID)
            }
            state.continuation?.resume(returning: true)
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

    /// Albums returns one fetch result per resolved album so PHChange can deliver
    /// per-album incremental details. Unresolved album IDs are silently dropped;
    /// store-level scope normalization surfaces the loss to the user.
    func fetchResults(query: PhotoLibraryQuery) -> [PHFetchResult<PHAsset>] {
        switch query {
        case .allAssets:
            return [fetchAssetsResult()]
        case .albums(let identifiers):
            return resolveUserAlbumCollections(identifiers).map {
                PHAsset.fetchAssets(in: $0, options: nil)
            }
        }
    }

    func fetchUserAlbums(shouldCancel: () -> Bool = { false }) -> [LocalAlbumDescriptor] {
        guard !shouldCancel() else { return [] }

        let collections = PHAssetCollection.fetchAssetCollections(
            with: .album,
            subtype: .albumRegular,
            options: nil
        )
        let assetOptions = PHFetchOptions()
        assetOptions.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]

        var albums: [LocalAlbumDescriptor] = []
        albums.reserveCapacity(collections.count)
        for index in 0 ..< collections.count {
            guard !shouldCancel() else { return [] }

            let collection = collections.object(at: index)
            let assets = PHAsset.fetchAssets(in: collection, options: assetOptions)
            let thumbnailAssetIdentifier = assets.count > 0 ? assets.object(at: 0).localIdentifier : nil
            albums.append(LocalAlbumDescriptor(
                localIdentifier: collection.localIdentifier,
                title: collection.localizedTitle ?? String(localized: "home.localAlbums.untitled"),
                assetCount: assets.count,
                thumbnailAssetIdentifier: thumbnailAssetIdentifier
            ))
        }

        return albums.sorted {
            let titleOrder = $0.title.localizedCaseInsensitiveCompare($1.title)
            if titleOrder != .orderedSame {
                return titleOrder == .orderedAscending
            }
            return $0.localIdentifier < $1.localIdentifier
        }
    }

    private func resolveUserAlbumCollections(_ albumIdentifiers: Set<String>) -> [PHAssetCollection] {
        guard !albumIdentifiers.isEmpty else { return [] }
        let fetched = PHAssetCollection.fetchAssetCollections(
            withLocalIdentifiers: Array(albumIdentifiers),
            options: nil
        )
        var result: [PHAssetCollection] = []
        result.reserveCapacity(fetched.count)
        for index in 0 ..< fetched.count {
            let collection = fetched.object(at: index)
            guard collection.assetCollectionType == .album,
                  collection.assetCollectionSubtype == .albumRegular
            else { continue }
            result.append(collection)
        }
        return result
    }

    func existingUserAlbumIdentifiers(in albumIdentifiers: Set<String>) -> Set<String> {
        Set(resolveUserAlbumCollections(albumIdentifiers).map(\.localIdentifier))
    }

    func fetchAssets(
        inAlbumIdentifiers albumIdentifiers: Set<String>,
        ascendingByCreationDate: Bool = false,
        shouldCancel: () -> Bool = { false }
    ) -> [PHAsset] {
        var assets: [PHAsset] = []
        let completed = enumerateAssets(
            inAlbumIdentifiers: albumIdentifiers,
            shouldCancel: shouldCancel
        ) { asset in
            assets.append(asset)
        }
        guard completed else { return [] }

        return assets.sorted { lhs, rhs in
            let lhsDate = lhs.creationDate ?? .distantPast
            let rhsDate = rhs.creationDate ?? .distantPast
            if lhsDate != rhsDate {
                return ascendingByCreationDate ? lhsDate < rhsDate : lhsDate > rhsDate
            }
            return lhs.localIdentifier < rhs.localIdentifier
        }
    }

    @discardableResult
    func enumerateAssets(
        inAlbumIdentifiers albumIdentifiers: Set<String>,
        shouldCancel: () -> Bool = { false },
        visit: (PHAsset) -> Void
    ) -> Bool {
        guard !shouldCancel() else { return false }
        let collections = resolveUserAlbumCollections(albumIdentifiers)

        var visitedAssetIDs = Set<String>()
        for collection in collections {
            guard !shouldCancel() else { return false }
            let assets = PHAsset.fetchAssets(in: collection, options: nil)
            for assetIndex in 0 ..< assets.count {
                guard !shouldCancel() else { return false }

                let asset = assets.object(at: assetIndex)
                guard visitedAssetIDs.insert(asset.localIdentifier).inserted else {
                    continue
                }
                visit(asset)
            }
        }

        return true
    }

    func fetchAssets(localIdentifiers: Set<String>) -> [PHAsset] {
        guard !localIdentifiers.isEmpty else { return [] }
        let result = PHAsset.fetchAssets(withLocalIdentifiers: Array(localIdentifiers), options: nil)
        var assets: [PHAsset] = []
        assets.reserveCapacity(result.count)
        for index in 0 ..< result.count {
            assets.append(result.object(at: index))
        }
        return assets
    }

    func exportResourceToTempFile(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController? = nil,
        allowNetworkAccess: Bool = true
    ) async throws -> URL {
        let exported = try await exportResourceToTempFileAndDigest(
            resource,
            cancellationController: cancellationController,
            allowNetworkAccess: allowNetworkAccess
        )
        return exported.fileURL
    }

    // requestData can deliver a multi-GB resource as one Data buffer (OOM jetsam).
    func exportResourceToTempFileAndDigest(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController? = nil,
        allowNetworkAccess: Bool = true
    ) async throws -> ExportedResourceFile {
        let ext = (resource.originalFilename as NSString).pathExtension
        let temp = FileManager.default.temporaryDirectory
        let filename = UUID().uuidString + (ext.isEmpty ? "" : ".\(ext)")
        let url = temp.appendingPathComponent(filename)
        try? FileManager.default.removeItem(at: url)

        let options = PHAssetResourceRequestOptions()
        options.isNetworkAccessAllowed = allowNetworkAccess

        let resourceManager = self.resourceManager
        let state = ExportRequestState()
        let cancellationHandlerID = cancellationController?.addCancellationHandler {
            state.cancel()
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
                    guard state.bind(continuation: continuation) else {
                        continuation.resume(throwing: CancellationError())
                        return
                    }

                    resourceManager.writeData(
                        for: resource,
                        toFile: url,
                        options: options,
                        completionHandler: { error in
                            if let error {
                                state.complete(.failure(error))
                            } else {
                                state.complete(.success(()))
                            }
                        }
                    )

                    if Task.isCancelled {
                        state.cancel()
                    }
                }
            }, onCancel: {
                state.cancel()
            })
        } catch {
            try? FileManager.default.removeItem(at: url)
            throw error
        }

        let hashAndSize: (hash: Data, size: Int64)
        do {
            hashAndSize = try AssetProcessor.contentHashAndSize(
                of: url,
                cancellationController: cancellationController
            )
        } catch {
            try? FileManager.default.removeItem(at: url)
            throw error
        }

        return ExportedResourceFile(
            fileURL: url,
            contentHash: hashAndSize.hash,
            fileSize: hashAndSize.size
        )
    }

    func isResourceLocallyAvailable(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController? = nil
    ) async throws -> Bool {
        let options = PHAssetResourceRequestOptions()
        options.isNetworkAccessAllowed = false

        let resourceManager = self.resourceManager
        let state = AvailabilityProbeRequestState()
        let cancellationHandlerID = cancellationController?.addCancellationHandler {
            state.cancel(using: resourceManager)
        }
        defer {
            if let cancellationHandlerID {
                cancellationController?.removeCancellationHandler(cancellationHandlerID)
            }
        }
        try cancellationController?.throwIfCancelled()

        return try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Bool, Error>) in
                guard state.bind(continuation: continuation) else {
                    continuation.resume(throwing: CancellationError())
                    return
                }

                let requestID = resourceManager.requestData(
                    for: resource,
                    options: options,
                    dataReceivedHandler: { data in
                        guard !data.isEmpty else { return }
                        state.markAvailable(using: resourceManager)
                    },
                    completionHandler: { error in
                        if let error {
                            if Self.isNetworkAccessRequiredError(error) {
                                state.complete(.success(false))
                            } else {
                                state.complete(.failure(error))
                            }
                        } else {
                            state.complete(.success(true))
                        }
                    }
                )

                state.attachRequestID(requestID, using: resourceManager)

                if Task.isCancelled {
                    state.cancel(using: resourceManager)
                }
            }
        }, onCancel: {
            state.cancel(using: resourceManager)
        })
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

    static func isNetworkAccessRequiredError(_ error: Error) -> Bool {
        let nsError = error as NSError
        return nsError.domain == PHPhotosErrorDomain &&
            nsError.code == PHPhotosError.Code.networkAccessRequired.rawValue
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

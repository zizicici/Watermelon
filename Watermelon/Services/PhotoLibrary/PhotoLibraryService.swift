import Foundation
import Photos
import UIKit
import UniformTypeIdentifiers

struct ExportedResourceFile: Sendable {
    let fileURL: URL
    let contentHash: Data
    let fileSize: Int64
}

enum PhotoLibraryQuery: Equatable, Sendable {
    case library(PhotoLibraryMediaFilter)
    case albums(Set<String>)
}

struct PhotoLibraryFetchCollection {
    enum Source: Hashable {
        case library
        case album(String)
        case burst(String)
    }

    let source: Source
    var assets: PHFetchResult<PHAsset>
}

final class PhotoLibraryService: @unchecked Sendable {
    private let resourceManager = PHAssetResourceManager.default()

    final class ExportRequestState: @unchecked Sendable {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Void, Error>?
        private var continuationResolved = false
        private var cancellationRequested = false
        private var writeStarted = false
        private var writeCompleted = false
        private let cleanupCancelledWrite: @Sendable () -> Void

        init(cleanupCancelledWrite: @escaping @Sendable () -> Void) {
            self.cleanupCancelledWrite = cleanupCancelledWrite
        }

        func bind(continuation: CheckedContinuation<Void, Error>) -> Bool {
            lock.withLock {
                guard !continuationResolved, !cancellationRequested else {
                    continuationResolved = true
                    return false
                }
                self.continuation = continuation
                return true
            }
        }

        func beginWrite() -> Bool {
            lock.withLock {
                guard !cancellationRequested, !writeStarted else { return false }
                writeStarted = true
                return true
            }
        }

        func complete(_ result: Result<Void, Error>) {
            let captured: (
                continuation: CheckedContinuation<Void, Error>?,
                cancellationRequested: Bool,
                shouldCleanup: Bool
            )? = lock.withLock {
                guard !writeCompleted else { return nil }
                writeCompleted = true
                let captured = (
                    continuationResolved ? nil : continuation,
                    cancellationRequested,
                    cancellationRequested
                )
                continuationResolved = true
                continuation = nil
                return captured
            }

            guard let captured else { return }
            if captured.shouldCleanup {
                cleanupCancelledWrite()
            }
            if captured.cancellationRequested {
                captured.continuation?.resume(throwing: CancellationError())
                return
            }
            switch result {
            case .success:
                captured.continuation?.resume(returning: ())
            case .failure(let error):
                captured.continuation?.resume(throwing: error)
            }
        }

        func requestCancellation() {
            let captured: CheckedContinuation<Void, Error>? = lock.withLock {
                guard !cancellationRequested, !writeCompleted else { return nil }
                cancellationRequested = true
                guard !continuationResolved else { return nil }
                continuationResolved = true
                let captured = continuation
                continuation = nil
                return captured
            }
            captured?.resume(throwing: CancellationError())
        }

        var callerCanRemoveTemporaryFile: Bool {
            lock.withLock { !writeStarted || writeCompleted }
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

    static func makeAssetFetchOptions() -> PHFetchOptions {
        let options = PHFetchOptions()
        // Every burst frame needs its own backup and download identity.
        options.includeAllBurstAssets = true
        return options
    }

    private func fetchBurstMembers(in collection: PHAssetCollection) -> PHFetchResult<PHAsset> {
        let options = Self.makeAssetFetchOptions()
        options.predicate = NSPredicate(format: "burstIdentifier != nil")
        return PHAsset.fetchAssets(in: collection, options: options)
    }

    private func fetchBurstCollections(
        in collections: [PHAssetCollection],
        options: PHFetchOptions,
        shouldCancel: () -> Bool = { false }
    ) -> [PhotoLibraryFetchCollection] {
        var results: [PhotoLibraryFetchCollection] = []
        var burstIDs = Set<String>()
        for collection in collections {
            guard !shouldCancel() else { return [] }
            // Discover membership without date filters; each expanded frame is filtered separately.
            let members = fetchBurstMembers(in: collection)
            for index in 0..<members.count {
                guard !shouldCancel() else { return [] }
                guard let burstID = members.object(at: index).burstIdentifier,
                      burstIDs.insert(burstID).inserted else { continue }
                results.append(PhotoLibraryFetchCollection(source: .burst(burstID),
                    assets: PHAsset.fetchAssets(withBurstIdentifier: burstID, options: options)))
            }
        }
        return results
    }

    func fetchBurstCollections(inAlbumIdentifiers identifiers: Set<String>) -> [PhotoLibraryFetchCollection] {
        fetchBurstCollections(in: resolveUserAlbumCollections(identifiers), options: Self.makeAssetFetchOptions())
    }

    func fetchAssetsResult(
        ascendingByCreationDate: Bool = false,
        since: Date? = nil,
        mediaFilter: PhotoLibraryMediaFilter = .all
    ) -> PHFetchResult<PHAsset> {
        let options = Self.makeAssetFetchOptions()
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: ascendingByCreationDate)]
        var predicates = [mediaFilter.predicate].compactMap { $0 }
        if let since {
            predicates.append(NSPredicate(format: "creationDate >= %@", since as NSDate))
        }
        if !predicates.isEmpty {
            options.predicate = NSCompoundPredicate(andPredicateWithSubpredicates: predicates)
        }
        return PHAsset.fetchAssets(with: options)
    }

    // Invalid album selections produce no content; the caller presents the repair action.
    func fetchResults(query: PhotoLibraryQuery) -> [PHFetchResult<PHAsset>] {
        fetchCollections(query: query).map(\.assets)
    }

    func fetchCollections(query: PhotoLibraryQuery) -> [PhotoLibraryFetchCollection] {
        switch query {
        case .library(let filter):
            return [PhotoLibraryFetchCollection(source: .library, assets: fetchAssetsResult(mediaFilter: filter))]
        case .albums(let identifiers):
            guard (try? validateAlbumSelection(identifiers)) != nil else { return [] }
            let collections = resolveUserAlbumCollections(identifiers)
            guard collections.count == identifiers.count else { return [] }
            return collections.map {
                PhotoLibraryFetchCollection(source: .album($0.localIdentifier),
                    assets: PHAsset.fetchAssets(in: $0, options: Self.makeAssetFetchOptions()))
            } + fetchBurstCollections(in: collections, options: Self.makeAssetFetchOptions())
        }
    }

    func collectAssetIDs(query: PhotoLibraryQuery) -> Set<String> {
        var ids = Set<String>()
        for result in fetchResults(query: query) {
            for index in 0 ..< result.count {
                ids.insert(result.object(at: index).localIdentifier)
            }
        }
        return ids
    }

    func fetchUserAlbums(shouldCancel: () -> Bool = { false }) -> [LocalAlbumDescriptor] {
        guard !shouldCancel() else { return [] }

        let collections = PHAssetCollection.fetchAssetCollections(
            with: .album,
            subtype: .albumRegular,
            options: nil
        )
        let assetOptions = Self.makeAssetFetchOptions()
        assetOptions.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]

        var albums: [LocalAlbumDescriptor] = []
        albums.reserveCapacity(collections.count)
        var frameIDsByBurst: [String: Set<String>] = [:]
        for index in 0 ..< collections.count {
            guard !shouldCancel() else { return [] }

            let collection = collections.object(at: index)
            let assets = PHAsset.fetchAssets(in: collection, options: assetOptions)
            let members = fetchBurstMembers(in: collection)
            var memberIDsByBurst: [String: Set<String>] = [:]
            for index in 0..<members.count {
                guard !shouldCancel() else { return [] }
                let member = members.object(at: index)
                if let burstID = member.burstIdentifier {
                    memberIDsByBurst[burstID, default: []].insert(member.localIdentifier)
                }
            }
            var assetCount = assets.count
            for (burstID, memberIDs) in memberIDsByBurst {
                guard !shouldCancel() else { return [] }
                if frameIDsByBurst[burstID] == nil {
                    let frames = PHAsset.fetchAssets(withBurstIdentifier: burstID, options: Self.makeAssetFetchOptions())
                    var frameIDs = Set<String>()
                    for index in 0..<frames.count {
                        guard !shouldCancel() else { return [] }
                        frameIDs.insert(frames.object(at: index).localIdentifier)
                    }
                    frameIDsByBurst[burstID] = frameIDs
                }
                assetCount += frameIDsByBurst[burstID, default: []].subtracting(memberIDs).count
            }
            let thumbnailAssetIdentifier = assets.firstObject?.localIdentifier
            albums.append(LocalAlbumDescriptor(
                localIdentifier: collection.localIdentifier,
                title: collection.localizedTitle ?? String(localized: "home.localAlbums.untitled"),
                assetCount: assetCount,
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

    func fetchUserAlbumReferences(in identifiers: Set<String>? = nil) -> [LocalAlbumReference] {
        let collections: [PHAssetCollection]
        if let identifiers {
            collections = resolveUserAlbumCollections(identifiers)
        } else {
            let fetched = PHAssetCollection.fetchAssetCollections(with: .album, subtype: .albumRegular, options: nil)
            collections = (0..<fetched.count).map { fetched.object(at: $0) }
        }
        return collections.map {
            LocalAlbumReference(id: $0.localIdentifier, name: $0.localizedTitle ?? String(localized: "home.localAlbums.untitled"))
        }.sorted {
            let order = $0.name.localizedCaseInsensitiveCompare($1.name)
            return order == .orderedSame ? $0.id < $1.id : order == .orderedAscending
        }
    }

    func fetchAssets(
        inAlbumIdentifiers albumIdentifiers: Set<String>,
        ascendingByCreationDate: Bool = false,
        since: Date? = nil,
        shouldCancel: () -> Bool = { false }
    ) -> [PHAsset] {
        var assets: [PHAsset] = []
        let completed = enumerateAssets(
            inAlbumIdentifiers: albumIdentifiers,
            since: since,
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

    // Resolves any library scope to a flat, creation-date-ordered, de-duplicated asset list.
    func fetchAssets(
        for query: PhotoLibraryQuery,
        ascendingByCreationDate: Bool = false,
        shouldCancel: () -> Bool = { false }
    ) -> [PHAsset] {
        switch query {
        case .library(let filter):
            let result = fetchAssetsResult(ascendingByCreationDate: ascendingByCreationDate, mediaFilter: filter)
            var assets: [PHAsset] = []
            assets.reserveCapacity(result.count)
            for index in 0 ..< result.count {
                if shouldCancel() { return [] }
                assets.append(result.object(at: index))
            }
            return assets
        case .albums(let identifiers):
            return fetchAssets(
                inAlbumIdentifiers: identifiers,
                ascendingByCreationDate: ascendingByCreationDate,
                shouldCancel: shouldCancel
            )
        }
    }

    @discardableResult
    func enumerateAssets(
        inAlbumIdentifiers albumIdentifiers: Set<String>,
        since: Date? = nil,
        shouldCancel: () -> Bool = { false },
        visit: (PHAsset) -> Void
    ) -> Bool {
        guard !shouldCancel() else { return false }
        let collections = resolveUserAlbumCollections(albumIdentifiers)
        let options = Self.makeAssetFetchOptions()
        if let since {
            options.predicate = NSPredicate(format: "creationDate >= %@", since as NSDate)
        }

        var visitedAssetIDs = Set<String>()
        for collection in collections {
            guard !shouldCancel() else { return false }
            let results = [PHAsset.fetchAssets(in: collection, options: options)]
                + fetchBurstCollections(in: [collection], options: options, shouldCancel: shouldCancel).map(\.assets)
            guard !shouldCancel() else { return false }
            for assets in results {
                for assetIndex in 0 ..< assets.count {
                    guard !shouldCancel() else { return false }

                    let asset = assets.object(at: assetIndex)
                    guard visitedAssetIDs.insert(asset.localIdentifier).inserted else {
                        continue
                    }
                    visit(asset)
                }
            }
        }

        return true
    }

    func fetchAssets(localIdentifiers: Set<String>) -> [PHAsset] {
        guard !localIdentifiers.isEmpty else { return [] }
        let result = PHAsset.fetchAssets(withLocalIdentifiers: Array(localIdentifiers), options: PhotoLibraryService.makeAssetFetchOptions())
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
        allowNetworkAccess: Bool = true,
        onProgress: (@Sendable (Double) -> Void)? = nil
    ) async throws -> URL {
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()
        let ext = (Self.safeOriginalFilename(for: resource) as NSString).pathExtension
        let temp = FileManager.default.temporaryDirectory
        let filename = UUID().uuidString + (ext.isEmpty ? "" : ".\(ext)")
        let url = temp.appendingPathComponent(filename)
        try? FileManager.default.removeItem(at: url)

        let options = PHAssetResourceRequestOptions()
        options.isNetworkAccessAllowed = allowNetworkAccess
        options.progressHandler = onProgress

        let resourceManager = self.resourceManager
        let state = ExportRequestState {
            try? FileManager.default.removeItem(at: url)
        }
        let cancellationHandlerID = cancellationController?.addCancellationHandler {
            state.requestCancellation()
        }
        defer {
            if let cancellationHandlerID {
                cancellationController?.removeCancellationHandler(cancellationHandlerID)
            }
        }
        do {
            try await withTaskCancellationHandler(operation: {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    guard state.bind(continuation: continuation) else {
                        continuation.resume(throwing: CancellationError())
                        return
                    }
                    guard state.beginWrite() else {
                        state.requestCancellation()
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
                        state.requestCancellation()
                    }
                }
            }, onCancel: {
                state.requestCancellation()
            })
            try Task.checkCancellation()
            try cancellationController?.throwIfCancelled()
        } catch {
            if state.callerCanRemoveTemporaryFile {
                try? FileManager.default.removeItem(at: url)
            }
            throw error
        }

        onProgress?(1)
        return url
    }

    func exportResourceToTempFileAndDigest(
        _ resource: PHAssetResource,
        cancellationController: BackupCancellationController? = nil,
        allowNetworkAccess: Bool = true,
        onProgress: (@Sendable (Double) -> Void)? = nil
    ) async throws -> ExportedResourceFile {
        let url = try await exportResourceToTempFile(
            resource,
            cancellationController: cancellationController,
            allowNetworkAccess: allowNetworkAccess,
            onProgress: { fraction in onProgress?(fraction * 0.5) }
        )

        let hashAndSize: (hash: Data, size: Int64)
        do {
            let expectedSize = max(1, (try? url.resourceValues(forKeys: [.fileSizeKey]).fileSize) ?? 0)
            hashAndSize = try AssetProcessor.contentHashAndSize(
                of: url,
                cancellationController: cancellationController,
                onProgress: { bytes in
                    onProgress?(0.5 + min(1, Double(bytes) / Double(expectedSize)) * 0.5)
                }
            )
        } catch {
            try? FileManager.default.removeItem(at: url)
            throw error
        }

        onProgress?(1)
        return ExportedResourceFile(
            fileURL: url,
            contentHash: hashAndSize.hash,
            fileSize: hashAndSize.size
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

    static func safeOriginalFilename(for resource: PHAssetResource) -> String {
        if let filename = resource.value(forKey: "originalFilename") as? String {
            return filename
        }
        return fallbackOriginalFilename(for: resource)
    }

    static func safeUniformTypeIdentifier(for resource: PHAssetResource) -> String? {
        resource.value(forKey: "uniformTypeIdentifier") as? String
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

    private static func fallbackOriginalFilename(for resource: PHAssetResource) -> String {
        let role = resourceTypeCode(resource.type)
        guard let ext = preferredFilenameExtension(for: resource), !ext.isEmpty else {
            return "resource_\(role)"
        }
        return "resource_\(role).\(ext)"
    }

    private static func preferredFilenameExtension(for resource: PHAssetResource) -> String? {
        if let uti = safeUniformTypeIdentifier(for: resource),
           let ext = UTType(uti)?.preferredFilenameExtension {
            return ext
        }
        switch resource.type {
        case .photo, .fullSizePhoto, .alternatePhoto, .adjustmentBasePhoto, .photoProxy:
            return "jpg"
        case .video, .fullSizeVideo, .pairedVideo, .fullSizePairedVideo, .adjustmentBaseVideo, .adjustmentBasePairedVideo:
            return "mov"
        case .audio:
            return "m4a"
        case .adjustmentData:
            return "plist"
        default:
            return nil
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

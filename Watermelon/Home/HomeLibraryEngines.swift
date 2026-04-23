import Foundation
@preconcurrency import Photos
import os.log

private let dataLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeData")

private struct HomeRemoteDelta {
    let changedMonths: Set<LibraryMonthKey>
}

private func homeMediaKind(for asset: PHAsset) -> AlbumMediaKind {
    if PhotoLibraryService.isLivePhoto(asset) {
        return .livePhoto
    }
    if asset.mediaType == .video {
        return .video
    }
    return .photo
}

private final class HomeLocalIndexEngine: @unchecked Sendable {
    struct MonthAggregate {
        let assetCount: Int
        let photoCount: Int
        let videoCount: Int
        let backedUpCount: Int
    }

    private(set) var localFetchResult: PHFetchResult<PHAsset>?
    private var localAssetIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
    private var assetIDToMonth: [String: LibraryMonthKey] = [:]
    private var mediaKindByAssetID: [String: AlbumMediaKind] = [:]
    // In-memory mirror of `local_assets.assetFingerprint` so recomputeAggregates can
    // compute backed-up counts without hitting the DB.
    private var fingerprintByAssetID: [String: Data] = [:]
    private var monthAggregates: [LibraryMonthKey: MonthAggregate] = [:]
    private(set) var monthFileSizes: [LibraryMonthKey: Int64] = [:]

    var hasLoadedIndex: Bool {
        localFetchResult != nil
    }

    var allMonths: Set<LibraryMonthKey> {
        Set(localAssetIDsByMonth.keys)
    }

    func knownAssetIDs(in assetIDs: Set<String>) -> Set<String> {
        var result: Set<String> = []
        result.reserveCapacity(assetIDs.count)
        for id in assetIDs where assetIDToMonth[id] != nil {
            result.insert(id)
        }
        return result
    }

    func localAssetIDs(for month: LibraryMonthKey) -> Set<String> {
        localAssetIDsByMonth[month] ?? []
    }

    func monthForAsset(_ assetID: String) -> LibraryMonthKey? {
        assetIDToMonth[assetID]
    }

    func fingerprints(for assetIDs: Set<String>) -> [String: Data] {
        guard !assetIDs.isEmpty else { return [:] }
        var result: [String: Data] = [:]
        result.reserveCapacity(assetIDs.count)
        for id in assetIDs {
            if let fp = fingerprintByAssetID[id] {
                result[id] = fp
            }
        }
        return result
    }

    func localMonthAssetCounts() -> [(month: LibraryMonthKey, count: Int)] {
        localAssetIDsByMonth
            .map { (month: $0.key, count: $0.value.count) }
            .sorted { $0.month > $1.month }
    }

    func localMonthSummary(for month: LibraryMonthKey) -> HomeMonthSummary? {
        guard let agg = monthAggregates[month] else { return nil }
        return HomeMonthSummary(
            month: month,
            assetCount: agg.assetCount,
            photoCount: agg.photoCount,
            videoCount: agg.videoCount,
            backedUpCount: agg.backedUpCount,
            totalSizeBytes: monthFileSizes[month]
        )
    }

    func setMonthFileSize(_ size: Int64, for month: LibraryMonthKey) {
        monthFileSizes[month] = size
    }

    func reloadAll(
        fetchResult: PHFetchResult<PHAsset>,
        fingerprintByAsset: [String: Data],
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) -> Set<LibraryMonthKey> {
        var newIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
        newIDsByMonth.reserveCapacity(24)
        var newAssetIDToMonth: [String: LibraryMonthKey] = [:]
        newAssetIDToMonth.reserveCapacity(fetchResult.count)
        var newMediaKindByAssetID: [String: AlbumMediaKind] = [:]
        newMediaKindByAssetID.reserveCapacity(fetchResult.count)
        // Filtered copy of the DB snapshot that keeps only IDs present in the new fetchResult,
        // so orphaned rows (asset deleted from PhotoKit but still in local_assets) don't linger.
        var newFingerprintByAssetID: [String: Data] = [:]
        newFingerprintByAssetID.reserveCapacity(fingerprintByAsset.count)

        for index in 0 ..< fetchResult.count {
            let asset = fetchResult.object(at: index)
            let assetID = asset.localIdentifier
            let month = LibraryMonthKey.from(date: asset.creationDate)
            newIDsByMonth[month, default: []].insert(assetID)
            newAssetIDToMonth[assetID] = month
            newMediaKindByAssetID[assetID] = homeMediaKind(for: asset)
            if let fp = fingerprintByAsset[assetID] {
                newFingerprintByAssetID[assetID] = fp
            }
        }

        let changedMonths = allMonths.union(newIDsByMonth.keys)

        localFetchResult = fetchResult
        localAssetIDsByMonth = newIDsByMonth
        assetIDToMonth = newAssetIDToMonth
        mediaKindByAssetID = newMediaKindByAssetID
        fingerprintByAssetID = newFingerprintByAssetID
        monthAggregates.removeAll(keepingCapacity: true)
        for month in monthFileSizes.keys where newIDsByMonth[month] == nil {
            monthFileSizes[month] = nil
        }

        recomputeAggregates(
            for: Set(newIDsByMonth.keys),
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )

        return changedMonths
    }

    func clearIfNeeded() -> Set<LibraryMonthKey> {
        guard !localAssetIDsByMonth.isEmpty else { return [] }

        let changedMonths = allMonths
        localFetchResult = nil
        localAssetIDsByMonth.removeAll()
        assetIDToMonth.removeAll()
        mediaKindByAssetID.removeAll()
        fingerprintByAssetID.removeAll()
        monthAggregates.removeAll()
        monthFileSizes.removeAll()
        return changedMonths
    }

    /// `fetchedAssetsByID` must cover any inserts and any assets whose creationDate or
    /// mediaType may have changed. Existing IDs without a refetched PHAsset keep their
    /// current month/mediaKind.
    func refresh(
        assetIDs: Set<String>,
        fetchedAssetsByID: [String: PHAsset],
        fingerprintsForIDs: (Set<String>) -> [String: Data],
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty else { return [] }
        guard localFetchResult != nil else { return [] }

        let existingIDs = knownAssetIDs(in: assetIDs)
        let insertedIDs = Set(fetchedAssetsByID.keys).subtracting(existingIDs)
        guard !existingIDs.isEmpty || !insertedIDs.isEmpty else { return [] }

        var changedMonths = Set<LibraryMonthKey>()

        for id in existingIDs {
            // If the asset was refetched its month/mediaKind could have changed; otherwise
            // membership is unchanged and only backed-up flag needs recompute in current month.
            if let asset = fetchedAssetsByID[id] {
                let newMonth = LibraryMonthKey.from(date: asset.creationDate)
                let mediaKind = homeMediaKind(for: asset)
                if let oldMonth = monthForAsset(id), oldMonth != newMonth {
                    removeFromIDSets(id, month: oldMonth)
                    changedMonths.insert(oldMonth)
                }
                insertAssetID(id, month: newMonth, mediaKind: mediaKind)
                changedMonths.insert(newMonth)
            } else if let month = monthForAsset(id) {
                changedMonths.insert(month)
            }
        }

        for id in insertedIDs {
            guard let asset = fetchedAssetsByID[id] else { continue }
            let month = LibraryMonthKey.from(date: asset.creationDate)
            let mediaKind = homeMediaKind(for: asset)
            insertAssetID(id, month: month, mediaKind: mediaKind)
            changedMonths.insert(month)
        }

        refreshFingerprintCache(
            for: existingIDs.union(insertedIDs),
            using: fingerprintsForIDs
        )
        recomputeAggregates(
            for: changedMonths,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
        return changedMonths
    }

    func applyPhotoLibraryChange(
        _ changeInstance: PHChange,
        fingerprintsForIDs: (Set<String>) -> [String: Data],
        fullFingerprintSnapshot: () -> [String: Data],
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard let currentFetchResult = localFetchResult,
              let details = changeInstance.changeDetails(for: currentFetchResult) else {
            return []
        }

        let nextFetchResult = details.fetchResultAfterChanges
        localFetchResult = nextFetchResult

        guard details.hasIncrementalChanges else {
            return reloadAll(
                fetchResult: nextFetchResult,
                fingerprintByAsset: fullFingerprintSnapshot(),
                remoteFingerprintsForMonth: remoteFingerprintsForMonth
            )
        }

        var removedAssetIDs = Set<String>()
        if let removedIndexes = details.removedIndexes {
            removedAssetIDs.reserveCapacity(removedIndexes.count)
            for index in removedIndexes {
                removedAssetIDs.insert(currentFetchResult.object(at: index).localIdentifier)
            }
        }

        var upsertAssetsByID: [String: PHAsset] = [:]

        if let insertedIndexes = details.insertedIndexes {
            for index in insertedIndexes {
                let asset = nextFetchResult.object(at: index)
                upsertAssetsByID[asset.localIdentifier] = asset
            }
        }

        if let changedIndexes = details.changedIndexes {
            for index in changedIndexes {
                let asset = nextFetchResult.object(at: index)
                upsertAssetsByID[asset.localIdentifier] = asset
            }
        }

        if details.hasMoves {
            details.enumerateMoves { _, toIndex in
                let asset = nextFetchResult.object(at: toIndex)
                upsertAssetsByID[asset.localIdentifier] = asset
            }
        }

        var changedMonths = Set<LibraryMonthKey>()

        for id in removedAssetIDs {
            if let month = monthForAsset(id) {
                removeFromIDSets(id, month: month)
                changedMonths.insert(month)
            }
        }

        for asset in upsertAssetsByID.values {
            let id = asset.localIdentifier
            let newMonth = LibraryMonthKey.from(date: asset.creationDate)
            let mediaKind = homeMediaKind(for: asset)
            if let oldMonth = monthForAsset(id), oldMonth != newMonth {
                removeFromIDSets(id, month: oldMonth)
                changedMonths.insert(oldMonth)
            }
            insertAssetID(id, month: newMonth, mediaKind: mediaKind)
            changedMonths.insert(newMonth)
        }

        refreshFingerprintCache(
            for: Set(upsertAssetsByID.keys),
            using: fingerprintsForIDs
        )
        recomputeAggregates(
            for: changedMonths,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
        return changedMonths
    }

    /// Recompute backed-up count for months whose remote fingerprints changed.
    func refreshBackedUpState(
        affectedMonths: Set<LibraryMonthKey>,
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard !affectedMonths.isEmpty else { return [] }
        let knownMonths = affectedMonths.filter { localAssetIDsByMonth[$0] != nil }
        recomputeAggregates(
            for: knownMonths,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
        return knownMonths
    }

    private func refreshFingerprintCache(
        for ids: Set<String>,
        using fingerprintsForIDs: (Set<String>) -> [String: Data]
    ) {
        guard !ids.isEmpty else { return }
        let fresh = fingerprintsForIDs(ids)
        for id in ids {
            // A missing key means the DB row has no fingerprint (e.g., size-only scan);
            // assigning nil clears any stale cache entry.
            fingerprintByAssetID[id] = fresh[id]
        }
    }

    private func recomputeAggregates(
        for months: Set<LibraryMonthKey>,
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) {
        // Per-month scratch Set reused across iterations to avoid repeated reallocations.
        var seenBackedUpFingerprints = Set<Data>()
        for month in months {
            guard let ids = localAssetIDsByMonth[month], !ids.isEmpty else {
                monthAggregates[month] = nil
                monthFileSizes[month] = nil
                localAssetIDsByMonth[month] = nil
                continue
            }
            let remoteSet = remoteFingerprintsForMonth(month)
            var photos = 0
            var videos = 0
            seenBackedUpFingerprints.removeAll(keepingCapacity: true)
            for id in ids {
                // livePhoto is folded into photoCount to match HomeRemoteIndexEngine's two-
                // bucket taxonomy (HomeMonthSummary has no livePhotoCount field).
                switch mediaKindByAssetID[id] ?? .photo {
                case .video:
                    videos += 1
                case .photo, .livePhoto:
                    photos += 1
                }
                if let fp = fingerprintByAssetID[id], remoteSet.contains(fp) {
                    // Dedup by fingerprint: two local assets sharing a fingerprint can match
                    // only one remote asset within the month, so counting each occurrence
                    // would over-report progress.
                    seenBackedUpFingerprints.insert(fp)
                }
            }
            monthAggregates[month] = MonthAggregate(
                assetCount: ids.count,
                photoCount: photos,
                videoCount: videos,
                backedUpCount: seenBackedUpFingerprints.count
            )
        }
    }

    private func insertAssetID(_ id: String, month: LibraryMonthKey, mediaKind: AlbumMediaKind) {
        localAssetIDsByMonth[month, default: []].insert(id)
        assetIDToMonth[id] = month
        mediaKindByAssetID[id] = mediaKind
    }

    private func removeFromIDSets(_ id: String, month: LibraryMonthKey) {
        if var ids = localAssetIDsByMonth[month] {
            ids.remove(id)
            localAssetIDsByMonth[month] = ids.isEmpty ? nil : ids
        }
        assetIDToMonth[id] = nil
        mediaKindByAssetID[id] = nil
        fingerprintByAssetID[id] = nil
    }
}

private final class HomeRemoteIndexEngine: @unchecked Sendable {
    private var remoteFingerprintsByMonth: [LibraryMonthKey: Set<Data>] = [:]
    private var summaryByMonth: [LibraryMonthKey: HomeMonthSummary] = [:]

    private(set) var snapshotRevision: UInt64?

    var allMonths: Set<LibraryMonthKey> {
        Set(remoteFingerprintsByMonth.keys)
    }

    func fingerprints(for month: LibraryMonthKey) -> Set<Data> {
        remoteFingerprintsByMonth[month] ?? []
    }

    func summary(for month: LibraryMonthKey) -> HomeMonthSummary? {
        summaryByMonth[month]
    }

    func apply(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) -> HomeRemoteDelta {
        var changedMonths = Set<LibraryMonthKey>()

        guard hasActiveConnection else {
            if !remoteFingerprintsByMonth.isEmpty {
                changedMonths.formUnion(remoteFingerprintsByMonth.keys)
                clearRemoteState()
            }
            snapshotRevision = state.revision
            return HomeRemoteDelta(changedMonths: changedMonths)
        }

        if snapshotRevision == state.revision, !state.isFullSnapshot {
            return HomeRemoteDelta(changedMonths: changedMonths)
        }

        if state.isFullSnapshot {
            changedMonths.formUnion(remoteFingerprintsByMonth.keys)
            clearRemoteState()
        }

        for monthDelta in state.monthDeltas {
            let month = monthDelta.month
            changedMonths.insert(month)

            let resolved = Self.resolveMonth(month, from: monthDelta)
            remoteFingerprintsByMonth[month] = resolved.fingerprints.isEmpty ? nil : resolved.fingerprints
            summaryByMonth[month] = resolved.summary
        }

        snapshotRevision = state.revision
        return HomeRemoteDelta(changedMonths: changedMonths)
    }

    private func clearRemoteState() {
        remoteFingerprintsByMonth.removeAll()
        summaryByMonth.removeAll()
    }

    private struct ResolvedMonth {
        let fingerprints: Set<Data>
        let summary: HomeMonthSummary?
    }

    /// Applies the same drop rules as `HomeAlbumMatching.buildRemoteItems`: an asset is
    /// included only when at least one of its links points at a resource present in
    /// `delta.resources`. Bytes are summed over those resolvable resources (deduped by
    /// hash) rather than `asset.totalFileSizeBytes`. This matters in the partial-flush
    /// window where assets + links have landed but resources have not.
    private static func resolveMonth(
        _ month: LibraryMonthKey,
        from delta: RemoteLibraryMonthDelta
    ) -> ResolvedMonth {
        guard !delta.assets.isEmpty else {
            return ResolvedMonth(fingerprints: [], summary: nil)
        }

        var resourceSizeByHash: [Data: Int64] = [:]
        resourceSizeByHash.reserveCapacity(delta.resources.count)
        for resource in delta.resources {
            resourceSizeByHash[resource.contentHash] = resource.fileSize
        }

        // Per-asset: collect link roles and the dedup'd set of resolvable resource hashes.
        // Same hash referenced by multiple role/slot pairs still contributes one resource
        // upstream (buildRemoteItems uses seenHashes), so dedup here to match.
        var rolesByAssetID: [String: [Int]] = [:]
        var resolvableHashesByAssetID: [String: Set<Data>] = [:]
        rolesByAssetID.reserveCapacity(delta.assets.count)
        resolvableHashesByAssetID.reserveCapacity(delta.assets.count)
        for link in delta.assetResourceLinks where resourceSizeByHash[link.resourceHash] != nil {
            rolesByAssetID[link.assetID, default: []].append(link.role)
            resolvableHashesByAssetID[link.assetID, default: []].insert(link.resourceHash)
        }

        var fingerprints = Set<Data>()
        fingerprints.reserveCapacity(delta.assets.count)
        var assetCount = 0
        var photoCount = 0
        var videoCount = 0
        var totalSize: Int64 = 0
        for asset in delta.assets {
            let roles = rolesByAssetID[asset.id] ?? []
            guard !roles.isEmpty else { continue }
            fingerprints.insert(asset.assetFingerprint)
            assetCount += 1
            for hash in resolvableHashesByAssetID[asset.id] ?? [] {
                totalSize += resourceSizeByHash[hash] ?? 0
            }
            let hasPairedVideo = roles.contains { ResourceTypeCode.isPairedVideo($0) }
            let hasPhotoLike = roles.contains { ResourceTypeCode.isPhotoLike($0) }
            let hasVideo = roles.contains { ResourceTypeCode.isVideoLike($0) }
            if hasPairedVideo, hasPhotoLike {
                photoCount += 1  // livePhoto
            } else if hasVideo {
                videoCount += 1
            } else {
                photoCount += 1
            }
        }
        let summary: HomeMonthSummary? = assetCount > 0
            ? HomeMonthSummary(
                month: month,
                assetCount: assetCount,
                photoCount: photoCount,
                videoCount: videoCount,
                backedUpCount: nil,
                totalSizeBytes: totalSize
            )
            : nil
        return ResolvedMonth(fingerprints: fingerprints, summary: summary)
    }
}

private struct HomeDataLoadResult {
    let didReload: Bool
    let changedMonths: Set<LibraryMonthKey>
    let isAuthorized: Bool
}

private struct RemoteOnlyQueryResult: Sendable {
    let remoteItems: [RemoteAlbumItem]
    let localFingerprintSet: Set<Data>
}

private final class HomeDataProcessingWorker: @unchecked Sendable {
    private let photoLibraryService: PhotoLibraryService
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let remoteMonthSnapshot: @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
    private let processingQueue = DispatchQueue(
        label: "com.zizicici.watermelon.homeData.processing",
        qos: .userInitiated
    )

    private let localIndex = HomeLocalIndexEngine()
    private let remoteIndex = HomeRemoteIndexEngine()

    private var hasActiveConnection = false
    private var needsRemoteBootstrap = false

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository,
        remoteMonthSnapshot: @escaping @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
    ) {
        self.photoLibraryService = photoLibraryService
        self.contentHashIndexRepository = contentHashIndexRepository
        self.remoteMonthSnapshot = remoteMonthSnapshot
    }

    func remoteSnapshotRevisionForQuery(hasActiveConnection: Bool) -> UInt64? {
        processingQueue.sync {
            if hasActiveConnection, needsRemoteBootstrap {
                return nil
            }
            return remoteIndex.snapshotRevision
        }
    }

    // Helpers bound to the processing queue: the engines that accept these closures
    // are only invoked while we're already executing on processingQueue.
    private func fetchFingerprintsForIDs(_ ids: Set<String>) -> [String: Data] {
        guard !ids.isEmpty else { return [:] }
        do {
            return try contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: ids)
        } catch {
            dataLog.error("[HomeData] fetchAssetFingerprintsByAsset(assetIDs:) failed: \(String(describing: error))")
            return [:]
        }
    }

    private func fetchAllFingerprints() -> [String: Data] {
        do {
            return try contentHashIndexRepository.fetchAssetFingerprintsByAsset()
        } catch {
            dataLog.error("[HomeData] fetchAssetFingerprintsByAsset() failed: \(String(describing: error))")
            return [:]
        }
    }

    private func remoteFingerprintsForMonth(_ month: LibraryMonthKey) -> Set<Data> {
        remoteIndex.fingerprints(for: month)
    }

    private func refreshBackedUpState(
        connectionFlipped: Bool,
        remoteChangedMonths: Set<LibraryMonthKey>
    ) -> Set<LibraryMonthKey> {
        if connectionFlipped {
            return localIndex.refreshBackedUpState(
                affectedMonths: localIndex.allMonths,
                remoteFingerprintsForMonth: remoteFingerprintsForMonth
            )
        }

        guard !remoteChangedMonths.isEmpty else { return [] }
        return localIndex.refreshBackedUpState(
            affectedMonths: remoteChangedMonths,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
    }

    func loadLocalIndex(forceReload: Bool) async -> HomeDataLoadResult {
        if !forceReload, processingQueue.sync(execute: { localIndex.hasLoadedIndex }) {
            return HomeDataLoadResult(didReload: false, changedMonths: [], isAuthorized: true)
        }

        let status = photoLibraryService.authorizationStatus()
        let authorized = (status == .authorized || status == .limited)

        guard authorized else {
            let changedMonths = await withCheckedContinuation { continuation in
                processingQueue.async {
                    continuation.resume(returning: self.localIndex.clearIfNeeded())
                }
            }
            return HomeDataLoadResult(didReload: true, changedMonths: changedMonths, isAuthorized: false)
        }

        let fetchResult = photoLibraryService.fetchAssetsResult()
        let fingerprintByAsset = fetchAllFingerprints()

        let changedMonths = await withCheckedContinuation { continuation in
            processingQueue.async {
                let changed = self.localIndex.reloadAll(
                    fetchResult: fetchResult,
                    fingerprintByAsset: fingerprintByAsset,
                    remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                )
                continuation.resume(returning: changed)
            }
        }

        return HomeDataLoadResult(didReload: true, changedMonths: changedMonths, isAuthorized: true)
    }

    func refreshLocalIndex(forAssetIDs assetIDs: Set<String>) async -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty else { return [] }

        return await withCheckedContinuation { continuation in
            processingQueue.async {
                let start = CFAbsoluteTimeGetCurrent()
                let existingIDs = self.localIndex.knownAssetIDs(in: assetIDs)
                let missingIDs = assetIDs.subtracting(existingIDs)
                let fetchedMissingAssets = self.photoLibraryService.fetchAssets(localIdentifiers: missingIDs)
                let fetchedAssetsByID = Dictionary(uniqueKeysWithValues: fetchedMissingAssets.map { ($0.localIdentifier, $0) })
                let targetIDs = existingIDs.union(fetchedAssetsByID.keys)
                guard !targetIDs.isEmpty else {
                    continuation.resume(returning: [])
                    return
                }

                let changedMonths = self.localIndex.refresh(
                    assetIDs: targetIDs,
                    fetchedAssetsByID: fetchedAssetsByID,
                    fingerprintsForIDs: self.fetchFingerprintsForIDs,
                    remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                )

                let elapsed = CFAbsoluteTimeGetCurrent() - start
                dataLog.info("[HomeData] refreshLocalIndex: assets=\(targetIDs.count), inserted=\(fetchedAssetsByID.count), months=\(changedMonths.count), \(String(format: "%.3f", elapsed))s")
                continuation.resume(returning: changedMonths)
            }
        }
    }

    func syncRemoteSnapshot(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) async -> Set<LibraryMonthKey> {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                let start = CFAbsoluteTimeGetCurrent()
                let connectionFlipped = self.hasActiveConnection != hasActiveConnection
                if connectionFlipped {
                    self.hasActiveConnection = hasActiveConnection
                    if !hasActiveConnection {
                        self.needsRemoteBootstrap = true
                    }
                }

                let localAllMonths = connectionFlipped ? self.localIndex.allMonths : []
                let remoteAllMonths = connectionFlipped ? self.remoteIndex.allMonths : []
                let remoteDelta = self.remoteIndex.apply(state: state, hasActiveConnection: hasActiveConnection)
                var changedMonths = remoteDelta.changedMonths

                let touched = self.refreshBackedUpState(
                    connectionFlipped: connectionFlipped,
                    remoteChangedMonths: remoteDelta.changedMonths
                )
                changedMonths.formUnion(touched)

                if connectionFlipped {
                    changedMonths.formUnion(localAllMonths)
                    changedMonths.formUnion(remoteAllMonths)
                }

                if hasActiveConnection, state.isFullSnapshot {
                    self.needsRemoteBootstrap = false
                }

                let elapsed = CFAbsoluteTimeGetCurrent() - start
                dataLog.info("[HomeData] processingQueue: months=\(changedMonths.count), remoteChanged=\(remoteDelta.changedMonths.count), \(String(format: "%.3f", elapsed))s")
                continuation.resume(returning: changedMonths)
            }
        }
    }

    func applyPhotoLibraryChange(_ changeInstance: PHChange) async -> Set<LibraryMonthKey> {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                let changedMonths = self.localIndex.applyPhotoLibraryChange(
                    changeInstance,
                    fingerprintsForIDs: self.fetchFingerprintsForIDs,
                    fullFingerprintSnapshot: self.fetchAllFingerprints,
                    remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                )
                continuation.resume(returning: changedMonths)
            }
        }
    }

    func monthRow(for month: LibraryMonthKey) -> HomeMonthRow {
        processingQueue.sync { monthRowLocked(for: month) }
    }

    func allMonthRows() -> [LibraryMonthKey: HomeMonthRow] {
        processingQueue.sync {
            let allMonths = localIndex.allMonths.union(remoteIndex.allMonths)
            var result: [LibraryMonthKey: HomeMonthRow] = [:]
            result.reserveCapacity(allMonths.count)
            for month in allMonths {
                result[month] = monthRowLocked(for: month)
            }
            return result
        }
    }

    // No hasActiveConnection guard: the remote engine drops its state on disconnect,
    // so remoteIndex.summary(for:) returns nil whenever we're not connected.
    private func monthRowLocked(for month: LibraryMonthKey) -> HomeMonthRow {
        HomeMonthRow(
            month: month,
            local: localIndex.localMonthSummary(for: month),
            remote: remoteIndex.summary(for: month)
        )
    }

    func localAssetIDs(for month: LibraryMonthKey) -> Set<String> {
        processingQueue.sync {
            localIndex.localAssetIDs(for: month)
        }
    }

    func remoteOnlyItems(for month: LibraryMonthKey) async -> [RemoteAlbumItem] {
        // Everything needed — raw remote delta and the local fingerprint set — is captured in
        // a single processingQueue hop so `buildRemoteItems` and the fingerprint diff below
        // see a consistent snapshot. Doing the PHAsset fetch outside the queue (as an earlier
        // version did) opened a window where a concurrent PHChange could delete a local asset
        // between snapshot and fetch, silently promoting its remote twin to remoteOnly and
        // triggering a redundant restore.
        let query: RemoteOnlyQueryResult? = await withCheckedContinuation { cont in
            processingQueue.async {
                guard self.hasActiveConnection,
                      let delta = self.remoteMonthSnapshot(month) else {
                    cont.resume(returning: nil)
                    return
                }
                let remoteItems = HomeAlbumMatching.buildRemoteItems(
                    assets: delta.assets,
                    resources: delta.resources,
                    links: delta.assetResourceLinks
                )
                let localIDs = self.localIndex.localAssetIDs(for: month)
                cont.resume(returning: RemoteOnlyQueryResult(
                    remoteItems: remoteItems,
                    localFingerprintSet: Set(self.localIndex.fingerprints(for: localIDs).values)
                ))
            }
        }
        guard let query, !query.remoteItems.isEmpty, !Task.isCancelled else { return [] }

        // Oldest-first so failed downloads retry in a deterministic order across runs.
        return query.remoteItems
            .filter { !query.localFingerprintSet.contains($0.assetFingerprint) }
            .sorted {
                if $0.creationDate != $1.creationDate {
                    return $0.creationDate < $1.creationDate
                }
                return $0.id < $1.id
            }
    }

    func matchedCount(for month: LibraryMonthKey) -> Int {
        // Only fingerprint-matched assets count here. Assets with matching content hashes but
        // no fingerprint (hash preflight without a subsequent upload) show as unbacked until
        // AssetProcessor or writeHashIndex stamps a fingerprint onto them.
        processingQueue.sync {
            guard hasActiveConnection else { return 0 }
            return localIndex.localMonthSummary(for: month)?.backedUpCount ?? 0
        }
    }

    func localMonthsForFileSizeScan() -> [LibraryMonthKey] {
        processingQueue.sync {
            localIndex.localMonthAssetCounts().map(\.month)
        }
    }

    func updateFileSize(
        for month: LibraryMonthKey,
        sizeCache: [String: AssetSizeSnapshot]
    ) async -> [AssetSizeUpdate] {
        let ids = await withCheckedContinuation { (cont: CheckedContinuation<Set<String>, Never>) in
            processingQueue.async {
                cont.resume(returning: self.localIndex.localAssetIDs(for: month))
            }
        }
        guard !ids.isEmpty else { return [] }

        let phAssets = photoLibraryService.fetchAssets(localIdentifiers: ids)
        var assetByID: [String: PHAsset] = [:]
        assetByID.reserveCapacity(phAssets.count)
        for asset in phAssets {
            assetByID[asset.localIdentifier] = asset
        }

        var total: Int64 = 0
        var updates: [AssetSizeUpdate] = []
        for id in ids {
            guard let asset = assetByID[id] else { continue }
            let mtime = asset.modificationDate?.millisecondsSinceEpoch

            if let mtime, let snapshot = sizeCache[id], snapshot.modificationDateMs == mtime {
                total += snapshot.totalFileSizeBytes
                continue
            }

            let resources = PHAssetResource.assetResources(for: asset)
            let computedSize = resources.reduce(Int64(0)) { partial, resource in
                partial + max(PhotoLibraryService.resourceFileSize(resource), 0)
            }
            let safeSize = max(computedSize, 0)
            total += safeSize

            if let mtime {
                updates.append(AssetSizeUpdate(
                    assetLocalIdentifier: id,
                    totalFileSizeBytes: safeSize,
                    modificationDateMs: mtime
                ))
            }
        }

        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            processingQueue.async {
                self.localIndex.setMonthFileSize(total, for: month)
                cont.resume()
            }
        }
        return updates
    }

}

@MainActor
final class HomeIncrementalDataManager: NSObject, PHPhotoLibraryChangeObserver {
    private static let fileSizeUpdateCoalescingDelayNs: UInt64 = 250_000_000
    private static let assetSizeWriteBackBatchSize = 200

    private enum FileSizeScanNotificationMode {
        case coalesced
        case byYear
    }

    private let contentHashIndexRepository: ContentHashIndexRepository
    private let processingWorker: HomeDataProcessingWorker

    private var isObservingPhotoLibrary = false
    private var processingMutationCount = 0
    private var deferredPhotoChanges: [PHChange] = []
    private var isDrainingDeferredPhotoChanges = false
    private var pendingFileSizeMonths = Set<LibraryMonthKey>()
    private var fileSizeUpdateTask: Task<Void, Never>?
    private var assetSizeSnapshot: [String: AssetSizeSnapshot] = [:]
    private var assetSizeSnapshotLoaded = false
    private var assetSizeSnapshotLoadTask: Task<[String: AssetSizeSnapshot], Never>?
    private var assetSizeSnapshotGeneration = 0

    var onMonthsChanged: ((Set<LibraryMonthKey>) -> Void)?
    var onFileSizesUpdated: ((Set<LibraryMonthKey>) -> Void)?
    // Startup scan (full library, by-year notifications). At most one runs at a time; a new
    // startup cancels its predecessor. Separate from the PHChange-driven rescan below so the
    // two cannot clobber each other.
    private var fileSizeScanTask: Task<Void, Never>?
    // PHChange rescan (partial, coalesced notifications). Single in-flight:
    // pendingRescanMonths accumulates while a rescan runs; the running task drains
    // it and auto-restarts if more arrived.
    private var fileSizeRescanTask: Task<Void, Never>?
    private var pendingRescanMonths = Set<LibraryMonthKey>()
    // Refcount so invalidateAssetSizeSnapshot() at the end of a scan fires only once all
    // in-flight scans have finished. Otherwise a fast rescan completing while startup is
    // still walking months would wipe the cache out from under startup.
    private var activeFileSizeScanCount = 0

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository,
        remoteMonthSnapshot: @escaping @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
    ) {
        self.contentHashIndexRepository = contentHashIndexRepository
        self.processingWorker = HomeDataProcessingWorker(
            photoLibraryService: photoLibraryService,
            contentHashIndexRepository: contentHashIndexRepository,
            remoteMonthSnapshot: remoteMonthSnapshot
        )
    }

    func remoteSnapshotRevisionForQuery(hasActiveConnection: Bool) -> UInt64? {
        processingWorker.remoteSnapshotRevisionForQuery(hasActiveConnection: hasActiveConnection)
    }

    @discardableResult
    func ensureLocalIndexLoaded() async -> Bool {
        await loadLocalIndex(forceReload: false)
    }

    @discardableResult
    func reloadLocalIndex() async -> Bool {
        await loadLocalIndex(forceReload: true)
    }

    @discardableResult
    func refreshLocalIndex(forAssetIDs assetIDs: Set<String>) async -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty else { return [] }
        beginProcessingMutation()
        let reconciledMonths = await processingWorker.refreshLocalIndex(forAssetIDs: assetIDs)
        finishProcessingMutation()
        return reconciledMonths
    }

    @discardableResult
    func syncRemoteSnapshotOnProcessingQueue(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) async -> Set<LibraryMonthKey> {
        beginProcessingMutation()
        let reconciledMonths = await processingWorker.syncRemoteSnapshot(
            state: state,
            hasActiveConnection: hasActiveConnection
        )
        finishProcessingMutation()
        return reconciledMonths
    }

    func monthRow(for month: LibraryMonthKey) -> HomeMonthRow {
        processingWorker.monthRow(for: month)
    }

    func allMonthRows() -> [LibraryMonthKey: HomeMonthRow] {
        processingWorker.allMonthRows()
    }

    func localAssetIDs(for month: LibraryMonthKey) -> Set<String> {
        processingWorker.localAssetIDs(for: month)
    }

    func remoteOnlyItems(for month: LibraryMonthKey) async -> [RemoteAlbumItem] {
        await processingWorker.remoteOnlyItems(for: month)
    }

    func matchedCount(for month: LibraryMonthKey) -> Int {
        processingWorker.matchedCount(for: month)
    }

    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        Task { @MainActor [weak self] in
            guard let self else { return }

            if self.processingMutationCount > 0 || self.isDrainingDeferredPhotoChanges {
                self.deferredPhotoChanges.append(changeInstance)
                return
            }

            await self.applyPhotoLibraryChangeNow(changeInstance)
        }
    }

    private func beginProcessingMutation() {
        processingMutationCount += 1
    }

    private func finishProcessingMutation() {
        if processingMutationCount > 0 {
            processingMutationCount -= 1
        }

        scheduleDeferredPhotoChangeDrainIfNeeded()
    }

    @discardableResult
    private func loadLocalIndex(forceReload: Bool) async -> Bool {
        let result = await processingWorker.loadLocalIndex(forceReload: forceReload)
        if result.isAuthorized {
            registerPhotoLibraryObserverIfNeeded()
            if result.didReload {
                startFileSizeScan()
            }
        } else {
            unregisterPhotoLibraryObserverIfNeeded()
            fileSizeScanTask?.cancel()
            fileSizeScanTask = nil
            fileSizeRescanTask?.cancel()
            fileSizeRescanTask = nil
            pendingRescanMonths.removeAll()
            resetPendingFileSizeUpdates()
        }
        return !result.changedMonths.isEmpty
    }

    private func applyPhotoLibraryChangeNow(_ changeInstance: PHChange) async {
        beginProcessingMutation()
        let reconciledMonths = await processingWorker.applyPhotoLibraryChange(changeInstance)
        finishProcessingMutation()
        if !reconciledMonths.isEmpty {
            rescanFileSizes(for: reconciledMonths)
            onMonthsChanged?(reconciledMonths)
        }
    }

    private func rescanFileSizes(for months: Set<LibraryMonthKey>) {
        guard !months.isEmpty else { return }
        pendingRescanMonths.formUnion(months)
        guard fileSizeRescanTask == nil else { return }
        startPendingRescan()
    }

    private func startPendingRescan() {
        guard fileSizeRescanTask == nil, !pendingRescanMonths.isEmpty else { return }
        let months = Array(pendingRescanMonths)
        pendingRescanMonths.removeAll()
        // `priority:` is a no-op here: runFileSizeScan is @MainActor, and MainActor ignores
        // task priority on its queue. The work inside the scan yields cooperatively via
        // Task.yield() between months, which is what actually lets UI interleave.
        fileSizeRescanTask = Task { [weak self] in
            await self?.runFileSizeScan(months: months, notificationMode: .coalesced)
            // If we were cancelled externally (startup supersede or auth loss), the caller
            // has already cleared the slot and the pending set; whatever lives in the slot
            // now belongs to a successor rescan, so leave it alone.
            guard let self, !Task.isCancelled else { return }
            self.fileSizeRescanTask = nil
            if !self.pendingRescanMonths.isEmpty {
                self.startPendingRescan()
            }
        }
    }

    private func startFileSizeScan() {
        fileSizeScanTask?.cancel()
        // A full startup scan supersedes any in-flight PHChange rescan: startup's months
        // come from localMonthsForFileSizeScan() — the complete library — so rescan's
        // partial progress is a subset that startup will revisit anyway. Leaving rescan
        // running would make it continue against the invalidated snapshot below, falling
        // back to cold PHAssetResource recomputation for its remaining months. Cancel it
        // and drop pending so the refcount-guarded cache-release logic isn't fighting
        // against the startup's explicit invalidate.
        fileSizeRescanTask?.cancel()
        fileSizeRescanTask = nil
        pendingRescanMonths.removeAll()
        resetPendingFileSizeUpdates()
        // Drop the in-memory snapshot: hash-builder paths (AssetProcessor) write
        // mtime into local_assets during execution without notifying us, so after
        // a forceReload the DB is more up-to-date than our cached dict.
        invalidateAssetSizeSnapshot()
        let months = processingWorker.localMonthsForFileSizeScan()
        fileSizeScanTask = Task { [weak self] in
            await self?.runFileSizeScan(months: months, notificationMode: .byYear)
        }
    }

    private func invalidateAssetSizeSnapshot() {
        assetSizeSnapshot.removeAll()
        assetSizeSnapshotLoaded = false
        assetSizeSnapshotLoadTask?.cancel()
        assetSizeSnapshotLoadTask = nil
        assetSizeSnapshotGeneration &+= 1
    }

    private func releaseAssetSizeSnapshotIfIdle() {
        // Free the ~5 MB working set only once every in-flight scan has finished.
        // Releasing while another scan is mid-walk would make it recompute sizes from
        // PHAssetResource for every remaining month.
        guard activeFileSizeScanCount == 0 else { return }
        invalidateAssetSizeSnapshot()
    }

    private func runFileSizeScan(
        months: [LibraryMonthKey],
        notificationMode: FileSizeScanNotificationMode
    ) async {
        activeFileSizeScanCount += 1
        defer {
            activeFileSizeScanCount -= 1
            releaseAssetSizeSnapshotIfIdle()
        }

        let repository = contentHashIndexRepository
        await ensureAssetSizeSnapshotLoaded(repository: repository)

        let orderedMonths = notificationMode == .byYear ? months.sorted(by: >) : months
        var pendingYearMonths = Set<LibraryMonthKey>()
        var writeBackBuffer: [AssetSizeUpdate] = []

        for (index, month) in orderedMonths.enumerated() {
            if Task.isCancelled {
                Self.flushAssetSizeWriteBack(&writeBackBuffer, repository: repository)
                return
            }
            let updates = await processingWorker.updateFileSize(
                for: month,
                sizeCache: assetSizeSnapshot
            )
            if Task.isCancelled {
                mergeIntoAssetSizeSnapshot(updates)
                writeBackBuffer.append(contentsOf: updates)
                Self.flushAssetSizeWriteBack(&writeBackBuffer, repository: repository)
                return
            }

            mergeIntoAssetSizeSnapshot(updates)
            writeBackBuffer.append(contentsOf: updates)
            if writeBackBuffer.count >= Self.assetSizeWriteBackBatchSize {
                Self.flushAssetSizeWriteBack(&writeBackBuffer, repository: repository)
            }

            switch notificationMode {
            case .coalesced:
                enqueueFileSizeUpdate(for: month)
            case .byYear:
                pendingYearMonths.insert(month)
                let nextYear = index + 1 < orderedMonths.count ? orderedMonths[index + 1].year : nil
                if nextYear != month.year {
                    onFileSizesUpdated?(pendingYearMonths)
                    pendingYearMonths.removeAll(keepingCapacity: true)
                }
            }

            await Task.yield()
        }

        Self.flushAssetSizeWriteBack(&writeBackBuffer, repository: repository)

        if Task.isCancelled { return }
        if notificationMode == .coalesced {
            flushPendingFileSizeUpdates()
        }
    }

    private func ensureAssetSizeSnapshotLoaded(repository: ContentHashIndexRepository) async {
        guard !assetSizeSnapshotLoaded else { return }
        let generation = assetSizeSnapshotGeneration

        let task: Task<[String: AssetSizeSnapshot], Never>
        if let existing = assetSizeSnapshotLoadTask {
            task = existing
        } else {
            task = Task.detached(priority: .utility) {
                (try? repository.fetchAssetSizes()) ?? [:]
            }
            assetSizeSnapshotLoadTask = task
        }

        let loaded = await task.value
        // If invalidated mid-flight, a newer generation is already loading fresh data; drop ours.
        guard !assetSizeSnapshotLoaded, generation == assetSizeSnapshotGeneration else { return }
        assetSizeSnapshot = loaded
        assetSizeSnapshotLoaded = true
        assetSizeSnapshotLoadTask = nil
    }

    private func mergeIntoAssetSizeSnapshot(_ updates: [AssetSizeUpdate]) {
        guard !updates.isEmpty else { return }
        for update in updates {
            assetSizeSnapshot[update.assetLocalIdentifier] = AssetSizeSnapshot(
                totalFileSizeBytes: update.totalFileSizeBytes,
                modificationDateMs: update.modificationDateMs
            )
        }
    }

    private static func flushAssetSizeWriteBack(
        _ buffer: inout [AssetSizeUpdate],
        repository: ContentHashIndexRepository
    ) {
        guard !buffer.isEmpty else { return }
        let entries = buffer
        buffer.removeAll(keepingCapacity: true)
        Task.detached(priority: .background) {
            do {
                try repository.upsertAssetSizes(entries)
            } catch {
                dataLog.error("[HomeData] upsertAssetSizes failed: \(String(describing: error))")
            }
        }
    }

    private func enqueueFileSizeUpdate(for month: LibraryMonthKey) {
        pendingFileSizeMonths.insert(month)
        guard fileSizeUpdateTask == nil else { return }

        fileSizeUpdateTask = Task { [weak self] in
            do {
                try await Task.sleep(nanoseconds: Self.fileSizeUpdateCoalescingDelayNs)
            } catch {
                return
            }

            guard let self, !Task.isCancelled else { return }
            self.flushPendingFileSizeUpdates()
        }
    }

    private func flushPendingFileSizeUpdates() {
        let months = pendingFileSizeMonths
        pendingFileSizeMonths.removeAll()
        fileSizeUpdateTask?.cancel()
        fileSizeUpdateTask = nil

        guard !months.isEmpty else { return }
        onFileSizesUpdated?(months)
    }

    private func resetPendingFileSizeUpdates() {
        pendingFileSizeMonths.removeAll()
        fileSizeUpdateTask?.cancel()
        fileSizeUpdateTask = nil
    }

    private func registerPhotoLibraryObserverIfNeeded() {
        guard !isObservingPhotoLibrary else { return }
        PHPhotoLibrary.shared().register(self)
        isObservingPhotoLibrary = true
    }

    private func unregisterPhotoLibraryObserverIfNeeded() {
        guard isObservingPhotoLibrary else { return }
        PHPhotoLibrary.shared().unregisterChangeObserver(self)
        isObservingPhotoLibrary = false
    }

    private func scheduleDeferredPhotoChangeDrainIfNeeded() {
        guard processingMutationCount == 0,
              !deferredPhotoChanges.isEmpty,
              !isDrainingDeferredPhotoChanges else { return }
        Task { @MainActor [weak self] in
            await self?.drainDeferredPhotoChangesIfNeeded()
        }
    }

    private func drainDeferredPhotoChangesIfNeeded() async {
        guard processingMutationCount == 0,
              !deferredPhotoChanges.isEmpty,
              !isDrainingDeferredPhotoChanges else { return }

        isDrainingDeferredPhotoChanges = true
        defer {
            isDrainingDeferredPhotoChanges = false
            scheduleDeferredPhotoChangeDrainIfNeeded()
        }

        while processingMutationCount == 0, !deferredPhotoChanges.isEmpty {
            let deferred = deferredPhotoChanges.removeFirst()
            await applyPhotoLibraryChangeNow(deferred)
        }
    }
}

import Foundation
@preconcurrency import Photos
import os.log

private let dataLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeData")

private struct HomeRemoteDelta {
    let changedMonths: Set<LibraryMonthKey>
    let changedFingerprints: Set<Data>
}

private final class HomeLocalIndexEngine: @unchecked Sendable {
    private struct LocalState {
        let asset: PHAsset
        let month: LibraryMonthKey
        let fingerprint: Data?
        let hashes: [Data]
        let creationDateNs: Int64
        let isBackedUp: Bool
        let mediaKind: AlbumMediaKind
    }

    private(set) var localFetchResult: PHFetchResult<PHAsset>?
    private var localStatesByAssetID: [String: LocalState] = [:]
    private var localAssetIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
    private var localAssetIDsByFingerprint: [Data: Set<String>] = [:]
    private(set) var monthFileSizes: [LibraryMonthKey: Int64] = [:]

    var hasLoadedIndex: Bool {
        localFetchResult != nil
    }

    var allMonths: Set<LibraryMonthKey> {
        Set(localAssetIDsByMonth.keys)
    }

    func knownAssetIDs(in assetIDs: Set<String>) -> Set<String> {
        Set(assetIDs.filter { localStatesByAssetID[$0] != nil })
    }

    func reloadAll(
        fetchResult: PHFetchResult<PHAsset>,
        hashMapByAsset: [String: [Data]],
        fingerprintByAsset: [String: Data],
        remoteFingerprintSet: Set<Data>
    ) -> Set<LibraryMonthKey> {
        var newStates: [String: LocalState] = [:]
        newStates.reserveCapacity(fetchResult.count)

        var newAssetIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
        newAssetIDsByMonth.reserveCapacity(24)

        var newAssetIDsByFingerprint: [Data: Set<String>] = [:]

        for index in 0 ..< fetchResult.count {
            let asset = fetchResult.object(at: index)
            let assetID = asset.localIdentifier
            let hashes = hashMapByAsset[assetID] ?? []
            let fingerprint = fingerprintByAsset[assetID]
            let month = LibraryMonthKey.from(date: asset.creationDate)
            let isBackedUp = fingerprint.map { remoteFingerprintSet.contains($0) } ?? false
            let mediaKind = Self.mediaKind(for: asset)
            let creationDate = asset.creationDate ?? Date(timeIntervalSince1970: 0)

            newStates[assetID] = LocalState(
                asset: asset,
                month: month,
                fingerprint: fingerprint,
                hashes: hashes,
                creationDateNs: creationDate.nanosecondsSinceEpoch,
                isBackedUp: isBackedUp,
                mediaKind: mediaKind
            )
            newAssetIDsByMonth[month, default: []].insert(assetID)

            if let fingerprint {
                newAssetIDsByFingerprint[fingerprint, default: []].insert(assetID)
            }
        }

        let changedMonths = allMonths.union(newAssetIDsByMonth.keys)

        localFetchResult = fetchResult
        localStatesByAssetID = newStates
        localAssetIDsByMonth = newAssetIDsByMonth
        localAssetIDsByFingerprint = newAssetIDsByFingerprint

        return changedMonths
    }

    func clearIfNeeded() -> Set<LibraryMonthKey> {
        guard !localStatesByAssetID.isEmpty || !localAssetIDsByMonth.isEmpty else {
            return []
        }

        let changedMonths = allMonths

        localFetchResult = nil
        localStatesByAssetID.removeAll()
        localAssetIDsByMonth.removeAll()
        localAssetIDsByFingerprint.removeAll()

        return changedMonths
    }

    func refresh(
        assetIDs: Set<String>,
        fetchedAssetsByID: [String: PHAsset],
        hashMapByAsset: [String: [Data]],
        fingerprintByAsset: [String: Data],
        remoteFingerprintSet: Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty else { return [] }
        guard localFetchResult != nil else { return [] }

        let existingIDs = knownAssetIDs(in: assetIDs)
        let insertedIDs = Set(fetchedAssetsByID.keys).subtracting(existingIDs)
        guard !existingIDs.isEmpty || !insertedIDs.isEmpty else { return [] }

        var changedMonths = Set<LibraryMonthKey>()
        for assetID in existingIDs {
            guard let current = localStatesByAssetID[assetID] else { continue }
            let hashes = hashMapByAsset[assetID] ?? []
            let fingerprint = fingerprintByAsset[assetID]
            changedMonths.formUnion(
                upsertLocalState(
                    asset: current.asset,
                    hashes: hashes,
                    fingerprint: fingerprint,
                    remoteFingerprintSet: remoteFingerprintSet
                )
            )
        }

        for assetID in insertedIDs {
            guard let asset = fetchedAssetsByID[assetID] else { continue }
            let hashes = hashMapByAsset[assetID] ?? []
            let fingerprint = fingerprintByAsset[assetID]
            changedMonths.formUnion(
                upsertLocalState(
                    asset: asset,
                    hashes: hashes,
                    fingerprint: fingerprint,
                    remoteFingerprintSet: remoteFingerprintSet
                )
            )
        }

        return changedMonths
    }

    func applyPhotoLibraryChange(
        _ changeInstance: PHChange,
        fetchHashMapByAsset: (Set<String>) -> [String: [Data]],
        fetchFingerprintByAsset: (Set<String>) -> [String: Data],
        fetchAllHashMapByAsset: () -> [String: [Data]],
        fetchAllFingerprintByAsset: () -> [String: Data],
        remoteFingerprintSet: Set<Data>
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
                hashMapByAsset: fetchAllHashMapByAsset(),
                fingerprintByAsset: fetchAllFingerprintByAsset(),
                remoteFingerprintSet: remoteFingerprintSet
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

        for assetID in removedAssetIDs {
            if let changedMonth = removeLocalState(forAssetID: assetID) {
                changedMonths.insert(changedMonth)
            }
        }

        let upsertAssetIDs = Set(upsertAssetsByID.keys)
        let hashMap = fetchHashMapByAsset(upsertAssetIDs)
        let fingerprintMap = fetchFingerprintByAsset(upsertAssetIDs)

        for asset in upsertAssetsByID.values {
            let hashes = hashMap[asset.localIdentifier] ?? []
            let fingerprint = fingerprintMap[asset.localIdentifier]
            changedMonths.formUnion(
                upsertLocalState(
                    asset: asset,
                    hashes: hashes,
                    fingerprint: fingerprint,
                    remoteFingerprintSet: remoteFingerprintSet
                )
            )
        }

        return changedMonths
    }

    func refreshBackedUpState(
        changedFingerprints: Set<Data>,
        remoteFingerprintSet: Set<Data>,
        limitMonths: Set<LibraryMonthKey>? = nil
    ) -> Set<LibraryMonthKey> {
        guard !changedFingerprints.isEmpty else { return [] }

        var targetAssetIDs = Set<String>()
        for fingerprint in changedFingerprints {
            targetAssetIDs.formUnion(localAssetIDsByFingerprint[fingerprint] ?? [])
        }

        guard !targetAssetIDs.isEmpty else { return [] }

        var changedMonths = Set<LibraryMonthKey>()
        for assetID in targetAssetIDs {
            guard let state = localStatesByAssetID[assetID] else { continue }
            if let limitMonths, !limitMonths.contains(state.month) {
                continue
            }
            changedMonths.formUnion(
                upsertLocalState(
                    asset: state.asset,
                    hashes: state.hashes,
                    fingerprint: state.fingerprint,
                    remoteFingerprintSet: remoteFingerprintSet
                )
            )
        }

        return changedMonths
    }

    func localItems(for month: LibraryMonthKey) -> [LocalAlbumItem] {
        (localAssetIDsByMonth[month] ?? [])
            .compactMap { assetID -> LocalAlbumItem? in
                guard let state = localStatesByAssetID[assetID] else { return nil }
                return LocalAlbumItem(
                    id: assetID,
                    asset: state.asset,
                    creationDate: state.asset.creationDate ?? Date(timeIntervalSince1970: 0),
                    isBackedUp: state.isBackedUp,
                    mediaKind: state.mediaKind,
                    contentHashes: state.hashes
                )
            }
            .sorted {
                if $0.creationDate != $1.creationDate {
                    return $0.creationDate > $1.creationDate
                }
                return $0.id > $1.id
            }
    }

    func localItemsSnapshot() -> [LocalAlbumItem] {
        let months = localAssetIDsByMonth.keys.sorted(by: >)
        var result: [LocalAlbumItem] = []
        result.reserveCapacity(localStatesByAssetID.count)

        for month in months {
            result.append(contentsOf: localItems(for: month))
        }

        return result
    }

    func localMonthAssetCounts() -> [(month: LibraryMonthKey, count: Int)] {
        localAssetIDsByMonth
            .map { (month: $0.key, count: $0.value.count) }
            .sorted { $0.month > $1.month }
    }

    func localMonthMediaCounts() -> [LibraryMonthKey: (photoCount: Int, videoCount: Int)] {
        var result: [LibraryMonthKey: (photoCount: Int, videoCount: Int)] = [:]
        for (month, assetIDs) in localAssetIDsByMonth {
            var photos = 0, videos = 0
            for id in assetIDs {
                guard let state = localStatesByAssetID[id] else { continue }
                switch state.mediaKind {
                case .photo, .livePhoto: photos += 1
                case .video: videos += 1
                }
            }
            result[month] = (photoCount: photos, videoCount: videos)
        }
        return result
    }

    func localMonthBackedUpCounts() -> [LibraryMonthKey: Int] {
        var result: [LibraryMonthKey: Int] = [:]
        for (month, assetIDs) in localAssetIDsByMonth {
            var count = 0
            for id in assetIDs {
                if localStatesByAssetID[id]?.isBackedUp == true {
                    count += 1
                }
            }
            result[month] = count
        }
        return result
    }

    func localMonthSummary(for month: LibraryMonthKey) -> HomeMonthSummary? {
        guard let assetIDs = localAssetIDsByMonth[month] else { return nil }
        var photos = 0, videos = 0, backedUp = 0
        for id in assetIDs {
            guard let state = localStatesByAssetID[id] else { continue }
            switch state.mediaKind {
            case .photo, .livePhoto: photos += 1
            case .video: videos += 1
            }
            if state.isBackedUp { backedUp += 1 }
        }
        return HomeMonthSummary(
            month: month, assetCount: assetIDs.count,
            photoCount: photos, videoCount: videos,
            backedUpCount: backedUp,
            totalSizeBytes: monthFileSizes[month]
        )
    }

    func localAssetIDs(for month: LibraryMonthKey) -> Set<String> {
        localAssetIDsByMonth[month] ?? []
    }

    func computeFileSize(for month: LibraryMonthKey, cachedSizes: [String: Int64]) -> Int64 {
        guard let assetIDs = localAssetIDsByMonth[month] else { return 0 }
        var total: Int64 = 0
        for assetID in assetIDs {
            if let cached = cachedSizes[assetID], cached > 0 {
                total += cached
            } else {
                guard let state = localStatesByAssetID[assetID] else { continue }
                let resources = PHAssetResource.assetResources(for: state.asset)
                for resource in resources {
                    total += PhotoLibraryService.resourceFileSize(resource)
                }
            }
        }
        return total
    }

    func setMonthFileSize(_ size: Int64, for month: LibraryMonthKey) {
        monthFileSizes[month] = size
    }

    private func removeLocalState(forAssetID assetID: String) -> LibraryMonthKey? {
        guard let previous = localStatesByAssetID.removeValue(forKey: assetID) else { return nil }

        if let previousFingerprint = previous.fingerprint {
            removeAssetID(assetID, fromFingerprint: previousFingerprint)
        }

        if var ids = localAssetIDsByMonth[previous.month] {
            ids.remove(assetID)
            if ids.isEmpty {
                localAssetIDsByMonth[previous.month] = nil
            } else {
                localAssetIDsByMonth[previous.month] = ids
            }
        }

        return previous.month
    }

    private func upsertLocalState(
        asset: PHAsset,
        hashes: [Data],
        fingerprint: Data?,
        remoteFingerprintSet: Set<Data>
    ) -> Set<LibraryMonthKey> {
        let assetID = asset.localIdentifier
        let month = LibraryMonthKey.from(date: asset.creationDate)
        let isBackedUp = fingerprint.map { remoteFingerprintSet.contains($0) } ?? false
        let mediaKind = Self.mediaKind(for: asset)
        let creationDate = asset.creationDate ?? Date(timeIntervalSince1970: 0)
        let creationDateNs = creationDate.nanosecondsSinceEpoch

        let previousFingerprint = localStatesByAssetID[assetID]?.fingerprint
        replaceAssetID(assetID, oldFingerprint: previousFingerprint, newFingerprint: fingerprint)

        if let previous = localStatesByAssetID[assetID],
           previous.month == month,
           previous.creationDateNs == creationDateNs,
           previous.isBackedUp == isBackedUp,
           previous.mediaKind == mediaKind,
           previous.hashes == hashes,
           previous.fingerprint == fingerprint {
            localStatesByAssetID[assetID] = LocalState(
                asset: asset,
                month: month,
                fingerprint: fingerprint,
                hashes: hashes,
                creationDateNs: creationDateNs,
                isBackedUp: isBackedUp,
                mediaKind: mediaKind
            )
            return []
        }

        var changedMonths = Set<LibraryMonthKey>()

        if let previous = localStatesByAssetID[assetID], previous.month != month {
            if var ids = localAssetIDsByMonth[previous.month] {
                ids.remove(assetID)
                if ids.isEmpty {
                    localAssetIDsByMonth[previous.month] = nil
                } else {
                    localAssetIDsByMonth[previous.month] = ids
                }
            }
            changedMonths.insert(previous.month)
        }

        localAssetIDsByMonth[month, default: []].insert(assetID)

        localStatesByAssetID[assetID] = LocalState(
            asset: asset,
            month: month,
            fingerprint: fingerprint,
            hashes: hashes,
            creationDateNs: creationDateNs,
            isBackedUp: isBackedUp,
            mediaKind: mediaKind
        )

        changedMonths.insert(month)
        return changedMonths
    }

    private func replaceAssetID(_ assetID: String, oldFingerprint: Data?, newFingerprint: Data?) {
        if let oldFingerprint, oldFingerprint != newFingerprint {
            removeAssetID(assetID, fromFingerprint: oldFingerprint)
        }

        guard let newFingerprint else { return }
        localAssetIDsByFingerprint[newFingerprint, default: []].insert(assetID)
    }

    private func removeAssetID(_ assetID: String, fromFingerprint fingerprint: Data) {
        guard var ids = localAssetIDsByFingerprint[fingerprint] else { return }
        ids.remove(assetID)
        if ids.isEmpty {
            localAssetIDsByFingerprint[fingerprint] = nil
        } else {
            localAssetIDsByFingerprint[fingerprint] = ids
        }
    }

    private static func mediaKind(for asset: PHAsset) -> AlbumMediaKind {
        if PhotoLibraryService.isLivePhoto(asset) {
            return .livePhoto
        }
        if asset.mediaType == .video {
            return .video
        }
        return .photo
    }

}

private final class HomeRemoteIndexEngine: @unchecked Sendable {
    private var remoteItemsByMonth: [LibraryMonthKey: [RemoteAlbumItem]] = [:]
    private var remoteFingerprintsByMonth: [LibraryMonthKey: Set<Data>] = [:]
    private var remoteFingerprintRefCount: [Data: Int] = [:]
    private var cachedFingerprintSet: Set<Data>?

    private(set) var snapshotRevision: UInt64?

    var allMonths: Set<LibraryMonthKey> {
        Set(remoteItemsByMonth.keys)
    }

    var assetFingerprintSet: Set<Data> {
        if let cached = cachedFingerprintSet { return cached }
        let set = Set(remoteFingerprintRefCount.keys)
        cachedFingerprintSet = set
        return set
    }

    func apply(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) -> HomeRemoteDelta {
        var changedMonths = Set<LibraryMonthKey>()
        var changedFingerprints = Set<Data>()

        guard hasActiveConnection else {
            if !remoteItemsByMonth.isEmpty || !remoteFingerprintRefCount.isEmpty {
                changedMonths.formUnion(remoteItemsByMonth.keys)
                changedFingerprints.formUnion(remoteFingerprintRefCount.keys)
                clearRemoteState()
            }
            snapshotRevision = state.revision
            return HomeRemoteDelta(changedMonths: changedMonths, changedFingerprints: changedFingerprints)
        }

        if snapshotRevision == state.revision, !state.isFullSnapshot {
            return HomeRemoteDelta(changedMonths: changedMonths, changedFingerprints: changedFingerprints)
        }

        if state.isFullSnapshot {
            changedMonths.formUnion(remoteItemsByMonth.keys)
            changedFingerprints.formUnion(remoteFingerprintRefCount.keys)
            clearRemoteState()
        }

        for monthDelta in state.monthDeltas {
            let month = monthDelta.month

            let previousFingerprints = remoteFingerprintsByMonth[month] ?? []
            let previousItems = remoteItemsByMonth[month] ?? []

            let nextFingerprints = Set(monthDelta.assets.map(\.assetFingerprint))

            let items = HomeAlbumMatching.buildRemoteItems(
                assets: monthDelta.assets,
                resources: monthDelta.resources,
                links: monthDelta.assetResourceLinks
            )

            let removedFingerprints = previousFingerprints.subtracting(nextFingerprints)
            let addedFingerprints = nextFingerprints.subtracting(previousFingerprints)

            guard !removedFingerprints.isEmpty || !addedFingerprints.isEmpty
                    || !Self.sameRemoteItems(lhs: previousItems, rhs: items) else {
                continue
            }

            for fingerprint in removedFingerprints {
                let nextCount = max((remoteFingerprintRefCount[fingerprint] ?? 0) - 1, 0)
                if nextCount == 0 {
                    remoteFingerprintRefCount[fingerprint] = nil
                    changedFingerprints.insert(fingerprint)
                } else {
                    remoteFingerprintRefCount[fingerprint] = nextCount
                }
            }

            for fingerprint in addedFingerprints {
                let oldCount = remoteFingerprintRefCount[fingerprint] ?? 0
                remoteFingerprintRefCount[fingerprint] = oldCount + 1
                if oldCount == 0 {
                    changedFingerprints.insert(fingerprint)
                }
            }

            remoteFingerprintsByMonth[month] = nextFingerprints.isEmpty ? nil : nextFingerprints
            remoteItemsByMonth[month] = items.isEmpty ? nil : items

            if !Self.sameRemoteItems(lhs: previousItems, rhs: items) {
                changedMonths.insert(month)
            }
        }

        if !changedFingerprints.isEmpty { cachedFingerprintSet = nil }
        snapshotRevision = state.revision
        return HomeRemoteDelta(changedMonths: changedMonths, changedFingerprints: changedFingerprints)
    }

    func remoteMonthSummary(for month: LibraryMonthKey) -> HomeMonthSummary? {
        guard let items = remoteItemsByMonth[month], !items.isEmpty else { return nil }
        var photos = 0, videos = 0
        var totalSize: Int64 = 0
        for item in items {
            switch item.mediaKind {
            case .photo, .livePhoto: photos += 1
            case .video: videos += 1
            }
            totalSize += item.resources.reduce(Int64(0)) { $0 + $1.fileSize }
        }
        return HomeMonthSummary(
            month: month, assetCount: items.count,
            photoCount: photos, videoCount: videos,
            backedUpCount: nil,
            totalSizeBytes: totalSize
        )
    }

    func remoteItems(for month: LibraryMonthKey) -> [RemoteAlbumItem] {
        remoteItemsByMonth[month] ?? []
    }

    func remoteItemsSnapshot() -> [RemoteAlbumItem] {
        let months = remoteItemsByMonth.keys.sorted(by: >)
        var result: [RemoteAlbumItem] = []
        result.reserveCapacity(remoteItemsByMonth.values.reduce(0) { $0 + $1.count })

        for month in months {
            let items = (remoteItemsByMonth[month] ?? []).sorted {
                if $0.creationDate != $1.creationDate {
                    return $0.creationDate > $1.creationDate
                }
                return $0.id > $1.id
            }
            result.append(contentsOf: items)
        }

        return result
    }

    private func clearRemoteState() {
        remoteItemsByMonth.removeAll()
        remoteFingerprintsByMonth.removeAll()
        remoteFingerprintRefCount.removeAll()
        cachedFingerprintSet = nil
    }

    private static func sameRemoteItems(lhs: [RemoteAlbumItem], rhs: [RemoteAlbumItem]) -> Bool {
        guard lhs.count == rhs.count else { return false }
        for (left, right) in zip(lhs, rhs) {
            if left.id != right.id ||
                left.creationDate != right.creationDate ||
                left.mediaKind != right.mediaKind ||
                left.representative.id != right.representative.id ||
                left.contentHashes != right.contentHashes {
                return false
            }
        }
        return true
    }
}

private final class HomeReconcileEngine: @unchecked Sendable {
    private var mergedByMonth: [LibraryMonthKey: [HomeAlbumItem]] = [:]

    func reconcile(
        changedMonths: Set<LibraryMonthKey>,
        localIndex: HomeLocalIndexEngine,
        remoteIndex: HomeRemoteIndexEngine,
        hasActiveConnection: Bool
    ) -> Set<LibraryMonthKey> {
        guard !changedMonths.isEmpty else { return [] }

        for month in changedMonths {
            let localItems = localIndex.localItems(for: month)
            let remoteItems = remoteIndex.remoteItems(for: month)

            if localItems.isEmpty, remoteItems.isEmpty {
                mergedByMonth[month] = nil
                continue
            }

            let hashIndex = Self.localHashIndex(from: localItems)
            let merged = HomeAlbumMatching.mergeItems(
                localItems: localItems,
                remoteItems: remoteItems,
                localAssetIdentifierByHash: hashIndex,
                hasActiveConnection: hasActiveConnection
            )

            if merged.isEmpty {
                mergedByMonth[month] = nil
            } else {
                mergedByMonth[month] = merged.sorted {
                    if $0.creationDate != $1.creationDate {
                        return $0.creationDate > $1.creationDate
                    }
                    return $0.id > $1.id
                }
            }
        }

        return changedMonths
    }

    func mergedMonthItemsSnapshot() -> [(month: LibraryMonthKey, items: [HomeAlbumItem])] {
        mergedByMonth.keys.sorted(by: >).compactMap { month in
            guard let items = mergedByMonth[month], !items.isEmpty else { return nil }
            return (month: month, items: items)
        }
    }

    func remoteOnlyItems(for month: LibraryMonthKey) -> [RemoteAlbumItem] {
        guard let items = mergedByMonth[month] else { return [] }
        return items
            .filter { $0.sourceTag == .remoteOnly }
            .compactMap(\.remoteItem)
            .reversed()
    }

    func matchedCount(for month: LibraryMonthKey) -> Int {
        guard let items = mergedByMonth[month] else { return 0 }
        return items.count { $0.sourceTag == .both }
    }

    private static func localHashIndex(from localItems: [LocalAlbumItem]) -> [Data: [String]] {
        var hashToAssetSet: [Data: Set<String>] = [:]

        for item in localItems {
            for hash in Set(item.contentHashes) {
                hashToAssetSet[hash, default: []].insert(item.id)
            }
        }

        return hashToAssetSet.mapValues { Array($0) }
    }
}

private struct HomeDataLoadResult {
    let didReload: Bool
    let changedMonths: Set<LibraryMonthKey>
    let isAuthorized: Bool
}

private final class HomeDataProcessingWorker: @unchecked Sendable {
    private let photoLibraryService: PhotoLibraryService
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let processingQueue = DispatchQueue(
        label: "com.zizicici.watermelon.homeData.processing",
        qos: .userInitiated
    )

    private let localIndex = HomeLocalIndexEngine()
    private let remoteIndex = HomeRemoteIndexEngine()
    private let reconcileIndex = HomeReconcileEngine()

    private var hasActiveConnection = false
    private var needsRemoteBootstrap = false
    private var cachedLocalSummaries: [LibraryMonthKey: HomeMonthSummary] = [:]
    private var cachedRemoteSummaries: [LibraryMonthKey: HomeMonthSummary] = [:]

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository
    ) {
        self.photoLibraryService = photoLibraryService
        self.contentHashIndexRepository = contentHashIndexRepository
    }

    func remoteSnapshotRevisionForQuery(hasActiveConnection: Bool) -> UInt64? {
        processingQueue.sync {
            if hasActiveConnection, needsRemoteBootstrap {
                return nil
            }
            return remoteIndex.snapshotRevision
        }
    }

    func loadLocalIndex(forceReload: Bool) async -> HomeDataLoadResult {
        if !forceReload, processingQueue.sync(execute: { localIndex.hasLoadedIndex }) {
            return HomeDataLoadResult(didReload: false, changedMonths: [], isAuthorized: true)
        }

        let status = photoLibraryService.authorizationStatus()
        let authorized: Bool
        if status == .authorized || status == .limited {
            authorized = true
        } else {
            let requested = await photoLibraryService.requestAuthorization()
            authorized = (requested == .authorized || requested == .limited)
        }

        guard authorized else {
            let changedMonths = await withCheckedContinuation { continuation in
                processingQueue.async {
                    let result = self.reconcileLocked(self.localIndex.clearIfNeeded())
                    continuation.resume(returning: result)
                }
            }
            return HomeDataLoadResult(didReload: true, changedMonths: changedMonths, isAuthorized: false)
        }

        let fetchResult = photoLibraryService.fetchAssetsResult()
        let hashMapByAsset = (try? contentHashIndexRepository.fetchHashMapByAsset()) ?? [:]
        let fingerprintByAsset = (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset()) ?? [:]

        let changedMonths = await withCheckedContinuation { continuation in
            processingQueue.async {
                let changed = self.localIndex.reloadAll(
                    fetchResult: fetchResult,
                    hashMapByAsset: hashMapByAsset,
                    fingerprintByAsset: fingerprintByAsset,
                    remoteFingerprintSet: self.remoteIndex.assetFingerprintSet
                )
                let result = self.reconcileLocked(changed)
                continuation.resume(returning: result)
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

                let hashMap = (try? self.contentHashIndexRepository.fetchHashMapByAsset(assetIDs: targetIDs)) ?? [:]
                let fingerprintMap = (try? self.contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: targetIDs)) ?? [:]
                let changedMonths = self.localIndex.refresh(
                    assetIDs: targetIDs,
                    fetchedAssetsByID: fetchedAssetsByID,
                    hashMapByAsset: hashMap,
                    fingerprintByAsset: fingerprintMap,
                    remoteFingerprintSet: self.remoteIndex.assetFingerprintSet
                )
                let result = self.reconcileLocked(changedMonths)
                let elapsed = CFAbsoluteTimeGetCurrent() - start
                dataLog.info("[HomeData] refreshLocalIndex: assets=\(targetIDs.count), inserted=\(fetchedAssetsByID.count), months=\(result.count), \(String(format: "%.3f", elapsed))s")
                continuation.resume(returning: result)
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

                if !remoteDelta.changedFingerprints.isEmpty {
                    changedMonths.formUnion(
                        self.localIndex.refreshBackedUpState(
                            changedFingerprints: remoteDelta.changedFingerprints,
                            remoteFingerprintSet: self.remoteIndex.assetFingerprintSet,
                            limitMonths: remoteDelta.changedMonths
                        )
                    )
                }

                if connectionFlipped {
                    changedMonths.formUnion(localAllMonths)
                    changedMonths.formUnion(remoteAllMonths)
                }

                if hasActiveConnection, state.isFullSnapshot {
                    self.needsRemoteBootstrap = false
                }

                let result = self.reconcileLocked(changedMonths)
                let elapsed = CFAbsoluteTimeGetCurrent() - start
                dataLog.info("[HomeData] processingQueue: months=\(result.count), fingerprints=\(remoteDelta.changedFingerprints.count), \(String(format: "%.3f", elapsed))s")
                continuation.resume(returning: result)
            }
        }
    }

    func applyPhotoLibraryChange(_ changeInstance: PHChange) async -> Set<LibraryMonthKey> {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                let changedMonths = self.localIndex.applyPhotoLibraryChange(
                    changeInstance,
                    fetchHashMapByAsset: { [contentHashIndexRepository = self.contentHashIndexRepository] assetIDs in
                        guard !assetIDs.isEmpty else { return [:] }
                        return (try? contentHashIndexRepository.fetchHashMapByAsset(assetIDs: assetIDs)) ?? [:]
                    },
                    fetchFingerprintByAsset: { [contentHashIndexRepository = self.contentHashIndexRepository] assetIDs in
                        guard !assetIDs.isEmpty else { return [:] }
                        return (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: assetIDs)) ?? [:]
                    },
                    fetchAllHashMapByAsset: { [contentHashIndexRepository = self.contentHashIndexRepository] in
                        (try? contentHashIndexRepository.fetchHashMapByAsset()) ?? [:]
                    },
                    fetchAllFingerprintByAsset: { [contentHashIndexRepository = self.contentHashIndexRepository] in
                        (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset()) ?? [:]
                    },
                    remoteFingerprintSet: self.remoteIndex.assetFingerprintSet
                )
                let result = self.reconcileLocked(changedMonths)
                continuation.resume(returning: result)
            }
        }
    }

    func localMonthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, backedUpCount: Int?, totalSizeBytes: Int64?)] {
        processingQueue.sync {
            let monthCounts = localIndex.localMonthAssetCounts()
            let mediaCounts = localIndex.localMonthMediaCounts()
            let backedUpCounts: [LibraryMonthKey: Int]? = hasActiveConnection ? localIndex.localMonthBackedUpCounts() : nil
            let fileSizes = localIndex.monthFileSizes
            return monthCounts.map { entry in
                let media = mediaCounts[entry.month]
                return (
                    month: entry.month,
                    assetCount: entry.count,
                    photoCount: media?.photoCount ?? 0,
                    videoCount: media?.videoCount ?? 0,
                    backedUpCount: backedUpCounts?[entry.month],
                    totalSizeBytes: fileSizes[entry.month]
                )
            }
        }
    }

    func monthRow(for month: LibraryMonthKey) -> HomeMonthRow {
        processingQueue.sync {
            HomeMonthRow(month: month, local: cachedLocalSummaries[month], remote: cachedRemoteSummaries[month])
        }
    }

    func allMonthRows() -> [LibraryMonthKey: HomeMonthRow] {
        processingQueue.sync {
            let allMonths = Set(cachedLocalSummaries.keys).union(cachedRemoteSummaries.keys)
            var result: [LibraryMonthKey: HomeMonthRow] = [:]
            result.reserveCapacity(allMonths.count)
            for month in allMonths {
                result[month] = HomeMonthRow(month: month, local: cachedLocalSummaries[month], remote: cachedRemoteSummaries[month])
            }
            return result
        }
    }

    func localAssetIDs(for month: LibraryMonthKey) -> Set<String> {
        processingQueue.sync {
            localIndex.localAssetIDs(for: month)
        }
    }

    func remoteOnlyItems(for month: LibraryMonthKey) -> [RemoteAlbumItem] {
        processingQueue.sync {
            reconcileIndex.remoteOnlyItems(for: month)
        }
    }

    func matchedCount(for month: LibraryMonthKey) -> Int {
        processingQueue.sync {
            reconcileIndex.matchedCount(for: month)
        }
    }

    func localMonthsForFileSizeScan() -> [LibraryMonthKey] {
        processingQueue.sync {
            localIndex.localMonthAssetCounts().map(\.month)
        }
    }

    func updateFileSize(for month: LibraryMonthKey, cachedSizes: [String: Int64]) async {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                let size = self.localIndex.computeFileSize(for: month, cachedSizes: cachedSizes)
                self.localIndex.setMonthFileSize(size, for: month)
                self.cachedLocalSummaries[month] = self.localIndex.localMonthSummary(for: month)
                continuation.resume()
            }
        }
    }

    private func reconcileLocked(_ changedMonths: Set<LibraryMonthKey>) -> Set<LibraryMonthKey> {
        let result = reconcileIndex.reconcile(
            changedMonths: changedMonths,
            localIndex: localIndex,
            remoteIndex: remoteIndex,
            hasActiveConnection: hasActiveConnection
        )
        if !result.isEmpty {
            updateCachedSummaries(for: result)
        }
        return result
    }

    private func updateCachedSummaries(for months: Set<LibraryMonthKey>) {
        for month in months {
            cachedLocalSummaries[month] = localIndex.localMonthSummary(for: month)
            cachedRemoteSummaries[month] = hasActiveConnection ? remoteIndex.remoteMonthSummary(for: month) : nil
        }
    }
}

@MainActor
final class HomeIncrementalDataManager: NSObject, PHPhotoLibraryChangeObserver {
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let processingWorker: HomeDataProcessingWorker

    private var isObservingPhotoLibrary = false
    private var processingMutationCount = 0
    private var deferredPhotoChanges: [PHChange] = []
    private var isDrainingDeferredPhotoChanges = false

    var onMonthsChanged: ((Set<LibraryMonthKey>) -> Void)?
    var onFileSizesUpdated: ((Set<LibraryMonthKey>) -> Void)?
    private var fileSizeScanTask: Task<Void, Never>?

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository
    ) {
        self.contentHashIndexRepository = contentHashIndexRepository
        self.processingWorker = HomeDataProcessingWorker(
            photoLibraryService: photoLibraryService,
            contentHashIndexRepository: contentHashIndexRepository
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

    func localMonthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, backedUpCount: Int?, totalSizeBytes: Int64?)] {
        processingWorker.localMonthSummaries()
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

    func remoteOnlyItems(for month: LibraryMonthKey) -> [RemoteAlbumItem] {
        processingWorker.remoteOnlyItems(for: month)
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
        scanFileSizes(months: Array(months), tracked: false)
    }

    private func startFileSizeScan() {
        fileSizeScanTask?.cancel()
        let months = processingWorker.localMonthsForFileSizeScan()
        scanFileSizes(months: months, tracked: true)
    }

    private func scanFileSizes(months: [LibraryMonthKey], tracked: Bool) {
        let cachedSizes = (try? contentHashIndexRepository.fetchFileSizeByAsset()) ?? [:]
        let task = Task { [weak self] in
            for month in months {
                guard let self, !Task.isCancelled else { return }
                await self.processingWorker.updateFileSize(for: month, cachedSizes: cachedSizes)
                guard !Task.isCancelled else { return }
                self.onFileSizesUpdated?([month])
                await Task.yield()
            }
        }
        if tracked { fileSizeScanTask = task }
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

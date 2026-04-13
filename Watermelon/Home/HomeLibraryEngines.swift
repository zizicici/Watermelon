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
        let month: LibraryMonthKey
        let fingerprint: Data?
        let hashes: [Data]
        let creationDateNs: Int64
        let isBackedUp: Bool
        let mediaKind: AlbumMediaKind
    }

    private static let fullReloadChunkSize = 400

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
            let fingerprint = fingerprintByAsset[assetID]
            let hashes = fingerprint == nil ? (hashMapByAsset[assetID] ?? []) : []
            let month = LibraryMonthKey.from(date: asset.creationDate)
            let creationDate = asset.creationDate ?? Date(timeIntervalSince1970: 0)

            newStates[assetID] = LocalState(
                month: month,
                fingerprint: fingerprint,
                hashes: hashes,
                creationDateNs: creationDate.nanosecondsSinceEpoch,
                isBackedUp: fingerprint.map { remoteFingerprintSet.contains($0) } ?? false,
                mediaKind: Self.mediaKind(for: asset)
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
        monthFileSizes = monthFileSizes.filter { newAssetIDsByMonth[$0.key] != nil && !changedMonths.contains($0.key) }

        return changedMonths
    }

    func reloadAllChunked(
        fetchResult: PHFetchResult<PHAsset>,
        fetchHashMapByAsset: (Set<String>) -> [String: [Data]],
        fetchFingerprintByAsset: (Set<String>) -> [String: Data],
        remoteFingerprintSet: Set<Data>
    ) -> Set<LibraryMonthKey> {
        var newStates: [String: LocalState] = [:]
        newStates.reserveCapacity(fetchResult.count)

        var newAssetIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
        newAssetIDsByMonth.reserveCapacity(24)

        var newAssetIDsByFingerprint: [Data: Set<String>] = [:]

        for chunkStart in stride(from: 0, to: fetchResult.count, by: Self.fullReloadChunkSize) {
            let chunkEnd = min(chunkStart + Self.fullReloadChunkSize, fetchResult.count)
            var chunkAssets: [PHAsset] = []
            chunkAssets.reserveCapacity(chunkEnd - chunkStart)
            var chunkAssetIDs = Set<String>()
            chunkAssetIDs.reserveCapacity(chunkEnd - chunkStart)

            for index in chunkStart ..< chunkEnd {
                let asset = fetchResult.object(at: index)
                chunkAssets.append(asset)
                chunkAssetIDs.insert(asset.localIdentifier)
            }

            let fingerprintByAsset = fetchFingerprintByAsset(chunkAssetIDs)
            let hashTargetIDs = Set(chunkAssetIDs.filter { fingerprintByAsset[$0] == nil })
            let hashMapByAsset = fetchHashMapByAsset(hashTargetIDs)

            for asset in chunkAssets {
                let assetID = asset.localIdentifier
                let fingerprint = fingerprintByAsset[assetID]
                let hashes = fingerprint == nil ? (hashMapByAsset[assetID] ?? []) : []
                let month = LibraryMonthKey.from(date: asset.creationDate)
                let creationDateNs = (asset.creationDate ?? Date(timeIntervalSince1970: 0)).nanosecondsSinceEpoch

                newStates[assetID] = LocalState(
                    month: month,
                    fingerprint: fingerprint,
                    hashes: hashes,
                    creationDateNs: creationDateNs,
                    isBackedUp: fingerprint.map { remoteFingerprintSet.contains($0) } ?? false,
                    mediaKind: Self.mediaKind(for: asset)
                )
                newAssetIDsByMonth[month, default: []].insert(assetID)

                if let fingerprint {
                    newAssetIDsByFingerprint[fingerprint, default: []].insert(assetID)
                }
            }
        }

        let changedMonths = allMonths.union(newAssetIDsByMonth.keys)

        localFetchResult = fetchResult
        localStatesByAssetID = newStates
        localAssetIDsByMonth = newAssetIDsByMonth
        localAssetIDsByFingerprint = newAssetIDsByFingerprint
        monthFileSizes = monthFileSizes.filter { newAssetIDsByMonth[$0.key] != nil && !changedMonths.contains($0.key) }

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
        monthFileSizes.removeAll()

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
            let fingerprint = fingerprintByAsset[assetID]
            let hashes = fingerprint == nil ? (hashMapByAsset[assetID] ?? []) : []
            changedMonths.formUnion(
                upsertLocalState(
                    assetID: assetID,
                    month: current.month,
                    creationDateNs: current.creationDateNs,
                    mediaKind: current.mediaKind,
                    hashes: hashes,
                    fingerprint: fingerprint,
                    remoteFingerprintSet: remoteFingerprintSet
                )
            )
        }

        for assetID in insertedIDs {
            guard let asset = fetchedAssetsByID[assetID] else { continue }
            let fingerprint = fingerprintByAsset[assetID]
            let hashes = fingerprint == nil ? (hashMapByAsset[assetID] ?? []) : []
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
        remoteFingerprintSet: Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard let currentFetchResult = localFetchResult,
              let details = changeInstance.changeDetails(for: currentFetchResult) else {
            return []
        }

        let nextFetchResult = details.fetchResultAfterChanges
        localFetchResult = nextFetchResult

        guard details.hasIncrementalChanges else {
            return reloadAllChunked(
                fetchResult: nextFetchResult,
                fetchHashMapByAsset: fetchHashMapByAsset,
                fetchFingerprintByAsset: fetchFingerprintByAsset,
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
        let fingerprintMap = fetchFingerprintByAsset(upsertAssetIDs)
        let hashTargetIDs = Set(upsertAssetIDs.filter { fingerprintMap[$0] == nil })
        let hashMap = fetchHashMapByAsset(hashTargetIDs)

        for asset in upsertAssetsByID.values {
            let fingerprint = fingerprintMap[asset.localIdentifier]
            let hashes = fingerprint == nil ? (hashMap[asset.localIdentifier] ?? []) : []
            changedMonths.formUnion(
                upsertLocalState(
                    asset: asset,
                    hashes: hashes,
                    fingerprint: fingerprint,
                    remoteFingerprintSet: remoteFingerprintSet
                )
            )
        }

        invalidateMonthFileSizes(for: changedMonths)
        return changedMonths
    }

    func refreshBackedUpState(
        changedFingerprints: Set<Data>,
        remoteFingerprintSet: Set<Data>
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
            changedMonths.formUnion(
                upsertLocalState(
                    assetID: assetID,
                    month: state.month,
                    creationDateNs: state.creationDateNs,
                    mediaKind: state.mediaKind,
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
                    creationDate: Date(nanosecondsSinceEpoch: state.creationDateNs),
                    isBackedUp: state.isBackedUp,
                    mediaKind: state.mediaKind,
                    contentHashes: state.hashes,
                    fingerprint: state.fingerprint
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

    func setMonthFileSize(_ size: Int64, for month: LibraryMonthKey) {
        monthFileSizes[month] = size
    }

    func invalidateMonthFileSizes(for months: Set<LibraryMonthKey>) {
        guard !months.isEmpty else { return }
        for month in months {
            monthFileSizes[month] = nil
        }
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
        upsertLocalState(
            assetID: asset.localIdentifier,
            month: LibraryMonthKey.from(date: asset.creationDate),
            creationDateNs: (asset.creationDate ?? Date(timeIntervalSince1970: 0)).nanosecondsSinceEpoch,
            mediaKind: Self.mediaKind(for: asset),
            hashes: hashes,
            fingerprint: fingerprint,
            remoteFingerprintSet: remoteFingerprintSet
        )
    }

    private func upsertLocalState(
        assetID: String,
        month: LibraryMonthKey,
        creationDateNs: Int64,
        mediaKind: AlbumMediaKind,
        hashes: [Data],
        fingerprint: Data?,
        remoteFingerprintSet: Set<Data>
    ) -> Set<LibraryMonthKey> {
        let isBackedUp = fingerprint.map { remoteFingerprintSet.contains($0) } ?? false
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
    private var remoteSummariesByMonth: [LibraryMonthKey: HomeMonthSummary] = [:]
    private var remoteFingerprintsByMonth: [LibraryMonthKey: Set<Data>] = [:]
    private var remoteFingerprintRefCount: [Data: Int] = [:]
    private var cachedFingerprintSet: Set<Data>?

    private(set) var snapshotRevision: UInt64?

    var allMonths: Set<LibraryMonthKey> {
        Set(remoteSummariesByMonth.keys).union(remoteFingerprintsByMonth.keys)
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
            if !remoteSummariesByMonth.isEmpty || !remoteFingerprintRefCount.isEmpty {
                changedMonths.formUnion(allMonths)
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
            changedMonths.formUnion(allMonths)
            changedFingerprints.formUnion(remoteFingerprintRefCount.keys)
            clearRemoteState()
        }

        for monthDelta in state.monthDeltas {
            let month = monthDelta.month

            let previousFingerprints = remoteFingerprintsByMonth[month] ?? []
            let previousSummary = remoteSummariesByMonth[month]

            let nextFingerprints = Set(monthDelta.assets.map(\.assetFingerprint))
            let nextSummary = Self.remoteMonthSummary(from: monthDelta)

            let removedFingerprints = previousFingerprints.subtracting(nextFingerprints)
            let addedFingerprints = nextFingerprints.subtracting(previousFingerprints)
            let summaryChanged = !Self.sameSummary(lhs: previousSummary, rhs: nextSummary)

            guard !removedFingerprints.isEmpty || !addedFingerprints.isEmpty
                    || summaryChanged else {
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
            remoteSummariesByMonth[month] = nextSummary

            if summaryChanged || !removedFingerprints.isEmpty || !addedFingerprints.isEmpty {
                changedMonths.insert(month)
            }
        }

        if !changedFingerprints.isEmpty { cachedFingerprintSet = nil }
        snapshotRevision = state.revision
        return HomeRemoteDelta(changedMonths: changedMonths, changedFingerprints: changedFingerprints)
    }

    func remoteMonthSummary(for month: LibraryMonthKey) -> HomeMonthSummary? {
        remoteSummariesByMonth[month]
    }

    func hasRemoteAssets(for month: LibraryMonthKey) -> Bool {
        (remoteSummariesByMonth[month]?.assetCount ?? 0) > 0
    }

    private func clearRemoteState() {
        remoteSummariesByMonth.removeAll()
        remoteFingerprintsByMonth.removeAll()
        remoteFingerprintRefCount.removeAll()
        cachedFingerprintSet = nil
    }

    private static func remoteMonthSummary(from monthDelta: RemoteLibraryMonthDelta) -> HomeMonthSummary? {
        guard !monthDelta.assets.isEmpty else { return nil }

        var rolesByAssetID: [String: [Int]] = [:]
        rolesByAssetID.reserveCapacity(monthDelta.assets.count)
        for link in monthDelta.assetResourceLinks {
            rolesByAssetID[link.assetID, default: []].append(link.role)
        }

        var videoCount = 0
        for asset in monthDelta.assets {
            let roles = rolesByAssetID[asset.id] ?? []
            let hasPairedVideo = roles.contains { ResourceTypeCode.isPairedVideo($0) }
            let hasPhotoLike = roles.contains { ResourceTypeCode.isPhotoLike($0) }
            if hasPairedVideo, hasPhotoLike { continue }
            if roles.contains(where: { ResourceTypeCode.isVideoLike($0) }) {
                videoCount += 1
            }
        }

        let totalSizeBytes = monthDelta.assets.reduce(Int64(0)) { $0 + $1.totalFileSizeBytes }
        return HomeMonthSummary(
            month: monthDelta.month,
            assetCount: monthDelta.assets.count,
            photoCount: monthDelta.assets.count - videoCount,
            videoCount: videoCount,
            backedUpCount: nil,
            totalSizeBytes: totalSizeBytes
        )
    }

    private static func sameSummary(lhs: HomeMonthSummary?, rhs: HomeMonthSummary?) -> Bool {
        switch (lhs, rhs) {
        case (nil, nil):
            return true
        case (.some(let left), .some(let right)):
            return left.month == right.month &&
                left.assetCount == right.assetCount &&
                left.photoCount == right.photoCount &&
                left.videoCount == right.videoCount &&
                left.totalSizeBytes == right.totalSizeBytes
        default:
            return false
        }
    }
}

private final class HomeReconcileEngine: @unchecked Sendable {
    private struct MonthMatchState {
        let matchedCount: Int
    }

    private var monthMatchStateByMonth: [LibraryMonthKey: MonthMatchState] = [:]

    func reconcile(
        changedMonths: Set<LibraryMonthKey>,
        localIndex: HomeLocalIndexEngine,
        remoteIndex: HomeRemoteIndexEngine,
        hasActiveConnection: Bool,
        loadRemoteItems: (LibraryMonthKey) -> [RemoteAlbumItem]
    ) -> Set<LibraryMonthKey> {
        guard !changedMonths.isEmpty else { return [] }

        guard hasActiveConnection else {
            for month in changedMonths {
                monthMatchStateByMonth[month] = nil
            }
            return changedMonths
        }

        for month in changedMonths {
            let localItems = localIndex.localItems(for: month)
            guard !localItems.isEmpty || remoteIndex.hasRemoteAssets(for: month) else {
                monthMatchStateByMonth[month] = nil
                continue
            }

            let remoteItems = loadRemoteItems(month)
            if localItems.isEmpty, remoteItems.isEmpty {
                monthMatchStateByMonth[month] = nil
                continue
            }

            let hashIndex = Self.localHashIndex(from: localItems)
            let match = HomeAlbumMatching.computeMatch(
                localItems: localItems,
                remoteItems: remoteItems,
                localAssetIdentifierByHash: hashIndex,
                hasActiveConnection: hasActiveConnection,
                includeRemoteOnlyItems: false
            )

            if match.matchedCount == 0 {
                monthMatchStateByMonth[month] = nil
            } else {
                monthMatchStateByMonth[month] = MonthMatchState(matchedCount: match.matchedCount)
            }
        }

        return changedMonths
    }

    func remoteOnlyItems(
        for month: LibraryMonthKey,
        localIndex: HomeLocalIndexEngine,
        remoteIndex: HomeRemoteIndexEngine,
        hasActiveConnection: Bool,
        loadRemoteItems: (LibraryMonthKey) -> [RemoteAlbumItem]
    ) -> [RemoteAlbumItem] {
        guard hasActiveConnection else { return [] }
        guard remoteIndex.hasRemoteAssets(for: month) else { return [] }

        let localItems = localIndex.localItems(for: month)
        let remoteItems = loadRemoteItems(month)
        guard !remoteItems.isEmpty else { return [] }
        let hashIndex = Self.localHashIndex(from: localItems)
        let match = HomeAlbumMatching.computeMatch(
            localItems: localItems,
            remoteItems: remoteItems,
            localAssetIdentifierByHash: hashIndex,
            hasActiveConnection: hasActiveConnection,
            includeRemoteOnlyItems: true
        )

        return match.remoteOnlyItems.sorted {
            if $0.creationDate != $1.creationDate {
                return $0.creationDate < $1.creationDate
            }
            return $0.id < $1.id
        }
    }

    func matchedCount(for month: LibraryMonthKey) -> Int {
        monthMatchStateByMonth[month]?.matchedCount ?? 0
    }

    private static func localHashIndex(from localItems: [LocalAlbumItem]) -> [Data: [String]] {
        var hashToAssetSet: [Data: Set<String>] = [:]

        for item in localItems where item.fingerprint == nil && !item.contentHashes.isEmpty {
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
    private let loadRemoteMonthDelta: @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
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
        contentHashIndexRepository: ContentHashIndexRepository,
        loadRemoteMonthDelta: @escaping @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
    ) {
        self.photoLibraryService = photoLibraryService
        self.contentHashIndexRepository = contentHashIndexRepository
        self.loadRemoteMonthDelta = loadRemoteMonthDelta
    }

    private func buildRemoteItems(from monthDelta: RemoteLibraryMonthDelta) -> [RemoteAlbumItem] {
        return HomeAlbumMatching.buildRemoteItems(
            assets: monthDelta.assets,
            resources: monthDelta.resources,
            links: monthDelta.assetResourceLinks
        )
    }

    private func loadRemoteItems(
        for month: LibraryMonthKey,
        monthDeltaOverrides: [LibraryMonthKey: RemoteLibraryMonthDelta] = [:]
    ) -> [RemoteAlbumItem] {
        if let monthDelta = monthDeltaOverrides[month] {
            return buildRemoteItems(from: monthDelta)
        }
        guard let monthDelta = loadRemoteMonthDelta(month) else { return [] }
        return buildRemoteItems(from: monthDelta)
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

        let changedMonths = await withCheckedContinuation { continuation in
            processingQueue.async {
                let changed = self.localIndex.reloadAllChunked(
                    fetchResult: fetchResult,
                    fetchHashMapByAsset: { [contentHashIndexRepository = self.contentHashIndexRepository] assetIDs in
                        guard !assetIDs.isEmpty else { return [:] }
                        return (try? contentHashIndexRepository.fetchHashMapByAsset(assetIDs: assetIDs)) ?? [:]
                    },
                    fetchFingerprintByAsset: { [contentHashIndexRepository = self.contentHashIndexRepository] assetIDs in
                        guard !assetIDs.isEmpty else { return [:] }
                        return (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: assetIDs)) ?? [:]
                    },
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

                let fingerprintMap = (try? self.contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: targetIDs)) ?? [:]
                let hashTargetIDs = Set(targetIDs.filter { fingerprintMap[$0] == nil })
                let hashMap = (try? self.contentHashIndexRepository.fetchHashMapByAsset(assetIDs: hashTargetIDs)) ?? [:]
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
                let remoteMonthDeltaOverrides = Dictionary(
                    uniqueKeysWithValues: state.monthDeltas.map { ($0.month, $0) }
                )
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
                            remoteFingerprintSet: self.remoteIndex.assetFingerprintSet
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

                let result = self.reconcileLocked(
                    changedMonths,
                    remoteMonthDeltaOverrides: remoteMonthDeltaOverrides
                )
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
            reconcileIndex.remoteOnlyItems(
                for: month,
                localIndex: localIndex,
                remoteIndex: remoteIndex,
                hasActiveConnection: hasActiveConnection,
                loadRemoteItems: { month in
                    self.loadRemoteItems(for: month)
                }
            )
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

    func updateFileSize(for month: LibraryMonthKey) async {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                let assetIDs = self.localIndex.localAssetIDs(for: month)
                let cachedSizes = (try? self.contentHashIndexRepository.fetchFileSizeByAsset(assetIDs: assetIDs)) ?? [:]
                var size = cachedSizes.values.reduce(Int64(0), +)

                let missingIDs = assetIDs.subtracting(Set(cachedSizes.keys))
                if !missingIDs.isEmpty {
                    let assets = self.photoLibraryService.fetchAssets(localIdentifiers: missingIDs)
                    for asset in assets {
                        let resources = PHAssetResource.assetResources(for: asset)
                        for resource in resources {
                            size += PhotoLibraryService.resourceFileSize(resource)
                        }
                    }
                }

                self.localIndex.setMonthFileSize(size, for: month)
                self.cachedLocalSummaries[month] = self.localIndex.localMonthSummary(for: month)
                continuation.resume()
            }
        }
    }

    private func reconcileLocked(
        _ changedMonths: Set<LibraryMonthKey>,
        remoteMonthDeltaOverrides: [LibraryMonthKey: RemoteLibraryMonthDelta] = [:]
    ) -> Set<LibraryMonthKey> {
        let result = reconcileIndex.reconcile(
            changedMonths: changedMonths,
            localIndex: localIndex,
            remoteIndex: remoteIndex,
            hasActiveConnection: hasActiveConnection,
            loadRemoteItems: { [remoteMonthDeltaOverrides] month in
                self.loadRemoteItems(for: month, monthDeltaOverrides: remoteMonthDeltaOverrides)
            }
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
        contentHashIndexRepository: ContentHashIndexRepository,
        loadRemoteMonthDelta: @escaping @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
    ) {
        self.processingWorker = HomeDataProcessingWorker(
            photoLibraryService: photoLibraryService,
            contentHashIndexRepository: contentHashIndexRepository,
            loadRemoteMonthDelta: loadRemoteMonthDelta
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
        let task = Task { [weak self] in
            for month in months {
                guard let self, !Task.isCancelled else { return }
                await self.processingWorker.updateFileSize(for: month)
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

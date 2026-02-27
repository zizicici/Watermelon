import Foundation
import Photos

private struct HomeRemoteDelta {
    let changedMonths: Set<LibraryMonthKey>
    let changedFingerprints: Set<Data>
}

private final class HomeLocalIndexEngine {
    private struct LocalSignature: Equatable {
        let creationDateNs: Int64
        let isBackedUp: Bool
        let mediaKind: AlbumMediaKind
        let contentHashes: [Data]
    }

    private struct LocalState {
        let asset: PHAsset
        let month: LibraryMonthKey
        let fingerprint: Data?
        let hashes: [Data]
        let item: LocalAlbumItem
        let signature: LocalSignature
    }

    private let calendar = Calendar(identifier: .gregorian)

    private(set) var localFetchResult: PHFetchResult<PHAsset>?
    private var localStatesByAssetID: [String: LocalState] = [:]
    private var localAssetIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
    private var localHashesByAssetID: [String: [Data]] = [:]
    private var localFingerprintByAssetID: [String: Data] = [:]
    private var localAssetIDsByFingerprint: [Data: Set<String>] = [:]

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
            let month = monthKey(for: asset.creationDate)
            let isBackedUp = fingerprint.map { remoteFingerprintSet.contains($0) } ?? false
            let mediaKind = Self.mediaKind(for: asset)

            let item = LocalAlbumItem(
                id: assetID,
                asset: asset,
                creationDate: asset.creationDate ?? Date(timeIntervalSince1970: 0),
                isBackedUp: isBackedUp,
                mediaKind: mediaKind,
                contentHashes: hashes
            )

            let signature = LocalSignature(
                creationDateNs: Self.nanosecondsSinceEpoch(item.creationDate),
                isBackedUp: isBackedUp,
                mediaKind: mediaKind,
                contentHashes: hashes
            )

            newStates[assetID] = LocalState(
                asset: asset,
                month: month,
                fingerprint: fingerprint,
                hashes: hashes,
                item: item,
                signature: signature
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
        localHashesByAssetID = hashMapByAsset
        localFingerprintByAssetID = fingerprintByAsset
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
        localHashesByAssetID.removeAll()
        localFingerprintByAssetID.removeAll()
        localAssetIDsByFingerprint.removeAll()

        return changedMonths
    }

    func refresh(
        assetIDs: Set<String>,
        hashMapByAsset: [String: [Data]],
        fingerprintByAsset: [String: Data],
        remoteFingerprintSet: Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty else { return [] }
        guard !localStatesByAssetID.isEmpty else { return [] }

        let existingIDs = knownAssetIDs(in: assetIDs)
        guard !existingIDs.isEmpty else { return [] }

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
            .compactMap { localStatesByAssetID[$0]?.item }
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

    private func removeLocalState(forAssetID assetID: String) -> LibraryMonthKey? {
        guard let previous = localStatesByAssetID.removeValue(forKey: assetID) else { return nil }

        localHashesByAssetID[assetID] = nil

        if let previousFingerprint = localFingerprintByAssetID.removeValue(forKey: assetID) {
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
        let month = monthKey(for: asset.creationDate)
        let isBackedUp = fingerprint.map { remoteFingerprintSet.contains($0) } ?? false
        let mediaKind = Self.mediaKind(for: asset)
        let creationDate = asset.creationDate ?? Date(timeIntervalSince1970: 0)

        let item = LocalAlbumItem(
            id: assetID,
            asset: asset,
            creationDate: creationDate,
            isBackedUp: isBackedUp,
            mediaKind: mediaKind,
            contentHashes: hashes
        )

        let signature = LocalSignature(
            creationDateNs: Self.nanosecondsSinceEpoch(creationDate),
            isBackedUp: isBackedUp,
            mediaKind: mediaKind,
            contentHashes: hashes
        )

        let previousFingerprint = localFingerprintByAssetID[assetID]
        localHashesByAssetID[assetID] = hashes
        localFingerprintByAssetID[assetID] = fingerprint
        replaceAssetID(assetID, oldFingerprint: previousFingerprint, newFingerprint: fingerprint)

        if let previous = localStatesByAssetID[assetID],
           previous.month == month,
           previous.signature == signature,
           previous.fingerprint == fingerprint {
            localStatesByAssetID[assetID] = LocalState(
                asset: asset,
                month: month,
                fingerprint: fingerprint,
                hashes: hashes,
                item: item,
                signature: signature
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
            item: item,
            signature: signature
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

    private func monthKey(for date: Date?) -> LibraryMonthKey {
        let actualDate = date ?? Date(timeIntervalSince1970: 0)
        let comps = calendar.dateComponents([.year, .month], from: actualDate)
        return LibraryMonthKey(year: comps.year ?? 1970, month: comps.month ?? 1)
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

    private static func nanosecondsSinceEpoch(_ date: Date) -> Int64 {
        Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())
    }
}

private final class HomeRemoteIndexEngine {
    private struct RemoteLinkKey: Hashable {
        let assetFingerprint: Data
        let role: Int
        let slot: Int
    }

    private var remoteAssetsByMonth: [LibraryMonthKey: [String: RemoteManifestAsset]] = [:]
    private var remoteResourcesByMonth: [LibraryMonthKey: [String: RemoteManifestResource]] = [:]
    private var remoteLinksByMonth: [LibraryMonthKey: [RemoteLinkKey: RemoteAssetResourceLink]] = [:]
    private var remoteItemsByMonth: [LibraryMonthKey: [RemoteAlbumItem]] = [:]
    private var remoteFingerprintRefCount: [Data: Int] = [:]

    private(set) var snapshotRevision: UInt64?

    var allMonths: Set<LibraryMonthKey> {
        Set(remoteItemsByMonth.keys)
    }

    var assetFingerprintSet: Set<Data> {
        Set(remoteFingerprintRefCount.keys)
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

            let previousAssetMap = remoteAssetsByMonth[month] ?? [:]
            let previousResourceMap = remoteResourcesByMonth[month] ?? [:]
            let previousLinkMap = remoteLinksByMonth[month] ?? [:]
            let previousItems = remoteItemsByMonth[month] ?? []

            let nextAssetMap = Dictionary(uniqueKeysWithValues: monthDelta.assets.map { ($0.id, $0) })
            let nextResourceMap = Dictionary(uniqueKeysWithValues: monthDelta.resources.map { ($0.id, $0) })
            let nextLinkMap = Dictionary(uniqueKeysWithValues: monthDelta.assetResourceLinks.map { link in
                (
                    RemoteLinkKey(assetFingerprint: link.assetFingerprint, role: link.role, slot: link.slot),
                    link
                )
            })

            if previousAssetMap == nextAssetMap,
               previousResourceMap == nextResourceMap,
               previousLinkMap == nextLinkMap {
                continue
            }

            let previousFingerprints = Set(previousAssetMap.values.map(\.assetFingerprint))
            let nextFingerprints = Set(nextAssetMap.values.map(\.assetFingerprint))
            let removedFingerprints = previousFingerprints.subtracting(nextFingerprints)
            let addedFingerprints = nextFingerprints.subtracting(previousFingerprints)

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

            remoteAssetsByMonth[month] = nextAssetMap.isEmpty ? nil : nextAssetMap
            remoteResourcesByMonth[month] = nextResourceMap.isEmpty ? nil : nextResourceMap
            remoteLinksByMonth[month] = nextLinkMap.isEmpty ? nil : nextLinkMap

            let items = HomeAlbumMatching.buildRemoteItems(
                assets: monthDelta.assets,
                resources: monthDelta.resources,
                links: monthDelta.assetResourceLinks
            )
            remoteItemsByMonth[month] = items.isEmpty ? nil : items

            if !Self.sameRemoteItems(lhs: previousItems, rhs: items) {
                changedMonths.insert(month)
            }
        }

        snapshotRevision = state.revision
        return HomeRemoteDelta(changedMonths: changedMonths, changedFingerprints: changedFingerprints)
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
        remoteAssetsByMonth.removeAll()
        remoteResourcesByMonth.removeAll()
        remoteLinksByMonth.removeAll()
        remoteItemsByMonth.removeAll()
        remoteFingerprintRefCount.removeAll()
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

private final class HomeReconcileEngine {
    private var mergedByMonth: [LibraryMonthKey: [HomeAlbumItem]] = [:]

    func reconcile(
        changedMonths: Set<LibraryMonthKey>,
        localIndex: HomeLocalIndexEngine,
        remoteIndex: HomeRemoteIndexEngine,
        hasActiveConnection: Bool
    ) -> Bool {
        guard !changedMonths.isEmpty else { return false }

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

        return true
    }

    func mergedMonthItemsSnapshot() -> [(month: LibraryMonthKey, items: [HomeAlbumItem])] {
        mergedByMonth.keys.sorted(by: >).compactMap { month in
            guard let items = mergedByMonth[month], !items.isEmpty else { return nil }
            return (month: month, items: items)
        }
    }

    private static func localHashIndex(from localItems: [LocalAlbumItem]) -> [Data: [String]] {
        var hashToAssetSet: [Data: Set<String>] = [:]

        for item in localItems {
            for hash in Set(item.contentHashes) {
                hashToAssetSet[hash, default: []].insert(item.id)
            }
        }

        return hashToAssetSet.mapValues { Array($0).sorted() }
    }
}

@MainActor
final class HomeIncrementalDataManager: NSObject, PHPhotoLibraryChangeObserver {
    struct MergedMonthSection {
        let month: LibraryMonthKey
        let items: [HomeAlbumItem]
    }

    private let photoLibraryService: PhotoLibraryService
    private let contentHashIndexRepository: ContentHashIndexRepository

    private let localIndex = HomeLocalIndexEngine()
    private let remoteIndex = HomeRemoteIndexEngine()
    private let reconcileIndex = HomeReconcileEngine()

    private var isObservingPhotoLibrary = false
    private var hasActiveConnection = false
    private var needsRemoteBootstrap = false

    var onDataChanged: (() -> Void)?

    func remoteSnapshotRevisionForQuery(hasActiveConnection: Bool) -> UInt64? {
        if hasActiveConnection, needsRemoteBootstrap {
            return nil
        }
        return remoteIndex.snapshotRevision
    }

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository
    ) {
        self.photoLibraryService = photoLibraryService
        self.contentHashIndexRepository = contentHashIndexRepository
    }

    func invalidate() {
        unregisterPhotoLibraryObserverIfNeeded()
        onDataChanged = nil
    }

    @discardableResult
    func ensureLocalIndexLoaded() async -> Bool {
        if localIndex.hasLoadedIndex {
            registerPhotoLibraryObserverIfNeeded()
            return false
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
            unregisterPhotoLibraryObserverIfNeeded()
            return reconcileIfNeeded(localIndex.clearIfNeeded())
        }

        let fetchResult = photoLibraryService.fetchAssetsResult()
        let hashMapByAsset = (try? contentHashIndexRepository.fetchHashMapByAsset()) ?? [:]
        let fingerprintByAsset = (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset()) ?? [:]

        let changedMonths = localIndex.reloadAll(
            fetchResult: fetchResult,
            hashMapByAsset: hashMapByAsset,
            fingerprintByAsset: fingerprintByAsset,
            remoteFingerprintSet: remoteIndex.assetFingerprintSet
        )

        registerPhotoLibraryObserverIfNeeded()
        return reconcileIfNeeded(changedMonths)
    }

    @discardableResult
    func refreshLocalIndex(forAssetIDs assetIDs: Set<String>) -> Bool {
        guard !assetIDs.isEmpty else { return false }
        let existingIDs = localIndex.knownAssetIDs(in: assetIDs)
        guard !existingIDs.isEmpty else { return false }

        let hashMap = (try? contentHashIndexRepository.fetchHashMapByAsset(assetIDs: existingIDs)) ?? [:]
        let fingerprintMap = (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: existingIDs)) ?? [:]

        let changedMonths = localIndex.refresh(
            assetIDs: existingIDs,
            hashMapByAsset: hashMap,
            fingerprintByAsset: fingerprintMap,
            remoteFingerprintSet: remoteIndex.assetFingerprintSet
        )
        return reconcileIfNeeded(changedMonths)
    }

    @discardableResult
    func syncRemoteSnapshot(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) -> Bool {
        var changedMonths = Set<LibraryMonthKey>()

        if self.hasActiveConnection != hasActiveConnection {
            self.hasActiveConnection = hasActiveConnection
            if !hasActiveConnection {
                needsRemoteBootstrap = true
            }
            changedMonths.formUnion(localIndex.allMonths)
            changedMonths.formUnion(remoteIndex.allMonths)
        }

        let remoteDelta = remoteIndex.apply(state: state, hasActiveConnection: hasActiveConnection)
        changedMonths.formUnion(remoteDelta.changedMonths)

        if !remoteDelta.changedFingerprints.isEmpty {
            changedMonths.formUnion(
                localIndex.refreshBackedUpState(
                    changedFingerprints: remoteDelta.changedFingerprints,
                    remoteFingerprintSet: remoteIndex.assetFingerprintSet,
                    limitMonths: remoteDelta.changedMonths
                )
            )
        }

        if hasActiveConnection, state.isFullSnapshot {
            needsRemoteBootstrap = false
        }

        return reconcileIfNeeded(changedMonths)
    }

    func localItemsSnapshot() -> [LocalAlbumItem] {
        localIndex.localItemsSnapshot()
    }

    func remoteItemsSnapshot() -> [RemoteAlbumItem] {
        remoteIndex.remoteItemsSnapshot()
    }

    func mergedMonthItemsSnapshot() -> [MergedMonthSection] {
        reconcileIndex.mergedMonthItemsSnapshot().map { month, items in
            MergedMonthSection(month: month, items: items)
        }
    }

    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        Task { @MainActor [weak self] in
            guard let self else { return }

            let changedMonths = self.localIndex.applyPhotoLibraryChange(
                changeInstance,
                fetchHashMapByAsset: { [contentHashIndexRepository] assetIDs in
                    guard !assetIDs.isEmpty else { return [:] }
                    return (try? contentHashIndexRepository.fetchHashMapByAsset(assetIDs: assetIDs)) ?? [:]
                },
                fetchFingerprintByAsset: { [contentHashIndexRepository] assetIDs in
                    guard !assetIDs.isEmpty else { return [:] }
                    return (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: assetIDs)) ?? [:]
                },
                fetchAllHashMapByAsset: { [contentHashIndexRepository] in
                    (try? contentHashIndexRepository.fetchHashMapByAsset()) ?? [:]
                },
                fetchAllFingerprintByAsset: { [contentHashIndexRepository] in
                    (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset()) ?? [:]
                },
                remoteFingerprintSet: self.remoteIndex.assetFingerprintSet
            )

            if self.reconcileIfNeeded(changedMonths) {
                self.onDataChanged?()
            }
        }
    }

    private func reconcileIfNeeded(_ changedMonths: Set<LibraryMonthKey>) -> Bool {
        reconcileIndex.reconcile(
            changedMonths: changedMonths,
            localIndex: localIndex,
            remoteIndex: remoteIndex,
            hasActiveConnection: hasActiveConnection
        )
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
}

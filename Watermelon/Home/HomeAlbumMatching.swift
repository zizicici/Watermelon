import Foundation
import Photos

enum AlbumMediaKind {
    case photo
    case video
    case livePhoto
}

enum ItemSourceTag {
    case localOnly
    case remoteOnly
    case both
}

struct LocalAlbumItem {
    let id: String
    let asset: PHAsset
    let creationDate: Date
    let isBackedUp: Bool
    let mediaKind: AlbumMediaKind
    let contentHashes: [Data]
}

struct RemoteAlbumItem {
    let id: String
    let creationDate: Date
    let resources: [RemoteManifestResource]
    let representative: RemoteManifestResource
    let mediaKind: AlbumMediaKind
    let pixelWidth: Int?
    let pixelHeight: Int?
    let contentHashes: [Data]
}

struct HomeAlbumItem {
    let id: String
    let creationDate: Date
    let sourceTag: ItemSourceTag
    let mediaKind: AlbumMediaKind
    let localItem: LocalAlbumItem?
    let remoteItem: RemoteAlbumItem?
}

enum HomeAlbumMatching {
    private struct ResourceLookupKey: Hashable {
        let year: Int
        let month: Int
        let hash: Data
    }

    static func makeHashToAssetIndex(_ hashMapByAsset: [String: [Data]]) -> [Data: [String]] {
        var hashToAssetSet: [Data: Set<String>] = [:]
        for (assetID, hashes) in hashMapByAsset {
            for hash in Set(hashes) {
                hashToAssetSet[hash, default: []].insert(assetID)
            }
        }
        return hashToAssetSet.mapValues { Array($0).sorted() }
    }

    static func buildRemoteItems(from snapshot: RemoteLibrarySnapshot) -> [RemoteAlbumItem] {
        buildRemoteItems(
            assets: snapshot.assets,
            resources: snapshot.resources,
            links: snapshot.assetResourceLinks
        )
    }

    static func buildRemoteItems(
        assets: [RemoteManifestAsset],
        resources: [RemoteManifestResource],
        links: [RemoteAssetResourceLink]
    ) -> [RemoteAlbumItem] {
        guard !assets.isEmpty else { return [] }

        let resourcesByMonthHash = Dictionary(uniqueKeysWithValues: resources.map { resource in
            (
                ResourceLookupKey(
                    year: resource.year,
                    month: resource.month,
                    hash: resource.contentHash
                ),
                resource
            )
        })

        var linksByAssetID: [String: [RemoteAssetResourceLink]] = [:]
        linksByAssetID.reserveCapacity(assets.count)
        for link in links {
            linksByAssetID[link.assetID, default: []].append(link)
        }

        var result: [RemoteAlbumItem] = []
        result.reserveCapacity(assets.count)

        for asset in assets {
            let sortedLinks = (linksByAssetID[asset.id] ?? []).sorted { lhs, rhs in
                if lhs.role != rhs.role { return lhs.role < rhs.role }
                if lhs.slot != rhs.slot { return lhs.slot < rhs.slot }
                return lhs.resourceHash.lexicographicallyPrecedes(rhs.resourceHash)
            }

            guard !sortedLinks.isEmpty else { continue }

            var groupedResources: [RemoteManifestResource] = []
            groupedResources.reserveCapacity(sortedLinks.count)
            var contentHashes: [Data] = []
            contentHashes.reserveCapacity(sortedLinks.count)
            var seenHashes = Set<Data>()

            for link in sortedLinks {
                let key = ResourceLookupKey(year: asset.year, month: asset.month, hash: link.resourceHash)
                if let resource = resourcesByMonthHash[key] {
                    groupedResources.append(resource)
                }
                if seenHashes.insert(link.resourceHash).inserted {
                    contentHashes.append(link.resourceHash)
                }
            }

            guard let representative = chooseRepresentativeResource(groupedResources) else { continue }
            result.append(
                RemoteAlbumItem(
                    id: asset.id,
                    creationDate: asset.creationDate,
                    resources: groupedResources,
                    representative: representative,
                    mediaKind: detectMediaKind(from: groupedResources),
                    pixelWidth: nil,
                    pixelHeight: nil,
                    contentHashes: contentHashes
                )
            )
        }

        return result.sorted {
            if $0.creationDate != $1.creationDate {
                return $0.creationDate > $1.creationDate
            }
            return $0.id > $1.id
        }
    }

    static func mergeItems(
        localItems: [LocalAlbumItem],
        remoteItems: [RemoteAlbumItem],
        localAssetIdentifierByHash: [Data: [String]],
        hasActiveConnection: Bool
    ) -> [HomeAlbumItem] {
        let localByID = Dictionary(uniqueKeysWithValues: localItems.map { ($0.id, $0) })
        let localHashSetByID = Dictionary(uniqueKeysWithValues: localItems.map { ($0.id, Set($0.contentHashes)) })
        var consumedLocalIDs = Set<String>()
        var result: [HomeAlbumItem] = []
        result.reserveCapacity(localItems.count + remoteItems.count)

        if hasActiveConnection {
            for remote in remoteItems {
                var candidateLocalIDSet = Set<String>()
                for hash in remote.contentHashes {
                    guard let localIDs = localAssetIdentifierByHash[hash] else { continue }
                    for localID in localIDs where !consumedLocalIDs.contains(localID) && localByID[localID] != nil {
                        candidateLocalIDSet.insert(localID)
                    }
                }

                let localID = bestLocalID(
                    candidateLocalIDs: Array(candidateLocalIDSet),
                    remote: remote,
                    localByID: localByID,
                    localHashSetByID: localHashSetByID
                )

                if let localID, let local = localByID[localID] {
                    consumedLocalIDs.insert(localID)
                    result.append(
                        HomeAlbumItem(
                            id: "both:\(local.id)",
                            creationDate: local.creationDate,
                            sourceTag: .both,
                            mediaKind: mergeMediaKind(local: local.mediaKind, remote: remote.mediaKind),
                            localItem: local,
                            remoteItem: remote
                        )
                    )
                } else {
                    result.append(
                        HomeAlbumItem(
                            id: "remote:\(remote.id)",
                            creationDate: remote.creationDate,
                            sourceTag: .remoteOnly,
                            mediaKind: remote.mediaKind,
                            localItem: nil,
                            remoteItem: remote
                        )
                    )
                }
            }
        }

        for local in localItems where !consumedLocalIDs.contains(local.id) {
            result.append(
                HomeAlbumItem(
                    id: "local:\(local.id)",
                    creationDate: local.creationDate,
                    sourceTag: .localOnly,
                    mediaKind: local.mediaKind,
                    localItem: local,
                    remoteItem: nil
                )
            )
        }

        return result
    }

    static func mergeMediaKind(local: AlbumMediaKind, remote: AlbumMediaKind) -> AlbumMediaKind {
        if local == .livePhoto || remote == .livePhoto {
            return .livePhoto
        }
        if local == .video || remote == .video {
            return .video
        }
        return .photo
    }

    private static func chooseRepresentativeResource(_ resources: [RemoteManifestResource]) -> RemoteManifestResource? {
        let preferred = resources.first {
            ResourceTypeCode.isPhotoLike($0.resourceType)
        }
        return preferred ?? resources.first
    }

    private static func detectMediaKind(from resources: [RemoteManifestResource]) -> AlbumMediaKind {
        let hasPairedVideo = resources.contains { $0.resourceType == ResourceTypeCode.pairedVideo }
        let hasPhotoLike = resources.contains { ResourceTypeCode.isPhotoLike($0.resourceType) }
        if hasPairedVideo, hasPhotoLike {
            return .livePhoto
        }

        let hasVideo = resources.contains { ResourceTypeCode.isVideoLike($0.resourceType) }
        return hasVideo ? .video : .photo
    }

    private static func bestLocalID(
        candidateLocalIDs: [String],
        remote: RemoteAlbumItem,
        localByID: [String: LocalAlbumItem],
        localHashSetByID: [String: Set<Data>]
    ) -> String? {
        let remoteHashSet = Set(remote.contentHashes)
        return candidateLocalIDs.max { lhs, rhs in
            guard let lhsLocal = localByID[lhs], let rhsLocal = localByID[rhs] else {
                return lhs < rhs
            }
            let lhsHashSet = localHashSetByID[lhs] ?? []
            let rhsHashSet = localHashSetByID[rhs] ?? []

            let lhsExactMatch = !remoteHashSet.isEmpty && lhsHashSet == remoteHashSet
            let rhsExactMatch = !remoteHashSet.isEmpty && rhsHashSet == remoteHashSet
            if lhsExactMatch != rhsExactMatch {
                return !lhsExactMatch && rhsExactMatch
            }

            let lhsIntersection = lhsHashSet.intersection(remoteHashSet).count
            let rhsIntersection = rhsHashSet.intersection(remoteHashSet).count
            if lhsIntersection != rhsIntersection {
                return lhsIntersection < rhsIntersection
            }

            if lhsLocal.isBackedUp != rhsLocal.isBackedUp {
                return !lhsLocal.isBackedUp && rhsLocal.isBackedUp
            }

            let lhsDistance = abs(lhsLocal.creationDate.timeIntervalSince(remote.creationDate))
            let rhsDistance = abs(rhsLocal.creationDate.timeIntervalSince(remote.creationDate))
            if lhsDistance != rhsDistance {
                return lhsDistance > rhsDistance
            }

            return lhs > rhs
        }
    }
}

@MainActor
final class HomeIncrementalDataManager: NSObject, PHPhotoLibraryChangeObserver {
    private struct RemoteLinkKey: Hashable {
        let assetFingerprint: Data
        let role: Int
        let slot: Int
    }

    struct MergedMonthSection {
        let month: LibraryMonthKey
        let items: [HomeAlbumItem]
    }

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
    private let photoLibraryService: PhotoLibraryService
    private let contentHashIndexRepository: ContentHashIndexRepository

    var onDataChanged: (() -> Void)?

    private var hasActiveConnection = false

    private var localFetchResult: PHFetchResult<PHAsset>?
    private var isObservingPhotoLibrary = false
    private var localStatesByAssetID: [String: LocalState] = [:]
    private var localAssetIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
    private var localHashesByAssetID: [String: [Data]] = [:]
    private var localFingerprintByAssetID: [String: Data] = [:]
    private var localAssetIDsByFingerprint: [Data: Set<String>] = [:]

    private var remoteAssetsByMonth: [LibraryMonthKey: [String: RemoteManifestAsset]] = [:]
    private var remoteResourcesByMonth: [LibraryMonthKey: [String: RemoteManifestResource]] = [:]
    private var remoteLinksByMonth: [LibraryMonthKey: [RemoteLinkKey: RemoteAssetResourceLink]] = [:]
    private var remoteItemsByMonth: [LibraryMonthKey: [RemoteAlbumItem]] = [:]
    private var remoteAssetFingerprintSet: Set<Data> = []
    private var remoteFingerprintRefCount: [Data: Int] = [:]
    private(set) var remoteSnapshotRevision: UInt64?

    private var mergedByMonth: [LibraryMonthKey: [HomeAlbumItem]] = [:]

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
        if localFetchResult != nil {
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
            return clearLocalStateIfNeeded()
        }

        let fetchResult = photoLibraryService.fetchAssetsResult()
        let hashMapByAsset = (try? contentHashIndexRepository.fetchHashMapByAsset()) ?? [:]
        let fingerprintByAsset = (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset()) ?? [:]

        let changed = rebuildAllLocalState(
            fetchResult: fetchResult,
            hashMapByAsset: hashMapByAsset,
            fingerprintByAsset: fingerprintByAsset
        )

        registerPhotoLibraryObserverIfNeeded()
        return changed
    }

    @discardableResult
    func refreshLocalIndex(forAssetIDs assetIDs: Set<String>) -> Bool {
        guard !assetIDs.isEmpty else { return false }
        guard !localStatesByAssetID.isEmpty else { return false }

        let existingIDs = assetIDs.filter { localStatesByAssetID[$0] != nil }
        guard !existingIDs.isEmpty else { return false }

        let hashMap = (try? contentHashIndexRepository.fetchHashMapByAsset(assetIDs: existingIDs)) ?? [:]
        let fingerprintMap = (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: existingIDs)) ?? [:]

        var changedMonths = Set<LibraryMonthKey>()

        for assetID in existingIDs {
            guard let current = localStatesByAssetID[assetID] else { continue }
            let hashes = hashMap[assetID] ?? []
            let fingerprint = fingerprintMap[assetID]
            changedMonths.formUnion(
                upsertLocalState(
                    asset: current.asset,
                    hashes: hashes,
                    fingerprint: fingerprint
                )
            )
        }

        guard !changedMonths.isEmpty else { return false }
        rebuildMergedItems(forMonths: changedMonths)
        return true
    }

    @discardableResult
    func syncRemoteSnapshot(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) -> Bool {
        var changedMonths = Set<LibraryMonthKey>()

        if self.hasActiveConnection != hasActiveConnection {
            self.hasActiveConnection = hasActiveConnection
            changedMonths.formUnion(localAssetIDsByMonth.keys)
            changedMonths.formUnion(remoteItemsByMonth.keys)
        }

        guard hasActiveConnection else {
            if !remoteItemsByMonth.isEmpty || !remoteAssetFingerprintSet.isEmpty {
                changedMonths.formUnion(remoteItemsByMonth.keys)
                let removedFingerprints = remoteAssetFingerprintSet

                remoteAssetsByMonth.removeAll()
                remoteResourcesByMonth.removeAll()
                remoteLinksByMonth.removeAll()
                remoteItemsByMonth.removeAll()
                remoteAssetFingerprintSet.removeAll()
                remoteFingerprintRefCount.removeAll()
                remoteSnapshotRevision = state.revision

                changedMonths.formUnion(refreshLocalBackedUpState(changedFingerprints: removedFingerprints))
            } else {
                remoteSnapshotRevision = state.revision
            }

            guard !changedMonths.isEmpty else { return false }
            rebuildMergedItems(forMonths: changedMonths)
            return true
        }

        if remoteSnapshotRevision == state.revision, !state.isFullSnapshot {
            guard !changedMonths.isEmpty else { return false }
            rebuildMergedItems(forMonths: changedMonths)
            return true
        }

        if state.isFullSnapshot {
            changedMonths.formUnion(remoteItemsByMonth.keys)
            let removedFingerprints = remoteAssetFingerprintSet
            remoteAssetsByMonth.removeAll()
            remoteResourcesByMonth.removeAll()
            remoteLinksByMonth.removeAll()
            remoteItemsByMonth.removeAll()
            remoteAssetFingerprintSet.removeAll()
            remoteFingerprintRefCount.removeAll()

            if !removedFingerprints.isEmpty {
                changedMonths.formUnion(refreshLocalBackedUpState(changedFingerprints: removedFingerprints))
            }
        }

        var changedFingerprints = Set<Data>()

        for monthDelta in state.monthDeltas {
            let month = monthDelta.month

            let previousAssetMap = remoteAssetsByMonth[month] ?? [:]
            let previousResourceMap = remoteResourcesByMonth[month] ?? [:]
            let previousLinkMap = remoteLinksByMonth[month] ?? [:]

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

            changedMonths.insert(month)

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
        }

        remoteAssetFingerprintSet = Set(remoteFingerprintRefCount.keys)
        remoteSnapshotRevision = state.revision

        if !changedFingerprints.isEmpty {
            changedMonths.formUnion(refreshLocalBackedUpState(changedFingerprints: changedFingerprints))
        }

        guard !changedMonths.isEmpty else { return false }
        rebuildMergedItems(forMonths: changedMonths)
        return true
    }

    func localItemsSnapshot() -> [LocalAlbumItem] {
        let months = localAssetIDsByMonth.keys.sorted(by: >)
        var result: [LocalAlbumItem] = []
        result.reserveCapacity(localStatesByAssetID.count)

        for month in months {
            let items = (localAssetIDsByMonth[month] ?? [])
                .compactMap { localStatesByAssetID[$0]?.item }
                .sorted {
                    if $0.creationDate != $1.creationDate {
                        return $0.creationDate > $1.creationDate
                    }
                    return $0.id > $1.id
                }
            result.append(contentsOf: items)
        }

        return result
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

    func mergedMonthItemsSnapshot() -> [MergedMonthSection] {
        mergedByMonth.keys.sorted(by: >).compactMap { month in
            guard let items = mergedByMonth[month], !items.isEmpty else { return nil }
            return MergedMonthSection(month: month, items: items)
        }
    }

    nonisolated func photoLibraryDidChange(_ changeInstance: PHChange) {
        Task { @MainActor [weak self] in
            guard let self,
                  let currentFetchResult = self.localFetchResult,
                  let details = changeInstance.changeDetails(for: currentFetchResult) else {
                return
            }

            let changed = self.applyLocalPhotoLibraryChange(
                details: details,
                previousFetchResult: currentFetchResult
            )
            if changed {
                self.onDataChanged?()
            }
        }
    }

    private func applyLocalPhotoLibraryChange(
        details: PHFetchResultChangeDetails<PHAsset>,
        previousFetchResult: PHFetchResult<PHAsset>
    ) -> Bool {
        let nextFetchResult = details.fetchResultAfterChanges
        localFetchResult = nextFetchResult

        guard details.hasIncrementalChanges else {
            let hashMapByAsset = (try? contentHashIndexRepository.fetchHashMapByAsset()) ?? [:]
            let fingerprintByAsset = (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset()) ?? [:]
            return rebuildAllLocalState(
                fetchResult: nextFetchResult,
                hashMapByAsset: hashMapByAsset,
                fingerprintByAsset: fingerprintByAsset
            )
        }

        var removedAssetIDs = Set<String>()
        if let removedIndexes = details.removedIndexes {
            removedAssetIDs.reserveCapacity(removedIndexes.count)
            for index in removedIndexes {
                removedAssetIDs.insert(previousFetchResult.object(at: index).localIdentifier)
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
        let hashMap = (try? contentHashIndexRepository.fetchHashMapByAsset(assetIDs: upsertAssetIDs)) ?? [:]
        let fingerprintMap = (try? contentHashIndexRepository.fetchAssetFingerprintsByAsset(assetIDs: upsertAssetIDs)) ?? [:]

        for asset in upsertAssetsByID.values {
            let hashes = hashMap[asset.localIdentifier] ?? []
            let fingerprint = fingerprintMap[asset.localIdentifier]
            changedMonths.formUnion(
                upsertLocalState(
                    asset: asset,
                    hashes: hashes,
                    fingerprint: fingerprint
                )
            )
        }

        guard !changedMonths.isEmpty else { return false }
        rebuildMergedItems(forMonths: changedMonths)
        return true
    }

    private func rebuildAllLocalState(
        fetchResult: PHFetchResult<PHAsset>,
        hashMapByAsset: [String: [Data]],
        fingerprintByAsset: [String: Data]
    ) -> Bool {
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
            let isBackedUp = fingerprint.map { remoteAssetFingerprintSet.contains($0) } ?? false
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

        let oldMonths = Set(localAssetIDsByMonth.keys)
        let newMonths = Set(newAssetIDsByMonth.keys)
        let changedMonths = oldMonths.union(newMonths)

        localFetchResult = fetchResult
        localStatesByAssetID = newStates
        localAssetIDsByMonth = newAssetIDsByMonth
        localHashesByAssetID = hashMapByAsset
        localFingerprintByAssetID = fingerprintByAsset
        localAssetIDsByFingerprint = newAssetIDsByFingerprint

        guard !changedMonths.isEmpty else { return false }
        rebuildMergedItems(forMonths: changedMonths)
        return true
    }

    private func clearLocalStateIfNeeded() -> Bool {
        guard !localStatesByAssetID.isEmpty || !localAssetIDsByMonth.isEmpty else {
            return false
        }

        let changedMonths = Set(localAssetIDsByMonth.keys)

        localFetchResult = nil
        localStatesByAssetID.removeAll()
        localAssetIDsByMonth.removeAll()
        localHashesByAssetID.removeAll()
        localFingerprintByAssetID.removeAll()
        localAssetIDsByFingerprint.removeAll()

        rebuildMergedItems(forMonths: changedMonths)
        return true
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

    private func upsertLocalState(asset: PHAsset, hashes: [Data], fingerprint: Data?) -> Set<LibraryMonthKey> {
        let assetID = asset.localIdentifier
        let month = monthKey(for: asset.creationDate)
        let isBackedUp = fingerprint.map { remoteAssetFingerprintSet.contains($0) } ?? false
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

    private func refreshLocalBackedUpState(changedFingerprints: Set<Data>) -> Set<LibraryMonthKey> {
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
                    asset: state.asset,
                    hashes: state.hashes,
                    fingerprint: state.fingerprint
                )
            )
        }

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

    private func rebuildMergedItems(forMonths months: Set<LibraryMonthKey>) {
        guard !months.isEmpty else { return }

        for month in months {
            let localItems = localItems(for: month)
            let remoteItems = remoteItemsByMonth[month] ?? []

            if localItems.isEmpty, remoteItems.isEmpty {
                mergedByMonth[month] = nil
                continue
            }

            let hashIndex = localHashIndex(from: localItems)
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

    }

    private func localItems(for month: LibraryMonthKey) -> [LocalAlbumItem] {
        (localAssetIDsByMonth[month] ?? [])
            .compactMap { localStatesByAssetID[$0]?.item }
            .sorted {
                if $0.creationDate != $1.creationDate {
                    return $0.creationDate > $1.creationDate
                }
                return $0.id > $1.id
            }
    }

    private func localHashIndex(from localItems: [LocalAlbumItem]) -> [Data: [String]] {
        var hashToAssetSet: [Data: Set<String>] = [:]

        for item in localItems {
            for hash in Set(item.contentHashes) {
                hashToAssetSet[hash, default: []].insert(item.id)
            }
        }

        return hashToAssetSet.mapValues { Array($0).sorted() }
    }

    private func monthKey(for date: Date?) -> LibraryMonthKey {
        let actualDate = date ?? Date(timeIntervalSince1970: 0)
        let comps = calendar.dateComponents([.year, .month], from: actualDate)
        return LibraryMonthKey(year: comps.year ?? 1970, month: comps.month ?? 1)
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

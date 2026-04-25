import Foundation

/// In-memory mirror of the local PhotoKit library, indexed by month with a
/// fingerprint cache for backed-up counts.
///
/// **Concurrency contract**: callers must serialize access externally. The
/// `@unchecked Sendable` conformance exists only because `HomeDataProcessingWorker`
/// crosses its `processingQueue` boundary with engine instances; the queue is
/// what actually serializes mutations. Direct concurrent access from multiple
/// threads is unsafe.
final class HomeLocalIndexEngine: @unchecked Sendable {
    struct MonthAggregate {
        let assetCount: Int
        let photoCount: Int
        let videoCount: Int
        let backedUpCount: Int
    }

    private struct TrackedCollection {
        let collection: LibraryAssetCollection
        var assetIDs: Set<String>
    }

    // For All Photos `trackedCollections.count == 1` and every membership count is 1.
    // For album scope an asset may belong to several selected albums; the count gates
    // eviction so a removal from one album doesn't drop the asset from the index.
    private var trackedCollections: [TrackedCollection] = []
    private var assetMembershipCount: [String: Int] = [:]

    private var localAssetIDsByMonth: [LibraryMonthKey: Set<String>] = [:]
    private var assetIDToMonth: [String: LibraryMonthKey] = [:]
    private var mediaKindByAssetID: [String: AlbumMediaKind] = [:]
    // In-memory mirror of `local_assets.assetFingerprint` so recomputeAggregates can
    // compute backed-up counts without hitting the DB.
    private var fingerprintByAssetID: [String: Data] = [:]
    private var monthAggregates: [LibraryMonthKey: MonthAggregate] = [:]
    private(set) var monthFileSizes: [LibraryMonthKey: Int64] = [:]

    var hasLoadedIndex: Bool {
        !trackedCollections.isEmpty
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

    func reload(
        collections: [LibraryAssetCollection],
        fingerprintByAsset: [String: Data],
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) -> Set<LibraryMonthKey> {
        let oldMonths = allMonths
        trackedCollections.removeAll(keepingCapacity: true)
        assetMembershipCount.removeAll(keepingCapacity: true)
        localAssetIDsByMonth.removeAll()
        assetIDToMonth.removeAll()
        mediaKindByAssetID.removeAll()
        fingerprintByAssetID.removeAll()
        monthAggregates.removeAll(keepingCapacity: true)
        // Wipe sizes on a full reload: prior values may have been computed against a
        // different scope and would mislead the UI until startFileSizeScan repopulates.
        monthFileSizes.removeAll(keepingCapacity: true)

        let assetCountHint = collections.reduce(0) { $0 + $1.assetSnapshots.count }
        assetIDToMonth.reserveCapacity(assetCountHint)
        mediaKindByAssetID.reserveCapacity(assetCountHint)
        fingerprintByAssetID.reserveCapacity(min(fingerprintByAsset.count, assetCountHint))
        assetMembershipCount.reserveCapacity(assetCountHint)
        trackedCollections.reserveCapacity(collections.count)

        for collection in collections {
            var collectionAssetIDs = Set<String>()
            let snapshots = collection.assetSnapshots
            collectionAssetIDs.reserveCapacity(snapshots.count)
            for snapshot in snapshots {
                let assetID = snapshot.localIdentifier
                guard collectionAssetIDs.insert(assetID).inserted else { continue }

                let oldCount = assetMembershipCount[assetID] ?? 0
                assetMembershipCount[assetID] = oldCount + 1
                if oldCount == 0 {
                    let month = LibraryMonthKey.from(date: snapshot.creationDate)
                    insertAssetID(assetID, month: month, mediaKind: snapshot.mediaKind)
                    // Orphans (DB entry whose PHAsset no longer exists) are dropped
                    // implicitly by only copying fingerprints for IDs we actually saw.
                    if let fp = fingerprintByAsset[assetID] {
                        fingerprintByAssetID[assetID] = fp
                    }
                }
            }
            trackedCollections.append(TrackedCollection(
                collection: collection,
                assetIDs: collectionAssetIDs
            ))
        }

        let newMonths = Set(localAssetIDsByMonth.keys)
        recomputeAggregates(
            for: newMonths,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
        return oldMonths.union(newMonths)
    }

    func clearIfNeeded() -> Set<LibraryMonthKey> {
        guard hasLoadedIndex || !localAssetIDsByMonth.isEmpty else { return [] }

        let changedMonths = allMonths
        trackedCollections.removeAll()
        assetMembershipCount.removeAll()
        localAssetIDsByMonth.removeAll()
        assetIDToMonth.removeAll()
        mediaKindByAssetID.removeAll()
        fingerprintByAssetID.removeAll()
        monthAggregates.removeAll()
        monthFileSizes.removeAll()
        return changedMonths
    }

    /// Recompute fingerprint / aggregate for IDs already represented in the index.
    /// Unknown IDs are silently skipped. Returns the months whose aggregates changed.
    func refreshExisting(
        assetIDs: Set<String>,
        fingerprintsForIDs: (Set<String>) -> [String: Data],
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty, hasLoadedIndex else { return [] }

        var changedMonths = Set<LibraryMonthKey>()
        var refreshIDs = Set<String>()
        refreshIDs.reserveCapacity(assetIDs.count)
        for id in assetIDs {
            guard let month = monthForAsset(id) else { continue }
            changedMonths.insert(month)
            refreshIDs.insert(id)
        }
        guard !refreshIDs.isEmpty else { return [] }

        refreshFingerprintCache(for: refreshIDs, using: fingerprintsForIDs)
        recomputeAggregates(
            for: changedMonths,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
        return changedMonths
    }

    /// Insert assets the engine doesn't yet track. Membership count is left at zero
    /// so the next PHChange's `insertedIndexes` ratifies them via
    /// `applyMembershipDelta` (0→1). Caller is responsible for ensuring such a
    /// PHChange will arrive — typical use is just-downloaded assets whose creation
    /// PHChange is in flight or deferred. Already-tracked IDs in `snapshots` are
    /// skipped so a double call is idempotent.
    func eagerlyInsert(
        _ snapshots: [String: LibraryAssetSnapshot],
        fingerprintsForIDs: (Set<String>) -> [String: Data],
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard !snapshots.isEmpty, hasLoadedIndex else { return [] }

        let candidateIDs = Set(snapshots.keys)
        let trackedIDs = knownAssetIDs(in: candidateIDs)
        let insertedIDs = candidateIDs.subtracting(trackedIDs)
        guard !insertedIDs.isEmpty else { return [] }

        var changedMonths = Set<LibraryMonthKey>()
        for id in insertedIDs {
            guard let snapshot = snapshots[id] else { continue }
            let month = LibraryMonthKey.from(date: snapshot.creationDate)
            insertAssetID(id, month: month, mediaKind: snapshot.mediaKind)
            changedMonths.insert(month)
        }

        refreshFingerprintCache(for: insertedIDs, using: fingerprintsForIDs)
        recomputeAggregates(
            for: changedMonths,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
        return changedMonths
    }

    /// Membership is reconciled via `assetMembershipCount` so an asset that lives in
    /// multiple tracked collections is evicted from the month index only when its
    /// last collection drops it.
    func applyChange(
        _ provider: LibraryChangeProvider,
        fingerprintsForIDs: (Set<String>) -> [String: Data],
        remoteFingerprintsForMonth: (LibraryMonthKey) -> Set<Data>
    ) -> Set<LibraryMonthKey> {
        guard !trackedCollections.isEmpty else { return [] }

        var changedMonths = Set<LibraryMonthKey>()
        var fingerprintRefreshIDs = Set<String>()

        for index in trackedCollections.indices {
            let tracked = trackedCollections[index]
            guard let change = provider.change(for: tracked.collection) else { continue }

            let previousAssetIDs = tracked.assetIDs
            var nextAssetIDs: Set<String>
            var upsertSnapshots: [String: LibraryAssetSnapshot] = [:]

            if change.hasIncrementalChanges {
                nextAssetIDs = previousAssetIDs

                for assetID in change.removedAssetIDs {
                    nextAssetIDs.remove(assetID)
                }

                for snapshot in change.insertedAssets {
                    nextAssetIDs.insert(snapshot.localIdentifier)
                    upsertSnapshots[snapshot.localIdentifier] = snapshot
                }

                for snapshot in change.changedAssets {
                    nextAssetIDs.insert(snapshot.localIdentifier)
                    upsertSnapshots[snapshot.localIdentifier] = snapshot
                }

                for snapshot in change.movedAssets {
                    nextAssetIDs.insert(snapshot.localIdentifier)
                    upsertSnapshots[snapshot.localIdentifier] = snapshot
                }
            } else {
                let nextSnapshots = change.nextCollection.assetSnapshots
                nextAssetIDs = Set<String>()
                nextAssetIDs.reserveCapacity(nextSnapshots.count)
                upsertSnapshots.reserveCapacity(nextSnapshots.count)
                for snapshot in nextSnapshots {
                    let assetID = snapshot.localIdentifier
                    if nextAssetIDs.insert(assetID).inserted {
                        upsertSnapshots[assetID] = snapshot
                    }
                }
            }

            trackedCollections[index] = TrackedCollection(
                collection: change.nextCollection,
                assetIDs: nextAssetIDs
            )

            let removed = previousAssetIDs.subtracting(nextAssetIDs)
            let added = nextAssetIDs.subtracting(previousAssetIDs)
            fingerprintRefreshIDs.formUnion(applyMembershipDelta(
                removedIDs: removed,
                addedIDs: added,
                changedMonths: &changedMonths
            ))
            fingerprintRefreshIDs.formUnion(upsertAssets(
                upsertSnapshots,
                changedMonths: &changedMonths
            ))
        }

        refreshFingerprintCache(
            for: fingerprintRefreshIDs,
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

    private func applyMembershipDelta(
        removedIDs: Set<String>,
        addedIDs: Set<String>,
        changedMonths: inout Set<LibraryMonthKey>
    ) -> Set<String> {
        var newlyRepresentedIDs = Set<String>()

        for id in removedIDs {
            guard let count = assetMembershipCount[id] else { continue }
            if count <= 1 {
                assetMembershipCount[id] = nil
                if let month = monthForAsset(id) {
                    removeFromIDSets(id, month: month)
                    changedMonths.insert(month)
                }
            } else {
                assetMembershipCount[id] = count - 1
            }
        }

        for id in addedIDs {
            let oldCount = assetMembershipCount[id] ?? 0
            assetMembershipCount[id] = oldCount + 1
            if oldCount == 0 {
                newlyRepresentedIDs.insert(id)
            }
        }

        return newlyRepresentedIDs
    }

    private func upsertAssets(
        _ snapshots: [String: LibraryAssetSnapshot],
        changedMonths: inout Set<LibraryMonthKey>
    ) -> Set<String> {
        var representedIDs = Set<String>()
        representedIDs.reserveCapacity(snapshots.count)

        for (id, snapshot) in snapshots {
            guard (assetMembershipCount[id] ?? 0) > 0 else { continue }

            let newMonth = LibraryMonthKey.from(date: snapshot.creationDate)
            if let oldMonth = monthForAsset(id), oldMonth != newMonth {
                removeFromIDSets(id, month: oldMonth)
                changedMonths.insert(oldMonth)
            }
            insertAssetID(id, month: newMonth, mediaKind: snapshot.mediaKind)
            changedMonths.insert(newMonth)
            representedIDs.insert(id)
        }

        return representedIDs
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

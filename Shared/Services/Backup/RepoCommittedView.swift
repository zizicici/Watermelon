import Foundation

/// Materialized commit-log projection. The "durable truth" layer.
///
/// Two mutation entry points are deliberately separated:
/// - `loadFromMaterialize(_)` — wholesale replace from a fresh `RepoMaterializer`
///   pass. The default and preferred path.
/// - `applyOptimisticUpsert(...)` — used by `AssetProcessor` to keep the cache
///   responsive within a long upload run. Resume planner compensates by
///   subtracting `OptimisticInflightTracker.uncommittedAssets()`, so the
///   committed-view semantic stays intact for resume even when the cache has
///   un-flushed entries layered on top.
///
/// Step 9 will lock down the optimistic path so only AssetProcessor can reach
/// it; for now the rename + comment locates the contract.
final class RepoCommittedView: @unchecked Sendable {
    private let cache: RemoteLibrarySnapshotCache
    /// Cache-wide overlay; source is worker-side listing pushed via
    /// `markPhysicallyMissingV2`. Subtracted from classifier inputs in Home /
    /// download / health / resume. Cleared on next materialize.
    private var physicallyMissingByMonth: PerMonth<Set<Data>> = PerMonth<Set<Data>>()
    private let missingLock = NSLock()

    init(cache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache()) {
        self.cache = cache
    }

    func markPhysicallyMissing(month: LibraryMonthKey, hashes: Set<Data>) {
        missingLock.lock()
        let previous = physicallyMissingByMonth[month] ?? []
        if hashes.isEmpty {
            physicallyMissingByMonth.remove(month)
        } else {
            physicallyMissingByMonth.set(hashes, for: month)
        }
        let changed = previous != hashes
        missingLock.unlock()
        // Overlay is part of the read model; cache revision must bump so
        // incremental Home sync (state(since:)) sees the month as changed.
        if changed { cache.markMonthsChanged([month]) }
    }

    func physicallyMissingHashes(for month: LibraryMonthKey) -> Set<Data> {
        missingLock.lock()
        defer { missingLock.unlock() }
        return physicallyMissingByMonth[month] ?? []
    }

    // MARK: - Read API (sole entry point for UI / resume / verify consumers)

    /// Reads MUST hold `missingLock` across both cache + overlay projections. cache
    /// has its own internal lock, but a writer order of "cache mutate → overlay mutate"
    /// would otherwise let a concurrent reader observe new cache + old overlay (or
    /// vice versa) → classifier sees a resource present whose hash is still flagged
    /// missing → spurious partiallyMissing.
    func current() -> RemoteLibrarySnapshot {
        missingLock.lock()
        defer { missingLock.unlock() }
        let base = cache.current()
        return RemoteLibrarySnapshot(
            resources: base.resources,
            assets: base.assets,
            assetResourceLinks: base.assetResourceLinks,
            physicallyMissingHashesByMonth: physicallyMissingSnapshotMapLocked()
        )
    }
    func state(since baseRevision: UInt64?) -> RemoteLibrarySnapshotState {
        missingLock.lock()
        defer { missingLock.unlock() }
        let base = cache.state(since: baseRevision)
        let overlay = physicallyMissingSnapshotMapLocked()
        let injected = base.monthDeltas.map { delta in
            RemoteLibraryMonthDelta(
                month: delta.month,
                resources: delta.resources,
                assets: delta.assets,
                assetResourceLinks: delta.assetResourceLinks,
                physicallyMissingHashes: overlay[delta.month] ?? []
            )
        }
        return RemoteLibrarySnapshotState(
            revision: base.revision,
            isFullSnapshot: base.isFullSnapshot,
            monthDeltas: injected
        )
    }
    func monthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, totalSizeBytes: Int64)] {
        missingLock.lock()
        defer { missingLock.unlock() }
        return cache.monthSummaries(physicallyMissingByMonth: physicallyMissingSnapshotMapLocked())
    }
    func healthDigest() -> RemoteHealthDigest {
        missingLock.lock()
        defer { missingLock.unlock() }
        return cache.healthDigest(physicallyMissingByMonth: physicallyMissingSnapshotMapLocked())
    }
    func allKnownMonths() -> Set<LibraryMonthKey> { cache.allKnownMonths() }
    func monthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        missingLock.lock()
        defer { missingLock.unlock() }
        guard let base = cache.monthRawData(for: month) else { return nil }
        return RemoteLibraryMonthDelta(
            month: base.month,
            resources: base.resources,
            assets: base.assets,
            assetResourceLinks: base.assetResourceLinks,
            physicallyMissingHashes: physicallyMissingByMonth[month] ?? []
        )
    }
    func fileNames(for month: LibraryMonthKey) -> Set<String> { cache.fileNames(for: month) }
    func counts() -> RemoteIndexSyncDigest { cache.counts() }
    func currentLastSyncedAt() -> Date? { cache.currentLastSyncedAt() }

    /// Requires caller to hold `missingLock`.
    private func physicallyMissingSnapshotMapLocked() -> [LibraryMonthKey: Set<Data>] {
        var map: [LibraryMonthKey: Set<Data>] = [:]
        for month in physicallyMissingByMonth.months {
            map[month] = physicallyMissingByMonth[month] ?? []
        }
        return map
    }

    /// Overlay snapshot for sync to preserve across loadFromMaterialize + per-month refresh failure.
    func physicallyMissingSnapshot() -> [LibraryMonthKey: Set<Data>] {
        missingLock.lock()
        defer { missingLock.unlock() }
        return physicallyMissingSnapshotMapLocked()
    }

    // MARK: - Mutation: post-materialize wholesale replace

    /// Loads from a fresh `RepoMaterializer.MaterializeOutput`. This is the canonical
    /// path — every commit-log fold lands here, replacing prior state per-month.
    /// Optimistic entries from `applyOptimisticUpsert` are overwritten by this call,
    /// which is correct: materialize is durable truth.
    func loadFromMaterialize(_ output: RepoMaterializer.MaterializeOutput) {
        missingLock.lock()
        defer { missingLock.unlock() }
        cache.reset()
        // Materialize doesn't probe physical state — caller must re-push the overlay.
        physicallyMissingByMonth.removeAll()
        for (month, monthState) in output.state.months {
            let resources = monthState.resources.values.map { row -> RemoteManifestResource in
                RemoteManifestResource(
                    year: month.year,
                    month: month.month,
                    physicalRemotePath: row.physicalRemotePath,
                    contentHash: row.contentHash,
                    fileSize: row.fileSize,
                    resourceType: row.resourceType,
                    creationDateMs: row.creationDateMs,
                    backedUpAtMs: row.backedUpAtMs,
                    crypto: row.crypto
                )
            }
            let assets = monthState.assets.values.map { row -> RemoteManifestAsset in
                RemoteManifestAsset(
                    year: month.year,
                    month: month.month,
                    assetFingerprint: row.assetFingerprint,
                    creationDateMs: row.creationDateMs,
                    backedUpAtMs: row.backedUpAtMs,
                    resourceCount: row.resourceCount,
                    totalFileSizeBytes: row.totalFileSizeBytes,
                    stamp: row.stamp
                )
            }
            let links = monthState.assetResources.values.map { row -> RemoteAssetResourceLink in
                RemoteAssetResourceLink(
                    year: month.year,
                    month: month.month,
                    assetFingerprint: row.assetFingerprint,
                    resourceHash: row.resourceHash,
                    role: row.role,
                    slot: row.slot,
                    logicalName: row.logicalName
                )
            }
            _ = cache.replaceMonth(month, resources: resources, assets: assets, assetResourceLinks: links)
        }
    }

    /// Wholesale per-month replace, used by V1 sync (which has its own per-month
    /// download flow rather than a materialize) and by verify-tombstone cleanup
    /// (which removes a subset of assets and rewrites the rest). Overlay is preserved —
    /// tombstone cleanup doesn't touch physical files.
    @discardableResult
    func replaceMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink]
    ) -> Bool {
        missingLock.lock()
        defer { missingLock.unlock() }
        return cache.replaceMonth(month, resources: resources, assets: assets, assetResourceLinks: assetResourceLinks)
    }

    @discardableResult
    func removeMonth(_ month: LibraryMonthKey) -> Bool {
        missingLock.lock()
        defer { missingLock.unlock() }
        physicallyMissingByMonth.remove(month)
        return cache.removeMonth(month)
    }

    func markSynced(_ at: Date) {
        cache.markSynced(at)
    }

    func reset() {
        missingLock.lock()
        defer { missingLock.unlock() }
        cache.reset()
        physicallyMissingByMonth.removeAll()
    }

    // MARK: - Mutation: optimistic UI freshness

    /// Optimistic write — keeps the cache reflective during a long upload run so
    /// HomeRemoteIndexEngine / HomeAlbumMatching observe newly-uploaded assets
    /// without waiting for the next materialize. Resume planner compensates by
    /// subtracting `OptimisticInflightTracker.uncommittedAssets()` from this view.
    /// Only `AssetProcessor` should reach for this — every other write path
    /// must go through `loadFromMaterialize`.
    func applyOptimisticUpsert(asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]?) {
        // Asset mutation doesn't touch the overlay; cache's own lock suffices.
        cache.upsertAsset(asset, links: links)
    }

    func applyOptimisticUpsert(resource: RemoteManifestResource) {
        // Holds missingLock across cache mutation + overlay subtract so a concurrent
        // reader can't see new resource row + still-missing overlay (torn read).
        missingLock.lock()
        defer { missingLock.unlock() }
        cache.upsertResource(resource)
        let month = LibraryMonthKey(year: resource.year, month: resource.month)
        let previous = physicallyMissingByMonth[month] ?? []
        physicallyMissingByMonth.subtract([resource.contentHash], from: month)
        let after = physicallyMissingByMonth[month] ?? []
        if previous != after { cache.markMonthsChanged([month]) }
    }
}

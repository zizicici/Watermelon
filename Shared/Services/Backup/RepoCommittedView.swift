import Foundation

final class RepoCommittedView: @unchecked Sendable {
    private let cache: RemoteLibrarySnapshotCache
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
        // Overlay changes must bump revision so incremental Home sync sees the month as changed.
        if changed { cache.markMonthsChanged([month]) }
    }

    func physicallyMissingHashes(for month: LibraryMonthKey) -> Set<Data> {
        missingLock.lock()
        defer { missingLock.unlock() }
        return physicallyMissingByMonth[month] ?? []
    }

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

    private func physicallyMissingSnapshotMapLocked() -> [LibraryMonthKey: Set<Data>] {
        var map: [LibraryMonthKey: Set<Data>] = [:]
        for month in physicallyMissingByMonth.months {
            map[month] = physicallyMissingByMonth[month] ?? []
        }
        return map
    }

    func physicallyMissingSnapshot() -> [LibraryMonthKey: Set<Data>] {
        missingLock.lock()
        defer { missingLock.unlock() }
        return physicallyMissingSnapshotMapLocked()
    }

    @discardableResult
    func loadFromMaterialize(_ output: RepoMaterializer.MaterializeOutput) -> [LibraryMonthKey: Set<Data>] {
        missingLock.lock()
        defer { missingLock.unlock() }
        cache.reset()
        // Capture + clear must be atomic so a concurrent markPhysicallyMissing is preserved by the caller.
        let priorOverlay = physicallyMissingSnapshotMapLocked()
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
        return priorOverlay
    }

    @discardableResult
    func replaceMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink]
    ) -> Bool {
        missingLock.lock()
        defer { missingLock.unlock() }
        let result = cache.replaceMonth(month, resources: resources, assets: assets, assetResourceLinks: assetResourceLinks)
        // Hashes no longer in the manifest aren't "missing" — they're tombstoned. Carry overlay hashes forward only when they still appear in the new manifest.
        if let previous = physicallyMissingByMonth[month] {
            let stillPresent = Set(resources.map(\.contentHash))
            let intersected = previous.intersection(stillPresent)
            if intersected.isEmpty {
                physicallyMissingByMonth.remove(month)
            } else if intersected != previous {
                physicallyMissingByMonth.set(intersected, for: month)
            }
        }
        return result
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

    func applyOptimisticUpsert(asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]?) {
        cache.upsertAsset(asset, links: links)
    }

    func applyOptimisticUpsert(resource: RemoteManifestResource) {
        // Holds missingLock across cache mutation + overlay subtract to avoid a torn read.
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

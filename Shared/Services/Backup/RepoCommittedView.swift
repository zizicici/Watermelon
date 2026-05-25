import Foundation

final class RepoCommittedView: @unchecked Sendable {
    enum PresenceFreshness {
        case keep
        case markFresh
        case markStale
    }

    private let cache: RemoteLibrarySnapshotCache
    private var physicallyMissingByMonth: PerMonth<Set<Data>> = PerMonth<Set<Data>>()
    // Co-located with physicallyMissingByMonth: lock-ordering is optimisticMutationLock (service, outer)
    // -> missingLock (view, inner). Membership changes participate in cache.markMonthsChanged so the
    // authoritative bit on RemoteLibraryMonthDelta.presence is visible to incremental state(since:) callers.
    private var physicalPresenceOverlayFreshMonths: Set<LibraryMonthKey> = []
    private let missingLock = NSLock()

    init(cache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache()) {
        self.cache = cache
    }

    func markPhysicallyMissing(month: LibraryMonthKey, hashes: Set<Data>) {
        missingLock.lock()
        defer { missingLock.unlock() }
        let previous = physicallyMissingByMonth[month] ?? []
        if hashes.isEmpty {
            physicallyMissingByMonth.remove(month)
        } else {
            physicallyMissingByMonth.set(hashes, for: month)
        }
        let changed = previous != hashes
        if changed { cache.markMonthsChanged([month]) }
    }

    func physicallyMissingHashes(for month: LibraryMonthKey) -> Set<Data> {
        missingLock.lock()
        defer { missingLock.unlock() }
        return physicallyMissingByMonth[month] ?? []
    }

    func currentRevision() -> UInt64 {
        cache.currentRevision()
    }

    func current() -> RemoteLibrarySnapshot {
        missingLock.lock()
        defer { missingLock.unlock() }
        let base = cache.current()
        return RemoteLibrarySnapshot(
            resources: base.resources,
            assets: base.assets,
            assetResourceLinks: base.assetResourceLinks,
            presence: fullPresenceSnapshotLocked()
        )
    }

    /// Single read for handle producers: revision + snapshot + overlay captured under both locks so a cache mutation can't slip between the reads.
    func currentSnapshotWithRevision() -> (revision: UInt64, snapshot: RemoteLibrarySnapshot) {
        missingLock.lock()
        defer { missingLock.unlock() }
        let combined = cache.currentWithRevision()
        let snapshot = RemoteLibrarySnapshot(
            resources: combined.snapshot.resources,
            assets: combined.snapshot.assets,
            assetResourceLinks: combined.snapshot.assetResourceLinks,
            presence: fullPresenceSnapshotLocked()
        )
        return (combined.revision, snapshot)
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
                presence: RemotePresenceSnapshot.Month(
                    missingHashes: overlay[delta.month] ?? [],
                    isAuthoritative: physicalPresenceOverlayFreshMonths.contains(delta.month)
                )
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
        return cache.monthSummaries(presence: fullPresenceSnapshotLocked())
    }
    func healthDigest() -> RemoteHealthDigest {
        missingLock.lock()
        defer { missingLock.unlock() }
        return cache.healthDigest(presence: fullPresenceSnapshotLocked())
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
            presence: RemotePresenceSnapshot.Month(
                missingHashes: physicallyMissingByMonth[month] ?? [],
                isAuthoritative: physicalPresenceOverlayFreshMonths.contains(month)
            )
        )
    }
    func fileNames(for month: LibraryMonthKey) -> Set<String> { cache.fileNames(for: month) }
    func counts() -> RemoteIndexSyncDigest {
        missingLock.lock()
        defer { missingLock.unlock() }
        return cache.counts(presence: fullPresenceSnapshotLocked())
    }
    func currentLastSyncedAt() -> Date? { cache.currentLastSyncedAt() }

    private func physicallyMissingSnapshotMapLocked() -> [LibraryMonthKey: Set<Data>] {
        var map: [LibraryMonthKey: Set<Data>] = [:]
        for month in physicallyMissingByMonth.months {
            map[month] = physicallyMissingByMonth[month] ?? []
        }
        return map
    }

    func verifiedPhysicallyMissingHashes(for month: LibraryMonthKey) -> Set<Data>? {
        missingLock.lock()
        defer { missingLock.unlock() }
        guard physicalPresenceOverlayFreshMonths.contains(month) else { return nil }
        return physicallyMissingByMonth[month] ?? []
    }

    func presenceSnapshot(for month: LibraryMonthKey) -> RemotePresenceSnapshot.Month {
        missingLock.lock()
        defer { missingLock.unlock() }
        return RemotePresenceSnapshot.Month(
            missingHashes: physicallyMissingByMonth[month] ?? [],
            isAuthoritative: physicalPresenceOverlayFreshMonths.contains(month)
        )
    }

    func fullPresenceSnapshot() -> RemotePresenceSnapshot {
        missingLock.lock()
        defer { missingLock.unlock() }
        return fullPresenceSnapshotLocked()
    }

    /// Caller MUST hold `missingLock`. Pure read of the overlay + freshness state.
    private func fullPresenceSnapshotLocked() -> RemotePresenceSnapshot {
        let missingMap = physicallyMissingSnapshotMapLocked()
        var builder = RemotePresenceSnapshot.Builder()
        // Union so authoritative-empty months are represented; physicallyMissingByMonth drops empty entries.
        let touched = Set(missingMap.keys).union(physicalPresenceOverlayFreshMonths)
        for month in touched {
            builder.set(
                month,
                missingHashes: missingMap[month] ?? [],
                isAuthoritative: physicalPresenceOverlayFreshMonths.contains(month)
            )
        }
        return builder.build()
    }

    /// Freshness-only changes participate in revision tracking via cache.markMonthsChanged on the
    /// symmetric difference, coalesced with any missing-hash deltas the apply produces.
    @discardableResult
    func applyPresenceSnapshot(
        _ snapshot: RemotePresenceSnapshot,
        expectedRevision: UInt64? = nil
    ) -> Bool {
        missingLock.lock()
        defer { missingLock.unlock() }
        if let expectedRevision, cache.currentRevision() != expectedRevision {
            let cleared = physicalPresenceOverlayFreshMonths
            physicalPresenceOverlayFreshMonths.removeAll()
            if !cleared.isEmpty {
                cache.markMonthsChanged(cleared)
            }
            return false
        }
        var coalescedChanged: Set<LibraryMonthKey> = []
        for entry in snapshot.entries {
            let previous = physicallyMissingByMonth[entry.month] ?? []
            if previous != entry.value.missingHashes {
                if entry.value.missingHashes.isEmpty {
                    physicallyMissingByMonth.remove(entry.month)
                } else {
                    physicallyMissingByMonth.set(entry.value.missingHashes, for: entry.month)
                }
                coalescedChanged.insert(entry.month)
            }
        }
        let newFresh = snapshot.freshMonths
        let freshnessDelta = physicalPresenceOverlayFreshMonths.symmetricDifference(newFresh)
        physicalPresenceOverlayFreshMonths = newFresh
        coalescedChanged.formUnion(freshnessDelta)
        if !coalescedChanged.isEmpty {
            cache.markMonthsChanged(coalescedChanged)
        }
        return true
    }

    func clearPresenceFreshness() {
        missingLock.lock()
        defer { missingLock.unlock() }
        let cleared = physicalPresenceOverlayFreshMonths
        physicalPresenceOverlayFreshMonths.removeAll()
        if !cleared.isEmpty {
            cache.markMonthsChanged(cleared)
        }
    }

    @discardableResult
    func loadFromMaterialize(_ output: RepoMaterializer.MaterializeOutput) -> RemotePresenceSnapshot {
        missingLock.lock()
        defer { missingLock.unlock() }
        let priorOverlay = physicallyMissingSnapshotMapLocked()
        var preservedOverlay: [LibraryMonthKey: Set<Data>] = [:]
        // cache.reset() advances lastResetRevision below, forcing the full-snapshot path on the next
        // state(since: baseRevision) call — no explicit markMonthsChanged needed for freshness clearing.
        physicalPresenceOverlayFreshMonths.removeAll()
        cache.reset()
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
            let stillPresent = Set(resources.map(\.contentHash))
            if let previous = priorOverlay[month] {
                let refined = previous.intersection(stillPresent)
                if !refined.isEmpty {
                    physicallyMissingByMonth.set(refined, for: month)
                    preservedOverlay[month] = refined
                }
            }
        }
        return RemotePresenceSnapshot.failClosed(missingByMonth: preservedOverlay)
    }

    @discardableResult
    func replaceMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink],
        physicallyMissingHashes: Set<Data>? = nil,
        freshness: PresenceFreshness = .keep
    ) -> Bool {
        missingLock.lock()
        defer { missingLock.unlock() }
        let previousMissing = physicallyMissingByMonth[month] ?? []
        let wasFresh = physicalPresenceOverlayFreshMonths.contains(month)
        let result = cache.replaceMonth(month, resources: resources, assets: assets, assetResourceLinks: assetResourceLinks)
        let stillPresent = Set(resources.map(\.contentHash))
        if let physicallyMissingHashes {
            let refined = physicallyMissingHashes.intersection(stillPresent)
            if refined.isEmpty {
                physicallyMissingByMonth.remove(month)
            } else {
                physicallyMissingByMonth.set(refined, for: month)
            }
        } else if let previous = physicallyMissingByMonth[month] {
            let intersected = previous.intersection(stillPresent)
            if intersected.isEmpty {
                physicallyMissingByMonth.remove(month)
            } else if intersected != previous {
                physicallyMissingByMonth.set(intersected, for: month)
            }
        }
        let willBeFresh: Bool
        switch freshness {
        case .keep:
            willBeFresh = wasFresh
        case .markFresh:
            physicalPresenceOverlayFreshMonths.insert(month)
            willBeFresh = true
        case .markStale:
            physicalPresenceOverlayFreshMonths.remove(month)
            willBeFresh = false
        }
        let currentMissing = physicallyMissingByMonth[month] ?? []
        let missingChanged = currentMissing != previousMissing
        let freshnessChanged = wasFresh != willBeFresh
        if missingChanged || freshnessChanged {
            cache.markMonthsChanged([month])
        }
        return result
    }

    @discardableResult
    func removeMonth(_ month: LibraryMonthKey) -> Bool {
        missingLock.lock()
        defer { missingLock.unlock() }
        let wasFresh = physicalPresenceOverlayFreshMonths.contains(month)
        let previousMissing = physicallyMissingByMonth[month] ?? []
        physicallyMissingByMonth.remove(month)
        physicalPresenceOverlayFreshMonths.remove(month)
        let removed = cache.removeMonth(month)
        // cache.removeMonth returns false (no revision bump) for cache-empty months. When such a
        // month had freshness set (authoritative-empty via applyPresenceSnapshot) or carried
        // non-authoritative missing hashes (via markPhysicallyMissing), the clear MUST still mark
        // the month changed so incremental state(since:) sees the authority drop / missing-set drop.
        if (wasFresh || !previousMissing.isEmpty) && !removed {
            cache.markMonthsChanged([month])
        }
        return removed
    }

    func markSynced(_ at: Date) {
        cache.markSynced(at)
    }

    func reset() {
        missingLock.lock()
        defer { missingLock.unlock() }
        cache.reset()
        physicallyMissingByMonth.removeAll()
        physicalPresenceOverlayFreshMonths.removeAll()
    }

    func markMonthsChanged(_ months: Set<LibraryMonthKey>) {
        cache.markMonthsChanged(months)
    }

    func applyOptimisticUpsert(asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]?) {
        missingLock.lock()
        defer { missingLock.unlock() }
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

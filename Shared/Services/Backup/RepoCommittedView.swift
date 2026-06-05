import Foundation

final class RepoCommittedView: @unchecked Sendable {
    enum PresenceFreshness {
        case keep
        case markFresh
        case markStale
    }

    private struct OverlayLinkKey: Hashable {
        let year: Int
        let month: Int
        let assetFingerprint: AssetFingerprint
        let role: Int
        let slot: Int
        init(_ link: RemoteAssetResourceLink) {
            year = link.year
            month = link.month
            assetFingerprint = link.assetFingerprint
            role = link.role
            slot = link.slot
        }
    }

    private let cache: RemoteLibrarySnapshotCache
    /// Session overlay: optimistic rows written before a durable publish/materialize catches up.
    /// Kept distinct from the durable `cache` so a hard abort can drop these rows without erasing
    /// the durable baseline. Effective reads compose durable + overlay; `cache` stays the single
    /// revision authority, so overlay mutations bump it via `markMonthsChanged`.
    private let sessionOverlay = RemoteLibrarySnapshotCache()
    private var physicallyMissingByMonth: PerMonth<Set<Data>> = PerMonth<Set<Data>>()
    // Co-located with physicallyMissingByMonth: lock-ordering is optimisticMutationLock (service, outer)
    // -> missingLock (view, inner). Membership changes participate in cache.markMonthsChanged so the
    // authoritative bit on RemoteLibraryMonthDelta.presence is visible to incremental state(since:) callers.
    private var physicalPresenceOverlayFreshMonths: Set<LibraryMonthKey> = []
    private var nonCleanOutcomeMonths: Set<LibraryMonthKey> = []
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
        let presence = fullPresenceSnapshotLocked()
        let overlayMonths = sessionOverlay.allKnownMonths()
        if overlayMonths.isEmpty {
            let base = cache.current()
            return RemoteLibrarySnapshot(
                resources: base.resources,
                assets: base.assets,
                assetResourceLinks: base.assetResourceLinks,
                presence: presence
            )
        }
        let merged = mergedRowsLocked(months: cache.allKnownMonths().union(overlayMonths))
        return RemoteLibrarySnapshot(
            resources: merged.resources,
            assets: merged.assets,
            assetResourceLinks: merged.links,
            presence: presence
        )
    }

    /// Single read for handle producers: revision + snapshot + overlay captured under both locks so a cache mutation can't slip between the reads.
    func currentSnapshotWithRevision() -> (revision: UInt64, snapshot: RemoteLibrarySnapshot) {
        missingLock.lock()
        defer { missingLock.unlock() }
        let presence = fullPresenceSnapshotLocked()
        let overlayMonths = sessionOverlay.allKnownMonths()
        if overlayMonths.isEmpty {
            let combined = cache.currentWithRevision()
            let snapshot = RemoteLibrarySnapshot(
                resources: combined.snapshot.resources,
                assets: combined.snapshot.assets,
                assetResourceLinks: combined.snapshot.assetResourceLinks,
                presence: presence
            )
            return (combined.revision, snapshot)
        }
        let revision = cache.currentRevision()
        let merged = mergedRowsLocked(months: cache.allKnownMonths().union(overlayMonths))
        let snapshot = RemoteLibrarySnapshot(
            resources: merged.resources,
            assets: merged.assets,
            assetResourceLinks: merged.links,
            presence: presence
        )
        return (revision, snapshot)
    }

    /// Composes durable (`cache`) and session-overlay rows for `months`. Overlay rows win on key
    /// collision (they reflect the latest in-session optimistic write).
    private func mergedRowsLocked(
        months: Set<LibraryMonthKey>
    ) -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        var resources: [RemoteManifestResource] = []
        var assets: [RemoteManifestAsset] = []
        var links: [RemoteAssetResourceLink] = []
        for month in months.sorted() {
            let merged = mergedMonthRowsLocked(month)
            resources.append(contentsOf: merged.resources)
            assets.append(contentsOf: merged.assets)
            links.append(contentsOf: merged.links)
        }
        return (resources, assets, links)
    }

    /// Per-month composition; returns one side directly when the other is empty so the all-durable
    /// path doesn't rebuild dictionaries.
    private func mergedMonthRowsLocked(
        _ month: LibraryMonthKey
    ) -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        let durable = cache.monthRawData(for: month)
        guard let overlayDelta = sessionOverlay.monthRawData(for: month) else {
            return (durable?.resources ?? [], durable?.assets ?? [], durable?.assetResourceLinks ?? [])
        }
        guard let durable else {
            return (overlayDelta.resources, overlayDelta.assets, overlayDelta.assetResourceLinks)
        }
        var resourcesByKey: [RemotePhysicalPathKey: RemoteManifestResource] = [:]
        for resource in durable.resources { resourcesByKey[RemotePhysicalPathKey(resource.physicalRemotePath)] = resource }
        for resource in overlayDelta.resources { resourcesByKey[RemotePhysicalPathKey(resource.physicalRemotePath)] = resource }
        var assetsByID: [String: RemoteManifestAsset] = [:]
        for asset in durable.assets { assetsByID[asset.id] = asset }
        for asset in overlayDelta.assets { assetsByID[asset.id] = asset }
        var linksByKey: [OverlayLinkKey: RemoteAssetResourceLink] = [:]
        for link in durable.assetResourceLinks { linksByKey[OverlayLinkKey(link)] = link }
        for link in overlayDelta.assetResourceLinks { linksByKey[OverlayLinkKey(link)] = link }
        return (Array(resourcesByKey.values), Array(assetsByID.values), Array(linksByKey.values))
    }
    func state(since baseRevision: UInt64?) -> RemoteLibrarySnapshotState {
        missingLock.lock()
        defer { missingLock.unlock() }
        let base = cache.state(since: baseRevision)
        let overlayMonths = sessionOverlay.allKnownMonths()
        if overlayMonths.isEmpty {
            let missingMap = physicallyMissingSnapshotMapLocked()
            let injected = base.monthDeltas.map { delta in
                RemoteLibraryMonthDelta(
                    month: delta.month,
                    resources: delta.resources,
                    assets: delta.assets,
                    assetResourceLinks: delta.assetResourceLinks,
                    presence: RemotePresenceSnapshot.Month(
                        missingHashes: missingMap[delta.month] ?? [],
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
        // Overlay writes mark their months changed on `cache`, so the incremental month set already
        // covers them; a full snapshot must additionally union overlay-only months.
        var monthSet = Set(base.monthDeltas.map(\.month))
        if base.isFullSnapshot {
            monthSet.formUnion(overlayMonths)
        }
        let injected = monthSet.sorted().map { month -> RemoteLibraryMonthDelta in
            let merged = mergedMonthRowsLocked(month)
            return RemoteLibraryMonthDelta(
                month: month,
                resources: merged.resources,
                assets: merged.assets,
                assetResourceLinks: merged.links,
                presence: RemotePresenceSnapshot.Month(
                    missingHashes: physicallyMissingByMonth[month] ?? [],
                    isAuthoritative: physicalPresenceOverlayFreshMonths.contains(month)
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
    func monthsWithNonCleanOutcome() -> Set<LibraryMonthKey> {
        missingLock.lock()
        defer { missingLock.unlock() }
        return nonCleanOutcomeMonths
    }
    func monthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        missingLock.lock()
        defer { missingLock.unlock() }
        guard cache.monthRawData(for: month) != nil || sessionOverlay.monthRawData(for: month) != nil else {
            return nil
        }
        let merged = mergedMonthRowsLocked(month)
        return RemoteLibraryMonthDelta(
            month: month,
            resources: merged.resources,
            assets: merged.assets,
            assetResourceLinks: merged.links,
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
        // Materialize rebuilds the durable view from remote truth; reconcile the session overlay by
        // clearing it so no stale optimistic rows survive the rebuild.
        sessionOverlay.reset()
        physicallyMissingByMonth.removeAll()
        nonCleanOutcomeMonths = Set(output.outcomeByMonth.filter { _, outcome in outcome != .clean }.keys)
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
        replaceMonthImpl(
            month,
            resources: resources,
            assets: assets,
            assetResourceLinks: assetResourceLinks,
            physicallyMissingHashes: physicallyMissingHashes,
            freshness: freshness,
            reconcileOverlay: true
        )
    }

    /// Durable verify prune: drop `fingerprints` (assets + their links) from the durable `cache`
    /// view of `month`, reading durable rows ONLY and leaving the session overlay untouched. A
    /// non-durable optimistic row is therefore never read into nor written from the durable view,
    /// so a later hard abort can still drop it as session-only. Durable resources are kept intact;
    /// the subsequent re-materialize reconciles any orphaned resource rows.
    func pruneDurableMonth(_ month: LibraryMonthKey, removingAssetFingerprints fingerprints: Set<AssetFingerprint>) {
        guard let durable = cache.monthRawData(for: month) else { return }
        let remainingAssets = durable.assets.filter { !fingerprints.contains($0.assetFingerprint) }
        let remainingLinks = durable.assetResourceLinks.filter { !fingerprints.contains($0.assetFingerprint) }
        _ = replaceMonthImpl(
            month,
            resources: durable.resources,
            assets: remainingAssets,
            assetResourceLinks: remainingLinks,
            physicallyMissingHashes: nil,
            freshness: .markStale,
            reconcileOverlay: false
        )
    }

    @discardableResult
    private func replaceMonthImpl(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink],
        physicallyMissingHashes: Set<Data>?,
        freshness: PresenceFreshness,
        reconcileOverlay: Bool
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
        // A full durable publish supersedes any optimistic rows for this month; clear the overlay so
        // the effective view is durable-only and consumers re-fetch the reconciled month. A partial
        // durable prune (verify tombstones) passes reconcileOverlay: false so it never drops or
        // promotes session-only rows.
        if reconcileOverlay, sessionOverlay.removeMonth(month) {
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
        sessionOverlay.reset()
        physicallyMissingByMonth.removeAll()
        physicalPresenceOverlayFreshMonths.removeAll()
        nonCleanOutcomeMonths = []
    }

    func markMonthsChanged(_ months: Set<LibraryMonthKey>) {
        cache.markMonthsChanged(months)
    }

    func appendOverlayAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]?) {
        missingLock.lock()
        defer { missingLock.unlock() }
        let before = sessionOverlay.currentRevision()
        sessionOverlay.upsertAsset(asset, links: links)
        guard sessionOverlay.currentRevision() != before else { return }
        var months: Set<LibraryMonthKey> = [LibraryMonthKey(year: asset.year, month: asset.month)]
        if let links {
            for link in links {
                months.insert(LibraryMonthKey(year: link.year, month: link.month))
            }
        }
        cache.markMonthsChanged(months)
    }

    func appendOverlayResource(_ resource: RemoteManifestResource) {
        // Holds missingLock across overlay mutation + presence subtract to avoid a torn read.
        missingLock.lock()
        defer { missingLock.unlock() }
        let month = LibraryMonthKey(year: resource.year, month: resource.month)
        let before = sessionOverlay.currentRevision()
        sessionOverlay.upsertResource(resource)
        let rowChanged = sessionOverlay.currentRevision() != before
        let previous = physicallyMissingByMonth[month] ?? []
        physicallyMissingByMonth.subtract([resource.contentHash], from: month)
        let presenceChanged = previous != (physicallyMissingByMonth[month] ?? [])
        if rowChanged || presenceChanged { cache.markMonthsChanged([month]) }
    }

    /// Hard-abort boundary: drop this month's session-overlay rows only. The durable baseline in
    /// `cache` and the physical-presence overlay stay intact, so a month that had durable rows
    /// before the aborted optimistic writes remains visible. Bumps the durable revision when rows
    /// were actually dropped so incremental `state(since:)` consumers re-fetch the reconciled month.
    func dropSessionOverlayMonth(_ month: LibraryMonthKey) {
        missingLock.lock()
        defer { missingLock.unlock() }
        if sessionOverlay.removeMonth(month) {
            cache.markMonthsChanged([month])
        }
    }
}

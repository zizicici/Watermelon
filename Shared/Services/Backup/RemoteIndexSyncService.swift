import Foundation
import os.log

private let syncLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

final class RemoteIndexSyncService: Sendable {
    private actor SyncGate {
        private struct Waiter { let id: UUID; let continuation: CheckedContinuation<Void, Error> }
        private var isLocked = false
        private var waiters: [Waiter] = []

        // Cancellation-aware so a run queued behind an unrelated reload observes stop/pause while parked, instead
        // of blocking until the holder releases. Throws CancellationError without taking the lock when cancelled.
        private func acquire() async throws {
            if !isLocked {
                isLocked = true
                return
            }
            let id = UUID()
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    if Task.isCancelled {
                        continuation.resume(throwing: CancellationError())
                    } else {
                        waiters.append(Waiter(id: id, continuation: continuation))
                    }
                }
            } onCancel: {
                Task { await self.cancelWaiter(id) }
            }
        }

        private func cancelWaiter(_ id: UUID) {
            guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
            let waiter = waiters.remove(at: index)
            waiter.continuation.resume(throwing: CancellationError())
        }

        private func release() {
            if !waiters.isEmpty {
                let next = waiters.removeFirst()
                next.continuation.resume()
            } else {
                isLocked = false
            }
        }

        // `operation` MUST NOT re-enter the gate (call another gated method) — this is a non-reentrant FIFO, so a
        // re-entrant call would park behind its own held lock and deadlock the waiter queue.
        func withLock<T>(_ operation: () async throws -> T) async throws -> T {
            try await acquire()   // throws if cancelled while queued → lock not taken, defer below not registered
            defer { release() }
            try Task.checkCancellation()
            return try await operation()
        }
    }

    private actor MutableState {
        private var activeRemoteProfileKey: String?
        private var remoteManifestDigests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]

        func ensureRemoteContext(profileKey: String) -> Bool {
            guard activeRemoteProfileKey != profileKey else { return false }
            activeRemoteProfileKey = profileKey
            remoteManifestDigests.removeAll()
            return true
        }

        func currentRemoteManifestDigests() -> [LibraryMonthKey: RemoteMonthManifestDigest] {
            remoteManifestDigests
        }

        func updateRemoteManifestDigests(_ digests: [LibraryMonthKey: RemoteMonthManifestDigest]) {
            remoteManifestDigests = digests
        }

        func reset() {
            activeRemoteProfileKey = nil
            remoteManifestDigests.removeAll()
        }

        // Digest state travels with the context key — an acting profile that no longer owns it must not
        // reset/forget another context's digests (mirrors the snapshot cache's owner gate).
        func resetIfOwned(by expectedProfileKey: String) {
            guard activeRemoteProfileKey == expectedProfileKey else { return }
            reset()
        }

        func forgetMonthDigest(_ month: LibraryMonthKey, ifOwnedBy expectedProfileKey: String) {
            guard activeRemoteProfileKey == expectedProfileKey else { return }
            remoteManifestDigests[month] = nil
        }
    }

    // Hands changed months to concurrent download workers in deterministic order.
    private actor MonthKeyQueue {
        private let months: [LibraryMonthKey]
        private var index = 0
        init(months: [LibraryMonthKey]) { self.months = months }
        func next() -> LibraryMonthKey? {
            guard index < months.count else { return nil }
            defer { index += 1 }
            return months[index]
        }
    }

    // Aggregates worker results and emits progress. record+emit run together under actor isolation, so
    // even with out-of-order worker completion the callback is delivered in monotonic 1, 2, 3, … order.
    private actor SyncProgressAggregator {
        private let total: Int
        private let onProgress: (@Sendable (RemoteSyncProgress) -> Void)?
        private var applied = 0
        private var processed = 0

        init(total: Int, onProgress: (@Sendable (RemoteSyncProgress) -> Void)?) {
            self.total = total
            self.onProgress = onProgress
        }

        func recordAndReport(appliedDelta: Int) {
            applied += appliedDelta
            processed += 1
            onProgress?(RemoteSyncProgress(current: processed, total: total))
        }
        func appliedCount() -> Int { applied }
        func processedCount() -> Int { processed }
    }

    private let snapshotCache: RemoteLibrarySnapshotCache
    private let syncGate = SyncGate()
    private let state = MutableState()

    init(
        snapshotCache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache()
    ) {
        self.snapshotCache = snapshotCache
    }

    // Whether a sync may take the shared cache over from another profile. `.claimAlways` is the connect /
    // reload behaviour (a switch is exactly a takeover). `.claimIfUnowned` is for a backup run's preflight:
    // a run whose captured profile lost the cache to a cross-profile connect mid-flight must not steal it
    // back — it throws `foreignSnapshotContextError` instead, and the run degrades to a nil seed lookup.
    enum ContextClaimPolicy: Sendable {
        case claimAlways
        case claimIfUnowned
    }

    static func foreignSnapshotContextError() -> NSError {
        NSError(domain: "RemoteIndexSyncService", code: -40)
    }

    func syncIndex(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil,
        layout: MonthManifestStore.ManifestLayout,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil,
        makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)? = nil,
        downloadConcurrency: Int = 1,
        monthFilter: Set<LibraryMonthKey>? = nil,
        newestMonthFirst: Bool = false,
        contextPolicy: ContextClaimPolicy = .claimAlways
    ) async throws -> RemoteIndexSyncDigest {
        try await syncGate.withLock {
            try await syncIndexUnlocked(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                layout: layout,
                liteMonthsListing: liteMonthsListing,
                makeClient: makeClient,
                downloadConcurrency: downloadConcurrency,
                monthFilter: monthFilter,
                newestMonthFirst: newestMonthFirst,
                contextPolicy: contextPolicy
            )
        }
    }

    /// Materialize a flat-array `RemoteLibrarySnapshot` from the current cache. O(N) in
    /// cached entries; call only when a caller actually needs the flat arrays (the
    /// `MonthSeedLookup` in `BackupRunPreparation` is the only current use).
    func fullSnapshot() -> RemoteLibrarySnapshot {
        snapshotCache.current()
    }

    private func syncIndexUnlocked(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        layout: MonthManifestStore.ManifestLayout,
        liteMonthsListing: LiteMonthsListingSnapshot?,
        makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)?,
        downloadConcurrency: Int,
        monthFilter: Set<LibraryMonthKey>? = nil,
        newestMonthFirst: Bool = false,
        contextPolicy: ContextClaimPolicy = .claimAlways
    ) async throws -> RemoteIndexSyncDigest {
        let syncStart = CFAbsoluteTimeGetCurrent()

        let activeProfileKey = Self.remoteProfileKey(profile)
        // Gate-held, so the ownership answer can't be raced by another sync's reset/re-tag.
        if contextPolicy == .claimIfUnowned,
           let ownerKey = snapshotCache.currentProfileKey(), ownerKey != activeProfileKey {
            throw Self.foreignSnapshotContextError()
        }
        let shouldResetSnapshot = await state.ensureRemoteContext(profileKey: activeProfileKey)
        if shouldResetSnapshot {
            snapshotCache.reset()
        }
        // Tag the cache with the owning profile so a browser source built for a different profile can
        // reject this snapshot instead of rendering it through the wrong connection.
        snapshotCache.setProfileKey(activeProfileKey)

        let scanStart = CFAbsoluteTimeGetCurrent()
        var remoteDigests = try await scanManifestDigests(
            client: client,
            basePath: profile.basePath,
            layout: layout,
            liteMonthsListing: liteMonthsListing
        )
        // A superseded sync (profile switched mid-scan) must stop before the eviction/markSynced writes below,
        // which have no other cancellation checkpoint.
        try Task.checkCancellation()
        // A scoped run only reconciles the filtered months. Restrict every set the diff/eviction below derives
        // from — including previous digests — so out-of-scope cached months are left untouched, never evicted.
        if let monthFilter {
            remoteDigests = remoteDigests.filter { monthFilter.contains($0.key) }
        }
        let scanElapsed = CFAbsoluteTimeGetCurrent() - scanStart
        syncLog.info("[SyncTiming] scanManifestDigests: \(Self.ms(scanElapsed))s (\(remoteDigests.count) months)")

        let allPreviousDigests = await state.currentRemoteManifestDigests()
        let previousDigests = monthFilter.map { filter in
            allPreviousDigests.filter { filter.contains($0.key) }
        } ?? allPreviousDigests

        let previousMonths = Set(previousDigests.keys)
        let remoteMonths = Set(remoteDigests.keys)

        var changedMonths = Set<LibraryMonthKey>()
        if previousDigests.isEmpty {
            changedMonths = remoteMonths
        } else {
            for (month, digest) in remoteDigests {
                if previousDigests[month] != digest || digest.manifestModifiedAtMs == nil {
                    changedMonths.insert(month)
                }
            }
        }

        let removedMonths = previousMonths.subtracting(remoteMonths)
        let totalMonthsToProcess = changedMonths.count + removedMonths.count
        onSyncProgress?(RemoteSyncProgress(current: 0, total: totalMonthsToProcess))

        // Evict unanchored cache entries: months in snapshotCache but absent from both current
        // and previous remote digests. These can arise from optimistic uploads whose flush never
        // committed a month sqlite. A scoped run only considers in-scope cached months — out-of-scope
        // entries are not part of this run and must never be evicted.
        let cachedMonths = snapshotCache.allKnownMonths()
        let anchoredMonths = remoteMonths.union(previousMonths)
        let evictionCandidates = monthFilter.map { cachedMonths.intersection($0) } ?? cachedMonths
        let unanchored = evictionCandidates.subtracting(anchoredMonths)
        var evictedAny = false
        for month in unanchored {
            if snapshotCache.removeMonth(month) { evictedAny = true }
        }

        if changedMonths.isEmpty, removedMonths.isEmpty {
            snapshotCache.markSynced(Date())
            // An eviction-only sync still mutated the browser-visible library — announce it so open browser
            // presence rebuilds (the changed-branch post below covers the changed/removed case).
            if evictedAny {
                NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
            }
            let digest = snapshotCache.counts()
            let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
            syncLog.info("[SyncTiming] No changes. Total: \(Self.ms(totalElapsed))s")
            eventStream?.emitLog(
                String.localizedStringWithFormat(String(localized: "backup.remoteIndex.unchanged"), remoteMonths.count),
                level: .debug
            )
            return digest
        }

        syncLog.info("[SyncTiming] changedMonths: \(changedMonths.count), removedMonths: \(removedMonths.count)")

        // Order changed-month processing to match the run's upload order. This only affects download/log order
        // within the sync phase (sync fully precedes upload, and background discards its per-run cache); the
        // durable newest-first guarantee comes from the upload scheduler + incremental flush, not from here.
        let orderedChangedMonths = changedMonths.sorted(by: newestMonthFirst ? (>) : (<))
        let (appliedChangedMonths, processedAfterChanged) = try await processChangedMonths(
            months: orderedChangedMonths,
            client: client,
            basePath: profile.basePath,
            layout: layout,
            totalMonthsToProcess: totalMonthsToProcess,
            makeClient: makeClient,
            downloadConcurrency: downloadConcurrency,
            onSyncProgress: onSyncProgress
        )

        var appliedRemovedMonths = 0
        var processedMonthCount = processedAfterChanged

        for month in removedMonths.sorted() {
            if snapshotCache.removeMonth(month) {
                appliedRemovedMonths += 1
            }
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
        }

        // Keep the scan-time digests even for months rewritten by
        // loadManifestDirect: the stale cache costs one extra download per
        // upgraded month next sync, while refreshing post-flush would race
        // with concurrent remote writers and silently drop their updates.
        // A scoped run merges only its months into the full digest map so out-of-scope months survive.
        if let monthFilter {
            var merged = allPreviousDigests
            for month in monthFilter { merged[month] = nil }
            for (month, digest) in remoteDigests { merged[month] = digest }
            await state.updateRemoteManifestDigests(merged)
        } else {
            await state.updateRemoteManifestDigests(remoteDigests)
        }

        snapshotCache.markSynced(Date())
        // This sync committed changes to the browser-visible remote library — announce it once so an open
        // browser's LibraryPresenceIndex rebuilds off the now-updated cache (not a pre-reload snapshot).
        NotificationCenter.default.post(name: .RemoteLibrarySnapshotDidChange, object: nil)
        let digest = snapshotCache.counts()
        let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
        syncLog.info("[SyncTiming] Sync complete. Total: \(Self.ms(totalElapsed))s, changed: \(appliedChangedMonths), removed: \(appliedRemovedMonths)")

        return digest
    }

    // Downloads + applies the changed months' manifests. Serial by default; bounded-concurrent (one client
    // per worker) when a client factory is supplied and the workload/layout allow it. Returns the applied
    // count plus the running processed count, so the removed-months loop can continue progress from it.
    private func processChangedMonths(
        months: [LibraryMonthKey],
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        totalMonthsToProcess: Int,
        makeClient: (@Sendable () throws -> any RemoteStorageClientProtocol)?,
        downloadConcurrency: Int,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?
    ) async throws -> (applied: Int, processed: Int) {
        let totalWorkers = min(downloadConcurrency, months.count)
        // V1 may schema-push under loadManifestDirect; only the pure-read .lite path is parallelized.
        guard let makeClient, totalWorkers >= 2, layout != .v1 else {
            return try await processChangedMonthsSerially(
                months: months,
                client: client,
                basePath: basePath,
                layout: layout,
                totalMonthsToProcess: totalMonthsToProcess,
                onSyncProgress: onSyncProgress
            )
        }

        // Fresh pool holds totalWorkers-1 connections; worker 0 reuses the caller-owned primary client, so
        // the data-plane peak is totalWorkers connections — ≤ the backup execution pool, never one more.
        // (The write-lock client, when a run holds one, is carried identically in both phases.)
        let pool = StorageClientPool(maxConnections: totalWorkers - 1, makeClient: makeClient)

        let queue = MonthKeyQueue(months: months)
        let aggregator = SyncProgressAggregator(total: totalMonthsToProcess, onProgress: onSyncProgress)
        do {
            try Task.checkCancellation()
            try await withThrowingTaskGroup(of: Void.self) { group in
                for workerIndex in 0 ..< totalWorkers {
                    group.addTask { [self] in
                        // Worker 0 uses the already-connected primary and starts downloading immediately (no
                        // handshake on the critical path). Other workers open their pooled connection lazily,
                        // concurrently with worker 0's downloads; if a connection can't be established this
                        // worker bows out and the primary (plus any worker that did connect) drains the rest.
                        let isPrimary = workerIndex == 0
                        let workerClient: RemoteStorageClientProtocol
                        if isPrimary {
                            workerClient = client
                        } else {
                            do {
                                workerClient = try await pool.acquire()
                            } catch {
                                if error is CancellationError || Task.isCancelled { throw error }
                                return
                            }
                        }
                        do {
                            try await self.runDownloadWorker(
                                client: workerClient,
                                basePath: basePath,
                                layout: layout,
                                queue: queue,
                                aggregator: aggregator
                            )
                        } catch {
                            if !isPrimary { await pool.release(workerClient, reusable: true) }
                            throw error
                        }
                        if !isPrimary { await pool.release(workerClient, reusable: true) }
                    }
                }
                try await group.waitForAll()
            }
        } catch {
            await pool.shutdown()
            throw error
        }
        await pool.shutdown()
        return (await aggregator.appliedCount(), await aggregator.processedCount())
    }

    private func processChangedMonthsSerially(
        months: [LibraryMonthKey],
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        totalMonthsToProcess: Int,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?
    ) async throws -> (applied: Int, processed: Int) {
        var applied = 0
        var processed = 0
        for month in months {
            try Task.checkCancellation()
            let didApply = try await downloadAndApplyMonth(
                month: month,
                client: client,
                basePath: basePath,
                layout: layout
            )
            if didApply { applied += 1 }
            processed += 1
            onSyncProgress?(RemoteSyncProgress(current: processed, total: totalMonthsToProcess))
        }
        return (applied, processed)
    }

    private func runDownloadWorker(
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        queue: MonthKeyQueue,
        aggregator: SyncProgressAggregator
    ) async throws {
        while let month = await queue.next() {
            try Task.checkCancellation()
            let didApply = try await downloadAndApplyMonth(
                month: month,
                client: client,
                basePath: basePath,
                layout: layout
            )
            await aggregator.recordAndReport(appliedDelta: didApply ? 1 : 0)
        }
    }

    // Downloads one month's manifest and applies it to the snapshot cache; returns whether the cache changed.
    // Pure read for `.lite` (no ownership assertion, no schema push). snapshotCache is NSLock-guarded, so
    // concurrent application from multiple workers is safe.
    private func downloadAndApplyMonth(
        month: LibraryMonthKey,
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout
    ) async throws -> Bool {
        let monthStart = CFAbsoluteTimeGetCurrent()
        guard let store = try await MonthManifestStore.loadManifestDirect(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            layout: layout,
            pushSchemaUpgrade: layout == .v1
        ) else {
            throw NSError(
                domain: "RemoteIndexSyncService",
                code: -21,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.remoteIndex.error.missingMonthManifest"),
                        month.text
                    )
                ]
            )
        }
        let downloadElapsed = CFAbsoluteTimeGetCurrent() - monthStart

        let processStart = CFAbsoluteTimeGetCurrent()
        let snapshot = store.unsortedSnapshot()
        let applied = snapshotCache.replaceMonth(
            month,
            resources: snapshot.resources,
            assets: snapshot.assets,
            assetResourceLinks: snapshot.links
        )
        let processElapsed = CFAbsoluteTimeGetCurrent() - processStart
        syncLog.info(
            "[SyncTiming] Month \(month.text): download=\(Self.ms(downloadElapsed))s, process=\(Self.ms(processElapsed))s, assets=\(snapshot.assets.count), resources=\(snapshot.resources.count), links=\(snapshot.links.count)"
        )
        return applied
    }

    /// Backup workers reconcile inline via `MonthManifestStore.loadOrCreate`.
    /// `assertOwnership`, when provided (Lite write lease), must confirm ownership before the reconcile
    /// flush; ownership failures fail closed so we never push a manifest we no longer own.
    func verifyMonth(
        client: RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        layout: MonthManifestStore.ManifestLayout,
        assertOwnership: MonthManifestOwnershipAssertion? = nil
    ) async throws {
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let manifestPath = layout.manifestAbsolutePath(basePath: basePath, year: month.year, month: month.month)
        func missingManifestError() -> NSError {
            NSError(
                domain: "RemoteIndexSyncService",
                code: -1,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadExistingManifest"),
                        monthRelativePath
                    )
                ]
            )
        }
        // Confirmed-absent canonical (evicted): the download must fail this month closed, unlike the transient
        // -1 above which keeps last-known-good cache and stays continuable.
        func confirmedMissingManifestError() -> NSError {
            NSError(
                domain: "RemoteIndexSyncService",
                code: -2,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadExistingManifest"),
                        monthRelativePath
                    )
                ]
            )
        }
        // Reconcile proved rows invalid in memory but the correction could not be durably persisted/published —
        // any fault in the prove→publish window (data-directory listing, listing reconcile, or corrective flush),
        // so the cache still holds the un-pruned rows: the download must fail this month closed (like -2) rather
        // than continue over a manifest this verify just proved invalid.
        func reconcileFlushFailedError() -> NSError {
            NSError(
                domain: "RemoteIndexSyncService",
                code: -3,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadExistingManifest"),
                        monthRelativePath
                    )
                ]
            )
        }
        // Pre-check distinguishes "manifest gone" (drop stale cache entry) from "download failed" (error);
        // `loadManifestDirect` collapses both into nil. A directory at the canonical slot is damaged/foreign
        // control state: an owned verify fails it closed (existingLiteManifestConflict, which is not a
        // continuable download-verify signal) so the month is never certified completed over it; a read-only
        // verify evicts the stale cache entry like a genuinely absent manifest.
        let manifestMetadata = try await client.metadata(path: manifestPath)
        if manifestMetadata == nil || manifestMetadata?.isDirectory == true {
            if let assertOwnership {
                try await assertOwnership()
                if manifestMetadata?.isDirectory == true {
                    throw LiteRepoError.existingLiteManifestConflict(month: month.text)
                }
                // Owned proof the canonical is absent: evict the stale cached month so a download/restore
                // consumer can't read it as current truth, and fail the month closed (confirmed, not transient)
                // so the download never falsely completes it from the cache this verify just evicted.
                _ = snapshotCache.removeMonth(month)
                throw confirmedMissingManifestError()
            }
            _ = snapshotCache.removeMonth(month)
            return
        }

        let store: MonthManifestStore
        do {
            guard let loaded = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: basePath,
                year: month.year,
                month: month.month,
                layout: layout,
                manifestAbsolutePath: manifestPath,
                // An owned verify must persist a schema-only migration; the upgrade is gated inside
                // loadManifestDirect by `assertOwnership != nil`, so a read-only verify still never writes.
                pushSchemaUpgrade: true,
                assertOwnership: assertOwnership,
                // Surface a clear download not-found (canonical deleted between probe and fetch) as a throw so
                // the catch can evict, rather than collapsing it into the transient nil path below.
                surfaceDownloadNotFound: true
            ) else {
                // metadata proved the canonical present but the download faulted transiently (not a clear
                // not-found): the canonical still exists, so keep the cache so the download can use last-known-good.
                throw missingManifestError()
            }
            store = loaded
        } catch {
            // Evict the stale cached month only when the verify proves the *current canonical itself* unusable, so
            // the download path can't restore from it: a confirmed-corrupt load (-34/-35) or a clear not-found on
            // the initial canonical fetch (deleted between the metadata probe and the download). A later owned
            // schema-flush/read-back failure — even one whose error chain wraps a not-found — must surface, not
            // evict; a transient initial download fault (loadManifestDirect → nil → missingManifestError) keeps
            // the cache. Match the dedicated marker, never a chain-classified not-found.
            if Self.isConfirmedInvalidManifest(error) {
                _ = snapshotCache.removeMonth(month)
                throw error
            }
            if MonthManifestStore.isManifestDownloadNotFoundError(error) {
                _ = snapshotCache.removeMonth(month)
                throw confirmedMissingManifestError()
            }
            throw error
        }

        let internalResult = try store.reconcileMonth()

        // From the in-memory prune above until the corrected manifest is published, the store may already hold
        // proof that rows are invalid (store.dirty). A non-cancellation/non-lease-loss fault anywhere in this
        // window — the data-directory listing probe, the V1 list, the listing reconcile, or the corrective flush —
        // must fail the download closed (-3); the raw retryable fault would otherwise read continuable and let a
        // consumer restore from the still-un-pruned cache this verify just proved invalid.
        do {
            let monthAbsolutePath = RemotePathBuilder.absolutePath(
                basePath: basePath,
                remoteRelativePath: monthRelativePath
            )
            let remoteFileNames: Set<String>
            if layout == .lite {
                // Lite stores month truth in .watermelon/months; a confirmed missing data directory collapses
                // to an empty listing, but any other fault surfaces. A destructive prune (whole-month clear or
                // large-ratio) of the manifest must be confirmed by a second listing before it is applied.
                let listing = try await LiteDataDirectoryProbe.probe(client: client, monthAbsolutePath: monthAbsolutePath)
                switch try await LiteDataDirectoryProbe.confirmPrune(
                    client: client,
                    monthAbsolutePath: monthAbsolutePath,
                    initial: listing,
                    manifestFileNames: store.manifestFileNames()
                ) {
                case .reconcile(let names, _):
                    remoteFileNames = names
                case .skip:
                    remoteFileNames = store.manifestFileNames()   // no listing-based prune this run
                }
            } else {
                let entries = try await client.list(path: monthAbsolutePath)
                remoteFileNames = Set(entries
                    .filter { !$0.isDirectory && $0.name != MonthManifestStore.manifestFileName }
                    .map(\.name))
            }
            let listingMissing = store.manifestFileNames().subtracting(remoteFileNames)
            let listingResult = try store.reconcileMonth(missingFileNames: listingMissing)

            let touched = internalResult.removedResourceCount + internalResult.removedAssetCount
                + internalResult.removedOrphanLinkCount
                + listingResult.removedResourceCount + listingResult.removedAssetCount
                + listingResult.removedOrphanLinkCount

            // Flush only a reconcile prune (touched > 0). A read-only verify never writes, and an owned schema-only
            // upgrade was already flushed inside loadManifestDirect. The store-owned ownership gate inside
            // flushToRemote re-asserts the lease and fails closed if it is lost.
            if touched > 0, store.dirty {
                try await store.flushToRemote()
            }
            if touched > 0 {
                syncLog.info("[verify] \(month.text): internal=\(internalResult.removedAssetCount)+\(internalResult.removedOrphanLinkCount)L, listing=\(listingResult.removedAssetCount)+\(listingResult.removedOrphanLinkCount)L")
            }
        } catch {
            // Cancellation and lease-loss carry their own download dispositions (cancelled / upload-fail-fast);
            // surface them unchanged. Otherwise, once reconcile proved rows invalid (store.dirty) but the
            // correction could not be durably published, fail closed (-3) rather than surface the raw retryable
            // fault. With nothing proven invalid yet (not dirty), a transient listing fault stays continuable over
            // the still-valid cache (a genuinely missing data file is caught later as an incomplete-item / 404).
            if store.dirty,
               RemoteFaultLite.classify(error) != .cancelled,
               !((error as? LiteRepoError)?.isUploadFailFast ?? false) {
                throw reconcileFlushFailedError()
            }
            throw error
        }

        // Publish the manifest this verify just loaded even when nothing was pruned, so a read/restore consumer
        // reads the freshly verified current month — not a staler cached one behind it. replaceMonth is
        // content-aware, so an already-current cache stays an untouched no-op.
        let snapshot = store.unsortedSnapshot()
        _ = snapshotCache.replaceMonth(
            month,
            resources: snapshot.resources,
            assets: snapshot.assets,
            assetResourceLinks: snapshot.links
        )
    }

    func healthDigest() -> RemoteHealthDigest {
        snapshotCache.healthDigest()
    }

    func allKnownMonths() -> Set<LibraryMonthKey> {
        snapshotCache.allKnownMonths()
    }

    // Directory entries occupying a `<YYYY-MM>.sqlite` Lite month slot. The digest scan skips these (a
    // directory is not a readable manifest), so an owned full verify must enumerate them separately to fail
    // closed on damaged control state instead of silently certifying the repo healthy.
    static func directoryValuedLiteMonthSlots(in entries: [RemoteStorageEntry]) -> Set<LibraryMonthKey> {
        Set(entries.compactMap { $0.isDirectory ? RepoLayoutLite.month(fromFilename: $0.name) : nil })
    }

    func remoteMonthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        snapshotCache.monthRawData(for: month)
    }

    func currentState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        snapshotCache.state(since: revision)
    }

    func snapshotRevision() -> UInt64 {
        snapshotCache.currentRevision()
    }

    func snapshotContainsAssetFingerprint(_ fingerprint: Data) -> (contains: Bool, profileKey: String?) {
        snapshotCache.containsAssetFingerprint(fingerprint)
    }

    // The out-of-band cache writers below carry the acting profile's key and are dropped once the shared
    // cache belongs to another profile (a cross-profile connect re-tagged it mid-action) — the acting
    // profile's next sync re-establishes its view. An explicit nil bypasses the gate (test seeding).
    func upsertCachedResource(_ item: RemoteManifestResource, expectedProfileKey: String?) {
        snapshotCache.upsertResource(item, onlyIfOwnedBy: expectedProfileKey)
    }

    func upsertCachedAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]? = nil, expectedProfileKey: String?) {
        snapshotCache.upsertAsset(asset, links: links, onlyIfOwnedBy: expectedProfileKey)
    }

    func replaceCachedMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        links: [RemoteAssetResourceLink],
        expectedProfileKey: String?
    ) {
        _ = snapshotCache.replaceMonth(month, resources: resources, assets: assets, assetResourceLinks: links, onlyIfOwnedBy: expectedProfileKey)
    }

    // Runs an out-of-band cache mutation (the on-demand delete path) under the SyncGate, so an in-flight
    // syncIndex can't land a stale (pre-delete) manifest on top of it — syncIndex's own commits hold the gate;
    // the backup hot path's writers stay gate-free and rely on the owner check against a racing reload.
    // If the acquire is cancelled while queued behind a long sync, apply anyway rather than leave the cache
    // stale — the write is an atomic NSLock update, no worse than the pre-gate behaviour.
    private func underSyncGateOrDirect(_ body: @escaping () async -> Void) async {
        do { try await syncGate.withLock(body) } catch { await body() }
    }

    // Delete-path variant of replaceCachedMonth (see underSyncGateOrDirect for why it's gated). The FIFO gate
    // lands this AFTER an in-flight cross-profile connect sync, so the owner check is what keeps the acting
    // profile's post-delete month out of the other profile's freshly-synced cache.
    func replaceCachedMonthSynchronized(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        links: [RemoteAssetResourceLink],
        expectedProfileKey: String?
    ) async {
        await underSyncGateOrDirect { _ = self.snapshotCache.replaceMonth(month, resources: resources, assets: assets, assetResourceLinks: links, onlyIfOwnedBy: expectedProfileKey) }
    }

    // Drops the entire cached snapshot — used when the repo itself is found gone (a delete against a `.fresh`
    // repo), so no cached month keeps showing backups that no longer exist. Also clears the manifest-digest
    // state so the cache and its digest map reset together (as on a profile switch); the next sync re-establishes.
    func resetSnapshotCache(expectedProfileKey: String) async {
        await underSyncGateOrDirect {
            if self.snapshotCache.resetIfOwned(by: expectedProfileKey) {
                await self.state.resetIfOwned(by: expectedProfileKey)
            }
        }
    }

    // Forgets one month's cached manifest digest — paired with emptying that month's snapshot when the month is
    // found gone, so a later sync can't read the stale digest as "unchanged" and skip re-pulling a re-created month.
    func forgetMonthDigest(_ month: LibraryMonthKey, expectedProfileKey: String) async {
        await underSyncGateOrDirect { await self.state.forgetMonthDigest(month, ifOwnedBy: expectedProfileKey) }
    }

    static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        [
            String(profile.id ?? 0),
            profile.storageType,
            profile.host,
            String(profile.port),
            profile.shareName,
            profile.basePath,
            profile.username,
            profile.domain ?? ""
        ].joined(separator: "|")
    }

    func scanManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        layout: MonthManifestStore.ManifestLayout,
        cancellationController: BackupCancellationController? = nil,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        switch layout {
        case .v1:
            return try await scanV1ManifestDigests(
                client: client,
                basePath: basePath,
                cancellationController: cancellationController
            )
        case .lite:
            return try await scanLiteManifestDigests(
                client: client,
                basePath: basePath,
                cancellationController: cancellationController,
                liteMonthsListing: liteMonthsListing
            )
        }
    }

    private func scanV1ManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        cancellationController: BackupCancellationController?
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        let manifests = try await V1ManifestScanner(client: client, basePath: basePath).scan(
            checkCancellation: {
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
            }
        )

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(manifests.count)
        for manifest in manifests {
            digests[manifest.month] = RemoteMonthManifestDigest(
                month: manifest.month,
                manifestSize: manifest.size,
                manifestModifiedAtMs: manifest.modificationDate?.millisecondsSinceEpoch
            )
        }
        return digests
    }

    private func scanLiteManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        cancellationController: BackupCancellationController?,
        liteMonthsListing: LiteMonthsListingSnapshot?
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()

        let entries: [RemoteStorageEntry]
        if let liteMonthsListing {
            entries = try await liteMonthsListing.entries(client: client, basePath: basePath)
        } else {
            let monthsDirectory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
            do {
                entries = try await client.list(path: monthsDirectory)
            } catch {
                // Absent months directory means a Lite repo with no months yet — not a fault. Any other
                // failure (offline / permissions) must surface so we never read it as "zero months".
                if RemoteFaultLite.classify(error) == .notFound { return [:] }
                throw error
            }
        }

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(entries.count)
        for entry in entries {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            guard !entry.isDirectory, let month = RepoLayoutLite.month(fromFilename: entry.name) else { continue }
            digests[month] = RemoteMonthManifestDigest(
                month: month,
                manifestSize: entry.size,
                manifestModifiedAtMs: entry.modificationDate?.millisecondsSinceEpoch
            )
        }
        return digests
    }

    // A canonical that loaded but failed SQLite validation / cache reload (codes -34/-35 from
    // loadManifestDirect) is confirmed-corrupt current truth, distinct from a transient download fault.
    private static func isConfirmedInvalidManifest(_ error: Error) -> Bool {
        let ns = error as NSError
        return ns.domain == "MonthManifestStore" && (ns.code == -34 || ns.code == -35)
    }

    private static func ms(_ seconds: CFAbsoluteTime) -> String {
        String(format: "%.3f", seconds)
    }
}

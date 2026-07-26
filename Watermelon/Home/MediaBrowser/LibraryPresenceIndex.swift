import Foundation
import Photos

// Single source of truth for "is this asset on device / on the remote / both" across the media browser.
// Owns the two facts every presence decision needs — the fingerprint→localIdentifier reverse map (built
// from the local hash index) and the set of fingerprints present on the connected remote (from the shared
// snapshot, gated on the owning profile) — so no source, viewer, or action re-derives them independently.
//
// One instance per browser session, shared by every source + RemoteThumbnailService + the action runner.
// NSLock-guarded (not an actor) so the fingerprint→handle reads stay synchronous for thumbnail rendering.
// Handles leave this class only through the current-bytes validators — the raw map is not exposed, so a
// stale hash row (asset edited after backup) can never bind a device handle to its pre-edit fingerprint.
final class LibraryPresenceIndex: @unchecked Sendable {
    typealias HomeLocalSeedProvider = @Sendable () async -> HomeBrowserLocalSeed?

    struct BrowserLocalProjectionInput: Sendable {
        let seed: HomeBrowserLocalSeed?
        let backedUpFingerprints: Set<Data>
    }

    private let hashIndexRepository: ContentHashIndexRepository
    private let coordinator: BackupCoordinator
    // Read live: the connected remote's profile key (nil = disconnected). Presence is relative to it, so a
    // stale / other-profile snapshot must never mark on-device assets as `.both`.
    private let profileKey: () -> String?

    private let lock = NSLock()
    private var localIDByFingerprint: [Data: String] = [:]
    private var cachedHomeLocalSeed: HomeBrowserLocalSeed?
    // Two notions: `remoteFingerprints` = present in a remote manifest (raw, gates "exists / can delete");
    // `backedUpFingerprints` = present AND has real media on the remote — a partial record that still resolves
    // a photo/video counts (its local twin is genuinely backed up), a config-only / phantom record does not
    // (its local twin keeps offering Upload). Gates the presence badge and the merged-tab dedup.
    private var remoteFingerprints: Set<Data> = []
    private var backedUpFingerprints: Set<Data> = []
    // Fingerprints with at least one COMPLETE remote record (every linked resource available, fingerprint
    // matches). backedUp minus this = partial-but-has-media backups, where the device copy is the only
    // complete instance — Delete-from-Device asks for consent before destroying it.
    private var completeFingerprints: Set<Data> = []
    // True only when the committed remote/backed-up sets were built from a snapshot owned by (or nil for) the
    // active profile. False during an A→B switch — the shared cache is tagged for the incoming profile while
    // this one is still active — where an empty remote set means "unknown", not "not backed up".
    private var remotePresenceAuthoritative = false
    private var hasBuilt = false
    private var builtProfileKey: String?
    // Snapshot-cache revision the committed sets were built from — lets consumers detect that the live cache
    // has moved since (an in-place sync mutates it month-by-month and posts only once at the end).
    private var builtRevision: UInt64?
    // Bumped by every invalidate(). A refresh() captures it before its off-lock build and only commits if
    // it is unchanged — so an invalidate() that lands mid-build isn't lost to the stale result overwriting it.
    private var generation = 0
    private var refreshScheduled = false
    // While > 0, upstream signals only mark stale (no reactive rebuild); a rebuild is coalesced to one on resume.
    // Lets a batch that emits many snapshot posts (an N-item remote delete) rebuild once instead of ~N times.
    private var suspendDepth = 0
    private var refreshPendingWhileSuspended = false

    // This index is the ONE place that knows which upstream events can change presence, so UI consumers observe
    // only `.LibraryPresenceDidChange` (posted by refresh) instead of subscribing to a growing set of proxies.
    //   · RemoteLibrarySnapshotDidChange   — remote facts changed, posted AFTER the cache is updated (race-free).
    //   · ExecutionLifecycleDidChange      — a foreground execution ended; the local hash index may have changed.
    //   · BackgroundBackupRunMarkerDidChange — a background run may have changed local fingerprints without a
    //     remote re-sync (e.g. a download), which the snapshot signal wouldn't cover.
    //   · LocalIndexChangePublisher — a local index build / duplicate cleanup mutated fingerprints without any
    //     execution lease or snapshot signal (it only reaches Home otherwise).
    private var observerTokens: [NSObjectProtocol] = []
    private let localIndexChangePublisher: LocalIndexChangePublisher?
    private let homeLocalSeedProvider: HomeLocalSeedProvider?
    private var indexChangeObserverID: UUID?

    init(
        hashIndexRepository: ContentHashIndexRepository,
        coordinator: BackupCoordinator,
        profileKey: @escaping () -> String?,
        localIndexChangePublisher: LocalIndexChangePublisher? = nil,
        homeLocalSeedProvider: HomeLocalSeedProvider? = nil,
        homeLocalSeedChangeObject: AnyObject? = nil
    ) {
        self.hashIndexRepository = hashIndexRepository
        self.coordinator = coordinator
        self.profileKey = profileKey
        self.localIndexChangePublisher = localIndexChangePublisher
        self.homeLocalSeedProvider = homeLocalSeedProvider
        for name in [Notification.Name.RemoteLibrarySnapshotDidChange, .ExecutionLifecycleDidChange, .BackgroundBackupRunMarkerDidChange] {
            let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { [weak self] _ in
                self?.upstreamStateChanged(
                    clearHomeLocalSeed: name != .RemoteLibrarySnapshotDidChange
                )
            }
            observerTokens.append(token)
        }
        if homeLocalSeedProvider != nil, let homeLocalSeedChangeObject {
            let token = NotificationCenter.default.addObserver(
                forName: .HomeBrowserLocalSeedDidChange,
                object: homeLocalSeedChangeObject,
                queue: nil
            ) { [weak self] _ in
                self?.upstreamStateChanged(clearHomeLocalSeed: true)
            }
            observerTokens.append(token)
        }
        indexChangeObserverID = localIndexChangePublisher?.addObserver { [weak self] _ in
            self?.upstreamStateChanged(clearHomeLocalSeed: true)
        }
    }

    deinit {
        observerTokens.forEach { NotificationCenter.default.removeObserver($0) }
        if let indexChangeObserverID { localIndexChangePublisher?.removeObserver(indexChangeObserverID) }
    }

    // An upstream signal may have changed presence — mark stale and schedule a single rebuild. Bursts (a sync
    // committing many months, or execution start+end) collapse to one refresh via `refreshScheduled`.
    private func upstreamStateChanged(clearHomeLocalSeed: Bool) {
        invalidate(clearHomeLocalSeed: clearHomeLocalSeed)
        let shouldSchedule = lock.withLock { () -> Bool in
            // Suspended (inside a batch): defer to a single rebuild on resume instead of one per post.
            if suspendDepth > 0 { refreshPendingWhileSuspended = true; return false }
            guard !refreshScheduled else { return false }
            refreshScheduled = true
            return true
        }
        guard shouldSchedule else { return }
        Task { [weak self] in
            guard let self else { return }
            lock.withLock { self.refreshScheduled = false }
            await self.refresh()
        }
    }

    // Bracket a batch that emits many upstream posts (e.g. an N-item remote delete): while suspended, those posts
    // only mark presence stale; resume performs at most one rebuild if any landed. Balanced calls; safe to nest.
    // Explicit `refresh()` (e.g. the upload success check) is unaffected — only the reactive path is deferred.
    func suspendUpstreamRefresh() {
        lock.withLock { suspendDepth += 1 }
    }

    func resumeUpstreamRefresh() {
        let shouldRefresh = lock.withLock { () -> Bool in
            if suspendDepth > 0 { suspendDepth -= 1 }
            guard suspendDepth == 0, refreshPendingWhileSuspended else { return false }
            refreshPendingWhileSuspended = false
            return true
        }
        guard shouldRefresh else { return }
        Task { [weak self] in await self?.refresh() }
    }

    // Rebuilds both facts off the main thread. Idempotent while the owning profile is unchanged; a profile
    // A→B switch (or first call) forces a rebuild even without an explicit invalidate. Posts on change.
    // Returns whether the committed state now reflects a build at least as fresh as this call's start —
    // false means the commit was dropped (a mutation landed mid-build) and the committed sets/flag still
    // describe an OLDER build; a one-shot verdict (upload success) must not read them as current.
    @discardableResult
    func refresh(notifyOnCommit: Bool = true) async -> Bool {
        let trace = MediaBrowserLoadTrace.context
        let startedAt = CFAbsoluteTimeGetCurrent()
        let currentKey = profileKey()
        let refreshContext: (generation: Int, homeSeed: HomeBrowserLocalSeed?)? = lock.withLock {
            if hasBuilt && builtProfileKey == currentKey { return nil }
            return (generation, cachedHomeLocalSeed)
        }
        guard let refreshContext else {
            MediaBrowserLoadTrace.emit("presenceCacheHit", context: trace, startedAt: startedAt)
            return true
        }
        let homeSeed: HomeBrowserLocalSeed?
        if let cached = refreshContext.homeSeed {
            homeSeed = cached
            emitHomeSeedCacheHit(cached, trace: trace)
        } else {
            homeSeed = await loadHomeLocalSeed(trace: trace)
        }
        let hashIndexRepository = hashIndexRepository
        let coordinator = coordinator
        let built = await withCancellableDetachedAsyncValue(priority: .userInitiated) { () -> (map: [Data: String], projection: RemoteBrowserProjection, authoritative: Bool)? in
            let buildStartedAt = CFAbsoluteTimeGetCurrent()
            let databaseStartedAt = CFAbsoluteTimeGetCurrent()
            let map = homeSeed?.localIDByFingerprint
                ?? (try? hashIndexRepository.fetchLocalIdentifiersByFingerprint())
                ?? [:]
            let databaseMs = (CFAbsoluteTimeGetCurrent() - databaseStartedAt) * 1_000
            let snapshotStartedAt = CFAbsoluteTimeGetCurrent()
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            let snapshotMs = (CFAbsoluteTimeGetCurrent() - snapshotStartedAt) * 1_000
            // Reject a foreign profile's snapshot (profile-switch window): no remote context ⇒ empty set. Record
            // whether the snapshot was authoritative for the active profile so remote-write readiness (Upload)
            // can suppress during the switch window instead of reading the empty set as "not backed up".
            let authoritative = state.profileKey == nil || state.profileKey == currentKey
            let projectionStartedAt = CFAbsoluteTimeGetCurrent()
            var projectionMetrics: RemoteBrowserProjectionMetrics?
            let projection = await RemoteBrowserAssetBuilder.buildProjectionConcurrently(
                from: authoritative
                    ? state
                    : RemoteLibrarySnapshotState(
                        revision: state.revision,
                        isFullSnapshot: state.isFullSnapshot,
                        monthDeltas: [],
                        profileKey: state.profileKey
                    ),
                includeBrowserAssets: false,
                collectPresence: true,
                shouldCancel: { Task.isCancelled },
                onMetrics: { projectionMetrics = $0 }
            )
            guard let projection else { return nil }
            let projectionMs = (CFAbsoluteTimeGetCurrent() - projectionStartedAt) * 1_000
            MediaBrowserLoadTrace.emit(
                "presenceBuild",
                context: trace,
                startedAt: buildStartedAt,
                details: "local=\(map.count) localSource=\(homeSeed == nil ? "db" : "home") months=\(state.monthDeltas.count) remote=\(projection.remoteFingerprints.count) backedUp=\(projection.backedUpFingerprints.count) dbMs=\(String(format: "%.1f", databaseMs)) snapshotMs=\(String(format: "%.1f", snapshotMs)) projectMs=\(String(format: "%.1f", projectionMs))\(Self.projectionMetricsDetails(projectionMetrics))"
            )
            return (map, projection, authoritative)
        }
        guard let built else {
            MediaBrowserLoadTrace.emit("presenceCancelled", context: trace, startedAt: startedAt)
            return false
        }
        let committed: Bool = lock.withLock {
            // Drop a now-stale result rather than let it overwrite fresher state: either an invalidate() /
            // another refresh landed mid-build (generation moved), or the profile switched under us since the
            // build captured `currentKey` (a slow A build must not clobber a committed B build). Leaving
            // hasBuilt = false makes the next refresh() rebuild from the current state.
            guard generation == refreshContext.generation, profileKey() == currentKey else { return false }
            localIDByFingerprint = built.map
            cachedHomeLocalSeed = homeSeed
            remoteFingerprints = built.projection.remoteFingerprints
            backedUpFingerprints = built.projection.backedUpFingerprints
            completeFingerprints = built.projection.completeFingerprints
            remotePresenceAuthoritative = built.authoritative
            builtProfileKey = currentKey
            builtRevision = built.projection.revision
            hasBuilt = true
            return true
        }
        if committed, notifyOnCommit {
            NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: self)
        }
        MediaBrowserLoadTrace.emit(
            "presenceCommit",
            context: trace,
            startedAt: startedAt,
            details: "committed=\(committed) notify=\(notifyOnCommit)"
        )
        return committed
    }

    func prepareRemoteBrowserProjection(notifyOnCommit: Bool = false) async -> RemoteBrowserProjection? {
        let trace = MediaBrowserLoadTrace.context
        let startedAt = CFAbsoluteTimeGetCurrent()
        let buildStartedAt = CFAbsoluteTimeGetCurrent()
        let currentKey = profileKey()
        let buildContext: (
            generation: Int,
            needsPresence: Bool,
            existingMap: [Data: String]?,
            existingHomeSeed: HomeBrowserLocalSeed?
        ) = lock.withLock {
            let needsPresence = !(hasBuilt && builtProfileKey == currentKey)
            return (
                generation,
                needsPresence,
                needsPresence ? nil : localIDByFingerprint,
                cachedHomeLocalSeed
            )
        }
        let homeSeed: HomeBrowserLocalSeed?
        if let cached = buildContext.existingHomeSeed {
            homeSeed = cached
            emitHomeSeedCacheHit(cached, trace: trace)
        } else if buildContext.needsPresence {
            homeSeed = await loadHomeLocalSeed(trace: trace)
        } else {
            homeSeed = nil
        }
        let hashIndexRepository = hashIndexRepository
        let coordinator = coordinator
        let inputs = await withCancellableDetachedValue(priority: .userInitiated) { () -> (map: [Data: String], state: RemoteLibrarySnapshotState, authoritative: Bool, databaseMs: Double, snapshotMs: Double, localSource: String)? in
            let databaseStartedAt = CFAbsoluteTimeGetCurrent()
            let map = buildContext.existingMap
                ?? homeSeed?.localIDByFingerprint
                ?? (try? hashIndexRepository.fetchLocalIdentifiersByFingerprint())
                ?? [:]
            let databaseMs = (CFAbsoluteTimeGetCurrent() - databaseStartedAt) * 1_000
            guard !Task.isCancelled else { return nil }

            let snapshotStartedAt = CFAbsoluteTimeGetCurrent()
            let state = coordinator.currentRemoteSnapshotState(since: nil)
            let snapshotMs = (CFAbsoluteTimeGetCurrent() - snapshotStartedAt) * 1_000
            let authoritative = state.profileKey == nil || state.profileKey == currentKey
            let localSource = buildContext.existingMap != nil
                ? "presence"
                : (homeSeed == nil ? "db" : "home")
            return (map, state, authoritative, databaseMs, snapshotMs, localSource)
        }
        guard let inputs else {
            MediaBrowserLoadTrace.emit("remoteSharedCancelled", context: trace, startedAt: startedAt)
            return nil
        }

        let effectiveState = inputs.authoritative
            ? inputs.state
            : RemoteLibrarySnapshotState(
                revision: inputs.state.revision,
                isFullSnapshot: inputs.state.isFullSnapshot,
                monthDeltas: [],
                profileKey: inputs.state.profileKey
            )
        async let projected: (projection: RemoteBrowserProjection?, elapsedMs: Double, metrics: RemoteBrowserProjectionMetrics?) = withCancellableDetachedAsyncValue(priority: .userInitiated) {
            let projectionStartedAt = CFAbsoluteTimeGetCurrent()
            var metrics: RemoteBrowserProjectionMetrics?
            let projection = await RemoteBrowserAssetBuilder.buildProjectionConcurrently(
                from: effectiveState,
                includeBrowserAssets: true,
                collectPresence: buildContext.needsPresence,
                shouldCancel: { Task.isCancelled },
                onMetrics: { metrics = $0 }
            )
            let projectionMs = (CFAbsoluteTimeGetCurrent() - projectionStartedAt) * 1_000
            return (projection, projectionMs, metrics)
        }
        async let resolvedHandles: (handles: [Data: String], elapsedMs: Double) = withCancellableDetachedValue(priority: .userInitiated) {
            let handlesStartedAt = CFAbsoluteTimeGetCurrent()
            let mapStartedAt = CFAbsoluteTimeGetCurrent()
            var mapHits: [Data: String] = [:]
            let handleMap = homeSeed?.localIDByFingerprint ?? inputs.map
            let remoteAssetCount = effectiveState.monthDeltas.reduce(0) { $0 + $1.assets.count }
            mapHits.reserveCapacity(min(handleMap.count, remoteAssetCount))
            for delta in effectiveState.monthDeltas {
                guard !Task.isCancelled else { return ([:], 0) }
                for asset in delta.assets where mapHits[asset.assetFingerprint] == nil {
                    if let localID = handleMap[asset.assetFingerprint] {
                        mapHits[asset.assetFingerprint] = localID
                    }
                }
            }
            let mapMs = (CFAbsoluteTimeGetCurrent() - mapStartedAt) * 1_000
            if homeSeed != nil {
                MediaBrowserLoadTrace.emit(
                    "handlesHomeSeed",
                    context: trace,
                    startedAt: handlesStartedAt,
                    details: "selected=\(mapHits.count) mapMs=\(String(format: "%.1f", mapMs))"
                )
                return (mapHits, (CFAbsoluteTimeGetCurrent() - handlesStartedAt) * 1_000)
            }
            let handles = self.resolveCurrentHandles(
                mapHits: mapHits,
                mapMs: mapMs,
                trace: trace
            )
            return (handles, (CFAbsoluteTimeGetCurrent() - handlesStartedAt) * 1_000)
        }
        let (projectionResult, handlesResult) = await (projected, resolvedHandles)
        guard let baseProjection = projectionResult.projection, !Task.isCancelled else {
            MediaBrowserLoadTrace.emit("remoteSharedCancelled", context: trace, startedAt: startedAt)
            return nil
        }
        let projection = baseProjection.attachingDeviceHandles(handlesResult.handles)
        let itemCount = projection.assetsByMonth.values.reduce(0) { $0 + $1.count }
        MediaBrowserLoadTrace.emit(
            "remoteSharedBuild",
            context: trace,
            startedAt: buildStartedAt,
            details: "presence=\(buildContext.needsPresence) local=\(inputs.map.count) localSource=\(inputs.localSource) months=\(inputs.state.monthDeltas.count) assets=\(itemCount) handles=\(handlesResult.handles.count) dbMs=\(String(format: "%.1f", inputs.databaseMs)) snapshotMs=\(String(format: "%.1f", inputs.snapshotMs)) projectMs=\(String(format: "%.1f", projectionResult.elapsedMs))\(Self.projectionMetricsDetails(projectionResult.metrics)) handlesMs=\(String(format: "%.1f", handlesResult.elapsedMs))"
        )

        let commit = lock.withLock { () -> (valid: Bool, presenceCommitted: Bool) in
            guard generation == buildContext.generation, profileKey() == currentKey else {
                return (false, false)
            }
            guard buildContext.needsPresence else {
                return (hasBuilt && builtProfileKey == currentKey, false)
            }
            localIDByFingerprint = inputs.map
            cachedHomeLocalSeed = homeSeed
            remoteFingerprints = projection.remoteFingerprints
            backedUpFingerprints = projection.backedUpFingerprints
            completeFingerprints = projection.completeFingerprints
            remotePresenceAuthoritative = inputs.authoritative
            builtProfileKey = currentKey
            builtRevision = projection.revision
            hasBuilt = true
            return (true, true)
        }
        guard commit.valid else {
            MediaBrowserLoadTrace.emit("remoteSharedDropped", context: trace, startedAt: startedAt)
            return nil
        }
        if commit.presenceCommitted, notifyOnCommit {
            NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: self)
        }
        MediaBrowserLoadTrace.emit(
            "remoteSharedCommit",
            context: trace,
            startedAt: startedAt,
            details: "presence=\(commit.presenceCommitted) notify=\(notifyOnCommit)"
        )
        return inputs.authoritative ? projection : nil
    }

    private func loadHomeLocalSeed(trace: MediaBrowserLoadTrace.Context?) async -> HomeBrowserLocalSeed? {
        guard let homeLocalSeedProvider else { return nil }
        let startedAt = CFAbsoluteTimeGetCurrent()
        let seed = await homeLocalSeedProvider()
        MediaBrowserLoadTrace.emit(
            seed == nil ? "homeSeedMiss" : "homeSeedHit",
            context: trace,
            startedAt: startedAt,
            details: seed.map { "fingerprints=\($0.localIDByFingerprint.count) assets=\($0.assets.count)" } ?? ""
        )
        return seed
    }

    private func emitHomeSeedCacheHit(
        _ seed: HomeBrowserLocalSeed,
        trace: MediaBrowserLoadTrace.Context?
    ) {
        MediaBrowserLoadTrace.emit(
            "homeSeedCacheHit",
            context: trace,
            details: "fingerprints=\(seed.localIDByFingerprint.count) assets=\(seed.assets.count)"
        )
    }

    private static func projectionMetricsDetails(_ metrics: RemoteBrowserProjectionMetrics?) -> String {
        guard let metrics else { return "" }
        return " projectWorkers=\(metrics.workerCount) resources=\(metrics.resourceCount) links=\(metrics.linkCount) resourceMapMs=\(String(format: "%.1f", metrics.resourceMapMs)) linkGroupMs=\(String(format: "%.1f", metrics.linkGroupMs)) assetProjectMs=\(String(format: "%.1f", metrics.assetProjectMs)) sortMs=\(String(format: "%.1f", metrics.sortMs)) sortPerformed=\(metrics.sortPerformedMonths) sortSkipped=\(metrics.sortSkippedMonths)"
    }

    func isRemoteBrowserProjectionRenderable(
        _ projection: RemoteBrowserProjection,
        expectedProfileKey: String
    ) -> Bool {
        return Self.isRemoteBrowserProjectionRenderable(
            projectionProfileKey: projection.ownerProfileKey,
            currentProfileKey: profileKey(),
            expectedProfileKey: expectedProfileKey
        )
    }

    static func isRemoteBrowserProjectionRenderable(
        projectionProfileKey: String?,
        currentProfileKey: String?,
        expectedProfileKey: String
    ) -> Bool {
        // Revision drift makes presence absence unknown, but the captured projection remains self-consistent.
        guard currentProfileKey == expectedProfileKey else { return false }
        return projectionProfileKey == nil || projectionProfileKey == expectedProfileKey
    }

    // Forces the next refresh() to rebuild — call after a library change (download / delete) mutates state.
    func invalidate() {
        invalidate(clearHomeLocalSeed: true)
    }

    func invalidateRemoteFacts() {
        invalidate(clearHomeLocalSeed: false)
    }

    private func invalidate(clearHomeLocalSeed: Bool) {
        lock.withLock {
            hasBuilt = false
            if clearHomeLocalSeed {
                cachedHomeLocalSeed = nil
            }
            generation &+= 1
        }
    }

    // Home's staleness rule: a hash row older than the asset's modificationDate no longer describes the
    // current bytes (edited after backup).
    static func isRowCurrent(recordUpdatedAt: Date, assetModificationDate: Date?) -> Bool {
        guard let assetModificationDate else { return true }
        return assetModificationDate <= recordUpdatedAt
    }

    // Local-render authority: the handle is returned only while its hash row still maps to this fingerprint
    // AND describes the asset's current bytes — an edited-after-backup asset must never be rendered (L1) or
    // published (shared L2 sidecar) as this fingerprint. Off-main only (single-row SQL + PHAsset fetch).
    func localIdentifierForCurrentBytes(_ fingerprint: Data) -> String? {
        localIdentifiersForCurrentBytes([fingerprint])[fingerprint]
    }

    // Batch form for projection builds that bind many handles per load (one chunked row fetch + one PHAsset
    // fetch over just the map-hit IDs). Same drop rules as the single-item helper. Off-main only.
    // The reverse map keeps one arbitrary row per fingerprint, so an older stale row can shadow a current
    // one (download-back of an edited-after-backup asset; a limited-access-excluded twin) — before dropping
    // a failed candidate, fall back to the fingerprint's other rows and bind any current one.
    func localIdentifiersForCurrentBytes(
        _ fingerprints: some Sequence<Data>,
        trace: MediaBrowserLoadTrace.Context? = nil
    ) -> [Data: String] {
        let mapStartedAt = CFAbsoluteTimeGetCurrent()
        let mapHits: [Data: String] = lock.withLock {
            var hits: [Data: String] = [:]
            for fingerprint in fingerprints where hits[fingerprint] == nil {
                if let localID = localIDByFingerprint[fingerprint] { hits[fingerprint] = localID }
            }
            return hits
        }
        let mapMs = (CFAbsoluteTimeGetCurrent() - mapStartedAt) * 1_000
        return resolveCurrentHandles(mapHits: mapHits, mapMs: mapMs, trace: trace)
    }

    private func resolveCurrentHandles(
        mapHits: [Data: String],
        mapMs: Double,
        trace: MediaBrowserLoadTrace.Context?
    ) -> [Data: String] {
        let startedAt = CFAbsoluteTimeGetCurrent()
        guard !mapHits.isEmpty else { return [:] }
        var known = currentFingerprints(
            forAssetIDs: mapHits.values,
            trace: trace,
            traceLabel: "primary",
            includeAllIndexedAssets: true
        )
        guard !Task.isCancelled else { return [:] }
        let failed = mapHits.filter { known[$0.value] != $0.key }.map(\.key)
        var alternatives: [Data: [String]] = [:]
        var fallbackDatabaseMs = 0.0
        var fallbackHandlesMs = 0.0
        if !failed.isEmpty {
            alternatives = Self.prevalidatedAlternatives(
                for: Set(failed),
                currentFingerprintsByAssetID: known
            )
        }
        let unresolved = failed.filter { alternatives[$0]?.isEmpty != false }
        if !unresolved.isEmpty {
            let fallbackDatabaseStartedAt = CFAbsoluteTimeGetCurrent()
            let fetchedAlternatives = (try? hashIndexRepository.fetchAssetIDsByFingerprints(Set(unresolved))) ?? [:]
            fallbackDatabaseMs = (CFAbsoluteTimeGetCurrent() - fallbackDatabaseStartedAt) * 1_000
            for (fingerprint, assetIDs) in fetchedAlternatives {
                alternatives[fingerprint, default: []].append(contentsOf: assetIDs)
            }
            let alternativeIDs = Set(fetchedAlternatives.values.joined())
                .subtracting(known.keys)
                .subtracting(mapHits.values)
            if !alternativeIDs.isEmpty {
                let fallbackHandlesStartedAt = CFAbsoluteTimeGetCurrent()
                known.merge(
                    currentFingerprints(
                        forAssetIDs: alternativeIDs,
                        trace: trace,
                        traceLabel: "fallback"
                    )
                ) { first, _ in first }
                guard !Task.isCancelled else { return [:] }
                fallbackHandlesMs = (CFAbsoluteTimeGetCurrent() - fallbackHandlesStartedAt) * 1_000
            }
        }
        let selectionStartedAt = CFAbsoluteTimeGetCurrent()
        let selected = Self.selectCurrentHandles(
            mapHits: mapHits,
            alternativesByFingerprint: alternatives,
            currentFingerprintsByAssetID: known
        )
        let selectionMs = (CFAbsoluteTimeGetCurrent() - selectionStartedAt) * 1_000
        MediaBrowserLoadTrace.emit(
            "handlesResolve",
            context: trace,
            startedAt: startedAt,
            details: "requested=\(mapHits.count) selected=\(selected.count) failed=\(failed.count) prevalidated=\(failed.count - unresolved.count) queried=\(unresolved.count) mapMs=\(String(format: "%.1f", mapMs)) fallbackDbMs=\(String(format: "%.1f", fallbackDatabaseMs)) fallbackHandlesMs=\(String(format: "%.1f", fallbackHandlesMs)) selectionMs=\(String(format: "%.1f", selectionMs))"
        )
        return selected
    }

    static func prevalidatedAlternatives(
        for fingerprints: Set<Data>,
        currentFingerprintsByAssetID: [String: Data]
    ) -> [Data: [String]] {
        var result: [Data: [String]] = [:]
        for (assetID, fingerprint) in currentFingerprintsByAssetID where fingerprints.contains(fingerprint) {
            result[fingerprint, default: []].append(assetID)
        }
        return result
    }

    // Pure core of the batch validator's selection (fetches injected) so the shadowed-row rule is pinnable:
    // the map candidate wins while its row still fingerprints the current bytes; otherwise the first current
    // alternative row binds; else the fingerprint drops.
    static func selectCurrentHandles(
        mapHits: [Data: String],
        alternativesByFingerprint: [Data: [String]],
        currentFingerprintsByAssetID: [String: Data]
    ) -> [Data: String] {
        var result: [Data: String] = [:]
        for (fingerprint, candidate) in mapHits {
            if currentFingerprintsByAssetID[candidate] == fingerprint {
                result[fingerprint] = candidate
            } else if let alternative = alternativesByFingerprint[fingerprint]?.first(where: { currentFingerprintsByAssetID[$0] == fingerprint }) {
                result[fingerprint] = alternative
            }
        }
        return result
    }

    // Current-bytes fingerprints for device assets, keyed by localIdentifier: a row older than the asset's
    // edit is dropped, as is an unfetchable asset (deleted, or outside a limited-access selection) — neither
    // proves the current bytes match the row's fingerprint. Off-main only.
    func currentFingerprints(
        forAssetIDs assetIDs: some Collection<String>,
        trace: MediaBrowserLoadTrace.Context? = nil,
        traceLabel: String = "",
        includeAllIndexedAssets: Bool = false
    ) -> [String: Data] {
        let startedAt = CFAbsoluteTimeGetCurrent()
        guard !assetIDs.isEmpty else { return [:] }
        let ids = Set(assetIDs)
        let photoQueryStartedAt = CFAbsoluteTimeGetCurrent()
        let seededLibraryCount = lock.withLock { cachedHomeLocalSeed?.assets.count }
        var libraryCount: Int?
        let fullLibrary: PHFetchResult<PHAsset>? = {
            guard ids.count >= 1_000 else { return nil }
            if let seededLibraryCount {
                libraryCount = seededLibraryCount
                guard Self.shouldScanFullLibrary(
                    requestedCount: ids.count,
                    libraryCount: seededLibraryCount
                ) else { return nil }
            }
            let assets = PHAsset.fetchAssets(with: PHFetchOptions())
            libraryCount = assets.count
            return Self.shouldScanFullLibrary(
                requestedCount: ids.count,
                libraryCount: assets.count
            ) ? assets : nil
        }()
        let photoQueryMs = (CFAbsoluteTimeGetCurrent() - photoQueryStartedAt) * 1_000
        let scansFullLibrary = fullLibrary != nil

        let databaseStartedAt = CFAbsoluteTimeGetCurrent()
        let records = scansFullLibrary
            ? (try? hashIndexRepository.fetchAssetFingerprintRecords()) ?? [:]
            : (try? hashIndexRepository.fetchAssetFingerprintRecords(assetIDs: ids)) ?? [:]
        let databaseMs = (CFAbsoluteTimeGetCurrent() - databaseStartedAt) * 1_000
        let photoStartedAt = CFAbsoluteTimeGetCurrent()
        var modificationDateByID: [String: Date?] = [:]
        var cancelled = false
        if let fullLibrary {
            fullLibrary.enumerateObjects { asset, _, stop in
                guard !Task.isCancelled else {
                    cancelled = true
                    stop.pointee = true
                    return
                }
                guard includeAllIndexedAssets
                    ? records[asset.localIdentifier] != nil
                    : ids.contains(asset.localIdentifier) else { return }
                modificationDateByID[asset.localIdentifier] = asset.modificationDate
            }
        } else {
            PHAsset.fetchAssets(withLocalIdentifiers: Array(ids), options: nil).enumerateObjects { asset, _, stop in
                guard !Task.isCancelled else {
                    cancelled = true
                    stop.pointee = true
                    return
                }
                modificationDateByID[asset.localIdentifier] = asset.modificationDate
            }
        }
        let photoEnumerationMs = (CFAbsoluteTimeGetCurrent() - photoStartedAt) * 1_000
        guard !cancelled else { return [:] }
        let projectionStartedAt = CFAbsoluteTimeGetCurrent()
        let current = Self.currentFingerprints(
            records: records,
            modificationDateByAssetID: modificationDateByID
        )
        let projectionMs = (CFAbsoluteTimeGetCurrent() - projectionStartedAt) * 1_000
        let photoMs = photoQueryMs + photoEnumerationMs
        MediaBrowserLoadTrace.emit(
            "handlesCurrent",
            context: trace,
            startedAt: startedAt,
            details: "pass=\(traceLabel) strategy=\(scansFullLibrary ? "all" : "ids") library=\(libraryCount.map { String($0) } ?? "-") requested=\(ids.count) records=\(records.count) photos=\(modificationDateByID.count) current=\(current.count) dbMs=\(String(format: "%.1f", databaseMs)) photoMs=\(String(format: "%.1f", photoMs)) photoQueryMs=\(String(format: "%.1f", photoQueryMs)) photoEnumerateMs=\(String(format: "%.1f", photoEnumerationMs)) projectMs=\(String(format: "%.1f", projectionMs))"
        )
        return current
    }

    static func shouldScanFullLibrary(requestedCount: Int, libraryCount: Int) -> Bool {
        requestedCount >= 1_000 &&
            libraryCount > 0 &&
            requestedCount >= (libraryCount + 1) / 2
    }

    // Pure core of the current-bytes validators (fetch results injected) so the drop rules are pinnable.
    static func currentFingerprints(
        records: [String: LocalAssetFingerprintRecord],
        modificationDateByAssetID: [String: Date?]
    ) -> [String: Data] {
        var result: [String: Data] = [:]
        for (assetID, record) in records {
            guard let modificationDate = modificationDateByAssetID[assetID] else { continue }
            guard isRowCurrent(recordUpdatedAt: record.updatedAt, assetModificationDate: modificationDate) else { continue }
            result[assetID] = record.fingerprint
        }
        return result
    }

    // Present in a remote manifest (raw) — the asset EXISTS remotely (even if incomplete). Gates "can delete
    // from backup" and viewer/More visibility.
    func isOnRemote(_ fingerprint: Data) -> Bool {
        lock.withLock { remoteFingerprints.contains(fingerprint) }
    }

    // Absence from the committed set can mean "the build predates this month" while an in-place sync mutates
    // the shared cache month-by-month (single post at sync end) — not "gone from the remote". Destructive
    // reconcile-as-gone decisions must confirm absence against the live cache with this instead.
    // `.unknown` = the live cache doesn't answer for the active profile (mid-switch it is reset and re-tagged
    // for the incoming profile while this session is still active) — never a confirmed absence.
    enum RemoteLivePresence {
        case present
        case absent
        case unknown
    }

    func remoteLivePresence(_ fingerprint: Data) async -> RemoteLivePresence {
        if isOnRemote(fingerprint) { return .present }
        let currentKey = profileKey()
        let coordinator = coordinator
        return await withCancellableDetachedValue(priority: .userInitiated) {
            let live = coordinator.snapshotContainsAssetFingerprint(fingerprint)
            return Self.classifyLivePresence(contains: live.contains, liveProfileKey: live.profileKey, currentKey: currentKey)
        }
    }

    // `internal` only so the unknown-vs-absent contract is directly pinnable by tests. An untagged cache
    // (nil key: just reset, not yet re-tagged) never authoritatively describes an active profile.
    static func classifyLivePresence(contains: Bool, liveProfileKey: String?, currentKey: String?) -> RemoteLivePresence {
        guard liveProfileKey == currentKey else { return .unknown }
        return contains ? .present : .absent
    }

    // True when the committed sets were built from the cache's current revision. False = a mutation landed
    // after the build (e.g. a mid-flight in-place sync): set-absence is then "unknown", not "gone".
    var isRemotePresenceCurrent: Bool {
        let built: UInt64? = lock.withLock { hasBuilt ? builtRevision : nil }
        guard let built else { return false }
        return built == coordinator.currentSnapshotRevision()
    }

    // Present AND has real media on the remote — a partial-but-has-media record still counts as backed up.
    // Gates the presence badge and whether a local copy still needs Upload. A config-only / phantom record
    // is on the remote (isOnRemote) but NOT backed up.
    func isBackedUp(_ fingerprint: Data) -> Bool {
        lock.withLock { backedUpFingerprints.contains(fingerprint) }
    }

    func browserLocalProjectionInput() -> BrowserLocalProjectionInput {
        let currentKey = profileKey()
        return lock.withLock {
            return BrowserLocalProjectionInput(
                seed: hasBuilt ? cachedHomeLocalSeed : nil,
                backedUpFingerprints: builtProfileKey == currentKey ? backedUpFingerprints : []
            )
        }
    }

    // At least one remote record for this fingerprint is complete. False for a partial-but-has-media backup
    // (still isBackedUp, badge `.both`) — the device copy is then the only complete instance, so the delete
    // paths ask for consent before removing it. Mirrors the download path's incomplete-record consent.
    func hasCompleteBackup(_ fingerprint: Data) -> Bool {
        lock.withLock { completeFingerprints.contains(fingerprint) }
    }

    // Whether the remote/backed-up sets authoritatively reflect the active profile. False during a profile
    // switch's reload window: the committed build is still the previous profile's while `profileKey()` already
    // answers for the new one, so an item's `.localOnly` projection is UNKNOWN rather than confirmed. Remote-
    // write action readiness (Upload) must not treat that as "safe to upload" — re-checking the built profile
    // against the live one catches the window the stored flag alone would miss.
    var isRemotePresenceAuthoritative: Bool {
        let currentKey = profileKey()
        return lock.withLock { hasBuilt && remotePresenceAuthoritative && builtProfileKey == currentKey }
    }
}

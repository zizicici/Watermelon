import Foundation
import Photos

actor PresenceRefreshSingleFlight {
    struct Key: Hashable, Sendable {
        let localGeneration: Int
        let remoteGeneration: Int
        let profileKey: String?
        let remoteRevision: UInt64
    }

    enum Outcome: Equatable {
        case current
        case committed
        case failed
    }

    private struct Flight {
        let task: Task<Outcome, Never>
        var wantsNotification: Bool
    }

    private var flights: [Key: Flight] = [:]

    func run(
        key: Key,
        notifyOnCommit: Bool,
        operation: @escaping @Sendable () async -> Outcome
    ) async -> (outcome: Outcome, shouldNotify: Bool) {
        if var flight = flights[key] {
            flight.wantsNotification = flight.wantsNotification || notifyOnCommit
            flights[key] = flight
            return (await flight.task.value, false)
        }

        for (otherKey, flight) in flights where otherKey != key {
            flight.task.cancel()
        }
        let task = Task { await operation() }
        flights[key] = Flight(task: task, wantsNotification: notifyOnCommit)
        let outcome = await task.value
        guard let flight = flights.removeValue(forKey: key) else { return (outcome, false) }
        let shouldNotify = outcome == .committed && flight.wantsNotification
        return (outcome, shouldNotify)
    }
}

struct PresenceInvalidationGate {
    private(set) var suspendDepth = 0
    private(set) var hasPendingInvalidation = false

    mutating func suspend() {
        suspendDepth += 1
    }

    mutating func recordInvalidation() -> Bool {
        guard suspendDepth > 0 else { return true }
        hasPendingInvalidation = true
        return false
    }

    mutating func resume() -> Bool {
        guard suspendDepth > 0 else { return false }
        suspendDepth -= 1
        guard suspendDepth == 0, hasPendingInvalidation else { return false }
        hasPendingInvalidation = false
        return true
    }
}

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

    private enum InvalidationScope {
        case local
        case remote
        case all
    }

    struct BrowserLocalProjectionInput: Sendable {
        let seed: HomeBrowserLocalSeed?
        let backedUpFingerprints: Set<Data>
    }

    enum BackupPresenceVerdict: Equatable, Sendable {
        case complete
        case incomplete
        case absent
        case unknown

        var isBackedUp: Bool {
            self == .complete || self == .incomplete
        }
    }

    private struct CommittedState {
        var localIDByFingerprint: [Data: String] = [:]
        var homeLocalSeed: HomeBrowserLocalSeed?
        var localIsBuilt = false
        var remoteFingerprints: Set<Data> = []
        var backedUpFingerprints: Set<Data> = []
        var completeFingerprints: Set<Data> = []
        var remoteIsAuthoritative = false
        var remoteIsBuilt = false
        var remoteProfileKey: String?
        var remoteRevision: UInt64?

        mutating func invalidateLocal(clearHomeLocalSeed: Bool) {
            localIsBuilt = false
            if clearHomeLocalSeed { homeLocalSeed = nil }
        }

        mutating func invalidateRemote() {
            remoteIsBuilt = false
        }

        mutating func commitLocal(
            localIDByFingerprint: [Data: String],
            homeLocalSeed: HomeBrowserLocalSeed?
        ) {
            self.localIDByFingerprint = localIDByFingerprint
            self.homeLocalSeed = homeLocalSeed
            localIsBuilt = true
        }

        mutating func commitRemote(
            projection: RemoteBrowserProjection,
            authoritative: Bool,
            ownerProfileKey: String?
        ) {
            remoteFingerprints = projection.remoteFingerprints
            backedUpFingerprints = projection.backedUpFingerprints
            completeFingerprints = projection.completeFingerprints
            remoteIsAuthoritative = authoritative
            remoteProfileKey = ownerProfileKey
            remoteRevision = projection.revision
            remoteIsBuilt = true
        }
    }

    private let hashIndexRepository: ContentHashIndexRepository
    private let coordinator: BackupCoordinator
    private let inputLoader: LibraryPresenceInputLoader
    // Read live: the connected remote's profile key (nil = disconnected). Presence is relative to it, so a
    // stale / other-profile snapshot must never mark on-device assets as `.both`.
    private let profileKey: () -> String?
    private let refreshSingleFlight = PresenceRefreshSingleFlight()

    private let lock = NSLock()
    private var state = CommittedState()
    private var localGeneration = 0
    private var remoteGeneration = 0
    private var invalidationGate = PresenceInvalidationGate()

    // This index is the ONE place that knows which upstream events can change presence, so UI consumers observe
    // only `.LibraryPresenceDidChange` instead of subscribing to a growing set of proxies.
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
    private let homeLocalSeedChanges: ChangeSignal<Void>?
    private var homeLocalSeedChangeObserverID: UUID?

    init(
        hashIndexRepository: ContentHashIndexRepository,
        coordinator: BackupCoordinator,
        profileKey: @escaping () -> String?,
        localIndexChangePublisher: LocalIndexChangePublisher? = nil,
        homeLocalSeedProvider: HomeLocalSeedProvider? = nil,
        homeLocalSeedChanges: ChangeSignal<Void>? = nil
    ) {
        self.hashIndexRepository = hashIndexRepository
        self.coordinator = coordinator
        inputLoader = LibraryPresenceInputLoader(
            hashIndexRepository: hashIndexRepository,
            coordinator: coordinator
        )
        self.profileKey = profileKey
        self.localIndexChangePublisher = localIndexChangePublisher
        self.homeLocalSeedProvider = homeLocalSeedProvider
        self.homeLocalSeedChanges = homeLocalSeedChanges
        for name in [Notification.Name.RemoteLibrarySnapshotDidChange, .ExecutionLifecycleDidChange, .BackgroundBackupRunMarkerDidChange] {
            let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { [weak self] _ in
                self?.upstreamStateChanged(
                    scope: name == .RemoteLibrarySnapshotDidChange ? .remote : .local,
                    clearHomeLocalSeed: name != .RemoteLibrarySnapshotDidChange
                )
            }
            observerTokens.append(token)
        }
        if homeLocalSeedProvider != nil {
            homeLocalSeedChangeObserverID = homeLocalSeedChanges?.addObserver { [weak self] in
                self?.upstreamStateChanged(scope: .local, clearHomeLocalSeed: true)
            }
        }
        indexChangeObserverID = localIndexChangePublisher?.addObserver { [weak self] _ in
            self?.upstreamStateChanged(scope: .local, clearHomeLocalSeed: true)
        }
    }

    deinit {
        observerTokens.forEach { NotificationCenter.default.removeObserver($0) }
        if let indexChangeObserverID { localIndexChangePublisher?.removeObserver(indexChangeObserverID) }
        if let homeLocalSeedChangeObserverID {
            homeLocalSeedChanges?.removeObserver(homeLocalSeedChangeObserverID)
        }
    }

    private func upstreamStateChanged(
        scope: InvalidationScope,
        clearHomeLocalSeed: Bool
    ) {
        invalidate(
            scope: scope,
            clearHomeLocalSeed: clearHomeLocalSeed
        )
        publishPresenceChangeWhenAllowed()
    }

    // Bracket a batch that emits many upstream posts. Facts become stale immediately; observers receive one
    // invalidation after the outermost resume and let the active source rebuild them.
    func suspendUpstreamRefresh() {
        lock.withLock { invalidationGate.suspend() }
    }

    func resumeUpstreamRefresh() {
        let shouldNotify = lock.withLock { invalidationGate.resume() }
        if shouldNotify {
            NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: self)
        }
    }

    private func publishPresenceChangeWhenAllowed() {
        let shouldPost = lock.withLock { invalidationGate.recordInvalidation() }
        guard shouldPost else { return }
        NotificationCenter.default.post(name: .LibraryPresenceDidChange, object: self)
    }

    private struct PresenceProjectionBuild {
        let input: LibraryRemotePresenceInput
        let projection: RemoteBrowserProjection
        let elapsedMs: Double
        let metrics: RemoteBrowserProjectionMetrics?
    }

    @discardableResult
    func refresh(notifyOnCommit: Bool = true) async -> Bool {
        let currentKey = profileKey()
        let currentRevision = coordinator.currentSnapshotRevision()
        let key = PresenceRefreshSingleFlight.Key(
            localGeneration: lock.withLock { localGeneration },
            remoteGeneration: lock.withLock { remoteGeneration },
            profileKey: currentKey,
            remoteRevision: currentRevision
        )
        let result = await refreshSingleFlight.run(
            key: key,
            notifyOnCommit: notifyOnCommit
        ) { [weak self] in
            guard let self else { return .failed }
            return await self.performRefresh(for: key)
        }
        if result.shouldNotify {
            publishPresenceChangeWhenAllowed()
        }
        return result.outcome != .failed
    }

    private func performRefresh(
        for key: PresenceRefreshSingleFlight.Key
    ) async -> PresenceRefreshSingleFlight.Outcome {
        let trace = MediaBrowserLoadTrace.context
        let startedAt = CFAbsoluteTimeGetCurrent()
        guard profileKey() == key.profileKey else { return .failed }
        let refreshContext: (
            homeSeed: HomeBrowserLocalSeed?,
            needsLocal: Bool,
            needsRemote: Bool
        )? = lock.withLock {
            guard localGeneration == key.localGeneration,
                  remoteGeneration == key.remoteGeneration else { return nil }
            return (
                state.homeLocalSeed,
                !state.localIsBuilt,
                !isRemoteStateCurrentLocked(
                    profileKey: key.profileKey,
                    revision: key.remoteRevision
                )
            )
        }
        guard let refreshContext else { return .failed }
        if !refreshContext.needsLocal, !refreshContext.needsRemote {
            MediaBrowserLoadTrace.emit("presenceCacheHit", context: trace, startedAt: startedAt)
            return .current
        }

        var homeSeed = refreshContext.homeSeed
        if refreshContext.needsLocal {
            if let homeSeed {
                emitHomeSeedCacheHit(homeSeed, trace: trace)
            } else {
                homeSeed = await loadHomeLocalSeed(trace: trace)
            }
        }
        let resolvedHomeSeed = homeSeed
        let buildStartedAt = CFAbsoluteTimeGetCurrent()
        async let loadedLocal: LibraryLocalPresenceInput? = refreshContext.needsLocal
            ? inputLoader.loadLocal(homeSeed: resolvedHomeSeed)
            : nil
        async let builtRemote: PresenceProjectionBuild? = refreshContext.needsRemote
            ? buildPresenceProjection(profileKey: key.profileKey)
            : nil
        let (localInput, remoteBuild) = await (loadedLocal, builtRemote)
        guard (!refreshContext.needsLocal || localInput != nil),
              (!refreshContext.needsRemote || remoteBuild != nil),
              !Task.isCancelled else {
            MediaBrowserLoadTrace.emit("presenceCancelled", context: trace, startedAt: startedAt)
            return .failed
        }

        let localCount = localInput?.localIDByFingerprint.count
            ?? lock.withLock { state.localIDByFingerprint.count }
        let localSource = localInput?.source ?? "presence"
        let databaseMs = localInput?.databaseMs ?? 0
        MediaBrowserLoadTrace.emit(
            "presenceBuild",
            context: trace,
            startedAt: buildStartedAt,
            details: "local=\(localCount) localSource=\(localSource) months=\(remoteBuild?.input.monthCount ?? 0) remote=\(remoteBuild?.projection.remoteFingerprints.count ?? 0) backedUp=\(remoteBuild?.projection.backedUpFingerprints.count ?? 0) dbMs=\(String(format: "%.1f", databaseMs)) snapshotMs=\(String(format: "%.1f", remoteBuild?.input.snapshotMs ?? 0)) projectMs=\(String(format: "%.1f", remoteBuild?.elapsedMs ?? 0))\(Self.projectionMetricsDetails(remoteBuild?.metrics))"
        )
        let committed = commitPresenceComponents(
            localInput: localInput,
            homeSeed: resolvedHomeSeed,
            remoteBuild: remoteBuild,
            expectedLocalGeneration: key.localGeneration,
            expectedRemoteGeneration: key.remoteGeneration,
            expectedProfileKey: key.profileKey,
            liveProfileKey: profileKey(),
            liveRemoteRevision: coordinator.currentSnapshotRevision()
        )
        MediaBrowserLoadTrace.emit(
            "presenceCommit",
            context: trace,
            startedAt: startedAt,
            details: "committed=\(committed)"
        )
        return committed ? .committed : .failed
    }

    func prepareRemoteBrowserProjection(notifyOnCommit: Bool = false) async -> RemoteBrowserProjection? {
        let trace = MediaBrowserLoadTrace.context
        let startedAt = CFAbsoluteTimeGetCurrent()
        let buildStartedAt = CFAbsoluteTimeGetCurrent()
        let monthGroupingTimeZone = MonthGroupingTimeZonePreference.frozenCurrent()
        let currentKey = profileKey()
        guard let remoteInput = await inputLoader.loadRemote(profileKey: currentKey) else {
            MediaBrowserLoadTrace.emit("remoteSharedCancelled", context: trace, startedAt: startedAt)
            return nil
        }
        let buildContext: (
            localGeneration: Int,
            remoteGeneration: Int,
            needsLocal: Bool,
            needsPresence: Bool,
            existingMap: [Data: String],
            existingHomeSeed: HomeBrowserLocalSeed?,
            existingRemoteFingerprints: Set<Data>,
            existingBackedUpFingerprints: Set<Data>,
            existingCompleteFingerprints: Set<Data>
        ) = lock.withLock {
            return (
                localGeneration,
                remoteGeneration,
                !state.localIsBuilt,
                !isRemoteStateCurrentLocked(
                    profileKey: currentKey,
                    revision: remoteInput.state.revision
                ),
                state.localIDByFingerprint,
                state.homeLocalSeed,
                state.remoteFingerprints,
                state.backedUpFingerprints,
                state.completeFingerprints
            )
        }
        var homeSeed: HomeBrowserLocalSeed?
        if let cached = buildContext.existingHomeSeed,
           cached.monthGroupingTimeZone == monthGroupingTimeZone {
            homeSeed = cached
            emitHomeSeedCacheHit(cached, trace: trace)
        } else if buildContext.needsLocal {
            let loaded = await loadHomeLocalSeed(trace: trace)
            homeSeed = loaded?.monthGroupingTimeZone == monthGroupingTimeZone ? loaded : nil
        } else {
            homeSeed = nil
        }
        let resolvedHomeSeed = homeSeed
        let localInput = buildContext.needsLocal
            ? await inputLoader.loadLocal(homeSeed: resolvedHomeSeed)
            : nil
        guard !buildContext.needsLocal || localInput != nil else {
            MediaBrowserLoadTrace.emit("remoteSharedCancelled", context: trace, startedAt: startedAt)
            return nil
        }
        let localMap = localInput?.localIDByFingerprint ?? buildContext.existingMap

        async let projected: (projection: RemoteBrowserProjection?, elapsedMs: Double, metrics: RemoteBrowserProjectionMetrics?) = withCancellableDetachedAsyncValue(priority: .userInitiated) {
            let projectionStartedAt = CFAbsoluteTimeGetCurrent()
            var metrics: RemoteBrowserProjectionMetrics?
            let projection = await RemoteBrowserAssetBuilder.buildProjectionConcurrently(
                from: remoteInput.state,
                includeBrowserAssets: true,
                collectPresence: buildContext.needsPresence,
                monthGroupingTimeZone: monthGroupingTimeZone,
                shouldCancel: { Task.isCancelled },
                onMetrics: { metrics = $0 }
            )
            let projectionMs = (CFAbsoluteTimeGetCurrent() - projectionStartedAt) * 1_000
            return (projection, projectionMs, metrics)
        }
        async let resolvedHandles: (handles: [Data: String], elapsedMs: Double) = withCancellableDetachedValue(priority: .userInitiated) {
            let handlesStartedAt = CFAbsoluteTimeGetCurrent()
            let mapStartedAt = CFAbsoluteTimeGetCurrent()
            if let resolvedHomeSeed {
                MediaBrowserLoadTrace.emit(
                    "handlesHomeSeed",
                    context: trace,
                    startedAt: handlesStartedAt,
                    details: "available=\(resolvedHomeSeed.localIDByFingerprint.count) mapMs=0.0 overlay=true"
                )
                return (
                    resolvedHomeSeed.localIDByFingerprint,
                    (CFAbsoluteTimeGetCurrent() - handlesStartedAt) * 1_000
                )
            }
            var mapHits: [Data: String] = [:]
            let remoteAssetCount = remoteInput.state.monthDeltas.reduce(0) { $0 + $1.assets.count }
            mapHits.reserveCapacity(min(localMap.count, remoteAssetCount))
            for delta in remoteInput.state.monthDeltas {
                guard !Task.isCancelled else { return ([:], 0) }
                for asset in delta.assets where mapHits[asset.assetFingerprint] == nil {
                    if let localID = localMap[asset.assetFingerprint] {
                        mapHits[asset.assetFingerprint] = localID
                    }
                }
            }
            let mapMs = (CFAbsoluteTimeGetCurrent() - mapStartedAt) * 1_000
            let handles = self.resolveCurrentHandles(
                mapHits: mapHits,
                mapMs: mapMs,
                trace: trace
            )
            return (handles, (CFAbsoluteTimeGetCurrent() - handlesStartedAt) * 1_000)
        }
        let (projectionResult, handlesResult) = await (projected, resolvedHandles)
        guard let baseProjection = projectionResult.projection,
              !Task.isCancelled,
              monthGroupingTimeZone == .frozenCurrent() else {
            MediaBrowserLoadTrace.emit("remoteSharedCancelled", context: trace, startedAt: startedAt)
            return nil
        }
        let projectionWithPresence = buildContext.needsPresence
            ? baseProjection
            : baseProjection.attachingPresenceFacts(
                remoteFingerprints: buildContext.existingRemoteFingerprints,
                backedUpFingerprints: buildContext.existingBackedUpFingerprints,
                completeFingerprints: buildContext.existingCompleteFingerprints
            )
        let projection = projectionWithPresence.attachingDeviceHandles(handlesResult.handles)
        let itemCount = projection.assetsByMonth.values.reduce(0) { $0 + $1.count }
        MediaBrowserLoadTrace.emit(
            "remoteSharedBuild",
            context: trace,
            startedAt: buildStartedAt,
            details: "presence=\(buildContext.needsPresence) local=\(localMap.count) localSource=\(localInput?.source ?? "presence") months=\(remoteInput.monthCount) assets=\(itemCount) handles=\(handlesResult.handles.count) dbMs=\(String(format: "%.1f", localInput?.databaseMs ?? 0)) snapshotMs=\(String(format: "%.1f", remoteInput.snapshotMs)) projectMs=\(String(format: "%.1f", projectionResult.elapsedMs))\(Self.projectionMetricsDetails(projectionResult.metrics)) handlesMs=\(String(format: "%.1f", handlesResult.elapsedMs))"
        )

        let presenceBuild = buildContext.needsPresence
            ? PresenceProjectionBuild(
                input: remoteInput,
                projection: projection,
                elapsedMs: projectionResult.elapsedMs,
                metrics: projectionResult.metrics
            )
            : nil
        let valid = commitPresenceComponents(
            localInput: localInput,
            homeSeed: resolvedHomeSeed,
            remoteBuild: presenceBuild,
            expectedLocalGeneration: buildContext.localGeneration,
            expectedRemoteGeneration: buildContext.remoteGeneration,
            expectedProfileKey: currentKey,
            liveProfileKey: profileKey(),
            liveRemoteRevision: coordinator.currentSnapshotRevision()
        )
        guard valid else {
            MediaBrowserLoadTrace.emit("remoteSharedDropped", context: trace, startedAt: startedAt)
            return nil
        }
        if (localInput != nil || presenceBuild != nil), notifyOnCommit {
            publishPresenceChangeWhenAllowed()
        }
        MediaBrowserLoadTrace.emit(
            "remoteSharedCommit",
            context: trace,
            startedAt: startedAt,
            details: "presence=\(presenceBuild != nil) notify=\(notifyOnCommit)"
        )
        return remoteInput.isAuthoritative ? projection : nil
    }

    private func buildPresenceProjection(
        profileKey: String?
    ) async -> PresenceProjectionBuild? {
        guard let input = await inputLoader.loadRemote(profileKey: profileKey) else { return nil }
        let startedAt = CFAbsoluteTimeGetCurrent()
        var metrics: RemoteBrowserProjectionMetrics?
        let projection = await RemoteBrowserAssetBuilder.buildProjectionConcurrently(
            from: input.state,
            includeBrowserAssets: false,
            collectPresence: true,
            shouldCancel: { Task.isCancelled },
            onMetrics: { metrics = $0 }
        )
        guard let projection else { return nil }
        return PresenceProjectionBuild(
            input: input,
            projection: projection,
            elapsedMs: (CFAbsoluteTimeGetCurrent() - startedAt) * 1_000,
            metrics: metrics
        )
    }

    private func commitPresenceComponents(
        localInput: LibraryLocalPresenceInput?,
        homeSeed: HomeBrowserLocalSeed?,
        remoteBuild: PresenceProjectionBuild?,
        expectedLocalGeneration: Int,
        expectedRemoteGeneration: Int,
        expectedProfileKey: String?,
        liveProfileKey: String?,
        liveRemoteRevision: UInt64
    ) -> Bool {
        guard liveProfileKey == expectedProfileKey else { return false }
        return lock.withLock {
            if let localInput {
                guard localGeneration == expectedLocalGeneration else { return false }
                state.commitLocal(
                    localIDByFingerprint: localInput.localIDByFingerprint,
                    homeLocalSeed: homeSeed
                )
            } else if localGeneration != expectedLocalGeneration || !state.localIsBuilt {
                return false
            }

            if let remoteBuild {
                guard remoteGeneration == expectedRemoteGeneration,
                      remoteBuild.projection.revision == liveRemoteRevision else { return false }
                if state.remoteIsBuilt,
                   state.remoteProfileKey == expectedProfileKey,
                   let committedRevision = state.remoteRevision,
                   committedRevision > remoteBuild.projection.revision {
                    return false
                }
                state.commitRemote(
                    projection: remoteBuild.projection,
                    authoritative: remoteBuild.input.isAuthoritative,
                    ownerProfileKey: remoteBuild.projection.ownerProfileKey
                )
            } else if remoteGeneration != expectedRemoteGeneration ||
                        !isRemoteStateCurrentLocked(
                            profileKey: expectedProfileKey,
                            revision: liveRemoteRevision
                        ) {
                return false
            }
            return state.localIsBuilt &&
                isRemoteStateCurrentLocked(
                    profileKey: expectedProfileKey,
                    revision: liveRemoteRevision
                )
        }
    }

    private func isRemoteStateCurrentLocked(
        profileKey: String?,
        revision: UInt64
    ) -> Bool {
        Self.remoteStateIsCurrent(
            isBuilt: state.remoteIsBuilt,
            isAuthoritative: state.remoteIsAuthoritative,
            ownerProfileKey: state.remoteProfileKey,
            expectedProfileKey: profileKey,
            committedRevision: state.remoteRevision,
            liveRevision: revision
        )
    }

    static func remoteStateIsCurrent(
        isBuilt: Bool,
        isAuthoritative: Bool,
        ownerProfileKey: String?,
        expectedProfileKey: String?,
        committedRevision: UInt64?,
        liveRevision: UInt64
    ) -> Bool {
        isBuilt &&
            isAuthoritative &&
            RemoteSnapshotOwnership.matches(
                ownerProfileKey: ownerProfileKey,
                expectedProfileKey: expectedProfileKey
            ) &&
            committedRevision == liveRevision
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
        guard projection.monthGroupingTimeZone == .frozenCurrent() else { return false }
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
        return RemoteSnapshotOwnership.matches(
            ownerProfileKey: projectionProfileKey,
            expectedProfileKey: expectedProfileKey
        )
    }

    // Forces the next refresh() to rebuild — call after a library change (download / delete) mutates state.
    func invalidate() {
        invalidate(scope: .local, clearHomeLocalSeed: true)
    }

    func invalidateRemoteFacts() {
        invalidate(scope: .remote, clearHomeLocalSeed: false)
    }

    func invalidateAllFacts() {
        invalidate(scope: .all, clearHomeLocalSeed: true)
    }

    private func invalidate(
        scope: InvalidationScope,
        clearHomeLocalSeed: Bool
    ) {
        lock.withLock {
            switch scope {
            case .local:
                state.invalidateLocal(clearHomeLocalSeed: clearHomeLocalSeed)
                localGeneration &+= 1
            case .remote:
                state.invalidateRemote()
                remoteGeneration &+= 1
            case .all:
                state.invalidateLocal(clearHomeLocalSeed: clearHomeLocalSeed)
                state.invalidateRemote()
                localGeneration &+= 1
                remoteGeneration &+= 1
            }
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

    // Query the repository so a just-inserted row can stop a duplicate restore.
    func repositoryLocalIdentifiersForCurrentBytes(
        _ fingerprints: some Sequence<Data>
    ) -> [Data: String] {
        let requested = Set(fingerprints)
        guard !requested.isEmpty, !Task.isCancelled else { return [:] }
        let candidates = (try? hashIndexRepository.fetchAssetIDsByFingerprints(requested)) ?? [:]
        let candidateIDs = Set(candidates.values.joined())
        guard !candidateIDs.isEmpty, !Task.isCancelled else { return [:] }
        let current = currentFingerprints(forAssetIDs: candidateIDs)
        guard !Task.isCancelled else { return [:] }
        var result: [Data: String] = [:]
        result.reserveCapacity(candidates.count)
        for (fingerprint, assetIDs) in candidates {
            if let identifier = assetIDs.first(where: { current[$0] == fingerprint }) {
                result[fingerprint] = identifier
            }
        }
        return result
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
                if let localID = state.localIDByFingerprint[fingerprint] { hits[fingerprint] = localID }
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
        let seededLibraryCount = lock.withLock { state.homeLocalSeed?.assets.count }
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
        let currentKey = profileKey()
        let revision = coordinator.currentSnapshotRevision()
        return lock.withLock {
            isRemoteStateCurrentLocked(profileKey: currentKey, revision: revision) &&
                state.remoteIsAuthoritative &&
                state.remoteFingerprints.contains(fingerprint)
        }
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
        guard RemoteSnapshotOwnership.matches(
            ownerProfileKey: liveProfileKey,
            expectedProfileKey: currentKey
        ) else { return .unknown }
        return contains ? .present : .absent
    }

    // True when the committed sets were built from the cache's current revision. False = a mutation landed
    // after the build (e.g. a mid-flight in-place sync): set-absence is then "unknown", not "gone".
    var isRemotePresenceCurrent: Bool {
        let currentKey = profileKey()
        let revision = coordinator.currentSnapshotRevision()
        return lock.withLock {
            isRemoteStateCurrentLocked(profileKey: currentKey, revision: revision)
        }
    }

    func backupPresenceVerdict(_ fingerprint: Data) -> BackupPresenceVerdict {
        let currentKey = profileKey()
        let revision = coordinator.currentSnapshotRevision()
        return lock.withLock {
            Self.classifyBackupPresence(
                isCurrent: isRemoteStateCurrentLocked(
                    profileKey: currentKey,
                    revision: revision
                ),
                isAuthoritative: state.remoteIsAuthoritative,
                isComplete: state.completeFingerprints.contains(fingerprint),
                isBackedUp: state.backedUpFingerprints.contains(fingerprint)
            )
        }
    }

    func backupPresenceVerdicts(
        _ fingerprints: some Sequence<Data>
    ) -> [Data: BackupPresenceVerdict] {
        let requested = Set(fingerprints)
        let currentKey = profileKey()
        let revision = coordinator.currentSnapshotRevision()
        return lock.withLock {
            let isCurrent = isRemoteStateCurrentLocked(
                profileKey: currentKey,
                revision: revision
            )
            var result: [Data: BackupPresenceVerdict] = [:]
            result.reserveCapacity(requested.count)
            for fingerprint in requested {
                result[fingerprint] = Self.classifyBackupPresence(
                    isCurrent: isCurrent,
                    isAuthoritative: state.remoteIsAuthoritative,
                    isComplete: state.completeFingerprints.contains(fingerprint),
                    isBackedUp: state.backedUpFingerprints.contains(fingerprint)
                )
            }
            return result
        }
    }

    func browserLocalProjectionInput() -> BrowserLocalProjectionInput {
        let currentKey = profileKey()
        let revision = coordinator.currentSnapshotRevision()
        return lock.withLock {
            return BrowserLocalProjectionInput(
                seed: state.localIsBuilt ? state.homeLocalSeed : nil,
                backedUpFingerprints: isRemoteStateCurrentLocked(
                    profileKey: currentKey,
                    revision: revision
                ) && state.remoteIsAuthoritative
                    ? state.backedUpFingerprints
                    : []
            )
        }
    }

    static func classifyBackupPresence(
        isCurrent: Bool,
        isAuthoritative: Bool,
        isComplete: Bool,
        isBackedUp: Bool
    ) -> BackupPresenceVerdict {
        guard isCurrent, isAuthoritative else { return .unknown }
        if isComplete && isBackedUp { return .complete }
        if isBackedUp { return .incomplete }
        return .absent
    }

    // Whether the remote/backed-up sets authoritatively reflect the active profile. False during a profile
    // switch's reload window: the committed build is still the previous profile's while `profileKey()` already
    // answers for the new one, so an item's `.localOnly` projection is UNKNOWN rather than confirmed. Remote-
    // write action readiness (Upload) must not treat that as "safe to upload" — re-checking the built profile
    // against the live one catches the window the stored flag alone would miss.
    var isRemotePresenceAuthoritative: Bool {
        let currentKey = profileKey()
        let revision = coordinator.currentSnapshotRevision()
        return lock.withLock {
            isRemoteStateCurrentLocked(profileKey: currentKey, revision: revision) &&
                state.remoteIsAuthoritative
        }
    }
}

import Foundation
import os.log

private let syncLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

final class RemoteIndexSyncService: @unchecked Sendable {
    private static let overlayProbeMaxVerifiedFilesPerMonth = 64
    private static let overlayProbeMaxVerifiedBytesPerMonth: Int64 = 32 * 1024 * 1024

    private actor SyncGate {
        private var isLocked = false
        private var waiters: [(UUID, CheckedContinuation<Void, Error>)] = []
        private var preCancelledIDs: Set<UUID> = []
        private var preCancelledOrder: [UUID] = []
        private var registeredIDs: Set<UUID> = []
        private var retiredIDs: Set<UUID> = []
        private var retiredOrder: [UUID] = []
        private static let preCancelledIDsCap = 4096
        private static let retiredIDsCap = 4096

        private func registerWaiter(id: UUID, continuation: CheckedContinuation<Void, Error>) {
            if consumePreCancelled(id) {
                recordRetired(id)
                continuation.resume(throwing: CancellationError())
                return
            }
            registeredIDs.insert(id)
            if !isLocked {
                isLocked = true
                continuation.resume(returning: ())
                return
            }
            waiters.append((id, continuation))
        }

        private func cancelWaiter(id: UUID) {
            if let idx = waiters.firstIndex(where: { $0.0 == id }) {
                let (_, cont) = waiters.remove(at: idx)
                registeredIDs.remove(id)
                recordRetired(id)
                cont.resume(throwing: CancellationError())
            } else if retiredIDs.contains(id) {
                return
            } else if !registeredIDs.contains(id) {
                recordPreCancelled(id)
            }
        }

        private func recordRetired(_ id: UUID) {
            if retiredIDs.contains(id) { return }
            if retiredIDs.count >= Self.retiredIDsCap,
               let oldest = retiredOrder.first {
                retiredOrder.removeFirst()
                retiredIDs.remove(oldest)
            }
            retiredIDs.insert(id)
            retiredOrder.append(id)
        }

        private func recordPreCancelled(_ id: UUID) {
            if preCancelledIDs.contains(id) { return }
            if preCancelledIDs.count >= Self.preCancelledIDsCap,
               let oldest = preCancelledOrder.first {
                preCancelledOrder.removeFirst()
                preCancelledIDs.remove(oldest)
            }
            preCancelledIDs.insert(id)
            preCancelledOrder.append(id)
        }

        private func consumePreCancelled(_ id: UUID) -> Bool {
            guard preCancelledIDs.remove(id) != nil else { return false }
            if let index = preCancelledOrder.firstIndex(of: id) {
                preCancelledOrder.remove(at: index)
            }
            return true
        }

        private func acquire(id: UUID) async throws {
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                    registerWaiter(id: id, continuation: cont)
                }
            } onCancel: {
                Task(priority: Task.currentPriority) { await self.cancelWaiter(id: id) }
            }
        }

        private func release(id: UUID) {
            registeredIDs.remove(id)
            recordRetired(id)
            while !waiters.isEmpty {
                let next = waiters.removeFirst()
                if consumePreCancelled(next.0) {
                    registeredIDs.remove(next.0)
                    recordRetired(next.0)
                    next.1.resume(throwing: CancellationError())
                    continue
                }
                next.1.resume(returning: ())
                return
            }
            isLocked = false
        }

        func withLock<T>(_ operation: () async throws -> T) async throws -> T {
            let id = UUID()
            try await acquire(id: id)
            defer { release(id: id) }
            try Task.checkCancellation()
            return try await operation()
        }
    }

    private actor MutableState {
        private var activeRemoteProfileKey: String?
        private var remoteManifestDigests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        private var lastInspectedFormatIsV2: Bool?
        private var materializedRepoID: String?

        func ensureRemoteContext(profileKey: String) -> Bool {
            guard activeRemoteProfileKey != profileKey else { return false }
            activeRemoteProfileKey = profileKey
            remoteManifestDigests.removeAll()
            lastInspectedFormatIsV2 = nil
            materializedRepoID = nil
            return true
        }

        func currentRemoteManifestDigests() -> [LibraryMonthKey: RemoteMonthManifestDigest] {
            remoteManifestDigests
        }

        func updateRemoteManifestDigests(_ digests: [LibraryMonthKey: RemoteMonthManifestDigest]) {
            remoteManifestDigests = digests
        }

        func setIsV2Repo(_ value: Bool) {
            lastInspectedFormatIsV2 = value
        }

        func isV2Repo() -> Bool? {
            lastInspectedFormatIsV2
        }

        func setMaterializedRepoID(_ id: String?) {
            materializedRepoID = id
        }

        func getMaterializedRepoID() -> String? {
            materializedRepoID
        }

        func reset() {
            activeRemoteProfileKey = nil
            remoteManifestDigests.removeAll()
            lastInspectedFormatIsV2 = nil
            materializedRepoID = nil
        }
    }

    private let committedView: RepoCommittedView
    private let optimisticMutationLock = NSLock()
    private let syncGate = SyncGate()
    private let state = MutableState()

    init(
        snapshotCache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache()
    ) {
        self.committedView = RepoCommittedView(cache: snapshotCache)
    }

    init(
        committedView: RepoCommittedView
    ) {
        self.committedView = committedView
    }

    /// `preInspection` lets the caller supply a known-current observation of the remote format
    /// (e.g. published by `BackupV2RuntimeServices.postOpenSyncInspection`). Must reflect the
    /// **current post-mutation** remote state — not a stale snapshot from before any open-side
    /// bootstrap / migration / cleanup writes. Callers that performed an open which mutated
    /// format-marker state MUST pass `nil` here so this method re-inspects after those writes.
    func syncIndex(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil,
        preMaterialized: RepoMaterializer.MaterializeOutput? = nil,
        preInspection: RemoteFormatInspection? = nil,
        expectV2: Bool = false,
        localRepoID: String? = nil
    ) async throws -> RemoteIndexSyncDigest {
        try await syncGate.withLock {
            try await syncIndexUnlocked(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                preMaterialized: preMaterialized,
                preInspection: preInspection,
                expectV2: expectV2,
                localRepoID: localRepoID
            )
        }
    }

    func syncOverlayAndCaptureHandle(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: RemotePresenceSnapshot? = nil,
        concurrencyCap: Int = 4
    ) async throws -> RemoteViewHandle {
        let captured = try await syncGate.withLock { [self] in
            optimisticMutationLock.withLock {
                let current = committedView.currentSnapshotWithRevision()
                return (
                    revision: current.revision,
                    snapshot: current.snapshot,
                    fallback: fallback ?? current.snapshot.presence
                )
            }
        }
        let probe = try await RemoteIndexPhysicalPresenceOverlayProbe().probe(
            snapshot: captured.snapshot,
            client: client,
            basePath: basePath,
            fallback: captured.fallback,
            budget: RemoteIndexOverlayProbeBudget(
                maxVerifiedFilesPerMonth: Self.overlayProbeMaxVerifiedFilesPerMonth,
                maxVerifiedBytesPerMonth: Self.overlayProbeMaxVerifiedBytesPerMonth
            ),
            staleFallbackPolicy: .failClosedWhenMissingFallback,
            concurrencyCap: concurrencyCap
        )
        return try await syncGate.withLock { [self] in
            // Apply + snapshot must share one lock acquisition so a worker can't bump
            // the revision and add fingerprints between the two — the handle would
            // otherwise claim `.fresh` for assets the probe never covered.
            let result: (handle: RemoteViewHandle, staleRevision: Bool) = optimisticMutationLock.withLock {
                let revisionUnchanged: Bool
                if committedView.currentRevision() != captured.revision {
                    committedView.clearPresenceFreshness()
                    revisionUnchanged = false
                } else {
                    committedView.applyPresenceSnapshot(probe.presence)
                    revisionUnchanged = true
                }
                let overlayFresh = revisionUnchanged && probe.allMonthsFresh
                let (revision, snapshot) = committedView.currentSnapshotWithRevision()
                let coverage = Self.resumeCoverage(from: snapshot)
                let nonCleanMonths = committedView.monthsWithNonCleanOutcome()
                let handle = RemoteViewHandle(
                    revision: revision,
                    resumeCoverage: coverage,
                    overlayFreshness: overlayFresh ? .fresh : .stale,
                    producedAt: Date(),
                    nonCleanMonths: nonCleanMonths
                )
                return (handle, !revisionUnchanged)
            }
            if result.staleRevision {
                syncLog.info("[SyncTiming] overlay handle captured stale revision; preserving previous overlay")
            }
            return result.handle
        }
    }

    private func handleFromCurrentSnapshot(overlayFresh: Bool) -> RemoteViewHandle {
        let captured = optimisticMutationLock.withLock {
            let (revision, snapshot) = committedView.currentSnapshotWithRevision()
            let coverage = Self.resumeCoverage(from: snapshot)
            let nonCleanMonths = committedView.monthsWithNonCleanOutcome()
            return (revision: revision, coverage: coverage, nonCleanMonths: nonCleanMonths)
        }
        return RemoteViewHandle(
            revision: captured.revision,
            resumeCoverage: captured.coverage,
            overlayFreshness: overlayFresh ? .fresh : .stale,
            producedAt: Date(),
            nonCleanMonths: captured.nonCleanMonths
        )
    }

    func fullSnapshot() -> RemoteLibrarySnapshot {
        optimisticMutationLock.withLock {
            committedView.current()
        }
    }

    private func syncIndexUnlocked(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        preMaterialized: RepoMaterializer.MaterializeOutput? = nil,
        preInspection: RemoteFormatInspection? = nil,
        expectV2: Bool = false,
        localRepoID: String? = nil
    ) async throws -> RemoteIndexSyncDigest {
        let syncStart = CFAbsoluteTimeGetCurrent()

        let shouldResetSnapshot = await state.ensureRemoteContext(profileKey: Self.remoteProfileKey(profile))
        if shouldResetSnapshot {
            resetCommittedViewAndOverlayFreshness()
        }

        let alreadyV2 = await state.isV2Repo() == true
        let inspection: RemoteFormatInspection
        if let preInspection {
            inspection = preInspection
        } else {
            do {
                inspection = try await RemoteFormatCompatibilityService()
                    .inspectRemoteFormat(client: client, profile: profile)
            } catch let compat as BackupCompatibilityError {
                // Deterministic format-damage (.damagedV2Repo) refuses the endpoint before a
                // route exists, so it bypasses the route-decision reset below. Drop the stale
                // committed view; transient list/transport failures throw raw errors (not
                // BackupCompatibilityError) and are left untouched.
                resetCommittedViewAndOverlayFreshness()
                throw compat
            }
        }
        let route: RemoteIndexSyncRoute
        do {
            route = try RemoteIndexFormatRouteDecision.decide(
                inspection: inspection,
                alreadyV2: alreadyV2,
                expectV2: expectV2 || (localRepoID != nil)
            )
        } catch {
            switch inspection {
            case .v2WithV1Manifests, .v1, .fresh, .unsupported:
                // Route refused: endpoint is no longer a readable cached V2 repo (wrong or
                // future format). Drop the stale committed view so Home can't keep serving
                // the old V2 rows; unlike .v1/.fresh, an unsupported endpoint never self-heals.
                resetCommittedViewAndOverlayFreshness()
            case .v2, .v2WithPendingMigrationCleanup:
                break
            }
            throw error
        }
        switch inspection {
        case .v2, .v2WithPendingMigrationCleanup:
            await state.setIsV2Repo(true)
        case .v1:
            await state.setIsV2Repo(false)
            clearPhysicalPresenceOverlayFreshness()
        case .fresh:
            clearPhysicalPresenceOverlayFreshness()
        case .unsupported, .v2WithV1Manifests:
            break
        }
        if case .v2(let allowPreMaterialized) = route {
            return try await syncIndexV2(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                syncStart: syncStart,
                preMaterialized: allowPreMaterialized ? preMaterialized : nil,
                localRepoID: localRepoID
            )
        }

        let previousDigests = await state.currentRemoteManifestDigests()
        let v1Result = try await RemoteIndexV1SyncEngine().sync(
            client: client,
            basePath: profile.basePath,
            previousDigests: previousDigests,
            onSyncProgress: onSyncProgress
        )

        if v1Result.changedMonths.isEmpty, v1Result.missingMonths.isEmpty, v1Result.removedMonths.isEmpty {
            committedView.markSynced(Date())
            let digest = committedView.counts()
            let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
            syncLog.info("[SyncTiming] No changes. Total: \(Self.ms(totalElapsed))s")
            eventStream?.emitLog(
                String.localizedStringWithFormat(String(localized: "backup.remoteIndex.unchanged"), v1Result.remoteMonthCount),
                level: .debug
            )
            return digest
        }

        try Task.checkCancellation()
        let appliedCounts = optimisticMutationLock.withLock { () -> (changed: Int, removed: Int) in
            var changed = 0
            var removed = 0
            for month in v1Result.missingMonths.sorted() {
                if committedView.removeMonth(month) {
                    changed += 1
                }
            }
            for month in v1Result.changedMonths.keys.sorted() {
                guard let snapshot = v1Result.changedMonths[month] else { continue }
                if committedView.replaceMonth(
                    month,
                    resources: snapshot.resources,
                    assets: snapshot.assets,
                    assetResourceLinks: snapshot.links
                ) {
                    changed += 1
                }
            }

            for month in v1Result.removedMonths.sorted() {
                if committedView.removeMonth(month) {
                    removed += 1
                }
            }
            return (changed, removed)
        }

        var processedMonthCount = v1Result.changedMonths.count + v1Result.missingMonths.count
        for _ in v1Result.removedMonths.sorted() {
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: v1Result.totalMonthsToProcess))
        }

        await state.updateRemoteManifestDigests(v1Result.effectiveRemoteDigests)

        committedView.markSynced(Date())
        let digest = committedView.counts()
        let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
        syncLog.info("[SyncTiming] Sync complete. Total: \(Self.ms(totalElapsed))s, changed: \(appliedCounts.changed), removed: \(appliedCounts.removed)")

        return digest
    }


    private func syncIndexV2(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        syncStart: CFAbsoluteTime,
        preMaterialized: RepoMaterializer.MaterializeOutput? = nil,
        localRepoID: String? = nil
    ) async throws -> RemoteIndexSyncDigest {
        // No persisted binding: fall back to the in-process materialized repo ID so an
        // externally-swapped V2 repo can't be silently adopted — mirrors verifyMonthV2's
        // nil-binding guard. First sync (no cached ID) still materializes freely.
        let expectedRepoID: String?
        if let localRepoID {
            expectedRepoID = localRepoID
        } else {
            expectedRepoID = await state.getMaterializedRepoID()
        }
        let output: RepoMaterializer.MaterializeOutput
        do {
            output = try await RemoteIndexV2SyncEngine().materialize(
                client: client,
                basePath: profile.basePath,
                preMaterialized: preMaterialized,
                localRepoID: expectedRepoID
            )
        } catch let compat as BackupCompatibilityError {
            // Route accepted .v2, but materialize proved the live repo isn't the cached one
            // (repoIdentityMismatch / mapped damaged-identity). Drop the stale committed view
            // before rethrowing; cancellation/transport errors propagate without clearing.
            resetCommittedViewAndOverlayFreshness()
            throw compat
        } catch let nsError as NSError where Self.isMissingCanonicalIdentityRefusal(nsError) {
            // Accepted .v2 but the live repo lacks canonical identity; sync can't trust
            // materialization. Same fail-closed class — drop the stale view. This deterministic
            // code is never a transport/cancellation error, which propagate without clearing.
            resetCommittedViewAndOverlayFreshness()
            throw nsError
        }
        await state.setMaterializedRepoID(output.repoID)
        let priorOverlay = loadMaterializedCommittedView(output)
        do {
            _ = try await refreshPhysicalPresenceOverlay(client: client, basePath: profile.basePath, fallback: priorOverlay)
        } catch is CancellationError {
            applyPhysicalPresenceOverlay(priorOverlay)
            throw CancellationError()
        } catch {
            applyPhysicalPresenceOverlay(priorOverlay)
            syncLog.info("[SyncTiming] overlay refresh skipped: \(error.localizedDescription)")
        }
        committedView.markSynced(Date())
        onSyncProgress?(RemoteSyncProgress(current: output.state.months.count, total: output.state.months.count))
        let digest = committedView.counts()
        let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
        syncLog.info("[SyncTiming] V2 materialize: \(Self.ms(totalElapsed))s (\(output.state.months.count) months)")
        eventStream?.emitLog(
            String.localizedStringWithFormat(String(localized: "backup.remoteIndex.unchanged"), output.state.months.count),
            level: .debug
        )
        return digest
    }

    /// V1-only verify; the version.json probe must fail-closed so a V2 repo can't be misclassified and corrupted.
    func verifyMonth(
        client: RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey
    ) async throws {
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        let versionMeta: RemoteStorageEntry?
        do {
            versionMeta = try await client.metadata(path: versionPath)
        } catch {
            if Self.isNotFoundError(error) {
                versionMeta = nil
            } else {
                throw error
            }
        }
        if let meta = versionMeta {
            guard !meta.isDirectory else {
                throw NSError(domain: "RemoteIndexSyncService", code: -62, userInfo: [
                    NSLocalizedDescriptionKey:
                        "verifyMonth(V1) refused: \(versionPath) is a directory"
                ])
            }
            throw NSError(domain: "RemoteIndexSyncService", code: -60, userInfo: [
                NSLocalizedDescriptionKey:
                    "verifyMonth(V1) refused: \(versionPath) exists — repo is V2; use RepoVerifyMonthService"
            ])
        }
        if try await RepoBootstrapInspectionFSM.hasAnyV2CommitOrSnapshotData(client: client, basePath: basePath) {
            throw NSError(domain: "RemoteIndexSyncService", code: -61, userInfo: [
                NSLocalizedDescriptionKey:
                    "verifyMonth(V1) refused: V2 commit/snapshot data exists without version.json"
            ])
        }
        let monthRelativePath = String(format: "%04d/%02d", month.year, month.month)
        let manifestPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRelativePath + "/" + MonthManifestStore.manifestFileName
        )
        // Pre-check distinguishes "manifest gone" (drop stale cache entry) from "download failed" (error); `loadManifestDirect` collapses both into nil.
        guard let metadata = try await client.metadata(path: manifestPath),
              !metadata.isDirectory else {
            optimisticMutationLock.withLock {
                _ = committedView.removeMonth(month)
            }
            return
        }

        guard let store = try await MonthManifestStore.loadManifestDirect(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            manifestAbsolutePath: manifestPath
        ) else {
            throw NSError(
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

        let internalResult = try store.reconcileMonth()

        let monthAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRelativePath
        )
        let entries = try await client.list(path: monthAbsolutePath)
        let remoteFileNames = entries
            .filter { !$0.isDirectory && $0.name != MonthManifestStore.manifestFileName }
            .map(\.name)
        let nameCase = client.backendNameCaseSensitivity
        let remotePresenceKeys = Set(remoteFileNames.map { nameCase.presenceKey(for: $0) })
        let listingMissing = store.existingFileNames().filter { name in
            !remotePresenceKeys.contains(nameCase.presenceKey(for: name))
        }
        let listingResult = try store.reconcileMonth(missingFileNames: Set(listingMissing))

        let touched = internalResult.removedResourceCount + internalResult.removedAssetCount
            + internalResult.removedOrphanLinkCount
            + listingResult.removedResourceCount + listingResult.removedAssetCount
            + listingResult.removedOrphanLinkCount
        guard touched > 0 else { return }

        if store.dirty {
            try await store.flushToRemote()
        }
        let snapshot = store.unsortedSnapshot()
        optimisticMutationLock.withLock {
            _ = committedView.replaceMonth(
                month,
                resources: snapshot.resources,
                assets: snapshot.assets,
                assetResourceLinks: snapshot.links
            )
        }
        syncLog.info("[verify] \(month.text): internal=\(internalResult.removedAssetCount)+\(internalResult.removedOrphanLinkCount)L, listing=\(listingResult.removedAssetCount)+\(listingResult.removedOrphanLinkCount)L")
    }

    func remoteMonthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, totalSizeBytes: Int64)] {
        committedView.monthSummaries()
    }

    func healthDigest() -> RemoteHealthDigest {
        committedView.healthDigest()
    }

    func allKnownMonths() -> Set<LibraryMonthKey> {
        committedView.allKnownMonths()
    }

    func nonCleanOutcomeMonths() -> Set<LibraryMonthKey> {
        committedView.monthsWithNonCleanOutcome()
    }

    func remoteMonthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        optimisticMutationLock.withLock {
            guard let delta = committedView.monthRawData(for: month) else { return nil }
            return delta
        }
    }

    func currentState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        optimisticMutationLock.withLock {
            committedView.state(since: revision)
        }
    }

    func makeOptimisticAssetWriter() -> OptimisticAssetWriter {
        OptimisticAssetWriter(service: self)
    }

    fileprivate func _optimisticUpsertResource(_ item: RemoteManifestResource) {
        optimisticMutationLock.withLock {
            committedView.appendOverlayResource(item)
        }
    }

    fileprivate func _appendOptimisticAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink]?
    ) {
        optimisticMutationLock.withLock {
            committedView.appendOverlayAsset(asset, links: links)
        }
    }

    /// U01 R05: drop the session-overlay rows of a month whose V2 batch hard-aborted, so the
    /// non-durable per-asset `appendAsset` rows stop surfacing through `remoteMonthRawData(for:)`
    /// / `resumeSafeToSkipAssetFingerprintsByMonth()` / `currentState(since:)`. Only the overlay
    /// is dropped: a durable baseline materialized or published before the aborted optimistic
    /// writes stays visible.
    func dropOptimisticMonthIfStale(month: LibraryMonthKey) {
        optimisticMutationLock.withLock {
            committedView.dropSessionOverlayMonth(month)
        }
    }

    func resetForProfileSwitch() async {
        resetCommittedViewAndOverlayFreshness()
        await state.reset()
    }

    /// Drop the committed view after a deterministic identity/format refusal proved the current
    /// endpoint is not the cached repo, so Home can't keep republishing the old repo's rows.
    /// Keeps the cached repo ID so later guards still reject the swapped endpoint fail-closed.
    func invalidateCommittedViewForCompatibilityFailure() {
        resetCommittedViewAndOverlayFreshness()
    }

    /// Structural form of the compatibility-failure invalidation: invalidates the committed view iff
    /// `body` throws a `BackupCompatibilityError`, then rethrows. Non-compatibility errors pass through
    /// untouched. Only wrap regions where every reachable compatibility throw already invalidates today.
    func withCommittedViewInvalidationOnCompatibilityFailure<T>(_ body: () async throws -> T) async rethrows -> T {
        do {
            return try await body()
        } catch let error as BackupCompatibilityError {
            invalidateCommittedViewForCompatibilityFailure()
            throw error
        }
    }

    private func loadMaterializedCommittedView(_ output: RepoMaterializer.MaterializeOutput) -> RemotePresenceSnapshot {
        optimisticMutationLock.withLock {
            committedView.loadFromMaterialize(output)
        }
    }

    private func resetCommittedViewAndOverlayFreshness() {
        optimisticMutationLock.withLock {
            committedView.reset()
        }
    }

    private func clearPhysicalPresenceOverlayFreshness() {
        optimisticMutationLock.withLock {
            committedView.clearPresenceFreshness()
        }
    }

    func currentRepoIsV2() async -> Bool? {
        await state.isV2Repo()
    }

    func materializedRepoID() async -> String? {
        await state.getMaterializedRepoID()
    }

    func markIsV2() async {
        await state.setIsV2Repo(true)
    }

    func resumeCoverageForCurrentView() -> RemoteResumeCoverage {
        optimisticMutationLock.withLock {
            Self.resumeCoverage(from: committedView.current())
        }
    }

    func resumeSafeToSkipAssetFingerprintsByMonth() -> PerMonth<Set<AssetFingerprint>> {
        resumeCoverageForCurrentView().safeToSkipAssetFingerprintsByMonth
    }

    private static func resumeCoverage(from snapshot: RemoteLibrarySnapshot) -> RemoteResumeCoverage {
        var linksByMonthFP: [LibraryMonthKey: [AssetFingerprint: [RemoteAssetResourceLink]]] = [:]
        for link in snapshot.assetResourceLinks {
            let month = LibraryMonthKey(year: link.year, month: link.month)
            linksByMonthFP[month, default: [:]][link.assetFingerprint, default: []].append(link)
        }
        var resourceHashesByMonth: [LibraryMonthKey: Set<Data>] = [:]
        for resource in snapshot.resources {
            let month = LibraryMonthKey(year: resource.year, month: resource.month)
            resourceHashesByMonth[month, default: []].insert(resource.contentHash)
        }
        var availableHashesByMonth = resourceHashesByMonth
        // Physical-missing overlays keep commit rows but make dependent assets repair work.
        for entry in snapshot.presence.entries where !entry.value.missingHashes.isEmpty {
            availableHashesByMonth[entry.month, default: []].subtract(entry.value.missingHashes)
        }
        var survivorKeySetsByMonthFP: [LibraryMonthKey: [AssetFingerprint: Set<AssetResourceLinkKey>]] = [:]
        for (month, linksByFingerprint) in linksByMonthFP {
            for (fingerprint, links) in linksByFingerprint {
                let keySet = AssetResourceLinkSetPredicate.keys(fromLinks: links)
                guard !keySet.isEmpty else { continue }
                survivorKeySetsByMonthFP[month, default: [:]][fingerprint] = keySet
            }
        }

        var healthyKeySetsByMonthFP: [LibraryMonthKey: [AssetFingerprint: Set<AssetResourceLinkKey>]] = [:]
        for asset in snapshot.assets {
            let month = LibraryMonthKey(year: asset.year, month: asset.month)
            let links = linksByMonthFP[month]?[asset.assetFingerprint] ?? []
            let availableHashes = availableHashesByMonth[month] ?? []
            let state = RemoteAssetIntegrityClassifier.classify(
                assetFingerprint: asset.assetFingerprint,
                links: links,
                isResourceAvailable: { availableHashes.contains($0) }
            )
            if state.isHealthy {
                healthyKeySetsByMonthFP[month, default: [:]][asset.assetFingerprint] =
                    survivorKeySetsByMonthFP[month]?[asset.assetFingerprint]
                    ?? AssetResourceLinkSetPredicate.keys(fromLinks: links)
            }
        }

        var safeToSkipByMonth = PerMonth<Set<AssetFingerprint>>()
        var healingRequiredByMonth = PerMonth<Set<AssetFingerprint>>()
        for (month, keySetsByFingerprint) in healthyKeySetsByMonthFP {
            let survivorKeySetsByFingerprint = survivorKeySetsByMonthFP[month] ?? [:]
            var fingerprintsByResourceKey: [AssetResourceLinkKey: Set<AssetFingerprint>] = [:]
            fingerprintsByResourceKey.reserveCapacity(
                survivorKeySetsByFingerprint.values.reduce(0) { $0 + $1.count }
            )
            for (fingerprint, keySet) in survivorKeySetsByFingerprint {
                for key in keySet {
                    fingerprintsByResourceKey[key, default: []].insert(fingerprint)
                }
            }

            var safeToSkip = Set(keySetsByFingerprint.keys)
            var healingRequired: Set<AssetFingerprint> = []
            healingRequired.reserveCapacity(keySetsByFingerprint.count)
            for (fingerprint, incomingKeys) in keySetsByFingerprint {
                var possibleSurvivors: Set<AssetFingerprint> = []
                for key in incomingKeys {
                    if let bucket = fingerprintsByResourceKey[key] {
                        possibleSurvivors.formUnion(bucket)
                    }
                }
                possibleSurvivors.remove(fingerprint)
                for survivor in possibleSurvivors {
                    guard let survivorKeys = survivorKeySetsByFingerprint[survivor] else { continue }
                    if AssetResourceLinkSetPredicate.isStrictSubset(survivorKeys, of: incomingKeys) {
                        healingRequired.insert(fingerprint)
                        safeToSkip.remove(fingerprint)
                        break
                    }
                }
            }
            if !safeToSkip.isEmpty {
                safeToSkipByMonth.set(safeToSkip, for: month)
            }
            if !healingRequired.isEmpty {
                healingRequiredByMonth.set(healingRequired, for: month)
            }
        }
        return RemoteResumeCoverage(
            safeToSkipAssetFingerprintsByMonth: safeToSkipByMonth,
            healingRequiredAssetFingerprintsByMonth: healingRequiredByMonth
        )
    }

    func replaceCachedMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        links: [RemoteAssetResourceLink],
        physicallyMissingHashes: Set<Data>? = nil
    ) {
        optimisticMutationLock.withLock {
            _ = committedView.replaceMonth(
                month,
                resources: resources,
                assets: assets,
                assetResourceLinks: links,
                physicallyMissingHashes: physicallyMissingHashes,
                freshness: physicallyMissingHashes == nil ? .markStale : .markFresh
            )
        }
    }

    /// Durable verify-prune entry point: evict the tombstoned (durable) fingerprints from the
    /// durable view of `month` without consuming the composed effective view or touching the
    /// session overlay. Keeps non-durable optimistic rows session-only so a later hard abort /
    /// cancelled sync can still drop them rather than having them promoted into the durable cache.
    func pruneDurableMonth(_ month: LibraryMonthKey, removingAssetFingerprints fingerprints: Set<AssetFingerprint>) {
        optimisticMutationLock.withLock {
            committedView.pruneDurableMonth(month, removingAssetFingerprints: fingerprints)
        }
    }

    func publishMonthSnapshot(of monthStore: any BackupMonthStore, for month: LibraryMonthKey) {
        let snapshot = monthStore.unsortedSnapshot()
        let presence = monthStore.presence
        replaceCachedMonth(
            month,
            resources: snapshot.resources,
            assets: snapshot.assets,
            links: snapshot.links,
            physicallyMissingHashes: presence.isAuthoritative ? presence.missingHashes : nil
        )
    }

    func markPhysicallyMissingV2(month: LibraryMonthKey, hashes: Set<Data>) {
        optimisticMutationLock.withLock {
            committedView.markPhysicallyMissing(month: month, hashes: hashes)
        }
    }

    func physicallyMissingHashes(for month: LibraryMonthKey) -> Set<Data> {
        optimisticMutationLock.withLock {
            committedView.physicallyMissingHashes(for: month)
        }
    }

    func verifiedPhysicallyMissingHashes(for month: LibraryMonthKey) -> Set<Data>? {
        optimisticMutationLock.withLock {
            committedView.verifiedPhysicallyMissingHashes(for: month)
        }
    }

    func presenceSnapshot(for month: LibraryMonthKey) -> RemotePresenceSnapshot.Month {
        optimisticMutationLock.withLock {
            committedView.presenceSnapshot(for: month)
        }
    }

    func fullPresenceSnapshot() -> RemotePresenceSnapshot {
        optimisticMutationLock.withLock {
            committedView.fullPresenceSnapshot()
        }
    }

    func physicallyMissingHashesForTest(month: LibraryMonthKey) -> Set<Data> {
        physicallyMissingHashes(for: month)
    }

    @discardableResult
    func loadMaterializedForTest(_ output: RepoMaterializer.MaterializeOutput) -> RemotePresenceSnapshot {
        loadMaterializedCommittedView(output)
    }

    @discardableResult
    func applyPresenceSnapshotForTest(_ snapshot: RemotePresenceSnapshot) -> Bool {
        applyPhysicalPresenceOverlay(snapshot)
    }

    @discardableResult
    func refreshPhysicalPresenceOverlay(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: RemotePresenceSnapshot = RemotePresenceSnapshot(),
        concurrencyCap: Int = 4
    ) async throws -> Bool {
        try Task.checkCancellation()
        let captured = optimisticMutationLock.withLock {
            committedView.currentSnapshotWithRevision()
        }
        let probe = try await RemoteIndexPhysicalPresenceOverlayProbe().probe(
            snapshot: captured.snapshot,
            client: client,
            basePath: basePath,
            fallback: fallback,
            budget: RemoteIndexOverlayProbeBudget(
                maxVerifiedFilesPerMonth: Self.overlayProbeMaxVerifiedFilesPerMonth,
                maxVerifiedBytesPerMonth: Self.overlayProbeMaxVerifiedBytesPerMonth
            ),
            staleFallbackPolicy: .preserveFallback,
            concurrencyCap: concurrencyCap
        )
        let applied = applyPhysicalPresenceOverlay(
            probe.presence,
            expectedRevision: captured.revision
        )
        if !applied {
            syncLog.info("[SyncTiming] overlay refresh captured stale revision; preserving previous overlay")
            return false
        }
        return probe.allMonthsFresh
    }

    @discardableResult
    private func applyPhysicalPresenceOverlay(
        _ snapshot: RemotePresenceSnapshot,
        expectedRevision: UInt64? = nil
    ) -> Bool {
        optimisticMutationLock.withLock {
            committedView.applyPresenceSnapshot(snapshot, expectedRevision: expectedRevision)
        }
    }

    private static func isNotFoundError(_ error: Error) -> Bool {
        isStorageNotFoundError(error)
    }

    private static func isMissingCanonicalIdentityRefusal(_ error: NSError) -> Bool {
        error.domain == RemoteIndexV2SyncEngine.missingCanonicalIdentityErrorDomain
            && error.code == RemoteIndexV2SyncEngine.missingCanonicalIdentityErrorCode
    }

    private static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        // External-volume endpoints live entirely in the bookmark; host/port/share/basePath are
        // fixed sentinels, so a repoint to a different directory must enter the key here or the
        // cached V2/V1 inspection + committed view carries over to the new endpoint.
        let externalEndpoint = profile.resolvedStorageType == .externalVolume
            ? (profile.externalVolumeParams?.displayPath ?? "")
            : ""
        return [
            String(profile.id ?? 0),
            profile.storageType,
            profile.host,
            String(profile.port),
            profile.shareName,
            profile.basePath,
            profile.username,
            profile.domain ?? "",
            externalEndpoint
        ].joined(separator: "|")
    }

    private static func ms(_ seconds: CFAbsoluteTime) -> String {
        String(format: "%.3f", seconds)
    }
}

struct OptimisticAssetWriter: Sendable {
    fileprivate let service: RemoteIndexSyncService

    func appendAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink]?
    ) {
        service._appendOptimisticAsset(asset, links: links)
    }

    func appendResource(_ resource: RemoteManifestResource) {
        service._optimisticUpsertResource(resource)
    }
}

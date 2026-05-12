import Foundation
import os.log

private let syncLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

// `uncommittedV2FingerprintsByMonth` is mutable but only mutated under `uncommittedLock`;
// other state is either `let` or wrapped in actors. Hence `@unchecked Sendable`.
final class RemoteIndexSyncService: @unchecked Sendable {
    private actor SyncGate {
        private var isLocked = false
        private var waiters: [(UUID, CheckedContinuation<Void, Error>)] = []
        private var preCancelledIDs: Set<UUID> = []

        private func registerWaiter(id: UUID, continuation: CheckedContinuation<Void, Error>) {
            if preCancelledIDs.remove(id) != nil {
                continuation.resume(throwing: CancellationError())
                return
            }
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
                cont.resume(throwing: CancellationError())
            } else {
                preCancelledIDs.insert(id)
            }
        }

        private func acquire() async throws {
            let id = UUID()
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                    Task { await self.registerWaiter(id: id, continuation: cont) }
                }
            } onCancel: {
                Task { await self.cancelWaiter(id: id) }
            }
        }

        private func release() {
            if !waiters.isEmpty {
                let next = waiters.removeFirst()
                next.1.resume(returning: ())
            } else {
                isLocked = false
            }
        }

        func withLock<T>(_ operation: () async throws -> T) async throws -> T {
            try await acquire()
            defer { release() }
            try Task.checkCancellation()
            return try await operation()
        }
    }

    private actor MutableState {
        private var activeRemoteProfileKey: String?
        private var remoteManifestDigests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        private var lastInspectedFormatIsV2: Bool?
        private var lastOverlayFresh: Bool = false

        func ensureRemoteContext(profileKey: String) -> Bool {
            guard activeRemoteProfileKey != profileKey else { return false }
            activeRemoteProfileKey = profileKey
            remoteManifestDigests.removeAll()
            lastInspectedFormatIsV2 = nil
            lastOverlayFresh = false
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

        func setOverlayFresh(_ value: Bool) {
            lastOverlayFresh = value
        }

        func overlayFresh() -> Bool {
            lastOverlayFresh
        }

        func reset() {
            activeRemoteProfileKey = nil
            remoteManifestDigests.removeAll()
            lastInspectedFormatIsV2 = nil
            lastOverlayFresh = false
        }
    }

    private let committedView: RepoCommittedView
    private let inflightTracker: OptimisticInflightTracker
    private let syncGate = SyncGate()
    private let state = MutableState()

    init(
        snapshotCache: RemoteLibrarySnapshotCache = RemoteLibrarySnapshotCache()
    ) {
        self.committedView = RepoCommittedView(cache: snapshotCache)
        self.inflightTracker = OptimisticInflightTracker()
    }

    init(
        committedView: RepoCommittedView,
        inflightTracker: OptimisticInflightTracker
    ) {
        self.committedView = committedView
        self.inflightTracker = inflightTracker
    }

    func syncIndex(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream? = nil,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)? = nil,
        preMaterialized: RepoMaterializer.MaterializeOutput? = nil,
        expectV2: Bool = false
    ) async throws -> RemoteIndexSyncDigest {
        try await syncGate.withLock {
            try await syncIndexUnlocked(
                client: client,
                profile: profile,
                eventStream: eventStream,
                onSyncProgress: onSyncProgress,
                preMaterialized: preMaterialized,
                expectV2: expectV2
            )
        }
    }

    /// O(N) flat-array projection — call only when callers actually need the flat arrays.
    func fullSnapshot() -> RemoteLibrarySnapshot {
        committedView.current()
    }

    private func syncIndexUnlocked(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        preMaterialized: RepoMaterializer.MaterializeOutput? = nil,
        expectV2: Bool = false
    ) async throws -> RemoteIndexSyncDigest {
        let syncStart = CFAbsoluteTimeGetCurrent()

        let shouldResetSnapshot = await state.ensureRemoteContext(profileKey: Self.remoteProfileKey(profile))
        if shouldResetSnapshot {
            committedView.reset()
            // Tied to snapshotCache identity: a profile switch invalidates whatever
            // uncommitted-V2 fingerprints belonged to the previous profile. Without
            // this, V1 path leaves stale entries that later make
            // `committedAssetFingerprintsByMonth` no-op on optional-chain subtraction
            // (cache no longer has the month, so byMonth[month]?.subtract(...) misses).
            resetUncommittedV2()
        }

        let inspection = try await RemoteFormatCompatibilityService()
            .inspectRemoteFormat(client: client, profile: profile)
        let alreadyV2 = await state.isV2Repo() == true
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .v2:
            await state.setIsV2Repo(true)
            return try await syncIndexV2(client: client, profile: profile, eventStream: eventStream, onSyncProgress: onSyncProgress, syncStart: syncStart, preMaterialized: preMaterialized)
        case .v1:
            // V1 manifest reappearing after V2 confirmation = legacy peer is writing; refuse stale V2 cache.
            if alreadyV2 || expectV2 {
                await state.setOverlayFresh(false)
                throw BackupCompatibilityError.requiresForegroundMigration
            }
            await state.setIsV2Repo(false)
            await state.setOverlayFresh(false)
        case .fresh:
            await state.setIsV2Repo(true)
            await state.setOverlayFresh(false)
        }

        let scanStart = CFAbsoluteTimeGetCurrent()
        let remoteDigests = try await scanManifestDigests(
            client: client,
            basePath: profile.basePath
        )
        let scanElapsed = CFAbsoluteTimeGetCurrent() - scanStart
        syncLog.info("[SyncTiming] scanManifestDigests: \(Self.ms(scanElapsed))s (\(remoteDigests.count) months)")

        let previousDigests = await state.currentRemoteManifestDigests()

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

        if changedMonths.isEmpty, removedMonths.isEmpty {
            committedView.markSynced(Date())
            let digest = committedView.counts()
            let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
            syncLog.info("[SyncTiming] No changes. Total: \(Self.ms(totalElapsed))s")
            eventStream?.emitLog(
                String.localizedStringWithFormat(String(localized: "backup.remoteIndex.unchanged"), remoteMonths.count),
                level: .debug
            )
            return digest
        }

        syncLog.info("[SyncTiming] changedMonths: \(changedMonths.count), removedMonths: \(removedMonths.count)")

        var appliedChangedMonths = 0
        var appliedRemovedMonths = 0
        var processedMonthCount = 0

        for month in changedMonths.sorted() {
            let monthStart = CFAbsoluteTimeGetCurrent()
            guard let store = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: profile.basePath,
                year: month.year,
                month: month.month
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
            if committedView.replaceMonth(
                month,
                resources: snapshot.resources,
                assets: snapshot.assets,
                assetResourceLinks: snapshot.links
            ) {
                appliedChangedMonths += 1
            }
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
            let processElapsed = CFAbsoluteTimeGetCurrent() - processStart
            syncLog.info(
                "[SyncTiming] Month \(month.text): download=\(Self.ms(downloadElapsed))s, process=\(Self.ms(processElapsed))s, assets=\(snapshot.assets.count), resources=\(snapshot.resources.count), links=\(snapshot.links.count)"
            )
        }

        for month in removedMonths.sorted() {
            if committedView.removeMonth(month) {
                appliedRemovedMonths += 1
            }
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
        }

        // Keep scan-time digests even when loadManifestDirect rewrote the manifest:
        // refreshing post-flush would race with concurrent writers and drop their updates.
        await state.updateRemoteManifestDigests(remoteDigests)

        committedView.markSynced(Date())
        let digest = committedView.counts()
        let totalElapsed = CFAbsoluteTimeGetCurrent() - syncStart
        syncLog.info("[SyncTiming] Sync complete. Total: \(Self.ms(totalElapsed))s, changed: \(appliedChangedMonths), removed: \(appliedRemovedMonths)")

        return digest
    }


    private func syncIndexV2(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        eventStream: BackupEventStream?,
        onSyncProgress: (@Sendable (RemoteSyncProgress) -> Void)?,
        syncStart: CFAbsoluteTime,
        preMaterialized: RepoMaterializer.MaterializeOutput? = nil
    ) async throws -> RemoteIndexSyncDigest {
        // Reuse builder's materialize output to avoid running it twice in prepareRun.
        let output: RepoMaterializer.MaterializeOutput
        if let preMaterialized {
            output = preMaterialized
        } else {
            let bootstrap = RepoBootstrap(client: client, basePath: profile.basePath)
            // Absent repo.json on a V2 repo is broken identity state — surface, not silently
            // disable repoID filtering (would let foreign commits leak into cache).
            let expectedRepoID: String
            switch try await bootstrap.loadRepoIDStrict() {
            case .absent:
                throw NSError(
                    domain: "RemoteIndexSyncService",
                    code: -50,
                    userInfo: [NSLocalizedDescriptionKey: "V2 repo missing .watermelon/repo.json — backup-flow can repair, sync cannot"]
                )
            case .found(let id):
                expectedRepoID = id
            }
            let materializer = RepoMaterializer(client: client, basePath: profile.basePath)
            output = try await materializer.materialize(expectedRepoID: expectedRepoID)
        }
        // After fresh materialize, all assets in the cache are committed.
        resetUncommittedV2()
        // Snapshot overlay before load-clear; on refresh failure stale > empty (empty makes resume skip real-missing).
        let priorOverlay = committedView.physicallyMissingSnapshot()
        committedView.loadFromMaterialize(output)
        var overlayFresh = false
        do {
            overlayFresh = try await refreshPhysicalPresenceOverlay(client: client, basePath: profile.basePath, fallback: priorOverlay)
        } catch is CancellationError {
            // Don't mask cancel as success.
            for (month, hashes) in priorOverlay {
                committedView.markPhysicallyMissing(month: month, hashes: hashes)
            }
            throw CancellationError()
        } catch {
            for (month, hashes) in priorOverlay {
                committedView.markPhysicallyMissing(month: month, hashes: hashes)
            }
            syncLog.info("[SyncTiming] overlay refresh skipped: \(error.localizedDescription)")
        }
        await state.setOverlayFresh(overlayFresh)
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

    /// Backup workers reconcile inline via `MonthManifestStore.loadOrCreate`.
    func verifyMonth(
        client: RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey
    ) async throws {
        // V1-only path. If a V2 repo lost metadata and inspection misclassified it
        // as V1, this would write V1 manifest state into `committedView` and pollute
        // the V2 cache. metadata() must fail-closed — `try?` would swallow a transient
        // permission/network error as "not V2" and let V1 logic run on a V2 repo.
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
        if let meta = versionMeta, !meta.isDirectory {
            throw NSError(domain: "RemoteIndexSyncService", code: -60, userInfo: [
                NSLocalizedDescriptionKey:
                    "verifyMonth(V1) refused: \(versionPath) exists — repo is V2; use RepoVerifyMonthService"
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
            _ = committedView.removeMonth(month)
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
        // Fold-compare: NAS may return listing in different case than manifest stored.
        let remoteFileNames = entries
            .filter { !$0.isDirectory && $0.name != MonthManifestStore.manifestFileName }
            .map(\.name)
        let remoteCollisionKeys = RemoteFileNaming.collisionKeySet(from: Set(remoteFileNames))
        let listingMissing = store.existingFileNames().filter { name in
            !remoteCollisionKeys.contains(RemoteFileNaming.collisionKey(for: name))
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
        _ = committedView.replaceMonth(
            month,
            resources: snapshot.resources,
            assets: snapshot.assets,
            assetResourceLinks: snapshot.links
        )
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

    func remoteMonthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        committedView.monthRawData(for: month)
    }

    func currentState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        committedView.state(since: revision)
    }

    /// Production callers must go through `OptimisticAssetWriter` so optimistic
    /// upsert + inflight marking can't drift out of sync.
    func makeOptimisticAssetWriter() -> OptimisticAssetWriter {
        OptimisticAssetWriter(service: self)
    }

    fileprivate func _optimisticUpsertResource(_ item: RemoteManifestResource) {
        committedView.applyOptimisticUpsert(resource: item)
    }

    fileprivate func _optimisticUpsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]?) {
        committedView.applyOptimisticUpsert(asset: asset, links: links)
    }

    fileprivate func _markUncommitted(month: LibraryMonthKey, fingerprints: Set<Data>) {
        inflightTracker.markUncommittedAssets(month: month, fingerprints: fingerprints)
    }

    /// Recovers inflight-tracker state from `V2MonthSession.FlushError.snapshotWriteFailed`.
    /// Caller pattern: `catch { remoteIndexService.recordCommittedFromError(month:, error); throw error }`.
    /// No-op for other error types.
    func recordCommittedFromFlushError(month: LibraryMonthKey, _ error: Error) {
        if case let V2MonthSession.FlushError.snapshotWriteFailed(assets, tombstones, _) = error {
            markCommittedV2(month: month, fingerprints: assets.union(tombstones))
        }
    }

    func markCommittedV2(month: LibraryMonthKey, fingerprints: Set<Data>) {
        inflightTracker.markCommitted(month: month, fingerprints: fingerprints)
    }

    /// Reset uncommitted tracking — used after a fresh V2 materialize or session boundary.
    func resetUncommittedV2() {
        inflightTracker.reset()
    }

    /// Full wipe for cross-profile reuse. BG runner shares one service across profiles;
    /// without clearing cache + overlay, profile A's resources mix with profile B's
    /// missing-overlay and committedAssetFingerprintsByMonth returns garbage.
    func resetForProfileSwitch() async {
        inflightTracker.reset()
        committedView.reset()
        await state.reset()
    }

    /// `nil` until the first successful inspection; callers must not collapse nil to V1.
    func currentRepoIsV2() async -> Bool? {
        await state.isV2Repo()
    }

    /// Pin V2 when a caller has independently confirmed it, so a non-fatal syncIndex throw can't strand isV2 unset.
    func markIsV2() async {
        await state.setIsV2Repo(true)
    }

    /// True only when the last overlay refresh probed every month successfully; fast-path skip gates on this.
    func lastSyncOverlayFresh() async -> Bool {
        await state.overlayFresh()
    }

    /// Returns committed fingerprints grouped by month. Cache + per-month uncommitted
    /// subtraction. Resume planner uses per-asset month to dedup correctly.
    /// Phantom / partially-missing / metadata-only assets are excluded — the resume
    /// planner would otherwise skip a local asset whose fingerprint is "in cache" but
    /// whose remote resources are gone, leaving a real gap unfilled.
    func committedAssetFingerprintsByMonth() -> PerMonth<Set<Data>> {
        let snapshot = committedView.current()
        var linksByMonthFP: [LibraryMonthKey: [Data: [RemoteAssetResourceLink]]] = [:]
        for link in snapshot.assetResourceLinks {
            let month = LibraryMonthKey(year: link.year, month: link.month)
            linksByMonthFP[month, default: [:]][link.assetFingerprint, default: []].append(link)
        }
        var resourceHashesByMonth: [LibraryMonthKey: Set<Data>] = [:]
        for resource in snapshot.resources {
            let month = LibraryMonthKey(year: resource.year, month: resource.month)
            resourceHashesByMonth[month, default: []].insert(resource.contentHash)
        }
        var byMonth = PerMonth<Set<Data>>()
        // Snapshot already carries an atomic overlay map — re-querying per asset both
        // duplicates lock acquires and risks reading a different generation of the overlay
        // than the cache snapshot above.
        let missingByMonth = snapshot.physicallyMissingHashesByMonth
        for asset in snapshot.assets {
            let month = LibraryMonthKey(year: asset.year, month: asset.month)
            let links = linksByMonthFP[month]?[asset.assetFingerprint] ?? []
            // Subtract physically-missing — commit log keeps the row, but the
            // file is gone, so resume planner must NOT skip the repair.
            let availableHashes = (resourceHashesByMonth[month] ?? [])
                .subtracting(missingByMonth[month] ?? [])
            let state = RemoteAssetIntegrityClassifier.classify(
                assetFingerprint: asset.assetFingerprint,
                links: links,
                isResourceAvailable: { availableHashes.contains($0) }
            )
            if state.isHealthy {
                byMonth.insert(asset.assetFingerprint, for: month)
            }
        }
        // Subtract inflight: assets/tombstones we wrote optimistically but
        // haven't confirmed via commit log fold yet. Resume planner needs the
        // pure-committed view; UI layers see committed + inflight via the cache.
        inflightTracker.readUncommittedAssets { uncommittedAssets in
            for month in uncommittedAssets.months {
                if let fps = uncommittedAssets[month] {
                    byMonth.subtract(fps, from: month)
                }
            }
        }
        return byMonth
    }

    func replaceCachedMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        links: [RemoteAssetResourceLink]
    ) {
        _ = committedView.replaceMonth(month, resources: resources, assets: assets, assetResourceLinks: links)
    }

    /// V2-only physical-presence overlay; subtracted in committed view.
    func markPhysicallyMissingV2(month: LibraryMonthKey, hashes: Set<Data>) {
        committedView.markPhysicallyMissing(month: month, hashes: hashes)
    }

    func physicallyMissingHashesForTest(month: LibraryMonthKey) -> Set<Data> {
        committedView.physicallyMissingHashes(for: month)
    }

    /// Per-month best-effort; throwing on one failure would wipe overlay worse than a stale partial. Returns true only when every month succeeded.
    @discardableResult
    func refreshPhysicalPresenceOverlay(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: [LibraryMonthKey: Set<Data>] = [:],
        concurrencyCap: Int = 4
    ) async throws -> Bool {
        try Task.checkCancellation()
        let snapshot = committedView.current()
        var resourcesByMonth: [LibraryMonthKey: [RemoteManifestResource]] = [:]
        for resource in snapshot.resources {
            let month = LibraryMonthKey(year: resource.year, month: resource.month)
            resourcesByMonth[month, default: []].append(resource)
        }
        let effectiveCap = client.concurrencyMode == .serialOnly ? 1 : concurrencyCap
        var iterator = resourcesByMonth.makeIterator()
        var anyFailure = false
        try await withThrowingTaskGroup(of: (LibraryMonthKey, Result<Set<Data>, Error>).self) { group in
            for _ in 0..<effectiveCap {
                guard let (month, resources) = iterator.next() else { break }
                group.addTask { [self] in
                    try Task.checkCancellation()
                    do {
                        let (m, missing) = try await probeMonthForMissing(client: client, basePath: basePath, month: month, resources: resources)
                        return (m, .success(missing))
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch {
                        return (month, .failure(error))
                    }
                }
            }
            while let (month, result) = try await group.next() {
                try Task.checkCancellation()
                switch result {
                case .success(let missing):
                    committedView.markPhysicallyMissing(month: month, hashes: missing)
                case .failure(let error):
                    anyFailure = true
                    if let stale = fallback[month] {
                        committedView.markPhysicallyMissing(month: month, hashes: stale)
                    }
                    // Cold-start failure leaves overlay untouched; callers must gate on `anyFailure`.
                    syncLog.info("[SyncTiming] probe failed for \(month.text): \(error.localizedDescription)")
                }
                if let (nextMonth, nextResources) = iterator.next() {
                    group.addTask { [self] in
                        try Task.checkCancellation()
                        do {
                            let (m, missing) = try await probeMonthForMissing(client: client, basePath: basePath, month: nextMonth, resources: nextResources)
                            return (m, .success(missing))
                        } catch is CancellationError {
                            throw CancellationError()
                        } catch {
                            return (nextMonth, .failure(error))
                        }
                    }
                }
            }
        }
        return !anyFailure
    }

    private func probeMonthForMissing(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        resources: [RemoteManifestResource]
    ) async throws -> (LibraryMonthKey, Set<Data>) {
        try Task.checkCancellation()
        let monthRel = String(format: "%04d/%02d", month.year, month.month)
        let monthAbs = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRel)
        let entries = try await client.list(path: monthAbs)
        try Task.checkCancellation()
        var sizeByKey: [String: Int64] = [:]
        for entry in entries where !entry.isDirectory {
            sizeByKey[RemoteFileNaming.collisionKey(for: entry.name)] = entry.size
        }
        var resourcesByHash: [Data: [RemoteManifestResource]] = [:]
        for resource in resources {
            resourcesByHash[resource.contentHash, default: []].append(resource)
        }
        var missing: Set<Data> = []
        for (hash, group) in resourcesByHash {
            let anyPresent = group.contains { resource in
                let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
                guard let listedSize = sizeByKey[RemoteFileNaming.collisionKey(for: leaf)] else {
                    return false
                }
                // Size mismatch = stale/truncated; same-size silent corruption needs deeper verify.
                return listedSize == resource.fileSize
            }
            if !anyPresent { missing.insert(hash) }
        }
        return (month, missing)
    }

    private static func isNotFoundError(_ error: Error) -> Bool {
        isStorageNotFoundError(error)
    }

    private static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
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

    private func scanManifestDigests(
        client: RemoteStorageClientProtocol,
        basePath: String,
        cancellationController: BackupCancellationController? = nil
    ) async throws -> [LibraryMonthKey: RemoteMonthManifestDigest] {
        let normalizedBasePath = RemotePathBuilder.normalizePath(basePath)
        try cancellationController?.throwIfCancelled()

        let yearEntries = try await client.list(path: normalizedBasePath)
            .filter { $0.isDirectory }
            .filter { Self.parseYear($0.name) != nil }
            .sorted { $0.name < $1.name }

        var digests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]
        digests.reserveCapacity(yearEntries.count * 12)

        for yearEntry in yearEntries {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            guard let year = Self.parseYear(yearEntry.name) else { continue }

            let monthEntries = try await client.list(path: yearEntry.path)
                .filter { $0.isDirectory }
                .filter { Self.parseMonth($0.name) != nil }
                .sorted { $0.name < $1.name }

            for monthEntry in monthEntries {
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
                guard let month = Self.parseMonth(monthEntry.name) else { continue }
                let manifestPath = RemotePathBuilder.absolutePath(
                    basePath: normalizedBasePath,
                    remoteRelativePath: "\(yearEntry.name)/\(monthEntry.name)/\(MonthManifestStore.manifestFileName)"
                )
                guard let manifestEntry = try await client.metadata(path: manifestPath),
                      manifestEntry.isDirectory == false else {
                    continue
                }

                let monthKey = LibraryMonthKey(year: year, month: month)
                let modifiedMs = manifestEntry.modificationDate?.millisecondsSinceEpoch
                digests[monthKey] = RemoteMonthManifestDigest(
                    month: monthKey,
                    manifestSize: manifestEntry.size,
                    manifestModifiedAtMs: modifiedMs
                )
            }
        }

        return digests
    }

    private static func parseYear(_ value: String) -> Int? {
        guard value.count == 4, let number = Int(value), number >= 1900 else { return nil }
        return number
    }

    private static func parseMonth(_ value: String) -> Int? {
        guard value.count == 2, let number = Int(value), (1 ... 12).contains(number) else { return nil }
        return number
    }

    private static func ms(_ seconds: CFAbsoluteTime) -> String {
        String(format: "%.3f", seconds)
    }
}

/// Sole production entry point for optimistic-cache writes that must keep
/// inflight tracking in sync. `RemoteIndexSyncService.markCommittedV2` clears
/// the inflight side at flush success — separate concern, kept on the service.
struct OptimisticAssetWriter: Sendable {
    fileprivate let service: RemoteIndexSyncService

    /// Optimistic cache write + (V2 only) inflight marking, atomic from the
    /// caller's view. `markUncommitted: false` is the phantom/test-asset path
    /// where we want classifier subtraction, not inflight subtraction.
    func appendAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink]?,
        markUncommitted: Bool
    ) {
        service._optimisticUpsertAsset(asset, links: links)
        if markUncommitted {
            let month = LibraryMonthKey(year: asset.year, month: asset.month)
            service._markUncommitted(month: month, fingerprints: [asset.assetFingerprint])
        }
    }

    func appendResource(_ resource: RemoteManifestResource) {
        service._optimisticUpsertResource(resource)
    }

    /// For tests / non-asset paths that need to seed inflight state directly.
    /// Production AssetProcessor path goes through `appendAsset(markUncommitted: true)`.
    func markUncommitted(month: LibraryMonthKey, fingerprints: Set<Data>) {
        service._markUncommitted(month: month, fingerprints: fingerprints)
    }
}

import Foundation
import os.log

private let syncLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

final class RemoteIndexSyncService: @unchecked Sendable {
    private static let overlayProbeMaxVerifiedFilesPerMonth = 64
    private static let overlayProbeMaxVerifiedBytesPerMonth: Int64 = 32 * 1024 * 1024

    private struct OverlayProbeBudget: Sendable {
        let maxVerifiedFilesPerMonth: Int
        let maxVerifiedBytesPerMonth: Int64
    }

    private enum OverlayStaleFallbackPolicy: Sendable {
        case failClosedWhenMissingFallback
        case preserveFallback
    }

    private struct OverlayMonthProbe: Sendable {
        let month: LibraryMonthKey
        let missingHashes: Set<Data>
        let inconclusiveHashes: Set<Data>

        var fresh: Bool { inconclusiveHashes.isEmpty }
    }

    private struct OverlayProbeResult: Sendable {
        let fresh: Bool
        let missingByMonth: [LibraryMonthKey: Set<Data>]
        let freshMonths: Set<LibraryMonthKey>
    }

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
                    Task(priority: Task.currentPriority) { await self.registerWaiter(id: id, continuation: cont) }
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

        func ensureRemoteContext(profileKey: String) -> Bool {
            guard activeRemoteProfileKey != profileKey else { return false }
            activeRemoteProfileKey = profileKey
            remoteManifestDigests.removeAll()
            lastInspectedFormatIsV2 = nil
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

        func reset() {
            activeRemoteProfileKey = nil
            remoteManifestDigests.removeAll()
            lastInspectedFormatIsV2 = nil
        }
    }

    private let committedView: RepoCommittedView
    private let inflightTracker: OptimisticInflightTracker
    private let optimisticMutationLock = NSLock()
    private var physicalPresenceOverlayFresh = false
    private var physicalPresenceOverlayFreshMonths: Set<LibraryMonthKey> = []
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

    @discardableResult
    func syncOverlayOnly(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: [LibraryMonthKey: Set<Data>] = [:],
        concurrencyCap: Int = 4
    ) async throws -> Bool {
        try await syncGate.withLock { [self] in
            try await refreshPhysicalPresenceOverlay(
                client: client,
                basePath: basePath,
                fallback: fallback,
                concurrencyCap: concurrencyCap
            )
        }
    }

    func syncOverlayAndCaptureHandle(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: [LibraryMonthKey: Set<Data>]? = nil,
        concurrencyCap: Int = 4
    ) async throws -> RemoteViewHandle {
        let captured = try await syncGate.withLock { [self] in
            optimisticMutationLock.withLock {
                let current = committedView.currentSnapshotWithRevision()
                return (
                    revision: current.revision,
                    snapshot: current.snapshot,
                    fallback: fallback ?? current.snapshot.physicallyMissingHashesByMonth
                )
            }
        }
        let probe = try await probePhysicalPresenceOverlay(
            snapshot: captured.snapshot,
            client: client,
            basePath: basePath,
            fallback: captured.fallback,
            budget: nil,
            staleFallbackPolicy: .failClosedWhenMissingFallback,
            concurrencyCap: concurrencyCap
        )
        return try await syncGate.withLock { [self] in
            let revisionUnchanged = applyPhysicalPresenceOverlay(
                probe.missingByMonth,
                expectedRevision: captured.revision,
                allFresh: probe.fresh,
                freshMonths: probe.freshMonths
            )
            if !revisionUnchanged {
                syncLog.info("[SyncTiming] overlay handle captured stale revision; preserving previous overlay")
            }
            let overlayFresh = revisionUnchanged && probe.fresh
            // Stale handles may include newer commits; resume callers reject stale overlay freshness.
            return self.handleFromCurrentSnapshot(overlayFresh: overlayFresh)
        }
    }

    private func handleFromCurrentSnapshot(overlayFresh: Bool) -> RemoteViewHandle {
        let captured = optimisticMutationLock.withLock {
            let (revision, snapshot) = committedView.currentSnapshotWithRevision()
            let fingerprints = Self.committedFingerprints(
                from: snapshot,
                subtractingInflight: inflightTracker
            )
            return (revision: revision, fingerprints: fingerprints)
        }
        return RemoteViewHandle(
            revision: captured.revision,
            committedAssetFingerprintsByMonth: captured.fingerprints,
            overlayFreshness: overlayFresh ? .fresh : .stale,
            producedAt: Date()
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
        expectV2: Bool = false
    ) async throws -> RemoteIndexSyncDigest {
        let syncStart = CFAbsoluteTimeGetCurrent()

        let shouldResetSnapshot = await state.ensureRemoteContext(profileKey: Self.remoteProfileKey(profile))
        if shouldResetSnapshot {
            resetCommittedViewAndUncommitted()
        }

        let alreadyV2 = await state.isV2Repo() == true
        let inspection = try await RemoteFormatCompatibilityService()
            .inspectRemoteFormat(client: client, profile: profile)
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .v2:
            await state.setIsV2Repo(true)
            return try await syncIndexV2(client: client, profile: profile, eventStream: eventStream, onSyncProgress: onSyncProgress, syncStart: syncStart, preMaterialized: preMaterialized)
        case .v2WithPendingMigrationCleanup:
            await state.setIsV2Repo(true)
            return try await syncIndexV2(client: client, profile: profile, eventStream: eventStream, onSyncProgress: onSyncProgress, syncStart: syncStart, preMaterialized: nil)
        case .v2WithV1Manifests:
            clearPhysicalPresenceOverlayFreshness()
            throw BackupCompatibilityError.requiresForegroundMigration
        case .v1:
            if alreadyV2 || expectV2 {
                clearPhysicalPresenceOverlayFreshness()
                throw BackupCompatibilityError.requiresForegroundMigration
            }
            await state.setIsV2Repo(false)
            clearPhysicalPresenceOverlayFreshness()
        case .fresh:
            if alreadyV2 || expectV2 {
                clearPhysicalPresenceOverlayFreshness()
                throw BackupCompatibilityError.damagedV2Repo
            }
            clearPhysicalPresenceOverlayFreshness()
        }

        let scanStart = CFAbsoluteTimeGetCurrent()
        let remoteDigests = try await scanManifestDigests(
            client: client,
            basePath: profile.basePath
        )
        var effectiveRemoteDigests = remoteDigests
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

        var stagedChangedMonths: [
            LibraryMonthKey: (
                resources: [RemoteManifestResource],
                assets: [RemoteManifestAsset],
                links: [RemoteAssetResourceLink]
            )
        ] = [:]
        var stagedMissingMonths = Set<LibraryMonthKey>()
        var processedMonthCount = 0

        for month in changedMonths.sorted() {
            try Task.checkCancellation()
            let monthStart = CFAbsoluteTimeGetCurrent()
            guard let store = try await MonthManifestStore.loadManifestDirect(
                client: client,
                basePath: profile.basePath,
                year: month.year,
                month: month.month
            ) else {
                stagedMissingMonths.insert(month)
                effectiveRemoteDigests.removeValue(forKey: month)
                processedMonthCount += 1
                onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
                continue
            }
            let downloadElapsed = CFAbsoluteTimeGetCurrent() - monthStart

            let processStart = CFAbsoluteTimeGetCurrent()
            let snapshot = store.unsortedSnapshot()
            stagedChangedMonths[month] = (snapshot.resources, snapshot.assets, snapshot.links)
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
            let processElapsed = CFAbsoluteTimeGetCurrent() - processStart
            syncLog.info(
                "[SyncTiming] Month \(month.text): download=\(Self.ms(downloadElapsed))s, process=\(Self.ms(processElapsed))s, assets=\(snapshot.assets.count), resources=\(snapshot.resources.count), links=\(snapshot.links.count)"
            )
        }

        try Task.checkCancellation()
        let appliedCounts = optimisticMutationLock.withLock { () -> (changed: Int, removed: Int) in
            var changed = 0
            var removed = 0
            for month in stagedMissingMonths.sorted() {
                if committedView.removeMonth(month) {
                    changed += 1
                }
            }
            for month in stagedChangedMonths.keys.sorted() {
                guard let snapshot = stagedChangedMonths[month] else { continue }
                if committedView.replaceMonth(
                    month,
                    resources: snapshot.resources,
                    assets: snapshot.assets,
                    assetResourceLinks: snapshot.links
                ) {
                    changed += 1
                }
            }

            for month in removedMonths.sorted() {
                if committedView.removeMonth(month) {
                    removed += 1
                }
            }
            return (changed, removed)
        }

        for _ in removedMonths.sorted() {
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
        }

        await state.updateRemoteManifestDigests(effectiveRemoteDigests)

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
        preMaterialized: RepoMaterializer.MaterializeOutput? = nil
    ) async throws -> RemoteIndexSyncDigest {
        let output: RepoMaterializer.MaterializeOutput
        if let preMaterialized {
            output = preMaterialized
        } else {
            let bootstrap = RepoBootstrap(client: client, basePath: profile.basePath)
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
        let priorOverlay = resetUncommittedAndLoadMaterialize(output)
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
        if let meta = versionMeta, !meta.isDirectory {
            throw NSError(domain: "RemoteIndexSyncService", code: -60, userInfo: [
                NSLocalizedDescriptionKey:
                    "verifyMonth(V1) refused: \(versionPath) exists — repo is V2; use RepoVerifyMonthService"
            ])
        }
        if try await Self.hasAnyV2CommitOrSnapshotData(client: client, basePath: basePath) {
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
        let caseSensitive = client.backendNameCaseSensitivity.usesExactNameMatchingForPresence
        func presenceKey(_ name: String) -> String {
            caseSensitive ? name : RemoteFileNaming.collisionKey(for: name)
        }
        let remotePresenceKeys = Set(remoteFileNames.map(presenceKey))
        let listingMissing = store.existingFileNames().filter { name in
            !remotePresenceKeys.contains(presenceKey(name))
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

    func remoteMonthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        optimisticMutationLock.withLock {
            guard let delta = committedView.monthRawData(for: month) else { return nil }
            let uncommitted = inflightTracker.readUncommittedAssets { $0[month] ?? [] }
            return Self.filterHomeFacingDelta(delta, excludingAssetFingerprints: uncommitted)
        }
    }

    func currentState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        optimisticMutationLock.withLock {
            let state = committedView.state(since: revision)
            let uncommitted = inflightTracker.readUncommittedAssets { $0 }
            guard !uncommitted.isEmpty else { return state }
            return RemoteLibrarySnapshotState(
                revision: state.revision,
                isFullSnapshot: state.isFullSnapshot,
                monthDeltas: state.monthDeltas.map { delta in
                    Self.filterHomeFacingDelta(
                        delta,
                        excludingAssetFingerprints: uncommitted[delta.month] ?? []
                    )
                }
            )
        }
    }

    func makeOptimisticAssetWriter() -> OptimisticAssetWriter {
        OptimisticAssetWriter(service: self)
    }

    fileprivate func _optimisticUpsertResource(_ item: RemoteManifestResource) {
        optimisticMutationLock.withLock {
            committedView.applyOptimisticUpsert(resource: item)
        }
    }

    fileprivate func _optimisticUpsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]?) {
        optimisticMutationLock.withLock {
            committedView.applyOptimisticUpsert(asset: asset, links: links)
        }
    }

    fileprivate func _markUncommitted(month: LibraryMonthKey, fingerprints: Set<Data>) {
        optimisticMutationLock.withLock {
            inflightTracker.markUncommittedAssets(month: month, fingerprints: fingerprints)
        }
    }

    fileprivate func _appendOptimisticAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink]?,
        markUncommitted: Bool
    ) {
        optimisticMutationLock.withLock {
            if markUncommitted {
                let month = LibraryMonthKey(year: asset.year, month: asset.month)
                inflightTracker.markUncommittedAssets(month: month, fingerprints: [asset.assetFingerprint])
            }
            committedView.applyOptimisticUpsert(asset: asset, links: links)
        }
    }

    func recordCommittedFromFlushError(month: LibraryMonthKey, _ error: Error) {
        if case let V2MonthSession.FlushError.snapshotWriteFailed(assets, tombstones, _) = error {
            markCommittedV2(month: month, fingerprints: assets.union(tombstones))
        } else {
            syncLog.info("[SyncTiming] flush error before durable snapshot handoff for \(month.text): \(error.localizedDescription)")
        }
    }

    func markCommittedV2(month: LibraryMonthKey, fingerprints: Set<Data>) {
        guard !fingerprints.isEmpty else { return }
        optimisticMutationLock.withLock {
            if inflightTracker.markCommitted(month: month, fingerprints: fingerprints) {
                committedView.markMonthsChanged([month])
            }
        }
    }

    func resetUncommittedV2() {
        optimisticMutationLock.withLock {
            let months = inflightTracker.reset()
            if !months.isEmpty {
                committedView.markMonthsChanged(months)
            }
        }
    }

    func resetForProfileSwitch() async {
        resetCommittedViewAndUncommitted()
        await state.reset()
    }

    private func resetUncommittedAndLoadMaterialize(_ output: RepoMaterializer.MaterializeOutput) -> [LibraryMonthKey: Set<Data>] {
        optimisticMutationLock.withLock {
            _ = inflightTracker.reset()
            clearPhysicalPresenceOverlayFreshnessLocked()
            return committedView.loadFromMaterialize(output)
        }
    }

    private func resetCommittedViewAndUncommitted() {
        optimisticMutationLock.withLock {
            _ = inflightTracker.reset()
            committedView.reset()
            clearPhysicalPresenceOverlayFreshnessLocked()
        }
    }

    private func clearPhysicalPresenceOverlayFreshness() {
        optimisticMutationLock.withLock {
            clearPhysicalPresenceOverlayFreshnessLocked()
        }
    }

    private func clearPhysicalPresenceOverlayFreshnessLocked() {
        physicalPresenceOverlayFresh = false
        physicalPresenceOverlayFreshMonths.removeAll()
    }

    private func setPhysicalPresenceOverlayFreshnessLocked(
        allFresh: Bool,
        freshMonths: Set<LibraryMonthKey>
    ) {
        physicalPresenceOverlayFresh = allFresh
        physicalPresenceOverlayFreshMonths = freshMonths
    }

    func currentRepoIsV2() async -> Bool? {
        await state.isV2Repo()
    }

    func markIsV2() async {
        await state.setIsV2Repo(true)
    }

    func lastSyncOverlayFresh() async -> Bool {
        optimisticMutationLock.withLock {
            physicalPresenceOverlayFresh
        }
    }

    func committedAssetFingerprintsByMonth() -> PerMonth<Set<Data>> {
        optimisticMutationLock.withLock {
            Self.committedFingerprints(
                from: committedView.current(),
                subtractingInflight: inflightTracker
            )
        }
    }

    private static func filterHomeFacingDelta(
        _ delta: RemoteLibraryMonthDelta,
        excludingAssetFingerprints: Set<Data>
    ) -> RemoteLibraryMonthDelta {
        guard !excludingAssetFingerprints.isEmpty else { return delta }
        return RemoteLibraryMonthDelta(
            month: delta.month,
            resources: delta.resources,
            assets: delta.assets.filter { !excludingAssetFingerprints.contains($0.assetFingerprint) },
            assetResourceLinks: delta.assetResourceLinks.filter { !excludingAssetFingerprints.contains($0.assetFingerprint) },
            physicallyMissingHashes: delta.physicallyMissingHashes
        )
    }

    private static func committedFingerprints(
        from snapshot: RemoteLibrarySnapshot,
        subtractingInflight inflightTracker: OptimisticInflightTracker
    ) -> PerMonth<Set<Data>> {
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
        let missingByMonth = snapshot.physicallyMissingHashesByMonth
        for asset in snapshot.assets {
            let month = LibraryMonthKey(year: asset.year, month: asset.month)
            let links = linksByMonthFP[month]?[asset.assetFingerprint] ?? []
            // Physically-missing resources still have commit-log rows; subtract so resume planner sees the asset as needing repair.
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
        links: [RemoteAssetResourceLink],
        physicallyMissingHashes: Set<Data>? = nil
    ) {
        optimisticMutationLock.withLock {
            _ = committedView.replaceMonth(
                month,
                resources: resources,
                assets: assets,
                assetResourceLinks: links,
                physicallyMissingHashes: physicallyMissingHashes
            )
            if physicallyMissingHashes == nil {
                physicalPresenceOverlayFreshMonths.remove(month)
                physicalPresenceOverlayFresh = false
            } else {
                physicalPresenceOverlayFreshMonths.insert(month)
            }
        }
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

    func verifiedPhysicallyMissingHashes(for month: LibraryMonthKey) async -> Set<Data>? {
        optimisticMutationLock.withLock {
            guard physicalPresenceOverlayFreshMonths.contains(month) else { return nil }
            return committedView.physicallyMissingHashes(for: month)
        }
    }

    func physicallyMissingHashesForTest(month: LibraryMonthKey) -> Set<Data> {
        physicallyMissingHashes(for: month)
    }

    func physicallyMissingSnapshot() -> [LibraryMonthKey: Set<Data>] {
        optimisticMutationLock.withLock {
            committedView.physicallyMissingSnapshot()
        }
    }

    @discardableResult
    func refreshPhysicalPresenceOverlay(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: [LibraryMonthKey: Set<Data>] = [:],
        concurrencyCap: Int = 4
    ) async throws -> Bool {
        try Task.checkCancellation()
        let captured = optimisticMutationLock.withLock {
            committedView.currentSnapshotWithRevision()
        }
        let probe = try await probePhysicalPresenceOverlay(
            snapshot: captured.snapshot,
            client: client,
            basePath: basePath,
            fallback: fallback,
            budget: OverlayProbeBudget(
                maxVerifiedFilesPerMonth: Self.overlayProbeMaxVerifiedFilesPerMonth,
                maxVerifiedBytesPerMonth: Self.overlayProbeMaxVerifiedBytesPerMonth
            ),
            staleFallbackPolicy: .preserveFallback,
            concurrencyCap: concurrencyCap
        )
        let applied = applyPhysicalPresenceOverlay(
            probe.missingByMonth,
            expectedRevision: captured.revision,
            allFresh: probe.fresh,
            freshMonths: probe.freshMonths
        )
        if !applied {
            syncLog.info("[SyncTiming] overlay refresh captured stale revision; preserving previous overlay")
            return false
        }
        return probe.fresh
    }

    @discardableResult
    private func applyPhysicalPresenceOverlay(
        _ missingByMonth: [LibraryMonthKey: Set<Data>],
        expectedRevision: UInt64? = nil,
        allFresh: Bool = false,
        freshMonths: Set<LibraryMonthKey> = []
    ) -> Bool {
        optimisticMutationLock.withLock {
            if let expectedRevision,
               committedView.currentRevision() != expectedRevision {
                clearPhysicalPresenceOverlayFreshnessLocked()
                return false
            }
            for (month, hashes) in missingByMonth {
                committedView.markPhysicallyMissing(month: month, hashes: hashes)
            }
            setPhysicalPresenceOverlayFreshnessLocked(allFresh: allFresh, freshMonths: freshMonths)
            return true
        }
    }

    private func probePhysicalPresenceOverlay(
        snapshot: RemoteLibrarySnapshot,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: [LibraryMonthKey: Set<Data>],
        budget: OverlayProbeBudget?,
        staleFallbackPolicy: OverlayStaleFallbackPolicy,
        concurrencyCap: Int
    ) async throws -> OverlayProbeResult {
        var resourcesByMonth: [LibraryMonthKey: [RemoteManifestResource]] = [:]
        for resource in snapshot.resources {
            let month = LibraryMonthKey(year: resource.year, month: resource.month)
            resourcesByMonth[month, default: []].append(resource)
        }
        let effectiveCap = client.concurrencyMode == .serialOnly ? 1 : concurrencyCap
        var iterator = resourcesByMonth.makeIterator()
        var anyFailure = false
        var missingByMonth: [LibraryMonthKey: Set<Data>] = [:]
        var freshMonths: Set<LibraryMonthKey> = []
        try await withThrowingTaskGroup(of: (LibraryMonthKey, Result<OverlayMonthProbe, Error>).self) { group in
            for _ in 0..<effectiveCap {
                guard let (month, resources) = iterator.next() else { break }
                group.addTask { [self] in
                    try Task.checkCancellation()
                    do {
                        let probe = try await probeMonthForMissing(client: client, basePath: basePath, month: month, resources: resources, budget: budget)
                        return (probe.month, .success(probe))
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
                case .success(let probe):
                    if !probe.fresh { anyFailure = true }
                    if probe.fresh { freshMonths.insert(month) }
                    var missing = probe.missingHashes
                    if !probe.inconclusiveHashes.isEmpty {
                        if let stale = fallback[month] {
                            missing.formUnion(stale.intersection(probe.inconclusiveHashes))
                        } else if case .failClosedWhenMissingFallback = staleFallbackPolicy {
                            missing.formUnion(probe.inconclusiveHashes)
                        }
                    }
                    missingByMonth[month] = missing
                case .failure(let error):
                    anyFailure = true
                    if let stale = fallback[month] {
                        missingByMonth[month] = stale
                    } else if case .failClosedWhenMissingFallback = staleFallbackPolicy {
                        let allHashes = Set((resourcesByMonth[month] ?? []).map(\.contentHash))
                        missingByMonth[month] = allHashes
                    } else {
                        missingByMonth[month] = []
                    }
                    syncLog.info("[SyncTiming] probe failed for \(month.text): \(error.localizedDescription)")
                }
                if let (nextMonth, nextResources) = iterator.next() {
                    group.addTask { [self] in
                        try Task.checkCancellation()
                        do {
                            let probe = try await probeMonthForMissing(client: client, basePath: basePath, month: nextMonth, resources: nextResources, budget: budget)
                            return (probe.month, .success(probe))
                        } catch is CancellationError {
                            throw CancellationError()
                        } catch {
                            return (nextMonth, .failure(error))
                        }
                    }
                }
            }
        }
        return OverlayProbeResult(
            fresh: !anyFailure,
            missingByMonth: missingByMonth,
            freshMonths: freshMonths
        )
    }

    private func probeMonthForMissing(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        budget: OverlayProbeBudget?
    ) async throws -> OverlayMonthProbe {
        try Task.checkCancellation()
        let monthRel = String(format: "%04d/%02d", month.year, month.month)
        let monthAbs = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRel)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbs)
        } catch {
            if isStorageNotFoundError(error) {
                return OverlayMonthProbe(
                    month: month,
                    missingHashes: Set(resources.map(\.contentHash)),
                    inconclusiveHashes: []
                )
            }
            throw error
        }
        try Task.checkCancellation()
        // Folding on a case-sensitive backend would equate IMG.JPG/img.jpg as one file.
        let caseSensitive = client.backendNameCaseSensitivity.usesExactNameMatchingForPresence
        func presenceKey(_ name: String) -> String {
            caseSensitive ? name : RemoteFileNaming.collisionKey(for: name)
        }
        struct ListedFile {
            let name: String
            let size: Int64
        }
        var entriesByKey: [String: [ListedFile]] = [:]
        for entry in entries where !entry.isDirectory {
            entriesByKey[presenceKey(entry.name), default: []].append(ListedFile(name: entry.name, size: entry.size))
        }
        var resourcesByHash: [Data: [RemoteManifestResource]] = [:]
        for resource in resources {
            resourcesByHash[resource.contentHash, default: []].append(resource)
        }
        var verifiedFileCount = 0
        var verifiedByteCount: Int64 = 0
        var loggedProbeBudgetExhausted = false
        var missingHashes: Set<Data> = []
        var inconclusiveHashes: Set<Data> = []
        for (hash, group) in resourcesByHash {
            var anyPresent = false
            var inconclusive = false
            candidateScan: for resource in group {
                try Task.checkCancellation()
                let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
                guard let listed = entriesByKey[presenceKey(leaf)] else { continue }
                let sizeMatches = listed.filter { $0.size == resource.fileSize }
                if sizeMatches.isEmpty { continue }
                for match in sizeMatches {
                    if let budget {
                        let budgetExceeded = verifiedFileCount >= budget.maxVerifiedFilesPerMonth ||
                            verifiedByteCount + resource.fileSize > budget.maxVerifiedBytesPerMonth
                        if budgetExceeded {
                            if !loggedProbeBudgetExhausted {
                                loggedProbeBudgetExhausted = true
                                syncLog.info("[SyncTiming] overlay probe budget exhausted for \(month.text); leaving unverified resources inconclusive")
                            }
                            inconclusive = true
                            break candidateScan
                        }
                    }
                    let path = RemotePathBuilder.absolutePath(
                        basePath: basePath,
                        remoteRelativePath: monthRel + "/" + match.name
                    )
                    do {
                        verifiedFileCount += 1
                        verifiedByteCount += resource.fileSize
                        if try await RemoteContentTrust.verifyHash(
                            client: client,
                            remotePath: path,
                            expectedSize: resource.fileSize,
                            expectedHash: hash
                        ) {
                            anyPresent = true
                            break candidateScan
                        }
                    } catch is CancellationError {
                        throw CancellationError()
                    } catch {
                        // Not-found = continue probing alternates; transport/truncation throws so overlay refresh records the month as inconclusive rather than falsely healthy.
                        if isStorageNotFoundError(error) { continue }
                        throw error
                    }
                }
            }
            if anyPresent {
                continue
            } else if inconclusive {
                inconclusiveHashes.insert(hash)
            } else {
                missingHashes.insert(hash)
            }
        }
        return OverlayMonthProbe(
            month: month,
            missingHashes: missingHashes,
            inconclusiveHashes: inconclusiveHashes
        )
    }

    private static func isNotFoundError(_ error: Error) -> Bool {
        isStorageNotFoundError(error)
    }

    private static func hasAnyV2CommitOrSnapshotData(
        client: RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Bool {
        for path in [
            RepoLayout.commitsDirectoryPath(base: basePath),
            RepoLayout.snapshotsDirectoryPath(base: basePath)
        ] {
            do {
                if !(try await client.list(path: path)).isEmpty {
                    return true
                }
            } catch {
                if isNotFoundError(error) { continue }
                throw error
            }
        }
        return false
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

struct OptimisticAssetWriter: Sendable {
    fileprivate let service: RemoteIndexSyncService

    /// Inflight must be marked before publish — reverse ordering lets a reader see the asset as committed but not inflight, and skip it as done.
    func appendAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink]?,
        markUncommitted: Bool
    ) {
        service._appendOptimisticAsset(asset, links: links, markUncommitted: markUncommitted)
    }

    func appendResource(_ resource: RemoteManifestResource) {
        service._optimisticUpsertResource(resource)
    }

    func markUncommitted(month: LibraryMonthKey, fingerprints: Set<Data>) {
        service._markUncommitted(month: month, fingerprints: fingerprints)
    }
}

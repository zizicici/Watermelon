import Foundation
import os.log

private let syncLog = Logger(subsystem: "com.zizicici.watermelon", category: "SyncTiming")

final class RemoteIndexSyncService: @unchecked Sendable {
    private actor SyncGate {
        private var isLocked = false
        private var waiters: [(UUID, CheckedContinuation<Void, Error>)] = []
        private var preCancelledIDs: Set<UUID> = []
        private var preCancelledOrder: [UUID] = []
        private var registeredIDs: Set<UUID> = []
        private static let preCancelledIDsCap = 256

        private func registerWaiter(id: UUID, continuation: CheckedContinuation<Void, Error>) {
            if consumePreCancelled(id) {
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
                cont.resume(throwing: CancellationError())
            } else if !registeredIDs.contains(id) {
                recordPreCancelled(id)
            }
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
                    Task { await self.registerWaiter(id: id, continuation: cont) }
                }
            } onCancel: {
                Task { await self.cancelWaiter(id: id) }
            }
        }

        private func release(id: UUID) {
            registeredIDs.remove(id)
            while !waiters.isEmpty {
                let next = waiters.removeFirst()
                if consumePreCancelled(next.0) {
                    registeredIDs.remove(next.0)
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
    private let optimisticMutationLock = NSLock()
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
            let fresh = try await refreshPhysicalPresenceOverlay(
                client: client,
                basePath: basePath,
                fallback: fallback,
                concurrencyCap: concurrencyCap
            )
            await state.setOverlayFresh(fresh)
            return fresh
        }
    }

    /// Single syncGate hold so a concurrent syncIndex can't mutate read-model between overlay refresh and handle capture.
    func syncOverlayAndCaptureHandle(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        fallback: [LibraryMonthKey: Set<Data>] = [:],
        concurrencyCap: Int = 4
    ) async throws -> RemoteViewHandle {
        try await syncGate.withLock { [self] in
            let fresh = try await refreshPhysicalPresenceOverlay(
                client: client,
                basePath: basePath,
                fallback: fallback,
                concurrencyCap: concurrencyCap
            )
            await state.setOverlayFresh(fresh)
            return self.handleFromCurrentSnapshot(overlayFresh: fresh)
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
            resetCommittedViewAndUncommitted()
        }

        let inspection = try await RemoteFormatCompatibilityService()
            .inspectRemoteFormat(client: client, profile: profile)
        let alreadyV2 = await state.isV2Repo() == true
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupCompatibilityError.remoteFormatUnsupported(minAppVersion: minAppVersion)
        case .v2, .v2WithPendingMigrationCleanup:
            await state.setIsV2Repo(true)
            return try await syncIndexV2(client: client, profile: profile, eventStream: eventStream, onSyncProgress: onSyncProgress, syncStart: syncStart, preMaterialized: preMaterialized)
        case .v2WithV1Manifests:
            await state.setOverlayFresh(false)
            throw BackupCompatibilityError.requiresForegroundMigration
        case .v1:
            if alreadyV2 || expectV2 {
                await state.setOverlayFresh(false)
                throw BackupCompatibilityError.requiresForegroundMigration
            }
            await state.setIsV2Repo(false)
            await state.setOverlayFresh(false)
        case .fresh:
            if alreadyV2 || expectV2 {
                await state.setOverlayFresh(false)
                throw BackupCompatibilityError.damagedV2Repo
            }
            await state.setOverlayFresh(false)
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
        var appliedChangedMonths = 0
        var appliedRemovedMonths = 0
        for month in stagedMissingMonths.sorted() {
            if committedView.removeMonth(month) {
                appliedChangedMonths += 1
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
                appliedChangedMonths += 1
            }
        }

        for month in removedMonths.sorted() {
            if committedView.removeMonth(month) {
                appliedRemovedMonths += 1
            }
            processedMonthCount += 1
            onSyncProgress?(RemoteSyncProgress(current: processedMonthCount, total: totalMonthsToProcess))
        }

        await state.updateRemoteManifestDigests(effectiveRemoteDigests)

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
        var overlayFresh = false
        do {
            overlayFresh = try await refreshPhysicalPresenceOverlay(client: client, basePath: profile.basePath, fallback: priorOverlay)
        } catch is CancellationError {
            for (month, hashes) in priorOverlay {
                committedView.markPhysicallyMissing(month: month, hashes: hashes)
            }
            await state.setOverlayFresh(false)
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
        let remoteFileNames = entries
            .filter { !$0.isDirectory && $0.name != MonthManifestStore.manifestFileName }
            .map(\.name)
        let caseSensitive = client.backendNameCaseSensitivity == .caseSensitive
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
        }
    }

    func markCommittedV2(month: LibraryMonthKey, fingerprints: Set<Data>) {
        optimisticMutationLock.withLock {
            inflightTracker.markCommitted(month: month, fingerprints: fingerprints)
        }
    }

    func resetUncommittedV2() {
        optimisticMutationLock.withLock {
            inflightTracker.reset()
        }
    }

    func resetForProfileSwitch() async {
        resetCommittedViewAndUncommitted()
        await state.reset()
    }

    private func resetUncommittedAndLoadMaterialize(_ output: RepoMaterializer.MaterializeOutput) -> [LibraryMonthKey: Set<Data>] {
        optimisticMutationLock.withLock {
            inflightTracker.reset()
            return committedView.loadFromMaterialize(output)
        }
    }

    private func resetCommittedViewAndUncommitted() {
        optimisticMutationLock.withLock {
            inflightTracker.reset()
            committedView.reset()
        }
    }

    func currentRepoIsV2() async -> Bool? {
        await state.isV2Repo()
    }

    func markIsV2() async {
        await state.setIsV2Repo(true)
    }

    func lastSyncOverlayFresh() async -> Bool {
        await state.overlayFresh()
    }

    func committedAssetFingerprintsByMonth() -> PerMonth<Set<Data>> {
        optimisticMutationLock.withLock {
            Self.committedFingerprints(
                from: committedView.current(),
                subtractingInflight: inflightTracker
            )
        }
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
                    } else {
                        // Fail-closed: Home/health/restore read the overlay without gating on freshness, so a cold-start probe failure must not look healthy.
                        let allHashes = Set((resourcesByMonth[month] ?? []).map(\.contentHash))
                        committedView.markPhysicallyMissing(month: month, hashes: allHashes)
                    }
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
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbs)
        } catch {
            if isStorageNotFoundError(error) {
                return (month, Set(resources.map(\.contentHash)))
            }
            throw error
        }
        try Task.checkCancellation()
        // Folding on a case-sensitive backend would equate IMG.JPG/img.jpg as one file.
        let caseSensitive = client.backendNameCaseSensitivity == .caseSensitive
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
        let smallLimit = RemoteContentTrust.overlayProbeSmallFileLimitBytes
        var missing: Set<Data> = []
        for (hash, group) in resourcesByHash {
            var anyPresent = false
            candidateScan: for resource in group {
                try Task.checkCancellation()
                let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
                guard let listed = entriesByKey[presenceKey(leaf)] else { continue }
                let sizeMatches = listed.filter { $0.size == resource.fileSize }
                if sizeMatches.isEmpty { continue }
                // Small files require content verification; size-only would dedup peer-overwritten bytes via BackupResumePlanner.
                if resource.fileSize >= smallLimit {
                    anyPresent = true
                    break
                }
                for match in sizeMatches {
                    let path = RemotePathBuilder.absolutePath(
                        basePath: basePath,
                        remoteRelativePath: monthRel + "/" + match.name
                    )
                    do {
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

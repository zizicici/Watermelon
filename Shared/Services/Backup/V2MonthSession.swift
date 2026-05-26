import Foundation
import os.log

private let v2SessionLog = Logger(subsystem: "com.zizicici.watermelon", category: "V2MonthSession")

final class V2MonthSession: BackupMonthStore {
    enum FlushError: Error {
        case concurrentFlushRejected
        /// Commit landed; caller must mark these fingerprints committed before rethrowing.
        case snapshotWriteFailed(committedAssets: Set<Data>, committedTombstones: Set<Data>, underlying: Error)

        /// SnapshotWriter can wrap CancellationError.
        var cancellationCause: CancellationError? {
            let matched = BackupErrorChain.contains(self) { node in
                if node is CancellationError { return true }
                let nsError = node as NSError
                return nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled
            }
            return matched ? CancellationError() : nil
        }
    }

    let year: Int
    let month: Int
    let v2Services: BackupV2RuntimeServices?
    private let basePath: String
    private let client: any RemoteStorageClientProtocol
    private let stepLogger: MonthManifestStepLogger?
    private let indexes: V2MonthIndexes
    private let snapshotFlusher: V2MonthSnapshotFlusher

    var monthRelativePath: String {
        String(format: "%04d/%02d", year, month)
    }

    var monthAbsolutePath: String {
        RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
    }

    private let materializedCovered: CoveredRanges

    /// Tombstone basis must include local ticks since session load.
    private let observedClockAtLoad: UInt64

    private(set) var dirty: Bool = false

    private let isPresenceAuthoritative: Bool
    private let flushStateLock = NSLock()
    private var isFlushing = false

    var hasAnyAsset: Bool { indexes.hasAnyAsset }

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        v2Services: BackupV2RuntimeServices,
        materializedState: RepoMonthState,
        materializedCovered: CoveredRanges,
        observedClockAtLoad: UInt64,
        remoteFilesByName: [String: MonthManifestStore.RemoteFileMetadata],
        presence: RemotePresenceSnapshot.Month = .absent,
        stepLogger: MonthManifestStepLogger? = nil
    ) {
        self.client = client
        self.basePath = basePath
        self.year = year
        self.month = month
        self.v2Services = v2Services
        self.materializedCovered = materializedCovered
        self.stepLogger = stepLogger
        self.observedClockAtLoad = observedClockAtLoad
        // Gate A: non-empty fail-closed input forwarding (authority-independent — matches today's
        // V2MonthLoadAndPublish line-for-line so non-authoritative missing evidence still drives
        // V2MonthIndexes.presenceMap to .missing). Gate B (authority bit) stored separately for publish.
        let verifiedMissingHashes: Set<Data>? = presence.missingHashes.isEmpty ? nil : presence.missingHashes
        self.isPresenceAuthoritative = presence.isAuthoritative
        let indexes = V2MonthIndexes(
            year: year,
            month: month,
            materializedState: materializedState,
            remoteFilesByName: remoteFilesByName,
            verifiedMissingHashes: verifiedMissingHashes,
            nameCase: client.backendNameCaseSensitivity
        )
        self.indexes = indexes
        self.snapshotFlusher = V2MonthSnapshotFlusher(
            services: v2Services,
            monthKey: LibraryMonthKey(year: year, month: month),
            materializedCovered: materializedCovered,
            indexes: indexes
        )
    }

    static func loadOrCreate(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        v2Services: BackupV2RuntimeServices,
        presence: RemotePresenceSnapshot.Month = .absent,
        stepLogger: MonthManifestStepLogger? = nil
    ) async throws -> V2MonthSession {
        let monthKey = LibraryMonthKey(year: year, month: month)
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materializeMonth(monthKey, expectedRepoID: v2Services.repoID)
        let monthState = output.state.months[monthKey] ?? .empty
        let materializedCovered = output.coveredByMonth[monthKey] ?? .empty
        // Observe-before-send: subsequent tickRange must happen above any peer clock we just read.
        try await v2Services.lamport.observe(output.state.observedClock)

        // WebDAV/SMB/SFTP don't auto-create parents; ensure dir exists for the list below.
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        do {
            try await client.createDirectory(path: monthAbsolutePath)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.createMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbsolutePath)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.listMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }
        let remoteFilesByName = MonthManifestStore.dedupedRemoteFilesByName(
            entries: entries, year: year, month: month
        )

        let session = V2MonthSession(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            v2Services: v2Services,
            materializedState: monthState,
            materializedCovered: materializedCovered,
            observedClockAtLoad: output.state.observedClock,
            remoteFilesByName: remoteFilesByName,
            presence: presence,
            stepLogger: stepLogger
        )
        if output.corruptedSnapshotMonths.contains(monthKey),
           materializedCovered.rangesByWriter.values.contains(where: { !$0.isEmpty }) {
            session.requestSnapshotRebaseline()
        }
        return session
    }

    func requestSnapshotRebaseline() {
        snapshotFlusher.requestRebaseline()
        dirty = true
    }


    func containsAssetFingerprint(_ fingerprint: Data) -> Bool {
        indexes.containsAssetFingerprint(fingerprint)
    }

    func containsDurableAssetFingerprint(_ fingerprint: Data) -> Bool {
        // Pending adds are in-memory only until the batch commit covering them lands;
        // cache-reuse short-circuits that trust durability must reject those.
        indexes.containsAssetFingerprint(fingerprint)
            && !indexes.pendingV2AssetFingerprints.contains(fingerprint)
    }

    var hasUncommittedV2Ops: Bool {
        indexes.hasUncommittedOps
    }

    func findStrictSubsetAssetFingerprints(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> [Data] {
        indexes.findStrictSubsetAssetFingerprints(forResourceKeys: keys)
    }

    func hasStrictSubsetAssetFingerprint(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> Bool {
        indexes.hasStrictSubsetAssetFingerprint(forResourceKeys: keys)
    }

    func isAssetIncomplete(_ fingerprint: Data) -> Bool {
        indexes.isAssetIncomplete(fingerprint)
    }

    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource? {
        indexes.findResourceByHash(contentHash)
    }

    func findByFileName(_ logicalName: String) -> RemoteManifestResource? {
        indexes.findByFileName(logicalName)
    }

    func existingFileNames() -> Set<String> {
        indexes.existingFileNames()
    }

    func existingCollisionKeys() -> Set<String> {
        indexes.existingCollisionKeys()
    }

    func remoteFileSize(named logicalName: String) -> Int64? {
        indexes.remoteFileSize(named: logicalName)
    }

    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        indexes.unsortedSnapshot()
    }

    var presence: RemotePresenceSnapshot.Month {
        RemotePresenceSnapshot.Month(
            missingHashes: indexes.physicallyMissingHashesSnapshot(),
            isAuthoritative: isPresenceAuthoritative
        )
    }


    @discardableResult
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource {
        let result = try indexes.upsertResource(resource)
        dirty = true
        return result
    }

    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<Data>
    ) throws {
        try indexes.upsertAsset(asset, links: links, replacingSubsetFingerprints: replacingSubsetFingerprints)
        dirty = true
    }

    func markRemoteFile(name: String, size: Int64) {
        indexes.markRemoteFile(name: name, size: size)
    }


    @discardableResult
    func commitPendingAssetToRemote(ignoreCancellation: Bool) async throws -> BackupMonthFlushDelta {
        guard beginFlush() else {
            throw FlushError.concurrentFlushRejected
        }
        defer { endFlush() }
        let drain = try await commitPendingAssetDrainLocked(force: true, ignoreCancellation: ignoreCancellation)
        return BackupMonthFlushDelta(
            didFlush: drain.lastSeq != nil,
            committedAssetFingerprints: drain.committedAssets,
            committedTombstoneFingerprints: drain.committedTombstones
        )
    }

    @discardableResult
    func flushToRemote(ignoreCancellation: Bool = false) async throws -> BackupMonthFlushDelta {
        guard beginFlush() else {
            throw FlushError.concurrentFlushRejected
        }
        defer { endFlush() }
        guard dirty, let services = v2Services else {
            return .none
        }
        if !ignoreCancellation { try Task.checkCancellation() }

        let drainResult: (lastSeq: UInt64?, committedAssets: Set<Data>, committedTombstones: Set<Data>)
        do {
            drainResult = try await commitPendingAssetDrainLocked(force: true, ignoreCancellation: ignoreCancellation)
        } catch {
            throw error
        }

        let wroteSnapshot: Bool
        do {
            wroteSnapshot = try await snapshotFlusher.flushSnapshotIfPending(ignoreCancellation: ignoreCancellation)
            dirty = indexes.hasUncommittedOps || snapshotFlusher.hasPendingSnapshotWork
        } catch {
            dirty = true
            throw FlushError.snapshotWriteFailed(
                committedAssets: drainResult.committedAssets,
                committedTombstones: drainResult.committedTombstones,
                underlying: error
            )
        }

        let didFlush = drainResult.lastSeq != nil || wroteSnapshot
        if !ignoreCancellation,
           didFlush,
           !dirty {
            do {
                try await runCheckpointBarrierHook(services: services)
            } catch {
                // U01 R04: the checkpoint-barrier hook propagates `CancellationError` (and any
                // other unexpected throws) AFTER the commit + snapshot are already durable.
                // Surface the durable delta through the same channel used for snapshot-write
                // failures so `flushMonthStorePublishingDefensiveCommits` returns
                // `.commitDurableSnapshotDeferred` and the executor still runs
                // `applyDurableBatchSideEffects` (intent drain + provisional mark-durable)
                // before the cancellation routes to pause/abort. Without this, a cancellation
                // landing during the post-commit barrier window would orphan the local
                // hash-index intents for a durable remote commit.
                throw FlushError.snapshotWriteFailed(
                    committedAssets: drainResult.committedAssets,
                    committedTombstones: drainResult.committedTombstones,
                    underlying: error
                )
            }
        }

        return BackupMonthFlushDelta(
            didFlush: didFlush,
            committedAssetFingerprints: drainResult.committedAssets,
            committedTombstoneFingerprints: drainResult.committedTombstones
        )
    }

    private func runCheckpointBarrierHook(services: BackupV2RuntimeServices) async throws {
        do {
            _ = try await RepoCheckpointBarrierHook(
                services: services,
                month: LibraryMonthKey(year: year, month: month)
            ).run()
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                throw CancellationError()
            }
            let message = "V2 checkpoint barrier maintenance failed for \(monthRelativePath): \(String(describing: error))"
            stepLogger?(message)
            v2SessionLog.notice("\(message, privacy: .public)")
        }
    }

    /// Drain pending V2 ops in commit-file chunks bounded by `BackupV2Constants.batchFlushInterval`.
    /// `force == true` drains until pending is empty; `force == false` only commits when pending
    /// already meets the threshold and stops once it drops below. Per-chunk `recordCommitted(seq:)`
    /// makes every committed seq reach `snapshotFlusher.sessionWrittenCovered` before the trailing
    /// snapshot is built (U01 hard-cap + multi-commit covered-range correctness).
    private func commitPendingAssetDrainLocked(
        force: Bool,
        ignoreCancellation: Bool
    ) async throws -> (lastSeq: UInt64?, committedAssets: Set<Data>, committedTombstones: Set<Data>) {
        guard let services = v2Services else {
            return (nil, [], [])
        }
        let threshold = BackupV2Constants.batchFlushInterval
        var lastSeq: UInt64?
        var committedAssets: Set<Data> = []
        var committedTombstones: Set<Data> = []
        while indexes.hasUncommittedOps {
            if !force, indexes.pendingOpsCount < threshold {
                break
            }
            let chunk: V2MonthCommitFlusher.Result?
            do {
                chunk = try await commitPendingAssetToRemoteLockedResult(
                    services: services,
                    limit: threshold,
                    ignoreCancellation: ignoreCancellation
                )
            } catch {
                // Multi-chunk partial durability: if earlier chunks already landed, surface them
                // via FlushError.snapshotWriteFailed so the downstream catches the partial as
                // `commitDurableSnapshotDeferred`, drains the matching hash-index intents, and
                // publishes the durable sweep before propagating the failure. Without this,
                // already-durable fingerprints would be rolled back in the foreground catch path
                // and their hash-index intents discarded — violating the "at most last batch is
                // redone" goal for U01.
                if !committedAssets.isEmpty || !committedTombstones.isEmpty {
                    throw FlushError.snapshotWriteFailed(
                        committedAssets: committedAssets,
                        committedTombstones: committedTombstones,
                        underlying: error
                    )
                }
                throw error
            }
            guard let chunk else { break }
            committedAssets.formUnion(chunk.committedAssets)
            committedTombstones.formUnion(chunk.committedTombstones)
            lastSeq = chunk.lastSeq
        }
        return (lastSeq, committedAssets, committedTombstones)
    }

    private func commitPendingAssetToRemoteLockedResult(
        services: BackupV2RuntimeServices,
        limit: Int? = nil,
        ignoreCancellation: Bool
    ) async throws -> V2MonthCommitFlusher.Result? {
        if !ignoreCancellation { try Task.checkCancellation() }
        let monthKey = LibraryMonthKey(year: year, month: month)
        let barrierAwareBasis: V2MonthCommitFlusher.Basis?
        if indexes.hasUncommittedOps {
            let localLamportBeforeBarrierObserve = await services.lamport.value()
            let tombstoneBasis = makeTombstoneObservationBasis(
                sessionWrittenCovered: snapshotFlusher.sessionWrittenCovered,
                localLamportBeforeBarrierObserve: localLamportBeforeBarrierObserve
            )
            if let refresh = try await V2RetentionBarrierRefresh(
                services: services,
                monthKey: monthKey
            ).commitRefresh(ignoreCancellation: ignoreCancellation) {
                barrierAwareBasis = V2MonthCommitFlusher.Basis(
                    clockFloor: refresh.clockFloor,
                    tombstoneObservationBasis: tombstoneBasis
                )
            } else {
                barrierAwareBasis = nil
            }
        } else {
            barrierAwareBasis = nil
        }
        let commitFlusher = V2MonthCommitFlusher(
            services: services,
            monthKey: monthKey,
            materializedCovered: materializedCovered,
            observedClockAtLoad: observedClockAtLoad,
            indexes: indexes
        )
        guard let result = try await commitFlusher.flushPending(
            sessionWrittenCovered: snapshotFlusher.sessionWrittenCovered,
            barrierAwareBasis: barrierAwareBasis,
            limit: limit,
            ignoreCancellation: ignoreCancellation
        ) else {
            return nil
        }
        snapshotFlusher.recordCommitted(seq: result.lastSeq)
        dirty = true
        return result
    }

    private func makeTombstoneObservationBasis(
        sessionWrittenCovered: CoveredRanges,
        localLamportBeforeBarrierObserve: UInt64
    ) -> TombstoneObservationBasis {
        let priorCovered = materializedCovered.merging(sessionWrittenCovered)
        var perWriterMaxSeq: [String: UInt64] = [:]
        for (writer, ranges) in priorCovered.rangesByWriter {
            perWriterMaxSeq[writer] = ranges.map(\.high).max() ?? 0
        }
        return TombstoneObservationBasis(
            perWriterMaxSeq: perWriterMaxSeq,
            lamportWatermark: max(observedClockAtLoad, localLamportBeforeBarrierObserve)
        )
    }

    private func beginFlush() -> Bool {
        flushStateLock.lock()
        defer { flushStateLock.unlock() }
        if isFlushing { return false }
        isFlushing = true
        return true
    }

    private func endFlush() {
        flushStateLock.withLock {
            isFlushing = false
        }
    }
}

extension ServerProfileRecord {
    func isConnectionUnavailableErrorIncludingFlushUnderlying(_ error: Error) -> Bool {
        BackupErrorChain.contains(error) { isConnectionUnavailableError($0) }
    }
}

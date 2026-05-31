import Foundation
import os.log

private let v2SessionLog = Logger(subsystem: "com.zizicici.watermelon", category: "V2MonthSession")

enum V2MonthSessionError: Error {
    case ambiguousMonth(LibraryMonthKey)
    case corruptMonth(LibraryMonthKey)
}

final class V2MonthSession: BackupMonthStore {
    enum FlushError: Error {
        case concurrentFlushRejected
        /// Post-commit side-effects failed after the commit landed durably. The committed delta is
        /// carried as a value (see `MonthDurableCommitPartial`), never on this error.
        case postCommitFailed(underlying: Error)

        /// SnapshotWriter/commit errors can wrap CancellationError.
        var cancellationCause: CancellationError? {
            let matched = BackupErrorChain.contains(self) { node in
                if node is CancellationError { return true }
                let nsError = node as NSError
                return nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled
            }
            return matched ? CancellationError() : nil
        }
    }

    /// Commit landed durably but a subsequent operation failed. Carries the durable delta as a value
    /// so callers keep their projection in sync without reading data off `FlushError`.
    struct MonthDurableCommitPartial: Error {
        let delta: BackupMonthFlushDelta
        let flushError: FlushError
    }

    let year: Int
    let month: Int
    let v2Services: BackupV2RuntimeServices?
    private let basePath: String
    private let client: any RemoteStorageClientProtocol
    private let stepLogger: MonthManifestStepLogger?
    private let indexes: V2MonthIndexes
    private let commitTracker: V2SessionCommitTracker

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
        self.commitTracker = V2SessionCommitTracker(writerID: v2Services.writerID)
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
        // Write/maintenance consumers must not construct a writable session for non-clean months.
        // Ambiguous months have incomparable trusted coverage; corrupt months have no trusted baseline.
        // Hot-path snapshot rebaseline was removed in Phase 5 — repair belongs to compaction.
        let outcome = output.outcomeByMonth[monthKey] ?? .clean
        if outcome == .ambiguous {
            throw V2MonthSessionError.ambiguousMonth(monthKey)
        }
        if outcome == .corrupt {
            throw V2MonthSessionError.corruptMonth(monthKey)
        }
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
        var remoteFilesByName = MonthManifestStore.dedupedRemoteFilesByName(
            entries: entries, year: year, month: month
        )
        // The month LIST can omit a materialized resource two ways: a grace backend's stale listing
        // hides a peer's just-written file, or an exact-match normalizing server (HFS+/SFTP) lists the
        // recorded NFC leaf back as NFD so the byte-exact presence key misses. Either way the resource
        // would be marked physically missing and drive a duplicate repair upload/commit. Probe the
        // recorded path directly and treat a content-confirmed hit as listed so the existing-hash fast
        // path still dedups. Unconfirmed omissions stay missing — the conservative duplicate, never a
        // wrong-bytes bind. Case-insensitive zero-grace backends fold NFC and lag-free, so they skip this.
        if client.readAfterWriteGraceSeconds > 0
            || client.backendNameCaseSensitivity.usesExactNameMatchingForPresence {
            remoteFilesByName = try await reconcileListOmittedResources(
                client: client,
                basePath: basePath,
                monthState: monthState,
                remoteFilesByName: remoteFilesByName
            )
        }

        return V2MonthSession(
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
    }

    private static let listReconcileMaxVerifiedFiles = 64
    private static let listReconcileMaxVerifiedBytes: Int64 = 32 * 1024 * 1024

    /// Confirm materialized resources the month LIST omitted by probing their recorded paths directly,
    /// so a stale grace-backend listing or an NFC/NFD normalization divergence can't disable the dedup
    /// fast path. Only content-confirmed hits are promoted to "listed"; not-found/size races and
    /// transport failures stay omitted (→ missing).
    private static func reconcileListOmittedResources(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        monthState: RepoMonthState,
        remoteFilesByName: [String: MonthManifestStore.RemoteFileMetadata]
    ) async throws -> [String: MonthManifestStore.RemoteFileMetadata] {
        let nameCase = client.backendNameCaseSensitivity
        var sizesByPresenceKey: [String: Set<Int64>] = [:]
        for (name, meta) in remoteFilesByName {
            sizesByPresenceKey[nameCase.presenceKey(for: name), default: []].insert(meta.size)
        }
        var result = remoteFilesByName
        var verifiedFileCount = 0
        var verifiedByteCount: Int64 = 0
        for row in monthState.resources.values {
            let leaf = (row.physicalRemotePath as NSString).lastPathComponent
            let key = nameCase.presenceKey(for: leaf)
            if sizesByPresenceKey[key]?.contains(row.fileSize) == true { continue }
            if verifiedFileCount >= listReconcileMaxVerifiedFiles { break }
            if verifiedByteCount + max(row.fileSize, 0) > listReconcileMaxVerifiedBytes { break }
            let path = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: row.physicalRemotePath)
            let outcome: RemoteContentTrust.HashVerificationResult
            do {
                outcome = try await RemoteContentTrust.verifyHashResult(
                    client: client,
                    remotePath: path,
                    expectedSize: row.fileSize,
                    expectedHash: row.contentHash
                )
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                continue
            }
            verifiedFileCount += 1
            verifiedByteCount += max(row.fileSize, 0)
            if case .matched = outcome {
                // Re-key under the recorded leaf: a normalizing backend listed an NFD spelling whose
                // String key canonically equals our NFC leaf, so a plain insert keeps the NFD key and
                // the byte-exact presence key still misses. Drop the divergent spelling, then insert.
                result.removeValue(forKey: leaf)
                result[leaf] = MonthManifestStore.RemoteFileMetadata(size: row.fileSize)
                sizesByPresenceKey[key, default: []].insert(row.fileSize)
            }
        }
        return result
    }


    func containsAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool {
        indexes.containsAssetFingerprint(fingerprint)
    }

    func containsDurableAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool {
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
    ) -> [AssetFingerprint] {
        indexes.findStrictSubsetAssetFingerprints(forResourceKeys: keys)
    }

    func hasStrictSubsetAssetFingerprint(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> Bool {
        indexes.hasStrictSubsetAssetFingerprint(forResourceKeys: keys)
    }

    func isAssetIncomplete(_ fingerprint: AssetFingerprint) -> Bool {
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
        replacingSubsetFingerprints: Set<AssetFingerprint>
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

        let drainResult: (lastSeq: UInt64?, committedAssets: Set<AssetFingerprint>, committedTombstones: Set<AssetFingerprint>)
        do {
            drainResult = try await commitPendingAssetDrainLocked(force: true, ignoreCancellation: ignoreCancellation)
        } catch {
            throw error
        }

        dirty = indexes.hasUncommittedOps

        return BackupMonthFlushDelta(
            didFlush: drainResult.lastSeq != nil,
            committedAssetFingerprints: drainResult.committedAssets,
            committedTombstoneFingerprints: drainResult.committedTombstones
        )
    }

    /// Drain pending V2 ops in commit-file chunks bounded by `BackupV2Constants.batchFlushInterval`.
    /// `force == true` drains until pending is empty; `force == false` only commits when pending
    /// already meets the threshold and stops once it drops below. Per-chunk `recordCommitted(seq:)`
    /// makes every committed seq reach `commitTracker.sessionWrittenCovered` before the next chunk
    /// builds its tombstone basis (multi-commit covered-range correctness).
    private func commitPendingAssetDrainLocked(
        force: Bool,
        ignoreCancellation: Bool
    ) async throws -> (lastSeq: UInt64?, committedAssets: Set<AssetFingerprint>, committedTombstones: Set<AssetFingerprint>) {
        guard let services = v2Services else {
            return (nil, [], [])
        }
        let threshold = BackupV2Constants.batchFlushInterval
        var lastSeq: UInt64?
        var committedAssets: Set<AssetFingerprint> = []
        var committedTombstones: Set<AssetFingerprint> = []
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
                // as a value via MonthDurableCommitPartial so the downstream catches the partial
                // as `commitDurablePartial`, drains the matching hash-index intents, and
                // publishes the durable sweep before propagating the failure. Without this,
                // already-durable fingerprints would be rolled back in the foreground catch path
                // and their hash-index intents discarded — violating the "at most last batch is
                // redone" goal.
                if !committedAssets.isEmpty || !committedTombstones.isEmpty {
                    throw MonthDurableCommitPartial(
                        delta: BackupMonthFlushDelta(
                            didFlush: true,
                            committedAssetFingerprints: committedAssets,
                            committedTombstoneFingerprints: committedTombstones
                        ),
                        flushError: FlushError.postCommitFailed(underlying: error)
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
            let localLamport = await services.lamport.value()
            let tombstoneBasis = makeTombstoneObservationBasis(
                sessionWrittenCovered: commitTracker.sessionWrittenCovered,
                localLamportBeforeBarrierObserve: localLamport
            )
            barrierAwareBasis = V2MonthCommitFlusher.Basis(
                clockFloor: max(observedClockAtLoad, localLamport),
                tombstoneObservationBasis: tombstoneBasis
            )
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
            sessionWrittenCovered: commitTracker.sessionWrittenCovered,
            barrierAwareBasis: barrierAwareBasis,
            limit: limit,
            ignoreCancellation: ignoreCancellation
        ) else {
            return nil
        }
        commitTracker.recordCommitted(seq: result.lastSeq)
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

/// Tracks session-local covered ranges from committed writes, replacing the snapshot-flusher-based
/// tracker so the hot path no longer depends on snapshot state.
final class V2SessionCommitTracker {
    private let writerID: String
    private(set) var sessionWrittenCovered: CoveredRanges = .empty

    init(writerID: String) {
        self.writerID = writerID
    }

    func recordCommitted(seq: UInt64) {
        sessionWrittenCovered.add(writerID: writerID, seq: seq)
    }
}

extension ServerProfileRecord {
    func isConnectionUnavailableErrorIncludingFlushUnderlying(_ error: Error) -> Bool {
        BackupErrorChain.contains(error) { isConnectionUnavailableError($0) }
    }
}

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
            switch self {
            case .concurrentFlushRejected:
                return nil
            case .snapshotWriteFailed(_, _, let underlying):
                return Self.cancellationCause(in: underlying)
            }
        }

        private static func cancellationCause(in error: Error) -> CancellationError? {
            var pending: [Error] = [error]
            var seen: Set<String> = []
            while let next = pending.popLast() {
                if let cancel = next as? CancellationError { return cancel }
                let nsError = next as NSError
                let key = "\(nsError.domain)#\(nsError.code)#\(nsError.localizedDescription)"
                guard seen.insert(key).inserted else { continue }
                switch next {
                case let flush as V2MonthSession.FlushError:
                    switch flush {
                    case .concurrentFlushRejected:
                        break
                    case .snapshotWriteFailed(_, _, let underlying):
                        pending.append(underlying)
                    }
                case let write as SnapshotWriter.WriteError:
                    switch write {
                    case .ioFailure(let inner), .finalizationFailed(let inner):
                        pending.append(inner)
                    case .verificationFailed:
                        break
                    }
                case let storage as RemoteStorageClientError:
                    switch storage {
                    case .underlying(let inner):
                        pending.append(inner)
                    default:
                        break
                    }
                default:
                    if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
                        pending.append(underlying)
                    }
                }
            }
            return nil
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

    let physicallyMissingHashesAreAuthoritative: Bool
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
        verifiedMissingHashes: Set<Data>? = nil,
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
        self.physicallyMissingHashesAreAuthoritative = verifiedMissingHashes != nil
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
        verifiedMissingHashes: Set<Data>? = nil,
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
            verifiedMissingHashes: verifiedMissingHashes,
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

    func physicallyMissingHashesSnapshot() -> Set<Data> {
        indexes.physicallyMissingHashesSnapshot()
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
    func commitPendingAssetToRemote(ignoreCancellation: Bool) async throws -> MonthManifestStore.FlushDelta {
        guard beginFlush() else {
            throw FlushError.concurrentFlushRejected
        }
        defer { endFlush() }
        return try await commitPendingAssetToRemoteLocked(ignoreCancellation: ignoreCancellation)
    }

    @discardableResult
    func flushToRemote(ignoreCancellation: Bool = false) async throws -> MonthManifestStore.FlushDelta {
        guard beginFlush() else {
            throw FlushError.concurrentFlushRejected
        }
        defer { endFlush() }
        guard dirty, let services = v2Services else {
            return .none
        }
        if !ignoreCancellation { try Task.checkCancellation() }

        let commitResult: V2MonthCommitFlusher.Result?
        do {
            commitResult = try await commitPendingAssetToRemoteLockedResult(
                services: services,
                ignoreCancellation: ignoreCancellation
            )
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
                committedAssets: commitResult?.committedAssets ?? [],
                committedTombstones: commitResult?.committedTombstones ?? [],
                underlying: error
            )
        }

        return MonthManifestStore.FlushDelta(
            didFlush: commitResult != nil || wroteSnapshot,
            committedV2AssetFingerprints: commitResult?.committedAssets ?? [],
            committedV2TombstoneFingerprints: commitResult?.committedTombstones ?? []
        )
    }

    private func commitPendingAssetToRemoteLocked(ignoreCancellation: Bool) async throws -> MonthManifestStore.FlushDelta {
        guard let services = v2Services else { return .none }
        guard let result = try await commitPendingAssetToRemoteLockedResult(
            services: services,
            ignoreCancellation: ignoreCancellation
        ) else {
            return .none
        }
        return MonthManifestStore.FlushDelta(
            didFlush: true,
            committedV2AssetFingerprints: result.committedAssets,
            committedV2TombstoneFingerprints: result.committedTombstones
        )
    }

    private func commitPendingAssetToRemoteLockedResult(
        services: BackupV2RuntimeServices,
        ignoreCancellation: Bool
    ) async throws -> V2MonthCommitFlusher.Result? {
        if !ignoreCancellation { try Task.checkCancellation() }
        let monthKey = LibraryMonthKey(year: year, month: month)
        let commitFlusher = V2MonthCommitFlusher(
            services: services,
            monthKey: monthKey,
            materializedCovered: materializedCovered,
            observedClockAtLoad: observedClockAtLoad,
            indexes: indexes
        )
        guard let result = try await commitFlusher.flushPending(
            sessionWrittenCovered: snapshotFlusher.sessionWrittenCovered,
            ignoreCancellation: ignoreCancellation
        ) else {
            return nil
        }
        snapshotFlusher.recordCommitted(seq: result.lastSeq)
        dirty = true
        return result
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
        var pending: [Error] = [error]
        var seen: Set<String> = []
        while let next = pending.popLast() {
            let nsError = next as NSError
            let key = "\(nsError.domain)#\(nsError.code)#\(nsError.localizedDescription)"
            guard seen.insert(key).inserted else { continue }
            if isConnectionUnavailableError(next) { return true }
            switch next {
            case let flush as V2MonthSession.FlushError:
                switch flush {
                case .concurrentFlushRejected:
                    break
                case .snapshotWriteFailed(_, _, let underlying):
                    pending.append(underlying)
                }
            case let write as SnapshotWriter.WriteError:
                switch write {
                case .ioFailure(let underlying), .finalizationFailed(let underlying):
                    pending.append(underlying)
                case .verificationFailed:
                    break
                }
            case let storage as RemoteStorageClientError:
                switch storage {
                case .underlying(let underlying):
                    pending.append(underlying)
                default:
                    break
                }
            default:
                if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
                    pending.append(underlying)
                }
            }
        }
        return false
    }
}

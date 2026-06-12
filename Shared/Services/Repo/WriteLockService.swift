import Foundation

// Single-writer lock unit for the Lite repo write path.
//
// Model: one file at `.watermelon/locks/<writerID>.lock` whose body (see `LockFileBody`) carries the
// writer, a per-session token, a per-acquisition lock token, a write generation, and a body timestamp.
// Freshness still comes from the backend LIST/metadata modification date — now with skew tolerance — but every
// destructive action (stale takeover, reclaim, release) second-confirms the candidate body before
// acting, so a just-refreshed foreign lock or a same-writer successor is never deleted. A missing mtime
// can fall back to the body timestamp when present; missing evidence still fails closed.
actor WriteLockService {
    static let expiry: TimeInterval = 5 * 60
    static let refreshInterval: TimeInterval = 2 * 60
    // A lease is only trusted briefly past the normal refresh cadence; expiry/skew still controls takeover.
    static let confidenceMaxAge: TimeInterval = 2.5 * 60
    // Tolerance for backend/device clock disagreement when judging another writer's mtime. Widening the
    // "fresh" band makes us slower to declare a foreign lock stale (and so slower to delete it).
    static let clockSkewTolerance: TimeInterval = 60

    enum Mode: Sendable {
        case foreground
        case background
    }

    enum Acquisition: Equatable, Sendable {
        case acquired
        case blocked                              // foreground fail-closed: an unsafe lock or post-write conflict
        case blockedByOwnLock                     // foreground: same writer's previous session is still live
        case skipped                              // background declined rather than risk a takeover
        case skippedByOwnLock                     // background: same writer's previous session is still live
        case faulted(RemoteFaultLite.Category)    // LIST / create / upload / delete transport fault
    }

    enum Refresh: Equatable, Sendable {
        case refreshed                            // own lock rewritten; confidence restored
        case degraded(RemoteFaultLite.Category)   // write failed/skipped; confidence dropped, ownership retained
    }

    enum Assertion: Equatable, Sendable {
        case stillOwned                           // safe to continue (confidence refreshed)
        case lost(LossReason)                     // stop: ownership can no longer be trusted
        case faulted(RemoteFaultLite.Category)    // transient LIST fault; lease retained, confidence dropped
    }

    enum LossReason: Equatable, Sendable {
        case otherWriter                          // a fresh or unknown-mtime other lock is present
        case ownLockDeleted                       // our lock is gone
    }

    private var client: any RemoteStorageClientProtocol
    private let writerID: String
    private let locksDirectoryPath: String
    private let ownLockPath: String
    private let ownLockFilename: String
    // Stable for this service's lifetime: distinguishes our session from any other (including a later
    // same-writer session) so release/cleanup never delete a successor's lock.
    private let sessionToken: String
    // Best-effort diagnostic hook: fired when a scan observes another writer's lock. Must never throw
    // and never change the caller's outcome.
    private let onForeignWriterObserved: (@Sendable () async -> Void)?

    // Regenerated on each acquire (a fresh acquisition identity); bumped-generation body written by every
    // own-lock write.
    private var lockToken: String
    private var generation = 0

    private var holdsLeaseValue = false
    private var confident = false
    private var lastSuccessfulRefresh: Date?

    init?(
        basePath: String,
        writerID: String,
        client: any RemoteStorageClientProtocol,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) {
        guard let lockPath = RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID),
              let filename = RepoLayoutLite.lockFilename(writerID: writerID) else {
            return nil
        }
        self.client = client
        self.writerID = writerID
        self.locksDirectoryPath = RepoLayoutLite.locksDirectoryPath(basePath: basePath)
        self.ownLockPath = lockPath
        self.ownLockFilename = filename
        self.sessionToken = UUID().uuidString
        self.lockToken = UUID().uuidString
        self.onForeignWriterObserved = onForeignWriterObserved
    }

    var holdsLease: Bool { holdsLeaseValue }

    func replaceClient(_ newClient: any RemoteStorageClientProtocol) {
        client = newClient
    }

    // MARK: - Acquire

    func acquire(mode: Mode, now: Date = Date()) async -> Acquisition {
        let operationClient = client
        let entries: [RemoteStorageEntry]
        do {
            entries = try await listLocks(client: operationClient, createIfMissing: true)
        } catch {
            return .faulted(RemoteFaultLite.classify(error))
        }

        let scan = scanLocks(entries, now: now)
        await reportForeignWriter(scan)
        // An unsafe other lock has top priority; our own lock must not hide it.
        if scan.hasUnsafeOther {
            return blockedOrSkipped(mode)
        }

        // No unsafe other lock: stale locks are foreground takeover candidates; missing-mtime locks need
        // body evidence, so background declines them rather than proving a takeover.
        if mode == .background {
            if !scan.missingMtimeOthers.isEmpty { return .skipped }
            if !scan.ownPresent, !scan.staleOthers.isEmpty {
                return .skipped   // background never takes over a stranger's stale lock
            }
        }

        if mode == .foreground {
            for stale in scan.foreignTakeoverCandidates {
                switch await confirmForeignStaleDeletable(
                    client: operationClient,
                    path: stale.path,
                    snapshotDate: stale.modificationDate,
                    now: now
                ) {
                case .deletable(let proof):
                    guard await confirmForeignStaleStillMatches(
                        client: operationClient,
                        path: stale.path,
                        proof: proof,
                        now: now
                    ) else {
                        return .blocked
                    }
                    do {
                        try await operationClient.delete(path: stale.path)
                    } catch {
                        let category = RemoteFaultLite.classify(error)
                        if category != .notFound {
                            return .faulted(category)
                        }
                    }
                case .live:
                    // A contender refreshed/changed since the snapshot: fail closed, take nothing over.
                    return .blocked
                case .gone:
                    continue
                case .fault(let category):
                    return .faulted(category)
                }
            }
        }

        if scan.ownPresent {
            switch await confirmOwnLockReclaimable(client: operationClient, scan: scan, now: now) {
            case .reclaimable, .gone:
                break
            case .live:
                return ownLockBlockedOrSkipped(mode)
            case .fault(let category):
                return .faulted(category)
            }
        }

        lockToken = UUID().uuidString   // fresh acquisition identity
        do {
            try await writeOwnLock(client: operationClient, now: now)
        } catch {
            // A timeout may have landed the PUT after throwing — remove our own orphaned lock.
            await deleteOwnLockBestEffort(client: operationClient)
            return .faulted(RemoteFaultLite.classify(error))
        }

        // Re-LIST after writing: if a fresh/unknown other lock now appears, neither side wins.
        let confirmation: [RemoteStorageEntry]
        do {
            confirmation = try await listLocks(client: operationClient, createIfMissing: false)
        } catch {
            await deleteOwnLockBestEffort(client: operationClient)
            return .faulted(RemoteFaultLite.classify(error))
        }
        let confirmationScan = scanLocks(confirmation, now: now)
        await reportForeignWriter(confirmationScan)
        if confirmationScan.hasBlockingOther {
            await deleteOwnLockBestEffort(client: operationClient)
            return blockedOrSkipped(mode)
        }
        switch await proveOwnLock(client: operationClient) {
        case .owned:
            break
        case .lost:
            await deleteOwnLockBestEffort(client: operationClient)
            return blockedOrSkipped(mode)
        case .unproven(let category):
            await deleteOwnLockBestEffort(client: operationClient)
            return .faulted(category)
        }

        holdsLeaseValue = true
        confident = true
        lastSuccessfulRefresh = now
        return .acquired
    }

    // MARK: - Release

    // Drops the lease and deletes our own lock. The delete is guarded by the lock body: if the remote
    // lock now belongs to a same-writer successor session (different session/token), it is left intact.
    func release() async {
        let operationClient = client
        holdsLeaseValue = false
        confident = false
        lastSuccessfulRefresh = nil
        await deleteOwnLockBestEffort(client: operationClient)
    }

    // MARK: - Refresh

    // Overwrites the own lock. A transient write failure only degrades confidence; it does not abort
    // ownership, because the lock may still be present and fresh on the backend. A successful write
    // within the confidence window re-proves ownership (no other writer could have legitimately
    // reclaimed a not-yet-expired lock), so it restores confidence after a prior transient loss.
    func refresh(now: Date = Date()) async -> Refresh {
        let operationClient = client
        guard holdsLeaseValue else {
            return .degraded(.retryable)
        }
        // Skip upload if the gap since the last confirmed write exceeds the confidence window. Another
        // writer may have reclaimed the expired lock; uploading would recreate our stale lock and evict
        // the new owner. A backward clock is equally untrustworthy.
        if let previous = lastSuccessfulRefresh {
            let elapsed = now.timeIntervalSince(previous)
            guard elapsed >= 0 else {
                confident = false
                return .degraded(.retryable)
            }
            if elapsed > Self.confidenceMaxAge || !confident {
                confident = false
                switch await assertStillOwned(now: now, client: operationClient) {
                case .stillOwned:
                    return .refreshed
                case .lost:
                    return .degraded(.retryable)
                case .faulted(let category):
                    return .degraded(category)
                }
            }
        }
        // Filename-presence is not ownership: only rewrite/recover if the remote body still proves this
        // session owns the lock. A successor/foreign/undecodable/absent body fails closed (drop the
        // lease, do not overwrite); a transient read fault retains the lease for a later retry.
        switch await proveOwnLock(client: operationClient) {
        case .owned:
            break
        case .lost:
            confident = false
            holdsLeaseValue = false
            return .degraded(.retryable)
        case .unproven(let category):
            confident = false
            return .degraded(category)
        }
        do {
            try await writeOwnLock(client: operationClient, now: now)
            guard holdsLeaseValue else {
                await deleteOwnLockBestEffort(client: operationClient)
                return .degraded(.retryable)
            }
            lastSuccessfulRefresh = now
            confident = true
            return .refreshed
        } catch {
            confident = false
            return .degraded(RemoteFaultLite.classify(error))
        }
    }

    // MARK: - Assert ownership

    func assertStillOwned(now: Date = Date()) async -> Assertion {
        let operationClient = client
        return await assertStillOwned(now: now, client: operationClient)
    }

    private func assertStillOwned(
        now: Date,
        client operationClient: any RemoteStorageClientProtocol
    ) async -> Assertion {
        guard holdsLeaseValue else {
            return .lost(.ownLockDeleted)
        }
        let entries: [RemoteStorageEntry]
        do {
            entries = try await listLocks(client: operationClient, createIfMissing: false)
        } catch {
            confident = false
            let category = RemoteFaultLite.classify(error)
            if category == .notFound {
                holdsLeaseValue = false
                return .lost(.ownLockDeleted)
            }
            return .faulted(category)
        }

        let scan = scanLocks(entries, now: now)
        if scan.hasBlockingOther {
            confident = false
            holdsLeaseValue = false
            return .lost(.otherWriter)
        }
        if !scan.ownPresent {
            confident = false
            holdsLeaseValue = false
            return .lost(.ownLockDeleted)
        }

        // The filename scan proves only that *a* lock at our path exists; a same-writer successor session
        // could own it. Prove the remote body still matches this session before reclaiming (overwriting)
        // it. A successor/foreign/undecodable/absent body fails closed; a transient fault returns faulted
        // (lease retained, cannot verify now).
        switch await proveOwnLock(client: operationClient) {
        case .owned:
            break
        case .lost:
            confident = false
            holdsLeaseValue = false
            return .lost(.ownLockDeleted)
        case .unproven(let category):
            confident = false
            return .faulted(category)
        }

        // Own lock still present (even stale/unknown) and no unsafe other lock: reclaim and continue.
        do {
            try await writeOwnLock(client: operationClient, now: now)
            guard holdsLeaseValue else {
                await deleteOwnLockBestEffort(client: operationClient)
                return .lost(.ownLockDeleted)
            }
            lastSuccessfulRefresh = now
        } catch {
            confident = false
            // A stale/unknown-mtime own lock is reclaimable by another foreground writer. If we can't
            // refresh it, we can't defend our claim; fail closed.
            if !scan.ownFresh {
                holdsLeaseValue = false
                await deleteOwnLockBestEffort(client: operationClient)
                return .lost(.ownLockDeleted)
            }
            return .faulted(RemoteFaultLite.classify(error))
        }

        // Confirmation re-LIST: a concurrent writer that acquired between our initial LIST and write now
        // appears as unsafe; neither side wins. A *transient fault* here cannot prove a conflict, so we
        // retain the lease and our (freshly written) own lock and only drop confidence for the moment —
        // a later successful refresh/assertion recovers.
        let confirmation: [RemoteStorageEntry]
        do {
            confirmation = try await listLocks(client: operationClient, createIfMissing: false)
        } catch {
            confident = false
            return .faulted(RemoteFaultLite.classify(error))
        }
        let confirmationScan = scanLocks(confirmation, now: now)
        await reportForeignWriter(confirmationScan)
        if confirmationScan.hasBlockingOther {
            holdsLeaseValue = false
            confident = false
            await deleteOwnLockBestEffort(client: operationClient)
            return .lost(.otherWriter)
        }

        confident = true
        return .stillOwned
    }

    // MARK: - Lease-confidence gate

    // External hook: distrust the lease until the next successful in-window refresh/assertion re-proves
    // ownership. Used by callers that detect a confidence-threatening event they cannot resolve here.
    func noteConfidenceLoss() {
        confident = false
    }

    // Callers consult this before a data upload. False means re-acquire/assert before trusting the lease.
    func hasLeaseConfidence(now: Date = Date()) -> Bool {
        guard holdsLeaseValue, confident, let last = lastSuccessfulRefresh else { return false }
        let elapsed = now.timeIntervalSince(last)
        return elapsed >= 0 && elapsed <= Self.confidenceMaxAge
    }

    func canRecoverRetryableRefresh(now: Date = Date()) -> Bool {
        guard holdsLeaseValue, let last = lastSuccessfulRefresh else { return false }
        return now.timeIntervalSince(last) >= 0
    }

    // MARK: - Lock scanning

    private enum Freshness {
        case fresh
        case stale
        case unknown
    }

    private struct LockScan {
        var ownPresent = false
        var ownFresh = false
        var ownFreshness: Freshness?
        var ownModificationDate: Date?
        var hasUnsafeOther = false
        var staleOthers: [(path: String, modificationDate: Date?)] = []
        var missingMtimeOthers: [String] = []

        // Any other writer's lock — fresh, unknown-mtime, or stale — was present in this snapshot.
        var otherWriterObserved: Bool {
            hasUnsafeOther || !staleOthers.isEmpty || !missingMtimeOthers.isEmpty
        }

        var hasBlockingOther: Bool {
            hasUnsafeOther || !missingMtimeOthers.isEmpty
        }

        var foreignTakeoverCandidates: [(path: String, modificationDate: Date?)] {
            staleOthers + missingMtimeOthers.map { (path: $0, modificationDate: nil) }
        }
    }

    private func reportForeignWriter(_ scan: LockScan) async {
        guard scan.otherWriterObserved, let onForeignWriterObserved else { return }
        await onForeignWriterObserved()
    }

    private func scanLocks(_ entries: [RemoteStorageEntry], now: Date) -> LockScan {
        var scan = LockScan()
        let suffix = ".\(RepoLayoutLite.lockFileExtension)"
        for entry in entries where !entry.isDirectory && entry.name.hasSuffix(suffix) {
            if entry.name == ownLockFilename {
                scan.ownPresent = true
                let entryFreshness = freshness(of: entry.modificationDate, now: now)
                scan.ownFresh = entryFreshness == .fresh
                scan.ownFreshness = entryFreshness
                scan.ownModificationDate = entry.modificationDate
                continue
            }
            switch freshness(of: entry.modificationDate, now: now) {
            case .fresh:
                scan.hasUnsafeOther = true
            case .unknown:
                if entry.modificationDate == nil {
                    scan.missingMtimeOthers.append(entry.path)
                } else {
                    scan.hasUnsafeOther = true
                }
            case .stale:
                scan.staleOthers.append((entry.path, entry.modificationDate))
            }
        }
        return scan
    }

    private func freshness(of modificationDate: Date?, now: Date) -> Freshness {
        guard let modificationDate else { return .unknown }
        return freshness(ofTimestamp: modificationDate, now: now)
    }

    private func freshness(of snapshot: RemoteLockReader.Snapshot, now: Date) -> Freshness {
        if let modificationDate = snapshot.modificationDate {
            return freshness(ofTimestamp: modificationDate, now: now)
        }
        guard let writtenAt = snapshot.body?.writtenAt else { return .unknown }
        return freshness(ofTimestamp: writtenAt, now: now)
    }

    private func freshness(ofTimestamp timestamp: Date, now: Date) -> Freshness {
        let elapsed = now.timeIntervalSince(timestamp)
        guard elapsed >= 0 else { return .unknown }   // backward clock: unjudgeable → unsafe
        return elapsed <= Self.expiry + Self.clockSkewTolerance ? .fresh : .stale
    }

    private func sameLockSnapshot(
        _ lhs: RemoteLockReader.Snapshot,
        _ rhs: RemoteLockReader.Snapshot
    ) -> Bool {
        lhs.rawData == rhs.rawData && lhs.modificationDate == rhs.modificationDate
    }

    // LIST and HEAD/metadata can differ in sub-second precision on S3-compatible backends; compare at second resolution.
    private static func sameSecond(_ a: Date?, _ b: Date?) -> Bool {
        switch (a, b) {
        case let (lhs?, rhs?): return Int(lhs.timeIntervalSince1970) == Int(rhs.timeIntervalSince1970)
        case (nil, nil): return true
        default: return false
        }
    }

    private func blockedOrSkipped(_ mode: Mode) -> Acquisition {
        mode == .foreground ? .blocked : .skipped
    }

    private func ownLockBlockedOrSkipped(_ mode: Mode) -> Acquisition {
        mode == .foreground ? .blockedByOwnLock : .skippedByOwnLock
    }

    // MARK: - Own stale reclaim confirmation

    private enum OwnReclaimDecision {
        case reclaimable
        case live
        case gone
        case fault(RemoteFaultLite.Category)
    }

    private func confirmOwnLockReclaimable(
        client operationClient: any RemoteStorageClientProtocol,
        scan: LockScan,
        now: Date
    ) async -> OwnReclaimDecision {
        guard scan.ownFreshness != .fresh else {
            return .live
        }

        let snapshot1: RemoteLockReader.Snapshot
        switch await RemoteLockReader.read(client: operationClient, path: ownLockPath) {
        case .absent:
            return .gone
        case .fault(let category):
            return .fault(category)
        case .present(let snapshot):
            snapshot1 = snapshot
        }
        guard freshness(of: snapshot1, now: now) == .stale else { return .live }
        if let snapshotDate = scan.ownModificationDate {
            guard let mtime1 = snapshot1.modificationDate,
                  Self.sameSecond(snapshotDate, mtime1) else {
                return .live
            }
        }

        switch await RemoteLockReader.read(client: operationClient, path: ownLockPath) {
        case .absent:
            return .gone
        case .fault(let category):
            return .fault(category)
        case .present(let snapshot2):
            guard freshness(of: snapshot2, now: now) == .stale else { return .live }
            guard sameLockSnapshot(snapshot1, snapshot2) else { return .live }
            return .reclaimable
        }
    }

    // MARK: - Foreign stale takeover confirmation

    private struct ForeignStaleProof {
        let snapshot: RemoteLockReader.Snapshot
    }

    private enum ForeignStaleDecision {
        case deletable(ForeignStaleProof)
        case live          // refreshed / token changed / now fresh: a live contender
        case gone
        case fault(RemoteFaultLite.Category)
    }

    // Second confirmation before deleting a foreign lock observed as stale in the scan. Reads the bytes
    // twice and only authorizes deletion when the raw bytes and mtime are unchanged and still stale.
    private func confirmForeignStaleDeletable(
        client operationClient: any RemoteStorageClientProtocol,
        path: String,
        snapshotDate: Date?,
        now: Date
    ) async -> ForeignStaleDecision {
        let snapshot1: RemoteLockReader.Snapshot
        switch await RemoteLockReader.read(client: operationClient, path: path) {
        case .absent:
            return .gone
        case .fault(let category):
            return .fault(category)
        case .present(let snapshot):
            snapshot1 = snapshot
        }
        guard freshness(of: snapshot1, now: now) == .stale else { return .live }
        if let snapshotDate {
            guard let mtime1 = snapshot1.modificationDate,
                  Self.sameSecond(snapshotDate, mtime1) else {
                return .live
            }
        }

        switch await RemoteLockReader.read(client: operationClient, path: path) {
        case .absent:
            return .gone
        case .fault(let category):
            return .fault(category)
        case .present(let snapshot2):
            guard freshness(of: snapshot2, now: now) == .stale else { return .live }
            guard sameLockSnapshot(snapshot1, snapshot2) else { return .live }
            return .deletable(ForeignStaleProof(snapshot: snapshot2))
        }
    }

    private func confirmForeignStaleStillMatches(
        client operationClient: any RemoteStorageClientProtocol,
        path: String,
        proof: ForeignStaleProof,
        now: Date
    ) async -> Bool {
        switch await RemoteLockReader.read(client: operationClient, path: path) {
        case .present(let snapshot):
            return sameLockSnapshot(snapshot, proof.snapshot)
                && freshness(of: snapshot, now: now) == .stale
        case .absent:
            return true
        case .fault:
            return false
        }
    }

    // MARK: - Remote primitives

    private func listLocks(
        client operationClient: any RemoteStorageClientProtocol,
        createIfMissing: Bool
    ) async throws -> [RemoteStorageEntry] {
        do {
            return try await operationClient.list(path: locksDirectoryPath)
        } catch {
            guard createIfMissing, RemoteFaultLite.classify(error) == .notFound else { throw error }
            try await operationClient.createDirectory(path: locksDirectoryPath)
            return try await operationClient.list(path: locksDirectoryPath)
        }
    }

    private func writeOwnLock(client operationClient: any RemoteStorageClientProtocol, now: Date) async throws {
        let nextGeneration = generation + 1
        let body = LockFileBody(
            writerID: writerID,
            sessionToken: sessionToken,
            lockToken: lockToken,
            generation: nextGeneration,
            writtenAt: now
        )
        let data = try LockFileCodec.encode(body)
        let temporaryURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.lockFileExtension)
        try data.write(to: temporaryURL)
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        try await operationClient.upload(
            localURL: temporaryURL,
            remotePath: ownLockPath,
            respectTaskCancellation: false,
            onProgress: nil
        )
        generation = nextGeneration
    }

    // Deletes the own lock only when the remote body still proves it is ours (this session + acquisition).
    // A read fault, an undecodable body, or a successor session leaves the lock intact (fail closed),
    // so a delayed upload from this session or a newer same-writer session is never wrongly removed.
    private func deleteOwnLockBestEffort(client operationClient: any RemoteStorageClientProtocol) async {
        // The proof+delete must complete even when the calling task is cancelled, or a just-landed own lock
        // leaks until lease expiry. A fresh unstructured Task does not inherit cancellation (M4 pattern).
        await Task { await self.performOwnLockDelete(client: operationClient) }.value
    }

    private func performOwnLockDelete(client operationClient: any RemoteStorageClientProtocol) async {
        let body: LockFileBody?
        do {
            body = try await downloadLockBody(client: operationClient, path: ownLockPath)
        } catch {
            return   // notFound (already gone) or transient fault: nothing safe to delete
        }
        guard let body, body.sessionToken == sessionToken, body.lockToken == lockToken else { return }
        try? await operationClient.delete(path: ownLockPath)
    }

    private func downloadLockBody(
        client operationClient: any RemoteStorageClientProtocol,
        path: String
    ) async throws -> LockFileBody? {
        let temporaryURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.lockFileExtension)
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        try await operationClient.download(remotePath: path, localURL: temporaryURL)
        let data = (try? Data(contentsOf: temporaryURL)) ?? Data()
        return LockFileCodec.decode(data)
    }

    // MARK: - Own-lock ownership proof

    private enum OwnLockProof {
        case owned                               // remote body decodes and matches this session + acquisition
        case lost                                // absent, undecodable, or a different session/token (successor/foreign)
        case unproven(RemoteFaultLite.Category)  // transient read fault: ownership cannot be verified right now
    }

    // Re-reads the remote own-lock body and proves it still belongs to *this* service instance. Filename
    // alone is not ownership: a same-writer successor session writes the same path with a different
    // session/token. Refresh and assert reclaim must consult this before rewriting the lock or restoring
    // confidence, so an older instance can never overwrite a successor's lock or regain trust on it.
    private func proveOwnLock(client operationClient: any RemoteStorageClientProtocol) async -> OwnLockProof {
        let body: LockFileBody?
        do {
            body = try await downloadLockBody(client: operationClient, path: ownLockPath)
        } catch {
            let category = RemoteFaultLite.classify(error)
            return category == .notFound ? .lost : .unproven(category)
        }
        guard let body, body.sessionToken == sessionToken, body.lockToken == lockToken else {
            return .lost
        }
        return .owned
    }
}

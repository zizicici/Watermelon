import Foundation
import os.log

// Diagnostic: surfaces which lock-loss branch fired. Console category "WriteLock".
private let writeLockLog = Logger(subsystem: "com.zizicici.watermelon", category: "WriteLock")

// Single-writer lock unit for the Lite repo write path.
//
// Model: one file at `.watermelon/locks/<writerID>.lock` whose body (see `LockFileBody`) carries the
// writer, a per-session token, a per-acquisition lock token, a write generation, and a body timestamp.
// Freshness still comes from the backend LIST/metadata modification date — now with skew tolerance — but every
// destructive action (stale takeover, reclaim, release) second-confirms the candidate body before
// acting, so a just-refreshed foreign lock or a same-writer successor is not deleted if observed.
// Missing mtime falls back to the body timestamp; without either, a stable lock is invalid.
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
        case blockedByOwnLock(OwnLockBlock)       // foreground: same writer's previous session is still live
        case skipped                              // background declined rather than risk a takeover
        case skippedByOwnLock(OwnLockBlock)       // background: same writer's previous session is still live
        case faulted(RemoteFaultLite.Category)    // LIST / create / upload / delete transport fault
    }

    struct OwnLockBlock: Equatable, Sendable {
        enum Reason: Equatable, Sendable {
            case stillFresh
            case missingTimeEvidence
            case changedDuringConfirmation
            case ownershipUnverified
        }

        let reason: Reason
        let retryAfter: Date?
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
        case ownLockStale                         // our lock is present + ours but stale; a read-only gate
                                                  // can't refresh it, so it's a confidence loss (recoverable
                                                  // by the refresh task), not an ownership loss
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
    // The mode this lease was acquired in; background keeps an extra pre-mutation remote check.
    private var acquiredMode: Mode = .foreground

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

    private enum OwnLockWriteFailure: Error {
        case lost
        case unproven(RemoteFaultLite.Category)
    }

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

        // Expired/invalid foreign locks are not active writers. Any attended or background acquire can clear
        // them after body confirmation; fresh/future/changed locks still fail closed.
        switch await clearForeignTakeoverCandidates(client: operationClient, scan: scan, now: now) {
        case .cleared:
            break
        case .blocked:
            return blockedOrSkipped(mode)
        case .fault(let category):
            return .faulted(category)
        }

        let ownReclaimProof: OwnStaleProof?
        if scan.ownPresent {
            switch await confirmOwnLockReclaimable(client: operationClient, scan: scan, now: now) {
            case .reclaimable(let proof):
                ownReclaimProof = proof
            case .gone:
                ownReclaimProof = nil
            case .live(let block):
                return ownLockBlockedOrSkipped(mode, block: block)
            case .fault(let category):
                return .faulted(category)
            }
        } else {
            ownReclaimProof = nil
        }

        lockToken = UUID().uuidString   // fresh acquisition identity
        if let ownReclaimProof {
            switch await deleteConfirmedOwnStaleLock(client: operationClient, proof: ownReclaimProof, now: now) {
            case .removed, .gone:
                break
            case .live(let block):
                return ownLockBlockedOrSkipped(mode, block: block)
            case .fault(let category):
                return .faulted(category)
            }
        }
        do {
            try await writeOwnLock(client: operationClient, now: now, uploadMode: .createIfAbsent)
        } catch {
            if case OwnLockWriteFailure.lost = error {
                return ownLockBlockedOrSkipped(
                    mode,
                    block: OwnLockBlock(reason: .ownershipUnverified, retryAfter: nil)
                )
            }
            if case OwnLockWriteFailure.unproven(let category) = error {
                return .faulted(category)
            }
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
        switch await clearForeignTakeoverCandidates(client: operationClient, scan: confirmationScan, now: now) {
        case .cleared:
            break
        case .blocked:
            await deleteOwnLockBestEffort(client: operationClient)
            return blockedOrSkipped(mode)
        case .fault(let category):
            await deleteOwnLockBestEffort(client: operationClient)
            return .faulted(category)
        }
        if confirmationScan.hasUnsafeOther {
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
        acquiredMode = mode
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
            if case OwnLockWriteFailure.lost = error {
                holdsLeaseValue = false
                return .degraded(.retryable)
            }
            if case OwnLockWriteFailure.unproven(let category) = error {
                return .degraded(category)
            }
            return .degraded(RemoteFaultLite.classify(error))
        }
    }

    // MARK: - Assert ownership

    // Full read-only ownership verification for the write-tier gate (manifest flush / verify / migration /
    // cleanup): it does everything assertStillOwned does to DECIDE ownership — LIST, body-confirm foreign
    // takeover candidates, require the own lock fresh,
    // prove the own-lock body — but NEVER writes the lock (no reclaim, no foreign delete, no post-write
    // confirmation), so concurrent gates cannot corrupt it. A read-only gate can't refresh a stale own lock,
    // so a non-fresh own lock fails closed (only the refresh task re-establishes freshness). On success it
    // does not touch `confident`/`lastSuccessfulRefresh` (no mtime was refreshed).
    func assertOwnedReadOnly(now: Date = Date()) async -> Assertion {
        let operationClient = client
        guard holdsLeaseValue else { return .lost(.ownLockDeleted) }
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
        await reportForeignWriter(scan)
        logLockScan(scan, label: "assertOwnedReadOnly scan")
        if !scan.ownPresent {
            confident = false
            holdsLeaseValue = false
            return .lost(.ownLockDeleted)
        }
        if liveAssertionBlocks(scan) {
            confident = false
            holdsLeaseValue = false
            return .lost(.otherWriter)
        }
        // A stale/missing-mtime LIST entry can still be a live writer once its body timestamp is read
        // (backend LIST mtime can lag the body — S3/Ceph). Body-confirm each candidate read-only (never
        // delete — acquire/refresh own that); a live or unconfirmable candidate fails closed.
        switch await confirmForeignTakeoverCandidatesAbsent(client: operationClient, scan: scan, now: now) {
        case .cleared:
            break
        case .blocked:
            confident = false
            holdsLeaseValue = false
            return .lost(.otherWriter)
        case .fault(let category):
            confident = false
            return .faulted(category)
        }
        // Prove the own-lock body is still ours AND still fresh. Freshness uses the LIST mtime OR the body's
        // `writtenAt` (backends that omit LIST mtime fall back to the body — matching the lock model), so a
        // present-but-stale own lock fails closed: a read-only gate cannot rewrite it to defend the claim
        // (only the refresh task does), and the moment it crosses expiry+skew another writer can take over.
        switch await RemoteLockReader.read(client: operationClient, path: ownLockPath) {
        case .absent:
            confident = false
            holdsLeaseValue = false
            return .lost(.ownLockDeleted)
        case .fault(let category):
            confident = false
            return .faulted(category)
        case .present(let snapshot):
            guard let body = snapshot.body,
                  body.sessionToken == sessionToken,
                  body.lockToken == lockToken else {
                confident = false
                holdsLeaseValue = false
                return .lost(.ownLockDeleted)
            }
            guard freshness(of: snapshot, now: now) == .fresh else {
                // Present + ours but stale: not safe to write now (a foreign writer may take over past
                // expiry+skew). It's a confidence loss, NOT an ownership loss — leave `holdsLeaseValue`
                // intact so the refresh task (the sole writer) can still reclaim it.
                confident = false
                return .lost(.ownLockStale)
            }
            return .stillOwned
        }
    }

    // Read-only counterpart of clearForeignTakeoverCandidates: body-confirms each stale/missing-mtime
    // foreign candidate but never deletes. A candidate that proves live (or whose freshness cannot be
    // confirmed stale/invalid) blocks; proven-stale/invalid or vanished candidates are left in place for
    // acquire/refresh to clear.
    private func confirmForeignTakeoverCandidatesAbsent(
        client operationClient: any RemoteStorageClientProtocol,
        scan: LockScan,
        now: Date
    ) async -> ForeignTakeoverClearance {
        for candidate in scan.foreignTakeoverCandidates {
            switch await confirmForeignStaleDeletable(
                client: operationClient,
                path: candidate.path,
                snapshotDate: candidate.modificationDate,
                now: now
            ) {
            case .deletable, .gone:
                continue
            case .live:
                return .blocked
            case .fault(let category):
                return .fault(category)
            }
        }
        return .cleared
    }

    func assertStillOwned(now: Date = Date()) async -> Assertion {
        let operationClient = client
        return await assertStillOwned(now: now, client: operationClient)
    }

    private func liveAssertionBlocks(_ scan: LockScan) -> Bool {
        scan.hasUnsafeOther
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
        await reportForeignWriter(scan)
        logLockScan(scan, label: "assertStillOwned scan")
        if !scan.ownPresent {
            confident = false
            holdsLeaseValue = false
            return .lost(.ownLockDeleted)
        }
        switch await clearForeignTakeoverCandidates(client: operationClient, scan: scan, now: now) {
        case .cleared:
            break
        case .blocked:
            confident = false
            holdsLeaseValue = false
            return .lost(.otherWriter)
        case .fault(let category):
            confident = false
            return .faulted(category)
        }
        if liveAssertionBlocks(scan) {
            confident = false
            holdsLeaseValue = false
            return .lost(.otherWriter)
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
            if case OwnLockWriteFailure.lost = error {
                holdsLeaseValue = false
                return .lost(.ownLockDeleted)
            }
            if case OwnLockWriteFailure.unproven(let category) = error {
                return .faulted(category)
            }
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
        logLockScan(confirmationScan, label: "assertStillOwned confirmation")
        if liveAssertionBlocks(confirmationScan) {
            holdsLeaseValue = false
            confident = false
            await deleteOwnLockBestEffort(client: operationClient)
            return .lost(.otherWriter)
        }

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

        switch await clearForeignTakeoverCandidates(client: operationClient, scan: confirmationScan, now: now) {
        case .cleared:
            break
        case .blocked:
            holdsLeaseValue = false
            confident = false
            await deleteOwnLockBestEffort(client: operationClient)
            return .lost(.otherWriter)
        case .fault(let category):
            confident = false
            return .faulted(category)
        }

        // Re-prove the body before restoring confidence: cleanup above may have taken time, and a filename-only
        // confirmation can't tell our body from a same-writer successor that reclaimed the path meanwhile.
        switch await proveOwnLock(client: operationClient) {
        case .owned:
            confident = true
            return .stillOwned
        case .lost:
            confident = false
            holdsLeaseValue = false
            return .lost(.ownLockDeleted)
        case .unproven(let category):
            confident = false
            return .faulted(category)
        }
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

    // True for an unattended (background) lease: it must still LIST before remote mutation because a foreign
    // lock can surface within the confidence window unseen.
    var isUnattendedLease: Bool { acquiredMode == .background }

    // Pre-mutation gate for a background lease. It clears stable expired/invalid foreign locks using the same
    // rule as acquire, and fails closed on fresh/future/changed foreign evidence before mutating remote bytes.
    // Unlike `assertStillOwned` it does not rewrite the own lock. A transient LIST fault drops confidence and
    // surfaces as `.faulted` (lease retained); a vanished own lock or notFound fails closed.
    func assertForeignAbsentForBackgroundWrite(now: Date = Date()) async -> Assertion {
        let operationClient = client
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
        await reportForeignWriter(scan)
        if !scan.ownPresent {
            confident = false
            holdsLeaseValue = false
            return .lost(.ownLockDeleted)
        }
        switch await clearForeignTakeoverCandidates(client: operationClient, scan: scan, now: now) {
        case .cleared:
            break
        case .blocked:
            confident = false
            holdsLeaseValue = false
            return .lost(.otherWriter)
        case .fault(let category):
            confident = false
            return .faulted(category)
        }
        if scan.hasUnsafeOther {
            confident = false
            holdsLeaseValue = false
            return .lost(.otherWriter)
        }
        return .stillOwned
    }

    func canRecoverRetryableRefresh(now: Date = Date()) -> Bool {
        guard holdsLeaseValue, let last = lastSuccessfulRefresh else { return false }
        return now.timeIntervalSince(last) >= 0
    }

    // MARK: - Lock scanning

    private enum Freshness: Equatable {
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

    // Diagnostic: only logs when our own lock is missing or any foreign lock was seen.
    private func logLockScan(_ scan: LockScan, label: String) {
        guard !scan.ownPresent || scan.otherWriterObserved else { return }
        writeLockLog.error("[WriteLock] \(label, privacy: .public) writer=\(self.writerID, privacy: .private(mask: .hash)): ownPresent=\(scan.ownPresent) ownFreshness=\(String(describing: scan.ownFreshness), privacy: .public) freshOrUnknownForeign=\(scan.hasUnsafeOther) staleForeign=\(scan.staleOthers.count) missingMtimeForeign=\(scan.missingMtimeOthers.count)")
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
        let sources = [snapshot.modificationDate, snapshot.body?.writtenAt].compactMap { $0 }
        guard !sources.isEmpty else { return .unknown }
        let sourceFreshness = sources.map { freshness(ofTimestamp: $0, now: now) }
        if sourceFreshness.contains(.unknown) { return .unknown }
        return sourceFreshness.contains(.fresh) ? .fresh : .stale
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

    private func blockedOrSkipped(_ mode: Mode) -> Acquisition {
        mode == .foreground ? .blocked : .skipped
    }

    private func ownLockBlockedOrSkipped(_ mode: Mode, block: OwnLockBlock) -> Acquisition {
        mode == .foreground
            ? .blockedByOwnLock(block)
            : .skippedByOwnLock(block)
    }

    private func retryAfter(forTimestamp timestamp: Date?, now: Date) -> Date? {
        guard let timestamp, now.timeIntervalSince(timestamp) >= 0 else { return nil }
        let retryAfter = timestamp.addingTimeInterval(Self.expiry + Self.clockSkewTolerance)
        return retryAfter > now ? retryAfter : nil
    }

    private func retryAfter(for snapshot: RemoteLockReader.Snapshot, now: Date) -> Date? {
        [snapshot.modificationDate, snapshot.body?.writtenAt]
            .compactMap { retryAfter(forTimestamp: $0, now: now) }
            .max()
    }

    private func lacksTimeEvidence(_ snapshot: RemoteLockReader.Snapshot) -> Bool {
        snapshot.modificationDate == nil && snapshot.body?.writtenAt == nil
    }

    private func liveOwnLockBlock(
        freshness: Freshness,
        retryAfter: Date?,
        missingTimeEvidence: Bool = false,
        fallbackReason: OwnLockBlock.Reason = .changedDuringConfirmation
    ) -> OwnLockBlock {
        let reason: OwnLockBlock.Reason = freshness == .unknown
            ? (missingTimeEvidence ? .missingTimeEvidence : .ownershipUnverified)
            : (freshness == .fresh ? .stillFresh : fallbackReason)
        return OwnLockBlock(reason: reason, retryAfter: retryAfter)
    }

    private func isReclaimable(
        _ snapshot: RemoteLockReader.Snapshot,
        now: Date,
        allowsInvalidWithoutTimeEvidence: Bool
    ) -> Bool {
        let snapshotFreshness = freshness(of: snapshot, now: now)
        return snapshotFreshness == .stale
            || (allowsInvalidWithoutTimeEvidence && snapshotFreshness == .unknown && lacksTimeEvidence(snapshot))
    }

    // MARK: - Own stale reclaim confirmation

    private struct OwnStaleProof: StaleLockProof {
        let snapshot: RemoteLockReader.Snapshot
        let allowsInvalidWithoutTimeEvidence: Bool
    }

    private enum OwnReclaimDecision {
        case reclaimable(OwnStaleProof)
        case live(OwnLockBlock)
        case gone
        case fault(RemoteFaultLite.Category)
    }

    private enum OwnStaleRemovalDecision {
        case removed
        case gone
        case live(OwnLockBlock)
        case fault(RemoteFaultLite.Category)
    }

    private func confirmOwnLockReclaimable(
        client operationClient: any RemoteStorageClientProtocol,
        scan: LockScan,
        now: Date
    ) async -> OwnReclaimDecision {
        guard scan.ownFreshness != .fresh else {
            return .live(OwnLockBlock(
                reason: .stillFresh,
                retryAfter: retryAfter(forTimestamp: scan.ownModificationDate, now: now)
            ))
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
        let snapshot1Freshness = freshness(of: snapshot1, now: now)
        let snapshot1AllowsInvalid = snapshot1Freshness == .unknown && lacksTimeEvidence(snapshot1)
        guard snapshot1Freshness == .stale || snapshot1AllowsInvalid else {
            return .live(liveOwnLockBlock(
                freshness: snapshot1Freshness,
                retryAfter: retryAfter(for: snapshot1, now: now),
                missingTimeEvidence: lacksTimeEvidence(snapshot1)
            ))
        }
        if let snapshotDate = scan.ownModificationDate {
            guard let mtime1 = snapshot1.modificationDate,
                  RemoteTimestampComparison.sameSecond(snapshotDate, mtime1) else {
                return .live(OwnLockBlock(
                    reason: .changedDuringConfirmation,
                    retryAfter: retryAfter(for: snapshot1, now: now)
                ))
            }
        }

        switch await RemoteLockReader.read(client: operationClient, path: ownLockPath) {
        case .absent:
            return .gone
        case .fault(let category):
            return .fault(category)
        case .present(let snapshot2):
            let snapshot2Freshness = freshness(of: snapshot2, now: now)
            let allowsInvalid = snapshot1AllowsInvalid
                && snapshot2Freshness == .unknown
                && lacksTimeEvidence(snapshot2)
            guard snapshot2Freshness == .stale || allowsInvalid else {
                return .live(liveOwnLockBlock(
                    freshness: snapshot2Freshness,
                    retryAfter: retryAfter(for: snapshot2, now: now),
                    missingTimeEvidence: lacksTimeEvidence(snapshot2)
                ))
            }
            guard sameLockSnapshot(snapshot1, snapshot2) else {
                return .live(OwnLockBlock(
                    reason: .changedDuringConfirmation,
                    retryAfter: retryAfter(for: snapshot2, now: now)
                ))
            }
            return .reclaimable(OwnStaleProof(
                snapshot: snapshot2,
                allowsInvalidWithoutTimeEvidence: allowsInvalid
            ))
        }
    }

    private func deleteConfirmedOwnStaleLock(
        client operationClient: any RemoteStorageClientProtocol,
        proof: OwnStaleProof,
        now: Date
    ) async -> OwnStaleRemovalDecision {
        switch await deleteConfirmedStaleLock(
            client: operationClient,
            path: ownLockPath,
            proof: proof,
            now: now
        ) {
        case .removed:
            return .removed
        case .gone:
            return .gone
        case .fault(let category):
            return .fault(category)
        case .changed(let snapshot):
            let snapshotFreshness = freshness(of: snapshot, now: now)
            return .live(liveOwnLockBlock(
                freshness: snapshotFreshness,
                retryAfter: retryAfter(for: snapshot, now: now),
                missingTimeEvidence: lacksTimeEvidence(snapshot)
            ))
        }
    }

    // MARK: - Foreign stale takeover confirmation

    private protocol StaleLockProof {
        var snapshot: RemoteLockReader.Snapshot { get }
        var allowsInvalidWithoutTimeEvidence: Bool { get }
    }

    private struct ForeignStaleProof: StaleLockProof {
        let snapshot: RemoteLockReader.Snapshot
        let allowsInvalidWithoutTimeEvidence: Bool
    }

    private enum ForeignStaleDecision {
        case deletable(ForeignStaleProof)
        case live          // refreshed / token changed / now fresh: a live contender
        case gone
        case fault(RemoteFaultLite.Category)
    }

    private enum ForeignTakeoverClearance {
        case cleared
        case blocked
        case fault(RemoteFaultLite.Category)
    }

    private func clearForeignTakeoverCandidates(
        client operationClient: any RemoteStorageClientProtocol,
        scan: LockScan,
        now: Date
    ) async -> ForeignTakeoverClearance {
        for stale in scan.foreignTakeoverCandidates {
            switch await confirmForeignStaleDeletable(
                client: operationClient,
                path: stale.path,
                snapshotDate: stale.modificationDate,
                now: now
            ) {
            case .deletable(let proof):
                switch await deleteConfirmedStaleLock(
                    client: operationClient,
                    path: stale.path,
                    proof: proof,
                    now: now
                ) {
                case .removed, .gone:
                    continue
                case .changed:
                    return .blocked
                case .fault(let category):
                    return .fault(category)
                }
            case .live:
                return .blocked
            case .gone:
                continue
            case .fault(let category):
                return .fault(category)
            }
        }
        return .cleared
    }

    // Second confirmation before deleting a stale lock observed in the scan.
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
        let snapshot1Freshness = freshness(of: snapshot1, now: now)
        let snapshot1AllowsInvalid = snapshot1Freshness == .unknown && lacksTimeEvidence(snapshot1)
        guard snapshot1Freshness == .stale || snapshot1AllowsInvalid else { return .live }
        if let snapshotDate {
            guard let mtime1 = snapshot1.modificationDate,
                  RemoteTimestampComparison.sameSecond(snapshotDate, mtime1) else {
                return .live
            }
        }

        switch await RemoteLockReader.read(client: operationClient, path: path) {
        case .absent:
            return .gone
        case .fault(let category):
            return .fault(category)
        case .present(let snapshot2):
            guard sameLockSnapshot(snapshot1, snapshot2) else { return .live }
            let snapshot2Freshness = freshness(of: snapshot2, now: now)
            let allowsInvalid = snapshot1AllowsInvalid
                && snapshot2Freshness == .unknown
                && lacksTimeEvidence(snapshot2)
            guard snapshot2Freshness == .stale || allowsInvalid else { return .live }
            return .deletable(ForeignStaleProof(
                snapshot: snapshot2,
                allowsInvalidWithoutTimeEvidence: allowsInvalid
            ))
        }
    }

    private enum StaleLockRemovalDecision {
        case removed
        case gone
        case changed(RemoteLockReader.Snapshot)
        case fault(RemoteFaultLite.Category)
    }

    private func deleteConfirmedStaleLock(
        client operationClient: any RemoteStorageClientProtocol,
        path: String,
        proof: any StaleLockProof,
        now: Date
    ) async -> StaleLockRemovalDecision {
        switch await RemoteLockReader.read(client: operationClient, path: path) {
        case .present(let snapshot):
            guard sameLockSnapshot(snapshot, proof.snapshot),
                  isReclaimable(
                    snapshot,
                    now: now,
                    allowsInvalidWithoutTimeEvidence: proof.allowsInvalidWithoutTimeEvidence
                  ) else {
                return .changed(snapshot)
            }
            do {
                try await operationClient.delete(path: path)
                return .removed
            } catch {
                let category = RemoteFaultLite.classify(error)
                return category == .notFound ? .gone : .fault(category)
            }
        case .absent:
            return .gone
        case .fault(let category):
            return .fault(category)
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

    private func writeOwnLock(
        client operationClient: any RemoteStorageClientProtocol,
        now: Date,
        uploadMode: RemoteUploadMode = .replace
    ) async throws {
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
        do {
            try await operationClient.upload(
                localURL: temporaryURL,
                remotePath: ownLockPath,
                mode: uploadMode,
                respectTaskCancellation: false,
                onProgress: nil
            )
            generation = nextGeneration
        } catch {
            if uploadMode == .createIfAbsent {
                switch await proveOwnLock(client: operationClient) {
                case .owned:
                    generation = nextGeneration
                    return
                case .lost:
                    break
                case .unproven(let category):
                    throw OwnLockWriteFailure.unproven(category)
                }
            }
            guard Self.isNameCollision(error) else { throw error }
            switch await proveOwnLock(client: operationClient) {
            case .owned:
                try await operationClient.setModificationDate(now, forPath: ownLockPath)
                switch await proveOwnLockTouched(client: operationClient, now: now) {
                case .touched:
                    break
                case .lost:
                    throw OwnLockWriteFailure.lost
                case .unproven(let category):
                    throw OwnLockWriteFailure.unproven(category)
                }
            case .lost:
                throw OwnLockWriteFailure.lost
            case .unproven(let category):
                throw OwnLockWriteFailure.unproven(category)
            }
        }
    }

    // Deletes the own lock only when the remote body still proves it is ours (this session + acquisition).
    // A read fault, an undecodable body, or a successor session leaves the lock intact (fail closed).
    private func deleteOwnLockBestEffort(client operationClient: any RemoteStorageClientProtocol) async {
        // The proof+delete must complete even when the calling task is cancelled, or a just-landed own lock
        // leaks until lease expiry. A fresh unstructured Task does not inherit cancellation (M4 pattern).
        await Task { await self.performOwnLockDelete(client: operationClient) }.value
    }

    private func performOwnLockDelete(client operationClient: any RemoteStorageClientProtocol) async {
        let body: LockFileBody?
        do {
            body = try await RemoteLockReader.downloadBody(client: operationClient, path: ownLockPath)
        } catch {
            return   // notFound (already gone) or transient fault: nothing safe to delete
        }
        guard let body, body.sessionToken == sessionToken, body.lockToken == lockToken else { return }
        try? await operationClient.delete(path: ownLockPath)
    }

    // MARK: - Own-lock ownership proof

    private enum OwnLockProof {
        case owned                               // remote body decodes and matches this session + acquisition
        case lost                                // absent, undecodable, or a different session/token (successor/foreign)
        case unproven(RemoteFaultLite.Category)  // transient read fault: ownership cannot be verified right now
    }

    private enum OwnLockTouchProof {
        case touched
        case lost
        case unproven(RemoteFaultLite.Category)
    }

    // Re-reads the remote own-lock body and proves it still belongs to *this* service instance. Filename
    // alone is not ownership: a same-writer successor session writes the same path with a different
    // session/token. Refresh and assert reclaim must consult this before rewriting the lock or restoring
    // confidence, so an older instance can never overwrite a successor's lock or regain trust on it.
    private func proveOwnLock(client operationClient: any RemoteStorageClientProtocol) async -> OwnLockProof {
        let body: LockFileBody?
        do {
            body = try await RemoteLockReader.downloadBody(client: operationClient, path: ownLockPath)
        } catch {
            let category = RemoteFaultLite.classify(error)
            if category == .notFound {
                writeLockLog.error("[WriteLock] proveOwnLock lost: own lock file not found at \(self.ownLockPath, privacy: .private)")
                return .lost
            }
            return .unproven(category)
        }
        guard let body else {
            writeLockLog.error("[WriteLock] proveOwnLock lost: own lock body undecodable/empty at \(self.ownLockPath, privacy: .private)")
            return .lost
        }
        guard body.sessionToken == sessionToken, body.lockToken == lockToken else {
            writeLockLog.error("[WriteLock] proveOwnLock lost: token mismatch — remote(session=\(body.sessionToken, privacy: .private(mask: .hash)) lock=\(body.lockToken, privacy: .private(mask: .hash)) gen=\(body.generation)) vs ours(session=\(self.sessionToken, privacy: .private(mask: .hash)) lock=\(self.lockToken, privacy: .private(mask: .hash)))")
            return .lost
        }
        return .owned
    }

    private func proveOwnLockTouched(
        client operationClient: any RemoteStorageClientProtocol,
        now: Date
    ) async -> OwnLockTouchProof {
        switch await RemoteLockReader.read(client: operationClient, path: ownLockPath) {
        case .absent:
            return .lost
        case .fault(let category):
            return .unproven(category)
        case .present(let snapshot):
            guard let body = snapshot.body,
                  body.sessionToken == sessionToken,
                  body.lockToken == lockToken else {
                return .lost
            }
            let touched = [snapshot.modificationDate, snapshot.body?.writtenAt]
                .compactMap { $0 }
                .contains { RemoteTimestampComparison.sameSecond($0, now) }
            return touched ? .touched : .unproven(.retryable)
        }
    }

    private static func isNameCollision(_ error: Error, maxDepth: Int = 32) -> Bool {
        var pending: [Error] = [error]
        var visited = Set<String>()
        while let next = pending.popLast(), visited.count < maxDepth {
            let ns = next as NSError
            let key = "\(ns.domain)#\(ns.code)#\(ns.localizedDescription)"
            guard visited.insert(key).inserted else { continue }

            if SMBErrorClassifier.isNameCollision(next) {
                return true
            }
            if let storage = next as? RemoteStorageClientError, case .underlying(let inner) = storage {
                pending.append(inner)
            }
            if let underlying = ns.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(underlying)
            }
        }
        return false
    }
}

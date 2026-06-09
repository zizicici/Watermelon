import Foundation

// Dormant single-writer lock unit (Repo V2 Stage B, Step 3). Nothing in production wires this yet;
// it only encodes the LIST-mtime lease semantics so later cutover work shares one implementation.
//
// Model: one empty file at `.watermelon/locks/<writerID>.lock`. Freshness is read from the backend
// LIST modification date only — there is no lock body, atomic-create, GET, or clock coordination.
// A nil mtime means "freshness unknown", which is treated as unsafe for other writers (it could be
// fresh) but reclaimable for our own lock when nothing else contends.
actor WriteLockService {
    static let expiry: TimeInterval = 5 * 60
    static let refreshInterval: TimeInterval = 2 * 60
    // A lease is only trusted up to two refresh intervals past the last confirmed write.
    static let confidenceMaxAge: TimeInterval = refreshInterval * 2

    enum Mode: Sendable {
        case foreground
        case background
    }

    enum Acquisition: Equatable, Sendable {
        case acquired
        case blocked                              // foreground fail-closed: an unsafe lock or post-write conflict
        case skipped                              // background declined rather than risk a takeover
        case faulted(RemoteFaultLite.Category)    // LIST / create / upload / delete transport fault
    }

    enum Refresh: Equatable, Sendable {
        case refreshed                            // own lock rewritten; confidence restored
        case degraded(RemoteFaultLite.Category)   // write failed; confidence dropped, ownership retained
    }

    enum Assertion: Equatable, Sendable {
        case stillOwned                           // safe to continue (confidence refreshed or degraded in place)
        case lost(LossReason)                     // stop: ownership can no longer be trusted
        case faulted(RemoteFaultLite.Category)    // LIST fault; cannot verify ownership
    }

    enum LossReason: Equatable, Sendable {
        case otherWriter                          // a fresh or unknown-mtime other lock is present
        case ownLockDeleted                       // our lock is gone
    }

    // Reasons a caller (or this service) drops lease confidence. The elapsed-since-refresh case is
    // computed by the gate, not signalled here.
    enum ConfidenceLossTrigger: Sendable, CaseIterable {
        case refreshTransportFailure
        case appLifecycleResume
        case appLifecycleSuspend
        case appLifecycleKillRecovery
        case foregroundBackgroundTransition
        case lockListFailure
    }

    private let client: any RemoteStorageClientProtocol
    private let locksDirectoryPath: String
    private let ownLockPath: String
    private let ownLockFilename: String
    // Best-effort diagnostic hook: fired when acquire observes another writer's lock. Must never throw
    // and never change acquire's outcome.
    private let onForeignWriterObserved: (@Sendable () async -> Void)?

    private var holdsLeaseValue = false
    private var confident = false
    private var lastSuccessfulRefresh: Date?
    private var confidenceLossPending = false

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
        self.locksDirectoryPath = RepoLayoutLite.locksDirectoryPath(basePath: basePath)
        self.ownLockPath = lockPath
        self.ownLockFilename = filename
        self.onForeignWriterObserved = onForeignWriterObserved
    }

    var holdsLease: Bool { holdsLeaseValue }

    // MARK: - Acquire

    func acquire(mode: Mode, now: Date = Date()) async -> Acquisition {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await listLocks(createIfMissing: true)
        } catch {
            return .faulted(RemoteFaultLite.classify(error))
        }

        let scan = scanLocks(entries, now: now)
        await reportForeignWriter(scan)
        // An unsafe other lock has top priority; our own lock must not hide it.
        if scan.hasUnsafeOther {
            return blockedOrSkipped(mode)
        }

        // No unsafe other lock: every other lock (if any) is stale.
        if mode == .background, !scan.ownPresent, !scan.staleOtherPaths.isEmpty {
            return .skipped   // background never takes over a stranger's stale lock
        }

        if mode == .foreground {
            for path in scan.staleOtherPaths {
                do {
                    try await client.delete(path: path)
                } catch {
                    let category = RemoteFaultLite.classify(error)
                    if category != .notFound {
                        return .faulted(category)
                    }
                }
            }
        }

        do {
            try await writeOwnLock()
        } catch {
            return .faulted(RemoteFaultLite.classify(error))
        }

        // Re-LIST after writing: if a fresh/unknown other lock now appears, neither side wins.
        let confirmation: [RemoteStorageEntry]
        do {
            confirmation = try await listLocks(createIfMissing: false)
        } catch {
            await deleteOwnLockBestEffort()
            return .faulted(RemoteFaultLite.classify(error))
        }
        let confirmationScan = scanLocks(confirmation, now: now)
        await reportForeignWriter(confirmationScan)
        if confirmationScan.hasUnsafeOther {
            await deleteOwnLockBestEffort()
            return blockedOrSkipped(mode)
        }

        holdsLeaseValue = true
        confident = true
        confidenceLossPending = false
        lastSuccessfulRefresh = now
        return .acquired
    }

    // MARK: - Release

    // Drops the lease: deletes our own lock and clears local state. Best-effort delete — we are
    // abandoning ownership regardless of whether the remote delete lands.
    func release() async {
        holdsLeaseValue = false
        confident = false
        lastSuccessfulRefresh = nil
        confidenceLossPending = false
        await deleteOwnLockBestEffort()
    }

    // MARK: - Refresh

    // Overwrites the own empty lock. A transient write failure only degrades confidence; it does not
    // abort ownership, because the lock may still be present and fresh on the backend.
    func refresh(now: Date = Date()) async -> Refresh {
        guard holdsLeaseValue else {
            return .degraded(.retryable)
        }
        // Skip upload if the gap since the last confirmed write exceeds the confidence window.
        // Another writer may have reclaimed the expired lock; uploading would recreate our stale
        // lock and evict the new owner.
        if let previous = lastSuccessfulRefresh {
            let elapsed = now.timeIntervalSince(previous)
            guard elapsed >= 0, elapsed <= Self.confidenceMaxAge else {
                confident = false
                confidenceLossPending = true
                return .degraded(.retryable)
            }
        }
        do {
            try await writeOwnLock()
            guard holdsLeaseValue else {
                await deleteOwnLockBestEffort()
                return .degraded(.retryable)
            }
            lastSuccessfulRefresh = now
            if !confidenceLossPending {
                confident = true
            }
            return .refreshed
        } catch {
            confident = false
            confidenceLossPending = true
            return .degraded(RemoteFaultLite.classify(error))
        }
    }

    // MARK: - Assert ownership

    func assertStillOwned(mode: Mode, now: Date = Date()) async -> Assertion {
        guard holdsLeaseValue else {
            return .lost(.ownLockDeleted)
        }
        let entries: [RemoteStorageEntry]
        do {
            entries = try await listLocks(createIfMissing: false)
        } catch {
            confident = false
            confidenceLossPending = true
            let category = RemoteFaultLite.classify(error)
            if category == .notFound {
                holdsLeaseValue = false
                return .lost(.ownLockDeleted)
            }
            return .faulted(category)
        }

        let scan = scanLocks(entries, now: now)
        if scan.hasUnsafeOther {
            confident = false
            holdsLeaseValue = false
            return .lost(.otherWriter)
        }
        if !scan.ownPresent {
            confident = false
            holdsLeaseValue = false
            return .lost(.ownLockDeleted)
        }

        // Own lock still present (even stale/unknown) and no unsafe other lock: reclaim and continue.
        do {
            try await writeOwnLock()
            guard holdsLeaseValue else {
                await deleteOwnLockBestEffort()
                return .lost(.ownLockDeleted)
            }
            lastSuccessfulRefresh = now
            confident = true
            confidenceLossPending = false
        } catch {
            confident = false
            confidenceLossPending = true
            // A stale/unknown-mtime own lock is reclaimable by another foreground writer.
            // If we can't refresh it, we can't defend our claim; fail closed.
            if !scan.ownFresh {
                holdsLeaseValue = false
                await deleteOwnLockBestEffort()
                return .lost(.ownLockDeleted)
            }
            return .faulted(RemoteFaultLite.classify(error))
        }

        // Confirmation re-LIST: mirror acquire's post-write check. A concurrent writer that acquired
        // between our initial LIST and writeOwnLock now appears as unsafe; neither side wins.
        let confirmation: [RemoteStorageEntry]
        do {
            confirmation = try await listLocks(createIfMissing: false)
        } catch {
            confident = false
            holdsLeaseValue = false
            await deleteOwnLockBestEffort()
            return .faulted(RemoteFaultLite.classify(error))
        }
        let confirmationScan = scanLocks(confirmation, now: now)
        await reportForeignWriter(confirmationScan)
        if confirmationScan.hasUnsafeOther {
            holdsLeaseValue = false
            confident = false
            await deleteOwnLockBestEffort()
            return .lost(.otherWriter)
        }

        return .stillOwned
    }

    // MARK: - Lease-confidence gate

    func noteConfidenceLoss(_ trigger: ConfidenceLossTrigger) {
        _ = trigger
        confident = false
        confidenceLossPending = true
    }

    // Callers consult this before a data upload. False means re-acquire/assert before trusting the lease.
    func hasLeaseConfidence(now: Date = Date()) -> Bool {
        guard holdsLeaseValue, confident, let last = lastSuccessfulRefresh else { return false }
        let elapsed = now.timeIntervalSince(last)
        return elapsed >= 0 && elapsed <= Self.confidenceMaxAge
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
        var hasUnsafeOther = false
        var staleOtherPaths: [String] = []

        // Any other writer's lock — fresh, unknown-mtime, or stale — was present in this snapshot.
        var otherWriterObserved: Bool { hasUnsafeOther || !staleOtherPaths.isEmpty }
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
                scan.ownFresh = freshness(of: entry.modificationDate, now: now) == .fresh
                continue
            }
            switch freshness(of: entry.modificationDate, now: now) {
            case .fresh, .unknown:
                scan.hasUnsafeOther = true
            case .stale:
                scan.staleOtherPaths.append(entry.path)
            }
        }
        return scan
    }

    private func freshness(of modificationDate: Date?, now: Date) -> Freshness {
        guard let modificationDate else { return .unknown }
        let elapsed = now.timeIntervalSince(modificationDate)
        guard elapsed >= 0 else { return .unknown }
        return elapsed <= Self.expiry ? .fresh : .stale
    }

    private func blockedOrSkipped(_ mode: Mode) -> Acquisition {
        mode == .foreground ? .blocked : .skipped
    }

    // MARK: - Remote primitives

    private func listLocks(createIfMissing: Bool) async throws -> [RemoteStorageEntry] {
        do {
            return try await client.list(path: locksDirectoryPath)
        } catch {
            guard createIfMissing, RemoteFaultLite.classify(error) == .notFound else { throw error }
            try await client.createDirectory(path: locksDirectoryPath)
            return try await client.list(path: locksDirectoryPath)
        }
    }

    private func writeOwnLock() async throws {
        let temporaryURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(RepoLayoutLite.lockFileExtension)
        try Data().write(to: temporaryURL)
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        try await client.upload(
            localURL: temporaryURL,
            remotePath: ownLockPath,
            respectTaskCancellation: false,
            onProgress: nil
        )
    }

    private func deleteOwnLockBestEffort() async {
        try? await client.delete(path: ownLockPath)
    }
}

import Foundation
import os.log

private let livenessLog = Logger(subsystem: "com.zizicici.watermelon", category: "LivenessTracker")

actor LivenessTracker {
    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let writerID: String
    private let isLocalVolume: Bool
    private var task: Task<Void, Never>?

    static let heartbeatInterval: TimeInterval = 30
    static let staleThreshold: TimeInterval = 5 * 60

    /// Explicit per-peer status — the value-type replacement for "is this writerID
    /// in the active set?". Caller MUST treat `.unknown` as not-cleanup-safe
    /// (peer might still be active, we just couldn't tell).
    enum PeerStatus: Sendable, Equatable {
        case active(lastSeenMs: Int64)
        case stale(lastSeenMs: Int64)
        case unknown(reason: UnknownReason)

        enum UnknownReason: Sendable, Equatable {
            /// Transient transport / parse error after the per-peer retry budget.
            case readFailed
            /// File 404'd, but the backend has post-write visibility lag (R2/MinIO/WebDAV-behind-cache).
            /// The peer may have just written and we're seeing pre-write state.
            case vanishedWithinGrace
        }
    }

    /// Cleanup gate: `OrphanMetadataCleanup.sweep` may only run when `isComplete`.
    /// Partial views (any unknown peer) must NOT trigger sweep — otherwise sweep
    /// would delete an active peer's `.staging-*` files.
    struct ActiveWritersView: Sendable, Equatable {
        let activePeerIDs: Set<String>
        let stalePeerIDs: Set<String>
        let unknownPeerIDs: Set<String>

        static let empty = ActiveWritersView(activePeerIDs: [], stalePeerIDs: [], unknownPeerIDs: [])

        var isComplete: Bool { unknownPeerIDs.isEmpty }

        /// What sweep must treat as "still active" — unions active + unknown, because
        /// an unknown peer might still be alive (we just couldn't confirm).
        var sweepProtectionSet: Set<String> { activePeerIDs.union(unknownPeerIDs) }
    }

    init(client: any RemoteStorageClientProtocol, basePath: String, writerID: String, isLocalVolume: Bool) {
        self.client = client
        self.basePath = basePath
        self.writerID = writerID
        self.isLocalVolume = isLocalVolume
    }

    func start() {
        guard !isLocalVolume, task == nil else { return }
        let captured = self
        // Task { } does not inherit cancellation; stopAndWait() is the only termination path.
        task = Task(priority: .utility) {
            while !Task.isCancelled {
                await captured.tick()
                do {
                    try await Task.sleep(for: .seconds(LivenessTracker.heartbeatInterval))
                } catch {
                    return
                }
            }
        }
    }

    func stop() {
        task?.cancel()
        task = nil
    }

    /// Cancels heartbeat AND awaits the current tick. Caller (shutdown) must use this
    /// before disconnecting `metadataClient` — otherwise an in-flight tick would keep
    /// using the just-closed connection.
    func stopAndWait() async {
        task?.cancel()
        _ = await task?.value
        task = nil
    }

    private func tick() async {
        // MARK: Phase A — write heartbeat to local temp
        let timestampMs = Int64(Date().timeIntervalSince1970 * 1000)
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("liveness-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        func writeHeartbeat(timestampMs: Int64) throws {
            let body: [String: Any] = ["ts": timestampMs]
            let data = try JSONSerialization.data(withJSONObject: body)
            try data.write(to: temp, options: .atomic)
        }
        do { try writeHeartbeat(timestampMs: timestampMs) } catch { return }
        let remotePath = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        // Staging + rename instead of delete-then-create: keeps a file at remotePath
        // throughout the swap. Peer's snapshotPeerStatuses sweep gate can't see us
        // as inactive during the brief window between the old delete and the new create.
        let stagingPath = remotePath + ".staging-\(UUID().uuidString).tmp"
        var lastFailure: Error?
        var stagingCreated = false

        // MARK: Phase B — publish via staging+move
        // Cancellation must surface so shutdown's stopAndWait can unblock past atomicCreate.
        do {
            _ = try await client.atomicCreate(
                localURL: temp,
                remotePath: stagingPath,
                respectTaskCancellation: true
            )
            stagingCreated = true
        } catch is CancellationError {
            try? await client.delete(path: stagingPath)
            return
        } catch {
            lastFailure = error
            try? await client.delete(path: stagingPath)
            livenessLog.warning("[Liveness] staging write failed for \(remotePath, privacy: .public): \(error.localizedDescription, privacy: .public)")
        }
        if Task.isCancelled {
            try? await client.delete(path: stagingPath)
            return
        }
        if stagingCreated {
            do {
                try await client.move(from: stagingPath, to: remotePath)
                return
            } catch is CancellationError {
                try? await client.delete(path: stagingPath)
                return
            } catch {
                lastFailure = error
            }
            if Task.isCancelled {
                try? await client.delete(path: stagingPath)
                return
            }
            let stagingStillExists: Bool
            do {
                let stagingEntry = try await client.metadata(path: stagingPath)
                stagingStillExists = stagingEntry?.isDirectory == false
            } catch is CancellationError {
                try? await client.delete(path: stagingPath)
                return
            } catch {
                stagingStillExists = isStorageNotFoundError(error) ? false : true
            }
            if !stagingStillExists, let lastFailure {
                livenessLog.warning("[Liveness] staging vanished after failed move for \(remotePath, privacy: .public): \(lastFailure.localizedDescription, privacy: .public)")
            }
            do {
                if try await concurrentWriterClaimedRemoteHeartbeat(remotePath: remotePath, timestampMs: timestampMs) {
                    try? await client.delete(path: stagingPath)
                    return
                }
            } catch is CancellationError {
                try? await client.delete(path: stagingPath)
                return
            } catch {
                livenessLog.warning("[Liveness] heartbeat probe failed after failed move for \(remotePath, privacy: .public), continuing exclusive fallback: \(error.localizedDescription, privacy: .public)")
            }
        }
        if Task.isCancelled {
            try? await client.delete(path: stagingPath)
            return
        }

        // MARK: Phase C — exclusive atomic-create fallback (only when backend gives exclusive guarantee)
        let heartbeatSize = (try? FileManager.default.attributesOfItem(atPath: temp.path)[.size] as? Int64) ?? 0
        guard client.atomicCreateGuarantee(forFileSize: heartbeatSize, remotePath: remotePath) == .exclusive else {
            try? await client.delete(path: stagingPath)
            if let lastFailure {
                livenessLog.warning("[Liveness] exclusive fallback unavailable after failed heartbeat write for \(remotePath, privacy: .public): \(lastFailure.localizedDescription, privacy: .public)")
            }
            return
        }
        do {
            if try await concurrentWriterClaimedRemoteHeartbeat(remotePath: remotePath, timestampMs: timestampMs) {
                try? await client.delete(path: stagingPath)
                return
            }
        } catch is CancellationError {
            try? await client.delete(path: stagingPath)
            return
        } catch {
            livenessLog.warning("[Liveness] heartbeat probe failed before fallback atomic create for \(remotePath, privacy: .public), continuing exclusive create: \(error.localizedDescription, privacy: .public)")
        }

        // MARK: Phase D — overwrite renewal (only when dataPathOverwriteRisk == .none)
        // Cancellation must surface here too so stopAndWait can unblock during shutdown.
        do {
            let fallbackTimestampMs = Int64(Date().timeIntervalSince1970 * 1000)
            do { try writeHeartbeat(timestampMs: fallbackTimestampMs) } catch {
                try? await client.delete(path: stagingPath)
                return
            }
            do {
                if try await concurrentWriterClaimedRemoteHeartbeat(remotePath: remotePath, timestampMs: fallbackTimestampMs) {
                    try? await client.delete(path: stagingPath)
                    return
                }
            } catch is CancellationError {
                try? await client.delete(path: stagingPath)
                return
            } catch {
                livenessLog.warning("[Liveness] heartbeat probe failed before refreshed fallback atomic create for \(remotePath, privacy: .public), continuing exclusive create: \(error.localizedDescription, privacy: .public)")
            }
            let createResult = try await client.atomicCreate(localURL: temp, remotePath: remotePath, respectTaskCancellation: true)
            if case .alreadyExists = createResult {
                guard client.supportsInPlaceLivenessRenewal else {
                    try? await client.delete(path: stagingPath)
                    return
                }
                let renewalTimestampMs = Int64(Date().timeIntervalSince1970 * 1000)
                do {
                    if try await concurrentWriterClaimedRemoteHeartbeat(remotePath: remotePath, timestampMs: renewalTimestampMs) {
                        try? await client.delete(path: stagingPath)
                        return
                    }
                } catch is CancellationError {
                    try? await client.delete(path: stagingPath)
                    return
                } catch {
                    livenessLog.warning("[Liveness] heartbeat probe failed before overwrite renewal for \(remotePath, privacy: .public), continuing renewal: \(error.localizedDescription, privacy: .public)")
                }
                do { try writeHeartbeat(timestampMs: renewalTimestampMs) } catch {
                    try? await client.delete(path: stagingPath)
                    return
                }
                try await client.upload(localURL: temp, remotePath: remotePath, respectTaskCancellation: true, onProgress: nil)
            }
            try? await client.delete(path: stagingPath)
            return
        } catch is CancellationError {
            try? await client.delete(path: stagingPath)
            return
        } catch {
            lastFailure = error
        }
        try? await client.delete(path: stagingPath)
        if let lastFailure {
            livenessLog.warning("[Liveness] all write strategies failed for \(remotePath, privacy: .public): \(lastFailure.localizedDescription, privacy: .public)")
        }
    }

    /// Same-writerID concurrency defense: another instance running under our writerID
    /// has just published a fresher (and not-yet-stale) heartbeat. If true, we yield
    /// — don't overwrite their newer write with our older timestamp. This is NOT a
    /// peer-staleness check; peer detection lives in `snapshotPeerStatuses`.
    private func concurrentWriterClaimedRemoteHeartbeat(remotePath: String, timestampMs: Int64) async throws -> Bool {
        guard let remoteTimestamp = try await existingHeartbeatTimestamp(remotePath: remotePath) else {
            return false
        }
        return remoteTimestamp > timestampMs && !LivenessTracker.isStale(timestampMs: remoteTimestamp)
    }

    private func existingHeartbeatTimestamp(remotePath: String) async throws -> Int64? {
        do {
            guard let metadata = try await client.metadata(path: remotePath), !metadata.isDirectory else {
                return nil
            }
        } catch {
            if isStorageNotFoundError(error) { return nil }
            throw error
        }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("liveness-existing-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await client.download(remotePath: remotePath, localURL: temp)
        } catch {
            if isStorageNotFoundError(error) { return nil }
            throw error
        }
        return try readHeartbeatTimestamp(from: temp, remotePath: remotePath)
    }

    static func isStale(
        timestampMs: Int64,
        now: Date = Date(),
        gracePeriodSec: TimeInterval = 0
    ) -> Bool {
        let timestamp = Date(timeIntervalSince1970: TimeInterval(timestampMs) / 1000)
        let age = now.timeIntervalSince(timestamp)
        return age > staleThreshold + gracePeriodSec
    }

    /// Classifies every peer heartbeat into `PeerStatus` and returns a view the
    /// builder can gate on. The whole-directory `client.list` failure still throws
    /// (no view at all) — caller skips cleanup. Per-peer indeterminate reads land
    /// as `.unknown` so they show up in `unknownPeerIDs` and block `isComplete`.
    func snapshotPeerStatuses() async throws -> ActiveWritersView {
        guard !isLocalVolume else { return .empty }
        let dir = RepoLayout.livenessDirectoryPath(base: basePath)
        let entries = try await client.list(path: dir)
        let now = Date()
        let gracePeriodSec = client.livenessConsistencyGraceSeconds

        var active: Set<String> = []
        var stale: Set<String> = []
        var unknown: Set<String> = []

        for entry in entries {
            guard !entry.isDirectory,
                  let parsed = RepoLayout.parseLivenessFilename(entry.name),
                  parsed != writerID else { continue }
            let path = RepoLayout.normalize(joining: [
                basePath, RepoLayout.watermelonDirectory, RepoLayout.livenessDirectory, entry.name
            ])
            // nil = confidently gone (404 under strong-consistency backend); peer drops out of all 3 sets.
            guard let status = try await classifyPeerHeartbeat(path: path, now: now, gracePeriodSec: gracePeriodSec) else {
                continue
            }
            switch status {
            case .active:
                active.insert(parsed)
            case .stale:
                stale.insert(parsed)
            case .unknown:
                unknown.insert(parsed)
            }
        }
        return ActiveWritersView(activePeerIDs: active, stalePeerIDs: stale, unknownPeerIDs: unknown)
    }

    /// Single retry on transient transport — without it, a momentary blip on one
    /// peer's file marks them inactive and sweep can delete their still-active staging.
    /// Cancellation must surface, not be swallowed.
    ///
    /// Returns `.unknown(.vanishedWithinGrace)` for 404s on backends with grace > 0,
    /// since post-write visibility lag can make a freshly-renewed peer's file 404 briefly.
    /// Returns nil (peer omitted) when grace = 0 and we got a confident 404 — that
    /// peer is genuinely gone.
    private func classifyPeerHeartbeat(
        path: String,
        now: Date,
        gracePeriodSec: TimeInterval
    ) async throws -> PeerStatus? {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("liveness-fetch-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }

        var downloaded = false
        var heartbeatVanished = false
        var lastDownloadError: Error?
        for attempt in 0..<2 {
            do {
                try await client.download(remotePath: path, localURL: temp)
                downloaded = true
                break
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if isStorageNotFoundError(error) {
                    heartbeatVanished = true
                    break
                }
                lastDownloadError = error
                if attempt == 0 {
                    try? await Task.sleep(for: .milliseconds(200))
                }
            }
        }
        if heartbeatVanished {
            // Under grace, 404 might mean "writer just renewed and we read pre-write state."
            if gracePeriodSec > 0 {
                livenessLog.warning("[Liveness] peer heartbeat at \(path, privacy: .public) 404 within \(gracePeriodSec, privacy: .public)s grace; marking unknown")
                return .unknown(reason: .vanishedWithinGrace)
            }
            return nil
        }
        if !downloaded {
            if let lastDownloadError {
                livenessLog.warning("[Liveness] peer heartbeat at \(path, privacy: .public) marked unknown after retry: \(lastDownloadError.localizedDescription, privacy: .public)")
            }
            return .unknown(reason: .readFailed)
        }
        let ts: Int64
        do {
            ts = try readHeartbeatTimestamp(from: temp, remotePath: path)
        } catch {
            livenessLog.warning("[Liveness] peer heartbeat at \(path, privacy: .public) marked unknown: parse failed (\(error.localizedDescription, privacy: .public))")
            return .unknown(reason: .readFailed)
        }
        if LivenessTracker.isStale(timestampMs: ts, now: now, gracePeriodSec: gracePeriodSec) {
            return .stale(lastSeenMs: ts)
        }
        return .active(lastSeenMs: ts)
    }

    private func readHeartbeatTimestamp(from url: URL, remotePath: String) throws -> Int64 {
        let data = try Data(contentsOf: url)
        guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let ts = (dict["ts"] as? Int64) ?? (dict["ts"] as? Int).map(Int64.init) else {
            throw NSError(domain: "LivenessTracker", code: -1, userInfo: [
                NSLocalizedDescriptionKey: "heartbeat at \(remotePath) is unreadable"
            ])
        }
        return ts
    }
}

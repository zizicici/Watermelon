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
        let body: [String: Any] = ["ts": Int64(Date().timeIntervalSince1970 * 1000)]
        guard let data = try? JSONSerialization.data(withJSONObject: body) else { return }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("liveness-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try data.write(to: temp, options: .atomic)
        } catch {
            return
        }
        let remotePath = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        // Staging + rename instead of delete-then-create: keeps a file at remotePath
        // throughout the swap. Peer's listOtherActiveWriters sweep gate can't see us
        // as inactive during the brief window between the old delete and the new create.
        let stagingPath = remotePath + ".staging-\(UUID().uuidString).tmp"
        var lastFailure: Error?
        var stagingCreated = false
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
            // S3 copy+delete failure can leave the bytes at remote after staging disappears.
            let stagingStillExists = (try? await client.metadata(path: stagingPath))?.isDirectory == false
            if !stagingStillExists, let lastFailure {
                livenessLog.warning("[Liveness] staging vanished after failed move for \(remotePath, privacy: .public): \(lastFailure.localizedDescription, privacy: .public)")
            }
            do {
                if try await existingHeartbeatIsActive(remotePath: remotePath) {
                    try? await client.delete(path: stagingPath)
                    return
                }
            } catch is CancellationError {
                try? await client.delete(path: stagingPath)
                return
            } catch {
                livenessLog.warning("[Liveness] heartbeat probe failed after failed move for \(remotePath, privacy: .public), continuing fallback write: \(error.localizedDescription, privacy: .public)")
            }
        }
        if Task.isCancelled {
            try? await client.delete(path: stagingPath)
            return
        }
        do {
            try await client.upload(localURL: temp, remotePath: remotePath, respectTaskCancellation: true, onProgress: nil)
            try? await client.delete(path: stagingPath)
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
        // Cancellation must surface here too so stopAndWait can unblock during shutdown.
        do {
            _ = try await client.atomicCreate(localURL: temp, remotePath: remotePath, respectTaskCancellation: true)
            try? await client.delete(path: stagingPath)
            return
        } catch is CancellationError {
            try? await client.delete(path: stagingPath)
            return
        } catch {
            lastFailure = error
        }
        if stagingCreated {
            try? await client.delete(path: stagingPath)
        }
        if let lastFailure {
            livenessLog.warning("[Liveness] all write strategies failed for \(remotePath, privacy: .public): \(lastFailure.localizedDescription, privacy: .public)")
        }
    }

    private func existingHeartbeatIsActive(remotePath: String) async throws -> Bool {
        do {
            guard let metadata = try await client.metadata(path: remotePath), !metadata.isDirectory else {
                return false
            }
        } catch {
            if isStorageNotFoundError(error) { return false }
            throw error
        }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("liveness-existing-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await client.download(remotePath: remotePath, localURL: temp)
        } catch {
            if isStorageNotFoundError(error) { return false }
            throw error
        }
        guard let data = try? Data(contentsOf: temp),
              let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let ts = (dict["ts"] as? Int64) ?? (dict["ts"] as? Int).map(Int64.init) else {
            return false
        }
        return !LivenessTracker.isStale(timestampMs: ts)
    }

    static func isStale(timestampMs: Int64, now: Date = Date()) -> Bool {
        let timestamp = Date(timeIntervalSince1970: TimeInterval(timestampMs) / 1000)
        let age = now.timeIntervalSince(timestamp)
        return age > staleThreshold
    }

    /// Throws on list infrastructure failure — caller (OrphanMetadataCleanup gate)
    /// MUST treat that as "can't determine activity" and skip sweep. Per-entry
    /// errors (single peer's file gone / unparseable) are logged + skipped: one bad
    /// peer file mustn't trigger self-DoS by aborting every sweep until manual
    /// cleanup. Fail-open is reserved for the list call itself; per-entry tolerance
    /// stays local to that entry.
    func listOtherActiveWriters() async throws -> [String] {
        guard !isLocalVolume else { return [] }
        let dir = RepoLayout.livenessDirectoryPath(base: basePath)
        let entries = try await client.list(path: dir)
        var active: [String] = []
        for entry in entries {
            guard !entry.isDirectory,
                  let parsed = RepoLayout.parseLivenessFilename(entry.name),
                  parsed != writerID else { continue }
            let path = RepoLayout.normalize(joining: [
                basePath, RepoLayout.watermelonDirectory, RepoLayout.livenessDirectory, entry.name
            ])
            let temp = FileManager.default.temporaryDirectory
                .appendingPathComponent("liveness-fetch-\(UUID().uuidString).json")
            defer { try? FileManager.default.removeItem(at: temp) }
            // Single retry to ride out transient transport — without it, a momentary
            // blip on one peer's file marks them inactive, and sweep can delete their
            // still-active staging. Cancellation must surface, not be swallowed.
            var downloaded = false
            for attempt in 0..<2 {
                do {
                    try await client.download(remotePath: path, localURL: temp)
                    downloaded = true
                    break
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    if attempt == 0 {
                        try? await Task.sleep(for: .milliseconds(200))
                    }
                }
            }
            if !downloaded { continue }
            guard let data = try? Data(contentsOf: temp),
                  let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let ts = (dict["ts"] as? Int64) ?? (dict["ts"] as? Int).map(Int64.init) else {
                // One bad peer file (schema drift / partial sync / external edit)
                // must not block sweep forever. Treat as inactive and continue.
                continue
            }
            if !LivenessTracker.isStale(timestampMs: ts) {
                active.append(parsed)
            }
        }
        return active
    }
}

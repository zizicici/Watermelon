import Foundation

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
        // Cancellation must surface so shutdown's stopAndWait can unblock past atomicCreate.
        do {
            _ = try await client.atomicCreate(
                localURL: temp,
                remotePath: stagingPath,
                respectTaskCancellation: true
            )
        } catch {
            try? await client.delete(path: stagingPath)
            return
        }
        do {
            try await client.move(from: stagingPath, to: remotePath)
            return
        } catch {}
        // S3 copy+delete failure leaves the bytes at remote; deleting it without an intact staging would wipe our liveness.
        let stagingStillExists = (try? await client.metadata(path: stagingPath))?.isDirectory == false
        guard stagingStillExists else { return }
        try? await client.delete(path: remotePath)
        do {
            try await client.move(from: stagingPath, to: remotePath)
            return
        } catch {}
        // Cancellation must surface here too so stopAndWait can unblock during shutdown.
        do {
            _ = try await client.atomicCreate(localURL: temp, remotePath: remotePath, respectTaskCancellation: true)
        } catch is CancellationError {
            try? await client.delete(path: stagingPath)
            return
        } catch {}
        try? await client.delete(path: stagingPath)
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

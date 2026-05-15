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
                if try await hasNewerLiveHeartbeat(remotePath: remotePath, timestampMs: timestampMs) {
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
        let heartbeatSize = (try? FileManager.default.attributesOfItem(atPath: temp.path)[.size] as? Int64) ?? 0
        guard client.atomicCreateGuarantee(forFileSize: heartbeatSize, remotePath: remotePath) == .exclusive else {
            try? await client.delete(path: stagingPath)
            if let lastFailure {
                livenessLog.warning("[Liveness] exclusive fallback unavailable after failed heartbeat write for \(remotePath, privacy: .public): \(lastFailure.localizedDescription, privacy: .public)")
            }
            return
        }
        do {
            if try await hasNewerLiveHeartbeat(remotePath: remotePath, timestampMs: timestampMs) {
                try? await client.delete(path: stagingPath)
                return
            }
        } catch is CancellationError {
            try? await client.delete(path: stagingPath)
            return
        } catch {
            livenessLog.warning("[Liveness] heartbeat probe failed before fallback atomic create for \(remotePath, privacy: .public), continuing exclusive create: \(error.localizedDescription, privacy: .public)")
        }
        // Cancellation must surface here too so stopAndWait can unblock during shutdown.
        do {
            let createResult = try await client.atomicCreate(localURL: temp, remotePath: remotePath, respectTaskCancellation: true)
            if case .alreadyExists = createResult {
                guard client.dataPathOverwriteRisk == .none else {
                    try? await client.delete(path: stagingPath)
                    return
                }
                let renewalTimestampMs = Int64(Date().timeIntervalSince1970 * 1000)
                do {
                    if try await hasNewerLiveHeartbeat(remotePath: remotePath, timestampMs: renewalTimestampMs) {
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

    private func hasNewerLiveHeartbeat(remotePath: String, timestampMs: Int64) async throws -> Bool {
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

    static func isStale(timestampMs: Int64, now: Date = Date()) -> Bool {
        let timestamp = Date(timeIntervalSince1970: TimeInterval(timestampMs) / 1000)
        let age = now.timeIntervalSince(timestamp)
        return age > staleThreshold
    }

    /// Throws when activity cannot be determined; caller skips orphan cleanup.
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
            if heartbeatVanished { continue }
            if !downloaded {
                throw heartbeatReadInconclusiveError(remotePath: path, underlying: lastDownloadError)
            }
            let ts = try readHeartbeatTimestamp(from: temp, remotePath: path)
            if !LivenessTracker.isStale(timestampMs: ts) {
                active.append(parsed)
            }
        }
        return active
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

    private func heartbeatReadInconclusiveError(remotePath: String, underlying: Error?) -> NSError {
        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: "heartbeat at \(remotePath) could not be downloaded"
        ]
        if let underlying {
            userInfo[NSUnderlyingErrorKey] = underlying
        }
        return NSError(domain: "LivenessTracker", code: -2, userInfo: userInfo)
    }
}

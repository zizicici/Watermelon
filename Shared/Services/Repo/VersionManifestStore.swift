import Foundation

nonisolated struct VersionManifest: Sendable {
    let formatVersion: Int
    let minAppVersion: String?
    let createdAtMs: Int64?
    let createdByWriter: String?
}

nonisolated struct VersionManifestStore: Sendable {
    enum Load: Sendable {
        case absent
        case found(VersionManifest)
    }

    enum WriteOutcome: Sendable {
        /// `MetadataCreateGate` SHA-confirmed our just-written bytes are at the remote path.
        case wroteVerifiedBytes
        /// Unverified writes require compatibility readback before trusting format.
        case requiresCompatibilityReadback
    }

    private static let postCreateReadRetryFloorSeconds: TimeInterval = 3

    let client: any RemoteStorageClientProtocol
    let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }


    /// `.absent` only on confirmed not-found; transport/read/parse errors throw so a
    /// transient 401/5xx cannot be silently downgraded to "no manifest, treat as fresh".
    func load() async throws -> Load {
        let path = RepoLayout.versionFilePath(base: basePath)
        guard let entry = try await metadataFileIfPresent(path: path) else {
            return .absent
        }
        if entry.isDirectory {
            throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 18,
                userInfo: [NSLocalizedDescriptionKey: "version manifest at \(path) is a directory"]
            ))
        }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("version-load-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await client.download(remotePath: path, localURL: temp)
        } catch {
            throw RemoteWriteClassifier.normalizedCancellation(error)
        }
        let data = try Data(contentsOf: temp)
        guard let wire = try? VersionManifestWire(data: data) else {
            throw RepoBootstrap.VersionConflict.unreadable(nil)
        }
        return .found(VersionManifest(
            formatVersion: wire.formatVersion,
            minAppVersion: wire.minAppVersion,
            createdAtMs: wire.createdAtMs,
            createdByWriter: wire.createdByWriter
        ))
    }

    /// Tolerant wrapper for diagnostics (e.g. surfacing `min_app_version` in an unsupported error).
    /// Any failure mode collapses to `nil`.
    func loadOrNil() async -> VersionManifest? {
        do {
            switch try await load() {
            case .absent: return nil
            case .found(let manifest): return manifest
            }
        } catch {
            return nil
        }
    }


    /// Pre-check + staged create + post-write compatibility verification, mirroring the
    /// pre-extraction ordering: existing remote metadata wins; only confirmed-absent
    /// proceeds to publish. Returns whether the caller still needs to readback-verify.
    @discardableResult
    func writeIfAbsent(writerID: String) async throws -> WriteOutcome {
        do {
            try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
            let versionPath = RepoLayout.versionFilePath(base: basePath)
            if let entry = try await metadataFileIfPresent(path: versionPath) {
                // Hard-fail directory-at-path before readback; otherwise the 3s retry collapses it to `.unreadable`.
                if entry.isDirectory {
                    throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                        domain: "RepoBootstrap",
                        code: 18,
                        userInfo: [NSLocalizedDescriptionKey: "version manifest at \(versionPath) is a directory"]
                    ))
                }
                try await verifyCompatibleWithRetries()
                return .requiresCompatibilityReadback
            }

            let createdAtMs = Int64(Date().timeIntervalSince1970 * 1000)
            let temp = try makeTempJSON(
                VersionManifestWire(
                    formatVersion: RepoLayout.formatVersion,
                    minAppVersion: RepoLayout.minAppVersionPlaceholder,
                    createdAtMs: createdAtMs,
                    createdByWriter: writerID
                ).encode(),
                prefix: "repo-version"
            )
            defer { try? FileManager.default.removeItem(at: temp) }
            let outcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
                client: client,
                localURL: temp,
                remotePath: versionPath,
                respectTaskCancellation: false,
                finalizationPolicy: .requireExclusiveMove
            )
            if outcome.verification == .verifiedLocalBytes {
                return .wroteVerifiedBytes
            }
            try await verifyCompatibleWithRetries()
            return .requiresCompatibilityReadback
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }


    func verifyCompatibleWithRetries() async throws {
        var lastUnreadable: RepoBootstrap.VersionConflict = .unreadable(nil)
        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: Self.postCreateReadRetryFloorSeconds)
        var attempt = 0
        while true {
            do {
                try await verifyCompatible()
                return
            } catch RepoBootstrap.VersionConflict.unreadable(let underlying) {
                if let underlying, RemoteWriteClassifier.isCancellation(underlying) { throw CancellationError() }
                lastUnreadable = .unreadable(underlying)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                throw error
            }
            guard Date() < deadline else { throw lastUnreadable }
            try await Task.sleep(for: .milliseconds(200 * (1 << min(attempt, 3))))
            attempt += 1
        }
    }

    func verifyCompatible() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("version-verify-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        do {
            try await client.download(remotePath: RepoLayout.versionFilePath(base: basePath), localURL: tempURL)
        } catch {
            // Surface read failures rather than overlaying V2 onto an unknown format.
            throw RepoBootstrap.VersionConflict.unreadable(error)
        }
        let data: Data
        do {
            data = try Data(contentsOf: tempURL)
        } catch {
            throw RepoBootstrap.VersionConflict.unreadable(error)
        }
        guard let wire = try? VersionManifestWire(data: data) else {
            throw RepoBootstrap.VersionConflict.unreadable(nil)
        }
        try Self.classify(remoteFormat: wire.formatVersion, minAppVersion: wire.minAppVersion)
    }

    /// Shared classifier so the bootstrap-side and inspect-side arms can never drift
    /// on missing / out-of-range `formatVersion`. Throws the canonical `VersionConflict`.
    static func classify(remoteFormat: Int, minAppVersion: String?) throws {
        if remoteFormat > RepoLayout.currentSupportedFormatVersion {
            throw RepoBootstrap.VersionConflict.higherFormatVersion(
                remote: remoteFormat,
                local: RepoLayout.currentSupportedFormatVersion,
                minAppVersion: minAppVersion
            )
        }
        // V2 wire schema is additive; lower bound stops V1 from getting silently overlaid.
        if remoteFormat < 2 {
            throw RepoBootstrap.VersionConflict.mismatchedFormatVersion(
                remote: remoteFormat,
                local: RepoLayout.formatVersion,
                minAppVersion: minAppVersion
            )
        }
    }

    private func metadataFileIfPresent(path: String) async throws -> RemoteStorageEntry? {
        do {
            return try await client.metadata(path: path)
        } catch {
            if isStorageNotFoundError(error) { return nil }
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    // Local encode/write errors stay raw so callers don't conflate "our disk is full"
    // with the `BootstrapError.ioFailure → damagedV2Repo` mapping reserved for
    // malformed remote V2 metadata.
    private func makeTempJSON(_ data: Data, prefix: String) throws -> URL {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("\(prefix)-\(UUID().uuidString).json")
        try data.write(to: temp, options: .atomic)
        return temp
    }
}

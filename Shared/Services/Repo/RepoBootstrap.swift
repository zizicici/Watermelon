import Foundation
import os.log

private let bootstrapLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoBootstrap")

/// Finalized identity supersedes repo.json so wipe-and-reuse can converge.
actor RepoBootstrap {
    enum BootstrapError: Error {
        case ioFailure(Error)
        case futureFormatVersion(minAppVersion: String?)
    }

    private static let postCreateReadRetryFloorSeconds: TimeInterval = 3

    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let claims: IdentityClaimStore

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
        self.claims = IdentityClaimStore(client: client, basePath: basePath)
    }

    @discardableResult
    func initializeFreshRepo(writerID: String) async throws -> String {
        // Identity / repo.json BEFORE version.json: crash between leaves the repo at
        // (marker + no version.json) → inspect falls through to `.fresh`, retry path
        // adopts our existing claim and continues. Reverse order produces
        // (version.json present + no claim) → sync routes to `.v2` and immediately
        // throws "missing repo.json" since no claim/identity exists.
        let suggested = UUID().uuidString.lowercased()
        var resolvedID = try await ensureRepoJSON(repoID: suggested, writerID: writerID)
        try await ensureSubdirectories()
        resolvedID = try await ensureIdentityFinalization(repoID: resolvedID, writerID: writerID)
        try await VersionManifestStore(client: client, basePath: basePath).writeIfAbsent(writerID: writerID)
        return try await ensureRepoJSON(repoID: resolvedID, writerID: writerID)
    }

    /// Thin compatibility wrapper for callers/tests that still target the legacy
    /// bootstrap entry point. Real IO lives in `VersionManifestStore`.
    func ensureVersionJSON(writerID: String) async throws {
        try await VersionManifestStore(client: client, basePath: basePath).writeIfAbsent(writerID: writerID)
    }

    // WebDAV/SMB/SFTP don't auto-create parents on PUT.
    func ensureSubdirectories() async throws {
        do {
            try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
            try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
            try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
            try await client.createDirectory(path: RepoLayout.identityDirectoryPath(base: basePath))
            try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    @discardableResult
    func ensureRepoJSON(repoID: String, writerID: String) async throws -> String {
        let requestedRepoID = try Self.canonicalRepoID(repoID, code: 20)
        do {
            try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
            try await client.createDirectory(path: RepoLayout.identityDirectoryPath(base: basePath))

            let firstFinalizedRepoID = try await loadFinalizedRepoID()

            let bootstrapForLegacy = self
            let election = try await claims.runOwnClaimElection(
                requestedRepoID: requestedRepoID,
                writerID: writerID,
                firstFinalizedRepoID: firstFinalizedRepoID,
                loadLegacyCacheRepoID: {
                    switch try await bootstrapForLegacy.loadRepoJSONStrictForElection() {
                    case .absent: return nil
                    case .found(let id): return id
                    }
                }
            )

            // Peer-finalize observation must precede any post-write claim re-read.
            let refreshedFinalizedRepoID = try await loadFinalizedRepoID()
            var canonical: String
            if let refreshedFinalizedRepoID {
                canonical = refreshedFinalizedRepoID
            } else {
                canonical = try await claims.canonicalRepoID() ?? election.suggested
                // Re-poll on fresh election; a concurrent first writer could publish a lower lex-min claim right after ours.
                if election.isElectingFresh {
                    canonical = try await claims.stabilizeFreshElection(initial: canonical)
                }
            }

            // Stale-claim hazard: if a peer's claim later disappears, our claim becomes canonical again and would flip us back to `suggested`.
            if canonical != election.suggested {
                try await claims.writeOwnClaim(repoID: canonical, writerID: writerID, createdAtMs: election.createdAtMs)
            }

            // repo.json is a read-cache; finalized identity/claims are authoritative.
            do {
                try await writeRepoJSONCache(canonical: canonical, writerID: writerID, createdAtMs: election.createdAtMs)
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                bootstrapLog.warning("[RepoBootstrap] repo.json cache write failed: \(error.localizedDescription, privacy: .public)")
            }
            return canonical
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    @discardableResult
    func ensureIdentityFinalization(repoID: String, writerID: String) async throws -> String {
        let canonicalRepoID = try Self.canonicalRepoID(repoID, code: 21)
        do {
            try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
            if let finalized = try await loadFinalizedRepoID() {
                return finalized
            }

            let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
            let createdAtMs = Int64(Date().timeIntervalSince1970 * 1000)
            let temp = try makeTempJSON(
                RepoIdentityFinalizationWire(
                    repoID: canonicalRepoID,
                    formatVersion: RepoLayout.formatVersion,
                    createdAtMs: createdAtMs,
                    createdByWriter: writerID
                ).encode(),
                prefix: "repo-identity-final"
            )
            defer { try? FileManager.default.removeItem(at: temp) }

            let outcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
                client: client,
                localURL: temp,
                remotePath: markerPath,
                respectTaskCancellation: false,
                finalizationPolicy: .requireExclusiveMove
            )
            // Gate already SHA-confirmed the remote bytes match `temp`, so `repoID`
            // is the canonical finalized id — skip a redundant readback loop.
            if outcome.verification == .verifiedLocalBytes {
                return canonicalRepoID
            }
            if let finalized = try await loadFinalizedRepoIDWithRetries() {
                return finalized
            }
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 11,
                userInfo: [NSLocalizedDescriptionKey: "repo identity finalization at \(markerPath) was written but no readable marker exists"]
            ))
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    func loadFinalizedRepoID() async throws -> String? {
        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        guard try await metadataFileIfPresent(path: markerPath, description: "repo identity finalization marker", code: 13) != nil else {
            return nil
        }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("repo-identity-final-read-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await client.download(remotePath: markerPath, localURL: temp)
            let data = try Data(contentsOf: temp)
            do {
                let wire = try RepoIdentityFinalizationWire(data: data)
                if let fv = wire.formatVersion, fv > RepoLayout.currentSupportedFormatVersion {
                    throw BootstrapError.futureFormatVersion(minAppVersion: nil)
                }
                return wire.repoID
            } catch let bootstrap as BootstrapError {
                throw bootstrap
            } catch {
                throw BootstrapError.ioFailure(NSError(
                    domain: "RepoBootstrap",
                    code: 12,
                    userInfo: [NSLocalizedDescriptionKey: "repo identity finalization marker at \(markerPath) is malformed"]
                ))
            }
        } catch let bootstrap as BootstrapError {
            throw bootstrap
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw BootstrapError.ioFailure(error)
        }
    }

    func loadRepoJSONStrictForElection() async throws -> RepoIDLoad {
        try await loadRepoJSONStrict()
    }

    private func loadRepoJSONStrict() async throws -> RepoIDLoad {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("repo-load-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        let path = RepoLayout.repoFilePath(base: basePath)
        guard try await metadataFileIfPresent(path: path, description: "repo identity cache", code: 14) != nil else {
            return .absent
        }
        do {
            try await client.download(remotePath: path, localURL: temp)
            let data = try Data(contentsOf: temp)
            do {
                return .found(try RepoCacheWire(data: data).repoID)
            } catch {
                throw BootstrapError.ioFailure(NSError(domain: "RepoBootstrap", code: 1, userInfo: [NSLocalizedDescriptionKey: "repo.json malformed"]))
            }
        } catch let bootstrap as BootstrapError {
            throw bootstrap
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw BootstrapError.ioFailure(error)
        }
    }

    private func writeRepoJSONCache(canonical: String, writerID: String, createdAtMs: Int64) async throws {
        let temp = try makeTempJSON(
            RepoCacheWire(repoID: canonical, createdAtMs: createdAtMs, createdByWriter: writerID).encode(),
            prefix: "repo-bootstrap"
        )
        defer { try? FileManager.default.removeItem(at: temp) }
        let storageClient = client
        let remotePath = RepoLayout.repoFilePath(base: basePath)
        _ = try await Task { @Sendable () throws -> AtomicCreateResult in
            try await storageClient.atomicCreate(
                localURL: temp,
                remotePath: remotePath,
                respectTaskCancellation: false
            )
        }.value
    }

    private func loadFinalizedRepoIDWithRetries() async throws -> String? {
        var lastError: Error?
        let deadline = postCreateReadRetryDeadline()
        var attempt = 0
        while true {
            do {
                if let finalized = try await loadFinalizedRepoID() {
                    return finalized
                }
                lastError = nil
            } catch is CancellationError {
                throw CancellationError()
            } catch let error where RemoteWriteClassifier.isCancellation(error) {
                throw CancellationError()
            } catch {
                lastError = error
            }
            guard Date() < deadline else {
                if let lastError { throw lastError }
                return nil
            }
            try await sleepBeforePostCreateReadRetry(attempt: attempt)
            attempt += 1
        }
    }

    private func postCreateReadRetryDeadline() -> Date {
        client.metadataReadAfterWriteDeadline(floorSeconds: Self.postCreateReadRetryFloorSeconds)
    }

    private func sleepBeforePostCreateReadRetry(attempt: Int) async throws {
        try await Task.sleep(for: .milliseconds(200 * (1 << min(attempt, 3))))
    }

    private func metadataFileIfPresent(path: String, description: String, code: Int) async throws -> RemoteStorageEntry? {
        let metadata: RemoteStorageEntry?
        do {
            metadata = try await client.metadata(path: path)
        } catch {
            if isStorageNotFoundError(error) { return nil }
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
        guard let metadata else { return nil }
        guard !metadata.isDirectory else {
            throw malformedMetadataDirectoryError(path: path, description: description, code: code)
        }
        return metadata
    }

    private func malformedMetadataDirectoryError(path: String, description: String, code: Int) -> BootstrapError {
        BootstrapError.ioFailure(NSError(
            domain: "RepoBootstrap",
            code: code,
            userInfo: [NSLocalizedDescriptionKey: "\(description) at \(path) is a directory"]
        ))
    }

    private static func canonicalRepoID(_ raw: String, code: Int) throws -> String {
        do {
            return try RepoWireValidator.validateRepoID(raw, field: "repoID")
        } catch {
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: code,
                userInfo: [NSLocalizedDescriptionKey: "repoID is malformed"]
            ))
        }
    }

    enum VersionConflict: Error {
        case higherFormatVersion(remote: Int, local: Int, minAppVersion: String?)
        case unreadable(Error?)
        case mismatchedFormatVersion(remote: Int, local: Int, minAppVersion: String?)
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

    enum RepoIDLoad: Sendable {
        case absent
        case found(String)
    }

    /// Finalized identity is authoritative; claims remain the pre-finalization election.
    func loadRepoIDStrict() async throws -> RepoIDLoad {
        if let finalized = try await loadFinalizedRepoID() {
            return .found(finalized)
        }
        if let canonical = try await claims.canonicalRepoID() {
            return .found(canonical)
        }
        return try await loadRepoJSONStrict()
    }

    func loadRepoID() async throws -> String? {
        switch try await loadRepoIDStrict() {
        case .absent: return nil
        case .found(let id): return id
        }
    }
}

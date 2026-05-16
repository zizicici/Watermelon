import Foundation
import os.log

private let bootstrapLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoBootstrap")

/// Finalized identity lives at `.watermelon/repo-identity.json`.
/// Before finalization, `IdentityClaimStore` elects lex-min `(created_at_ms, writerID)`.
/// `repo.json` is a legacy/cache pointer.
actor RepoBootstrap {
    enum BootstrapError: Error {
        case ioFailure(Error)
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
        try await ensureVersionJSON(writerID: writerID)
        return try await ensureRepoJSON(repoID: resolvedID, writerID: writerID)
    }

    // WebDAV/SMB/SFTP don't auto-create parents on PUT.
    func ensureSubdirectories() async throws {
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.identityDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
    }

    @discardableResult
    func ensureRepoJSON(repoID: String, writerID: String) async throws -> String {
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        try await client.createDirectory(path: RepoLayout.identityDirectoryPath(base: basePath))

        let finalizedRepoID = try await loadFinalizedRepoID()

        // Non-empty self claim might be canonical; only 0-byte is safe to overwrite.
        try await claims.healZeroByteSelfClaim(writerID: writerID)

        let claimElection: ClaimElectionResult
        if finalizedRepoID == nil {
            claimElection = try await claims.canonicalElection(ignoringCorruptSelfClaimFor: writerID)
        } else {
            claimElection = ClaimElectionResult(repoID: nil, ignoredSelfCorrupt: false)
        }
        let existingCanonical = claimElection.repoID
        let suggested: String
        let isElectingFresh: Bool
        if let finalizedRepoID {
            suggested = finalizedRepoID
            isElectingFresh = false
        } else if let existingCanonical {
            suggested = existingCanonical
            isElectingFresh = false
        } else if case .found(let legacyID) = try await loadRepoJSONStrict() {
            suggested = legacyID
            isElectingFresh = false
        } else if claimElection.ignoredSelfCorrupt {
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 10,
                userInfo: [NSLocalizedDescriptionKey: "own identity claim is corrupt and no trusted repo ID exists; inspect/delete manually"]
            ))
        } else {
            suggested = repoID
            isElectingFresh = true
        }

        let createdAtMs = Int64(Date().timeIntervalSince1970 * 1000)
        try await claims.writeOwnClaim(repoID: suggested, writerID: writerID, createdAtMs: createdAtMs)

        let refreshedFinalizedRepoID = try await loadFinalizedRepoID()
        var canonical: String
        if let refreshedFinalizedRepoID {
            canonical = refreshedFinalizedRepoID
        } else {
            canonical = try await claims.canonicalRepoID() ?? suggested
            // Re-poll on fresh election; a concurrent first writer could publish a lower lex-min claim right after ours.
            if isElectingFresh {
                canonical = try await claims.stabilizeFreshElection(initial: canonical)
            }
        }

        // Stale-claim hazard: if a peer's claim later disappears, our claim becomes canonical again and would flip us back to `suggested`.
        if canonical != suggested {
            try await claims.writeOwnClaim(repoID: canonical, writerID: writerID, createdAtMs: createdAtMs)
        }

        // repo.json is a read-cache; finalized identity/claims are authoritative.
        do {
            try await writeRepoJSONCache(canonical: canonical, writerID: writerID, createdAtMs: createdAtMs)
        } catch {
            bootstrapLog.warning("[RepoBootstrap] repo.json cache write failed: \(error.localizedDescription, privacy: .public)")
        }
        return canonical
    }

    @discardableResult
    func ensureIdentityFinalization(repoID: String, writerID: String) async throws -> String {
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))
        if let finalized = try await loadFinalizedRepoID() {
            return finalized
        }

        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        let createdAtMs = Int64(Date().timeIntervalSince1970 * 1000)
        let dict: [String: Any] = [
            "v": 1,
            "repo_id": repoID,
            "format_version": RepoLayout.formatVersion,
            "created_at_ms": createdAtMs,
            "created_by_writer": writerID
        ]
        let temp = try makeTempJSON(dict: dict, prefix: "repo-identity-final")
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
        if outcome.verifiedAgainstLocalContent {
            return repoID
        }
        if let finalized = try await loadFinalizedRepoIDWithRetries() {
            return finalized
        }
        throw BootstrapError.ioFailure(NSError(
            domain: "RepoBootstrap",
            code: 11,
            userInfo: [NSLocalizedDescriptionKey: "repo identity finalization at \(markerPath) was written but no readable marker exists"]
        ))
    }

    func loadFinalizedRepoID() async throws -> String? {
        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        guard try await metadataFileIfPresent(path: markerPath, description: "repo identity finalization marker", code: 13) != nil else {
            return nil
        }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("repo-identity-final-read-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: markerPath, localURL: temp)
        let data = try Data(contentsOf: temp)
        guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let repoID = dict["repo_id"] as? String, !repoID.isEmpty else {
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 12,
                userInfo: [NSLocalizedDescriptionKey: "repo identity finalization marker at \(markerPath) is malformed"]
            ))
        }
        return repoID
    }

    private func loadRepoJSONStrict() async throws -> RepoIDLoad {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("repo-load-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        let path = RepoLayout.repoFilePath(base: basePath)
        guard try await metadataFileIfPresent(path: path, description: "repo identity cache", code: 14) != nil else {
            return .absent
        }
        try await client.download(remotePath: path, localURL: temp)
        let data = try Data(contentsOf: temp)
        guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let id = dict["repo_id"] as? String, !id.isEmpty else {
            throw BootstrapError.ioFailure(NSError(domain: "RepoBootstrap", code: 1, userInfo: [NSLocalizedDescriptionKey: "repo.json malformed"]))
        }
        return .found(id)
    }

    private func writeRepoJSONCache(canonical: String, writerID: String, createdAtMs: Int64) async throws {
        let repoDict: [String: Any] = [
            "v": 1,
            "repo_id": canonical,
            "created_at_ms": createdAtMs,
            "created_by_writer": writerID
        ]
        let temp = try makeTempJSON(dict: repoDict, prefix: "repo-bootstrap")
        defer { try? FileManager.default.removeItem(at: temp) }
        _ = try await client.atomicCreate(
            localURL: temp,
            remotePath: RepoLayout.repoFilePath(base: basePath),
            respectTaskCancellation: false
        )
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

    enum VersionConflict: Error {
        case higherFormatVersion(remote: Int, local: Int, minAppVersion: String?)
        case unreadable(Error?)
        case mismatchedFormatVersion(remote: Int, local: Int, minAppVersion: String?)
    }

    func ensureVersionJSON(writerID: String) async throws {
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))

        let versionPath = RepoLayout.versionFilePath(base: basePath)
        // Existing version metadata wins; creation only starts after a confirmed absent precheck.
        if try await metadataFileIfPresent(path: versionPath, description: "version manifest", code: 18) != nil {
            try await verifyVersionCompatibleWithRetries()
            return
        }

        let createdAtMs = Int64(Date().timeIntervalSince1970 * 1000)
        let versionDict: [String: Any] = [
            "format_version": RepoLayout.formatVersion,
            "min_app_version": RepoLayout.minAppVersionPlaceholder,
            "created_at_ms": createdAtMs,
            "created_by_writer": writerID
        ]
        let temp = try makeTempJSON(dict: versionDict, prefix: "repo-version")
        defer { try? FileManager.default.removeItem(at: temp) }
        let outcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
            client: client,
            localURL: temp,
            remotePath: versionPath,
            respectTaskCancellation: false,
            finalizationPolicy: .requireExclusiveMove
        )
        // Verified bytes are our just-written `format_version`, so it is compatible
        // by construction; skip the readback compatibility loop.
        if outcome.verifiedAgainstLocalContent {
            return
        }
        try await verifyVersionCompatibleWithRetries()
    }

    private func verifyVersionCompatibleWithRetries() async throws {
        var lastUnreadable: VersionConflict = .unreadable(nil)
        let deadline = postCreateReadRetryDeadline()
        var attempt = 0
        while true {
            do {
                try await verifyVersionCompatible()
                return
            } catch VersionConflict.unreadable(let underlying) {
                if underlying is CancellationError {
                    throw CancellationError()
                }
                lastUnreadable = .unreadable(underlying)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                throw error
            }
            guard Date() < deadline else { throw lastUnreadable }
            try await sleepBeforePostCreateReadRetry(attempt: attempt)
            attempt += 1
        }
    }

    private func verifyVersionCompatible() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("repo-version-verify-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        do {
            try await client.download(remotePath: RepoLayout.versionFilePath(base: basePath), localURL: tempURL)
        } catch {
            // Read failure must surface; silently continuing risks overlaying V2 onto an
            // unknown format.
            throw VersionConflict.unreadable(error)
        }
        let data: Data
        do {
            data = try Data(contentsOf: tempURL)
        } catch {
            throw VersionConflict.unreadable(error)
        }
        guard let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let remoteFormat = dict["format_version"] as? Int else {
            throw VersionConflict.unreadable(nil)
        }
        let minApp = dict["min_app_version"] as? String
        if remoteFormat > RepoLayout.currentSupportedFormatVersion {
            throw VersionConflict.higherFormatVersion(remote: remoteFormat, local: RepoLayout.currentSupportedFormatVersion, minAppVersion: minApp)
        }
        // V2 wire schema is additive: optional fields (stamps, observedBasis)
        // round-trip through any v2 reader / writer. The lower bound stops V1
        // from getting silently overlaid.
        if remoteFormat < 2 {
            throw VersionConflict.mismatchedFormatVersion(remote: remoteFormat, local: RepoLayout.formatVersion, minAppVersion: minApp)
        }
    }

    private func makeTempJSON(dict: [String: Any], prefix: String) throws -> URL {
        let data: Data
        do {
            data = try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys, .prettyPrinted])
        } catch {
            throw BootstrapError.ioFailure(error)
        }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("\(prefix)-\(UUID().uuidString).json")
        do {
            try data.write(to: temp, options: .atomic)
        } catch {
            throw BootstrapError.ioFailure(error)
        }
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

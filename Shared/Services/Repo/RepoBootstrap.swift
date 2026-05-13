import Foundation
import os.log

private let bootstrapLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoBootstrap")

/// Identity = writer-unique claim file at `.watermelon/identity/<wid>.json`.
/// Canonical repoID = lex-min `(created_at_ms, writerID)` over all claims.
/// Race-free because each writer's claim is exclusive and lex-min is
/// deterministic. `repo.json` is a legacy/cache pointer; claims dir wins.
actor RepoBootstrap {
    enum BootstrapError: Error {
        case ioFailure(Error)
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    @discardableResult
    func initializeFreshRepo(writerID: String) async throws -> String {
        // Identity / repo.json BEFORE version.json: crash between leaves the repo at
        // (marker + no version.json) → inspect falls through to `.fresh`, retry path
        // adopts our existing claim and continues. Reverse order produces
        // (version.json present + no claim) → sync routes to `.v2` and immediately
        // throws "missing repo.json" since no claim/identity exists.
        let suggested = UUID().uuidString.lowercased()
        let resolvedID = try await ensureRepoJSON(repoID: suggested, writerID: writerID)
        try await ensureSubdirectories()
        try await ensureVersionJSON(writerID: writerID)
        return resolvedID
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

        // Non-empty self claim might be canonical; only 0-byte is safe to overwrite.
        try await healZeroByteSelfClaim(writerID: writerID)

        let existingCanonical = try await canonicalRepoIDFromClaims()
        let suggested: String
        let isElectingFresh: Bool
        if let existingCanonical {
            suggested = existingCanonical
            isElectingFresh = false
        } else if case .found(let legacyID) = try await loadRepoJSONStrict() {
            suggested = legacyID
            isElectingFresh = false
        } else {
            suggested = repoID
            isElectingFresh = true
        }

        let createdAtMs = Int64(Date().timeIntervalSince1970 * 1000)
        try await writeIdentityClaim(repoID: suggested, writerID: writerID, createdAtMs: createdAtMs)

        var canonical = try await canonicalRepoIDFromClaims() ?? suggested
        // Re-poll on fresh election; a concurrent first writer could publish a lower lex-min claim right after ours.
        if isElectingFresh {
            canonical = try await stabilizeFreshElection(initial: canonical)
        }

        // repo.json is a read-cache; claims/ is authoritative. Log so a silent fail
        // doesn't surface later as "V2 repo missing repo.json".
        do {
            try await writeRepoJSONCache(canonical: canonical, writerID: writerID, createdAtMs: createdAtMs)
        } catch {
            bootstrapLog.warning("[RepoBootstrap] repo.json cache write failed: \(error.localizedDescription, privacy: .public)")
        }
        return canonical
    }

    private func loadRepoJSONStrict() async throws -> RepoIDLoad {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("repo-load-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        let path = RepoLayout.repoFilePath(base: basePath)
        guard let metadata = try await client.metadata(path: path), !metadata.isDirectory else {
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

    private func writeIdentityClaim(repoID: String, writerID: String, createdAtMs: Int64) async throws {
        let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
        if let meta = try await client.metadata(path: claimPath), !meta.isDirectory {
            switch try await classifyExistingClaim(claimPath: claimPath, writerID: writerID, suggestedRepoID: repoID) {
            case .ours:
                return
            case .zeroByte, .staleRepoID:
                try await client.delete(path: claimPath)
            }
        }
        let dict: [String: Any] = [
            "v": 1,
            "repo_id": repoID,
            "created_at_ms": createdAtMs,
            "writer_id": writerID
        ]
        let temp = try makeTempJSON(dict: dict, prefix: "repo-identity-claim")
        defer { try? FileManager.default.removeItem(at: temp) }
        let result = try await client.atomicCreate(localURL: temp, remotePath: claimPath, respectTaskCancellation: false)
        // Verify our bytes landed. `.bestEffortRetry` on .overwritePossible
        // backends signals "exists+upload could have raced"; we MUST read back
        // and compare content — if it's not ours, identity got hijacked.
        try await verifyIdentityClaim(repoID: repoID, writerID: writerID, createdAtMs: createdAtMs, claimPath: claimPath, atomicResult: result)
    }

    /// Only 0-byte (atomicCreate half-failed) is safe to clear; any other content might be canonical.
    private func healZeroByteSelfClaim(writerID: String) async throws {
        let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
        guard let meta = try await client.metadata(path: claimPath), !meta.isDirectory else { return }
        if meta.size > 0 { return }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("self-claim-preflight-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: claimPath, localURL: temp)
        let data = (try? Data(contentsOf: temp)) ?? Data()
        guard data.isEmpty else { return }
        try await client.delete(path: claimPath)
    }

    private func stabilizeFreshElection(initial: String) async throws -> String {
        var current = initial
        let maxRounds = 6
        let interval: Duration = .milliseconds(250)
        for _ in 0..<maxRounds {
            try? await Task.sleep(for: interval)
            let observed = try await canonicalRepoIDFromClaims() ?? current
            if observed == current { return current }
            current = observed
        }
        return current
    }

    private enum ExistingClaimClassification {
        case ours
        case zeroByte
        case staleRepoID
    }

    private func classifyExistingClaim(claimPath: String, writerID: String, suggestedRepoID: String) async throws -> ExistingClaimClassification {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("claim-precheck-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: claimPath, localURL: temp)
        let data = (try? Data(contentsOf: temp)) ?? Data()
        if data.isEmpty { return .zeroByte }
        guard let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let landedWriterID = dict["writer_id"] as? String,
              landedWriterID == writerID,
              let landedRepoID = dict["repo_id"] as? String, !landedRepoID.isEmpty else {
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 8,
                userInfo: [NSLocalizedDescriptionKey:
                    "claim at \(claimPath) corrupted or carries a foreign writerID — refusing to auto-overwrite (delete manually if you're sure)"]
            ))
        }
        // A stale self-claim can become canonical again after the peer claim that superseded it vanishes.
        if landedRepoID != suggestedRepoID {
            return .staleRepoID
        }
        return .ours
    }

    private func verifyIdentityClaim(
        repoID: String,
        writerID: String,
        createdAtMs: Int64,
        claimPath: String,
        atomicResult: AtomicCreateResult
    ) async throws {
        guard let meta = try await client.metadata(path: claimPath), !meta.isDirectory else {
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: "identity claim atomicCreate reported \(atomicResult) but file not readable at \(claimPath)"]
            ))
        }
        // For .bestEffortRetry we don't trust that our bytes are at the path —
        // a peer could have raced exists+upload. Read back; if the content
        // says a different repoID/writerID/ts, our identity is unanchored.
        guard atomicResult == .bestEffortRetry else { return }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("claim-verify-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: claimPath, localURL: temp)
        let data = try Data(contentsOf: temp)
        guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let landedRepoID = dict["repo_id"] as? String,
              let landedWriterID = dict["writer_id"] as? String,
              let landedTs = (dict["created_at_ms"] as? Int64) ?? (dict["created_at_ms"] as? Int).map(Int64.init) else {
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 4,
                userInfo: [NSLocalizedDescriptionKey: "identity claim at \(claimPath) unparseable after write"]
            ))
        }
        guard landedRepoID == repoID, landedWriterID == writerID, landedTs == createdAtMs else {
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 5,
                userInfo: [NSLocalizedDescriptionKey: "identity claim content drift at \(claimPath): expected (\(writerID), \(createdAtMs)) got (\(landedWriterID), \(landedTs))"]
            ))
        }
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

    private struct IdentityClaim {
        let repoID: String
        let writerID: String
        let createdAtMs: Int64
    }

    /// Fail-closed: any read failure or unparseable claim throws. A single
    /// transient blip or one corrupt claim must NOT silently flip canonical
    /// to a stale repo.json or a different writer.
    private func canonicalRepoIDFromClaims() async throws -> String? {
        let dir = RepoLayout.identityDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            // "Directory doesn't exist" = no claims yet (legitimate pre-claims
            // state). Anything else is unreadable identity → propagate.
            if isNotFoundError(error) { return nil }
            throw error
        }
        let claimEntries = entries.filter { !$0.isDirectory && $0.name.hasSuffix(".json") }
        // .serialOnly backends (SMB/SFTP via raw client here) violate single-connection
        // contract if we fan out via TaskGroup. Sequential fallback for those.
        var claims: [IdentityClaim] = []
        if client.concurrencyMode == .serialOnly {
            for entry in claimEntries {
                if let claim = try await fetchClaim(dir: dir, entryName: entry.name) {
                    claims.append(claim)
                }
            }
        } else {
            claims = try await withThrowingTaskGroup(of: IdentityClaim?.self) { group in
                for entry in claimEntries {
                    let entryName = entry.name
                    group.addTask { [self] in
                        try await fetchClaim(dir: dir, entryName: entryName)
                    }
                }
                var collected: [IdentityClaim] = []
                for try await result in group {
                    if let claim = result {
                        collected.append(claim)
                    }
                }
                return collected
            }
        }
        guard !claims.isEmpty else { return nil }
        // writerID tiebreak makes ms-collision deterministic.
        let canonical = claims.min { lhs, rhs in
            if lhs.createdAtMs != rhs.createdAtMs { return lhs.createdAtMs < rhs.createdAtMs }
            return lhs.writerID < rhs.writerID
        }!
        return canonical.repoID
    }

    private func fetchClaim(dir: String, entryName: String) async throws -> IdentityClaim? {
        let path = RepoLayout.normalize(joining: [dir, entryName])
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("claim-fetch-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await downloadWithRetry(remotePath: path, localURL: temp, attempts: 3)
        } catch {
            if isNotFoundError(error) { return nil }
            throw error
        }
        let data = (try? Data(contentsOf: temp)) ?? Data()
        let parsed = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        // Filename must be a `<writerID>.json` shape; payload's writer_id must match —
        // otherwise a stray .json with a forged older timestamp could win election.
        let expectedWriterID = entryName.hasSuffix(".json") ? String(entryName.dropLast(5)) : entryName
        if let dict = parsed,
           let id = dict["repo_id"] as? String, !id.isEmpty,
           let wid = dict["writer_id"] as? String, !wid.isEmpty, wid == expectedWriterID,
           RepoLayout.isValidWriterID(wid),
           let ts = (dict["created_at_ms"] as? Int64) ?? (dict["created_at_ms"] as? Int).map(Int64.init) {
            return IdentityClaim(repoID: id, writerID: wid, createdAtMs: ts)
        }
        // A writerID-shaped filename could have been canonical (lex-min); quarantining would silently flip the adopted repoID.
        if RepoLayout.isValidWriterID(expectedWriterID) {
            throw BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 9,
                userInfo: [NSLocalizedDescriptionKey:
                    "identity claim \(entryName) unparseable — refusing election to avoid silent canonical flip (inspect/delete manually)"]
            ))
        }
        // Quarantine suffix is `.bad.<ts>` (no `.json`) so the next election doesn't
        // re-scan the file, fail-parse again, and re-quarantine in a loop.
        bootstrapLog.error("[RepoBootstrap] non-claim .json \(entryName, privacy: .public); quarantining")
        let quarantineName = entryName + ".bad.\(Int64(Date().timeIntervalSince1970 * 1000))"
        let quarantinePath = RepoLayout.normalize(joining: [dir, quarantineName])
        try? await client.move(from: path, to: quarantinePath)
        return nil
    }

    private func downloadWithRetry(remotePath: String, localURL: URL, attempts: Int) async throws {
        var lastError: Error?
        for attempt in 0..<attempts {
            do {
                try await client.download(remotePath: remotePath, localURL: localURL)
                return
            } catch {
                lastError = error
                if isNotFoundError(error) { throw error }  // 404 isn't transient
                if attempt + 1 < attempts {
                    try await Task.sleep(for: .milliseconds(200 * (1 << attempt)))
                }
            }
        }
        throw lastError ?? NSError(domain: "RepoBootstrap", code: 7, userInfo: [NSLocalizedDescriptionKey: "download retries exhausted at \(remotePath)"])
    }

    private func isNotFoundError(_ error: Error) -> Bool {
        isStorageNotFoundError(error)
    }

    enum VersionConflict: Error {
        case higherFormatVersion(remote: Int, local: Int, minAppVersion: String?)
        case unreadable(Error?)
        case mismatchedFormatVersion(remote: Int, local: Int, minAppVersion: String?)
    }

    func ensureVersionJSON(writerID: String) async throws {
        try await client.createDirectory(path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]))

        let versionPath = RepoLayout.versionFilePath(base: basePath)
        // atomicCreate on .overwritePossible backends would clobber a future-format peer.
        if let meta = try await client.metadata(path: versionPath), !meta.isDirectory {
            try await verifyVersionCompatible()
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
        let result = try await MetadataCreateGate.createWithStagingFallback(
            client: client,
            localURL: temp,
            remotePath: versionPath,
            respectTaskCancellation: false
        )
        switch result {
        case .created:
            return
        case .alreadyExists, .bestEffortRetry:
            try await verifyVersionCompatible()
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

    /// Claims dir is authoritative; repo.json is legacy fallback only.
    func loadRepoIDStrict() async throws -> RepoIDLoad {
        if let canonical = try await canonicalRepoIDFromClaims() {
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

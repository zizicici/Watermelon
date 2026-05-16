import Foundation
import os.log

private let identityClaimStoreLog = Logger(
    subsystem: "com.zizicici.watermelon",
    category: "IdentityClaimStore"
)

/// Owns `.watermelon/identity/<writerID>.json`: claim listing/parse, lex-min
/// election, zero-byte self-claim heal, classification of an existing self-claim,
/// own-claim write + post-write byte verification, and fresh-election stabilization.
/// Errors are thrown as `RepoBootstrap.BootstrapError.ioFailure(NSError(domain: "RepoBootstrap", code: ...))`
/// so external callers and tests observe the same shape as before the extraction.
nonisolated struct IdentityClaim: Sendable {
    let repoID: String
    let writerID: String
    let createdAtMs: Int64
}

nonisolated struct ClaimElectionResult: Sendable {
    let repoID: String?
    let ignoredSelfCorrupt: Bool
}

nonisolated enum ExistingClaimClassification: Sendable {
    case ours
    case zeroByte
    case staleRepoID
    /// Non-empty but unparseable / foreign writer_id at our exclusive path: write half-failed or stale bytes; reclaim by delete + rewrite.
    case corrupt
}

nonisolated struct IdentityClaimStore: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    // MARK: - Election

    /// Fail-closed: any read failure or unparseable claim throws. A single
    /// transient blip or one corrupt claim must NOT silently flip canonical
    /// to a stale repo.json or a different writer.
    func canonicalElection(ignoringCorruptSelfClaimFor selfWriterID: String? = nil) async throws -> ClaimElectionResult {
        let dir = RepoLayout.identityDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            // "Directory doesn't exist" = no claims yet (legitimate pre-claims
            // state). Anything else is unreadable identity → propagate.
            if isStorageNotFoundError(error) { return ClaimElectionResult(repoID: nil, ignoredSelfCorrupt: false) }
            throw error
        }
        if let malformedDirectory = entries.first(where: { $0.isDirectory && $0.name.hasSuffix(".json") }) {
            throw malformedMetadataDirectoryError(
                path: RepoLayout.normalize(joining: [dir, malformedDirectory.name]),
                description: "identity claim",
                code: 17
            )
        }
        let claimEntries = entries.filter { !$0.isDirectory && $0.name.hasSuffix(".json") }
        var claims: [IdentityClaim] = []
        var ignoredSelfCorrupt = false
        // .serialOnly backends (SMB/SFTP raw client) violate single-connection contract if we fan out via TaskGroup.
        if client.concurrencyMode == .serialOnly {
            for entry in claimEntries {
                switch try await fetchClaim(dir: dir, entryName: entry.name, selfWriterID: selfWriterID) {
                case .claim(let claim):
                    claims.append(claim)
                case .ignoredSelfCorrupt:
                    ignoredSelfCorrupt = true
                case .none:
                    break
                }
            }
        } else {
            let result = try await withThrowingTaskGroup(of: ClaimFetchResult.self) { group in
                for entry in claimEntries {
                    let entryName = entry.name
                    let store = self
                    group.addTask {
                        try await store.fetchClaim(dir: dir, entryName: entryName, selfWriterID: selfWriterID)
                    }
                }
                var collected: [IdentityClaim] = []
                var ignored = false
                for try await result in group {
                    switch result {
                    case .claim(let claim):
                        collected.append(claim)
                    case .ignoredSelfCorrupt:
                        ignored = true
                    case .none:
                        break
                    }
                }
                return (collected, ignored)
            }
            claims = result.0
            ignoredSelfCorrupt = result.1
        }
        guard !claims.isEmpty else { return ClaimElectionResult(repoID: nil, ignoredSelfCorrupt: ignoredSelfCorrupt) }
        // writerID tiebreak makes ms-collision deterministic.
        let canonical = claims.min { lhs, rhs in
            if lhs.createdAtMs != rhs.createdAtMs { return lhs.createdAtMs < rhs.createdAtMs }
            return lhs.writerID < rhs.writerID
        }!
        return ClaimElectionResult(repoID: canonical.repoID, ignoredSelfCorrupt: ignoredSelfCorrupt)
    }

    func canonicalRepoID() async throws -> String? {
        try await canonicalElection().repoID
    }

    // MARK: - Heal

    /// Only 0-byte (atomicCreate half-failed) is safe to clear; any other content might be canonical.
    func healZeroByteSelfClaim(writerID: String) async throws {
        let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
        guard let meta = try await metadataFileIfPresent(path: claimPath, description: "identity claim", code: 16) else { return }
        if meta.size > 0 { return }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("self-claim-preflight-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: claimPath, localURL: temp)
        // Surface local-read failures: silently treating them as empty would delete a non-zero remote claim under disk pressure.
        let data = try Data(contentsOf: temp)
        guard data.isEmpty else { return }
        try await client.delete(path: claimPath)
    }

    // MARK: - Classification

    func classifyExistingClaim(claimPath: String, writerID: String, suggestedRepoID: String) async throws -> ExistingClaimClassification {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("claim-precheck-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: claimPath, localURL: temp)
        // Surface local-read failures: silently classifying as zero-byte would delete and reclaim a healthy remote on disk pressure.
        let data = try Data(contentsOf: temp)
        if data.isEmpty { return .zeroByte }
        guard let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let landedWriterID = dict["writer_id"] as? String,
              landedWriterID == writerID,
              let landedRepoID = dict["repo_id"] as? String, !landedRepoID.isEmpty else {
            identityClaimStoreLog.warning("claim at \(claimPath, privacy: .public) corrupt or carries a foreign writerID — reclaiming")
            return .corrupt
        }
        if landedRepoID != suggestedRepoID {
            return .staleRepoID
        }
        return .ours
    }

    // MARK: - Write own claim

    func writeOwnClaim(repoID: String, writerID: String, createdAtMs: Int64) async throws {
        let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
        if try await metadataFileIfPresent(path: claimPath, description: "identity claim", code: 15) != nil {
            switch try await classifyExistingClaim(claimPath: claimPath, writerID: writerID, suggestedRepoID: repoID) {
            case .ours:
                return
            case .zeroByte, .staleRepoID, .corrupt:
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
        try await verifyOwnClaim(repoID: repoID, writerID: writerID, createdAtMs: createdAtMs, claimPath: claimPath, atomicResult: result)
    }

    // MARK: - Stabilize fresh election

    func stabilizeFreshElection(
        initial: String,
        maxRounds: Int = 6,
        interval: Duration = .milliseconds(250)
    ) async throws -> String {
        var lastRead: String?
        var stableReads = 0
        for _ in 0..<maxRounds {
            try await Task.sleep(for: interval)
            try Task.checkCancellation()
            do {
                let current = try await canonicalRepoID()
                if current == lastRead, let current {
                    stableReads += 1
                    if stableReads >= 1 { return current }
                } else {
                    lastRead = current
                    stableReads = 0
                }
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                lastRead = nil
                stableReads = 0
            }
        }
        return try await canonicalRepoID() ?? initial
    }

    // MARK: - Internals

    private enum ClaimFetchResult: Sendable {
        case claim(IdentityClaim)
        case ignoredSelfCorrupt
        case none
    }

    private func fetchClaim(dir: String, entryName: String, selfWriterID: String?) async throws -> ClaimFetchResult {
        let path = RepoLayout.normalize(joining: [dir, entryName])
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("claim-fetch-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await downloadWithRetry(remotePath: path, localURL: temp, attempts: 3)
        } catch {
            if isStorageNotFoundError(error) { return .none }
            throw error
        }
        let data = try Data(contentsOf: temp)
        let parsed = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        // Filename must be a `<writerID>.json` shape; payload's writer_id must match —
        // otherwise a stray .json with a forged older timestamp could win election.
        let expectedWriterID = entryName.hasSuffix(".json") ? String(entryName.dropLast(5)) : entryName
        if let dict = parsed,
           let id = dict["repo_id"] as? String, !id.isEmpty,
           let wid = dict["writer_id"] as? String, !wid.isEmpty, wid == expectedWriterID,
           RepoLayout.isValidWriterID(wid),
           let ts = Self.strictInt64(dict["created_at_ms"]) {
            return .claim(IdentityClaim(repoID: id, writerID: wid, createdAtMs: ts))
        }
        // A writerID-shaped filename could have been canonical (lex-min); quarantining would silently flip the adopted repoID.
        if RepoLayout.isValidWriterID(expectedWriterID) {
            if expectedWriterID == selfWriterID {
                identityClaimStoreLog.warning("own identity claim \(entryName, privacy: .public) is corrupt; will repair after repo ID is resolved")
                return .ignoredSelfCorrupt
            }
            throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 9,
                userInfo: [NSLocalizedDescriptionKey:
                    "identity claim \(entryName) unparseable — refusing election to avoid silent canonical flip (inspect/delete manually)"]
            ))
        }
        // Quarantine suffix is `.bad.<ts>` (no `.json`) so the next election doesn't re-scan, fail-parse, and re-quarantine in a loop.
        identityClaimStoreLog.error("non-claim .json \(entryName, privacy: .public); quarantining")
        let quarantineName = entryName + ".bad.\(Int64(Date().timeIntervalSince1970 * 1000))"
        let quarantinePath = RepoLayout.normalize(joining: [dir, quarantineName])
        try? await client.move(from: path, to: quarantinePath)
        return .none
    }

    private func verifyOwnClaim(
        repoID: String,
        writerID: String,
        createdAtMs: Int64,
        claimPath: String,
        atomicResult: AtomicCreateResult
    ) async throws {
        guard let meta = try await client.metadata(path: claimPath), !meta.isDirectory else {
            throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: "identity claim atomicCreate reported \(atomicResult) but file not readable at \(claimPath)"]
            ))
        }
        // Ambiguous outcomes need byte-level validation; metadata existence alone can be a stale read.
        guard atomicResult == .bestEffortRetry || atomicResult == .alreadyExists else { return }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("claim-verify-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: claimPath, localURL: temp)
        let data = try Data(contentsOf: temp)
        guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let landedRepoID = dict["repo_id"] as? String,
              let landedWriterID = dict["writer_id"] as? String,
              let landedTs = Self.strictInt64(dict["created_at_ms"]) else {
            throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 4,
                userInfo: [NSLocalizedDescriptionKey: "identity claim at \(claimPath) unparseable after write"]
            ))
        }
        if atomicResult == .alreadyExists {
            guard landedRepoID == repoID, landedWriterID == writerID else {
                throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                    domain: "RepoBootstrap",
                    code: 5,
                    userInfo: [NSLocalizedDescriptionKey: "identity claim content drift at \(claimPath): expected \(writerID) got \(landedWriterID)"]
                ))
            }
            return
        }
        guard landedRepoID == repoID, landedWriterID == writerID, landedTs == createdAtMs else {
            throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 5,
                userInfo: [NSLocalizedDescriptionKey: "identity claim content drift at \(claimPath): expected (\(writerID), \(createdAtMs)) got (\(landedWriterID), \(landedTs))"]
            ))
        }
    }

    private func downloadWithRetry(remotePath: String, localURL: URL, attempts: Int) async throws {
        var lastError: Error?
        for attempt in 0..<attempts {
            do {
                try await client.download(remotePath: remotePath, localURL: localURL)
                return
            } catch {
                lastError = error
                if isStorageNotFoundError(error) { throw error }  // 404 isn't transient
                if attempt + 1 < attempts {
                    try await Task.sleep(for: .milliseconds(200 * (1 << attempt)))
                }
            }
        }
        throw lastError ?? NSError(domain: "RepoBootstrap", code: 7, userInfo: [NSLocalizedDescriptionKey: "download retries exhausted at \(remotePath)"])
    }

    /// JSON `true`/`false` bridges to NSNumber that `as? Int`-casts to 1/0;
    /// a foreign claim with `"created_at_ms": true` would otherwise be treated
    /// as a valid claim at timestamp 1 and could win lex-min election.
    private static func strictInt64(_ raw: Any?) -> Int64? {
        guard let raw else { return nil }
        if CFGetTypeID(raw as CFTypeRef) == CFBooleanGetTypeID() { return nil }
        if let v = raw as? Int64 { return v }
        if let v = raw as? Int { return Int64(v) }
        return nil
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

    private func malformedMetadataDirectoryError(path: String, description: String, code: Int) -> RepoBootstrap.BootstrapError {
        RepoBootstrap.BootstrapError.ioFailure(NSError(
            domain: "RepoBootstrap",
            code: code,
            userInfo: [NSLocalizedDescriptionKey: "\(description) at \(path) is a directory"]
        ))
    }

    private func makeTempJSON(dict: [String: Any], prefix: String) throws -> URL {
        let data: Data
        do {
            data = try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys, .prettyPrinted])
        } catch {
            throw RepoBootstrap.BootstrapError.ioFailure(error)
        }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("\(prefix)-\(UUID().uuidString).json")
        do {
            try data.write(to: temp, options: .atomic)
        } catch {
            throw RepoBootstrap.BootstrapError.ioFailure(error)
        }
        return temp
    }
}

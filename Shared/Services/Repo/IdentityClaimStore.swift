import Foundation
import os.log

private let identityClaimStoreLog = Logger(
    subsystem: "com.zizicici.watermelon",
    category: "IdentityClaimStore"
)

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


    /// Fail-closed so unreadable identity cannot elect a stale repo pointer.
    func canonicalElection(ignoringCorruptSelfClaimFor selfWriterID: String? = nil) async throws -> ClaimElectionResult {
        let dir = RepoLayout.identityDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            // Missing identity directory is legitimate pre-claim state.
            if isStorageNotFoundError(error) { return ClaimElectionResult(repoID: nil, ignoredSelfCorrupt: false) }
            throw RemoteWriteClassifier.normalizedCancellation(error)
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


    /// Only 0-byte (atomicCreate half-failed) is safe to clear; any other content might be canonical.
    func healZeroByteSelfClaim(writerID: String) async throws {
        do {
            let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
            guard let meta = try await metadataFileIfPresent(path: claimPath, description: "identity claim", code: 16) else { return }
            if meta.size > 0 { return }
            let temp = FileManager.default.temporaryDirectory
                .appendingPathComponent("self-claim-preflight-\(UUID().uuidString).json")
            defer { try? FileManager.default.removeItem(at: temp) }
            do {
                try await downloadListedClaimToleratingVisibilityLag(remotePath: claimPath, localURL: temp)
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                // A metadata-visible zero-byte claim whose GET stays 404 past grace is already gone:
                // the broken claim this heal targets no longer exists, so don't abort bootstrap.
                if isStorageNotFoundError(error) { return }
                throw error
            }
            // Surface local-read failures: silently treating them as empty would delete a non-zero remote claim under disk pressure.
            let data = try Data(contentsOf: temp)
            guard data.isEmpty else { return }
            try await client.delete(path: claimPath)
        } catch {
            throw RemoteWriteClassifier.normalizedCancellation(error)
        }
    }


    func classifyExistingClaim(claimPath: String, writerID: String, suggestedRepoID: String) async throws -> ExistingClaimClassification {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("claim-precheck-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        // Caller reached here via a metadata hit; on grace backends the data-path GET can still
        // 404 inside the visibility window. Spend the grace budget before letting a not-found abort
        // bootstrap / claim rewrite — a persistent not-found after grace still surfaces as an error.
        try await downloadListedClaimToleratingVisibilityLag(remotePath: claimPath, localURL: temp)
        // Surface local-read failures: silently classifying as zero-byte would delete and reclaim a healthy remote on disk pressure.
        let data = try Data(contentsOf: temp)
        if data.isEmpty { return .zeroByte }
        guard let wire = try? IdentityClaimWire(data: data),
              wire.writerID == writerID else {
            identityClaimStoreLog.warning("claim at \(claimPath, privacy: .public) corrupt or carries a foreign writerID — reclaiming")
            return .corrupt
        }
        if wire.repoID != suggestedRepoID {
            return .staleRepoID
        }
        return .ours
    }


    func writeOwnClaim(repoID: String, writerID: String, createdAtMs: Int64) async throws {
        do {
            let canonicalRepoID = try Self.canonicalRepoID(repoID)
            let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
            if try await metadataFileIfPresent(path: claimPath, description: "identity claim", code: 15) != nil {
                switch try await classifyExistingClaim(claimPath: claimPath, writerID: writerID, suggestedRepoID: canonicalRepoID) {
                case .ours:
                    return
                case .zeroByte, .staleRepoID, .corrupt:
                    try await client.delete(path: claimPath)
                }
            }
            let temp = try makeTempJSON(
                IdentityClaimWire(repoID: canonicalRepoID, createdAtMs: createdAtMs, writerID: writerID).encode(),
                prefix: "repo-identity-claim"
            )
            defer { try? FileManager.default.removeItem(at: temp) }
            do {
                try await Task { @Sendable () throws -> Void in
                    let result = try await client.atomicCreate(
                        localURL: temp,
                        remotePath: claimPath,
                        respectTaskCancellation: false
                    )
                    try await Self.verifyOwnClaim(
                        client: client,
                        repoID: canonicalRepoID,
                        writerID: writerID,
                        createdAtMs: createdAtMs,
                        claimPath: claimPath,
                        atomicResult: result
                    )
                }.value
            } catch {
                throw RemoteWriteClassifier.normalizedCancellation(error)
            }
        } catch {
            throw RemoteWriteClassifier.normalizedCancellation(error)
        }
    }

    private static func canonicalRepoID(_ raw: String) throws -> String {
        do {
            return try RepoCanonicalIdentity.validate(raw, field: "repoID")
        } catch {
            throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 19,
                userInfo: [NSLocalizedDescriptionKey: "identity claim repoID is malformed"]
            ))
        }
    }



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
        // Filename must be a `<writerID>.json` shape; payload's writer_id must match —
        // otherwise a stray .json with a forged older timestamp could win election.
        let expectedWriterID = entryName.hasSuffix(".json") ? String(entryName.dropLast(5)) : entryName
        do {
            try await downloadListedClaimToleratingVisibilityLag(remotePath: path, localURL: temp)
        } catch {
            if isStorageNotFoundError(error) {
                // The file was listed, so it existed; on a grace backend an unreadable-within-grace
                // writerID-shaped claim could be the lex-min canonical one — fail closed rather than
                // silently flip the adopted repoID. Self is included: dropping an unreadable own claim
                // here lets a peer win election, then writeOwnClaim deletes the now-visible self claim
                // as stale, turning a transient self-read lag into a durable repo-ID flip. Zero-grace
                // backends have no visibility lag, so a 404 is a genuine concurrent deletion; only
                // non-claim-shaped names are skipped.
                if client.readAfterWriteGraceSeconds > 0,
                   RepoLayout.isValidWriterID(expectedWriterID) {
                    throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                        domain: "RepoBootstrap",
                        code: 9,
                        userInfo: [NSLocalizedDescriptionKey:
                            "identity claim \(entryName) listed but unreadable within read-after-write grace — refusing election to avoid silent canonical flip (inspect/delete manually)"]
                    ))
                }
                return .none
            }
            throw RemoteWriteClassifier.normalizedCancellation(error)
        }
        let data = try Data(contentsOf: temp)
        if let wire = try? IdentityClaimWire(data: data),
           wire.writerID == expectedWriterID,
           RepoLayout.isValidWriterID(wire.writerID) {
            return .claim(IdentityClaim(repoID: wire.repoID, writerID: wire.writerID, createdAtMs: wire.createdAtMs))
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

    private static func verifyOwnClaim(
        client: any RemoteStorageClientProtocol,
        repoID: String,
        writerID: String,
        createdAtMs: Int64,
        claimPath: String,
        atomicResult: AtomicCreateResult
    ) async throws {
        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: 0)
        var attempt = 0
        while true {
            do {
                try await verifyOwnClaimOnce(
                    client: client,
                    repoID: repoID,
                    writerID: writerID,
                    createdAtMs: createdAtMs,
                    claimPath: claimPath,
                    atomicResult: atomicResult
                )
                return
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                guard Date() < deadline else { throw error }
                try await Task.sleep(for: .milliseconds(200 * (1 << min(attempt, 3))))
                attempt += 1
            }
        }
    }

    private static func verifyOwnClaimOnce(
        client: any RemoteStorageClientProtocol,
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
        guard atomicResult == .bestEffortRetry || atomicResult == .alreadyExists else { return }
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("claim-verify-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: claimPath, localURL: temp)
        let data = try Data(contentsOf: temp)
        guard let wire = try? IdentityClaimWire(data: data) else {
            throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 4,
                userInfo: [NSLocalizedDescriptionKey: "identity claim at \(claimPath) unparseable after write"]
            ))
        }
        if atomicResult == .alreadyExists {
            guard wire.repoID == repoID, wire.writerID == writerID else {
                throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                    domain: "RepoBootstrap",
                    code: 5,
                    userInfo: [NSLocalizedDescriptionKey: "identity claim content drift at \(claimPath): expected \(writerID) got \(wire.writerID)"]
                ))
            }
            return
        }
        guard wire.repoID == repoID, wire.writerID == writerID, wire.createdAtMs == createdAtMs else {
            throw RepoBootstrap.BootstrapError.ioFailure(NSError(
                domain: "RepoBootstrap",
                code: 5,
                userInfo: [NSLocalizedDescriptionKey: "identity claim content drift at \(claimPath): expected (\(writerID), \(createdAtMs)) got (\(wire.writerID), \(wire.createdAtMs))"]
            ))
        }
    }

    /// A claim entry surfaced by the directory listing exists, but on grace backends its data-path
    /// download can 404 while list/HEAD already see it. Spend the read-after-write grace budget on
    /// that not-found before reporting absence; every other error and zero-grace backends keep the
    /// single authoritative read.
    private func downloadListedClaimToleratingVisibilityLag(remotePath: String, localURL: URL) async throws {
        do {
            try await downloadWithRetry(remotePath: remotePath, localURL: localURL, attempts: 3)
            return
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            guard client.readAfterWriteGraceSeconds > 0, isStorageNotFoundError(error) else { throw error }
            let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: 1)
            var lastError = error
            var attempt = 0
            while Date() < deadline {
                try await Task.sleep(for: .milliseconds(200 * (1 << min(attempt, 3))))
                attempt += 1
                do {
                    try await downloadWithRetry(remotePath: remotePath, localURL: localURL, attempts: 3)
                    return
                } catch {
                    if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                    guard isStorageNotFoundError(error) else { throw error }
                    lastError = error
                }
            }
            throw lastError
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
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                if attempt + 1 < attempts {
                    try await Task.sleep(for: .milliseconds(200 * (1 << attempt)))
                }
            }
        }
        throw lastError ?? NSError(domain: "RepoBootstrap", code: 7, userInfo: [NSLocalizedDescriptionKey: "download retries exhausted at \(remotePath)"])
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

    private func malformedMetadataDirectoryError(path: String, description: String, code: Int) -> RepoBootstrap.BootstrapError {
        RepoBootstrap.BootstrapError.ioFailure(NSError(
            domain: "RepoBootstrap",
            code: code,
            userInfo: [NSLocalizedDescriptionKey: "\(description) at \(path) is a directory"]
        ))
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

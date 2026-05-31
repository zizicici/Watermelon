import Foundation
import os.log

private let bootstrapLog = Logger(subsystem: "com.zizicici.watermelon", category: "RepoBootstrap")

/// Finalized identity is the authoritative source.
actor RepoBootstrap {
    enum BootstrapError: Error {
        case ioFailure(Error)
        case futureFormatVersion(minAppVersion: String?)
    }

    private static let postCreateReadRetryFloorSeconds: TimeInterval = 3

    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    @discardableResult
    func initializeFreshRepo(writerID: String) async throws -> String {
        let suggested = UUID().uuidString.lowercased()
        let resolvedID = try await ensureIdentityFinalization(repoID: suggested, writerID: writerID)
        try await ensureSubdirectories()
        try await VersionManifestStore(client: client, basePath: basePath).writeIfAbsent(writerID: writerID)
        return resolvedID
    }

    // WebDAV/SMB/SFTP don't auto-create parents on PUT.
    func ensureSubdirectories() async throws {
        do {
            try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
            try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
            try await client.createDirectory(path: RepoLayout.identityDirectoryPath(base: basePath))
            try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
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
            if let finalized = try await loadFinalizedRepoIDToleratingDownloadVisibilityLag() {
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

            let outcome = try await MetadataCreateGate.createAuthoritativeOutcome(
                client: client,
                localURL: temp,
                remotePath: markerPath,
                respectTaskCancellation: false
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

    /// A finalized marker can be metadata-visible while its data-path download still 404s inside a
    /// grace backend's read-after-write window; spend that budget instead of classifying the
    /// recoverable absence as a damaged repo. Genuine absence (no metadata) returns nil fast, so
    /// fresh remotes are not slowed; malformed/transport/format errors keep their strict mapping.
    func loadFinalizedRepoIDToleratingDownloadVisibilityLag() async throws -> String? {
        do {
            return try await loadFinalizedRepoID()
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            guard client.readAfterWriteGraceSeconds > 0, Self.isDownloadVisibilityLag(error) else { throw error }
            var lastError = error
            let result = try await GracefulRead.retryWithinGrace(
                client: client,
                floorSeconds: 1,
                backoff: .exponential(baseMs: 200, maxShift: 3)
            ) {
                do {
                    if let finalized = try await self.loadFinalizedRepoID() {
                        return finalized
                    }
                    // The first read proved the finalized marker's metadata exists; a `nil` here means
                    // the metadata read itself flapped to not-found mid-grace. Inside the window that is
                    // still visibility lag, not deletion — keep spending the budget rather than demoting
                    // an authoritative finalized identity to absence. Past the deadline the retained
                    // download-lag `lastError` rethrows (fail-closed), never a bare nil.
                    return nil
                } catch {
                    if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                    guard Self.isDownloadVisibilityLag(error) else { throw error }
                    lastError = error
                    return nil
                }
            }
            if let result { return result }
            throw lastError
        }
    }

    private static func isDownloadVisibilityLag(_ error: Error) -> Bool {
        guard case BootstrapError.ioFailure(let underlying) = error else { return false }
        return isStorageNotFoundError(underlying)
    }

    private func loadFinalizedRepoIDWithRetries() async throws -> String? {
        var lastError: Error?
        let result = try await GracefulRead.retryWithFloor(
            client: client,
            floorSeconds: Self.postCreateReadRetryFloorSeconds
        ) {
            do {
                if let finalized = try await self.loadFinalizedRepoID() {
                    return finalized
                }
                lastError = nil
                return nil
            } catch is CancellationError {
                throw CancellationError()
            } catch let error where RemoteWriteClassifier.isCancellation(error) {
                throw CancellationError()
            } catch {
                lastError = error
                return nil
            }
        }
        if let result { return result }
        if let lastError { throw lastError }
        return nil
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

    /// Finalized identity is the single authoritative source.
    func loadRepoIDStrict() async throws -> RepoIDLoad {
        if let finalized = try await loadFinalizedRepoIDToleratingDownloadVisibilityLag() {
            return .found(finalized)
        }
        return .absent
    }

    func loadRepoID() async throws -> String? {
        switch try await loadRepoIDStrict() {
        case .absent: return nil
        case .found(let id): return id
        }
    }
}

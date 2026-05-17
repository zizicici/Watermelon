import Foundation

/// Bundles the three sources of repoID truth (DB, remote `.watermelon/repo.json`
/// claims, existing V2 commit/snapshot data) and owns the canonical resolution
/// flow used by `BackupV2RuntimeBuilder` and `V1MigrationService`.
struct RepoIdentitySources: Sendable {
    let stored: String?
    let remote: String?
    let data: String?
    let suggested: String

    /// Triangulates `(stored, remote, data)` and pre-computes `suggested`.
    /// `client` is the data-path client — heavy commit/snapshot list+read I/O
    /// stays off the metadata connection so we don't serialise against liveness
    /// on `.serialOnly` backends (SMB/SFTP).
    static func collect(
        profileID: Int64,
        identity: RepoIdentity,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        format: RemoteFormatCompatibilityService
    ) async throws -> RepoIdentitySources {
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let remote = try await bootstrap.loadRepoID()
        let data = try await checkedExistingV2DataRepoID(
            storedRepoID: nil,
            client: client,
            basePath: basePath,
            format: format
        )
        if let remote, let data, remote != data {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: data, remote: remote)
        }
        // Prefer the exact (profileID, currentRepoID) row when the remote carries
        // an identity; the per-profile fallback can otherwise surface a stale row
        // for an old wiped-and-reused repo and trigger a false identity mismatch.
        let currentRepoID = remote ?? data
        let stored: String?
        if let currentRepoID,
           let exact = try await identity.loadRepoState(profileID: profileID, repoID: currentRepoID) {
            stored = exact.repoID
        } else {
            stored = try await identity.findRepoStateByProfile(profileID: profileID)?.repoID
        }
        if let stored, let remote, stored != remote {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: stored, remote: remote)
        }
        if let stored, let data, stored != data {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: stored, remote: data)
        }
        let suggested = remote ?? data ?? stored ?? UUID().uuidString.lowercased()
        return RepoIdentitySources(stored: stored, remote: remote, data: data, suggested: suggested)
    }

    /// Writes `repo.json` (claim election) and the finalization marker on the
    /// caller-chosen `bootstrap`. Cleanup-only path passes the metadata-client
    /// bootstrap to keep publication off the data connection.
    @discardableResult
    func publish(bootstrap: RepoBootstrap, writerID: String) async throws -> String {
        let resolvedRepoID = try await bootstrap.ensureRepoJSON(repoID: suggested, writerID: writerID)
        if let stored, resolvedRepoID != stored {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: stored, remote: resolvedRepoID)
        }
        if let remote, resolvedRepoID != remote {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: remote, remote: resolvedRepoID)
        }
        if let data, resolvedRepoID != data {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: data, remote: resolvedRepoID)
        }
        let finalizedRepoID = try await bootstrap.ensureIdentityFinalization(repoID: resolvedRepoID, writerID: writerID)
        if finalizedRepoID != resolvedRepoID {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: resolvedRepoID, remote: finalizedRepoID)
        }
        return resolvedRepoID
    }

    private static func checkedExistingV2DataRepoID(
        storedRepoID: String?,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        format: RemoteFormatCompatibilityService
    ) async throws -> String? {
        let dataRepoIDs = try await existingRepoIDsInV2Data(client: client, basePath: basePath)
        guard !dataRepoIDs.isEmpty else {
            if try await format.hasAnyV2CommitOrSnapshotData(client: client, basePath: basePath) {
                throw BackupV2RuntimeBuildError.damagedV2Repo
            }
            return nil
        }
        guard dataRepoIDs.count == 1, let dataRepoID = dataRepoIDs.first else {
            throw BackupV2RuntimeBuildError.damagedV2Repo
        }
        if let storedRepoID, storedRepoID != dataRepoID {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: storedRepoID, remote: dataRepoID)
        }
        return dataRepoID
    }

    private static func existingRepoIDsInV2Data(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Set<String> {
        let effectiveClient = wrapIfSerial(client)
        let commitReader = CommitLogReader(client: effectiveClient, basePath: basePath)
        let snapshotReader = SnapshotReader(client: effectiveClient, basePath: basePath)
        async let commitFilenames = commitReader.listCommitFilenames()
        async let snapshotFilenames = snapshotReader.listSnapshotFilenames()
        var repoIDs: Set<String> = []
        for filename in try await commitFilenames {
            guard RepoLayout.parseCommitFilename(filename) != nil else { continue }
            do {
                let file = try await commitReader.read(filename: filename)
                repoIDs.insert(file.header.repoID)
            } catch is CancellationError {
                throw CancellationError()
            } catch is CommitLogReader.ReadError {
                continue
            } catch {
                if isStorageNotFoundError(error) { continue }
                throw error
            }
        }
        for filename in try await snapshotFilenames {
            guard RepoLayout.parseSnapshotFilename(filename) != nil else { continue }
            do {
                let file = try await snapshotReader.read(filename: filename)
                if file.header.repoID.isEmpty { continue }
                repoIDs.insert(file.header.repoID)
            } catch is CancellationError {
                throw CancellationError()
            } catch is SnapshotReader.ReadError {
                continue
            } catch {
                if isStorageNotFoundError(error) { continue }
                throw error
            }
        }
        return repoIDs
    }
}

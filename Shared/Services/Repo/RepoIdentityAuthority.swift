import Foundation

struct RepoIdentityAuthorityContext: Sendable {
    let profileID: Int64
    let writerID: String
    let basePath: String
    let dataClient: any RemoteStorageClientProtocol
    let identity: RepoIdentity
    let format: RemoteFormatCompatibilityService
}

struct RepoIdentityResolution: Sendable {
    let stored: String?
    let remote: String?
    let data: String?
    let suggested: String
}

struct RepoIdentityAuthority: Sendable {
    let context: RepoIdentityAuthorityContext

    func resolve() async throws -> RepoIdentityResolution {
        let bootstrap = RepoBootstrap(client: context.dataClient, basePath: context.basePath)
        let remote = try await bootstrap.loadRepoID()
        let data = try await checkedExistingV2DataRepoID()
        if let remote, let data, remote != data {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: data, observed: remote)
        }

        let currentRepoID = remote ?? data
        let stored: String?
        if let currentRepoID,
           let exact = try await context.identity.loadRepoState(
                profileID: context.profileID,
                repoID: currentRepoID
           ) {
            stored = exact.repoID
        } else if let fallback = try await context.identity.findRepoStateByProfile(profileID: context.profileID)?.repoID {
            stored = try await storedFallback(fallback, currentRepoID: currentRepoID)
        } else {
            stored = nil
        }

        if let stored, let remote, stored != remote {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: stored, observed: remote)
        }
        if let stored, let data, stored != data {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: stored, observed: data)
        }
        let suggested = remote ?? data ?? stored ?? UUID().uuidString.lowercased()
        return RepoIdentityResolution(stored: stored, remote: remote, data: data, suggested: suggested)
    }

    @discardableResult
    func publish(
        _ resolution: RepoIdentityResolution,
        using publishBootstrap: RepoBootstrap
    ) async throws -> String {
        try await Self.publish(resolution, using: publishBootstrap, writerID: context.writerID)
    }

    @discardableResult
    static func publish(
        _ resolution: RepoIdentityResolution,
        using publishBootstrap: RepoBootstrap,
        writerID: String
    ) async throws -> String {
        let resolvedRepoID = try await publishBootstrap.ensureRepoJSON(repoID: resolution.suggested, writerID: writerID)
        if let stored = resolution.stored, resolvedRepoID != stored {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: stored, observed: resolvedRepoID)
        }
        if let remote = resolution.remote, resolvedRepoID != remote {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: remote, observed: resolvedRepoID)
        }
        if let data = resolution.data, resolvedRepoID != data {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: data, observed: resolvedRepoID)
        }
        let finalizedRepoID = try await publishBootstrap.ensureIdentityFinalization(
            repoID: resolvedRepoID,
            writerID: writerID
        )
        if finalizedRepoID != resolvedRepoID {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(stored: resolvedRepoID, observed: finalizedRepoID)
        }
        return resolvedRepoID
    }

    private func storedFallback(_ fallback: String, currentRepoID: String?) async throws -> String? {
        guard let currentRepoID, fallback != currentRepoID else {
            return fallback
        }
        let claims = IdentityClaimStore(client: context.dataClient, basePath: context.basePath)
        if let ownClaim = try await claims.readOwnClaim(writerID: context.writerID),
           ownClaim.repoID == currentRepoID {
            return nil
        }
        return fallback
    }

    private func checkedExistingV2DataRepoID() async throws -> String? {
        let dataRepoIDs = try await existingRepoIDsInV2Data()
        guard !dataRepoIDs.isEmpty else {
            if try await context.format.hasAnyV2CommitOrSnapshotData(
                client: context.dataClient,
                basePath: context.basePath
            ) {
                throw BackupV2RuntimeBuildError.damagedV2Repo
            }
            return nil
        }
        guard dataRepoIDs.count == 1, let dataRepoID = dataRepoIDs.first else {
            throw BackupV2RuntimeBuildError.damagedV2Repo
        }
        return dataRepoID
    }

    private func existingRepoIDsInV2Data() async throws -> Set<String> {
        let effectiveClient = wrapIfSerial(context.dataClient)
        let commitReader = CommitLogReader(client: effectiveClient, basePath: context.basePath)
        let snapshotReader = SnapshotReader(client: effectiveClient, basePath: context.basePath)
        async let commitFilenames = commitReader.listCommitFilenames()
        async let snapshotFilenames = snapshotReader.listSnapshotFilenames()
        var repoIDs: Set<String> = []
        for filename in try await commitFilenames {
            guard RepoLayout.parseCommitFilename(filename) != nil else { continue }
            do {
                let file = try await Self.readSpendingGraceOnNotFound(client: effectiveClient) {
                    try await commitReader.read(filename: filename)
                }
                repoIDs.insert(file.header.repoID)
            } catch is CancellationError {
                throw CancellationError()
            } catch is RepoJSONLReadError {
                continue
            } catch {
                if RemoteStorageErrorClassifier.isNotFound(error) { continue }
                throw error
            }
        }
        for filename in try await snapshotFilenames {
            guard RepoLayout.parseSnapshotFilename(filename) != nil else { continue }
            do {
                let file = try await Self.readSpendingGraceOnNotFound(client: effectiveClient) {
                    try await snapshotReader.read(filename: filename)
                }
                repoIDs.insert(file.header.repoID)
            } catch is CancellationError {
                throw CancellationError()
            } catch is RepoJSONLReadError {
                continue
            } catch {
                if RemoteStorageErrorClassifier.isNotFound(error) { continue }
                throw error
            }
        }
        return repoIDs
    }

    // A listed commit/snapshot can 404 on GET inside the backend read-after-write window. Spend the
    // grace budget on a `.notFound` before letting the caller skip it; otherwise a lagging-but-listed
    // sole data file would scan to empty and route the repo to `damagedV2Repo`. Corrupt-file errors
    // and post-grace not-found still surface immediately so the caller's skip/fail-closed paths hold.
    private static func readSpendingGraceOnNotFound<T>(
        client: any RemoteStorageClientProtocol,
        _ read: @Sendable () async throws -> T
    ) async throws -> T {
        // Only a `.notFound` is retryable absence; corrupt-file errors and any non-not-found surface
        // immediately. `retryWithinGrace` short-circuits zero-grace to one attempt, then the retained
        // not-found rethrows past the deadline so the caller stays fail-closed.
        var lastNotFound: RepoJSONLReadError?
        let value = try await GracefulRead.retryWithinGrace(
            client: client,
            floorSeconds: 1,
            backoff: .exponential(baseMs: 200, maxShift: 3)
        ) {
            do {
                return try await read()
            } catch let error as RepoJSONLReadError {
                guard case .notFound = error else { throw error }
                lastNotFound = error
                return nil
            }
        }
        if let value { return value }
        throw lastNotFound!
    }
}

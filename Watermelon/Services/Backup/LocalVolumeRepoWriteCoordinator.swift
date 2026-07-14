import Foundation

struct LocalVolumeRepoWriteCoordinator: RepoWriteCoordinator {
    private static let claimAttempts = 11
    private static let claimRetryDelay = Duration.milliseconds(100)

    let client: LocalVolumeClient

    func acquire(
        basePath: String,
        writerID: String?,
        mode: RepoWritePreparationMode,
        now: Date
    ) async throws -> RepoWriteAcquisition<LocalVolumeWriteSession> {
        let sessionKey = try await client.writeSessionKey(basePath: basePath)
        var session: LocalVolumeWriteSession?
        for attempt in 0..<Self.claimAttempts {
            if let claimed = LocalVolumeWriteSession.claim(key: sessionKey) {
                session = claimed
                break
            }
            if attempt < Self.claimAttempts - 1 {
                try await Task.sleep(for: Self.claimRetryDelay)
            }
        }
        guard let session else { return .declined(.localWriteInProgress) }
        do {
            try Task.checkCancellation()
            try await client.createDirectory(path: RemotePathBuilder.normalizePath(basePath))
            let repoDirectory = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
            let monthsDirectory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
            if try await client.metadata(path: monthsDirectory)?.isDirectory == true {
                try await client.synchronizeDirectoryHierarchy(path: monthsDirectory)
            } else if try await client.metadata(path: repoDirectory)?.isDirectory == true {
                try await client.synchronizeDirectoryHierarchy(path: repoDirectory)
            }
            try Task.checkCancellation()
        } catch {
            await session.release()
            throw error
        }
        return .acquired(AcquiredRepoWriteAuthority(
            session: session,
            authorID: writerID ?? session.authorID,
            cleansCoordinationArtifacts: false
        ))
    }
}

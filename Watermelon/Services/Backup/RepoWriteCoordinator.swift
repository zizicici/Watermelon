import Foundation

enum RepoWritePreparationMode: Sendable {
    case foreground
    case background
    case maintenance
}

struct AcquiredRepoWriteAuthority<Session: RepoWriteSession>: Sendable {
    let session: Session
    let authorID: String
    let cleansCoordinationArtifacts: Bool
}

enum RepoWriteAcquisition<Session: RepoWriteSession>: Sendable {
    case acquired(AcquiredRepoWriteAuthority<Session>)
    case declined(LiteRepoError)
}

protocol RepoWriteCoordinator: Sendable {
    associatedtype Session: RepoWriteSession

    func acquire(
        basePath: String,
        writerID: String?,
        mode: RepoWritePreparationMode,
        now: Date
    ) async throws -> RepoWriteAcquisition<Session>
}

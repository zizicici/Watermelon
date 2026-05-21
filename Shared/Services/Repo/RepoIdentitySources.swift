import Foundation

struct RepoIdentitySources: Sendable {
    let stored: String?
    let remote: String?
    let data: String?
    let suggested: String

    /// Data-path identity scans stay off the metadata connection on serial-only backends.
    static func collect(
        profileID: Int64,
        writerID: String,
        identity: RepoIdentity,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        format: RemoteFormatCompatibilityService
    ) async throws -> RepoIdentitySources {
        let resolution = try await RepoIdentityAuthority(
            context: RepoIdentityAuthorityContext(
                profileID: profileID,
                writerID: writerID,
                basePath: basePath,
                dataClient: client,
                identity: identity,
                format: format
            )
        ).resolve()
        return RepoIdentitySources(
            stored: resolution.stored,
            remote: resolution.remote,
            data: resolution.data,
            suggested: resolution.suggested
        )
    }

    /// Writes `repo.json` (claim election) and the finalization marker on the
    /// caller-chosen `bootstrap`. Cleanup-only path passes the metadata-client
    /// bootstrap to keep publication off the data connection.
    @discardableResult
    func publish(bootstrap: RepoBootstrap, writerID: String) async throws -> String {
        let resolution = RepoIdentityResolution(stored: stored, remote: remote, data: data, suggested: suggested)
        return try await RepoIdentityAuthority.publish(resolution, using: bootstrap, writerID: writerID)
    }
}

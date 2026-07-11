import Foundation

final class StorageProfileConnectionService: @unchecked Sendable {
    typealias ConfirmSFTPHostKey = @MainActor (SFTPHostKeyPromptPolicy.Decision, String) async -> Bool

    struct Hooks: Sendable {
        let captureSFTPHostKey: @Sendable (String, Int) async throws -> String

        static let live = Hooks(captureSFTPHostKey: { host, port in
            try await SFTPClient.captureHostKeyFingerprint(host: host, port: port)
        })
    }

    private let databaseManager: DatabaseManager
    private let hooks: Hooks

    init(
        databaseManager: DatabaseManager,
        hooks: Hooks = .live
    ) {
        self.databaseManager = databaseManager
        self.hooks = hooks
    }

    func prepareForConnection(
        profile: ServerProfileRecord,
        confirmSFTPHostKey: ConfirmSFTPHostKey
    ) async throws -> ServerProfileRecord {
        guard profile.resolvedStorageType == .sftp else { return profile }
        guard let params = profile.sftpParams else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let actual = try await hooks.captureSFTPHostKey(profile.host, profile.port)
        let decision = SFTPHostKeyPromptPolicy.decision(
            existingHost: profile.host,
            existingPort: profile.port,
            expectedFingerprint: params.hostKeyFingerprintSHA256,
            proposedHost: profile.host,
            proposedPort: profile.port,
            actualFingerprint: actual
        )
        guard decision != .none else { return profile }
        guard await confirmSFTPHostKey(decision, actual) else { throw CancellationError() }
        try Task.checkCancellation()
        guard let profileID = profile.id else { throw RemoteStorageClientError.invalidConfiguration }
        return try databaseManager.updateSFTPHostKeyFingerprint(
            profileID: profileID,
            expected: profile,
            fingerprint: actual
        )
    }
}

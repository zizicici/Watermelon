import UIKit

struct DropboxProfileSetupDraft {
    let connectionParams: DropboxConnectionParams
    let credentialJSONString: String
    let username: String?
}

@MainActor
final class DropboxProfileSetupCoordinator {
    private let authenticationService: DropboxOAuthService
    private let tokenService: DropboxTokenService
    private let sharedState: DropboxSharedState

    nonisolated init(
        authenticationService: DropboxOAuthService,
        tokenService: DropboxTokenService,
        sharedState: DropboxSharedState
    ) {
        self.authenticationService = authenticationService
        self.tokenService = tokenService
        self.sharedState = sharedState
    }

    func prepare(
        from parent: UIViewController,
        forceReauthentication: Bool
    ) async throws -> DropboxProfileSetupDraft {
        let signIn = try await authenticationService.signIn(
            from: parent,
            forceReauthentication: forceReauthentication
        )
        try Task.checkCancellation()
        let connection = try CanonicalDropboxConnection(params: signIn.connectionParams)
        let client = DropboxClient(
            config: DropboxClient.Config(connection: connection),
            credential: signIn.credential,
            tokenProvider: tokenService,
            sharedState: sharedState
        )
        try await client.verifyWriteAccess()
        try Task.checkCancellation()
        return DropboxProfileSetupDraft(
            connectionParams: signIn.connectionParams,
            credentialJSONString: try signIn.credential.encodedJSONString(),
            username: signIn.username
        )
    }

    func cancelInteractiveSignIn() {
        authenticationService.cancelInteractiveSignIn()
    }
}

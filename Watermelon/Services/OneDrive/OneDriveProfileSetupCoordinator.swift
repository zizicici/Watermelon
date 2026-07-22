import UIKit

final class PendingOneDriveAccountLease {
    enum Disposition: Equatable {
        case committed
        case relinquished
        case discarded
    }

    let credential: OneDriveCredentialBlob

    private let finalize: (OneDriveCredentialBlob, Disposition) -> Void
    private var isFinalized = false

    init(
        credential: OneDriveCredentialBlob,
        finalize: @escaping (OneDriveCredentialBlob, Disposition) -> Void
    ) {
        self.credential = credential
        self.finalize = finalize
    }

    deinit {
        guard !isFinalized else { return }
        finalize(credential, .discarded)
    }

    func commit() {
        finish(.committed)
    }

    func relinquishToReplacement() {
        finish(.relinquished)
    }

    func discard() {
        finish(.discarded)
    }

    private func finish(_ disposition: Disposition) {
        guard !isFinalized else { return }
        isFinalized = true
        finalize(credential, disposition)
    }
}

struct OneDriveProfileSetupDraft {
    let connectionParams: OneDriveConnectionParams
    let credentialJSONString: String
    let username: String?
    let accountLease: PendingOneDriveAccountLease
}

@MainActor
final class OneDriveProfileSetupCoordinator {
    private let authenticationService: OneDriveMSALService
    private let bootstrapService: OneDriveAppFolderBootstrapService
    private let sharedState: OneDriveSharedState
    private let credentialLifecycleService: OneDriveCredentialLifecycleService

    nonisolated init(
        authenticationService: OneDriveMSALService,
        bootstrapService: OneDriveAppFolderBootstrapService,
        sharedState: OneDriveSharedState,
        credentialLifecycleService: OneDriveCredentialLifecycleService
    ) {
        self.authenticationService = authenticationService
        self.bootstrapService = bootstrapService
        self.sharedState = sharedState
        self.credentialLifecycleService = credentialLifecycleService
    }

    func prepare(from parent: UIViewController) async throws -> OneDriveProfileSetupDraft {
        let signIn: OneDriveInteractiveSignInResult
        do {
            signIn = try await authenticationService.signIn(from: parent)
        } catch {
            credentialLifecycleService.reconcileCachedAccounts()
            throw error
        }
        let lease = credentialLifecycleService.makePendingAccountLease(credential: signIn.credential)
        do {
            try Task.checkCancellation()
            let bootstrap = try await bootstrapService.bootstrap(credential: signIn.credential)
            try Task.checkCancellation()
            let connection = try CanonicalOneDriveConnection(params: bootstrap.connectionParams)
            let client = OneDriveClient(
                config: OneDriveClient.Config(connection: connection),
                credential: signIn.credential,
                tokenProvider: authenticationService,
                sharedState: sharedState
            )
            try await client.verifyWriteAccess()
            try Task.checkCancellation()
            return OneDriveProfileSetupDraft(
                connectionParams: bootstrap.connectionParams,
                credentialJSONString: try signIn.credential.encodedJSONString(),
                username: signIn.username,
                accountLease: lease
            )
        } catch let error as RemoteProbeDeferredCleanupError {
            Task { @MainActor [lease] in
                await error.waitUntilCleanupCompletes()
                lease.discard()
            }
            throw error.underlyingError
        } catch {
            lease.discard()
            throw error
        }
    }
}

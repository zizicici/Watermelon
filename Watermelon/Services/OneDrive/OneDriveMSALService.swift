import Foundation
import MSAL
import UIKit

struct OneDriveInteractiveSignInResult: Sendable {
    let credential: OneDriveCredentialBlob
    let username: String?
}

final class OneDriveMSALService: OneDriveAccessTokenProviding, @unchecked Sendable {
    static let scopes = ["Files.ReadWrite.AppFolder"]

    private let lock = NSLock()
    private var cachedApplication: MSALPublicClientApplication?

    func accessToken(
        for credential: OneDriveCredentialBlob,
        forceRefresh: Bool,
        claims: String?
    ) async throws -> OneDriveAccessToken {
        let application = try makeApplication()
        let account: MSALAccount?
        do {
            account = try application.account(forIdentifier: credential.homeAccountIdentifier)
        } catch let error as OneDriveAuthenticationError {
            throw error
        } catch {
            throw OneDriveAuthenticationError.reauthenticationRequired
        }
        guard let account else {
            throw OneDriveAuthenticationError.reauthenticationRequired
        }
        try validate(account: account, against: credential)

        let parameters = MSALSilentTokenParameters(scopes: Self.scopes, account: account)
        parameters.forceRefresh = forceRefresh
        if let claims, !claims.isEmpty {
            var claimsError: NSError?
            let claimsRequest = MSALClaimsRequest(jsonString: claims, error: &claimsError)
            guard claimsError == nil else {
                throw OneDriveAuthenticationError.reauthenticationRequired
            }
            parameters.claimsRequest = claimsRequest
        }

        return try await withCheckedThrowingContinuation { continuation in
            application.acquireTokenSilent(with: parameters) { result, error in
                guard let result, error == nil else {
                    continuation.resume(throwing: Self.mapSilentError(error))
                    return
                }
                do {
                    try Self.validate(result: result, against: credential)
                    continuation.resume(returning: OneDriveAccessToken(
                        value: result.accessToken,
                        expiresAt: result.expiresOn
                    ))
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    @MainActor
    func signIn(from parentViewController: UIViewController) async throws -> OneDriveInteractiveSignInResult {
        let application = try makeApplication()
        let webParameters = MSALWebviewParameters(authPresentationViewController: parentViewController)
        let parameters = MSALInteractiveTokenParameters(scopes: Self.scopes, webviewParameters: webParameters)
        parameters.promptType = .selectAccount

        let result: MSALResult = try await withCheckedThrowingContinuation { continuation in
            application.acquireToken(with: parameters) { result, error in
                guard let result, error == nil else {
                    continuation.resume(throwing: Self.mapInteractiveError(error))
                    return
                }
                continuation.resume(returning: result)
            }
        }
        guard Self.hasRequiredScope(result.scopes),
              let identifier = result.account.identifier,
              let tenantID = result.account.homeAccountId?.tenantId ?? result.tenantProfile.tenantId,
              !tenantID.isEmpty else {
            throw OneDriveAuthenticationError.unsupportedAccount
        }
        let credential = OneDriveCredentialBlob(
            homeAccountIdentifier: identifier,
            tenantID: tenantID,
            authorityEnvironment: result.account.environment
        )
        try Self.validate(result: result, against: credential)
        return OneDriveInteractiveSignInResult(
            credential: credential,
            username: result.account.username
        )
    }

    static func handleRedirect(url: URL, sourceApplication: String?) -> Bool {
        MSALPublicClientApplication.handleMSALResponse(url, sourceApplication: sourceApplication)
    }

    static func cancelInteractiveSignIn() {
        _ = MSALPublicClientApplication.cancelCurrentWebAuthSession()
    }

    func removeCachedAccount(homeAccountIdentifier: String) throws {
        let application = try makeApplication()
        let account: MSALAccount? = try application.account(forIdentifier: homeAccountIdentifier)
        guard let account else { return }
        try application.remove(account)
    }

    func cachedHomeAccountIdentifiers() throws -> [String] {
        try makeApplication().allAccounts().compactMap(\.identifier)
    }

    private func makeApplication() throws -> MSALPublicClientApplication {
        if let cached = lock.withLock({ cachedApplication }) { return cached }
        guard let clientID = Bundle.main.object(forInfoDictionaryKey: "OneDriveClientID") as? String,
              !clientID.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
              let redirectURI = Bundle.main.object(forInfoDictionaryKey: "OneDriveRedirectURI") as? String,
              !redirectURI.isEmpty,
              let authorityURL = URL(string: "https://login.microsoftonline.com/consumers") else {
            throw OneDriveAuthenticationError.configurationMissing
        }
        let authority: MSALAADAuthority
        do {
            authority = try MSALAADAuthority(url: authorityURL)
        } catch {
            throw OneDriveAuthenticationError.configurationMissing
        }
        let configuration = MSALPublicClientApplicationConfig(
            clientId: clientID,
            redirectUri: redirectURI,
            authority: authority
        )
        let application: MSALPublicClientApplication
        do {
            application = try MSALPublicClientApplication(configuration: configuration)
        } catch {
            throw OneDriveAuthenticationError.configurationMissing
        }
        return lock.withLock {
            if let cachedApplication { return cachedApplication }
            cachedApplication = application
            return application
        }
    }

    private func validate(account: MSALAccount, against credential: OneDriveCredentialBlob) throws {
        guard account.identifier == credential.homeAccountIdentifier,
              account.environment.caseInsensitiveCompare(credential.authorityEnvironment) == .orderedSame,
              account.homeAccountId?.tenantId == nil
                || account.homeAccountId?.tenantId == credential.tenantID else {
            throw OneDriveAuthenticationError.accountMismatch
        }
    }

    private static func validate(result: MSALResult, against credential: OneDriveCredentialBlob) throws {
        guard hasRequiredScope(result.scopes),
              result.account.identifier == credential.homeAccountIdentifier,
              result.account.environment.caseInsensitiveCompare(credential.authorityEnvironment) == .orderedSame,
              (result.account.homeAccountId?.tenantId ?? result.tenantProfile.tenantId) == credential.tenantID else {
            throw OneDriveAuthenticationError.accountMismatch
        }
    }

    private static func hasRequiredScope(_ scopes: [String]) -> Bool {
        scopes.contains { $0.caseInsensitiveCompare(Self.scopes[0]) == .orderedSame }
    }

    private static func mapSilentError(_ error: Error?) -> Error {
        guard let error else { return OneDriveAuthenticationError.reauthenticationRequired }
        let nsError = error as NSError
        if nsError.domain == NSURLErrorDomain {
            return OneDriveErrorClassifier.sanitizedTransportError(error)
        }
        return OneDriveAuthenticationError.reauthenticationRequired
    }

    private static func mapInteractiveError(_ error: Error?) -> Error {
        guard let error else { return OneDriveAuthenticationError.reauthenticationRequired }
        let nsError = error as NSError
        if nsError.domain == MSALErrorDomain,
           nsError.code == MSALError.userCanceled.rawValue {
            return CancellationError()
        }
        if nsError.domain == NSURLErrorDomain {
            return OneDriveErrorClassifier.sanitizedTransportError(error)
        }
        return OneDriveAuthenticationError.reauthenticationRequired
    }
}

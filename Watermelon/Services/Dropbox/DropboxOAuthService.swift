import AuthenticationServices
import CryptoKit
import Foundation
import Security
import UIKit

nonisolated struct DropboxInteractiveSignInResult: Sendable {
    let connectionParams: DropboxConnectionParams
    let credential: DropboxCredentialBlob
    let username: String?
}

fileprivate struct DropboxOAuthTokenResponse: Decodable {
    let accessToken: String
    let expiresIn: TimeInterval
    let refreshToken: String?
    let accountID: String?

    private enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
        case expiresIn = "expires_in"
        case refreshToken = "refresh_token"
        case accountID = "account_id"
    }
}

fileprivate struct DropboxCurrentAccount: Decodable {
    struct Name: Decodable {
        let displayName: String

        private enum CodingKeys: String, CodingKey {
            case displayName = "display_name"
        }
    }

    let accountID: String
    let email: String?
    let name: Name

    private enum CodingKeys: String, CodingKey {
        case accountID = "account_id"
        case email
        case name
    }
}

actor DropboxTokenService: DropboxAccessTokenProviding {
    private struct CacheKey: Hashable {
        let appKey: String
        let accountID: String
    }

    private let session: URLSession
    private let sharedState: DropboxSharedState
    private var cachedTokens: [CacheKey: DropboxAccessToken] = [:]
    private var refreshTasks: [CacheKey: Task<DropboxAccessToken, Error>] = [:]

    nonisolated var dropboxSharedState: DropboxSharedState? { sharedState }

    init(
        sharedState: DropboxSharedState = DropboxSharedState(),
        sessionConfiguration: URLSessionConfiguration? = nil
    ) {
        self.sharedState = sharedState
        let configuration = sessionConfiguration?.copy() as? URLSessionConfiguration ?? .ephemeral
        configuration.timeoutIntervalForRequest = 120
        configuration.timeoutIntervalForResource = 120
        configuration.urlCache = nil
        configuration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        session = URLSession(configuration: configuration)
    }

    func accessToken(
        for credential: DropboxCredentialBlob,
        appKey: String,
        forceRefresh: Bool
    ) async throws -> DropboxAccessToken {
        guard !appKey.isEmpty else { throw DropboxAuthenticationError.configurationMissing }
        let cacheKey = CacheKey(appKey: appKey, accountID: credential.accountID)
        if !forceRefresh, let cached = cachedTokens[cacheKey], cached.isUsable() {
            return cached
        }
        if let task = refreshTasks[cacheKey] {
            return try await task.value
        }
        let task = Task { [self] in
            try await refreshAccessToken(
                credential: credential,
                appKey: appKey,
                cacheKey: cacheKey
            )
        }
        refreshTasks[cacheKey] = task
        do {
            let token = try await task.value
            cachedTokens[cacheKey] = token
            refreshTasks.removeValue(forKey: cacheKey)
            return token
        } catch {
            refreshTasks.removeValue(forKey: cacheKey)
            throw error
        }
    }

    fileprivate func exchangeAuthorizationCode(
        _ code: String,
        verifier: String,
        appKey: String,
        redirectURI: String
    ) async throws -> DropboxOAuthTokenResponse {
        try await requestToken(
            parameters: [
                URLQueryItem(name: "code", value: code),
                URLQueryItem(name: "grant_type", value: "authorization_code"),
                URLQueryItem(name: "client_id", value: appKey),
                URLQueryItem(name: "redirect_uri", value: redirectURI),
                URLQueryItem(name: "code_verifier", value: verifier)
            ],
            throttleKey: nil
        )
    }

    func adopt(accessToken: String, expiresIn: TimeInterval, appKey: String, accountID: String) {
        cachedTokens[CacheKey(appKey: appKey, accountID: accountID)] = DropboxAccessToken(
            value: accessToken,
            expiresAt: Date().addingTimeInterval(expiresIn)
        )
    }

    private func refreshAccessToken(
        credential: DropboxCredentialBlob,
        appKey: String,
        cacheKey: CacheKey
    ) async throws -> DropboxAccessToken {
        let throttleKey = DropboxThrottleGate.Key(appKey: appKey, accountID: credential.accountID)
        let response = try await requestToken(
            parameters: [
                URLQueryItem(name: "grant_type", value: "refresh_token"),
                URLQueryItem(name: "refresh_token", value: credential.refreshToken),
                URLQueryItem(name: "client_id", value: appKey)
            ],
            throttleKey: throttleKey
        )
        let token = DropboxAccessToken(
            value: response.accessToken,
            expiresAt: Date().addingTimeInterval(response.expiresIn)
        )
        guard token.isUsable(), response.accountID == nil || response.accountID == credential.accountID else {
            throw DropboxAuthenticationError.accountMismatch
        }
        try await verifyCurrentAccount(
            accessToken: token.value,
            expectedAccountID: cacheKey.accountID,
            throttleKey: throttleKey
        )
        return token
    }

    private func requestToken(
        parameters: [URLQueryItem],
        throttleKey: DropboxThrottleGate.Key?
    ) async throws -> DropboxOAuthTokenResponse {
        guard let url = URL(string: "https://api.dropboxapi.com/oauth2/token") else {
            throw DropboxAuthenticationError.configurationMissing
        }
        if let throttleKey {
            try await sharedState.throttleGate.waitForPermit(for: throttleKey)
        }
        var form = URLComponents()
        form.queryItems = parameters
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        request.httpBody = form.percentEncodedQuery?.data(using: .utf8)
        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw DropboxErrorClassifier.isConnectionUnavailable(error)
                ? error
                : RemoteStorageClientError.unavailable
        }
        guard let http = response as? HTTPURLResponse else {
            throw DropboxAuthenticationError.invalidResponse
        }
        guard http.statusCode == 200 else {
            let error = DropboxErrorClassifier.makeServiceError(
                data: data,
                response: http,
                endpoint: "oauth2/token"
            )
            if let throttleKey, let retryAfter = DropboxErrorClassifier.retryAfter(in: error) {
                await sharedState.throttleGate.record(retryAfter: retryAfter, for: throttleKey)
            }
            if http.statusCode == 400 || http.statusCode == 401 {
                throw DropboxAuthenticationError.reauthenticationRequired
            }
            throw error
        }
        do {
            return try JSONDecoder().decode(DropboxOAuthTokenResponse.self, from: data)
        } catch {
            throw DropboxAuthenticationError.invalidResponse
        }
    }

    private func verifyCurrentAccount(
        accessToken: String,
        expectedAccountID: String,
        throttleKey: DropboxThrottleGate.Key
    ) async throws {
        guard let url = URL(string: "https://api.dropboxapi.com/2/users/get_current_account") else {
            throw DropboxAuthenticationError.configurationMissing
        }
        try await sharedState.throttleGate.waitForPermit(for: throttleKey)
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = Data("null".utf8)
        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw DropboxErrorClassifier.isConnectionUnavailable(error)
                ? error
                : RemoteStorageClientError.unavailable
        }
        guard let http = response as? HTTPURLResponse else {
            throw DropboxAuthenticationError.invalidResponse
        }
        guard http.statusCode == 200 else {
            let error = DropboxErrorClassifier.makeServiceError(
                data: data,
                response: http,
                endpoint: "users/get_current_account"
            )
            if let retryAfter = DropboxErrorClassifier.retryAfter(in: error) {
                await sharedState.throttleGate.record(retryAfter: retryAfter, for: throttleKey)
            }
            throw error
        }
        let account: DropboxCurrentAccount
        do {
            account = try JSONDecoder().decode(DropboxCurrentAccount.self, from: data)
        } catch {
            throw DropboxAuthenticationError.invalidResponse
        }
        guard account.accountID == expectedAccountID else {
            throw DropboxAuthenticationError.accountMismatch
        }
    }
}

@MainActor
final class DropboxOAuthService {
    private static let scopes = [
        "account_info.read",
        "files.metadata.read",
        "files.content.read",
        "files.content.write"
    ]

    private let tokenService: DropboxTokenService
    private let session: URLSession
    private var currentWebSession: ASWebAuthenticationSession?
    private var currentPresentationProvider: DropboxAuthenticationPresentationProvider?

    nonisolated init(tokenService: DropboxTokenService, sessionConfiguration: URLSessionConfiguration? = nil) {
        self.tokenService = tokenService
        let configuration = sessionConfiguration?.copy() as? URLSessionConfiguration ?? .ephemeral
        configuration.timeoutIntervalForRequest = 120
        configuration.timeoutIntervalForResource = 120
        configuration.urlCache = nil
        configuration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        session = URLSession(configuration: configuration)
    }

    func signIn(
        from parent: UIViewController,
        forceReauthentication: Bool = false
    ) async throws -> DropboxInteractiveSignInResult {
        let appKey = try configuredAppKey()
        let callbackScheme = "db-\(appKey)"
        let redirectURI = "\(callbackScheme)://2/token"
        let state = UUID().uuidString.lowercased()
        let verifier = try Self.makeVerifier()
        let challenge = Self.base64URL(Data(SHA256.hash(data: Data(verifier.utf8))))
        let authorizationURL = try Self.authorizationURL(
            appKey: appKey,
            redirectURI: redirectURI,
            state: state,
            challenge: challenge,
            forceReauthentication: forceReauthentication
        )
        guard let window = parent.viewIfLoaded?.window else {
            throw DropboxAuthenticationError.configurationMissing
        }
        let provider = DropboxAuthenticationPresentationProvider(window: window)
        let callbackURL = try await runWebAuthentication(
            url: authorizationURL,
            callbackScheme: callbackScheme,
            presentationProvider: provider
        )
        guard let callback = URLComponents(url: callbackURL, resolvingAgainstBaseURL: false),
              callback.scheme == callbackScheme,
              callback.host == "2",
              callback.path == "/token",
              callback.queryItems?.first(where: { $0.name == "state" })?.value == state else {
            throw DropboxAuthenticationError.invalidResponse
        }
        if callback.queryItems?.contains(where: { $0.name == "error" && $0.value == "access_denied" }) == true {
            throw CancellationError()
        }
        guard let code = callback.queryItems?.first(where: { $0.name == "code" })?.value,
              !code.isEmpty else {
            throw DropboxAuthenticationError.reauthenticationRequired
        }
        let tokenResponse = try await tokenService.exchangeAuthorizationCode(
            code,
            verifier: verifier,
            appKey: appKey,
            redirectURI: redirectURI
        )
        guard let refreshToken = tokenResponse.refreshToken,
              !refreshToken.isEmpty,
              let accountID = tokenResponse.accountID,
              !accountID.isEmpty else {
            throw DropboxAuthenticationError.reauthenticationRequired
        }
        let account = try await fetchCurrentAccount(accessToken: tokenResponse.accessToken)
        guard account.accountID == accountID else {
            throw DropboxAuthenticationError.accountMismatch
        }
        await tokenService.adopt(
            accessToken: tokenResponse.accessToken,
            expiresIn: tokenResponse.expiresIn,
            appKey: appKey,
            accountID: accountID
        )
        let username = account.email?.isEmpty == false ? account.email : account.name.displayName
        return DropboxInteractiveSignInResult(
            connectionParams: DropboxConnectionParams(
                appKey: appKey,
                accountID: accountID,
                displayRootPath: DropboxConnectionParams.appFolderDisplayPath
            ),
            credential: DropboxCredentialBlob(
                accountID: accountID,
                refreshToken: refreshToken
            ),
            username: username
        )
    }

    static func authorizationURL(
        appKey: String,
        redirectURI: String,
        state: String,
        challenge: String,
        forceReauthentication: Bool
    ) throws -> URL {
        guard var components = URLComponents(string: "https://www.dropbox.com/oauth2/authorize") else {
            throw DropboxAuthenticationError.configurationMissing
        }
        var queryItems = [
            URLQueryItem(name: "client_id", value: appKey),
            URLQueryItem(name: "response_type", value: "code"),
            URLQueryItem(name: "redirect_uri", value: redirectURI),
            URLQueryItem(name: "state", value: state),
            URLQueryItem(name: "code_challenge", value: challenge),
            URLQueryItem(name: "code_challenge_method", value: "S256"),
            URLQueryItem(name: "token_access_type", value: "offline"),
            URLQueryItem(name: "scope", value: Self.scopes.joined(separator: " "))
        ]
        if forceReauthentication {
            queryItems.append(URLQueryItem(name: "force_reauthentication", value: "true"))
        }
        components.queryItems = queryItems
        guard let url = components.url else { throw DropboxAuthenticationError.configurationMissing }
        return url
    }

    func cancelInteractiveSignIn() {
        currentWebSession?.cancel()
        currentWebSession = nil
        currentPresentationProvider = nil
    }

    private func runWebAuthentication(
        url: URL,
        callbackScheme: String,
        presentationProvider: DropboxAuthenticationPresentationProvider
    ) async throws -> URL {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let webSession = ASWebAuthenticationSession(
                    url: url,
                    callbackURLScheme: callbackScheme
                ) { [weak self] callbackURL, error in
                    Task { @MainActor in
                        self?.currentWebSession = nil
                        self?.currentPresentationProvider = nil
                        if let callbackURL {
                            continuation.resume(returning: callbackURL)
                        } else if let ns = error as NSError?,
                                  ns.domain == ASWebAuthenticationSessionError.errorDomain,
                                  ns.code == ASWebAuthenticationSessionError.canceledLogin.rawValue {
                            continuation.resume(throwing: CancellationError())
                        } else {
                            continuation.resume(throwing: DropboxAuthenticationError.reauthenticationRequired)
                        }
                    }
                }
                webSession.presentationContextProvider = presentationProvider
                webSession.prefersEphemeralWebBrowserSession = false
                currentWebSession = webSession
                currentPresentationProvider = presentationProvider
                guard webSession.start() else {
                    currentWebSession = nil
                    currentPresentationProvider = nil
                    continuation.resume(throwing: DropboxAuthenticationError.configurationMissing)
                    return
                }
            }
        } onCancel: {
            Task { @MainActor [weak self] in self?.cancelInteractiveSignIn() }
        }
    }

    private func fetchCurrentAccount(accessToken: String) async throws -> DropboxCurrentAccount {
        guard let url = URL(string: "https://api.dropboxapi.com/2/users/get_current_account") else {
            throw DropboxAuthenticationError.configurationMissing
        }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = Data("null".utf8)
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw DropboxAuthenticationError.invalidResponse
        }
        guard http.statusCode == 200 else {
            throw DropboxErrorClassifier.makeServiceError(
                data: data,
                response: http,
                endpoint: "users/get_current_account"
            )
        }
        do {
            return try JSONDecoder().decode(DropboxCurrentAccount.self, from: data)
        } catch {
            throw DropboxAuthenticationError.invalidResponse
        }
    }

    private func configuredAppKey() throws -> String {
        guard let raw = Bundle.main.object(forInfoDictionaryKey: "DropboxAppKey") as? String else {
            throw DropboxAuthenticationError.configurationMissing
        }
        let appKey = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !appKey.isEmpty,
              !appKey.contains("$("),
              appKey != "DROPBOX_APP_KEY" else {
            throw DropboxAuthenticationError.configurationMissing
        }
        return appKey
    }

    private static func makeVerifier() throws -> String {
        var bytes = [UInt8](repeating: 0, count: 32)
        guard SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes) == errSecSuccess else {
            throw DropboxAuthenticationError.configurationMissing
        }
        return base64URL(Data(bytes))
    }

    private static func base64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}

private final class DropboxAuthenticationPresentationProvider: NSObject, ASWebAuthenticationPresentationContextProviding {
    private let window: UIWindow

    init(window: UIWindow) {
        self.window = window
    }

    func presentationAnchor(for _: ASWebAuthenticationSession) -> ASPresentationAnchor {
        window
    }
}

import AuthenticationServices
import CryptoKit
import Foundation
import Security
import UIKit

nonisolated struct GoogleDriveInteractiveSignInResult: Sendable {
    let clientID: String
    let credential: GoogleDriveCredentialBlob
    let accountDisplayName: String?
    let refreshTokenExpiresAt: Date?
}

private struct GoogleDriveOAuthTokenResponse: Decodable {
    let accessToken: String
    let expiresIn: TimeInterval
    let refreshToken: String?
    let refreshTokenExpiresIn: TimeInterval?

    private enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
        case expiresIn = "expires_in"
        case refreshToken = "refresh_token"
        case refreshTokenExpiresIn = "refresh_token_expires_in"
    }
}

nonisolated private final class GoogleDriveTokenTaskWaiter: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<GoogleDriveAccessToken, Error>?
    private var finished = false

    func install(_ continuation: CheckedContinuation<GoogleDriveAccessToken, Error>) {
        let shouldCancel = lock.withLock { () -> Bool in
            guard !finished else { return true }
            self.continuation = continuation
            return false
        }
        if shouldCancel { continuation.resume(throwing: CancellationError()) }
    }

    func cancel() {
        let continuation = lock.withLock { () -> CheckedContinuation<GoogleDriveAccessToken, Error>? in
            guard !finished else { return nil }
            finished = true
            defer { self.continuation = nil }
            return self.continuation
        }
        continuation?.resume(throwing: CancellationError())
    }

    func resolve(_ result: Result<GoogleDriveAccessToken, Error>) {
        let continuation = lock.withLock { () -> CheckedContinuation<GoogleDriveAccessToken, Error>? in
            guard !finished else { return nil }
            finished = true
            defer { self.continuation = nil }
            return self.continuation
        }
        continuation?.resume(with: result)
    }
}

nonisolated struct GoogleDriveUserInfo: Decodable, Sendable {
    let subject: String
    let email: String?
    let name: String?

    private enum CodingKeys: String, CodingKey {
        case subject = "sub"
        case email
        case name
    }
}

actor GoogleDriveTokenService: GoogleDriveAccessTokenProviding {
    private struct CacheKey: Hashable {
        let clientID: String
        let accountSubject: String
    }

    private let session: URLSession
    private var cachedTokens: [CacheKey: GoogleDriveAccessToken] = [:]
    private var refreshTasks: [CacheKey: Task<GoogleDriveAccessToken, Error>] = [:]

    init(sessionConfiguration: URLSessionConfiguration? = nil) {
        let configuration = sessionConfiguration?.copy() as? URLSessionConfiguration ?? .ephemeral
        configuration.timeoutIntervalForRequest = 120
        configuration.timeoutIntervalForResource = 120
        configuration.urlCache = nil
        configuration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        session = URLSession(configuration: configuration)
    }

    func accessToken(
        for credential: GoogleDriveCredentialBlob,
        clientID: String,
        forceRefresh: Bool
    ) async throws -> GoogleDriveAccessToken {
        guard GoogleDriveOAuthClientConfiguration.isValidClientID(clientID) else {
            throw GoogleDriveAuthenticationError.invalidClientID
        }
        let key = CacheKey(clientID: clientID.lowercased(), accountSubject: credential.accountSubject)
        if !forceRefresh, let cached = cachedTokens[key], cached.isUsable() {
            return cached
        }
        if let task = refreshTasks[key] {
            return try await Self.waitForRefresh(task)
        }
        let task = Task { [self] in
            try await refreshAccessToken(credential: credential, clientID: clientID)
        }
        refreshTasks[key] = task
        Task { [weak self] in
            do {
                let token = try await task.value
                await self?.finishRefresh(key: key, token: token)
            } catch {
                await self?.finishRefresh(key: key, token: nil)
            }
        }
        return try await Self.waitForRefresh(task)
    }

    private func finishRefresh(key: CacheKey, token: GoogleDriveAccessToken?) {
        refreshTasks.removeValue(forKey: key)
        if let token { cachedTokens[key] = token }
    }

    nonisolated private static func waitForRefresh(
        _ task: Task<GoogleDriveAccessToken, Error>
    ) async throws -> GoogleDriveAccessToken {
        let waiter = GoogleDriveTokenTaskWaiter()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                waiter.install(continuation)
                Task {
                    do {
                        waiter.resolve(.success(try await task.value))
                    } catch {
                        waiter.resolve(.failure(error))
                    }
                }
            }
        } onCancel: {
            waiter.cancel()
        }
    }

    fileprivate func exchangeAuthorizationCode(
        _ code: String,
        verifier: String,
        clientID: String,
        redirectURI: String
    ) async throws -> GoogleDriveOAuthTokenResponse {
        try await requestToken(parameters: [
            URLQueryItem(name: "client_id", value: clientID),
            URLQueryItem(name: "code", value: code),
            URLQueryItem(name: "code_verifier", value: verifier),
            URLQueryItem(name: "grant_type", value: "authorization_code"),
            URLQueryItem(name: "redirect_uri", value: redirectURI)
        ])
    }

    fileprivate func adopt(
        accessToken: String,
        expiresIn: TimeInterval,
        clientID: String,
        accountSubject: String
    ) {
        let key = CacheKey(clientID: clientID.lowercased(), accountSubject: accountSubject)
        cachedTokens[key] = GoogleDriveAccessToken(
            value: accessToken,
            expiresAt: Date().addingTimeInterval(expiresIn)
        )
    }

    fileprivate func fetchUserInfo(accessToken: String) async throws -> GoogleDriveUserInfo {
        var request = URLRequest(url: URL(string: "https://openidconnect.googleapis.com/v1/userinfo")!)
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        guard http.statusCode == 200 else {
            if http.statusCode == 401 { throw GoogleDriveAuthenticationError.reauthenticationRequired }
            throw GoogleDriveErrorClassifier.makeServiceError(data: data, response: http)
        }
        do {
            let info = try JSONDecoder().decode(GoogleDriveUserInfo.self, from: data)
            guard !info.subject.isEmpty else { throw GoogleDriveAuthenticationError.invalidResponse }
            return info
        } catch let error as GoogleDriveAuthenticationError {
            throw error
        } catch {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
    }

    private func refreshAccessToken(
        credential: GoogleDriveCredentialBlob,
        clientID: String
    ) async throws -> GoogleDriveAccessToken {
        let response = try await requestToken(parameters: [
            URLQueryItem(name: "client_id", value: clientID),
            URLQueryItem(name: "refresh_token", value: credential.refreshToken),
            URLQueryItem(name: "grant_type", value: "refresh_token")
        ])
        let token = GoogleDriveAccessToken(
            value: response.accessToken,
            expiresAt: Date().addingTimeInterval(response.expiresIn)
        )
        guard token.isUsable() else { throw GoogleDriveAuthenticationError.invalidResponse }
        return token
    }

    private func requestToken(parameters: [URLQueryItem]) async throws -> GoogleDriveOAuthTokenResponse {
        var form = URLComponents()
        form.queryItems = parameters
        var request = URLRequest(url: URL(string: "https://oauth2.googleapis.com/token")!)
        request.httpMethod = "POST"
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")
        request.httpBody = form.percentEncodedQuery?.data(using: .utf8)
        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw GoogleDriveErrorClassifier.isConnectionUnavailable(error)
                ? error
                : RemoteStorageClientError.unavailable
        }
        guard let http = response as? HTTPURLResponse else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        guard http.statusCode == 200 else {
            if http.statusCode == 400 || http.statusCode == 401 {
                throw GoogleDriveAuthenticationError.reauthenticationRequired
            }
            throw GoogleDriveErrorClassifier.makeServiceError(data: data, response: http)
        }
        do {
            return try JSONDecoder().decode(GoogleDriveOAuthTokenResponse.self, from: data)
        } catch {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
    }
}

@MainActor
final class GoogleDriveOAuthService {
    private static let scopes = [
        "openid",
        "email",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive.appdata"
    ]

    private let tokenService: GoogleDriveTokenService
    private var currentWebSession: ASWebAuthenticationSession?
    private var currentPresentationProvider: GoogleDriveAuthenticationPresentationProvider?

    nonisolated init(tokenService: GoogleDriveTokenService) {
        self.tokenService = tokenService
    }

    func signIn(
        clientID rawClientID: String,
        from parent: UIViewController,
        forceReauthentication: Bool
    ) async throws -> GoogleDriveInteractiveSignInResult {
        let clientID = rawClientID.trimmingCharacters(in: .whitespacesAndNewlines)
        let callbackScheme = try GoogleDriveOAuthClientConfiguration.callbackScheme(for: clientID)
        let redirectURI = try GoogleDriveOAuthClientConfiguration.redirectURI(for: clientID)
        let verifier = try Self.makeVerifier()
        let state = UUID().uuidString.lowercased()
        let challenge = Self.base64URL(Data(SHA256.hash(data: Data(verifier.utf8))))
        let authorizationURL = try Self.authorizationURL(
            clientID: clientID,
            redirectURI: redirectURI,
            state: state,
            challenge: challenge,
            forceReauthentication: forceReauthentication
        )
        guard let window = parent.viewIfLoaded?.window else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        let provider = GoogleDriveAuthenticationPresentationProvider(window: window)
        let callbackURL = try await runWebAuthentication(
            url: authorizationURL,
            callbackScheme: callbackScheme,
            presentationProvider: provider
        )
        guard let callback = URLComponents(url: callbackURL, resolvingAgainstBaseURL: false),
              callback.scheme == callbackScheme,
              callback.path == "/oauth2redirect",
              callback.queryItems?.first(where: { $0.name == "state" })?.value == state else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        if callback.queryItems?.contains(where: { $0.name == "error" && $0.value == "access_denied" }) == true {
            throw CancellationError()
        }
        guard let code = callback.queryItems?.first(where: { $0.name == "code" })?.value,
              !code.isEmpty else {
            throw GoogleDriveAuthenticationError.reauthenticationRequired
        }
        let tokenResponse = try await tokenService.exchangeAuthorizationCode(
            code,
            verifier: verifier,
            clientID: clientID,
            redirectURI: redirectURI
        )
        guard let refreshToken = tokenResponse.refreshToken, !refreshToken.isEmpty else {
            throw GoogleDriveAuthenticationError.reauthenticationRequired
        }
        let userInfo = try await tokenService.fetchUserInfo(accessToken: tokenResponse.accessToken)
        await tokenService.adopt(
            accessToken: tokenResponse.accessToken,
            expiresIn: tokenResponse.expiresIn,
            clientID: clientID,
            accountSubject: userInfo.subject
        )
        return GoogleDriveInteractiveSignInResult(
            clientID: clientID,
            credential: GoogleDriveCredentialBlob(
                accountSubject: userInfo.subject,
                refreshToken: refreshToken
            ),
            accountDisplayName: userInfo.email ?? userInfo.name,
            refreshTokenExpiresAt: tokenResponse.refreshTokenExpiresIn.map { Date().addingTimeInterval($0) }
        )
    }

    static func authorizationURL(
        clientID: String,
        redirectURI: String,
        state: String,
        challenge: String,
        forceReauthentication: Bool
    ) throws -> URL {
        guard GoogleDriveOAuthClientConfiguration.isValidClientID(clientID),
              var components = URLComponents(string: "https://accounts.google.com/o/oauth2/v2/auth") else {
            throw GoogleDriveAuthenticationError.invalidClientID
        }
        components.queryItems = [
            URLQueryItem(name: "client_id", value: clientID),
            URLQueryItem(name: "redirect_uri", value: redirectURI),
            URLQueryItem(name: "response_type", value: "code"),
            URLQueryItem(name: "scope", value: scopes.joined(separator: " ")),
            URLQueryItem(name: "access_type", value: "offline"),
            URLQueryItem(name: "prompt", value: forceReauthentication ? "select_account consent" : "consent"),
            URLQueryItem(name: "state", value: state),
            URLQueryItem(name: "code_challenge", value: challenge),
            URLQueryItem(name: "code_challenge_method", value: "S256")
        ]
        guard let url = components.url else { throw GoogleDriveAuthenticationError.invalidResponse }
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
        presentationProvider: GoogleDriveAuthenticationPresentationProvider
    ) async throws -> URL {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let session = ASWebAuthenticationSession(
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
                            continuation.resume(throwing: GoogleDriveAuthenticationError.reauthenticationRequired)
                        }
                    }
                }
                session.presentationContextProvider = presentationProvider
                session.prefersEphemeralWebBrowserSession = false
                currentWebSession = session
                currentPresentationProvider = presentationProvider
                guard session.start() else {
                    currentWebSession = nil
                    currentPresentationProvider = nil
                    continuation.resume(throwing: GoogleDriveAuthenticationError.invalidResponse)
                    return
                }
            }
        } onCancel: {
            Task { @MainActor [weak self] in self?.cancelInteractiveSignIn() }
        }
    }

    private static func makeVerifier() throws -> String {
        var bytes = [UInt8](repeating: 0, count: 32)
        guard SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes) == errSecSuccess else {
            throw GoogleDriveAuthenticationError.invalidResponse
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

private final class GoogleDriveAuthenticationPresentationProvider: NSObject,
    ASWebAuthenticationPresentationContextProviding {
    private let window: UIWindow

    init(window: UIWindow) {
        self.window = window
    }

    func presentationAnchor(for _: ASWebAuthenticationSession) -> ASPresentationAnchor {
        window
    }
}

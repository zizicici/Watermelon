import Foundation
import os.log

private let oneDriveGraphLog = Logger(subsystem: "com.zizicici.watermelon", category: "OneDriveGraph")

nonisolated final class OneDriveRedirectDelegate: URLSessionStallWatchdog.Delegate, @unchecked Sendable {
    func urlSession(
        _: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection _: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        guard request.url?.scheme?.lowercased() == "https",
              request.url?.user == nil,
              request.url?.password == nil else {
            completionHandler(nil)
            return
        }
        var redirected = request
        if Self.origin(of: task.originalRequest?.url) != Self.origin(of: request.url) {
            redirected.setValue(nil, forHTTPHeaderField: "Authorization")
        }
        completionHandler(redirected)
    }

    private static func origin(of url: URL?) -> String? {
        guard let url else { return nil }
        return "\(url.scheme?.lowercased() ?? "")://\(url.host?.lowercased() ?? ""):\(url.port ?? defaultPort(for: url.scheme))"
    }

    private static func defaultPort(for scheme: String?) -> Int {
        scheme?.lowercased() == "http" ? 80 : 443
    }
}

actor OneDriveGraphTransport {
    private static let requestTimeout: TimeInterval = 120
    private static let transferTimeout: TimeInterval = 7 * 24 * 60 * 60
    private static let defaultStallTimeouts = URLSessionStallWatchdog.Timeouts(
        uploadBodyStall: 3 * 60,
        uploadResponseStall: 5 * 60,
        downloadFirstByte: 5 * 60,
        downloadStall: 3 * 60,
        pollInterval: 5
    )

    private let credential: OneDriveCredentialBlob
    private let tokenProvider: any OneDriveAccessTokenProviding
    private let sharedState: OneDriveSharedState
    private let graphBaseURL: URL
    private let redirectDelegate: OneDriveRedirectDelegate
    private let stallTimeouts: URLSessionStallWatchdog.Timeouts
    private let session: URLSession
    nonisolated private let tasks = URLSessionTaskRegistry()
    private var cachedToken: OneDriveAccessToken?

    init(
        credential: OneDriveCredentialBlob,
        tokenProvider: any OneDriveAccessTokenProviding,
        sharedState: OneDriveSharedState,
        graphBaseURL: URL,
        sessionConfiguration: URLSessionConfiguration? = nil,
        stallTimeouts: URLSessionStallWatchdog.Timeouts? = nil
    ) {
        self.credential = credential
        self.tokenProvider = tokenProvider
        self.sharedState = sharedState
        self.graphBaseURL = graphBaseURL
        self.stallTimeouts = stallTimeouts ?? Self.defaultStallTimeouts
        redirectDelegate = OneDriveRedirectDelegate()

        let configuration = sessionConfiguration?.copy() as? URLSessionConfiguration ?? .ephemeral
        configuration.timeoutIntervalForRequest = Self.requestTimeout
        configuration.timeoutIntervalForResource = Self.transferTimeout
        configuration.urlCache = nil
        configuration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        session = URLSession(configuration: configuration, delegate: redirectDelegate, delegateQueue: nil)
    }

    deinit {
        session.invalidateAndCancel()
    }

    nonisolated func cancelActiveOperations() {
        tasks.cancelAll()
    }

    func resetAuthentication() {
        cachedToken = nil
    }

    func performGraph(
        method: String,
        url: URL,
        headers: [String: String] = [:],
        body: Data? = nil,
        expected: Set<Int>,
        waitForThrottle: Bool = false
    ) async throws -> (Data, HTTPURLResponse) {
        try Self.validateGraphURL(url, baseURL: graphBaseURL)
        let totalStart = CFAbsoluteTimeGetCurrent()
        let throttleStart = CFAbsoluteTimeGetCurrent()
        try await sharedState.throttleGate.waitForPermit()
        let throttleSeconds = Self.elapsedSeconds(since: throttleStart)
        var forceRefresh = false
        var claims: String?
        for attempt in 0 ... 1 {
            let tokenStart = CFAbsoluteTimeGetCurrent()
            let token = try await accessToken(forceRefresh: forceRefresh, claims: claims)
            let tokenSeconds = Self.elapsedSeconds(since: tokenStart)
            let clientRequestID = UUID().uuidString
            var request = URLRequest(url: url)
            request.httpMethod = method
            request.timeoutInterval = Self.requestTimeout
            request.setValue("Bearer \(token.value)", forHTTPHeaderField: "Authorization")
            request.setValue(clientRequestID, forHTTPHeaderField: "client-request-id")
            request.setValue("true", forHTTPHeaderField: "return-client-request-id")
            if body != nil { request.setValue("application/json", forHTTPHeaderField: "Content-Type") }
            for (name, value) in headers { request.setValue(value, forHTTPHeaderField: name) }
            request.httpBody = body
            let transportStart = CFAbsoluteTimeGetCurrent()
            let data: Data
            let response: HTTPURLResponse
            do {
                (data, response) = try await transportData(for: request)
            } catch {
                Self.logHTTPTrace(
                    channel: "graph",
                    method: method,
                    url: url,
                    status: nil,
                    attempt: attempt + 1,
                    requestBytes: body?.count ?? 0,
                    responseBytes: nil,
                    totalSeconds: Self.elapsedSeconds(since: totalStart),
                    throttleSeconds: throttleSeconds,
                    tokenSeconds: tokenSeconds,
                    transportSeconds: Self.elapsedSeconds(since: transportStart),
                    requestID: nil,
                    clientRequestID: clientRequestID,
                    error: error
                )
                throw error
            }
            Self.logHTTPTrace(
                channel: "graph",
                method: method,
                url: response.url ?? url,
                status: response.statusCode,
                attempt: attempt + 1,
                requestBytes: body?.count ?? 0,
                responseBytes: data.count,
                totalSeconds: Self.elapsedSeconds(since: totalStart),
                throttleSeconds: throttleSeconds,
                tokenSeconds: tokenSeconds,
                transportSeconds: Self.elapsedSeconds(since: transportStart),
                requestID: Self.requestID(from: response),
                clientRequestID: Self.clientRequestID(from: response) ?? clientRequestID,
                error: nil
            )
            if response.statusCode == 401, attempt == 0 {
                cachedToken = nil
                forceRefresh = true
                claims = Self.claims(from: response)
                continue
            }
            try await validate(data: data, response: response, expected: expected)
            return (data, response)
        }
        throw OneDriveAuthenticationError.reauthenticationRequired
    }

    func performGraphDownload(
        url: URL,
        onProgress: ((Double) -> Void)?
    ) async throws -> URL {
        try Self.validateGraphURL(url, baseURL: graphBaseURL)
        let totalStart = CFAbsoluteTimeGetCurrent()
        let throttleStart = CFAbsoluteTimeGetCurrent()
        try await sharedState.throttleGate.waitForPermit()
        let throttleSeconds = Self.elapsedSeconds(since: throttleStart)
        var forceRefresh = false
        var claims: String?
        for attempt in 0 ... 1 {
            let tokenStart = CFAbsoluteTimeGetCurrent()
            let token = try await accessToken(forceRefresh: forceRefresh, claims: claims)
            let tokenSeconds = Self.elapsedSeconds(since: tokenStart)
            let clientRequestID = UUID().uuidString
            var request = URLRequest(url: url)
            request.httpMethod = "GET"
            request.setValue("Bearer \(token.value)", forHTTPHeaderField: "Authorization")
            request.setValue(clientRequestID, forHTTPHeaderField: "client-request-id")
            request.setValue("true", forHTTPHeaderField: "return-client-request-id")
            let transportStart = CFAbsoluteTimeGetCurrent()
            let temporaryURL: URL
            let response: HTTPURLResponse
            do {
                (temporaryURL, response) = try await transportDownload(
                    for: request,
                    onProgress: onProgress
                )
            } catch {
                Self.logHTTPTrace(
                    channel: "graph-download",
                    method: "GET",
                    url: url,
                    status: nil,
                    attempt: attempt + 1,
                    requestBytes: 0,
                    responseBytes: nil,
                    totalSeconds: Self.elapsedSeconds(since: totalStart),
                    throttleSeconds: throttleSeconds,
                    tokenSeconds: tokenSeconds,
                    transportSeconds: Self.elapsedSeconds(since: transportStart),
                    requestID: nil,
                    clientRequestID: clientRequestID,
                    error: error
                )
                throw error
            }
            let responseBytes = (try? FileManager.default.attributesOfItem(atPath: temporaryURL.path)[.size] as? NSNumber)?.int64Value
            Self.logHTTPTrace(
                channel: "graph-download",
                method: "GET",
                url: response.url ?? url,
                status: response.statusCode,
                attempt: attempt + 1,
                requestBytes: 0,
                responseBytes: responseBytes.map(Int.init),
                totalSeconds: Self.elapsedSeconds(since: totalStart),
                throttleSeconds: throttleSeconds,
                tokenSeconds: tokenSeconds,
                transportSeconds: Self.elapsedSeconds(since: transportStart),
                requestID: Self.requestID(from: response),
                clientRequestID: Self.clientRequestID(from: response) ?? clientRequestID,
                error: nil
            )
            if response.statusCode == 401, attempt == 0 {
                try? FileManager.default.removeItem(at: temporaryURL)
                cachedToken = nil
                forceRefresh = true
                claims = Self.claims(from: response)
                continue
            }
            guard response.statusCode == 200 else {
                let errorData = (try? Data(contentsOf: temporaryURL)) ?? Data()
                try? FileManager.default.removeItem(at: temporaryURL)
                try await validate(data: errorData, response: response, expected: [200])
                throw URLError(.badServerResponse)
            }
            return temporaryURL
        }
        throw OneDriveAuthenticationError.reauthenticationRequired
    }

    func performPreauthenticated(
        method: String,
        url: URL,
        headers: [String: String] = [:],
        body: Data? = nil,
        expected: Set<Int>
    ) async throws -> (Data, HTTPURLResponse) {
        try Self.validateOpaqueHTTPSURL(url)
        let totalStart = CFAbsoluteTimeGetCurrent()
        let throttleStart = CFAbsoluteTimeGetCurrent()
        try await sharedState.throttleGate.waitForPermit()
        let throttleSeconds = Self.elapsedSeconds(since: throttleStart)
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.timeoutInterval = Self.transferTimeout
        for (name, value) in headers { request.setValue(value, forHTTPHeaderField: name) }
        let result: (Data, HTTPURLResponse)
        let transportStart = CFAbsoluteTimeGetCurrent()
        if let body {
            do {
                result = try await transportUpload(for: request, body: body)
            } catch {
                Self.logHTTPTrace(
                    channel: "upload-session",
                    method: method,
                    url: url,
                    status: nil,
                    attempt: 1,
                    requestBytes: body.count,
                    responseBytes: nil,
                    totalSeconds: Self.elapsedSeconds(since: totalStart),
                    throttleSeconds: throttleSeconds,
                    tokenSeconds: nil,
                    transportSeconds: Self.elapsedSeconds(since: transportStart),
                    requestID: nil,
                    clientRequestID: nil,
                    error: error
                )
                throw error
            }
        } else {
            do {
                result = try await transportResponseData(for: request)
            } catch {
                Self.logHTTPTrace(
                    channel: "upload-session",
                    method: method,
                    url: url,
                    status: nil,
                    attempt: 1,
                    requestBytes: 0,
                    responseBytes: nil,
                    totalSeconds: Self.elapsedSeconds(since: totalStart),
                    throttleSeconds: throttleSeconds,
                    tokenSeconds: nil,
                    transportSeconds: Self.elapsedSeconds(since: transportStart),
                    requestID: nil,
                    clientRequestID: nil,
                    error: error
                )
                throw error
            }
        }
        Self.logHTTPTrace(
            channel: "upload-session",
            method: method,
            url: result.1.url ?? url,
            status: result.1.statusCode,
            attempt: 1,
            requestBytes: body?.count ?? 0,
            responseBytes: result.0.count,
            totalSeconds: Self.elapsedSeconds(since: totalStart),
            throttleSeconds: throttleSeconds,
            tokenSeconds: nil,
            transportSeconds: Self.elapsedSeconds(since: transportStart),
            requestID: Self.requestID(from: result.1),
            clientRequestID: Self.clientRequestID(from: result.1),
            error: nil
        )
        try await validate(data: result.0, response: result.1, expected: expected)
        return result
    }

    func performUnauthenticated(
        method: String,
        url: URL,
        expected: Set<Int>
    ) async throws -> (Data, HTTPURLResponse) {
        try Self.validateOpaqueHTTPSURL(url)
        let totalStart = CFAbsoluteTimeGetCurrent()
        let throttleStart = CFAbsoluteTimeGetCurrent()
        try await sharedState.throttleGate.waitForPermit()
        let throttleSeconds = Self.elapsedSeconds(since: throttleStart)
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.timeoutInterval = Self.requestTimeout
        let transportStart = CFAbsoluteTimeGetCurrent()
        let data: Data
        let response: HTTPURLResponse
        do {
            (data, response) = try await transportData(for: request)
        } catch {
            Self.logHTTPTrace(
                channel: "unauthenticated",
                method: method,
                url: url,
                status: nil,
                attempt: 1,
                requestBytes: 0,
                responseBytes: nil,
                totalSeconds: Self.elapsedSeconds(since: totalStart),
                throttleSeconds: throttleSeconds,
                tokenSeconds: nil,
                transportSeconds: Self.elapsedSeconds(since: transportStart),
                requestID: nil,
                clientRequestID: nil,
                error: error
            )
            throw error
        }
        Self.logHTTPTrace(
            channel: "unauthenticated",
            method: method,
            url: response.url ?? url,
            status: response.statusCode,
            attempt: 1,
            requestBytes: 0,
            responseBytes: data.count,
            totalSeconds: Self.elapsedSeconds(since: totalStart),
            throttleSeconds: throttleSeconds,
            tokenSeconds: nil,
            transportSeconds: Self.elapsedSeconds(since: transportStart),
            requestID: Self.requestID(from: response),
            clientRequestID: Self.clientRequestID(from: response),
            error: nil
        )
        try await validate(data: data, response: response, expected: expected)
        return (data, response)
    }

    private func accessToken(forceRefresh: Bool, claims: String?) async throws -> OneDriveAccessToken {
        if !forceRefresh, let cachedToken, cachedToken.isUsable() {
            return cachedToken
        }
        let token = try await tokenProvider.accessToken(
            for: credential,
            forceRefresh: forceRefresh,
            claims: claims
        )
        guard token.isUsable() else { throw OneDriveAuthenticationError.reauthenticationRequired }
        cachedToken = token
        return token
    }

    private func validate(data: Data, response: HTTPURLResponse, expected: Set<Int>) async throws {
        guard !expected.contains(response.statusCode) else { return }
        let retryAfter = OneDriveErrorClassifier.retryAfter(from: response)
        if OneDriveErrorClassifier.isRetryableStatus(response.statusCode), let retryAfter {
            await sharedState.throttleGate.record(retryAfter: retryAfter)
        }
        Self.logGraphFailure(data: data, response: response)
        throw OneDriveErrorClassifier.makeServiceError(
            statusCode: response.statusCode,
            code: OneDriveJSON.errorCode(from: data),
            message: OneDriveJSON.errorMessage(from: data),
            retryAfter: retryAfter,
            claims: Self.claims(from: response)
        )
    }

    nonisolated private static func logGraphFailure(data: Data, response: HTTPURLResponse) {
        let code = OneDriveJSON.errorCode(from: data) ?? "-"
        let message = OneDriveJSON.errorMessage(from: data) ?? "-"
        let requestID = requestID(from: response) ?? "-"
        let clientRequestID = clientRequestID(from: response) ?? "-"
        let relativeURL = sanitizedRelativeURL(response.url)
        if response.statusCode == 404 {
            oneDriveGraphLog.debug(
                "Graph notFound status=\(response.statusCode, privacy: .public) code=\(code, privacy: .public) message=\(message, privacy: .public) requestID=\(requestID, privacy: .public) clientRequestID=\(clientRequestID, privacy: .public) url=\(relativeURL, privacy: .public)"
            )
            return
        }
        oneDriveGraphLog.error(
            "Graph failure status=\(response.statusCode, privacy: .public) code=\(code, privacy: .public) message=\(message, privacy: .public) requestID=\(requestID, privacy: .public) clientRequestID=\(clientRequestID, privacy: .public) url=\(relativeURL, privacy: .public)"
        )
        #if DEBUG
        print("[OneDriveGraph] status=\(response.statusCode) code=\(code) message=\(message) requestID=\(requestID) clientRequestID=\(clientRequestID) url=\(relativeURL)")
        #endif
    }

    nonisolated private static func logHTTPTrace(
        channel: String,
        method: String,
        url: URL,
        status: Int?,
        attempt: Int,
        requestBytes: Int,
        responseBytes: Int?,
        totalSeconds: TimeInterval,
        throttleSeconds: TimeInterval,
        tokenSeconds: TimeInterval?,
        transportSeconds: TimeInterval,
        requestID: String?,
        clientRequestID: String?,
        error: Error?
    ) {
        let statusText = status.map(String.init) ?? "transportError"
        let responseBytesText = responseBytes.map(String.init) ?? "-"
        let tokenText = tokenSeconds.map { Self.msText($0) } ?? "-"
        let requestIDText = requestID ?? "-"
        let clientRequestIDText = clientRequestID ?? "-"
        let relativeURL = sanitizedRelativeURL(url)
        let errorText = error.map { " error=\(Self.errorDescription($0))" } ?? ""
        let message = "[OneDriveTrace] channel=\(channel) method=\(method) status=\(statusText) attempt=\(attempt) totalMs=\(msText(totalSeconds)) throttleMs=\(msText(throttleSeconds)) tokenMs=\(tokenText) transportMs=\(msText(transportSeconds)) requestBytes=\(requestBytes) responseBytes=\(responseBytesText) requestID=\(requestIDText) clientRequestID=\(clientRequestIDText) url=\(relativeURL)\(errorText)"
        #if DEBUG
        print(message)
        #endif
    }

    nonisolated private static func requestID(from response: HTTPURLResponse) -> String? {
        response.value(forHTTPHeaderField: "request-id")
            ?? response.value(forHTTPHeaderField: "x-ms-request-id")
    }

    nonisolated private static func clientRequestID(from response: HTTPURLResponse) -> String? {
        response.value(forHTTPHeaderField: "client-request-id")
            ?? response.value(forHTTPHeaderField: "x-ms-client-request-id")
    }

    nonisolated private static func elapsedSeconds(since start: CFAbsoluteTime) -> TimeInterval {
        max(CFAbsoluteTimeGetCurrent() - start, 0)
    }

    nonisolated private static func msText(_ seconds: TimeInterval) -> String {
        String(format: "%.1f", seconds * 1_000)
    }

    nonisolated private static func errorDescription(_ error: Error) -> String {
        (error as NSError).localizedDescription
    }

    nonisolated private static func sanitizedRelativeURL(_ url: URL?) -> String {
        guard let url,
              var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return "-"
        }
        components.query = nil
        components.fragment = nil
        components.scheme = nil
        components.host = nil
        components.port = nil
        components.user = nil
        components.password = nil
        return components.string ?? url.path
    }

    private func transportData(for request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        do {
            let (data, response) = try await tasks.data(for: request, in: session)
            guard let response = response as? HTTPURLResponse else { throw URLError(.badServerResponse) }
            return (data, response)
        } catch {
            throw OneDriveErrorClassifier.sanitizedTransportError(error)
        }
    }

    private func transportUpload(for request: URLRequest, body: Data) async throws -> (Data, HTTPURLResponse) {
        do {
            return try await URLSessionStallWatchdog.runUpload(
                session: session,
                delegate: redirectDelegate,
                registry: tasks,
                request: request,
                body: .data(body),
                onProgress: nil,
                timeouts: stallTimeouts,
                makeStallError: { _, _, _, _ in URLError(.timedOut) }
            )
        } catch {
            throw OneDriveErrorClassifier.sanitizedTransportError(error)
        }
    }

    private func transportResponseData(for request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        do {
            return try await URLSessionStallWatchdog.runData(
                session: session,
                registry: tasks,
                request: request,
                timeouts: stallTimeouts,
                makeStallError: { _, _, _, _ in URLError(.timedOut) }
            )
        } catch {
            throw OneDriveErrorClassifier.sanitizedTransportError(error)
        }
    }

    private func transportDownload(
        for request: URLRequest,
        onProgress: ((Double) -> Void)?
    ) async throws -> (URL, HTTPURLResponse) {
        do {
            return try await URLSessionStallWatchdog.runDownload(
                session: session,
                registry: tasks,
                request: request,
                onProgress: onProgress,
                timeouts: stallTimeouts,
                makeStallError: { _, _, _, _ in URLError(.timedOut) }
            )
        } catch {
            throw OneDriveErrorClassifier.sanitizedTransportError(error)
        }
    }

    nonisolated static func validateGraphURL(_ url: URL, baseURL: URL) throws {
        guard url.scheme?.lowercased() == "https",
              url.host?.lowercased() == baseURL.host?.lowercased(),
              Self.effectivePort(for: url) == Self.effectivePort(for: baseURL),
              Self.path(url.path, isWithinGraphBasePath: baseURL.path),
              url.user == nil,
              url.password == nil else {
            throw RemoteStorageClientError.invalidConfiguration
        }
    }

    private static func validateOpaqueHTTPSURL(_ url: URL) throws {
        guard url.scheme?.lowercased() == "https", url.user == nil, url.password == nil else {
            throw RemoteStorageClientError.invalidConfiguration
        }
    }

    private static func effectivePort(for url: URL) -> Int {
        url.port ?? (url.scheme?.lowercased() == "http" ? 80 : 443)
    }

    private static func path(_ path: String, isWithinGraphBasePath basePath: String) -> Bool {
        let normalizedBase = basePath.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        guard !normalizedBase.isEmpty else { return true }
        let prefix = "/" + normalizedBase
        return path == prefix || path.hasPrefix(prefix + "/")
    }

    private static func claims(from response: HTTPURLResponse) -> String? {
        guard let header = response.value(forHTTPHeaderField: "WWW-Authenticate"),
              let range = header.range(of: "claims=\"", options: .caseInsensitive) else { return nil }
        let remainder = header[range.upperBound...]
        guard let end = remainder.firstIndex(of: "\"") else { return nil }
        return String(remainder[..<end])
    }
}

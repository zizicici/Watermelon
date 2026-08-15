import Foundation

nonisolated final class GoogleDriveTransport: @unchecked Sendable {
    private static let requestTimeout: TimeInterval = 120
    private static let transferTimeout: TimeInterval = 7 * 24 * 60 * 60
    private static let stallTimeouts = URLSessionStallWatchdog.Timeouts(
        uploadBodyStall: 3 * 60,
        uploadResponseStall: 5 * 60,
        downloadFirstByte: 5 * 60,
        downloadStall: 3 * 60,
        pollInterval: 5
    )

    private let clientID: String
    private let credential: GoogleDriveCredentialBlob

    private let tokenProvider: any GoogleDriveAccessTokenProviding
    private let sharedState: GoogleDriveSharedState
    private let throttleKey: GoogleDriveThrottleGate.Key
    private let session: URLSession
    private let transferSession: URLSession
    private let transferDelegate = URLSessionStallWatchdog.Delegate()
    private let tasks = URLSessionTaskRegistry()

    init(
        clientID: String,
        credential: GoogleDriveCredentialBlob,
        tokenProvider: any GoogleDriveAccessTokenProviding,
        sharedState: GoogleDriveSharedState = GoogleDriveSharedState(),
        sessionConfiguration: URLSessionConfiguration? = nil
    ) {
        self.clientID = clientID
        self.credential = credential
        self.tokenProvider = tokenProvider
        self.sharedState = sharedState
        throttleKey = GoogleDriveThrottleGate.Key(
            clientID: clientID.lowercased(),
            accountSubject: credential.accountSubject
        )

        let metadataConfiguration = sessionConfiguration?.copy() as? URLSessionConfiguration ?? .ephemeral
        metadataConfiguration.timeoutIntervalForRequest = Self.requestTimeout
        metadataConfiguration.timeoutIntervalForResource = Self.requestTimeout
        metadataConfiguration.urlCache = nil
        metadataConfiguration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        session = URLSession(configuration: metadataConfiguration)

        let transferConfiguration = sessionConfiguration?.copy() as? URLSessionConfiguration ?? .ephemeral
        transferConfiguration.timeoutIntervalForRequest = Self.transferTimeout
        transferConfiguration.timeoutIntervalForResource = Self.transferTimeout
        transferConfiguration.urlCache = nil
        transferConfiguration.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        transferSession = URLSession(
            configuration: transferConfiguration,
            delegate: transferDelegate,
            delegateQueue: nil
        )
    }

    deinit {
        session.invalidateAndCancel()
        transferSession.invalidateAndCancel()
    }

    func cancelAll() {
        tasks.cancelAll()
    }

    func data(
        for baseRequest: URLRequest,
        expectedStatusCodes: Set<Int> = [200]
    ) async throws -> (Data, HTTPURLResponse) {
        for attempt in 0 ... 1 {
            try await sharedState.throttleGate.waitForPermit(for: throttleKey)
            let request = try await authorized(baseRequest, forceRefresh: attempt > 0)
            let data: Data
            let response: URLResponse
            do {
                (data, response) = try await tasks.data(for: request, in: session)
            } catch {
                throw Self.sanitizedTransportError(error)
            }
            guard let http = response as? HTTPURLResponse else {
                throw RemoteStorageClientError.unavailable
            }
            if http.statusCode == 401, attempt == 0 { continue }
            guard expectedStatusCodes.contains(http.statusCode) else {
                if let retryAfter = GoogleDriveErrorClassifier.retryAfter(from: http) {
                    await sharedState.throttleGate.record(retryAfter: retryAfter, for: throttleKey)
                }
                throw GoogleDriveErrorClassifier.makeServiceError(data: data, response: http)
            }
            return (data, http)
        }
        throw GoogleDriveAuthenticationError.reauthenticationRequired
    }

    func upload(
        for baseRequest: URLRequest,
        body: URLSessionStallWatchdog.Body,
        expectedStatusCodes: Set<Int>,
        onProgress: ((Double) -> Void)?
    ) async throws -> (Data, HTTPURLResponse) {
        for attempt in 0 ... 1 {
            try await sharedState.throttleGate.waitForPermit(for: throttleKey)
            let request = try await authorized(baseRequest, forceRefresh: attempt > 0)
            let result: (Data, HTTPURLResponse)
            do {
                result = try await URLSessionStallWatchdog.runUpload(
                    session: transferSession,
                    delegate: transferDelegate,
                    registry: tasks,
                    request: request,
                    body: body,
                    onProgress: onProgress,
                    timeouts: Self.stallTimeouts,
                    makeStallError: { _, _, _, _ in URLError(.timedOut) }
                )
            } catch {
                throw Self.sanitizedTransportError(error)
            }
            if result.1.statusCode == 401, attempt == 0 { continue }
            guard expectedStatusCodes.contains(result.1.statusCode) else {
                if let retryAfter = GoogleDriveErrorClassifier.retryAfter(from: result.1) {
                    await sharedState.throttleGate.record(retryAfter: retryAfter, for: throttleKey)
                }
                throw GoogleDriveErrorClassifier.makeServiceError(data: result.0, response: result.1)
            }
            return result
        }
        throw GoogleDriveAuthenticationError.reauthenticationRequired
    }

    func download(
        for baseRequest: URLRequest,
        expectedStatusCodes: Set<Int> = [200],
        onProgress: ((Double) -> Void)?
    ) async throws -> (URL, HTTPURLResponse) {
        for attempt in 0 ... 1 {
            try await sharedState.throttleGate.waitForPermit(for: throttleKey)
            let request = try await authorized(baseRequest, forceRefresh: attempt > 0)
            let temporaryURL: URL
            let response: HTTPURLResponse
            do {
                (temporaryURL, response) = try await URLSessionStallWatchdog.runDownload(
                    session: transferSession,
                    registry: tasks,
                    request: request,
                    onProgress: onProgress,
                    timeouts: Self.stallTimeouts,
                    makeStallError: { _, _, _, _ in URLError(.timedOut) }
                )
            } catch {
                throw Self.sanitizedTransportError(error)
            }
            if response.statusCode == 401, attempt == 0 {
                try? FileManager.default.removeItem(at: temporaryURL)
                continue
            }
            guard expectedStatusCodes.contains(response.statusCode) else {
                let data = (try? Data(contentsOf: temporaryURL)) ?? Data()
                try? FileManager.default.removeItem(at: temporaryURL)
                if let retryAfter = GoogleDriveErrorClassifier.retryAfter(from: response) {
                    await sharedState.throttleGate.record(retryAfter: retryAfter, for: throttleKey)
                }
                throw GoogleDriveErrorClassifier.makeServiceError(data: data, response: response)
            }
            return (temporaryURL, response)
        }
        throw GoogleDriveAuthenticationError.reauthenticationRequired
    }

    func file(id: String) async throws -> GoogleDriveFile? {
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL.appendingPathComponent("files").appendingPathComponent(id),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [URLQueryItem(name: "fields", value: GoogleDriveConstants.fileFields)]
        do {
            let (data, _) = try await data(for: URLRequest(url: components.url!))
            return try GoogleDriveJSON.decodeResponse(GoogleDriveFile.self, from: data)
        } catch {
            if GoogleDriveErrorClassifier.isNotFound(error) { return nil }
            throw error
        }
    }

    func listFiles(
        query: String,
        pageSize: Int = 1_000,
        fields: String = GoogleDriveConstants.fileFields,
        consumeAllPages: Bool = true
    ) async throws -> [GoogleDriveFile] {
        var pageToken: String?
        var files: [GoogleDriveFile] = []
        repeat {
            var components = URLComponents(
                url: GoogleDriveConstants.apiBaseURL.appendingPathComponent("files"),
                resolvingAgainstBaseURL: false
            )!
            var queryItems = [
                URLQueryItem(name: "q", value: query),
                URLQueryItem(name: "spaces", value: "drive"),
                URLQueryItem(name: "pageSize", value: String(pageSize)),
                URLQueryItem(name: "fields", value: "nextPageToken,files(\(fields))")
            ]
            if let pageToken { queryItems.append(URLQueryItem(name: "pageToken", value: pageToken)) }
            components.queryItems = queryItems
            let (data, _) = try await data(for: URLRequest(url: components.url!))
            let page = try GoogleDriveJSON.decodeResponse(GoogleDriveFileList.self, from: data)
            files.append(contentsOf: page.files)
            if !consumeAllPages { return files }
            pageToken = page.nextPageToken
        } while pageToken != nil
        return files
    }

    func generateFileIDs(count: Int) async throws -> [String] {
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL
                .appendingPathComponent("files")
                .appendingPathComponent("generateIds"),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [
            URLQueryItem(name: "count", value: String(count)),
            URLQueryItem(name: "space", value: "drive"),
            URLQueryItem(name: "type", value: "files")
        ]
        let (data, _) = try await data(for: URLRequest(url: components.url!))
        let ids = try GoogleDriveJSON.decodeResponse(GoogleDriveGeneratedIDs.self, from: data).ids
        guard ids.count == count, ids.allSatisfy({ !$0.isEmpty }) else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        return ids
    }

    func createFolder(
        id: String,
        name: String,
        parentID: String,
        appProperties: [String: String]? = nil
    ) async throws -> GoogleDriveFile {
        var metadata: [String: Any] = [
            "id": id,
            "name": name,
            "mimeType": GoogleDriveConstants.folderMIMEType,
            "parents": [parentID]
        ]
        if let appProperties { metadata["appProperties"] = appProperties }
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL.appendingPathComponent("files"),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [URLQueryItem(name: "fields", value: GoogleDriveConstants.fileFields)]
        var request = URLRequest(url: components.url!)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: metadata)
        let (responseData, _) = try await data(for: request, expectedStatusCodes: [200, 201])
        return try GoogleDriveJSON.decodeResponse(GoogleDriveFile.self, from: responseData)
    }

    func hasChildren(parentID: String) async throws -> Bool {
        let files = try await listFiles(
            query: "'\(parentID)' in parents and trashed = false",
            pageSize: 1,
            fields: "id",
            consumeAllPages: false
        )
        return !files.isEmpty
    }

    func deleteFile(id: String) async throws {
        var request = URLRequest(
            url: GoogleDriveConstants.apiBaseURL.appendingPathComponent("files").appendingPathComponent(id)
        )
        request.httpMethod = "DELETE"
        _ = try await data(for: request, expectedStatusCodes: [204])
    }

    private func authorized(_ request: URLRequest, forceRefresh: Bool) async throws -> URLRequest {
        guard GoogleDriveOAuthClientConfiguration.isValidClientID(clientID),
              credential.accountSubject.isEmpty == false else {
            throw GoogleDriveAuthenticationError.invalidClientID
        }
        let token = try await tokenProvider.accessToken(
            for: credential,
            clientID: clientID,
            forceRefresh: forceRefresh
        )
        var request = request
        request.setValue("Bearer \(token.value)", forHTTPHeaderField: "Authorization")
        return request
    }

    private static func sanitizedTransportError(_ error: Error) -> Error {
        if error is CancellationError { return error }
        let ns = error as NSError
        if ns.domain == NSURLErrorDomain, ns.code == NSURLErrorCancelled {
            return CancellationError()
        }
        guard ns.domain == NSURLErrorDomain else { return RemoteStorageClientError.unavailable }
        return NSError(
            domain: NSURLErrorDomain,
            code: ns.code,
            userInfo: [NSLocalizedDescriptionKey: URLError(URLError.Code(rawValue: ns.code)).localizedDescription]
        )
    }
}

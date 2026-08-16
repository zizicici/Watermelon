import XCTest
@testable import Watermelon

final class DropboxClientTests: XCTestCase {
    override func tearDown() {
        DropboxMockURLProtocol.handler = nil
        super.tearDown()
    }

    func testCredentialRoundTripPinsAccountAndRefreshToken() throws {
        let original = DropboxCredentialBlob(accountID: " dbid:account ", refreshToken: " refresh ")

        let decoded = try DropboxCredentialBlob.decode(from: original.encodedJSONString())

        XCTAssertEqual(decoded.accountID, "dbid:account")
        XCTAssertEqual(decoded.refreshToken, "refresh")
    }

    func testProfileCrossesCanonicalIdentityAndFactoryBoundary() throws {
        let params = DropboxConnectionParams(
            appKey: " app-key ",
            accountID: " dbid:account ",
            displayRootPath: "/Watermelon"
        )
        let profile = ServerProfileRecord(
            name: "Dropbox",
            storageType: StorageType.dropbox.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(params),
            sortOrder: 0,
            host: "api.dropboxapi.com",
            port: 443,
            shareName: "dbid:account",
            basePath: "/",
            username: "account@example.com",
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
        let credential = DropboxCredentialBlob(accountID: "dbid:account", refreshToken: "refresh")

        let descriptor = try StorageClientFactory.canonicalConnection(for: profile)
        guard case .dropbox(let connection) = descriptor else {
            return XCTFail("Expected Dropbox descriptor")
        }
        XCTAssertEqual(connection.appKey, "app-key")
        XCTAssertEqual(connection.accountID, "dbid:account")
        XCTAssertEqual(connection.displayRootPath, "/Apps/Watermelon Backup")
        XCTAssertEqual(descriptor.publishedV2IdentityComponents, ["app-key", "dbid:account"])
        let client = try StorageClientFactory(
            dropboxTokenProvider: DropboxTestTokenProvider()
        ).makeClient(
            profile: profile,
            credentialPayload: try credential.encodedJSONString()
        )
        XCTAssertTrue(client is DropboxClient)
    }

    func testClientRejectsLegacyV1Migration() {
        XCTAssertFalse(makeClient().supportsLegacyV1Migration())
        XCTAssertTrue(makeClient().allowsUnattendedOrdinaryWriteConfidence())
    }

    func testListFolderDrainsOpaqueCursor() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            if request.url?.path.hasSuffix("/list_folder/continue") == true {
                return .json("""
                {"entries":[{".tag":"folder","name":"second","path_display":"/second","id":"id:2"}],"cursor":"cursor-2","has_more":false}
                """)
            }
            return .json("""
            {"entries":[{".tag":"file","name":"first.jpg","path_display":"/first.jpg","id":"id:1","client_modified":"2026-01-02T03:04:05Z","server_modified":"2026-01-02T03:04:06Z","size":7}],"cursor":"cursor-1","has_more":true}
            """)
        }
        let client = makeClient()

        let entries = try await client.list(path: "/")

        XCTAssertEqual(entries.map(\.name), ["first.jpg", "second"])
        XCTAssertEqual(entries.map(\.isDirectory), [false, true])
        XCTAssertEqual(recorder.requests.count, 2)
        let continueBody = try XCTUnwrap(recorder.requests.last?.httpBody)
        let json = try XCTUnwrap(JSONSerialization.jsonObject(with: continueBody) as? [String: String])
        XCTAssertEqual(json["cursor"], "cursor-1")
    }

    func testConditionalUploadUsesStrictAddWithoutAutorename() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json("""
            {".tag":"file","name":"lock","path_display":"/.watermelon/locks/writer.lock","id":"id:lock","client_modified":"2026-01-02T03:04:05Z","server_modified":"2026-01-02T03:04:06Z","size":4}
            """)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("lock".utf8).write(to: localURL)
        let clientModified = try XCTUnwrap(ISO8601DateFormatter().date(from: "2020-04-05T06:07:08Z"))
        try FileManager.default.setAttributes([.modificationDate: clientModified], ofItemAtPath: localURL.path)
        defer { try? FileManager.default.removeItem(at: localURL) }
        let client = makeClient()

        try await client.upload(
            localURL: localURL,
            remotePath: "/.watermelon/locks/writer.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let request = try XCTUnwrap(recorder.requests.first)
        XCTAssertEqual(request.url?.path, "/2/files/upload")
        let argument = try XCTUnwrap(request.value(forHTTPHeaderField: "Dropbox-API-Arg"))
        let json = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(argument.utf8)) as? [String: Any]
        )
        XCTAssertEqual(json["mode"] as? String, "add")
        XCTAssertEqual(json["autorename"] as? Bool, false)
        XCTAssertEqual(json["strict_conflict"] as? Bool, true)
        XCTAssertEqual(json["client_modified"] as? String, "2020-04-05T06:07:08Z")
    }

    func testContentArgumentEscapesUnicodePathForHTTPHeader() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json("""
            {".tag":"file","name":"照片.jpg","path_display":"/相册/照片.jpg","id":"id:file","client_modified":"2026-01-02T03:04:05Z","server_modified":"2026-01-02T03:04:06Z","size":4}
            """)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("test".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        try await makeClient().upload(
            localURL: localURL,
            remotePath: "/相册/照片.jpg",
            respectTaskCancellation: true,
            onProgress: nil
        )

        let argument = try XCTUnwrap(
            recorder.requests.first?.value(forHTTPHeaderField: "Dropbox-API-Arg")
        )
        XCTAssertTrue(argument.unicodeScalars.allSatisfy(\.isASCII))
        let json = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(argument.utf8)) as? [String: Any]
        )
        XCTAssertEqual(json["path"] as? String, "/相册/照片.jpg")
    }

    func testMetadataMapsDropboxNotFoundToNil() async throws {
        DropboxMockURLProtocol.handler = { _ in
            .json("{\"error_summary\":\"path/not_found/..\",\"error\":{\".tag\":\"path\"}}", status: 409)
        }

        let entry = try await makeClient().metadata(path: "/missing")

        XCTAssertNil(entry)
    }

    func testCreateDirectoryReturnsAfterOneMetadataRequestWhenTargetExists() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json("""
            {".tag":"folder","name":"months","path_display":"/.watermelon/months","id":"id:months"}
            """)
        }

        try await makeClient().createDirectory(path: "/.watermelon/months")

        XCTAssertEqual(recorder.requests.map(\.url?.lastPathComponent), ["get_metadata"])
    }

    func testConflictParticipatesInGenericCollisionClassification() {
        let error = DropboxErrorClassifier.makeServiceError(
            statusCode: 409,
            summary: "path/conflict/file/.."
        )

        XCTAssertTrue(DropboxErrorClassifier.isNameCollision(error))
        XCTAssertTrue(remoteStorageIsNameCollision(error))
    }

    func testFileAncestorConflictIsNotANameCollision() {
        let error = makeServiceError(json: """
        {
          "error_summary":"path/conflict/file_ancestor/..",
          "error":{".tag":"path","path":{".tag":"conflict","conflict":{".tag":"file_ancestor"}}}
        }
        """)

        XCTAssertFalse(DropboxErrorClassifier.isNameCollision(error))
        XCTAssertFalse(remoteStorageIsNameCollision(error))
    }

    func testSourceWriteConflictIsNotATargetNameCollision() {
        let error = makeServiceError(json: """
        {
          "error_summary":"from_write/conflict/file/..",
          "error":{ ".tag":"from_write", "from_write":{ ".tag":"conflict", "conflict":{ ".tag":"file" } } }
        }
        """)

        XCTAssertFalse(DropboxErrorClassifier.isNameCollision(error))
        XCTAssertFalse(remoteStorageIsNameCollision(error))
    }

    func testUploadSessionNotFoundIsNotRemoteObjectAbsence() {
        let error = makeServiceError(json: """
        {
          "error_summary":"lookup_failed/not_found/..",
          "error":{".tag":"lookup_failed","lookup_failed":{".tag":"not_found"}}
        }
        """)

        XCTAssertFalse(DropboxErrorClassifier.isNotFound(error))
        XCTAssertEqual(RemoteFaultLite.classify(error), .terminal)
    }

    func testEndpointSpecificTooManyWritesIsRetryable() {
        let error = makeServiceError(json: """
        {
          "error_summary":"too_many_write_operations/..",
          "error":{".tag":"too_many_write_operations"}
        }
        """)

        XCTAssertTrue(DropboxErrorClassifier.isConnectionUnavailable(error))
        XCTAssertEqual(RemoteFaultLite.classify(error), .retryable)
    }

    func testBusinessTransportLimitUsesDropboxUserMessage() {
        let error = makeServiceError(
            json: """
            {
              "error_summary":"invalid_account_type/feature/..",
              "error":{".tag":"invalid_account_type","invalid_account_type":{".tag":"feature"}},
              "user_message":{"locale":"en","text":"Monthly data transport limit reached."}
            }
            """,
            status: 403
        )

        XCTAssertTrue(DropboxErrorClassifier.describe(error).contains("Monthly data transport limit reached."))
    }

    func testRetryAfterBlocksSiblingClientForSameAccount() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            if recorder.requests.count == 1 {
                return .json(
                    "{\"error_summary\":\"too_many_requests/..\"}",
                    status: 429,
                    headers: ["Retry-After": "0.08"]
                )
            }
            return .json("{\"entries\":[],\"cursor\":\"done\",\"has_more\":false}")
        }
        let sharedState = DropboxSharedState()
        let firstClient = makeClient(sharedState: sharedState)
        let secondClient = makeClient(sharedState: sharedState)

        do {
            _ = try await firstClient.list(path: "/")
            XCTFail("Expected rate limiting")
        } catch {
            XCTAssertTrue(DropboxErrorClassifier.isConnectionUnavailable(error))
        }
        let start = Date()
        _ = try await secondClient.list(path: "/")

        XCTAssertGreaterThanOrEqual(Date().timeIntervalSince(start), 0.05)
    }

    func testDropboxPathPolicyRepairsTrailingWhitespace() {
        XCTAssertEqual(RemoteFileNamePolicy.dropbox.sanitize("photo.jpg "), "photo.jpg_")
        XCTAssertFalse(RemoteFileNamePolicy.dropbox.isValid("photo.jpg "))
        XCTAssertTrue(RemoteFileNamePolicy.dropbox.isValid("photo.jpg"))
    }

    func testRemoteFileMatchUsesDropboxContentHash() async throws {
        DropboxMockURLProtocol.handler = { _ in
            .json("""
            {".tag":"file","name":"asset.bin","path_display":"/asset.bin","id":"id:file","client_modified":"2026-01-02T03:04:05Z","server_modified":"2026-01-02T03:04:06Z","content_hash":"4f8b42c22dd3729b519ba6f68d2da7cc5b2d606d05daed5ad5128cc03e6c6358","size":3}
            """)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: localURL) }
        try Data("abc".utf8).write(to: localURL)

        let matchingResult = try await makeClient().remoteFileMatches(
            localURL: localURL,
            remotePath: "/asset.bin"
        )
        XCTAssertTrue(matchingResult)

        try Data("abd".utf8).write(to: localURL)
        let mismatchingResult = try await makeClient().remoteFileMatches(
            localURL: localURL,
            remotePath: "/asset.bin"
        )
        XCTAssertFalse(mismatchingResult)
    }

    func testDropboxContentHasherUsesFourMiBBlocks() throws {
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: localURL) }
        var data = Data(repeating: 0, count: 4 * 1024 * 1024)
        data.append(Data("abc".utf8))
        try data.write(to: localURL)

        XCTAssertEqual(
            try DropboxContentHasher.hexDigest(of: localURL),
            "3fa6db8310a97daafad66b46d1d89070c4fe821f88b40cb9a06fa9aca84e30ce"
        )
    }

    func testConditionalUploadNormalizesDropboxConflictForBackupPipeline() async throws {
        DropboxMockURLProtocol.handler = { _ in
            .json("{\"error_summary\":\"path/conflict/file/..\"}", status: 409)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("lock".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await makeClient().upload(
                localURL: localURL,
                remotePath: "/already-exists",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected a collision")
        } catch {
            XCTAssertTrue(SMBErrorClassifier.isNameCollision(error))
            XCTAssertTrue(remoteStorageIsNameCollision(error))
        }
    }

    func testTokenServiceRefreshesAsPublicClient() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            if request.url?.path == "/2/users/get_current_account" {
                return .json(Self.currentAccountJSON(accountID: "dbid:account"))
            }
            return .json("""
            {"access_token":"fresh-token","token_type":"bearer","expires_in":14400}
            """)
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [DropboxMockURLProtocol.self]
        let service = DropboxTokenService(sessionConfiguration: configuration)

        let token = try await service.accessToken(
            for: DropboxCredentialBlob(accountID: "dbid:account", refreshToken: "refresh-token"),
            appKey: "app-key",
            forceRefresh: false
        )

        XCTAssertEqual(token.value, "fresh-token")
        let request = try XCTUnwrap(recorder.requests.first)
        XCTAssertEqual(request.url?.path, "/oauth2/token")
        var form = URLComponents()
        form.percentEncodedQuery = String(decoding: try XCTUnwrap(request.httpBody), as: UTF8.self)
        let items = Dictionary(uniqueKeysWithValues: try XCTUnwrap(form.queryItems).compactMap { item in
            item.value.map { (item.name, $0) }
        })
        XCTAssertEqual(items["grant_type"], "refresh_token")
        XCTAssertEqual(items["refresh_token"], "refresh-token")
        XCTAssertEqual(items["client_id"], "app-key")
        XCTAssertNil(items["client_secret"])
        XCTAssertEqual(recorder.requests.map(\.url?.path), [
            "/oauth2/token",
            "/2/users/get_current_account"
        ])
        XCTAssertEqual(
            recorder.requests.last?.value(forHTTPHeaderField: "Authorization"),
            "Bearer fresh-token"
        )
    }

    func testConcurrentTokenRefreshUsesSingleFlight() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            if request.url?.path == "/oauth2/token" {
                Thread.sleep(forTimeInterval: 0.05)
                return .json("""
                {"access_token":"fresh-token","token_type":"bearer","expires_in":14400}
                """)
            }
            return .json(Self.currentAccountJSON(accountID: "dbid:account"))
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [DropboxMockURLProtocol.self]
        let service = DropboxTokenService(sessionConfiguration: configuration)
        let credential = DropboxCredentialBlob(accountID: "dbid:account", refreshToken: "refresh-token")

        async let first = service.accessToken(for: credential, appKey: "app-key", forceRefresh: false)
        async let second = service.accessToken(for: credential, appKey: "app-key", forceRefresh: false)
        let tokens = try await (first, second)

        XCTAssertEqual([tokens.0.value, tokens.1.value], ["fresh-token", "fresh-token"])
        XCTAssertEqual(recorder.requests.filter { $0.url?.path == "/oauth2/token" }.count, 1)
        XCTAssertEqual(recorder.requests.filter { $0.url?.path == "/2/users/get_current_account" }.count, 1)
    }

    func testTokenRefreshRejectsDifferentRemoteAccount() async throws {
        DropboxMockURLProtocol.handler = { request in
            if request.url?.path == "/oauth2/token" {
                return .json("""
                {"access_token":"fresh-token","token_type":"bearer","expires_in":14400}
                """)
            }
            return .json(Self.currentAccountJSON(accountID: "dbid:other"))
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [DropboxMockURLProtocol.self]
        let service = DropboxTokenService(sessionConfiguration: configuration)

        do {
            _ = try await service.accessToken(
                for: DropboxCredentialBlob(accountID: "dbid:account", refreshToken: "refresh-token"),
                appKey: "app-key",
                forceRefresh: false
            )
            XCTFail("Expected account mismatch")
        } catch DropboxAuthenticationError.accountMismatch {
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testTokenRateLimitUpdatesSharedThrottle() async throws {
        DropboxMockURLProtocol.handler = { _ in
            .json(
                "{\"error_summary\":\"too_many_requests/..\"}",
                status: 429,
                headers: ["Retry-After": "0.08"]
            )
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [DropboxMockURLProtocol.self]
        let sharedState = DropboxSharedState()
        let service = DropboxTokenService(sharedState: sharedState, sessionConfiguration: configuration)
        let credential = DropboxCredentialBlob(accountID: "dbid:account", refreshToken: "refresh-token")

        do {
            _ = try await service.accessToken(for: credential, appKey: "app-key", forceRefresh: false)
            XCTFail("Expected rate limiting")
        } catch {
            XCTAssertTrue(DropboxErrorClassifier.isConnectionUnavailable(error))
        }
        let start = Date()
        try await sharedState.throttleGate.waitForPermit(
            for: DropboxThrottleGate.Key(appKey: "app-key", accountID: "dbid:account")
        )
        XCTAssertGreaterThanOrEqual(Date().timeIntervalSince(start), 0.05)
    }

    @MainActor
    func testAccountSwitchAuthorizationForcesReauthentication() throws {
        let url = try DropboxOAuthService.authorizationURL(
            appKey: "app-key",
            redirectURI: "db-app-key://2/token",
            state: "state",
            challenge: "challenge",
            forceReauthentication: true
        )
        let items = Dictionary(uniqueKeysWithValues: try XCTUnwrap(
            URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems
        ).compactMap { item in
            item.value.map { (item.name, $0) }
        })

        XCTAssertEqual(items["force_reauthentication"], "true")
        XCTAssertEqual(items["token_access_type"], "offline")
        XCTAssertEqual(items["code_challenge_method"], "S256")
    }

    func testUnauthorizedRequestRefreshesTokenOnce() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            if recorder.requests.count == 1 {
                return .json("{\"error_summary\":\"invalid_access_token/..\"}", status: 401)
            }
            return .json("{\"entries\":[],\"cursor\":\"done\",\"has_more\":false}")
        }
        let provider = DropboxRecordingTokenProvider()
        let connection = try CanonicalDropboxConnection(params: DropboxConnectionParams(
            appKey: "app-key",
            accountID: "dbid:account",
            displayRootPath: DropboxConnectionParams.appFolderDisplayPath
        ))
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [DropboxMockURLProtocol.self]
        let client = DropboxClient(
            config: DropboxClient.Config(connection: connection),
            credential: DropboxCredentialBlob(accountID: "dbid:account", refreshToken: "refresh"),
            tokenProvider: provider,
            sessionConfiguration: configuration
        )

        try await client.connect()

        let refreshFlags = await provider.refreshFlags()
        XCTAssertEqual(refreshFlags, [false, true])
        XCTAssertEqual(recorder.requests.count, 2)
    }

    func testMoveSurfacesDestinationConflictWithoutTouchingTarget() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(
                "{\"error_summary\":\"to/conflict/file/..\",\"error\":{\".tag\":\"to\",\"to\":{\".tag\":\"conflict\",\"conflict\":{\".tag\":\"file\"}}}}",
                status: 409
            )
        }

        do {
            try await makeClient().move(from: "/source", to: "/destination")
            XCTFail("Expected destination conflict")
        } catch {
            XCTAssertTrue(DropboxErrorClassifier.isNameCollision(error))
        }

        XCTAssertEqual(recorder.requests.map(\.url?.lastPathComponent), ["move_v2"])
        XCTAssertFalse(recorder.requests.contains { $0.url?.lastPathComponent == "delete_v2" })
    }

    func testMoveDoesNotTouchDestinationWhenSourceIsMissing() async throws {
        let recorder = DropboxRequestRecorder()
        DropboxMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(
                "{\"error_summary\":\"from_lookup/not_found/..\",\"error\":{\".tag\":\"from_lookup\",\"from_lookup\":{\".tag\":\"not_found\"}}}",
                status: 409
            )
        }

        do {
            try await makeClient().move(from: "/missing", to: "/destination")
            XCTFail("Expected missing source")
        } catch {
            XCTAssertTrue(DropboxErrorClassifier.isNotFound(error))
        }

        XCTAssertEqual(recorder.requests.map(\.url?.lastPathComponent), ["move_v2"])
        XCTAssertFalse(recorder.requests.contains { $0.url?.lastPathComponent == "delete_v2" })
    }

    private func makeClient(sharedState: DropboxSharedState = DropboxSharedState()) -> DropboxClient {
        let connection = try! CanonicalDropboxConnection(params: DropboxConnectionParams(
            appKey: "app-key",
            accountID: "dbid:account",
            displayRootPath: DropboxConnectionParams.appFolderDisplayPath
        ))
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [DropboxMockURLProtocol.self]
        return DropboxClient(
            config: DropboxClient.Config(connection: connection),
            credential: DropboxCredentialBlob(accountID: "dbid:account", refreshToken: "refresh"),
            tokenProvider: DropboxTestTokenProvider(),
            sharedState: sharedState,
            sessionConfiguration: configuration
        )
    }

    private func makeServiceError(json: String, status: Int = 409) -> NSError {
        let response = HTTPURLResponse(
            url: URL(string: "https://api.dropboxapi.com/2/files/upload")!,
            statusCode: status,
            httpVersion: "HTTP/1.1",
            headerFields: ["X-Dropbox-Request-Id": "request-id"]
        )!
        return DropboxErrorClassifier.makeServiceError(
            data: Data(json.utf8),
            response: response,
            endpoint: "files/upload"
        )
    }

    private static func currentAccountJSON(accountID: String) -> String {
        """
        {"account_id":"\(accountID)","email":"account@example.com","name":{"display_name":"Account"}}
        """
    }

}

private struct DropboxTestTokenProvider: DropboxAccessTokenProviding {
    func accessToken(
        for _: DropboxCredentialBlob,
        appKey _: String,
        forceRefresh _: Bool
    ) async throws -> DropboxAccessToken {
        DropboxAccessToken(value: "test-token", expiresAt: Date().addingTimeInterval(3_600))
    }
}

private actor DropboxRecordingTokenProvider: DropboxAccessTokenProviding {
    private var flags: [Bool] = []

    func accessToken(
        for _: DropboxCredentialBlob,
        appKey _: String,
        forceRefresh: Bool
    ) async throws -> DropboxAccessToken {
        flags.append(forceRefresh)
        return DropboxAccessToken(
            value: forceRefresh ? "refreshed-token" : "stale-token",
            expiresAt: Date().addingTimeInterval(3_600)
        )
    }

    func refreshFlags() -> [Bool] {
        flags
    }
}

private final class DropboxRequestRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [URLRequest] = []

    var requests: [URLRequest] { lock.withLock { storage } }

    func append(_ request: URLRequest) {
        var captured = request
        if captured.httpBody == nil, let stream = captured.httpBodyStream {
            stream.open()
            defer { stream.close() }
            var data = Data()
            var buffer = [UInt8](repeating: 0, count: 4_096)
            while stream.hasBytesAvailable {
                let count = stream.read(&buffer, maxLength: buffer.count)
                guard count > 0 else { break }
                data.append(buffer, count: count)
            }
            captured.httpBody = data
        }
        lock.withLock { storage.append(captured) }
    }
}

private final class DropboxMockURLProtocol: URLProtocol {
    struct Response {
        let data: Data
        let status: Int
        let headers: [String: String]

        static func json(
            _ json: String,
            status: Int = 200,
            headers: [String: String] = [:]
        ) -> Response {
            var responseHeaders = headers
            responseHeaders["Content-Type"] = "application/json"
            return Response(
                data: Data(json.utf8),
                status: status,
                headers: responseHeaders
            )
        }
    }

    static var handler: ((URLRequest) throws -> Response)?

    override class func canInit(with _: URLRequest) -> Bool { true }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler, let url = request.url else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }
        do {
            let result = try handler(request)
            guard let response = HTTPURLResponse(
                url: url,
                statusCode: result.status,
                httpVersion: "HTTP/1.1",
                headerFields: result.headers
            ) else {
                throw URLError(.badServerResponse)
            }
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: result.data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

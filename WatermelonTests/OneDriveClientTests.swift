import XCTest
@testable import Watermelon

final class OneDriveClientTests: XCTestCase {
    override func tearDown() {
        OneDriveMockURLProtocol.handler = nil
        super.tearDown()
    }

    func testCredentialRoundTripPinsAccountIdentity() throws {
        let original = OneDriveCredentialBlob(
            homeAccountIdentifier: "home-account",
            tenantID: "consumer-tenant",
            authorityEnvironment: "LOGIN.MICROSOFTONLINE.COM"
        )
        let decoded = try OneDriveCredentialBlob.decode(from: original.encodedJSONString())
        XCTAssertEqual(decoded.homeAccountIdentifier, "home-account")
        XCTAssertEqual(decoded.tenantID, "consumer-tenant")
        XCTAssertEqual(decoded.authorityEnvironment, "login.microsoftonline.com")
    }

    func testProfileCrossesCanonicalIdentityAndFactoryBoundary() throws {
        let params = OneDriveConnectionParams(
            driveID: " drive ",
            rootItemID: " root ",
            displayRootPath: "OneDrive/Apps/Watermelon"
        )
        let profile = ServerProfileRecord(
            name: "OneDrive",
            storageType: StorageType.onedrive.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(params),
            sortOrder: 0,
            host: "graph.microsoft.com",
            port: 443,
            shareName: "root",
            basePath: "/",
            username: "account@example.com",
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
        let credential = Self.credential(homeAccountIdentifier: "home")

        let descriptor = try StorageClientFactory.canonicalConnection(for: profile)
        guard case .oneDrive(let connection) = descriptor else {
            return XCTFail("Expected OneDrive descriptor")
        }
        XCTAssertEqual(connection.driveID, "drive")
        XCTAssertEqual(connection.rootItemID, "root")
        XCTAssertEqual(connection.displayRootPath, "/Apps/Watermelon")
        XCTAssertEqual(descriptor.publishedV2IdentityComponents, ["global", "drive", "root"])
        XCTAssertEqual(profile.duplicateIdentity?.components, descriptor.publishedV2IdentityComponents)
        XCTAssertEqual(profile.remoteDestinationIdentity.components, descriptor.publishedV2RemoteIdentityComponents)
        XCTAssertEqual(
            StorageProfilePersistence.credentialRef(for: try XCTUnwrap(profile.duplicateIdentity)),
            StorageProfilePersistence.credentialRef(for: descriptor.duplicateIdentity)
        )
        let client = try StorageClientFactory(
            oneDriveTokenProvider: OneDriveTestTokenProvider()
        ).makeClient(
            profile: profile,
            credentialPayload: try credential.encodedJSONString()
        )
        XCTAssertTrue(client is OneDriveClient)
    }

    func testFactoryDoesNotResolveOneDriveContextForOtherBackends() throws {
        let profile = ServerProfileRecord(
            name: "SMB",
            storageType: StorageType.smb.rawValue,
            sortOrder: 0,
            host: "example.com",
            port: 445,
            shareName: "share",
            basePath: "/",
            username: "user",
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
        var resolvedOneDriveContext = false
        _ = try StorageClientFactory(
            oneDriveClientContextProvider: {
                resolvedOneDriveContext = true
                return nil
            }
        ).makeClient(
            profile: profile,
            credentialPayload: "secret"
        )
        XCTAssertFalse(resolvedOneDriveContext)
    }

    func testConnectAcceptsGraphCanonicalizedIdentifiersForPinnedEndpoint() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json("""
            {
              "id":"67b27108921acb92!SD3773CDB08154638A248939205E89B7A",
              "name":"Watermelon",
              "size":0,
              "folder":{},
              "parentReference":{"driveId":"67B27108921ACB92"}
            }
            """)
        }
        let client = makeClient(
            driveID: "67b27108921acb92",
            rootItemID: "67B27108921ACB92!sd3773cdb08154638a248939205e89b7a"
        )

        try await client.connect()

        let request = try XCTUnwrap(recorder.requests.first)
        XCTAssertEqual(request.httpMethod, "GET")
        XCTAssertTrue(request.url?.absoluteString.contains("/drives/67b27108921acb92/items/") == true)
        XCTAssertTrue(request.url?.absoluteString.contains("67B27108921ACB92%21sd3773cdb08154638a248939205e89b7a") == true)
    }

    func testConnectStillRejectsPinnedItemThatIsNotFolder() async {
        OneDriveMockURLProtocol.handler = { _ in
            .json(Self.item(id: "root", name: "not-a-folder", folder: false))
        }

        do {
            try await makeClient().connect()
            XCTFail("Expected invalid configuration")
        } catch {
            guard case RemoteStorageClientError.invalidConfiguration = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
    }

    func testMetadataAcceptsCanonicalizedParentDriveIDFromScopedEndpoint() async throws {
        OneDriveMockURLProtocol.handler = { _ in
            .json("""
            {
              "id":"photo-id",
              "name":"photo.jpg",
              "size":5,
              "file":{},
              "parentReference":{"driveId":"67B27108921ACB92"}
            }
            """)
        }
        let client = makeClient(driveID: "67b27108921acb92")

        let entry = try await client.metadata(path: "/photo.jpg")

        XCTAssertEqual(entry?.name, "photo.jpg")
    }

    func testCachedAccountRetentionWaitsForLastProfile() {
        let first = OneDriveCredentialBlob(
            homeAccountIdentifier: "home-a",
            tenantID: "tenant",
            authorityEnvironment: "login.microsoftonline.com"
        )
        let second = OneDriveCredentialBlob(
            homeAccountIdentifier: "home-b",
            tenantID: "tenant",
            authorityEnvironment: "login.microsoftonline.com"
        )
        XCTAssertFalse(OneDriveCachedAccountRetentionPolicy.shouldRemove(
            deletedHomeAccountIdentifier: "home-a",
            remainingCredentials: [first, second]
        ))
        XCTAssertTrue(OneDriveCachedAccountRetentionPolicy.shouldRemove(
            deletedHomeAccountIdentifier: "home-a",
            remainingCredentials: [second]
        ))
    }

    func testPendingAccountLeaseCleansDiscardedAndAbandonedAccount() {
        let credential = Self.credential(homeAccountIdentifier: "home-a")
        var cleanedAccounts: [String] = []
        var abandoned: PendingOneDriveAccountLease? = PendingOneDriveAccountLease(
            credential: credential,
            finalize: { credential, disposition in
                if disposition == .discarded {
                    cleanedAccounts.append(credential.homeAccountIdentifier)
                }
            }
        )

        abandoned = nil
        XCTAssertNil(abandoned)
        XCTAssertEqual(cleanedAccounts, ["home-a"])

        let discarded = PendingOneDriveAccountLease(
            credential: credential,
            finalize: { credential, disposition in
                if disposition == .discarded {
                    cleanedAccounts.append(credential.homeAccountIdentifier)
                }
            }
        )
        discarded.discard()
        discarded.discard()
        XCTAssertEqual(cleanedAccounts, ["home-a", "home-a"])
    }

    func testPendingAccountLeaseKeepsCommittedAccount() {
        let credential = Self.credential(homeAccountIdentifier: "home-a")
        var cleanedAccounts: [String] = []
        do {
            let lease = PendingOneDriveAccountLease(
                credential: credential,
                finalize: { credential, disposition in
                    if disposition == .discarded {
                        cleanedAccounts.append(credential.homeAccountIdentifier)
                    }
                }
            )
            lease.commit()
        }
        XCTAssertTrue(cleanedAccounts.isEmpty)
    }

    func testPendingAccountLeaseCanTransferSameAccountWithoutCleanup() {
        let credential = Self.credential(homeAccountIdentifier: "home-a")
        var cleanedAccounts: [String] = []
        do {
            let lease = PendingOneDriveAccountLease(
                credential: credential,
                finalize: { credential, disposition in
                    if disposition == .discarded {
                        cleanedAccounts.append(credential.homeAccountIdentifier)
                    }
                }
            )
            lease.relinquishToReplacement()
        }
        XCTAssertTrue(cleanedAccounts.isEmpty)
    }

    func testPendingAccountRegistryProtectsSameAccountUntilLastLeaseEnds() {
        let registry = OneDrivePendingAccountRegistry()
        registry.retain(homeAccountIdentifier: "home-a")
        registry.retain(homeAccountIdentifier: "home-a")

        XCTAssertFalse(registry.release(homeAccountIdentifier: "home-a"))
        XCTAssertTrue(registry.contains(homeAccountIdentifier: "home-a"))
        XCTAssertTrue(registry.release(homeAccountIdentifier: "home-a"))
        XCTAssertFalse(registry.contains(homeAccountIdentifier: "home-a"))
    }

    func testAppFolderBootstrapUsesGraphTransportAuthenticationRetry() async throws {
        let tokenProvider = OneDriveRecordingTokenProvider()
        let requestCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            let attempt = requestCounter.increment()
            XCTAssertEqual(request.url?.path, "/v1.0/me/drive/special/approot")
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer token-\(attempt)")
            if attempt == 1 {
                return .json(
                    "{\"error\":{\"code\":\"InvalidAuthenticationToken\"}}",
                    status: 401,
                    headers: ["WWW-Authenticate": "Bearer ClAiMs=\"claim-token\""]
                )
            }
            return .json(Self.item(id: "app-root", name: "Watermelon", folder: true))
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [OneDriveMockURLProtocol.self]
        let service = OneDriveAppFolderBootstrapService(
            tokenProvider: tokenProvider,
            sessionConfiguration: configuration
        )

        let result = try await service.bootstrap(credential: Self.credential(homeAccountIdentifier: "home"))

        XCTAssertEqual(result.connectionParams.driveID, "drive")
        XCTAssertEqual(result.connectionParams.rootItemID, "app-root")
        XCTAssertEqual(result.connectionParams.displayRootPath, "/Apps/Watermelon")
        XCTAssertEqual(tokenProvider.calls.map(\.forceRefresh), [false, true])
        XCTAssertEqual(tokenProvider.calls.map(\.claims), [nil, "claim-token"])
    }

    func testAppFolderBootstrapSurfacesGraphErrorMessage() async throws {
        OneDriveMockURLProtocol.handler = { _ in
            .json(
                "{\"error\":{\"code\":\"BadRequest\",\"message\":\"App folder is not available for this drive.\"}}",
                status: 400
            )
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [OneDriveMockURLProtocol.self]
        let service = OneDriveAppFolderBootstrapService(
            tokenProvider: OneDriveTestTokenProvider(),
            sessionConfiguration: configuration
        )

        do {
            _ = try await service.bootstrap(credential: Self.credential(homeAccountIdentifier: "home"))
            XCTFail("Expected Graph error")
        } catch {
            let nsError = error as NSError
            XCTAssertEqual(nsError.domain, OneDriveErrorClassifier.errorDomain)
            XCTAssertEqual(nsError.code, 400)
            XCTAssertEqual(nsError.localizedDescription, "App folder is not available for this drive.")
        }
    }

    func testGraphMySiteFailureExplainsOneDriveProvisioning() {
        let error = OneDriveErrorClassifier.makeServiceError(
            statusCode: 400,
            code: "BadRequest",
            message: "Unable to retrieve user's mysite URL."
        )

        XCTAssertEqual(
            OneDriveErrorClassifier.describe(error),
            String(localized: "onedrive.error.driveNotReady")
        )
    }

    func testListFollowsOpaqueNextLinkAndAuthenticatesEveryGraphPage() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/items/root:/folder:/children") {
                return .json("""
                {"value":[{"id":"a","name":"a.jpg","size":3,"file":{}}],
                 "@odata.nextLink":"https://graph.microsoft.com:443/v1.0/opaque/page-token?cursor=a%2Fb"}
                """)
            }
            if path == "/v1.0/opaque/page-token" {
                return .json("""
                {"value":[{"id":"b","name":"b.jpg","size":4,"file":{}}]}
                """)
            }
            return .status(500)
        }

        let entries = try await makeClient().list(path: "/folder")
        XCTAssertEqual(entries.map(\.path), ["/folder/a.jpg", "/folder/b.jpg"])
        XCTAssertEqual(recorder.requests.count, 2)
        XCTAssertTrue(recorder.requests.allSatisfy {
            $0.value(forHTTPHeaderField: "Authorization") == "Bearer test-token"
        })
        XCTAssertEqual(recorder.requests.last?.url?.query, "cursor=a%2Fb")
    }

    func testSmallCreateIfAbsentUsesConflictBehaviorDirectUploadAndMapsConflict() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/lock.json:/content") {
                return .json(
                    "{\"error\":{\"code\":\"nameAlreadyExists\",\"message\":\"conflict\"}}",
                    status: 409
                )
            }
            return .status(500)
        }

        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("lock-body".utf8).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }
        do {
            try await makeClient().upload(
                localURL: fileURL,
                remotePath: "/lock.json",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected collision")
        } catch {
            XCTAssertTrue(remoteStorageIsNameCollision(error))
        }

        XCTAssertFalse(recorder.requests.contains { $0.url?.path.hasSuffix("/createUploadSession") == true })
        let uploadRequest = try XCTUnwrap(recorder.requests.first { $0.url?.path.hasSuffix("/items/root:/lock.json:/content") == true })
        XCTAssertEqual(uploadRequest.httpMethod, "PUT")
        let queryItems = URLComponents(url: try XCTUnwrap(uploadRequest.url), resolvingAgainstBaseURL: false)?.queryItems
        XCTAssertTrue(queryItems?.contains(URLQueryItem(name: "@microsoft.graph.conflictBehavior", value: "fail")) == true)
        XCTAssertNil(uploadRequest.value(forHTTPHeaderField: "If-None-Match"))
        XCTAssertEqual(uploadRequest.value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
    }

    func testLargeCreateIfAbsentUsesUploadSessionAndMapsFinalConflict() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/createUploadSession") {
                return .json("{\"uploadUrl\":\"https://upload.example/session\"}")
            }
            if host == "upload.example", request.httpMethod == "PUT" {
                return .json(
                    "{\"error\":{\"code\":\"nameAlreadyExists\",\"message\":\"conflict\"}}",
                    status: 409
                )
            }
            return .status(500)
        }

        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(count: 10 * 1024 * 1024).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }
        do {
            try await makeClient().upload(
                localURL: fileURL,
                remotePath: "/large-lock.bin",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected collision")
        } catch {
            XCTAssertTrue(remoteStorageIsNameCollision(error))
        }

        guard let createRequest = recorder.requests.first(where: { $0.url?.path.hasSuffix("/createUploadSession") == true }),
              let body = createRequest.httpBody,
              let json = try JSONSerialization.jsonObject(with: body) as? [String: Any],
              let item = json["item"] as? [String: Any] else {
            return XCTFail("Missing upload-session request")
        }
        XCTAssertEqual(item["@microsoft.graph.conflictBehavior"] as? String, "fail")
        XCTAssertNil(item["fileSize"])
        XCTAssertNil(item["name"])
        XCTAssertNil(json["deferCommit"])
        let uploadRequest = try XCTUnwrap(recorder.requests.first { $0.url?.host == "upload.example" })
        XCTAssertNil(uploadRequest.value(forHTTPHeaderField: "Authorization"))
        XCTAssertNil(uploadRequest.value(forHTTPHeaderField: "If-None-Match"))
        XCTAssertNil(uploadRequest.value(forHTTPHeaderField: "Content-Type"))
    }

    func testUploadCreatesMissingParentDirectoriesLazily() async throws {
        let recorder = OneDriveRequestRecorder()
        let uploadCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/2026/03/photo.jpg:/content") {
                if uploadCounter.increment() == 1 {
                    return .json(
                        "{\"error\":{\"code\":\"itemNotFound\",\"message\":\"missing parent\"}}",
                        status: 404
                    )
                }
                return .json(Self.item(id: "photo-id", name: "photo.jpg", folder: false), status: 201)
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root/children"),
               request.httpMethod == "POST" {
                return .json(Self.item(id: "year-id", name: "2026", folder: true), status: 201)
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/year-id/children"),
               request.httpMethod == "POST" {
                return .json(Self.item(id: "month-id", name: "03", folder: true), status: 201)
            }
            return .status(500)
        }

        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("photo".utf8).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        try await makeClient().upload(
            localURL: fileURL,
            remotePath: "/2026/03/photo.jpg",
            mode: .replace,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertTrue(recorder.requests.contains { $0.httpMethod == "POST" && $0.url?.path.hasSuffix("/items/root/children") == true })
        XCTAssertTrue(recorder.requests.contains { $0.httpMethod == "POST" && $0.url?.path.hasSuffix("/items/year-id/children") == true })
        XCTAssertEqual(uploadCounter.value, 2)
        XCTAssertTrue(recorder.requests.contains { $0.httpMethod == "PUT" && $0.url?.path.hasSuffix("/items/root:/2026/03/photo.jpg:/content") == true })
    }

    func testUploadUsesRootRelativePathWithoutParentLookupWhenParentExists() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/2026/03/first.jpg:/content") {
                return .json(Self.item(id: "first-id", name: "first.jpg", folder: false), status: 201)
            }
            return .status(500)
        }

        let firstURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("first".utf8).write(to: firstURL)
        defer { try? FileManager.default.removeItem(at: firstURL) }

        let client = makeClient()
        try await client.upload(
            localURL: firstURL,
            remotePath: "/2026/03/first.jpg",
            mode: .replace,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertTrue(recorder.requests.contains { $0.httpMethod == "PUT" && $0.url?.path.hasSuffix("/items/root:/2026/03/first.jpg:/content") == true })
        XCTAssertFalse(recorder.requests.contains { $0.httpMethod == "GET" && $0.url?.path.hasSuffix("/items/root:/2026/03") == true })
        XCTAssertFalse(recorder.requests.contains { $0.httpMethod == "POST" && $0.url?.path.hasSuffix("/children") == true })
    }

    func testSetModificationDateUsesUploadedItemWithoutPathLookup() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/photo.jpg:/content") {
                return .json(Self.item(id: "uploaded-id", name: "photo.jpg", folder: false), status: 201)
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/uploaded-id"), request.httpMethod == "PATCH" {
                return .json(Self.item(id: "uploaded-id", name: "photo.jpg", folder: false))
            }
            return .status(500)
        }

        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("photo".utf8).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        let client = makeClient()
        try await client.upload(
            localURL: fileURL,
            remotePath: "/photo.jpg",
            mode: .replace,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.setModificationDate(Date(timeIntervalSince1970: 1_700_000_000), forPath: "/photo.jpg")

        XCTAssertFalse(recorder.requests.contains { request in
            request.httpMethod == "GET" && request.url?.path.hasSuffix("/items/root:/photo.jpg") == true
        })
        let patchRequest = try XCTUnwrap(recorder.requests.first { request in
            request.httpMethod == "PATCH" && request.url?.path.hasSuffix("/items/uploaded-id") == true
        })
        XCTAssertEqual(patchRequest.value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
        XCTAssertNil(patchRequest.value(forHTTPHeaderField: "If-Match"))
    }

    func testWriteAccessProbeNeverUsesVersionConditions() async throws {
        let recorder = OneDriveRequestRecorder()
        let uploadCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/items/root"), request.httpMethod == "GET" {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if path.hasSuffix("/items/root/children"), request.httpMethod == "POST" {
                return .json("""
                {
                  "id":"probe-directory-id",
                  "name":"probe",
                  "size":0,
                  "eTag":"stale-directory-tag",
                  "folder":{},
                  "parentReference":{"driveId":"drive"}
                }
                """, status: 201)
            }
            if path.hasSuffix("/write-test:/content"), request.httpMethod == "PUT" {
                if uploadCounter.increment() == 1 {
                    return .json(
                        Self.item(
                            id: "probe-file-id",
                            name: "write-test",
                            folder: false,
                            eTag: "probe-file-tag"
                        ),
                        status: 201
                    )
                }
                return .json(
                    "{\"error\":{\"code\":\"nameAlreadyExists\",\"message\":\"conflict\"}}",
                    status: 409
                )
            }
            if path.hasSuffix("/items/probe-file-id/content"), request.httpMethod == "GET" {
                return OneDriveMockURLProtocol.Response(
                    data: Data("watermelon-write-probe-a".utf8),
                    status: 200,
                    headers: [:]
                )
            }
            if path.hasSuffix("/items/probe-file-id"), request.httpMethod == "DELETE" {
                return .status(204)
            }
            if path.hasSuffix("/items/probe-directory-id"), request.httpMethod == "DELETE" {
                return .status(204)
            }
            return .status(500)
        }

        try await makeClient().verifyWriteAccess()

        let deletes = recorder.requests.filter { $0.httpMethod == "DELETE" }
        XCTAssertEqual(deletes.count, 2)
        let fileDelete = try XCTUnwrap(deletes.first { $0.url?.path.hasSuffix("/items/probe-file-id") == true })
        XCTAssertNil(fileDelete.value(forHTTPHeaderField: "If-Match"))
        let directoryDelete = try XCTUnwrap(deletes.first {
            $0.url?.path.hasSuffix("/items/probe-directory-id") == true
        })
        XCTAssertNil(directoryDelete.value(forHTTPHeaderField: "If-Match"))
        XCTAssertFalse(recorder.requests.contains {
            $0.url?.query?.localizedCaseInsensitiveContains("etag") == true
                || $0.url?.query?.localizedCaseInsensitiveContains("ctag") == true
        })
    }

    func testKnownFileDeleteUsesItemIDWithoutPathLookup() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/backup-id"), request.httpMethod == "DELETE" {
                return .status(204)
            }
            return .status(500)
        }

        let client = makeClient()
        try await client.deleteKnownPresentFile(OneDriveKnownFile(itemID: "backup-id"))

        XCTAssertFalse(recorder.requests.contains { request in
            request.httpMethod == "GET" && request.url?.path.hasSuffix("/items/root:/backup.sqlite.bak") == true
        })
        let deleteRequest = try XCTUnwrap(recorder.requests.first { request in
            request.httpMethod == "DELETE" && request.url?.path.hasSuffix("/items/backup-id") == true
        })
        XCTAssertNil(deleteRequest.value(forHTTPHeaderField: "If-Match"))
    }

    func testKnownPresentPathDeleteRefreshesPathBeforeDeleting() async throws {
        let recorder = OneDriveRequestRecorder()
        let backupPathResolveCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/temp.sqlite.tmp") {
                return .json(Self.item(id: "temp-id", name: "temp.sqlite.tmp", folder: false))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/backup.sqlite.bak") {
                let attempt = backupPathResolveCounter.increment()
                if attempt == 1 {
                    return .json(
                        "{\"error\":{\"code\":\"itemNotFound\",\"message\":\"missing\"}}",
                        status: 404
                    )
                }
                return .json(Self.item(id: "live-backup-id", name: "backup.sqlite.bak", folder: false))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/temp-id"), request.httpMethod == "PATCH" {
                return .json(Self.item(id: "stale-backup-id", name: "backup.sqlite.bak", folder: false))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/live-backup-id"), request.httpMethod == "DELETE" {
                return .status(204)
            }
            return .status(500)
        }

        let client = makeClient()
        try await client.move(from: "/temp.sqlite.tmp", to: "/backup.sqlite.bak")
        try await client.deleteKnownPresentFile(path: "/backup.sqlite.bak")

        XCTAssertTrue(recorder.requests.contains { request in
            request.httpMethod == "GET" && request.url?.path.hasSuffix("/items/root:/backup.sqlite.bak") == true
        })
        XCTAssertTrue(recorder.requests.contains { request in
            request.httpMethod == "DELETE" && request.url?.path.hasSuffix("/items/live-backup-id") == true
        })
        XCTAssertFalse(recorder.requests.contains { request in
            request.httpMethod == "DELETE" && request.url?.path.hasSuffix("/items/stale-backup-id") == true
        })
    }

    func testManifestPublishUsesItemIDsWithoutBackupPathProbe() async throws {
        let recorder = OneDriveRequestRecorder()
        let ownershipCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/month.sqlite.tmp") {
                return .json(Self.item(
                    id: "temp-id",
                    name: "month.sqlite.tmp",
                    folder: false,
                    eTag: "temp-tag"
                ))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/month.sqlite") {
                return .json(Self.item(
                    id: "final-old-id",
                    name: "month.sqlite",
                    folder: false,
                    eTag: "final-tag"
                ))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/final-old-id"), request.httpMethod == "PATCH" {
                return .json(Self.item(id: "backup-id", name: "month.sqlite.bak", folder: false))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/temp-id"), request.httpMethod == "PATCH" {
                return .json(Self.item(id: "final-new-id", name: "month.sqlite", folder: false))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/final-new-id/content") {
                return .json("verified")
            }
            return .status(500)
        }

        let client = makeClient()
        let outcome = try await client.publishUploadedManifest(
            tempPath: "/month.sqlite.tmp",
            finalPath: "/month.sqlite",
            backupPath: "/month.sqlite.bak",
            ignoreCancellation: false,
            assertOwnership: {
                _ = ownershipCounter.increment()
            }
        )
        let requestCountAfterPublish = recorder.requests.count
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: downloadURL) }
        try await client.downloadKnownFileForReadBackVerification(outcome.finalFile, localURL: downloadURL)

        XCTAssertNotNil(outcome.backupFile)
        XCTAssertGreaterThanOrEqual(ownershipCounter.value, 3)
        XCTAssertFalse(recorder.requests.contains { request in
            request.httpMethod == "GET" && request.url?.path.hasSuffix("/items/root:/month.sqlite.bak") == true
        })
        XCTAssertFalse(recorder.requests.dropFirst(requestCountAfterPublish).contains { request in
            request.httpMethod == "GET" && request.url?.path.hasSuffix("/items/root:/month.sqlite") == true
        })
        XCTAssertTrue(recorder.requests.contains { request in
            request.httpMethod == "GET" && request.url?.path.hasSuffix("/items/final-new-id/content") == true
        })
    }

    func testManifestPublishWaitsForThrottleAfterMovingCanonicalToBackup() async throws {
        let recorder = OneDriveRequestRecorder()
        let sharedState = OneDriveSharedState()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/items/root:/month.sqlite.tmp") {
                return .json(Self.item(id: "temp-id", name: "month.sqlite.tmp", folder: false))
            }
            if path.hasSuffix("/items/root:/month.sqlite") {
                return .json(Self.item(id: "final-old-id", name: "month.sqlite", folder: false))
            }
            if path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if path.hasSuffix("/items/final-old-id"), request.httpMethod == "PATCH" {
                let recorded = DispatchSemaphore(value: 0)
                Task {
                    await sharedState.throttleGate.record(
                        retryAfter: Date().addingTimeInterval(0.05),
                        for: Self.throttleKey
                    )
                    recorded.signal()
                }
                recorded.wait()
                return .json(Self.item(id: "backup-id", name: "month.sqlite.bak", folder: false))
            }
            if path.hasSuffix("/items/temp-id"), request.httpMethod == "PATCH" {
                return .json(Self.item(id: "final-new-id", name: "month.sqlite", folder: false))
            }
            return .status(500)
        }

        let outcome = try await makeClient(sharedState: sharedState).publishUploadedManifest(
            tempPath: "/month.sqlite.tmp",
            finalPath: "/month.sqlite",
            backupPath: "/month.sqlite.bak",
            ignoreCancellation: false,
            assertOwnership: {}
        )

        XCTAssertNotNil(outcome.backupFile)
        XCTAssertEqual(recorder.requests.filter { $0.httpMethod == "PATCH" }.count, 2)
    }

    func testManifestPublishFailsFastWhenThrottleClosesBeforeFirstMove() async throws {
        let recorder = OneDriveRequestRecorder()
        let sharedState = OneDriveSharedState()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/items/root:/month.sqlite.tmp") {
                return .json(Self.item(id: "temp-id", name: "month.sqlite.tmp", folder: false))
            }
            if path.hasSuffix("/items/root:/month.sqlite") {
                return .json(Self.item(id: "final-old-id", name: "month.sqlite", folder: false))
            }
            if path.hasSuffix("/items/root") {
                let recorded = DispatchSemaphore(value: 0)
                Task {
                    await sharedState.throttleGate.record(
                        retryAfter: Date().addingTimeInterval(30),
                        for: Self.throttleKey
                    )
                    recorded.signal()
                }
                recorded.wait()
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            return .status(500)
        }

        do {
            _ = try await makeClient(sharedState: sharedState).publishUploadedManifest(
                tempPath: "/month.sqlite.tmp",
                finalPath: "/month.sqlite",
                backupPath: "/month.sqlite.bak",
                ignoreCancellation: false,
                assertOwnership: {}
            )
            XCTFail("Expected throttle error")
        } catch {
            XCTAssertEqual((error as NSError).code, 429)
        }

        XCTAssertFalse(recorder.requests.contains { $0.httpMethod == "PATCH" })
        XCTAssertEqual(
            recorder.requests.filter { $0.url?.path.hasSuffix("/items/root:/month.sqlite") == true }.count,
            1
        )
    }

    func testManifestPublishRenamesRefuseToLandOnAnOccupiedDestination() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/month.sqlite.tmp") {
                return .json(Self.item(
                    id: "temp-id",
                    name: "month.sqlite.tmp",
                    folder: false,
                    eTag: "temp-tag"
                ))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/month.sqlite") {
                return .json(Self.item(
                    id: "final-old-id",
                    name: "month.sqlite",
                    folder: false,
                    eTag: "final-tag"
                ))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/final-old-id"), request.httpMethod == "PATCH" {
                return .json(Self.item(id: "backup-id", name: "month.sqlite.bak", folder: false))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/temp-id"), request.httpMethod == "PATCH" {
                return .json(Self.item(id: "final-new-id", name: "month.sqlite", folder: false))
            }
            return .status(500)
        }

        _ = try await makeClient().publishUploadedManifest(
            tempPath: "/month.sqlite.tmp",
            finalPath: "/month.sqlite",
            backupPath: "/month.sqlite.bak",
            ignoreCancellation: false,
            assertOwnership: {}
        )

        let patches = recorder.requests.filter { $0.httpMethod == "PATCH" }
        XCTAssertEqual(patches.count, 2)
        for patch in patches {
            XCTAssertNil(patch.value(forHTTPHeaderField: "If-Match"))
            XCTAssertEqual(
                Self.conflictBehavior(in: patch),
                "fail",
                "every publish rename must refuse an occupied destination: \(patch.url?.path ?? "-")"
            )
        }
    }

    func testManifestPublishFailsClosedWhenCanonicalReappearsBeforeTheCommit() async throws {
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/month.sqlite.tmp") {
                return .json(Self.item(id: "temp-id", name: "month.sqlite.tmp", folder: false))
            }
            // Absent when we look, so publish takes the no-backup commit path...
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/month.sqlite") {
                return .json("{\"error\":{\"code\":\"itemNotFound\",\"message\":\"not found\"}}", status: 404)
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            // ...but a foreign writer published it in the meantime.
            if host == "graph.microsoft.com", path.hasSuffix("/items/temp-id"), request.httpMethod == "PATCH" {
                return .json("{\"error\":{\"code\":\"nameAlreadyExists\",\"message\":\"conflict\"}}", status: 409)
            }
            return .status(500)
        }

        do {
            _ = try await makeClient().publishUploadedManifest(
                tempPath: "/month.sqlite.tmp",
                finalPath: "/month.sqlite",
                backupPath: "/month.sqlite.bak",
                ignoreCancellation: false,
                assertOwnership: {}
            )
            XCTFail("Expected the commit to fail closed instead of clobbering a foreign canonical")
        } catch {
            XCTAssertTrue(OneDriveErrorClassifier.isNameCollision(error))
        }
    }

    func testGenericMoveKeepsReplaceSemantics() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/source.txt") {
                return .json(Self.item(
                    id: "source-id",
                    name: "source.txt",
                    folder: false,
                    eTag: "source-tag"
                ))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/destination.txt") {
                return .json(Self.item(
                    id: "destination-id",
                    name: "destination.txt",
                    folder: false,
                    eTag: "destination-tag"
                ))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/destination-id"), request.httpMethod == "DELETE" {
                return .status(204)
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/source-id"), request.httpMethod == "PATCH" {
                return .json(Self.item(id: "source-id", name: "destination.txt", folder: false))
            }
            return .status(500)
        }

        try await makeClient().move(from: "/source.txt", to: "/destination.txt")

        // The protocol contract is replace (ten callers rely on it), so the destination is deleted first and the
        // rename must NOT carry the publish-only refusal.
        let deleteRequest = try XCTUnwrap(recorder.requests.first { request in
            request.httpMethod == "DELETE" && request.url?.path.hasSuffix("/items/destination-id") == true
        })
        XCTAssertNil(deleteRequest.value(forHTTPHeaderField: "If-Match"))
        let patch = try XCTUnwrap(recorder.requests.first { $0.httpMethod == "PATCH" })
        XCTAssertNil(patch.value(forHTTPHeaderField: "If-Match"))
        XCTAssertNil(Self.conflictBehavior(in: patch))
    }

    func testCreateDirectoryRecoversWhenTransientCreateActuallySucceeded() async throws {
        let createCounter = OneDriveCounter()
        let resolveCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/probe") {
                let attempt = resolveCounter.increment()
                if attempt == 1 {
                    return .json(
                        "{\"error\":{\"code\":\"itemNotFound\",\"message\":\"The resource could not be found.\"}}",
                        status: 404
                    )
                }
                return .json(Self.item(id: "probe-id", name: "probe", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root/children") {
                _ = createCounter.increment()
                return .json(
                    "{\"error\":{\"code\":\"serviceNotAvailable\",\"message\":\"Service unavailable\"}}",
                    status: 503
                )
            }
            return .status(500)
        }

        try await makeClient().createDirectory(path: "/probe")

        XCTAssertEqual(createCounter.value, 1)
        XCTAssertEqual(resolveCounter.value, 2)
    }

    func testCopyUsesServerSideOperationAndWaitsForTerminalMonitor() async throws {
        let recorder = OneDriveRequestRecorder()
        let destinationCounter = OneDriveCounter()
        let monitorCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "monitor.example" {
                switch monitorCounter.increment() {
                case 1:
                    return .json("{\"status\":\"inProgress\",\"percentageComplete\":50}", status: 202)
                case 2:
                    return .json("{\"status\":\"cancelPending\",\"percentageComplete\":75}", status: 202)
                default:
                    return .json("{\"status\":\"completed\",\"percentageComplete\":100}", status: 202)
                }
            }
            if path.hasSuffix("/items/root:/source.bin") {
                return .json(Self.item(id: "source", name: "source.bin", folder: false))
            }
            if path.hasSuffix("/items/root:/target.bin") {
                if destinationCounter.increment() == 1 {
                    return .json("{\"error\":{\"code\":\"itemNotFound\"}}", status: 404)
                }
                return .json(Self.item(id: "target", name: "target.bin", folder: false))
            }
            if path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if path.hasSuffix("/items/source/copy") {
                return .status(202, headers: ["Location": "https://monitor.example/jobs/1"])
            }
            return .status(500)
        }

        try await makeClient().copy(from: "/source.bin", to: "/target.bin")

        XCTAssertTrue(recorder.requests.contains { $0.url?.path.hasSuffix("/items/source/copy") == true })
        XCTAssertFalse(recorder.requests.contains { $0.url?.path.hasSuffix("/content") == true })
        let monitorRequest = try XCTUnwrap(recorder.requests.first { $0.url?.host == "monitor.example" })
        XCTAssertNil(monitorRequest.value(forHTTPHeaderField: "Authorization"))
        XCTAssertEqual(monitorCounter.value, 3)
        XCTAssertEqual(destinationCounter.value, 2)
    }

    func testCopyRetriesMonitorThrottlingUntilCompleted() async throws {
        let monitorCounter = OneDriveCounter()
        let destinationCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "monitor.example" {
                if monitorCounter.increment() == 1 {
                    return .json(
                        "{\"error\":{\"code\":\"throttledRequest\"}}",
                        status: 429,
                        headers: ["Retry-After": "0"]
                    )
                }
                return .json("{\"status\":\"completed\"}")
            }
            if path.hasSuffix("/items/root:/source.bin") {
                return .json(Self.item(id: "source", name: "source.bin", folder: false))
            }
            if path.hasSuffix("/items/root:/target.bin") {
                if destinationCounter.increment() == 1 {
                    return .json("{\"error\":{\"code\":\"itemNotFound\"}}", status: 404)
                }
                return .json(Self.item(id: "target", name: "target.bin", folder: false))
            }
            if path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if path.hasSuffix("/items/source/copy") {
                return .status(202, headers: ["Location": "https://monitor.example/jobs/1"])
            }
            return .status(500)
        }

        try await makeClient().copy(from: "/source.bin", to: "/target.bin")
        XCTAssertEqual(monitorCounter.value, 2)
    }

    func testCopyWaitsForThrottleBeforeVerifyingCompletedResource() async throws {
        let recorder = OneDriveRequestRecorder()
        let destinationCounter = OneDriveCounter()
        let sharedState = OneDriveSharedState()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "monitor.example" {
                let recorded = DispatchSemaphore(value: 0)
                Task {
                    await sharedState.throttleGate.record(
                        retryAfter: Date().addingTimeInterval(0.05),
                        for: Self.throttleKey
                    )
                    recorded.signal()
                }
                recorded.wait()
                return .json("{\"status\":\"completed\",\"resourceId\":\"copied\"}")
            }
            if path.hasSuffix("/items/root:/source.bin") {
                return .json(Self.item(id: "source", name: "source.bin", folder: false))
            }
            if path.hasSuffix("/items/root:/target.bin") {
                if destinationCounter.increment() == 1 {
                    return .json("{\"error\":{\"code\":\"itemNotFound\"}}", status: 404)
                }
                return .json(Self.item(id: "copied", name: "target.bin", folder: false))
            }
            if path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if path.hasSuffix("/items/source/copy") {
                return .status(202, headers: ["Location": "https://monitor.example/jobs/1"])
            }
            if path.hasSuffix("/items/copied") {
                return .json(Self.item(id: "copied", name: "target.bin", folder: false))
            }
            return .status(500)
        }

        try await makeClient(sharedState: sharedState).copy(from: "/source.bin", to: "/target.bin")

        XCTAssertEqual(recorder.requests.filter { $0.url?.path.hasSuffix("/items/source/copy") == true }.count, 1)
        XCTAssertFalse(recorder.requests.contains { $0.httpMethod == "DELETE" })
        XCTAssertTrue(recorder.requests.contains { $0.url?.path.hasSuffix("/items/copied") == true })
    }

    func testCopyToSameItemDoesNotDeleteSource() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/items/root:/same.bin") {
                return .json(Self.item(id: "same", name: "same.bin", folder: false))
            }
            if path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            return .status(500)
        }

        try await makeClient().copy(from: "/same.bin", to: "/same.bin")
        XCTAssertFalse(recorder.requests.contains { $0.httpMethod == "DELETE" })
        XCTAssertFalse(recorder.requests.contains { $0.url?.path.hasSuffix("/copy") == true })
    }

    func testPreauthenticatedTransportErrorDoesNotExposeUploadURL() async throws {
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/createUploadSession") {
                return .json("{\"uploadUrl\":\"https://upload.example/session?token=SECRET\"}")
            }
            if host == "upload.example" {
                throw NSError(
                    domain: NSURLErrorDomain,
                    code: NSURLErrorBadServerResponse,
                    userInfo: [
                        NSURLErrorFailingURLStringErrorKey: "https://upload.example/session?token=SECRET",
                        NSLocalizedDescriptionKey: "SECRET"
                    ]
                )
            }
            return .status(500)
        }
        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(count: 10 * 1024 * 1024).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        do {
            try await makeClient().upload(
                localURL: fileURL,
                remotePath: "/lock.json",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected transport failure")
        } catch {
            XCTAssertEqual((error as NSError).domain, NSURLErrorDomain)
            XCTAssertEqual((error as NSError).code, NSURLErrorBadServerResponse)
            XCTAssertFalse(String(reflecting: error).contains("SECRET"))
            XCTAssertFalse(String(reflecting: error).contains("upload.example"))
        }
    }

    func testAppDisablesCFNetworkLoggingForPreauthenticatedURLSafety() {
        let preferences = Bundle.main.object(forInfoDictionaryKey: "OSLogPreferences") as? [String: Any]
        let subsystem = preferences?["com.apple.CFNetwork"] as? [String: Any]
        let defaults = subsystem?["DEFAULT-OPTIONS"] as? [String: Any]
        let level = defaults?["Level"] as? [String: Any]
        XCTAssertEqual(level?["Enable"] as? String, "Off")
    }

    func testUploadSessionResumesAfterFragmentThrottleWithoutCreatingNewSession() async throws {
        let createCounter = OneDriveCounter()
        let fragmentCounter = OneDriveCounter()
        let secondFragmentCounter = OneDriveCounter()
        let statusCounter = OneDriveCounter()
        let fragmentSize = 10 * 1024 * 1024
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/createUploadSession") {
                _ = createCounter.increment()
                return .json("{\"uploadUrl\":\"https://upload.example/session\"}")
            }
            if host == "upload.example", request.httpMethod == "GET" {
                _ = statusCounter.increment()
                return .json("{\"nextExpectedRanges\":[\"10485760-\"]}")
            }
            if host == "upload.example", request.httpMethod == "PUT" {
                _ = fragmentCounter.increment()
                let range = request.value(forHTTPHeaderField: "Content-Range") ?? ""
                if range.hasPrefix("bytes 0-") {
                    return .json("{\"nextExpectedRanges\":[\"10485760-\"]}", status: 202)
                }
                if secondFragmentCounter.increment() == 1 {
                    return .json(
                        "{\"error\":{\"code\":\"throttledRequest\"}}",
                        status: 429,
                        headers: ["Retry-After": "0"]
                    )
                }
                return .json(Self.item(id: "uploaded", name: "video.bin", folder: false), status: 201)
            }
            return .status(500)
        }

        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(count: fragmentSize * 2).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        do {
            try await makeClient().upload(
                localURL: fileURL,
                remotePath: "/video.bin",
                mode: .replace,
                respectTaskCancellation: true,
                onProgress: nil
            )
        } catch {
            XCTFail(
                "Upload failed: \(error); create=\(createCounter.value), fragments=\(fragmentCounter.value), "
                    + "second=\(secondFragmentCounter.value), status=\(statusCounter.value)"
            )
            return
        }

        XCTAssertEqual(createCounter.value, 1)
        XCTAssertEqual(fragmentCounter.value, 3)
        XCTAssertEqual(secondFragmentCounter.value, 2)
        XCTAssertEqual(statusCounter.value, 1)
    }

    func testUploadSessionBoundsRepeated416WhenRangeShapeChangesWithoutProgress() async throws {
        let fragmentCounter = OneDriveCounter()
        let statusCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/createUploadSession") {
                return .json(
                    "{\"uploadUrl\":\"https://upload.example/session\","
                        + "\"nextExpectedRanges\":[\"0-10485759\"]}"
                )
            }
            if host == "upload.example", request.httpMethod == "PUT" {
                _ = fragmentCounter.increment()
                return .status(416)
            }
            if host == "upload.example", request.httpMethod == "GET" {
                let statusAttempt = statusCounter.increment()
                let range = statusAttempt.isMultiple(of: 2) ? "0-10485759" : "0-"
                return .json("{\"nextExpectedRanges\":[\"" + range + "\"]}")
            }
            return .status(500)
        }

        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(count: 10 * 1024 * 1024).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        do {
            try await makeClient().upload(
                localURL: fileURL,
                remotePath: "/video.bin",
                mode: .replace,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected bounded range failure")
        } catch {
            XCTAssertEqual((error as NSError).domain, OneDriveErrorClassifier.errorDomain)
            XCTAssertEqual((error as NSError).code, 416)
        }
        XCTAssertEqual(fragmentCounter.value, 6)
        XCTAssertEqual(statusCounter.value, 5)
    }

    func testUploadSessionDoesNotTreatFiniteAlignedMissingRangeAsFragmentShape() async throws {
        let ranges = OneDriveStringRecorder()
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/createUploadSession") {
                return .json(
                    "{\"uploadUrl\":\"https://upload.example/session\","
                        + "\"nextExpectedRanges\":[\"0-655359\"]}"
                )
            }
            if host == "upload.example", request.httpMethod == "PUT" {
                ranges.append(request.value(forHTTPHeaderField: "Content-Range") ?? "")
                return .json(Self.item(id: "uploaded", name: "video.bin", folder: false), status: 201)
            }
            return .status(500)
        }

        let fileURL = try makeSparseUploadFile(size: 10 * 1024 * 1024)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        try await makeClient().upload(
            localURL: fileURL,
            remotePath: "/video.bin",
            mode: .replace,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(ranges.values, ["bytes 0-10485759/10485760"])
    }

    func testUploadSessionDoesNotTreatFiniteMissingRangeAsFragmentShape() async throws {
        let ranges = OneDriveStringRecorder()
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/createUploadSession") {
                return .json(
                    "{\"uploadUrl\":\"https://upload.example/session\","
                        + "\"nextExpectedRanges\":[\"0-10\"]}"
                )
            }
            if host == "upload.example", request.httpMethod == "PUT" {
                ranges.append(request.value(forHTTPHeaderField: "Content-Range") ?? "")
                return .json(Self.item(id: "uploaded", name: "video.bin", folder: false), status: 201)
            }
            return .status(500)
        }

        let fileURL = try makeSparseUploadFile(size: 10 * 1024 * 1024)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        try await makeClient().upload(
            localURL: fileURL,
            remotePath: "/video.bin",
            mode: .replace,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(ranges.values, ["bytes 0-10485759/10485760"])
    }

    func testUploadSessionRecoversFromWatchdogStallWithoutCreatingNewSession() async throws {
        let createCounter = OneDriveCounter()
        let fragmentCounter = OneDriveCounter()
        let statusCounter = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            let host = request.url?.host
            let path = request.url?.path ?? ""
            if host == "graph.microsoft.com", path.hasSuffix("/items/root") {
                return .json(Self.item(id: "root", name: "Watermelon", folder: true))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/createUploadSession") {
                _ = createCounter.increment()
                return .json("{\"uploadUrl\":\"https://upload.example/session\"}")
            }
            if host == "upload.example", request.httpMethod == "GET" {
                _ = statusCounter.increment()
                return .json("{\"nextExpectedRanges\":[\"0-\"]}")
            }
            if host == "upload.example", request.httpMethod == "PUT" {
                if fragmentCounter.increment() == 1 {
                    Thread.sleep(forTimeInterval: 0.1)
                }
                return .json(Self.item(id: "uploaded", name: "lock.json", folder: false), status: 201)
            }
            return .status(500)
        }

        let fileURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(count: 10 * 1024 * 1024).write(to: fileURL)
        defer { try? FileManager.default.removeItem(at: fileURL) }
        let stallTimeouts = URLSessionStallWatchdog.Timeouts(
            uploadBodyStall: 0.02,
            uploadResponseStall: 0.02,
            downloadFirstByte: 0.02,
            downloadStall: 0.02,
            pollInterval: 0.005
        )

        try await makeClient(stallTimeouts: stallTimeouts).upload(
            localURL: fileURL,
            remotePath: "/lock.json",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(createCounter.value, 1)
        XCTAssertEqual(fragmentCounter.value, 2)
        XCTAssertEqual(statusCounter.value, 1)
    }

    func testDownloadSurfacesWatchdogStall() async throws {
        OneDriveMockURLProtocol.handler = { request in
            let path = request.url?.path ?? ""
            if path.hasSuffix("/items/root:/photo.jpg") {
                return .json(Self.item(id: "photo", name: "photo.jpg", folder: false))
            }
            if path.hasSuffix("/items/photo/content") {
                Thread.sleep(forTimeInterval: 0.1)
                return .status(200)
            }
            return .status(500)
        }
        let destination = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: destination) }
        let stallTimeouts = URLSessionStallWatchdog.Timeouts(
            uploadBodyStall: 0.02,
            uploadResponseStall: 0.02,
            downloadFirstByte: 0.02,
            downloadStall: 0.02,
            pollInterval: 0.005
        )

        do {
            try await makeClient(stallTimeouts: stallTimeouts).download(
                remotePath: "/photo.jpg",
                localURL: destination
            )
            XCTFail("Expected download stall")
        } catch {
            let nsError = error as NSError
            XCTAssertEqual(nsError.domain, NSURLErrorDomain)
            XCTAssertEqual(nsError.code, URLError.timedOut.rawValue)
        }
    }

    func testCrossOriginRedirectStripsAuthorization() throws {
        let delegate = OneDriveRedirectDelegate()
        let session = URLSession(configuration: .ephemeral)
        var original = URLRequest(url: URL(string: "https://graph.microsoft.com/v1.0/content")!)
        original.setValue("Bearer secret", forHTTPHeaderField: "Authorization")
        let task = session.dataTask(with: original)
        var redirected = URLRequest(url: URL(string: "https://public.dm.files.1drv.com/download")!)
        redirected.setValue("Bearer secret", forHTTPHeaderField: "Authorization")
        let response = HTTPURLResponse(
            url: original.url!,
            statusCode: 302,
            httpVersion: nil,
            headerFields: nil
        )!
        var result: URLRequest?
        delegate.urlSession(
            session,
            task: task,
            willPerformHTTPRedirection: response,
            newRequest: redirected
        ) { result = $0 }
        XCTAssertNil(result?.value(forHTTPHeaderField: "Authorization"))
        session.invalidateAndCancel()
    }

    func testRedirectRejectsHTTPSDowngrade() throws {
        let delegate = OneDriveRedirectDelegate()
        let session = URLSession(configuration: .ephemeral)
        let original = URLRequest(url: URL(string: "https://upload.example/session")!)
        let task = session.dataTask(with: original)
        let redirected = URLRequest(url: URL(string: "http://upload.example/session")!)
        let response = HTTPURLResponse(
            url: original.url!,
            statusCode: 302,
            httpVersion: nil,
            headerFields: nil
        )!
        var result: URLRequest?
        var completed = false
        delegate.urlSession(
            session,
            task: task,
            willPerformHTTPRedirection: response,
            newRequest: redirected
        ) {
            result = $0
            completed = true
        }
        XCTAssertTrue(completed)
        XCTAssertNil(result)
        session.invalidateAndCancel()
    }

    func testThrottleGateFailsFastUntilRetryAfter() async {
        let gate = OneDriveThrottleGate()
        await gate.record(retryAfter: Date().addingTimeInterval(30), for: Self.throttleKey)
        do {
            try await gate.requirePermit(for: Self.throttleKey)
            XCTFail("Expected throttling error")
        } catch {
            XCTAssertTrue(OneDriveErrorClassifier.isConnectionUnavailable(error))
            XCTAssertEqual((error as NSError).code, 429)
        }
    }

    func testThrottleGateCanWaitForAcceptedOperation() async throws {
        let gate = OneDriveThrottleGate()
        await gate.record(retryAfter: Date().addingTimeInterval(0.05), for: Self.throttleKey)
        try await gate.waitForPermit(for: Self.throttleKey)
        try await gate.requirePermit(for: Self.throttleKey)
    }

    func testThrottleGateDoesNotBlockAnotherAccount() async throws {
        let gate = OneDriveThrottleGate()
        let other = OneDriveThrottleGate.Key(
            authorityEnvironment: "login.microsoftonline.com",
            homeAccountIdentifier: "other-home"
        )
        await gate.record(retryAfter: Date().addingTimeInterval(30), for: Self.throttleKey)

        try await gate.requirePermit(for: other)
    }

    func testNewGraphRequestFailsFastWithoutDroppingRootProof() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(Self.item(id: "root", name: "Watermelon", folder: true))
        }
        let sharedState = OneDriveSharedState()
        let client = makeClient(sharedState: sharedState)
        try await client.connect()
        await sharedState.throttleGate.record(retryAfter: Date().addingTimeInterval(30), for: Self.throttleKey)

        do {
            _ = try await client.metadata(path: "/photo.jpg")
            XCTFail("Expected throttling error")
        } catch {
            XCTAssertEqual((error as NSError).code, 429)
        }
        try await client.connect()
        XCTAssertEqual(recorder.requests.count, 1)
    }

    func testNewDownloadFailsFastWhileThrottleGateIsClosed() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(Self.item(id: "photo", name: "photo.jpg", folder: false))
        }
        let sharedState = OneDriveSharedState()
        let client = makeClient(sharedState: sharedState)
        _ = try await client.metadata(path: "/photo.jpg")
        await sharedState.throttleGate.record(retryAfter: Date().addingTimeInterval(30), for: Self.throttleKey)
        let destination = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: destination) }

        do {
            try await client.download(remotePath: "/photo.jpg", localURL: destination)
            XCTFail("Expected throttling error")
        } catch {
            XCTAssertEqual((error as NSError).code, 429)
        }
        XCTAssertEqual(recorder.requests.count, 1)
    }

    func testDownloadNotFoundEvictsCachedPathBeforeRetry() async throws {
        let recorder = OneDriveRequestRecorder()
        let pathResolveCount = OneDriveCounter()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/items/root/children") {
                return .json("{\"value\":[\(Self.item(id: "old-photo", name: "photo.jpg", folder: false))]}")
            }
            if path.hasSuffix("/items/root:/photo.jpg") {
                _ = pathResolveCount.increment()
                return .json(Self.item(id: "new-photo", name: "photo.jpg", folder: false))
            }
            if path.hasSuffix("/items/old-photo/content") {
                return .json("{\"error\":{\"code\":\"itemNotFound\",\"message\":\"missing\"}}", status: 404)
            }
            if path.hasSuffix("/items/new-photo/content") {
                return OneDriveMockURLProtocol.Response(data: Data("new".utf8), status: 200, headers: [:])
            }
            return .status(500)
        }
        let destination = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: destination) }
        let client = makeClient()
        _ = try await client.list(path: "/")

        do {
            try await client.download(remotePath: "/photo.jpg", localURL: destination)
            XCTFail("Expected stale item ID to be missing")
        } catch {
            XCTAssertTrue(OneDriveErrorClassifier.isNotFound(error))
        }
        try await client.download(remotePath: "/photo.jpg", localURL: destination)

        XCTAssertEqual(try Data(contentsOf: destination), Data("new".utf8))
        XCTAssertEqual(pathResolveCount.value, 1)
    }

    func testItemIDEvictionClearsAliasesWithoutRemovingAReplacementAtTheSamePath() throws {
        let index = OneDriveItemIndex()
        let namespace = OneDriveItemIndex.Namespace(
            cloudEnvironment: "public",
            driveID: "drive",
            rootItemID: "root"
        )
        let oldItem = try OneDriveJSON.decode(
            OneDriveDriveItem.self,
            from: Data(Self.item(id: "old", name: "photo.jpg", folder: false).utf8)
        )
        let replacement = try OneDriveJSON.decode(
            OneDriveDriveItem.self,
            from: Data(Self.item(id: "new", name: "photo.jpg", folder: false).utf8)
        )

        index.cache(oldItem, namespace: namespace, path: "/photo.jpg")
        index.cache(oldItem, namespace: namespace, path: "/alias.jpg")
        index.cache(replacement, namespace: namespace, path: "/photo.jpg")
        index.remove(namespace: namespace, id: oldItem.id)

        XCTAssertEqual(index.item(namespace: namespace, path: "/photo.jpg")?.id, replacement.id)
        XCTAssertNil(index.item(namespace: namespace, path: "/alias.jpg"))
    }

    func testKnownFileReadBackStopsAfterFourNotFoundAttempts() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json("{\"error\":{\"code\":\"itemNotFound\"}}", status: 404)
        }
        let destination = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: destination) }

        do {
            try await makeClient().downloadKnownFileForReadBackVerification(
                OneDriveKnownFile(itemID: "manifest-id"),
                localURL: destination
            )
            XCTFail("Expected exhausted read-back")
        } catch {
            XCTAssertTrue(error is RemoteReadBackRetryExhaustedError)
        }
        XCTAssertEqual(recorder.requests.count, 4)
    }

    func testKnownFileReadBackWaitsForAnAcceptedPublishThrottle() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return OneDriveMockURLProtocol.Response(data: Data("verified".utf8), status: 200, headers: [:])
        }
        let destination = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: destination) }
        let sharedState = OneDriveSharedState()
        await sharedState.throttleGate.record(
            retryAfter: Date().addingTimeInterval(0.05),
            for: Self.throttleKey
        )

        try await makeClient(sharedState: sharedState).downloadKnownFileForReadBackVerification(
            OneDriveKnownFile(itemID: "manifest-id"),
            localURL: destination
        )

        XCTAssertEqual(try Data(contentsOf: destination), Data("verified".utf8))
        XCTAssertEqual(recorder.requests.count, 1)
    }

    // Scratch repair downloads every candidate for a month; OneDrive's publish PATCH-moves the temp onto the
    // canonical, so the residue it leaves can never be reclaimed and the validation would delete nothing.
    func testClientOptsOutOfMonthScratchRepair() {
        XCTAssertFalse(makeClient().repairsMonthScratch())
    }

    func testClientRejectsLegacyV1Migration() {
        XCTAssertFalse(makeClient().supportsLegacyV1Migration())
        XCTAssertTrue(makeClient().allowsUnattendedOrdinaryWriteConfidence())
    }

    // Every pooled/worker client of a run proves the same root; the shared item index makes one proof serve
    // all of them instead of one Graph round trip per client.
    func testConnectReusesTheSharedRootProofAcrossClients() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(Self.item(id: "root", name: "Watermelon", folder: true))
        }
        let sharedState = OneDriveSharedState()

        try await makeClient(sharedState: sharedState).connect()
        try await makeClient(sharedState: sharedState).connect()
        try await makeClient(sharedState: sharedState).connect()

        XCTAssertEqual(recorder.requests.count, 1, "only the first client may pay for the root proof")
    }

    // Clients of a different profile must not inherit the proof: the index is namespaced by drive + root.
    func testConnectDoesNotReuseAnotherDrivesRootProof() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(Self.item(id: "root", name: "Watermelon", folder: true))
        }
        let sharedState = OneDriveSharedState()

        try await makeClient(sharedState: sharedState, driveID: "drive-a").connect()
        try await makeClient(sharedState: sharedState, driveID: "drive-b").connect()

        XCTAssertEqual(recorder.requests.count, 2)
    }

    // Reuse must not blind the reconnect loop, which calls connect() to decide the link is back: a
    // connectivity fault drops the proof so the next connect() goes back to the network.
    func testConnectivityFaultDropsTheSharedRootProof() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(Self.item(id: "root", name: "Watermelon", folder: true))
        }
        let sharedState = OneDriveSharedState()
        try await makeClient(sharedState: sharedState).connect()
        XCTAssertEqual(recorder.requests.count, 1)

        OneDriveMockURLProtocol.handler = { _ in throw URLError(.notConnectedToInternet) }
        let offline = makeClient(sharedState: sharedState)
        _ = try? await offline.metadata(path: "/anything")

        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(Self.item(id: "root", name: "Watermelon", folder: true))
        }
        try await makeClient(sharedState: sharedState).connect()

        XCTAssertEqual(recorder.requests.count, 2, "the dropped proof must be re-fetched from the network")
    }

    // A month manifest that a directory listing already proved absent must not be re-probed: on OneDrive each
    // of those 404s is a ~1.2s round trip, and the run makes several per absent month.
    func testMetadataAnswersAbsenceFromAFullyEnumeratedDirectory() async throws {
        let recorder = OneDriveRequestRecorder()
        // Without the memo this still answers nil — but only after paying the round trip the memo exists to save.
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            if request.url?.absoluteString.contains("children") == true {
                return .json("""
                {"value":[{"id":"m1","name":"2025-08.sqlite","size":10,"file":{},"parentReference":{"driveId":"drive"}}]}
                """)
            }
            return .json("{\"error\":{\"code\":\"itemNotFound\",\"message\":\"not found\"}}", status: 404)
        }
        let client = makeClient()

        _ = try await client.list(path: "/.watermelon/months")
        let listed = recorder.requests.count
        let absent = try await client.metadata(path: "/.watermelon/months/2025-12.sqlite")

        XCTAssertNil(absent)
        XCTAssertEqual(recorder.requests.count, listed, "a proven absence must not cost a request")
    }

    // The same enumeration answers presence: the listing already returned every child in full.
    func testMetadataAnswersPresenceFromAFullyEnumeratedDirectory() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            if request.url?.absoluteString.contains("children") == true {
                return .json("""
                {"value":[{"id":"m1","name":"2025-08.sqlite","size":10,"file":{},"parentReference":{"driveId":"drive"}}]}
                """)
            }
            return .json("{\"error\":{\"code\":\"itemNotFound\",\"message\":\"not found\"}}", status: 404)
        }
        let client = makeClient()

        _ = try await client.list(path: "/.watermelon/months")
        let listed = recorder.requests.count
        let present = try await client.metadata(path: "/.watermelon/months/2025-08.sqlite")

        XCTAssertEqual(present?.name, "2025-08.sqlite")
        XCTAssertEqual(present?.size, 10)
        XCTAssertEqual(recorder.requests.count, listed, "an enumerated presence must not cost a request")
    }

    // A directory nobody enumerated keeps the authoritative probe.
    func testMetadataStillProbesDirectoriesThatWereNeverEnumerated() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            return .json(Self.item(id: "m1", name: "2025-08.sqlite", folder: false))
        }
        let client = makeClient()

        _ = try await client.metadata(path: "/.watermelon/months/2025-08.sqlite")

        XCTAssertEqual(recorder.requests.count, 1)
    }

    // The memo is only sound while nothing has been written, so any mutation drops it.
    func testWriteDropsTheEnumeratedAbsenceMemo() async throws {
        let recorder = OneDriveRequestRecorder()
        OneDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            if request.url?.absoluteString.contains("children") == true {
                return .json("""
                {"value":[{"id":"m1","name":"2025-08.sqlite","size":10,"file":{},"parentReference":{"driveId":"drive"}}]}
                """)
            }
            return .json(Self.item(id: "new", name: "2025-12.sqlite", folder: false))
        }
        let client = makeClient()
        _ = try await client.list(path: "/.watermelon/months")

        let scratch = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data([0x01]).write(to: scratch)
        defer { try? FileManager.default.removeItem(at: scratch) }
        try await client.upload(
            localURL: scratch,
            remotePath: "/.watermelon/months/2025-12.sqlite",
            respectTaskCancellation: false,
            onProgress: nil
        )
        let afterWrite = recorder.requests.count
        _ = try await client.metadata(path: "/.watermelon/months/2025-11.sqlite")

        XCTAssertEqual(recorder.requests.count, afterWrite + 1, "a write invalidates the memo for the whole namespace")
    }

    private func makeClient(
        sharedState: OneDriveSharedState = OneDriveSharedState(),
        stallTimeouts: URLSessionStallWatchdog.Timeouts? = nil,
        driveID: String = "drive",
        rootItemID: String = "root"
    ) -> OneDriveClient {
        let params = OneDriveConnectionParams(
            driveID: driveID,
            rootItemID: rootItemID,
            displayRootPath: "OneDrive/Apps/Watermelon"
        )
        let connection = try! CanonicalOneDriveConnection(params: params)
        let credential = OneDriveCredentialBlob(
            homeAccountIdentifier: "home",
            tenantID: "tenant",
            authorityEnvironment: "login.microsoftonline.com"
        )
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [OneDriveMockURLProtocol.self]
        return OneDriveClient(
            config: OneDriveClient.Config(connection: connection),
            credential: credential,
            tokenProvider: OneDriveTestTokenProvider(),
            sharedState: sharedState,
            sessionConfiguration: configuration,
            stallTimeouts: stallTimeouts
        )
    }

    private func makeSparseUploadFile(size: UInt64) throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        _ = FileManager.default.createFile(atPath: url.path, contents: nil)
        let handle = try FileHandle(forWritingTo: url)
        defer { try? handle.close() }
        try handle.truncate(atOffset: size)
        return url
    }

    private static func profile() throws -> ServerProfileRecord {
        let params = OneDriveConnectionParams(
            driveID: "drive",
            rootItemID: "root",
            displayRootPath: "OneDrive/Apps/Watermelon"
        )
        return ServerProfileRecord(
            name: "OneDrive",
            storageType: StorageType.onedrive.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(params),
            sortOrder: 0,
            host: "graph.microsoft.com",
            port: 443,
            shareName: "root",
            basePath: "/",
            username: "account@example.com",
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
    }

    private static func conflictBehavior(in request: URLRequest) -> String? {
        guard let body = request.httpBody,
              let object = try? JSONSerialization.jsonObject(with: body) as? [String: Any] else { return nil }
        return object["@microsoft.graph.conflictBehavior"] as? String
    }

    private static func item(
        id: String,
        name: String,
        folder: Bool,
        size: Int64 = 0,
        eTag: String? = nil
    ) -> String {
        let facet = folder ? "\"folder\":{}" : "\"file\":{}"
        let eTagField = eTag.map { "\"eTag\":\"\($0)\"," } ?? ""
        return "{\"id\":\"\(id)\",\"name\":\"\(name)\",\"size\":\(size),\(eTagField)\(facet),\"parentReference\":{\"driveId\":\"drive\"}}"
    }

    private static func credential(homeAccountIdentifier: String) -> OneDriveCredentialBlob {
        OneDriveCredentialBlob(
            homeAccountIdentifier: homeAccountIdentifier,
            tenantID: "tenant",
            authorityEnvironment: "login.microsoftonline.com"
        )
    }

    private static let throttleKey = OneDriveThrottleGate.Key(
        authorityEnvironment: "login.microsoftonline.com",
        homeAccountIdentifier: "home"
    )
}

private struct OneDriveTestTokenProvider: OneDriveAccessTokenProviding {
    func accessToken(
        for credential: OneDriveCredentialBlob,
        forceRefresh: Bool,
        claims: String?
    ) async throws -> OneDriveAccessToken {
        OneDriveAccessToken(value: "test-token", expiresAt: Date().addingTimeInterval(3_600))
    }
}

private final class OneDriveRecordingTokenProvider: OneDriveAccessTokenProviding, @unchecked Sendable {
    struct Call {
        let forceRefresh: Bool
        let claims: String?
    }

    private let lock = NSLock()
    private var storage: [Call] = []

    var calls: [Call] { lock.withLock { storage } }

    func accessToken(
        for credential: OneDriveCredentialBlob,
        forceRefresh: Bool,
        claims: String?
    ) async throws -> OneDriveAccessToken {
        let callNumber = lock.withLock {
            storage.append(Call(forceRefresh: forceRefresh, claims: claims))
            return storage.count
        }
        return OneDriveAccessToken(
            value: "token-\(callNumber)",
            expiresAt: Date().addingTimeInterval(3_600)
        )
    }
}

private final class OneDriveRequestRecorder: @unchecked Sendable {
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

private final class OneDriveCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var storage = 0

    var value: Int { lock.withLock { storage } }

    func increment() -> Int {
        lock.withLock {
            storage += 1
            return storage
        }
    }
}

private final class OneDriveStringRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [String] = []

    var values: [String] { lock.withLock { storage } }

    func append(_ value: String) {
        lock.withLock { storage.append(value) }
    }
}

private final class OneDriveMockURLProtocol: URLProtocol {
    struct Response {
        let data: Data
        let status: Int
        let headers: [String: String]

        static func json(
            _ json: String,
            status: Int = 200,
            headers: [String: String] = [:]
        ) -> Response {
            Response(
                data: Data(json.utf8),
                status: status,
                headers: headers.merging(["Content-Type": "application/json"]) { current, _ in current }
            )
        }

        static func status(_ status: Int, headers: [String: String] = [:]) -> Response {
            Response(data: Data(), status: status, headers: headers)
        }
    }

    static var handler: ((URLRequest) throws -> Response)?

    override class func canInit(with request: URLRequest) -> Bool { true }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler,
              let url = request.url else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }
        let result: Response
        do {
            result = try handler(request)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
            return
        }
        guard let response = HTTPURLResponse(
            url: url,
            statusCode: result.status,
            httpVersion: "HTTP/1.1",
            headerFields: result.headers
        ) else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        if !result.data.isEmpty { client?.urlProtocol(self, didLoad: result.data) }
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

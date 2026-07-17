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
        XCTAssertEqual(result.connectionParams.displayRootPath, "OneDrive/Apps/Watermelon")
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
            message: "Unable to retrieve user's mysite URL.",
            retryAfter: nil,
            claims: nil
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
        try await client.deleteKnownPresentFile(OneDriveKnownFile(
            path: "/backup.sqlite.bak",
            itemID: "backup-id",
            eTag: nil,
            size: nil
        ))

        XCTAssertFalse(recorder.requests.contains { request in
            request.httpMethod == "GET" && request.url?.path.hasSuffix("/items/root:/backup.sqlite.bak") == true
        })
        XCTAssertTrue(recorder.requests.contains { request in
            request.httpMethod == "DELETE" && request.url?.path.hasSuffix("/items/backup-id") == true
        })
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
                return .json(Self.item(id: "temp-id", name: "month.sqlite.tmp", folder: false))
            }
            if host == "graph.microsoft.com", path.hasSuffix("/items/root:/month.sqlite") {
                return .json(Self.item(id: "final-old-id", name: "month.sqlite", folder: false))
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
        try await client.downloadForReadBackVerification(remotePath: "/month.sqlite", localURL: downloadURL)

        XCTAssertTrue(outcome.backedUpPriorFinal)
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
                    await sharedState.throttleGate.record(retryAfter: Date().addingTimeInterval(0.05))
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
        await gate.record(retryAfter: Date().addingTimeInterval(30))
        do {
            try await gate.requirePermit()
            XCTFail("Expected throttling error")
        } catch {
            XCTAssertTrue(OneDriveErrorClassifier.isConnectionUnavailable(error))
            XCTAssertEqual((error as NSError).code, 429)
        }
    }

    func testThrottleGateCanWaitForAcceptedOperation() async throws {
        let gate = OneDriveThrottleGate()
        await gate.record(retryAfter: Date().addingTimeInterval(0.05))
        try await gate.waitForPermit()
        try await gate.requirePermit()
    }

    private func makeClient(
        sharedState: OneDriveSharedState = OneDriveSharedState(),
        stallTimeouts: URLSessionStallWatchdog.Timeouts? = nil
    ) -> OneDriveClient {
        let params = OneDriveConnectionParams(
            driveID: "drive",
            rootItemID: "root",
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

    private static func item(id: String, name: String, folder: Bool, size: Int64 = 0) -> String {
        let facet = folder ? "\"folder\":{}" : "\"file\":{}"
        return "{\"id\":\"\(id)\",\"name\":\"\(name)\",\"size\":\(size),\(facet),\"parentReference\":{\"driveId\":\"drive\"}}"
    }

    private static func credential(homeAccountIdentifier: String) -> OneDriveCredentialBlob {
        OneDriveCredentialBlob(
            homeAccountIdentifier: homeAccountIdentifier,
            tenantID: "tenant",
            authorityEnvironment: "login.microsoftonline.com"
        )
    }
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

import XCTest
@testable import Watermelon

final class GoogleDriveClientTests: XCTestCase {
    override func tearDown() {
        GoogleDriveMockURLProtocol.handler = nil
        super.tearDown()
    }

    func testCredentialAndProfileCrossFactoryBoundary() throws {
        let params = GoogleDriveConnectionParams(
            clientID: " 123456789012-iosclient.apps.googleusercontent.com ",
            accountSubject: " subject ",
            rootFolderID: " root ",
            lockRootSlotID: " slot ",
            displayRootPath: "/Watermelon"
        )
        let profile = ServerProfileRecord(
            name: "Google Drive",
            storageType: StorageType.googleDrive.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(params),
            sortOrder: 0,
            host: "www.googleapis.com",
            port: 443,
            shareName: "root",
            basePath: "/",
            username: "account@example.com",
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
        let credential = GoogleDriveCredentialBlob(accountSubject: " subject ", refreshToken: " refresh ")

        let decodedCredential = try GoogleDriveCredentialBlob.decode(from: credential.encodedJSONString())
        XCTAssertEqual(decodedCredential.accountSubject, "subject")
        XCTAssertEqual(decodedCredential.refreshToken, "refresh")
        let descriptor = try StorageClientFactory.canonicalConnection(for: profile)
        guard case .googleDrive(let connection) = descriptor else {
            return XCTFail("Expected Google Drive descriptor")
        }
        XCTAssertEqual(connection.accountSubject, "subject")
        XCTAssertEqual(connection.rootFolderID, "root")
        XCTAssertEqual(connection.lockRootSlotID, "slot")
        XCTAssertEqual(
            descriptor.publishedV2IdentityComponents,
            [connection.clientID.lowercased(), "subject", "root"]
        )
        XCTAssertEqual(descriptor.publishedV2RemoteIdentityComponents, ["subject", "root"])
        let client = try StorageClientFactory(
            googleDriveTokenProvider: GoogleDriveTestTokenProvider()
        ).makeClient(
            profile: profile,
            credentialPayload: try credential.encodedJSONString()
        )
        XCTAssertTrue(client is GoogleDriveClient)
        XCTAssertFalse(client.supportsLegacyV1Migration())
        XCTAssertFalse(client.shouldSetModificationDate())
        XCTAssertTrue(client.trustsLeaseConfidenceForDestructiveWrite())
    }

    @MainActor
    func testOAuthUsesReversedClientSchemePKCEAndRequiredDriveScopes() throws {
        let clientID = "123456789012-iosclient.apps.googleusercontent.com"
        let scheme = try GoogleDriveOAuthClientConfiguration.callbackScheme(for: clientID)
        let redirectURI = try GoogleDriveOAuthClientConfiguration.redirectURI(for: clientID)

        XCTAssertEqual(
            scheme,
            "com.googleusercontent.apps.123456789012-iosclient"
        )
        XCTAssertEqual(redirectURI, scheme + ":/oauth2redirect")
        let url = try GoogleDriveOAuthService.authorizationURL(
            clientID: clientID,
            redirectURI: redirectURI,
            state: "state",
            challenge: "challenge",
            forceReauthentication: false
        )
        let items = Dictionary(uniqueKeysWithValues: try XCTUnwrap(
            URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems
        ).compactMap { item in item.value.map { (item.name, $0) } })
        XCTAssertEqual(items["code_challenge_method"], "S256")
        XCTAssertTrue(items["scope"]?.contains("https://www.googleapis.com/auth/drive.file") == true)
        XCTAssertTrue(items["scope"]?.contains("https://www.googleapis.com/auth/drive.appdata") == true)
        XCTAssertFalse(items["scope"]?.contains("https://www.googleapis.com/auth/drive ") == true)
        XCTAssertEqual(items["access_type"], "offline")
    }

    func testMutationOutcomeClassificationDoesNotTreatRateLimitAsAmbiguousCommit() throws {
        let response = try XCTUnwrap(HTTPURLResponse(
            url: URL(string: "https://www.googleapis.com/drive/v3/files")!,
            statusCode: 429,
            httpVersion: nil,
            headerFields: nil
        ))
        let rateLimit = GoogleDriveErrorClassifier.makeServiceError(
            data: Data("{\"error\":{\"code\":429}}".utf8),
            response: response
        )

        XCTAssertTrue(GoogleDriveErrorClassifier.isConnectionUnavailable(rateLimit))
        XCTAssertFalse(GoogleDriveErrorClassifier.isMutationOutcomeUnknown(rateLimit))
        XCTAssertTrue(GoogleDriveErrorClassifier.isMutationOutcomeUnknown(URLError(.timedOut)))
        XCTAssertTrue(GoogleDriveErrorClassifier.isMutationOutcomeUnknown(URLError(.networkConnectionLost)))
        XCTAssertFalse(GoogleDriveErrorClassifier.isMutationOutcomeUnknown(URLError(.notConnectedToInternet)))
    }

    func testRootBootstrapFindsExistingRootOnLaterPageWithoutCreatingAnother() async throws {
        let server = GoogleDriveRootBootstrapMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [GoogleDriveMockURLProtocol.self]
        let service = GoogleDriveAppFolderBootstrapService(
            clientID: "123456789012-test.apps.googleusercontent.com",
            credential: GoogleDriveCredentialBlob(accountSubject: "subject", refreshToken: "refresh"),
            tokenProvider: GoogleDriveTestTokenProvider(),
            sharedState: GoogleDriveSharedState(),
            sessionConfiguration: configuration
        )

        let result = try await service.resolveOrCreateRoot()

        XCTAssertEqual(result.rootFolderID, "root-folder")
        XCTAssertEqual(result.lockRootSlotID, "slot-root")
        XCTAssertEqual(server.listCount, 2)
        XCTAssertEqual(server.mutationCount, 0)
    }

    func testRootBootstrapWaitsForSuccessfulCreateToAppearInSearch() async throws {
        let server = GoogleDriveEventuallyVisibleRootMockServer(hiddenSearchesAfterCreate: 2)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [GoogleDriveMockURLProtocol.self]
        let service = GoogleDriveAppFolderBootstrapService(
            clientID: "123456789012-test.apps.googleusercontent.com",
            credential: GoogleDriveCredentialBlob(accountSubject: "subject", refreshToken: "refresh"),
            tokenProvider: GoogleDriveTestTokenProvider(),
            sharedState: GoogleDriveSharedState(),
            sessionConfiguration: configuration
        )

        let result = try await service.resolveOrCreateRoot()

        XCTAssertEqual(result.rootFolderID, "created-root")
        XCTAssertEqual(result.displayRootPath, "/Watermelon Backup")
        XCTAssertEqual(server.createdRootIDs, ["created-root"])
        XCTAssertEqual(server.postCreateSearchCount, 4)
    }

    func testRootBootstrapRotatesLockSlotWhenAppDataWasCleared() async throws {
        let server = GoogleDriveClearedAppDataRootMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [GoogleDriveMockURLProtocol.self]
        let service = GoogleDriveAppFolderBootstrapService(
            clientID: "123456789012-test.apps.googleusercontent.com",
            credential: GoogleDriveCredentialBlob(accountSubject: "subject", refreshToken: "refresh"),
            tokenProvider: GoogleDriveTestTokenProvider(),
            sharedState: GoogleDriveSharedState(),
            sessionConfiguration: configuration
        )

        let result = try await service.resolveOrCreateRoot()

        XCTAssertEqual(result.rootFolderID, "root-folder")
        XCTAssertEqual(result.lockRootSlotID, "fresh-slot")
        XCTAssertEqual(server.rootCreateCount, 0)
        XCTAssertEqual(server.rootUpdateCount, 1)
    }

    func testTokenRefreshIsPublicClientAndUsesOneRequest() async throws {
        let recorder = GoogleDriveRequestRecorder()
        GoogleDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            if request.url?.host == "oauth2.googleapis.com" {
                return .json("{\"access_token\":\"fresh-token\",\"expires_in\":3600}")
            }
            return .json("{\"sub\":\"subject\",\"email\":\"account@example.com\"}")
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [GoogleDriveMockURLProtocol.self]
        let service = GoogleDriveTokenService(sessionConfiguration: configuration)

        let token = try await service.accessToken(
            for: GoogleDriveCredentialBlob(accountSubject: "subject", refreshToken: "refresh-token"),
            clientID: "123456789012-test.apps.googleusercontent.com",
            forceRefresh: false
        )

        XCTAssertEqual(token.value, "fresh-token")
        let tokenRequest = try XCTUnwrap(recorder.requests.first)
        let formText = String(decoding: try XCTUnwrap(tokenRequest.httpBody), as: UTF8.self)
        var form = URLComponents()
        form.percentEncodedQuery = formText
        let items = Dictionary(uniqueKeysWithValues: try XCTUnwrap(form.queryItems).compactMap { item in
            item.value.map { (item.name, $0) }
        })
        XCTAssertEqual(items["grant_type"], "refresh_token")
        XCTAssertEqual(items["refresh_token"], "refresh-token")
        XCTAssertNil(items["client_secret"])
        XCTAssertEqual(recorder.requests.count, 1)
    }

    func testTokenRefreshWaiterReturnsPromptlyWhenCancelled() async throws {
        GoogleDriveMockURLProtocol.handler = { request in
            if request.url?.host == "oauth2.googleapis.com" {
                Thread.sleep(forTimeInterval: 0.4)
                return .json("{\"access_token\":\"fresh-token\",\"expires_in\":3600}")
            }
            return .json("{\"sub\":\"subject\"}")
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [GoogleDriveMockURLProtocol.self]
        let service = GoogleDriveTokenService(sessionConfiguration: configuration)
        let task = Task {
            try await service.accessToken(
                for: GoogleDriveCredentialBlob(accountSubject: "subject", refreshToken: "refresh-token"),
                clientID: "123456789012-test.apps.googleusercontent.com",
                forceRefresh: false
            )
        }
        try await Task.sleep(for: .milliseconds(50))
        let cancellationStarted = Date()
        task.cancel()

        do {
            _ = try await task.value
            XCTFail("Expected cancellation")
        } catch is CancellationError {}

        XCTAssertLessThan(Date().timeIntervalSince(cancellationStarted), 0.2)
        try await Task.sleep(for: .milliseconds(450))
    }

    func testCancellingOneTokenWaiterDoesNotCancelSharedRefresh() async throws {
        let recorder = GoogleDriveRequestRecorder()
        GoogleDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            if request.url?.host == "oauth2.googleapis.com" {
                Thread.sleep(forTimeInterval: 0.25)
                return .json("{\"access_token\":\"shared-token\",\"expires_in\":3600}")
            }
            return .json("{\"sub\":\"subject\"}")
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [GoogleDriveMockURLProtocol.self]
        let service = GoogleDriveTokenService(sessionConfiguration: configuration)
        let credential = GoogleDriveCredentialBlob(accountSubject: "subject", refreshToken: "refresh-token")
        let first = Task {
            try await service.accessToken(
                for: credential,
                clientID: "123456789012-test.apps.googleusercontent.com",
                forceRefresh: false
            )
        }
        let second = Task {
            try await service.accessToken(
                for: credential,
                clientID: "123456789012-test.apps.googleusercontent.com",
                forceRefresh: false
            )
        }
        try await Task.sleep(for: .milliseconds(50))
        first.cancel()

        do {
            _ = try await first.value
            XCTFail("Expected the first waiter to cancel")
        } catch is CancellationError {}
        let token = try await second.value

        XCTAssertEqual(token.value, "shared-token")
        XCTAssertEqual(recorder.requests.filter { $0.url?.host == "oauth2.googleapis.com" }.count, 1)
    }

    func testListDrainsEveryPageToken() async throws {
        let recorder = GoogleDriveRequestRecorder()
        GoogleDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/files/root-folder") {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
            if query.contains(where: { $0.name == "pageToken" && $0.value == "next-page" }) {
                return .json("{\"files\":[\(Self.fileJSON(id: "second", name: "second", folder: true))]}")
            }
            return .json("{\"files\":[\(Self.fileJSON(id: "first", name: "first.jpg", folder: false))],\"nextPageToken\":\"next-page\"}")
        }
        let client = makeClient()
        try await client.connect()

        let entries = try await client.list(path: "/")

        XCTAssertEqual(entries.map(\.name), ["first.jpg", "second"])
        XCTAssertEqual(entries.map(\.isDirectory), [false, true])
        XCTAssertEqual(recorder.requests.filter { $0.url?.path == "/drive/v3/files" }.count, 2)
    }

    func testDuplicateDriveNamesFailClosedDuringPathResolution() async throws {
        GoogleDriveMockURLProtocol.handler = { request in
            if request.url?.path.hasSuffix("/files/root-folder") == true {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            return .json("{\"files\":[\(Self.fileJSON(id: "a", name: "duplicate", folder: false)),\(Self.fileJSON(id: "b", name: "duplicate", folder: false))]}")
        }
        let client = makeClient()
        try await client.connect()

        do {
            _ = try await client.metadata(path: "/duplicate")
            XCTFail("Expected ambiguous names to fail closed")
        } catch {
            guard case RemoteStorageClientError.invalidConfiguration = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
    }

    func testDuplicateDriveNamesFailClosedDuringListing() async throws {
        GoogleDriveMockURLProtocol.handler = { request in
            if request.url?.path.hasSuffix("/files/root-folder") == true {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            return .json("{\"files\":[\(Self.fileJSON(id: "a", name: "duplicate", folder: false)),\(Self.fileJSON(id: "b", name: "duplicate", folder: false))]}")
        }
        let client = makeClient()
        try await client.connect()

        do {
            _ = try await client.list(path: "/")
            XCTFail("Expected ambiguous listing to fail closed")
        } catch {
            guard case RemoteStorageClientError.invalidConfiguration = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
    }

    func testFreshResolutionRejectsDuplicateAddedAfterPathWasCached() async throws {
        let state = GoogleDriveDuplicateMutationState()
        GoogleDriveMockURLProtocol.handler = { request in
            if request.url?.path.hasSuffix("/files/root-folder") == true {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            let files = state.hasDuplicate
                ? "\(Self.fileJSON(id: "a", name: "duplicate", folder: false, parentID: "root-folder")),\(Self.fileJSON(id: "b", name: "duplicate", folder: false, parentID: "root-folder"))"
                : Self.fileJSON(id: "a", name: "duplicate", folder: false, parentID: "root-folder")
            return .json("{\"files\":[\(files)]}")
        }
        let client = makeClient()
        try await client.connect()
        let initial = try await client.metadata(path: "/duplicate")
        XCTAssertNotNil(initial)
        state.addDuplicate()

        do {
            _ = try await client.metadata(path: "/duplicate")
            XCTFail("Expected a newly ambiguous path to fail closed")
        } catch {
            guard case RemoteStorageClientError.invalidConfiguration = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
    }

    func testConnectRejectsMismatchedRemoteLockAnchor() async throws {
        GoogleDriveMockURLProtocol.handler = { request in
            guard request.url?.path.hasSuffix("/files/root-folder") == true else {
                throw URLError(.badServerResponse)
            }
            return .json(Self.fileJSON(
                id: "root-folder",
                name: "Watermelon",
                folder: true,
                lockRootSlotID: "different-slot"
            ))
        }
        let client = makeClient()

        do {
            try await client.connect()
            XCTFail("Expected lock anchor mismatch")
        } catch {
            guard case GoogleDriveAuthenticationError.accountMismatch = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
    }

    func testSameDirectoryMoveOnlyRenamesAndDoesNotMutateParents() async throws {
        let recorder = GoogleDriveRequestRecorder()
        GoogleDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/files/root-folder") {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            if path.hasSuffix("/files/source-id") {
                return .json(Self.fileJSON(
                    id: "source-id",
                    name: request.httpMethod == "PATCH" ? "final.json" : "temp.json",
                    folder: false,
                    parentID: "root-folder"
                ))
            }
            let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
            let q = query.first(where: { $0.name == "q" })?.value ?? ""
            if q.contains("name = 'temp.json'") {
                return .json("{\"files\":[\(Self.fileJSON(id: "source-id", name: "temp.json", folder: false, parentID: "root-folder"))]}")
            }
            if q.contains("name = 'final.json'") { return .json("{\"files\":[]}") }
            throw URLError(.badServerResponse)
        }
        let client = makeClient()
        try await client.connect()

        try await client.move(from: "/temp.json", to: "/final.json")

        let patch = try XCTUnwrap(recorder.requests.first { $0.httpMethod == "PATCH" })
        let queryNames = Set(URLComponents(url: patch.url!, resolvingAgainstBaseURL: false)?.queryItems?.map(\.name) ?? [])
        XCTAssertFalse(queryNames.contains("addParents"))
        XCTAssertFalse(queryNames.contains("removeParents"))
        XCTAssertTrue(String(decoding: try XCTUnwrap(patch.httpBody), as: UTF8.self).contains("final.json"))
    }

    func testAmbiguousMoveResponseReconcilesByFileIDWithoutStaleSourceAlias() async throws {
        let server = GoogleDriveAmbiguousMoveMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()

        try await client.move(from: "/temp.json", to: "/final.json")

        let final = try await client.metadata(path: "/final.json")
        let temporary = try await client.metadata(path: "/temp.json")
        XCTAssertNotNil(final)
        XCTAssertNil(temporary)
    }

    func testAmbiguousCopyResponseRecoversByGeneratedDestinationID() async throws {
        let server = GoogleDriveCopyMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()

        try await client.copy(from: "/scratch.sqlite", to: "/2024-01.sqlite")

        let copied = try await client.metadata(path: "/2024-01.sqlite")
        XCTAssertEqual(copied?.size, 5)
        XCTAssertEqual(server.copyCount, 1)
        XCTAssertEqual(server.destinationIDs, ["copy-id"])
    }

    func testConditionalFileCreatesReuseGeneratedIDBatchAndConfirmExactWinner() async throws {
        let recorder = GoogleDriveRequestRecorder()
        let state = GoogleDriveOrdinaryCreateState()
        GoogleDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/files/root-folder") {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            if path.hasSuffix("/generateIds") {
                return .json(googleDriveGeneratedIDsJSON(firstID: "generated-file-id", request: request))
            }
            if request.httpMethod == "POST", path == "/upload/drive/v3/files" {
                let creation = state.markCreated()
                let id = creation == 1 ? "generated-file-id" : "generated-file-id-1"
                let name = creation == 1 ? "asset.bin" : "asset-2.bin"
                return .json(Self.fileJSON(id: id, name: name, folder: false))
            }
            if path == "/drive/v3/files" {
                let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
                let q = query.first(where: { $0.name == "q" })?.value ?? ""
                if q.contains("name = 'asset.bin'"), state.creationCount >= 1 {
                    return .json("{\"files\":[\(Self.fileJSON(id: "generated-file-id", name: "asset.bin", folder: false))]}")
                }
                if q.contains("name = 'asset-2.bin'"), state.creationCount >= 2 {
                    return .json("{\"files\":[\(Self.fileJSON(id: "generated-file-id-1", name: "asset-2.bin", folder: false))]}")
                }
                return .json("{\"files\":[]}")
            }
            throw URLError(.badServerResponse)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        let client = makeClient()
        try await client.connect()

        try await client.upload(
            localURL: localURL,
            remotePath: "/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.upload(
            localURL: localURL,
            remotePath: "/asset-2.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let uploads = recorder.requests.filter {
            $0.httpMethod == "POST" && $0.url?.path == "/upload/drive/v3/files"
        }
        XCTAssertEqual(uploads.count, 2)
        let firstUploadBody = String(decoding: try XCTUnwrap(uploads[0].httpBody), as: UTF8.self)
        XCTAssertTrue(firstUploadBody.contains("\"id\":\"generated-file-id\""))
        XCTAssertTrue(firstUploadBody.contains("\"modifiedTime\":"))
        XCTAssertTrue(String(decoding: try XCTUnwrap(uploads[1].httpBody), as: UTF8.self).contains("\"id\":\"generated-file-id-1\""))
        let generatedIDRequests = recorder.requests.filter { $0.url?.path.hasSuffix("/generateIds") == true }
        XCTAssertEqual(generatedIDRequests.count, 1)
        let count = URLComponents(
            url: try XCTUnwrap(generatedIDRequests[0].url),
            resolvingAgainstBaseURL: false
        )?.queryItems?.first(where: { $0.name == "count" })?.value
        XCTAssertEqual(count, "64")
    }

    func testLeasedNamespaceSharesSnapshotAcrossClientsWithoutReconcilingKnownSuccess() async throws {
        let server = GoogleDriveLeasedTransferMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let sharedState = GoogleDriveSharedState()
        let leaseClient = makeClient(sharedState: sharedState)
        let workerClient = makeClient(sharedState: sharedState)
        try await leaseClient.connect()
        try await workerClient.connect()
        await leaseClient.beginLeasedNamespaceSession()

        _ = try await workerClient.list(path: "/")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer {
            try? FileManager.default.removeItem(at: localURL)
            try? FileManager.default.removeItem(at: downloadURL)
        }

        try await workerClient.upload(
            localURL: localURL,
            remotePath: "/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await workerClient.upload(
            localURL: localURL,
            remotePath: "/asset-2.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(server.uploadCount, 2)
        XCTAssertEqual(server.listRequestCount, 1)

        try await workerClient.download(remotePath: "/asset.bin", localURL: downloadURL)
        XCTAssertEqual(try Data(contentsOf: downloadURL), Data("asset".utf8))
        XCTAssertEqual(server.mediaDownloadCount, 1)
        XCTAssertEqual(server.listRequestCount, 1)

        await leaseClient.endLeasedNamespaceSession()
        let strictMetadata = try await workerClient.metadata(path: "/asset.bin")
        XCTAssertNotNil(strictMetadata)
        XCTAssertEqual(server.listRequestCount, 2)
    }

    func testUnleasedDownloadsReuseResolvedParentDirectories() async throws {
        let server = GoogleDriveLeasedTransferMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        try await client.createDirectory(path: "/2024/03")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let firstDownloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let secondDownloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let repeatedDownloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer {
            try? FileManager.default.removeItem(at: localURL)
            try? FileManager.default.removeItem(at: firstDownloadURL)
            try? FileManager.default.removeItem(at: secondDownloadURL)
            try? FileManager.default.removeItem(at: repeatedDownloadURL)
        }
        try await client.upload(
            localURL: localURL,
            remotePath: "/2024/03/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.upload(
            localURL: localURL,
            remotePath: "/2024/03/asset-2.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        await client.endLeasedNamespaceSession()

        let beforeFirstDownload = server.totalRequestCount
        try await client.download(
            remotePath: "/2024/03/asset.bin",
            localURL: firstDownloadURL
        )
        XCTAssertEqual(server.totalRequestCount, beforeFirstDownload + 4)

        let beforeSecondDownload = server.totalRequestCount
        try await client.download(
            remotePath: "/2024/03/asset-2.bin",
            localURL: secondDownloadURL
        )
        XCTAssertEqual(server.totalRequestCount, beforeSecondDownload + 2)

        let beforeRepeatedDownload = server.totalRequestCount
        try await client.download(
            remotePath: "/2024/03/asset.bin",
            localURL: repeatedDownloadURL
        )
        XCTAssertEqual(server.totalRequestCount, beforeRepeatedDownload + 2)
    }

    func testLeasedNamespaceInstallsOneSnapshotForNewMonthUploads() async throws {
        let server = GoogleDriveLeasedTransferMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        let requestsBeforeDirectoryCreation = server.totalRequestCount
        try await client.createDirectory(path: "/2024/03")
        XCTAssertEqual(server.totalRequestCount, requestsBeforeDirectoryCreation + 4)
        let listsAfterDirectoryCreation = server.listRequestCount
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        let requestsBeforeUploads = server.totalRequestCount

        try await client.upload(
            localURL: localURL,
            remotePath: "/2024/03/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.upload(
            localURL: localURL,
            remotePath: "/2024/03/asset-2.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(server.uploadCount, 2)
        XCTAssertEqual(server.listRequestCount, listsAfterDirectoryCreation)
        XCTAssertEqual(server.totalRequestCount, requestsBeforeUploads + 2)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedNestedListingReusesSeededAncestorSnapshot() async throws {
        let server = GoogleDriveLeasedTransferMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        try await client.createDirectory(path: "/2024/03")
        await client.endLeasedNamespaceSession()

        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let requestsBeforeMonthListing = server.totalRequestCount
        _ = try await client.list(path: "/2024/03")

        XCTAssertEqual(server.totalRequestCount, requestsBeforeMonthListing + 2)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedControlDirectoryCommitUsesConfirmedSnapshot() async throws {
        let server = GoogleDriveLeasedTransferMockServer(seedWatermelonFolder: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let requestsBeforeControlCommit = server.totalRequestCount
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let readBackURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer {
            try? FileManager.default.removeItem(at: localURL)
            try? FileManager.default.removeItem(at: readBackURL)
        }

        try await client.createDirectory(path: "/.watermelon")
        try await client.upload(
            localURL: localURL,
            remotePath: "/.watermelon/version.tmp",
            mode: .replace,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.move(
            from: "/.watermelon/version.tmp",
            to: "/.watermelon/version.json"
        )
        try await client.download(
            remotePath: "/.watermelon/version.json",
            localURL: readBackURL
        )

        XCTAssertEqual(server.totalRequestCount, requestsBeforeControlCommit + 5)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedMissingControlDirectoryUsesCreateResponseAsSnapshot() async throws {
        let server = GoogleDriveLeasedTransferMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let requestsBeforeCreation = server.totalRequestCount
        let listsBeforeCreation = server.listRequestCount

        try await client.createDirectory(path: "/.watermelon")
        let missingChild = try await client.metadata(path: "/.watermelon/version.json")

        XCTAssertNil(missingChild)
        XCTAssertEqual(server.totalRequestCount, requestsBeforeCreation + 2)
        XCTAssertEqual(server.listRequestCount, listsBeforeCreation)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedUploadRetriesAfterGeneratedIDRequestFails() async throws {
        let server = GoogleDriveLeasedTransferMockServer(failFirstGeneratedIDRequest: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/asset.bin",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected generated ID request to fail")
        } catch {
            XCTAssertTrue(GoogleDriveErrorClassifier.isConnectionUnavailable(error))
        }

        try await client.upload(
            localURL: localURL,
            remotePath: "/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(server.generatedIDRequestCount, 2)
        XCTAssertEqual(server.uploadCount, 1)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedKnownMissingPathDoesNotFallBackToRemoteResolution() async throws {
        let server = GoogleDriveLeasedTransferMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let requestCount = server.totalRequestCount

        let missingMetadata = try await client.metadata(path: "/missing.bin")
        let missingExists = try await client.exists(path: "/missing.bin")
        XCTAssertNil(missingMetadata)
        XCTAssertFalse(missingExists)
        XCTAssertEqual(server.totalRequestCount, requestCount)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedSameDirectoryMoveUsesSnapshotWithoutPreflightRequests() async throws {
        let server = GoogleDriveLeasedTransferMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        try await client.upload(
            localURL: localURL,
            remotePath: "/source.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        let requestCount = server.totalRequestCount

        try await client.move(from: "/source.bin", to: "/final.bin")

        XCTAssertEqual(server.totalRequestCount, requestCount + 1)
        let sourceMetadata = try await client.metadata(path: "/source.bin")
        let finalMetadata = try await client.metadata(path: "/final.bin")
        XCTAssertNil(sourceMetadata)
        XCTAssertNotNil(finalMetadata)
        XCTAssertEqual(server.totalRequestCount, requestCount + 1)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedDeleteUsesSnapshotWithoutPreflightRequests() async throws {
        let server = GoogleDriveLeasedTransferMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        try await client.upload(
            localURL: localURL,
            remotePath: "/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        let requestCount = server.totalRequestCount

        try await client.delete(path: "/asset.bin")

        XCTAssertEqual(server.totalRequestCount, requestCount + 1)
        let metadata = try await client.metadata(path: "/asset.bin")
        XCTAssertNil(metadata)
        XCTAssertEqual(server.totalRequestCount, requestCount + 1)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedDeleteRecoversWhenResponseIsLostAfterCommit() async throws {
        let server = GoogleDriveLeasedTransferMockServer(failFirstDeleteAfterCommit: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        try await client.upload(
            localURL: localURL,
            remotePath: "/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        let requestCount = server.totalRequestCount

        try await client.delete(path: "/asset.bin")

        XCTAssertEqual(server.totalRequestCount, requestCount + 2)
        let metadata = try await client.metadata(path: "/asset.bin")
        XCTAssertNil(metadata)
        XCTAssertEqual(server.totalRequestCount, requestCount + 2)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedUnknownCreateRecoversByFixedIDOnNextUploadAttempt() async throws {
        let server = GoogleDriveLeasedTransferMockServer(failFirstCreateAndRecoveryAfterCommit: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/asset.bin",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected the ambiguous create recovery to fail")
        } catch {
            XCTAssertTrue(GoogleDriveErrorClassifier.isConnectionUnavailable(error))
        }

        try await client.upload(
            localURL: localURL,
            remotePath: "/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        XCTAssertEqual(server.uploadCount, 1)
        let metadata = try await client.metadata(path: "/asset.bin")
        XCTAssertNotNil(metadata)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedUnknownCreateIsResolvedByAuthoritativeList() async throws {
        let server = GoogleDriveLeasedTransferMockServer(failFirstCreateAndRecoveryAfterCommit: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/asset.bin",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected the first create response to be indeterminate")
        } catch {
            XCTAssertTrue(GoogleDriveErrorClassifier.isConnectionUnavailable(error))
        }
        _ = try await client.list(path: "/")
        XCTAssertEqual(server.uploadCount, 1)
        let metadata = try await client.metadata(path: "/asset.bin")
        XCTAssertNotNil(metadata)
        await client.endLeasedNamespaceSession()
    }

    func testLeasedUnknownCreateKeepsFixedIDAcrossNegativeRecoveryAndEmptyList() async throws {
        let server = GoogleDriveLeasedTransferMockServer(delayFirstCreateUntilSameIDRetry: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        await client.beginLeasedNamespaceSession()
        _ = try await client.list(path: "/")
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/asset.bin",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected the first create result to remain unknown")
        } catch {
            XCTAssertTrue(GoogleDriveErrorClassifier.isConnectionUnavailable(error))
        }

        _ = try await client.list(path: "/")
        try await client.upload(
            localURL: localURL,
            remotePath: "/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(server.uploadItemIDs.count, 2)
        XCTAssertEqual(Set(server.uploadItemIDs).count, 1)
        XCTAssertEqual(server.storedFileCount, 1)
        await client.endLeasedNamespaceSession()
    }

    func testStrictCreateRecoversWhenWinnerListFailsAfterUpload() async throws {
        let server = GoogleDriveLeasedTransferMockServer(failFirstUploadWinnerList: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        try await client.upload(
            localURL: localURL,
            remotePath: "/asset.bin",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(server.uploadCount, 1)
        let metadata = try await client.metadata(path: "/asset.bin")
        XCTAssertNotNil(metadata)
    }

    func testStrictUnknownCreateCanBeVerifiedWithoutUploadingAgain() async throws {
        let server = GoogleDriveLeasedTransferMockServer(failFirstCreateAndRecoveryAfterCommit: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/asset.bin",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected the first upload outcome to remain unknown")
        } catch {
            XCTAssertTrue(GoogleDriveErrorClassifier.isConnectionUnavailable(error))
        }

        let matches = try await client.remoteFileMatches(localURL: localURL, remotePath: "/asset.bin")
        XCTAssertTrue(matches)
        XCTAssertEqual(server.uploadCount, 1)
    }

    func testLeasedNamespaceRebindsRecoveredMoveAndAllowsTempNameReuse() async throws {
        let state = GoogleDriveWriteSessionState()
        let key = GoogleDriveWriteSessionKey(accountSubject: "subject", rootFolderID: "root-folder")
        let root = GoogleDriveFile(
            id: "root-folder",
            name: "Watermelon",
            mimeType: GoogleDriveConstants.folderMIMEType,
            size: nil,
            md5Checksum: nil,
            createdTime: nil,
            modifiedTime: nil,
            parents: nil,
            trashed: false,
            appProperties: nil
        )
        let temp = GoogleDriveFile(
            id: "temp-id",
            name: "manifest.tmp",
            mimeType: "application/octet-stream",
            size: "5",
            md5Checksum: nil,
            createdTime: nil,
            modifiedTime: nil,
            parents: [root.id],
            trashed: false,
            appProperties: nil
        )
        let generation = await state.begin(for: key, root: root)
        try await state.install(
            path: "/",
            folder: root,
            childrenByName: [:],
            key: key,
            generation: generation
        )
        guard case .ready(let ticket) = await state.prepareUpload(
            parentPath: "/",
            name: "manifest.tmp",
            mode: .replace,
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected the first temp upload to reserve its name")
        }
        let boundResult = await state.bindUpload(
            ticket,
            itemID: temp.id,
            expectedSize: 5,
            key: key
        )
        let bound = try XCTUnwrap(boundResult)
        let committed = await state.completeUpload(bound, file: temp, key: key)
        XCTAssertTrue(committed)

        let final = GoogleDriveFile(
            id: temp.id,
            name: "manifest.sqlite",
            mimeType: temp.mimeType,
            size: temp.size,
            md5Checksum: temp.md5Checksum,
            createdTime: temp.createdTime,
            modifiedTime: temp.modifiedTime,
            parents: temp.parents,
            trashed: false,
            appProperties: temp.appProperties
        )
        await state.applyMove(
            final,
            from: "/manifest.tmp",
            to: "/manifest.sqlite",
            key: key,
            generation: generation
        )
        try await state.install(
            path: "/",
            folder: root,
            childrenByName: ["manifest.sqlite": final],
            key: key,
            generation: generation
        )

        guard case .ready = await state.prepareUpload(
            parentPath: "/",
            name: "manifest.tmp",
            mode: .replace,
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected a moved temp name to be reusable")
        }
    }

    func testWriteSessionKeepsUncertainUploadAsOneExplicitEntryState() async throws {
        let state = GoogleDriveWriteSessionState()
        let key = GoogleDriveWriteSessionKey(accountSubject: "subject", rootFolderID: "root-folder")
        let root = GoogleDriveFile(
            id: "root-folder",
            name: "Watermelon",
            mimeType: GoogleDriveConstants.folderMIMEType,
            size: nil,
            md5Checksum: nil,
            createdTime: nil,
            modifiedTime: nil,
            parents: nil,
            trashed: false,
            appProperties: nil
        )
        let generation = await state.begin(for: key, root: root)
        try await state.install(
            path: "/",
            folder: root,
            childrenByName: [:],
            key: key,
            generation: generation
        )
        guard case .ready(let ticket) = await state.prepareUpload(
            parentPath: "/",
            name: "asset.bin",
            mode: .createIfAbsent,
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected an upload plan")
        }
        guard case .busy = await state.lookup(
            path: "/asset.bin",
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected an in-flight entry")
        }
        let boundResult = await state.bindUpload(
            ticket,
            itemID: "asset-id",
            expectedSize: 5,
            key: key
        )
        let bound = try XCTUnwrap(boundResult)
        await state.markUploadUncertain(bound, expectedMD5: nil, key: key)
        guard case .recover(let recovery) = await state.prepareUpload(
            parentPath: "/",
            name: "asset.bin",
            mode: .createIfAbsent,
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected the same upload plan to be recoverable")
        }
        XCTAssertEqual(recovery.itemID, "asset-id")
        await state.cancelUpload(recovery.ticket, key: key)
        guard case .missing = await state.lookup(
            path: "/asset.bin",
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected rollback to restore the known-missing entry")
        }
        guard case .ready(let retryTicket) = await state.prepareUpload(
            parentPath: "/",
            name: "asset.bin",
            mode: .createIfAbsent,
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected a fresh upload plan after rollback")
        }
        let retryBoundResult = await state.bindUpload(
            retryTicket,
            itemID: "asset-id-2",
            expectedSize: 5,
            key: key
        )
        let retryBound = try XCTUnwrap(retryBoundResult)
        let uploaded = GoogleDriveFile(
            id: retryBound.itemID,
            name: "asset.bin",
            mimeType: "application/octet-stream",
            size: "5",
            md5Checksum: nil,
            createdTime: nil,
            modifiedTime: nil,
            parents: [root.id],
            trashed: false,
            appProperties: nil
        )
        let completed = await state.completeUpload(retryBound, file: uploaded, key: key)
        XCTAssertTrue(completed)
        await state.observe(uploaded, path: "/asset.bin", key: key, generation: generation)
    }

    func testWriteSessionAuthoritativeListRestoresUnchangedReplace() async throws {
        let state = GoogleDriveWriteSessionState()
        let key = GoogleDriveWriteSessionKey(accountSubject: "subject", rootFolderID: "root-folder")
        let root = GoogleDriveFile(
            id: "root-folder",
            name: "Watermelon",
            mimeType: GoogleDriveConstants.folderMIMEType,
            size: nil,
            md5Checksum: nil,
            createdTime: nil,
            modifiedTime: nil,
            parents: nil,
            trashed: false,
            appProperties: nil
        )
        let previous = GoogleDriveFile(
            id: "asset-id",
            name: "asset.bin",
            mimeType: "application/octet-stream",
            size: "5",
            md5Checksum: "c04e34d445e31a2159c1bfeb882ba212",
            createdTime: nil,
            modifiedTime: nil,
            parents: [root.id],
            trashed: false,
            appProperties: nil
        )
        let generation = await state.begin(for: key, root: root)
        try await state.install(
            path: "/",
            folder: root,
            childrenByName: ["asset.bin": previous],
            key: key,
            generation: generation
        )
        guard case .ready(let ticket) = await state.prepareUpload(
            parentPath: "/",
            name: "asset.bin",
            mode: .replace,
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected a replace upload plan")
        }
        let boundResult = await state.bindUpload(
            ticket,
            itemID: previous.id,
            expectedSize: 5,
            key: key
        )
        let bound = try XCTUnwrap(boundResult)
        await state.markUploadUncertain(
            bound,
            expectedMD5: "7d793037a0760186574b0282f2f435e7",
            key: key
        )

        try await state.install(
            path: "/",
            folder: root,
            childrenByName: ["asset.bin": previous],
            key: key,
            generation: generation
        )

        guard case .file(let restored) = await state.lookup(
            path: "/asset.bin",
            key: key,
            generation: generation
        ) else {
            return XCTFail("Expected the unchanged previous file to be restored")
        }
        XCTAssertEqual(restored.id, previous.id)
    }

    func testLeasedParentRefreshInvalidatesReplacedChildDirectorySnapshot() async throws {
        let state = GoogleDriveWriteSessionState()
        let key = GoogleDriveWriteSessionKey(accountSubject: "subject", rootFolderID: "root-folder")
        let root = GoogleDriveFile(
            id: "root-folder",
            name: "Watermelon",
            mimeType: GoogleDriveConstants.folderMIMEType,
            size: nil,
            md5Checksum: nil,
            createdTime: nil,
            modifiedTime: nil,
            parents: nil,
            trashed: false,
            appProperties: nil
        )
        let oldFolder = GoogleDriveFile(
            id: "old-album",
            name: "album",
            mimeType: GoogleDriveConstants.folderMIMEType,
            size: nil,
            md5Checksum: nil,
            createdTime: nil,
            modifiedTime: nil,
            parents: [root.id],
            trashed: false,
            appProperties: nil
        )
        let newFolder = GoogleDriveFile(
            id: "new-album",
            name: "album",
            mimeType: GoogleDriveConstants.folderMIMEType,
            size: nil,
            md5Checksum: nil,
            createdTime: nil,
            modifiedTime: nil,
            parents: [root.id],
            trashed: false,
            appProperties: nil
        )
        let child = GoogleDriveFile(
            id: "asset-id",
            name: "asset.bin",
            mimeType: "application/octet-stream",
            size: "5",
            md5Checksum: "c04e34d445e31a2159c1bfeb882ba212",
            createdTime: nil,
            modifiedTime: nil,
            parents: [oldFolder.id],
            trashed: false,
            appProperties: nil
        )
        let generation = await state.begin(for: key, root: root)
        try await state.install(
            path: "/",
            folder: root,
            childrenByName: ["album": oldFolder],
            key: key,
            generation: generation
        )
        try await state.install(
            path: "/album",
            folder: oldFolder,
            childrenByName: ["asset.bin": child],
            key: key,
            generation: generation
        )

        try await state.install(
            path: "/",
            folder: root,
            childrenByName: ["album": newFolder],
            key: key,
            generation: generation
        )

        let staleChild = await state.lookup(
            path: "/album/asset.bin",
            key: key,
            generation: generation
        )
        let staleSnapshot = await state.isDirectoryLoaded(
            path: "/album",
            key: key,
            generation: generation
        )
        guard case .unavailable = staleChild else {
            return XCTFail("Expected the replaced directory subtree to be invalidated")
        }
        XCTAssertFalse(staleSnapshot)
        await state.end(for: key, generation: generation)
    }

    func testUploadRejectsParentMovedAfterItWasCached() async throws {
        let state = GoogleDriveParentMutationState()
        let recorder = GoogleDriveRequestRecorder()
        GoogleDriveMockURLProtocol.handler = { request in
            recorder.append(request)
            let path = request.url?.path ?? ""
            if path.hasSuffix("/files/root-folder") {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            if path == "/drive/v3/files" {
                let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
                let q = query.first(where: { $0.name == "q" })?.value ?? ""
                if q.contains("name = '2024'") {
                    return state.isMoved
                        ? .json("{\"files\":[]}")
                        : .json("{\"files\":[\(Self.fileJSON(id: "year-id", name: "2024", folder: true, parentID: "root-folder"))]}")
                }
                return .json("{\"files\":[]}")
            }
            throw URLError(.badServerResponse)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        let client = makeClient()
        try await client.connect()
        let parent = try await client.metadata(path: "/2024")
        XCTAssertNotNil(parent)
        state.move()

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/2024/asset.bin",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected stale parent validation to fail")
        } catch {
            XCTAssertTrue(GoogleDriveErrorClassifier.isNotFound(error))
        }

        XCTAssertFalse(recorder.requests.contains { $0.url?.path == "/upload/drive/v3/files" })
    }

    func testFreshResolutionUsesReplacementAncestorInsteadOfCachedParent() async throws {
        let state = GoogleDriveAncestorReplacementState()
        GoogleDriveMockURLProtocol.handler = { request in
            let path = request.url?.path ?? ""
            if path.hasSuffix("/files/root-folder") {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            if path == "/drive/v3/files" {
                let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
                let q = query.first(where: { $0.name == "q" })?.value ?? ""
                if q.contains("'root-folder' in parents"), q.contains("name = 'album'") {
                    let id = state.isReplaced ? "new-album" : "old-album"
                    return .json("{\"files\":[\(Self.fileJSON(id: id, name: "album", folder: true, parentID: "root-folder"))]}")
                }
                if q.contains("'new-album' in parents"), q.contains("name = 'asset.jpg'") {
                    return .json("{\"files\":[\(Self.fileJSON(id: "asset-id", name: "asset.jpg", folder: false, parentID: "new-album"))]}")
                }
                return .json("{\"files\":[]}")
            }
            throw URLError(.badServerResponse)
        }
        let client = makeClient()
        try await client.connect()
        let initial = try await client.metadata(path: "/album")
        XCTAssertNotNil(initial)
        state.replace()

        let child = try await client.metadata(path: "/album/asset.jpg")

        XCTAssertEqual(child?.name, "asset.jpg")
    }

    func testReplaceUploadFailsClosedWhenFinalPathBecomesAmbiguous() async throws {
        let state = GoogleDriveUploadMutationState()
        GoogleDriveMockURLProtocol.handler = { request in
            let path = request.url?.path ?? ""
            if path.hasSuffix("/files/root-folder") {
                return .json(Self.fileJSON(id: "root-folder", name: "Watermelon", folder: true))
            }
            if request.httpMethod == "PATCH", path == "/upload/drive/v3/files/existing-id" {
                state.addDuplicate()
                return .json(Self.fileJSON(
                    id: "existing-id",
                    name: "asset.bin",
                    folder: false,
                    parentID: "root-folder"
                ))
            }
            if path == "/drive/v3/files" {
                let existing = Self.fileJSON(
                    id: "existing-id",
                    name: "asset.bin",
                    folder: false,
                    parentID: "root-folder"
                )
                let files = state.hasDuplicate
                    ? "\(existing),\(Self.fileJSON(id: "duplicate-id", name: "asset.bin", folder: false, parentID: "root-folder"))"
                    : existing
                return .json("{\"files\":[\(files)]}")
            }
            throw URLError(.badServerResponse)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("asset".utf8).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        let client = makeClient()
        try await client.connect()

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/asset.bin",
                mode: .replace,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected ambiguous final path to fail closed")
        } catch {
            XCTAssertTrue(remoteStorageIsNameCollision(error))
        }
    }

    func testLargeUploadUsesResumableChunks() async throws {
        let server = GoogleDriveResumableMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x5a, count: 9 * 1024 * 1024).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        let client = makeClient()
        try await client.connect()

        try await client.upload(
            localURL: localURL,
            remotePath: "/large.mov",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(server.contentRanges, [
            "bytes 0-8388607/9437184",
            "bytes 8388608-9437183/9437184"
        ])
    }

    func testResumableUploadStopsAfterRepeatedNoProgress() async throws {
        let server = GoogleDriveStalledResumableMockServer(chunkError: URLError(.networkConnectionLost))
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x5a, count: 6 * 1024 * 1024).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        let client = makeClient()
        try await client.connect()

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/stalled.mov",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected stalled resumable upload to stop")
        } catch {}

        XCTAssertEqual(server.chunkAttempts, 4)
        XCTAssertEqual(server.statusQueries, 4)
    }

    func testResumableCancellationDoesNotQuerySessionStatus() async throws {
        let server = GoogleDriveStalledResumableMockServer(chunkError: URLError(.cancelled))
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x5a, count: 6 * 1024 * 1024).write(to: localURL)
        defer { try? FileManager.default.removeItem(at: localURL) }
        let client = makeClient()
        try await client.connect()

        do {
            try await client.upload(
                localURL: localURL,
                remotePath: "/cancelled.mov",
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            XCTFail("Expected cancellation")
        } catch is CancellationError {}

        XCTAssertEqual(server.chunkAttempts, 1)
        XCTAssertEqual(server.statusQueries, 0)
    }

    func testConcurrentDirectoryCreationSharesOneParentFolder() async throws {
        let server = GoogleDriveDirectoryMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let sharedState = GoogleDriveSharedState()
        let first = makeClient(sharedState: sharedState)
        let second = makeClient(sharedState: sharedState)
        try await first.connect()
        try await second.connect()

        async let january: Void = first.createDirectory(path: "/2024/01")
        async let february: Void = second.createDirectory(path: "/2024/02")
        _ = try await (january, february)

        XCTAssertEqual(server.yearFolderCount, 1)
        XCTAssertEqual(server.monthNames, ["01", "02"])
    }

    func testControlDirectoryCreationDoesNotCreateVisibleDriveFolders() async throws {
        let server = GoogleDriveDirectoryMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let first = makeClient()
        let second = makeClient()
        try await first.connect()
        try await second.connect()

        async let firstCreate: Void = first.createDirectory(path: "/.watermelon/locks")
        async let secondCreate: Void = second.createDirectory(path: "/.watermelon/locks")
        _ = try await (firstCreate, secondCreate)

        XCTAssertEqual(server.watermelonFolderCount, 0)
        XCTAssertEqual(server.locksFolderCount, 0)
    }

    func testEmptyVirtualControlDirectoryListsAppDataWithoutVisibleFolders() async throws {
        let server = GoogleDriveLockMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()

        let entries = try await client.list(path: "/.watermelon/locks")

        XCTAssertTrue(entries.isEmpty)
        XCTAssertTrue(server.requests.contains { request in
            guard request.url?.path == "/drive/v3/files" else { return false }
            let items = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems
            let query = items?.first(where: { $0.name == "q" })?.value
            return items?.contains(where: { $0.name == "spaces" && $0.value == "appDataFolder" }) == true
                && query?.contains("wmRepoRootID") == true
                && query?.contains("root-folder") == true
        })
    }

    func testControlDirectoryRetryConvergesPreexistingEmptyDuplicates() async throws {
        let server = GoogleDriveDirectoryMockServer()
        server.seedDuplicateWatermelonFolders()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()

        try await client.createDirectory(path: "/.watermelon")

        XCTAssertEqual(server.watermelonFolderCount, 1)
    }

    func testReleasedLockAdvancesToFreshGeneratedSlotWithoutDeletingRecord() async throws {
        let server = GoogleDriveLockMockServer()
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let firstBody = LockFileBody(
            writerID: "writer-a",
            sessionToken: "session-a",
            lockToken: "lock-a",
            generation: 1,
            writtenAt: Date(timeIntervalSince1970: 1_700_000_000),
            freshTakeoverScope: nil
        )
        let secondBody = LockFileBody(
            writerID: "writer-b",
            sessionToken: "session-b",
            lockToken: "lock-b",
            generation: 1,
            writtenAt: Date(timeIntervalSince1970: 1_700_000_100),
            freshTakeoverScope: nil
        )
        let firstURL = try temporaryLockFile(firstBody)
        let secondURL = try temporaryLockFile(secondBody)
        defer {
            try? FileManager.default.removeItem(at: firstURL)
            try? FileManager.default.removeItem(at: secondURL)
        }
        let client = makeClient()
        try await client.connect()

        try await client.upload(
            localURL: firstURL,
            remotePath: "/.watermelon/locks/writer-a.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.delete(path: "/.watermelon/locks/writer-a.lock")
        try await client.upload(
            localURL: secondURL,
            remotePath: "/.watermelon/locks/writer-b.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let bodies = server.uploadBodies
        XCTAssertEqual(bodies.count, 3)
        XCTAssertTrue(String(decoding: bodies[0], as: UTF8.self).contains("\"id\":\"slot-root\""))
        XCTAssertTrue(String(decoding: bodies[1], as: UTF8.self).contains("\"id\":\"release-1\""))
        XCTAssertTrue(String(decoding: bodies[2], as: UTF8.self).contains("\"id\":\"slot-next\""))
        XCTAssertFalse(server.requests.contains { $0.httpMethod == "DELETE" })
        XCTAssertEqual(server.requests.filter {
            $0.url?.path.hasSuffix("/files/slot-root") == true
                && URLComponents(url: $0.url!, resolvingAgainstBaseURL: false)?.queryItems?
                    .contains(where: { $0.name == "alt" && $0.value == "media" }) != true
        }.count, 1)
        XCTAssertFalse(server.requests.contains {
            $0.httpMethod == "GET" && $0.url?.path.hasSuffix("/files/release-1") == true
        })
        XCTAssertEqual(server.requests.filter {
            guard $0.httpMethod == "GET", $0.url?.path == "/drive/v3/files" else { return false }
            let query = URLComponents(url: $0.url!, resolvingAgainstBaseURL: false)?.queryItems
            return query?.contains(where: { $0.name == "spaces" && $0.value == "appDataFolder" }) == true
        }.count, 2)
        XCTAssertTrue(bodies.allSatisfy {
            String(decoding: $0, as: UTF8.self).contains("\"parents\":[\"appDataFolder\"]")
        })
        XCTAssertTrue(server.requests.filter {
            $0.url?.path.hasSuffix("/generateIds") == true
        }.allSatisfy { request in
            URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems?
                .contains(where: { $0.name == "space" && $0.value == "appDataFolder" }) == true
        })
    }

    func testMissingOldLockHistoryStillUsesLatestRecord() async throws {
        let server = GoogleDriveLockMockServer(omitReleasedHistoryAfterSuccessor: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let firstURL = try temporaryLockFile(LockFileBody(
            writerID: "writer-a",
            sessionToken: "session-a",
            lockToken: "lock-a",
            generation: 1,
            writtenAt: Date(),
            freshTakeoverScope: nil
        ))
        let secondURL = try temporaryLockFile(LockFileBody(
            writerID: "writer-b",
            sessionToken: "session-b",
            lockToken: "lock-b",
            generation: 1,
            writtenAt: Date(),
            freshTakeoverScope: nil
        ))
        defer {
            try? FileManager.default.removeItem(at: firstURL)
            try? FileManager.default.removeItem(at: secondURL)
        }
        let client = makeClient()
        try await client.connect()
        try await client.upload(
            localURL: firstURL,
            remotePath: "/.watermelon/locks/writer-a.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.delete(path: "/.watermelon/locks/writer-a.lock")
        try await client.upload(
            localURL: secondURL,
            remotePath: "/.watermelon/locks/writer-b.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        let entries = try await client.list(path: "/.watermelon/locks")

        XCTAssertEqual(entries.map(\.path), ["/.watermelon/locks/writer-b.lock"])
    }

    func testAmbiguousLockCreateRecoversByFixedSlotIDAndCanRelease() async throws {
        let server = GoogleDriveLockMockServer(failFirstUploadAfterCommit: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let body = LockFileBody(
            writerID: "writer-a",
            sessionToken: "session-a",
            lockToken: "lock-a",
            generation: 1,
            writtenAt: Date(timeIntervalSince1970: 1_700_000_000),
            freshTakeoverScope: nil
        )
        let url = try temporaryLockFile(body)
        defer { try? FileManager.default.removeItem(at: url) }
        let client = makeClient()
        try await client.connect()

        try await client.upload(
            localURL: url,
            remotePath: "/.watermelon/locks/writer-a.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.delete(path: "/.watermelon/locks/writer-a.lock")

        XCTAssertEqual(server.uploadBodies.count, 2)
    }

    func testAmbiguousLockReleaseRecoversByFixedMarkerID() async throws {
        let server = GoogleDriveLockMockServer(failReleaseAfterCommit: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let body = LockFileBody(
            writerID: "writer-a",
            sessionToken: "session-a",
            lockToken: "lock-a",
            generation: 1,
            writtenAt: Date(timeIntervalSince1970: 1_700_000_000),
            freshTakeoverScope: nil
        )
        let url = try temporaryLockFile(body)
        defer { try? FileManager.default.removeItem(at: url) }
        let client = makeClient()
        try await client.connect()

        try await client.upload(
            localURL: url,
            remotePath: "/.watermelon/locks/writer-a.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )
        try await client.delete(path: "/.watermelon/locks/writer-a.lock")

        XCTAssertEqual(server.uploadBodies.count, 2)
        XCTAssertTrue(server.requests.contains {
            $0.httpMethod == "GET"
                && $0.url?.path.hasSuffix("/files/release-1") == true
                && URLComponents(url: $0.url!, resolvingAgainstBaseURL: false)?.queryItems?
                    .contains(where: { $0.name == "alt" && $0.value == "media" }) == true
        })
    }

    func testAmbiguousLockRefreshAcceptsCommittedReplacement() async throws {
        let server = GoogleDriveLockMockServer(failFirstRefreshAfterCommit: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let original = LockFileBody(
            writerID: "writer-a",
            sessionToken: "session-a",
            lockToken: "lock-a",
            generation: 1,
            writtenAt: Date(timeIntervalSince1970: 1_700_000_000),
            freshTakeoverScope: nil
        )
        let refreshed = LockFileBody(
            writerID: original.writerID,
            sessionToken: original.sessionToken,
            lockToken: original.lockToken,
            generation: original.generation,
            writtenAt: Date(timeIntervalSince1970: 1_700_000_030),
            freshTakeoverScope: nil
        )
        let originalURL = try temporaryLockFile(original)
        let refreshedURL = try temporaryLockFile(refreshed)
        defer {
            try? FileManager.default.removeItem(at: originalURL)
            try? FileManager.default.removeItem(at: refreshedURL)
        }
        let client = makeClient()
        try await client.connect()
        try await client.upload(
            localURL: originalURL,
            remotePath: "/.watermelon/locks/writer-a.lock",
            mode: .createIfAbsent,
            respectTaskCancellation: true,
            onProgress: nil
        )

        try await client.upload(
            localURL: refreshedURL,
            remotePath: "/.watermelon/locks/writer-a.lock",
            mode: .replace,
            respectTaskCancellation: true,
            onProgress: nil
        )

        XCTAssertEqual(server.refreshCount, 1)
    }

    func testVerifyWriteAccessFailsWhenSetupLockReleaseIsNotCommitted() async throws {
        let server = GoogleDriveLockMockServer(failReleaseBeforeCommit: true)
        GoogleDriveMockURLProtocol.handler = { request in
            try server.response(for: request)
        }
        let client = makeClient()
        try await client.connect()

        do {
            try await client.verifyWriteAccess()
            XCTFail("Expected unconfirmed setup lock release to fail")
        } catch {
            guard case RemoteStorageClientError.unavailable = error else {
                return XCTFail("Unexpected error: \(error)")
            }
        }
    }

    private func makeClient(sharedState: GoogleDriveSharedState = GoogleDriveSharedState()) -> GoogleDriveClient {
        let connection = try! CanonicalGoogleDriveConnection(params: GoogleDriveConnectionParams(
            clientID: "123456789012-test.apps.googleusercontent.com",
            accountSubject: "subject",
            rootFolderID: "root-folder",
            lockRootSlotID: "slot-root",
            displayRootPath: "/Watermelon"
        ))
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [GoogleDriveMockURLProtocol.self]
        return GoogleDriveClient(
            config: GoogleDriveClient.Config(connection: connection),
            credential: GoogleDriveCredentialBlob(accountSubject: "subject", refreshToken: "refresh"),
            tokenProvider: GoogleDriveTestTokenProvider(),
            sharedState: sharedState,
            sessionConfiguration: configuration
        )
    }

    private func temporaryLockFile(_ body: LockFileBody) throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try LockFileCodec.encode(body).write(to: url)
        return url
    }

    private static func fileJSON(
        id: String,
        name: String,
        folder: Bool,
        parentID: String? = nil,
        lockRootSlotID: String = "slot-root"
    ) -> String {
        let parents = parentID.map { ",\"parents\":[\"\($0)\"]" } ?? ""
        let properties = id == "root-folder"
            ? ",\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"\(lockRootSlotID)\"}"
            : ""
        return "{\"id\":\"\(id)\",\"name\":\"\(name)\",\"mimeType\":\"\(folder ? GoogleDriveConstants.folderMIMEType : "application/octet-stream")\",\"trashed\":false\(parents)\(properties)}"
    }
}

private struct GoogleDriveTestTokenProvider: GoogleDriveAccessTokenProviding {
    func accessToken(
        for _: GoogleDriveCredentialBlob,
        clientID _: String,
        forceRefresh _: Bool
    ) async throws -> GoogleDriveAccessToken {
        GoogleDriveAccessToken(value: "test-token", expiresAt: Date().addingTimeInterval(3_600))
    }
}

private final class GoogleDriveLeasedTransferMockServer: @unchecked Sendable {
    private struct StoredFile {
        let id: String
        let name: String
        let mimeType: String
        let parentID: String
        let size: String?
        let md5Checksum: String?
    }

    private let lock = NSLock()
    private let failFirstCreateAndRecoveryAfterCommit: Bool
    private let delayFirstCreateUntilSameIDRetry: Bool
    private let failFirstUploadWinnerList: Bool
    private let failFirstDeleteAfterCommit: Bool
    private let failFirstGeneratedIDRequest: Bool
    private var files: [StoredFile] = []
    private var requests = 0
    private var uploads = 0
    private var lists = 0
    private var mediaDownloads = 0
    private var failedCreateResponse = false
    private var failedRecoveryRequest = false
    private var failedWinnerList = false
    private var failedDeleteResponse = false
    private var delayedCreate: StoredFile?
    private var didDelayFirstCreate = false
    private var uploadedIDs: [String] = []
    private var generatedIDRequests = 0

    init(
        failFirstCreateAndRecoveryAfterCommit: Bool = false,
        delayFirstCreateUntilSameIDRetry: Bool = false,
        failFirstUploadWinnerList: Bool = false,
        failFirstDeleteAfterCommit: Bool = false,
        failFirstGeneratedIDRequest: Bool = false,
        seedWatermelonFolder: Bool = false
    ) {
        self.failFirstCreateAndRecoveryAfterCommit = failFirstCreateAndRecoveryAfterCommit
        self.delayFirstCreateUntilSameIDRetry = delayFirstCreateUntilSameIDRetry
        self.failFirstUploadWinnerList = failFirstUploadWinnerList
        self.failFirstDeleteAfterCommit = failFirstDeleteAfterCommit
        self.failFirstGeneratedIDRequest = failFirstGeneratedIDRequest
        if seedWatermelonFolder {
            files = [
                StoredFile(
                    id: "watermelon-folder",
                    name: ".watermelon",
                    mimeType: GoogleDriveConstants.folderMIMEType,
                    parentID: "root-folder",
                    size: nil,
                    md5Checksum: nil
                )
            ]
        }
    }

    var uploadCount: Int { lock.withLock { uploads } }
    var totalRequestCount: Int { lock.withLock { requests } }
    var listRequestCount: Int { lock.withLock { lists } }
    var mediaDownloadCount: Int { lock.withLock { mediaDownloads } }
    var uploadItemIDs: [String] { lock.withLock { uploadedIDs } }
    var storedFileCount: Int { lock.withLock { files.count } }
    var generatedIDRequestCount: Int { lock.withLock { generatedIDRequests } }

    func removeAllFiles() {
        lock.withLock { files.removeAll() }
    }

    func response(for original: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        lock.withLock { requests += 1 }
        var request = original
        if request.httpBody == nil, let stream = request.httpBodyStream {
            request.httpBody = Self.read(stream)
        }
        let path = request.url?.path ?? ""
        let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
        if path.hasSuffix("/files/root-folder") {
            return .json(Self.root)
        }
        if path.hasSuffix("/generateIds") {
            let shouldFail = lock.withLock { () -> Bool in
                generatedIDRequests += 1
                return failFirstGeneratedIDRequest && generatedIDRequests == 1
            }
            if shouldFail { throw URLError(.networkConnectionLost) }
            let count = query.first(where: { $0.name == "count" })?.value.flatMap(Int.init) ?? 1
            let firstID: String
            if count == 1 {
                firstID = lock.withLock { "leased-folder-\(files.count + 1)" }
            } else {
                firstID = "leased-id"
            }
            return .json(googleDriveGeneratedIDsJSON(firstID: firstID, request: request))
        }
        if request.httpMethod == "POST", path == "/drive/v3/files" {
            let metadata = try Self.jsonObject(from: try XCTUnwrap(request.httpBody))
            let stored = StoredFile(
                id: try XCTUnwrap(metadata["id"] as? String),
                name: try XCTUnwrap(metadata["name"] as? String),
                mimeType: try XCTUnwrap(metadata["mimeType"] as? String),
                parentID: try XCTUnwrap((metadata["parents"] as? [String])?.first),
                size: nil,
                md5Checksum: nil
            )
            lock.withLock { files.append(stored) }
            return .json(Self.file(stored))
        }
        if request.httpMethod == "POST", path == "/upload/drive/v3/files" {
            let metadata = try Self.multipartMetadata(from: try XCTUnwrap(request.httpBody))
            let (stored, shouldFail, delayedRetry) = lock.withLock { () -> (StoredFile, Bool, Bool) in
                uploads += 1
                let stored = StoredFile(
                    id: metadata["id"] as! String,
                    name: metadata["name"] as! String,
                    mimeType: "application/octet-stream",
                    parentID: (metadata["parents"] as! [String])[0],
                    size: "5",
                    md5Checksum: "c04e34d445e31a2159c1bfeb882ba212"
                )
                uploadedIDs.append(stored.id)
                if delayFirstCreateUntilSameIDRetry, !didDelayFirstCreate {
                    didDelayFirstCreate = true
                    delayedCreate = stored
                    return (stored, true, false)
                }
                if let delayedCreate {
                    self.delayedCreate = nil
                    files.append(delayedCreate)
                    return (delayedCreate, false, delayedCreate.id == stored.id)
                }
                files.append(stored)
                let shouldFail = failFirstCreateAndRecoveryAfterCommit && !failedCreateResponse
                if shouldFail { failedCreateResponse = true }
                return (stored, shouldFail, false)
            }
            if shouldFail { throw URLError(.networkConnectionLost) }
            if delayedRetry {
                return .json(
                    "{\"error\":{\"code\":409,\"message\":\"Conflict\",\"errors\":[{\"reason\":\"conflict\"}]}}",
                    status: 409
                )
            }
            return .json(Self.file(stored))
        }
        if request.httpMethod == "PATCH", path.hasPrefix("/drive/v3/files/"),
           let id = path.split(separator: "/").last,
           let data = request.httpBody,
           let name = try Self.jsonObject(from: data)["name"] as? String {
            let moved = lock.withLock { () -> StoredFile? in
                guard let index = files.firstIndex(where: { $0.id == id }) else { return nil }
                let existing = files[index]
                let moved = StoredFile(
                    id: existing.id,
                    name: name,
                    mimeType: existing.mimeType,
                    parentID: existing.parentID,
                    size: existing.size,
                    md5Checksum: existing.md5Checksum
                )
                files[index] = moved
                return moved
            }
            guard let moved else { return .json(Self.notFound, status: 404) }
            return .json(Self.file(moved))
        }
        if request.httpMethod == "DELETE", path.hasPrefix("/drive/v3/files/"),
           let id = path.split(separator: "/").last {
            let (removed, shouldFail) = lock.withLock { () -> (Bool, Bool) in
                guard let index = files.firstIndex(where: { $0.id == id }) else {
                    return (false, false)
                }
                files.remove(at: index)
                let shouldFail = failFirstDeleteAfterCommit && !failedDeleteResponse
                if shouldFail { failedDeleteResponse = true }
                return (true, shouldFail)
            }
            if shouldFail { throw URLError(.networkConnectionLost) }
            return removed ? .data(Data(), status: 204) : .json(Self.notFound, status: 404)
        }
        if path == "/drive/v3/files" {
            lock.withLock { lists += 1 }
            let q = query.first(where: { $0.name == "q" })?.value ?? ""
            let shouldFail = lock.withLock { () -> Bool in
                guard failFirstUploadWinnerList,
                      uploads > 0,
                      q.contains("name = 'asset.bin'"),
                      !failedWinnerList else { return false }
                failedWinnerList = true
                return true
            }
            if shouldFail { throw URLError(.networkConnectionLost) }
            let visible = lock.withLock {
                files.filter { file in
                    guard q.contains("'\(file.parentID)' in parents") else { return false }
                    if q.contains("name = 'asset.bin'") { return file.name == "asset.bin" }
                    if q.contains("name = 'asset-2.bin'") { return file.name == "asset-2.bin" }
                    if q.contains("name = '2024'") { return file.name == "2024" }
                    if q.contains("name = '03'") { return file.name == "03" }
                    return true
                }
            }
            return .json("{\"files\":[\(visible.map(Self.file).joined(separator: ","))]}")
        }
        if request.httpMethod == "GET",
           query.contains(where: { $0.name == "alt" && $0.value == "media" }),
           let id = path.split(separator: "/").last,
           lock.withLock({ files.contains(where: { $0.id == id }) }) {
            lock.withLock { mediaDownloads += 1 }
            return .data(Data("asset".utf8))
        }
        if request.httpMethod == "GET", let id = path.split(separator: "/").last {
            let shouldFail = lock.withLock { () -> Bool in
                guard failFirstCreateAndRecoveryAfterCommit,
                      failedCreateResponse,
                      !failedRecoveryRequest else { return false }
                failedRecoveryRequest = true
                return true
            }
            if shouldFail { throw URLError(.networkConnectionLost) }
            if let stored = lock.withLock({ files.first(where: { $0.id == id }) }) {
                return .json(Self.file(stored))
            }
            return .json(Self.notFound, status: 404)
        }
        throw URLError(.badServerResponse)
    }

    private static func file(_ file: StoredFile) -> String {
        let size = file.size.map { ",\"size\":\"\($0)\"" } ?? ""
        let md5 = file.md5Checksum.map { ",\"md5Checksum\":\"\($0)\"" } ?? ""
        return "{\"id\":\"\(file.id)\",\"name\":\"\(file.name)\",\"mimeType\":\"\(file.mimeType)\"\(size)\(md5),\"parents\":[\"\(file.parentID)\"],\"trashed\":false}"
    }

    private static let notFound = "{\"error\":{\"code\":404,\"message\":\"not found\",\"errors\":[{\"reason\":\"notFound\"}]}}"

    private static func multipartMetadata(from data: Data) throws -> [String: Any] {
        let body = String(decoding: data, as: UTF8.self)
        guard let start = body.firstIndex(of: "{"),
              let end = body[start...].firstIndex(of: "}") else {
            throw URLError(.cannotParseResponse)
        }
        return try jsonObject(from: Data(body[start...end].utf8))
    }

    private static func jsonObject(from data: Data) throws -> [String: Any] {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw URLError(.cannotParseResponse)
        }
        return object
    }

    private static func read(_ stream: InputStream) -> Data {
        stream.open()
        defer { stream.close() }
        var result = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            guard count > 0 else { break }
            result.append(buffer, count: count)
        }
        return result
    }

    private static let root = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"slot-root\"}}"
}

private final class GoogleDriveOrdinaryCreateState: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    var creationCount: Int { lock.withLock { count } }

    func markCreated() -> Int {
        lock.withLock {
            count += 1
            return count
        }
    }
}

private final class GoogleDriveDuplicateMutationState: @unchecked Sendable {
    private let lock = NSLock()
    private var duplicate = false

    var hasDuplicate: Bool { lock.withLock { duplicate } }

    func addDuplicate() {
        lock.withLock { duplicate = true }
    }
}

private final class GoogleDriveParentMutationState: @unchecked Sendable {
    private let lock = NSLock()
    private var moved = false

    var isMoved: Bool { lock.withLock { moved } }

    func move() {
        lock.withLock { moved = true }
    }
}

private final class GoogleDriveAncestorReplacementState: @unchecked Sendable {
    private let lock = NSLock()
    private var replaced = false

    var isReplaced: Bool { lock.withLock { replaced } }

    func replace() {
        lock.withLock { replaced = true }
    }
}

private final class GoogleDriveUploadMutationState: @unchecked Sendable {
    private let lock = NSLock()
    private var duplicate = false

    var hasDuplicate: Bool { lock.withLock { duplicate } }

    func addDuplicate() {
        lock.withLock { duplicate = true }
    }
}

private final class GoogleDriveAmbiguousMoveMockServer: @unchecked Sendable {
    private let lock = NSLock()
    private var moved = false

    func response(for request: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        let path = request.url?.path ?? ""
        if path.hasSuffix("/files/root-folder") { return .json(Self.root) }
        if path.hasSuffix("/files/source-id") {
            if request.httpMethod == "PATCH" {
                lock.withLock { moved = true }
                throw URLError(.networkConnectionLost)
            }
            return .json(lock.withLock { moved } ? Self.final : Self.temp)
        }
        if path == "/drive/v3/files" {
            let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
            let q = query.first(where: { $0.name == "q" })?.value ?? ""
            let isMoved = lock.withLock { moved }
            if q.contains("name = 'temp.json'") {
                return .json(isMoved ? "{\"files\":[]}" : "{\"files\":[\(Self.temp)]}")
            }
            if q.contains("name = 'final.json'") {
                return .json(isMoved ? "{\"files\":[\(Self.final)]}" : "{\"files\":[]}")
            }
        }
        throw URLError(.badServerResponse)
    }

    private static let root = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"slot-root\"}}"
    private static let temp = "{\"id\":\"source-id\",\"name\":\"temp.json\",\"mimeType\":\"application/json\",\"parents\":[\"root-folder\"],\"trashed\":false}"
    private static let final = "{\"id\":\"source-id\",\"name\":\"final.json\",\"mimeType\":\"application/json\",\"parents\":[\"root-folder\"],\"trashed\":false}"
}

private final class GoogleDriveResumableMockServer: @unchecked Sendable {
    private let lock = NSLock()
    private var ranges: [String] = []
    private var completed = false

    var contentRanges: [String] { lock.withLock { ranges } }

    func response(for request: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        let path = request.url?.path ?? ""
        let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
        if path.hasSuffix("/files/root-folder") {
            return .json(Self.root)
        }
        if path.hasSuffix("/generateIds") {
            return .json(googleDriveGeneratedIDsJSON(firstID: "large-id", request: request))
        }
        if request.httpMethod == "POST", path == "/upload/drive/v3/files" {
            return GoogleDriveMockURLProtocol.Response(
                data: Data("{}".utf8),
                status: 200,
                headers: ["Location": "https://upload.example/resumable/session"]
            )
        }
        if request.httpMethod == "PUT", request.url?.host == "upload.example" {
            let range = request.value(forHTTPHeaderField: "Content-Range") ?? ""
            lock.withLock { ranges.append(range) }
            if range.hasPrefix("bytes 0-") {
                return GoogleDriveMockURLProtocol.Response(
                    data: Data(),
                    status: 308,
                    headers: ["Range": "bytes=0-8388607"]
                )
            }
            lock.withLock { completed = true }
            return .json(Self.largeFile)
        }
        if path == "/drive/v3/files" {
            let q = query.first(where: { $0.name == "q" })?.value ?? ""
            if q.contains("name = 'large.mov'") {
                return lock.withLock { completed }
                    ? .json("{\"files\":[\(Self.largeFile)]}")
                    : .json("{\"files\":[]}")
            }
            return .json("{\"files\":[]}")
        }
        throw URLError(.badServerResponse)
    }

    private static let root = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"slot-root\"}}"
    private static let largeFile = "{\"id\":\"large-id\",\"name\":\"large.mov\",\"mimeType\":\"application/octet-stream\",\"size\":\"9437184\",\"parents\":[\"root-folder\"],\"trashed\":false}"
}

private final class GoogleDriveStalledResumableMockServer: @unchecked Sendable {
    private let lock = NSLock()
    private let chunkError: Error
    private var chunkAttemptStorage = 0
    private var statusQueryStorage = 0

    init(chunkError: Error) {
        self.chunkError = chunkError
    }

    var chunkAttempts: Int { lock.withLock { chunkAttemptStorage } }
    var statusQueries: Int { lock.withLock { statusQueryStorage } }

    func response(for request: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        let path = request.url?.path ?? ""
        if path.hasSuffix("/files/root-folder") { return .json(Self.root) }
        if path.hasSuffix("/generateIds") {
            return .json(googleDriveGeneratedIDsJSON(firstID: "stalled-id", request: request))
        }
        if request.httpMethod == "POST", path == "/upload/drive/v3/files" {
            return GoogleDriveMockURLProtocol.Response(
                data: Data("{}".utf8),
                status: 200,
                headers: ["Location": "https://upload.example/resumable/stalled"]
            )
        }
        if request.httpMethod == "PUT", request.url?.host == "upload.example" {
            let range = request.value(forHTTPHeaderField: "Content-Range") ?? ""
            if range.hasPrefix("bytes */") {
                lock.withLock { statusQueryStorage += 1 }
                return GoogleDriveMockURLProtocol.Response(data: Data(), status: 308, headers: [:])
            }
            lock.withLock { chunkAttemptStorage += 1 }
            throw chunkError
        }
        if path == "/drive/v3/files" { return .json("{\"files\":[]}") }
        throw URLError(.badServerResponse)
    }

    private static let root = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"slot-root\"}}"
}

private final class GoogleDriveDirectoryMockServer: @unchecked Sendable {
    private struct Folder {
        let id: String
        let name: String
        let parentID: String
        let createdTime: String
    }

    private let lock = NSLock()
    private let controlFolderVisibilityDelay: Int
    private var generatedID = 0
    private var folders: [Folder] = []
    private var hiddenControlListsByName: [String: Int] = [:]

    init(controlFolderVisibilityDelay: Int = 0) {
        self.controlFolderVisibilityDelay = controlFolderVisibilityDelay
    }

    var yearFolderCount: Int { lock.withLock { folders.filter { $0.name == "2024" }.count } }
    var watermelonFolderCount: Int { lock.withLock { folders.filter { $0.name == ".watermelon" }.count } }
    var locksFolderCount: Int { lock.withLock { folders.filter { $0.name == "locks" }.count } }
    var monthNames: [String] {
        lock.withLock { folders.filter { $0.name == "01" || $0.name == "02" }.map(\.name).sorted() }
    }

    func seedDuplicateWatermelonFolders() {
        lock.withLock {
            folders = [
                Folder(
                    id: "watermelon-oldest",
                    name: ".watermelon",
                    parentID: "root-folder",
                    createdTime: "2026-08-15T00:00:00Z"
                ),
                Folder(
                    id: "watermelon-loser",
                    name: ".watermelon",
                    parentID: "root-folder",
                    createdTime: "2026-08-15T00:00:01Z"
                )
            ]
        }
    }

    func response(for original: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        var request = original
        if request.httpBody == nil, let stream = request.httpBodyStream {
            request.httpBody = Self.read(stream)
        }
        let path = request.url?.path ?? ""
        if path.hasSuffix("/files/root-folder") { return .json(Self.root) }
        if path.hasSuffix("/generateIds") {
            let firstID = lock.withLock { () -> String in
                generatedID += 1
                return "folder-\(generatedID)"
            }
            return .json(googleDriveGeneratedIDsJSON(firstID: firstID, request: request))
        }
        if request.httpMethod == "POST", path == "/drive/v3/files" {
            let data = try XCTUnwrap(request.httpBody)
            let body = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
            let id = try XCTUnwrap(body["id"] as? String)
            let name = try XCTUnwrap(body["name"] as? String)
            let parents = try XCTUnwrap(body["parents"] as? [String])
            let parentID = try XCTUnwrap(parents.first)
            let result = lock.withLock { () -> Folder in
                let value = Folder(
                    id: id,
                    name: name,
                    parentID: parentID,
                    createdTime: "2026-08-15T00:00:0\(folders.count)Z"
                )
                folders.append(value)
                if name == ".watermelon" || name == "locks" {
                    hiddenControlListsByName[name] = controlFolderVisibilityDelay
                }
                return value
            }
            return .json(Self.json(result))
        }
        if request.httpMethod == "DELETE", path.hasPrefix("/drive/v3/files/") {
            let id = String(path.split(separator: "/").last ?? "")
            let removed = lock.withLock { () -> Bool in
                guard let index = folders.firstIndex(where: { $0.id == id }) else { return false }
                folders.remove(at: index)
                return true
            }
            return removed
                ? GoogleDriveMockURLProtocol.Response(data: Data(), status: 204, headers: [:])
                : .json("{\"error\":{\"code\":404,\"message\":\"Not found\",\"errors\":[{\"reason\":\"notFound\"}]}}", status: 404)
        }
        if path == "/drive/v3/files" {
            let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
            let q = query.first(where: { $0.name == "q" })?.value ?? ""
            let parentID = Self.quotedValues(in: q).first ?? ""
            let name = Self.name(in: q)
            let matches = lock.withLock { () -> [Folder] in
                if let name,
                   let remaining = hiddenControlListsByName[name],
                   remaining > 0 {
                    hiddenControlListsByName[name] = remaining - 1
                    return []
                }
                return folders.filter { folder in
                    folder.parentID == parentID && (name == nil || folder.name == name)
                }
            }
            if let name, ["2024", ".watermelon", "locks"].contains(name), matches.isEmpty {
                Thread.sleep(forTimeInterval: 0.05)
            }
            return .json("{\"files\":[\(matches.map(Self.json).joined(separator: ","))]}")
        }
        if request.httpMethod == "GET", let id = path.split(separator: "/").last {
            if let folder = lock.withLock({ folders.first { $0.id == id } }) {
                return .json(Self.json(folder))
            }
            return .json(
                "{\"error\":{\"code\":404,\"message\":\"Not found\",\"errors\":[{\"reason\":\"notFound\"}]}}",
                status: 404
            )
        }
        throw URLError(.badServerResponse)
    }

    private static func quotedValues(in query: String) -> [String] {
        var values: [String] = []
        var remainder = query[...]
        while let opening = remainder.firstIndex(of: "'") {
            let content = remainder.index(after: opening)...
            guard let closing = query[content].firstIndex(of: "'") else { break }
            values.append(String(query[content.lowerBound ..< closing]))
            remainder = query[query.index(after: closing)...]
        }
        return values
    }

    private static func name(in query: String) -> String? {
        guard let range = query.range(of: "name = '") else { return nil }
        let suffix = query[range.upperBound...]
        return suffix.split(separator: "'").first.map(String.init)
    }

    private static func json(_ folder: Folder) -> String {
        "{\"id\":\"\(folder.id)\",\"name\":\"\(folder.name)\",\"mimeType\":\"application/vnd.google-apps.folder\",\"createdTime\":\"\(folder.createdTime)\",\"parents\":[\"\(folder.parentID)\"],\"trashed\":false}"
    }

    private static func read(_ stream: InputStream) -> Data {
        stream.open()
        defer { stream.close() }
        var result = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            guard count > 0 else { break }
            result.append(buffer, count: count)
        }
        return result
    }

    private static let root = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"slot-root\"}}"
}

private final class GoogleDriveCopyMockServer: @unchecked Sendable {
    private let lock = NSLock()
    private var copied = false
    private var copyRequests = 0
    private var destinationIDStorage: [String] = []

    var copyCount: Int { lock.withLock { copyRequests } }
    var destinationIDs: [String] { lock.withLock { destinationIDStorage } }

    func response(for original: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        var request = original
        if request.httpBody == nil, let stream = request.httpBodyStream {
            request.httpBody = Self.read(stream)
        }
        let path = request.url?.path ?? ""
        let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
        if path.hasSuffix("/files/root-folder") { return .json(Self.root) }
        if path.hasSuffix("/generateIds") {
            return .json(googleDriveGeneratedIDsJSON(firstID: "copy-id", request: request))
        }
        if request.httpMethod == "POST", path.hasSuffix("/files/source-id/copy") {
            let body = try XCTUnwrap(request.httpBody)
            let metadata = try XCTUnwrap(JSONSerialization.jsonObject(with: body) as? [String: Any])
            let id = try XCTUnwrap(metadata["id"] as? String)
            let name = try XCTUnwrap(metadata["name"] as? String)
            let parentID = try XCTUnwrap((metadata["parents"] as? [String])?.first)
            guard name == "2024-01.sqlite", parentID == "root-folder" else {
                throw URLError(.badServerResponse)
            }
            lock.withLock {
                copyRequests += 1
                destinationIDStorage.append(id)
                copied = true
            }
            throw URLError(.networkConnectionLost)
        }
        if request.httpMethod == "GET", path.hasSuffix("/files/copy-id") {
            return lock.withLock { copied }
                ? .json(Self.destination)
                : .json(Self.notFound, status: 404)
        }
        if path == "/drive/v3/files" {
            let q = query.first(where: { $0.name == "q" })?.value ?? ""
            if q.contains("name = 'scratch.sqlite'") {
                return .json("{\"files\":[\(Self.source)]}")
            }
            if q.contains("name = '2024-01.sqlite'"), lock.withLock({ copied }) {
                return .json("{\"files\":[\(Self.destination)]}")
            }
            return .json("{\"files\":[]}")
        }
        throw URLError(.badServerResponse)
    }

    private static func read(_ stream: InputStream) -> Data {
        stream.open()
        defer { stream.close() }
        var result = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            guard count > 0 else { break }
            result.append(buffer, count: count)
        }
        return result
    }

    private static let root = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"slot-root\"}}"
    private static let source = "{\"id\":\"source-id\",\"name\":\"scratch.sqlite\",\"mimeType\":\"application/octet-stream\",\"parents\":[\"root-folder\"],\"trashed\":false,\"size\":\"5\",\"md5Checksum\":\"c04e34d445e31a2159c1bfeb882ba212\"}"
    private static let destination = "{\"id\":\"copy-id\",\"name\":\"2024-01.sqlite\",\"mimeType\":\"application/octet-stream\",\"parents\":[\"root-folder\"],\"trashed\":false,\"size\":\"5\",\"md5Checksum\":\"c04e34d445e31a2159c1bfeb882ba212\"}"
    private static let notFound = "{\"error\":{\"code\":404,\"message\":\"Not found\",\"errors\":[{\"reason\":\"notFound\"}]}}"
}

private final class GoogleDriveRootBootstrapMockServer: @unchecked Sendable {
    private let lock = NSLock()
    private var lists = 0
    private var mutations = 0

    var listCount: Int { lock.withLock { lists } }
    var mutationCount: Int { lock.withLock { mutations } }

    func response(for request: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        let path = request.url?.path ?? ""
        let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
        if request.httpMethod == "GET", path == "/drive/v3/files" {
            if query.contains(where: { $0.name == "spaces" && $0.value == "appDataFolder" }) {
                return .json("{\"files\":[\(Self.lockRecord)]}")
            }
            lock.withLock { lists += 1 }
            if query.contains(where: { $0.name == "pageToken" && $0.value == "page-2" }) {
                return .json("{\"files\":[\(Self.root)]}")
            }
            return .json("{\"nextPageToken\":\"page-2\",\"files\":[]}")
        }
        if request.httpMethod == "POST" || request.httpMethod == "DELETE" {
            lock.withLock { mutations += 1 }
        }
        throw URLError(.badServerResponse)
    }

    private static let root = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"createdTime\":\"2026-08-15T00:00:00Z\",\"parents\":[\"actual-my-drive-root-id\"],\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"slot-root\"}}"
    private static let lockRecord = "{\"id\":\"slot-root\",\"name\":\".gdrive-lock-record-1\",\"mimeType\":\"application/json\",\"appProperties\":{\"wmRole\":\"watermelonLockRecord\",\"wmRepoRootID\":\"root-folder\"}}"
}

private final class GoogleDriveClearedAppDataRootMockServer: @unchecked Sendable {
    private let lock = NSLock()
    private var rootCreates = 0
    private var rootUpdates = 0

    var rootCreateCount: Int { lock.withLock { rootCreates } }
    var rootUpdateCount: Int { lock.withLock { rootUpdates } }

    func response(for original: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        var request = original
        if request.httpBody == nil, let stream = request.httpBodyStream {
            request.httpBody = Self.read(stream)
        }
        let path = request.url?.path ?? ""
        let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
        if request.httpMethod == "GET", path == "/drive/v3/files" {
            let appData = query.contains { $0.name == "spaces" && $0.value == "appDataFolder" }
            return appData ? .json("{\"files\":[]}") : .json("{\"files\":[\(Self.oldRoot)]}")
        }
        if request.httpMethod == "GET", path.hasSuffix("/generateIds") {
            XCTAssertEqual(query.first(where: { $0.name == "space" })?.value, "appDataFolder")
            return .json("{\"ids\":[\"fresh-slot\"]}")
        }
        if request.httpMethod == "PATCH", path == "/drive/v3/files/root-folder" {
            let body = try XCTUnwrap(request.httpBody)
            let object = try XCTUnwrap(JSONSerialization.jsonObject(with: body) as? [String: Any])
            let properties = try XCTUnwrap(object["appProperties"] as? [String: String])
            XCTAssertEqual(properties[GoogleDriveConstants.rootRoleKey], GoogleDriveConstants.rootRole)
            XCTAssertEqual(properties[GoogleDriveConstants.rootSchemaKey], GoogleDriveConstants.rootSchemaVersion)
            XCTAssertEqual(properties[GoogleDriveConstants.lockRootSlotKey], "fresh-slot")
            lock.withLock { rootUpdates += 1 }
            return .json(Self.updatedRoot)
        }
        if request.httpMethod == "POST", path == "/drive/v3/files" {
            lock.withLock { rootCreates += 1 }
        }
        throw URLError(.badServerResponse)
    }

    private static func read(_ stream: InputStream) -> Data {
        stream.open()
        defer { stream.close() }
        var result = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            guard count > 0 else { break }
            result.append(buffer, count: count)
        }
        return result
    }

    private static let oldRoot = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"parents\":[\"actual-my-drive-root-id\"],\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"old-slot\"}}"
    private static let updatedRoot = "{\"id\":\"root-folder\",\"name\":\"Watermelon\",\"mimeType\":\"application/vnd.google-apps.folder\",\"parents\":[\"actual-my-drive-root-id\"],\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"fresh-slot\"}}"
}

private final class GoogleDriveEventuallyVisibleRootMockServer: @unchecked Sendable {
    private let lock = NSLock()
    private let hiddenSearchesAfterCreate: Int
    private var created = false
    private var postCreateSearches = 0
    private var rootIDStorage: [String] = []

    init(hiddenSearchesAfterCreate: Int) {
        self.hiddenSearchesAfterCreate = hiddenSearchesAfterCreate
    }

    var createdRootIDs: [String] { lock.withLock { rootIDStorage } }
    var postCreateSearchCount: Int { lock.withLock { postCreateSearches } }

    func response(for original: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        var request = original
        if request.httpBody == nil, let stream = request.httpBodyStream {
            request.httpBody = Self.read(stream)
        }
        let path = request.url?.path ?? ""
        if request.httpMethod == "GET", path == "/drive/v3/files" {
            let visible = lock.withLock { () -> Bool in
                guard created else { return false }
                postCreateSearches += 1
                return postCreateSearches > hiddenSearchesAfterCreate
            }
            return visible ? .json("{\"files\":[\(Self.root)]}") : .json("{\"files\":[]}")
        }
        if path.hasSuffix("/generateIds") {
            let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems ?? []
            let space = query.first(where: { $0.name == "space" })?.value
            return space == "appDataFolder"
                ? .json("{\"ids\":[\"created-slot\"]}")
                : .json("{\"ids\":[\"created-root\"]}")
        }
        if request.httpMethod == "POST", path == "/drive/v3/files" {
            let body = try XCTUnwrap(request.httpBody)
            let object = try XCTUnwrap(JSONSerialization.jsonObject(with: body) as? [String: Any])
            let id = try XCTUnwrap(object["id"] as? String)
            XCTAssertEqual(object["name"] as? String, "Watermelon Backup")
            lock.withLock {
                rootIDStorage.append(id)
                created = true
            }
            return .json(Self.root)
        }
        throw URLError(.badServerResponse)
    }

    private static func read(_ stream: InputStream) -> Data {
        stream.open()
        defer { stream.close() }
        var result = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            guard count > 0 else { break }
            result.append(buffer, count: count)
        }
        return result
    }

    private static let root = "{\"id\":\"created-root\",\"name\":\"Watermelon Backup\",\"mimeType\":\"application/vnd.google-apps.folder\",\"createdTime\":\"2026-08-15T00:00:00Z\",\"parents\":[\"actual-my-drive-root-id\"],\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"created-slot\"}}"
}

private final class GoogleDriveLockMockServer: @unchecked Sendable {
    private let lock = NSLock()
    private var uploadCount = 0
    private var generatedCount = 0
    private var releaseCreated = false
    private var requestStorage: [URLRequest] = []
    private var uploadBodyStorage: [Data] = []
    private var firstLockBody: Data
    private let secondLockBody: Data
    private let failFirstUploadAfterCommit: Bool
    private let failFirstRefreshAfterCommit: Bool
    private let failReleaseBeforeCommit: Bool
    private let failReleaseAfterCommit: Bool
    private let omitReleasedHistoryAfterSuccessor: Bool
    private var failedRefreshResponse = false

    init(
        failFirstUploadAfterCommit: Bool = false,
        failFirstRefreshAfterCommit: Bool = false,
        failReleaseBeforeCommit: Bool = false,
        failReleaseAfterCommit: Bool = false,
        omitReleasedHistoryAfterSuccessor: Bool = false
    ) {
        self.failFirstUploadAfterCommit = failFirstUploadAfterCommit
        self.failFirstRefreshAfterCommit = failFirstRefreshAfterCommit
        self.failReleaseBeforeCommit = failReleaseBeforeCommit
        self.failReleaseAfterCommit = failReleaseAfterCommit
        self.omitReleasedHistoryAfterSuccessor = omitReleasedHistoryAfterSuccessor
        firstLockBody = try! GoogleDriveJSON.encode(GoogleDriveLockRecord(
            sequence: 1,
            virtualPath: "/.watermelon/locks/writer-a.lock",
            lockBody: LockFileCodec.encode(LockFileBody(
                writerID: "writer-a",
                sessionToken: "session-a",
                lockToken: "lock-a",
                generation: 1,
                writtenAt: Date(timeIntervalSince1970: 1_700_000_000),
                freshTakeoverScope: nil
            )),
            nextSlotID: "slot-next",
            releaseMarkerID: "release-1"
        ))
        secondLockBody = try! GoogleDriveJSON.encode(GoogleDriveLockRecord(
            sequence: 2,
            virtualPath: "/.watermelon/locks/writer-b.lock",
            lockBody: LockFileCodec.encode(LockFileBody(
                writerID: "writer-b",
                sessionToken: "session-b",
                lockToken: "lock-b",
                generation: 1,
                writtenAt: Date(timeIntervalSince1970: 1_700_000_100),
                freshTakeoverScope: nil
            )),
            nextSlotID: "slot-third",
            releaseMarkerID: "release-2"
        ))
    }

    var requests: [URLRequest] { lock.withLock { requestStorage } }
    var uploadBodies: [Data] { lock.withLock { uploadBodyStorage } }
    var refreshCount: Int {
        lock.withLock {
            requestStorage.filter {
                $0.httpMethod == "PATCH"
                    && $0.url?.path.hasSuffix("/upload/drive/v3/files/slot-root") == true
            }.count
        }
    }

    func response(for original: URLRequest) throws -> GoogleDriveMockURLProtocol.Response {
        var request = original
        if request.httpBody == nil, let stream = request.httpBodyStream {
            request.httpBody = Self.read(stream: stream)
        }
        return try lock.withLock {
            requestStorage.append(request)
            let path = request.url?.path ?? ""
            let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems
            if request.httpMethod == "PATCH", path.hasSuffix("/upload/drive/v3/files/slot-root") {
                let uploadBody = request.httpBody ?? Data()
                uploadBodyStorage.append(uploadBody)
                if let recordBody = Self.multipartContent(from: uploadBody) {
                    firstLockBody = recordBody
                }
                if failFirstRefreshAfterCommit, !failedRefreshResponse {
                    failedRefreshResponse = true
                    throw URLError(.networkConnectionLost)
                }
                return .json("{\"id\":\"slot-root\"}")
            }
            if request.httpMethod == "POST", path == "/upload/drive/v3/files" {
                uploadCount += 1
                let uploadBody = request.httpBody ?? Data()
                uploadBodyStorage.append(uploadBody)
                if uploadCount == 1, let recordBody = Self.multipartContent(from: uploadBody) {
                    firstLockBody = recordBody
                }
                if uploadCount == 1, failFirstUploadAfterCommit {
                    throw URLError(.networkConnectionLost)
                }
                if uploadCount == 2, failReleaseBeforeCommit {
                    throw URLError(.networkConnectionLost)
                }
                if uploadCount == 2 { releaseCreated = true }
                if uploadCount == 2, failReleaseAfterCommit {
                    throw URLError(.networkConnectionLost)
                }
                let id = uploadCount == 1 ? "slot-root" : (uploadCount == 2 ? "release-1" : "slot-next")
                return .json("{\"id\":\"\(id)\"}")
            }
            if path.hasSuffix("/generateIds") {
                generatedCount += 1
                return generatedCount == 1
                    ? .json("{\"ids\":[\"slot-next\",\"release-1\"]}")
                    : .json("{\"ids\":[\"slot-third\",\"release-2\"]}")
            }
            if path.hasSuffix("/files/root-folder") {
                return .json(Self.file(id: "root-folder", name: "Watermelon", folder: true))
            }
            if path.hasSuffix("/files/slot-root") {
                if query?.contains(where: { $0.name == "alt" && $0.value == "media" }) == true {
                    return .data(firstLockBody)
                }
                if uploadCount == 0 { return .json(Self.notFound, status: 404) }
                return .json(Self.lockFile(id: "slot-root", sequence: 1))
            }
            if path.hasSuffix("/files/release-1") {
                guard releaseCreated else { return .json(Self.notFound, status: 404) }
                if query?.contains(where: { $0.name == "alt" && $0.value == "media" }) == true {
                    return .json(Self.releaseBody)
                }
                return .json(Self.releaseFile)
            }
            if path.hasSuffix("/files/slot-next") {
                if query?.contains(where: { $0.name == "alt" && $0.value == "media" }) == true {
                    return .data(secondLockBody)
                }
                if uploadCount < 3 { return .json(Self.notFound, status: 404) }
                return .json(Self.lockFile(id: "slot-next", sequence: 2))
            }
            if path == "/drive/v3/files" {
                let q = query?.first(where: { $0.name == "q" })?.value ?? ""
                let spaces = query?.first(where: { $0.name == "spaces" })?.value
                if spaces == "appDataFolder", q.contains("wmRepoRootID") {
                    var files: [String] = []
                    let omitHistory = omitReleasedHistoryAfterSuccessor && uploadCount >= 3
                    if uploadCount >= 1, !omitHistory { files.append(Self.lockFile(id: "slot-root", sequence: 1)) }
                    if releaseCreated, !omitHistory { files.append(Self.releaseFile) }
                    if uploadCount >= 3 { files.append(Self.lockFile(id: "slot-next", sequence: 2)) }
                    return .json("{\"files\":[\(files.joined(separator: ","))]}")
                }
                return .json("{\"files\":[]}")
            }
            throw URLError(.badServerResponse)
        }
    }

    private static let notFound = "{\"error\":{\"code\":404,\"message\":\"Not found\",\"errors\":[{\"reason\":\"notFound\"}]}}"

    private static func file(id: String, name: String, folder: Bool) -> String {
        let properties = id == "root-folder"
            ? ",\"appProperties\":{\"wmRole\":\"watermelonRoot\",\"wmSchema\":\"1\",\"wmLockRootSlot\":\"slot-root\"}"
            : ""
        return "{\"id\":\"\(id)\",\"name\":\"\(name)\",\"mimeType\":\"\(folder ? GoogleDriveConstants.folderMIMEType : "application/octet-stream")\",\"trashed\":false\(properties)}"
    }

    private static func lockFile(id: String, sequence: Int) -> String {
        let next = sequence == 1 ? "slot-next" : "slot-third"
        let release = sequence == 1 ? "release-1" : "release-2"
        return "{\"id\":\"\(id)\",\"name\":\".gdrive-lock-record-\(sequence)\",\"mimeType\":\"application/json\",\"trashed\":false,\"appProperties\":{\"wmRole\":\"watermelonLockRecord\",\"wmLockSequence\":\"\(sequence)\",\"wmLockNextSlot\":\"\(next)\",\"wmLockReleaseMarker\":\"\(release)\",\"wmRepoRootID\":\"root-folder\"}}"
    }

    private static let releaseFile = "{\"id\":\"release-1\",\"appProperties\":{\"wmRole\":\"watermelonLockRelease\",\"wmLockSequence\":\"1\",\"wmLockRecordID\":\"slot-root\",\"wmRepoRootID\":\"root-folder\"}}"
    private static let releaseBody = "{\"schemaVersion\":1,\"recordID\":\"slot-root\",\"sequence\":1}"

    private static func read(stream: InputStream) -> Data {
        stream.open()
        defer { stream.close() }
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            guard count > 0 else { break }
            data.append(buffer, count: count)
        }
        return data
    }

    private static func multipartContent(from data: Data) -> Data? {
        let marker = Data("\r\nContent-Type: application/json\r\n\r\n".utf8)
        guard let contentHeader = data.range(of: marker) else {
            return nil
        }
        let start = contentHeader.upperBound
        guard let end = data.range(of: Data("\r\n--watermelon-".utf8), in: start ..< data.endIndex)?.lowerBound else {
            return nil
        }
        return data[start ..< end]
    }
}

private final class GoogleDriveRequestRecorder: @unchecked Sendable {
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

private func googleDriveGeneratedIDsJSON(firstID: String, request: URLRequest) -> String {
    let countText = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems?
        .first(where: { $0.name == "count" })?.value
    let count = max(1, Int(countText ?? "1") ?? 1)
    let ids = [firstID] + (1 ..< count).map { "\(firstID)-\($0)" }
    return "{\"ids\":[\(ids.map { "\"\($0)\"" }.joined(separator: ","))]}"
}

private final class GoogleDriveMockURLProtocol: URLProtocol {
    struct Response {
        let data: Data
        let status: Int
        let headers: [String: String]

        static func json(_ value: String, status: Int = 200) -> Response {
            Response(data: Data(value.utf8), status: status, headers: ["Content-Type": "application/json"])
        }

        static func data(_ value: Data, status: Int = 200) -> Response {
            Response(data: value, status: status, headers: ["Content-Type": "application/octet-stream"])
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
            let response = try XCTUnwrap(HTTPURLResponse(
                url: url,
                statusCode: result.status,
                httpVersion: "HTTP/1.1",
                headerFields: result.headers
            ))
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: result.data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

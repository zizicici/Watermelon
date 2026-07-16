import Foundation

final actor OneDriveClient: RemoteStorageClientProtocol, OneDriveUploadCollisionPolicyClient, OneDriveManifestItemIDClient {
    struct Config: Sendable {
        let connection: CanonicalOneDriveConnection
    }

    private struct Destination {
        let parent: OneDriveDriveItem
        let parentPath: String
        let name: String
    }

    private struct UploadDestination {
        let path: String
        let parentPath: String
        let name: String
    }

    nonisolated private static let directUploadThreshold: Int64 = 10 * 1024 * 1024
    nonisolated private static let uploadFragmentSize: Int64 = 10 * 1024 * 1024
    nonisolated private static let copyPollLimit = 1_440
    nonisolated private static let acceptedVerificationLimit = 8
    nonisolated private static let uploadRecoveryLimit = 5
    nonisolated private static let driveItemSelect = [
        "id",
        "name",
        "size",
        "eTag",
        "cTag",
        "createdDateTime",
        "lastModifiedDateTime",
        "folder",
        "file",
        "parentReference",
        "fileSystemInfo"
    ].joined(separator: ",")
    private let config: Config
    private let credential: OneDriveCredentialBlob
    private let tokenProvider: any OneDriveAccessTokenProviding
    private let sharedState: OneDriveSharedState
    private let itemNamespace: OneDriveItemIndex.Namespace
    nonisolated private let transport: OneDriveGraphTransport

    init(
        config: Config,
        credential: OneDriveCredentialBlob,
        tokenProvider: any OneDriveAccessTokenProviding,
        sharedState: OneDriveSharedState,
        sessionConfiguration: URLSessionConfiguration? = nil,
        stallTimeouts: URLSessionStallWatchdog.Timeouts? = nil
    ) {
        self.config = config
        self.credential = credential
        self.tokenProvider = tokenProvider
        self.sharedState = sharedState
        itemNamespace = OneDriveItemIndex.Namespace(
            cloudEnvironment: config.connection.cloudEnvironment.rawValue,
            driveID: config.connection.driveID,
            rootItemID: config.connection.rootItemID
        )
        transport = OneDriveGraphTransport(
            credential: credential,
            tokenProvider: tokenProvider,
            sharedState: sharedState,
            graphBaseURL: config.connection.cloudEnvironment.graphBaseURL,
            sessionConfiguration: sessionConfiguration,
            stallTimeouts: stallTimeouts
        )
    }

    nonisolated func shouldLimitUploadRetries(for error: Error) -> Bool {
        OneDriveErrorClassifier.isNameCollision(error)
    }

    nonisolated func shouldSetModificationDate() -> Bool {
        false
    }

    nonisolated var shouldDownloadRemoteFileForNameCollision: Bool {
        false
    }

    nonisolated func cancelActiveOperationsForAbandonment() {
        transport.cancelActiveOperations()
    }

    func connect() async throws {
        let root = try await itemByID(config.connection.rootItemID)
        guard root.folder != nil,
              root.id == config.connection.rootItemID,
              root.parentReference?.driveId == nil
                || root.parentReference?.driveId == config.connection.driveID else {
            throw RemoteStorageClientError.invalidConfiguration
        }
    }

    func disconnect() async {
        await transport.resetAuthentication()
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        nil
    }

    func verifyWriteAccess() async throws {
        let config = config
        let credential = credential
        let tokenProvider = tokenProvider
        let sharedState = sharedState
        try await RemoteStorageWriteVerifier.verify(
            client: self,
            cleanupClientFactory: {
                OneDriveClient(
                    config: config,
                    credential: credential,
                    tokenProvider: tokenProvider,
                    sharedState: sharedState
                )
            },
            basePath: "/",
            failureCleanupPolicy: .waitForCompletion
        )
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let normalized = try Self.canonicalRelativePath(path)
        var nextURL: URL? = try childrenURL(at: normalized)
        var entries: [RemoteStorageEntry] = []
        var seen: Set<String> = []
        while let url = nextURL {
            let (data, _) = try await performGraph(method: "GET", url: url, expected: [200])
            let page = try OneDriveJSON.decode(OneDriveDriveItemPage.self, from: data)
            for item in page.value where seen.insert(item.id).inserted {
                let itemPath = Self.appending(item.name, to: normalized)
                cacheItem(item, path: itemPath)
                entries.append(remoteEntry(item, path: itemPath))
            }
            nextURL = try page.nextLink.map(validatedNextLink)
        }
        return entries
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let normalized = try Self.canonicalRelativePath(path)
        do {
            let item = try await fetchItem(at: normalized)
            return remoteEntry(item, path: normalized)
        } catch {
            if OneDriveErrorClassifier.isNotFound(error) { return nil }
            throw error
        }
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try await upload(
            localURL: localURL,
            remotePath: remotePath,
            mode: .replace,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func upload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        if respectTaskCancellation {
            try Task.checkCancellation()
            try await performUpload(localURL: localURL, remotePath: remotePath, mode: mode, onProgress: onProgress)
            return
        }
        let operation = Task {
            try await self.performUpload(localURL: localURL, remotePath: remotePath, mode: mode, onProgress: onProgress)
        }
        try await withTaskCancellationHandler {
            try await operation.value
        } onCancel: {}
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        let normalized = try Self.canonicalRelativePath(path)
        let item: OneDriveDriveItem
        if let cached = cachedItem(path: normalized) {
            item = cached
        } else {
            item = try await resolveItem(at: normalized)
        }
        let body = try OneDriveJSON.body([
            "fileSystemInfo": ["lastModifiedDateTime": OneDriveDateCodec.string(from: date)]
        ])
        var headers: [String: String] = [:]
        if let eTag = item.eTag { headers["If-Match"] = eTag }
        let url = try itemURL(item.id)
        _ = try await performGraph(method: "PATCH", url: url, headers: headers, body: body, expected: [200])
    }

    func download(remotePath: String, localURL: URL) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        let item = try await resolveItem(at: Self.canonicalRelativePath(remotePath))
        try await download(item: item, localURL: localURL, onProgress: onProgress)
    }

    func downloadKnownFileForReadBackVerification(_ file: OneDriveKnownFile, localURL: URL) async throws {
        let item = OneDriveDriveItem(
            id: file.itemID,
            name: URL(fileURLWithPath: file.path).lastPathComponent,
            size: file.size,
            eTag: file.eTag,
            cTag: nil,
            createdDateTime: nil,
            lastModifiedDateTime: nil,
            folder: nil,
            file: nil,
            parentReference: nil,
            fileSystemInfo: nil
        )
        var lastError: Error?
        for attempt in 0 ..< 4 {
            do {
                try await download(item: item, localURL: localURL, onProgress: nil)
                return
            } catch {
                guard OneDriveErrorClassifier.isNotFound(error), attempt < 3 else { throw error }
                lastError = error
                removeCachedItem(path: file.path)
                removeCachedItem(id: file.itemID)
                try await Task.sleep(for: .milliseconds(250 * (attempt + 1)))
            }
        }
        throw RemoteReadBackRetryExhaustedError(underlying: lastError ?? URLError(.unknown))
    }

    private func download(item: OneDriveDriveItem, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        guard item.folder == nil else { throw Self.serviceError(status: 400, code: "folderContentNotSupported") }
        let url = try graphURL("/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(item.id))/content")
        let temporaryURL = try await performGraphDownload(url: url, onProgress: onProgress)
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        let parent = localURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
        if FileManager.default.fileExists(atPath: localURL.path) {
            try FileManager.default.removeItem(at: localURL)
        }
        try FileManager.default.moveItem(at: temporaryURL, to: localURL)
        onProgress?(1)
    }

    func downloadForReadBackVerification(remotePath: String, localURL: URL) async throws {
        var lastError: Error?
        for attempt in 0 ..< 4 {
            do {
                try await download(remotePath: remotePath, localURL: localURL)
                return
            } catch {
                guard OneDriveErrorClassifier.isNotFound(error), attempt < 3 else { throw error }
                lastError = error
                try await Task.sleep(for: .milliseconds(250 * (attempt + 1)))
            }
        }
        throw RemoteReadBackRetryExhaustedError(underlying: lastError ?? URLError(.unknown))
    }

    func exists(path: String) async throws -> Bool {
        try await metadata(path: path) != nil
    }

    func delete(path: String) async throws {
        let normalized = try Self.canonicalRelativePath(path)
        guard normalized != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        let item = try await resolveItem(at: normalized)
        try await delete(item: item)
        removeCachedItem(path: normalized)
    }

    func deleteKnownPresentFile(path: String) async throws {
        let normalized = try Self.canonicalRelativePath(path)
        guard normalized != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        let item = try await fetchItem(at: normalized)
        guard item.folder == nil else { throw Self.serviceError(status: 400, code: "notAFile") }
        try await delete(item: item)
        removeCachedItem(path: normalized)
    }

    func deleteKnownPresentFile(_ file: OneDriveKnownFile) async throws {
        guard file.path != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        let item = OneDriveDriveItem(
            id: file.itemID,
            name: URL(fileURLWithPath: file.path).lastPathComponent,
            size: file.size,
            eTag: file.eTag,
            cTag: nil,
            createdDateTime: nil,
            lastModifiedDateTime: nil,
            folder: nil,
            file: nil,
            parentReference: nil,
            fileSystemInfo: nil
        )
        try await delete(item: item)
        removeCachedItem(path: file.path)
        removeCachedItem(id: file.itemID)
    }

    func createDirectory(path: String) async throws {
        let normalized = try Self.canonicalRelativePath(path)
        if normalized == "/" { return }
        var parent = try await rootDirectory()
        var parentPath = "/"
        for component in try Self.pathComponents(normalized) {
            let candidate = Self.appending(component, to: parentPath)
            if let cached = cachedDirectory(path: candidate) {
                parent = cached
            } else {
                parent = try await createDirectoryComponent(
                    name: component,
                    parent: parent,
                    candidatePath: candidate
                )
                cacheDirectory(parent, path: candidate)
            }
            parentPath = candidate
        }
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        let sourceNormalized = try Self.canonicalRelativePath(sourcePath)
        let destinationNormalized = try Self.canonicalRelativePath(destinationPath)
        guard sourceNormalized != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        let source = try await resolveItem(at: sourceNormalized)
        let destination = try await resolveDestination(destinationNormalized)
        if let existing = try await itemIfPresent(at: destinationNormalized, useCache: false), existing.id != source.id {
            try await delete(item: existing)
            removeCachedItem(path: destinationNormalized)
            removeCachedItem(id: existing.id)
        }
        _ = try await moveItem(
            source,
            fromPath: sourceNormalized,
            to: destination,
            destinationPath: destinationNormalized
        )
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try Task.checkCancellation()
        let source = try await resolveItem(at: Self.canonicalRelativePath(sourcePath))
        let destinationPath = try Self.canonicalRelativePath(destinationPath)
        let destination = try await resolveDestination(destinationPath)
        if let existing = try await itemIfPresent(at: destinationPath, useCache: false) {
            if existing.id == source.id { return }
            try await delete(item: existing)
            removeCachedItem(path: destinationPath)
            removeCachedItem(id: existing.id)
        }
        let body = try OneDriveJSON.body([
            "name": destination.name,
            "parentReference": ["driveId": config.connection.driveID, "id": destination.parent.id]
        ])
        let (_, response) = try await performGraph(
            method: "POST",
            url: try graphURL("/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(source.id))/copy"),
            body: body,
            expected: [202]
        )
        guard let rawLocation = response.value(forHTTPHeaderField: "Location"),
              let monitorURL = URL(string: rawLocation),
              monitorURL.scheme?.lowercased() == "https" else {
            throw OneDriveMutationOutcomeUnknownError(operation: "copy")
        }

        let acceptedOperation = Task {
            let resourceID = try await self.waitForCopyCompletion(monitorURL: monitorURL)
            guard try await self.verifyCompletedCopy(
                resourceID: resourceID,
                destinationPath: destinationPath,
                destination: destination
            ) else {
                throw OneDriveMutationOutcomeUnknownError(operation: "copy")
            }
        }
        do {
            try await withTaskCancellationHandler {
                try await acceptedOperation.value
            } onCancel: {}
        } catch let error as OneDriveMutationOutcomeUnknownError {
            throw error
        } catch {
            if OneDriveErrorClassifier.isNameCollision(error) { throw error }
            throw OneDriveMutationOutcomeUnknownError(operation: "copy")
        }
    }

    func publishUploadedManifest(
        tempPath: String,
        finalPath: String,
        backupPath: String,
        ignoreCancellation: Bool,
        assertOwnership: @escaping @Sendable () async throws -> Void
    ) async throws -> OneDriveManifestPublishOutcome {
        let tempNormalized = try Self.canonicalRelativePath(tempPath)
        let finalNormalized = try Self.canonicalRelativePath(finalPath)
        let backupNormalized = try Self.canonicalRelativePath(backupPath)
        let temp = try await resolveItem(at: tempNormalized)
        guard temp.folder == nil else { throw Self.serviceError(status: 400, code: "notAFile") }

        try Self.checkCancellation(unless: ignoreCancellation)
        try await assertOwnership()
        let final = try await itemIfPresent(at: finalNormalized, useCache: false)
        let finalDestination = try await resolveDestination(finalNormalized)

        guard let final else {
            try Self.checkCancellation(unless: ignoreCancellation)
            try await assertOwnership()
            let finalItem = try await moveItem(
                temp,
                fromPath: tempNormalized,
                to: finalDestination,
                destinationPath: finalNormalized
            )
            return OneDriveManifestPublishOutcome(
                backedUpPriorFinal: false,
                finalFile: knownFile(from: finalItem, path: finalNormalized),
                backupFile: nil
            )
        }

        let backupDestination = try await resolveDestination(backupNormalized)
        let backupItem: OneDriveDriveItem
        do {
            try Self.checkCancellation(unless: ignoreCancellation)
            try await assertOwnership()
            backupItem = try await moveItem(
                final,
                fromPath: finalNormalized,
                to: backupDestination,
                destinationPath: backupNormalized
            )
        } catch {
            if OneDriveErrorClassifier.isNotFound(error) {
                try Self.checkCancellation(unless: ignoreCancellation)
                try await assertOwnership()
                let finalItem = try await moveItem(
                    temp,
                    fromPath: tempNormalized,
                    to: finalDestination,
                    destinationPath: finalNormalized
                )
                return OneDriveManifestPublishOutcome(
                    backedUpPriorFinal: false,
                    finalFile: knownFile(from: finalItem, path: finalNormalized),
                    backupFile: nil
                )
            }
            await restoreBackupIfFinalMissing(
                backupPath: backupNormalized,
                finalPath: finalNormalized,
                assertOwnership: assertOwnership
            )
            throw error
        }

        try Self.checkCancellation(unless: ignoreCancellation)
        try await assertOwnership()
        let finalItem: OneDriveDriveItem
        do {
            finalItem = try await moveItem(
                temp,
                fromPath: tempNormalized,
                to: finalDestination,
                destinationPath: finalNormalized
            )
        } catch {
            await restoreBackupIfFinalMissing(
                backupPath: backupNormalized,
                finalPath: finalNormalized,
                assertOwnership: assertOwnership
            )
            throw error
        }
        return OneDriveManifestPublishOutcome(
            backedUpPriorFinal: true,
            finalFile: knownFile(from: finalItem, path: finalNormalized),
            backupFile: knownFile(from: backupItem, path: backupNormalized)
        )
    }

    private func performUpload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        onProgress: ((Double) -> Void)?
    ) async throws {
        let attributes = try FileManager.default.attributesOfItem(atPath: localURL.path)
        guard let number = attributes[.size] as? NSNumber else { throw RemoteStorageClientError.invalidConfiguration }
        let size = number.int64Value
        let normalizedPath = try Self.canonicalRelativePath(remotePath)
        let destination = try Self.uploadDestination(normalizedPath)
        onProgress?(0)
        let start = CFAbsoluteTimeGetCurrent()
        logUpload("start", remotePath: remotePath, size: size, mode: mode, duration: nil)
        do {
            let uploaded: OneDriveDriveItem
            do {
                uploaded = try await uploadResolved(
                    localURL: localURL,
                    size: size,
                    destination: destination,
                    mode: mode,
                    onProgress: onProgress
                )
            } catch {
                guard OneDriveErrorClassifier.isNotFound(error) else { throw error }
                let createStart = CFAbsoluteTimeGetCurrent()
                try await createDirectory(path: destination.parentPath)
                Self.logTrace(
                    "uploadParent.createAfterNotFound",
                    path: destination.parentPath,
                    size: size,
                    duration: Self.elapsedSeconds(since: createStart)
                )
                uploaded = try await uploadResolved(
                    localURL: localURL,
                    size: size,
                    destination: destination,
                    mode: mode,
                    onProgress: onProgress
                )
            }
            cacheRecentlyUploadedItem(uploaded, path: normalizedPath)
        } catch {
            if mode == .createIfAbsent, OneDriveErrorClassifier.isNameCollision(error) {
                throw remoteStorageNameCollisionError(path: remotePath)
            }
            logUpload("failed", remotePath: remotePath, size: size, mode: mode, duration: Self.elapsedSeconds(since: start), error: error)
            throw error
        }
        logUpload("complete", remotePath: remotePath, size: size, mode: mode, duration: Self.elapsedSeconds(since: start))
    }

    private func uploadResolved(
        localURL: URL,
        size: Int64,
        destination: UploadDestination,
        mode: RemoteUploadMode,
        onProgress: ((Double) -> Void)?
    ) async throws -> OneDriveDriveItem {
        if size < Self.directUploadThreshold {
            let uploaded = try await directUpload(localURL: localURL, size: size, destination: destination, mode: mode)
            onProgress?(1)
            return uploaded
        }
        guard size > 0 else { throw RemoteStorageClientError.unsafeConditionalCreateUnsupported }
        return try await uploadWithSession(
            localURL: localURL,
            size: size,
            destination: destination,
            mode: mode,
            onProgress: onProgress
        )
    }

    private func logUpload(
        _ event: String,
        remotePath: String,
        size: Int64,
        mode: RemoteUploadMode,
        duration: TimeInterval?,
        error: Error? = nil
    ) {
        let durationText = duration.map(Self.msText) ?? "-"
        let errorText = error.map { " error=\(($0 as NSError).localizedDescription)" } ?? ""
        let message = "[OneDriveUpload] event=\(event) path=\(remotePath) size=\(size) mode=\(mode) durationMs=\(durationText)\(errorText)"
        #if DEBUG
        print(message)
        #endif
    }

    nonisolated private static func logTrace(
        _ operation: String,
        path: String,
        size: Int64,
        duration: TimeInterval,
        extra: String = ""
    ) {
        let suffix = extra.isEmpty ? "" : " \(extra)"
        let message = "[OneDriveTrace] operation=\(operation) path=\(path) size=\(size) durationMs=\(msText(duration))\(suffix)"
        #if DEBUG
        print(message)
        #endif
    }

    private func directUpload(
        localURL: URL,
        size: Int64,
        destination: UploadDestination,
        mode: RemoteUploadMode
    ) async throws -> OneDriveDriveItem {
        guard size <= Self.directUploadThreshold else { throw RemoteStorageClientError.invalidConfiguration }
        let readStart = CFAbsoluteTimeGetCurrent()
        let data = try Data(contentsOf: localURL, options: .mappedIfSafe)
        Self.logTrace(
            "directUpload.read",
            path: destination.name,
            size: size,
            duration: Self.elapsedSeconds(since: readStart)
        )
        let queryItems: [URLQueryItem] = mode == .createIfAbsent
            ? [URLQueryItem(name: "@microsoft.graph.conflictBehavior", value: "fail")]
            : []
        let encodedPath = try Self.encodedRelativePath(destination.path)
        let url = try graphURL(
            "/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(config.connection.rootItemID)):/\(encodedPath):/content",
            queryItems: queryItems
        )
        let headers = ["Content-Type": "application/octet-stream"]
        let requestStart = CFAbsoluteTimeGetCurrent()
        let (responseData, _) = try await performGraph(
            method: "PUT",
            url: url,
            headers: headers,
            body: data,
            expected: [200, 201]
        )
        Self.logTrace(
            "directUpload.request",
            path: destination.name,
            size: size,
            duration: Self.elapsedSeconds(since: requestStart)
        )
        return try OneDriveJSON.decode(OneDriveDriveItem.self, from: responseData)
    }

    private func uploadWithSession(
        localURL: URL,
        size: Int64,
        destination: UploadDestination,
        mode: RemoteUploadMode,
        onProgress: ((Double) -> Void)?
    ) async throws -> OneDriveDriveItem {
        let behavior = mode == .createIfAbsent ? "fail" : "replace"
        let body = try OneDriveJSON.body([
            "item": [
                "@microsoft.graph.conflictBehavior": behavior
            ]
        ])
        let encodedPath = try Self.encodedRelativePath(destination.path)
        let createURL = try graphURL(
            "/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(config.connection.rootItemID)):/\(encodedPath):/createUploadSession"
        )
        let createStart = CFAbsoluteTimeGetCurrent()
        let (sessionData, _) = try await performGraph(method: "POST", url: createURL, body: body, expected: [200])
        Self.logTrace(
            "uploadSession.create",
            path: destination.name,
            size: size,
            duration: Self.elapsedSeconds(since: createStart)
        )
        let uploadSession = try OneDriveJSON.decode(OneDriveUploadSession.self, from: sessionData)
        guard let uploadURL = URL(string: uploadSession.uploadUrl),
              uploadURL.scheme?.lowercased() == "https" else {
            throw Self.serviceError(status: -1, code: "invalidUploadSession")
        }

        var nextOffset: Int64
        if uploadSession.nextExpectedRanges == nil {
            nextOffset = 0
        } else if let offset = Self.validatedUploadOffset(
            from: uploadSession.nextExpectedRanges,
            size: size
        ) {
            nextOffset = offset
        } else {
            throw Self.serviceError(status: -1, code: "invalidUploadStatus")
        }
        var fragmentRecoveryCount = 0
        while nextOffset < size {
            try Task.checkCancellation()
            let offset = nextOffset
            let remainingLength = size - offset
            guard offset >= 0,
                  remainingLength > 0 else {
                throw Self.serviceError(status: -1, code: "invalidUploadStatus")
            }
            let length = min(Self.uploadFragmentSize, remainingLength)
            let readStart = CFAbsoluteTimeGetCurrent()
            let fragment = try Self.readFileSlice(at: localURL, offset: offset, length: length)
            Self.logTrace(
                "uploadSession.fragmentRead",
                path: destination.name,
                size: Int64(fragment.count),
                duration: Self.elapsedSeconds(since: readStart),
                extra: "offset=\(offset)"
            )
            let headers = [
                "Content-Length": String(fragment.count),
                "Content-Range": "bytes \(offset)-\(offset + length - 1)/\(size)"
            ]
            let data: Data
            let response: HTTPURLResponse
            let putStart = CFAbsoluteTimeGetCurrent()
            do {
                (data, response) = try await performPreauthenticated(
                    method: "PUT",
                    url: uploadURL,
                    headers: headers,
                    body: fragment,
                    expected: [200, 201, 202, 416]
                )
            } catch {
                Self.logTrace(
                    "uploadSession.fragmentPut.failed",
                    path: destination.name,
                    size: Int64(fragment.count),
                    duration: Self.elapsedSeconds(since: putStart),
                    extra: "offset=\(offset) error=\((error as NSError).localizedDescription)"
                )
                guard OneDriveErrorClassifier.isConnectionUnavailable(error),
                      fragmentRecoveryCount < Self.uploadRecoveryLimit else { throw error }
                fragmentRecoveryCount += 1
                let recoverStart = CFAbsoluteTimeGetCurrent()
                let recoveredOffset = try await recoverUploadOffset(
                    uploadURL: uploadURL,
                    size: size,
                    delayBeforeFirstRequest: true
                )
                Self.logTrace(
                    "uploadSession.recoverOffset",
                    path: destination.name,
                    size: size,
                    duration: Self.elapsedSeconds(since: recoverStart),
                    extra: "from=\(offset) to=\(recoveredOffset)"
                )
                if recoveredOffset > offset {
                    fragmentRecoveryCount = 0
                }
                nextOffset = recoveredOffset
                onProgress?(min(1, Double(nextOffset) / Double(size)))
                continue
            }
            Self.logTrace(
                "uploadSession.fragmentPut",
                path: destination.name,
                size: Int64(fragment.count),
                duration: Self.elapsedSeconds(since: putStart),
                extra: "offset=\(offset) status=\(response.statusCode)"
            )
            switch response.statusCode {
            case 200, 201:
                let uploaded = try OneDriveJSON.decode(OneDriveDriveItem.self, from: data)
                onProgress?(1)
                return uploaded
            case 202:
                let status = try OneDriveJSON.decode(OneDriveUploadStatus.self, from: data)
                guard let offset = Self.validatedUploadOffset(from: status.nextExpectedRanges, size: size) else {
                    throw Self.serviceError(status: -1, code: "invalidUploadStatus")
                }
                nextOffset = offset
                fragmentRecoveryCount = 0
            case 416:
                guard fragmentRecoveryCount < Self.uploadRecoveryLimit else {
                    throw Self.serviceError(status: 416, code: "invalidRange")
                }
                fragmentRecoveryCount += 1
                let recoverStart = CFAbsoluteTimeGetCurrent()
                let recoveredOffset = try await recoverUploadOffset(
                    uploadURL: uploadURL,
                    size: size,
                    delayBeforeFirstRequest: false
                )
                Self.logTrace(
                    "uploadSession.recoverOffset",
                    path: destination.name,
                    size: size,
                    duration: Self.elapsedSeconds(since: recoverStart),
                    extra: "from=\(offset) to=\(recoveredOffset)"
                )
                if recoveredOffset > offset {
                    fragmentRecoveryCount = 0
                }
                nextOffset = recoveredOffset
            default:
                throw Self.serviceError(status: response.statusCode, code: nil)
            }
            onProgress?(min(1, Double(nextOffset) / Double(size)))
        }
        throw OneDriveMutationOutcomeUnknownError(operation: "upload")
    }

    private func moveItem(
        _ source: OneDriveDriveItem,
        fromPath sourcePath: String,
        to destination: Destination,
        destinationPath: String
    ) async throws -> OneDriveDriveItem {
        let body = try OneDriveJSON.body([
            "name": destination.name,
            "parentReference": ["driveId": config.connection.driveID, "id": destination.parent.id]
        ])
        var headers: [String: String] = [:]
        if let eTag = source.eTag { headers["If-Match"] = eTag }
        let (data, _) = try await performGraph(
            method: "PATCH",
            url: try itemURL(source.id),
            headers: headers,
            body: body,
            expected: [200]
        )
        let moved = try OneDriveJSON.decode(OneDriveDriveItem.self, from: data)
        guard moved.name == destination.name,
              moved.parentReference?.id == nil || moved.parentReference?.id == destination.parent.id else {
            throw OneDriveMutationOutcomeUnknownError(operation: "move")
        }
        removeCachedItem(path: sourcePath)
        cacheItem(moved, path: destinationPath)
        return moved
    }

    private func restoreBackupIfFinalMissing(
        backupPath: String,
        finalPath: String,
        assertOwnership: @escaping @Sendable () async throws -> Void
    ) async {
        do {
            guard try await itemIfPresent(at: finalPath, useCache: false) == nil else { return }
            try await assertOwnership()
            guard let backup = try await itemIfPresent(at: backupPath, useCache: false) else { return }
            let finalDestination = try await resolveDestination(finalPath)
            _ = try await moveItem(
                backup,
                fromPath: backupPath,
                to: finalDestination,
                destinationPath: finalPath
            )
        } catch {}
    }

    private func createDirectoryComponent(
        name: String,
        parent: OneDriveDriveItem,
        candidatePath: String
    ) async throws -> OneDriveDriveItem {
        let body = try OneDriveJSON.body([
            "name": name,
            "folder": [String: Any](),
            "@microsoft.graph.conflictBehavior": "fail"
        ])
        let url = try graphURL("/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(parent.id))/children")
        var lastError: Error?
        for attempt in 0 ..< 5 {
            do {
                let (data, _) = try await performGraph(method: "POST", url: url, body: body, expected: [201])
                let created = try OneDriveJSON.decode(OneDriveDriveItem.self, from: data)
                guard created.folder != nil else { throw remoteStorageNameCollisionError(path: candidatePath) }
                return created
            } catch {
                if OneDriveErrorClassifier.isNameCollision(error) {
                    let existing = try await resolveItem(at: candidatePath)
                    guard existing.folder != nil else { throw remoteStorageNameCollisionError(path: candidatePath) }
                    return existing
                }
                guard OneDriveErrorClassifier.isConnectionUnavailable(error), attempt < 4 else { throw error }
                lastError = error
                if let existing = try await directoryIfPresent(at: candidatePath) {
                    return existing
                }
                try await Task.sleep(for: .seconds(Self.retryDelay(attempt: attempt)))
                if let existing = try await directoryIfPresent(at: candidatePath) {
                    return existing
                }
            }
        }
        throw lastError ?? RemoteStorageClientError.unavailable
    }

    private func directoryIfPresent(at normalizedPath: String) async throws -> OneDriveDriveItem? {
        do {
            guard let existing = try await itemIfPresent(at: normalizedPath) else { return nil }
            guard existing.folder != nil else { throw remoteStorageNameCollisionError(path: normalizedPath) }
            return existing
        } catch {
            guard OneDriveErrorClassifier.isConnectionUnavailable(error) else { throw error }
            return nil
        }
    }

    private func resolveDestination(_ rawPath: String) async throws -> Destination {
        let normalized = try Self.canonicalRelativePath(rawPath)
        let components = try Self.pathComponents(normalized)
        guard let name = components.last else { throw RemoteStorageClientError.invalidConfiguration }
        let parentComponents = components.dropLast()
        let parentPath = parentComponents.isEmpty ? "/" : "/" + parentComponents.joined(separator: "/")
        let parent = try await resolveDirectory(at: parentPath)
        guard parent.folder != nil else { throw Self.serviceError(status: 400, code: "notAFolder") }
        return Destination(parent: parent, parentPath: parentPath, name: name)
    }

    private func rootDirectory() async throws -> OneDriveDriveItem {
        if let cached = cachedDirectory(path: "/") { return cached }
        let root = try await itemByID(config.connection.rootItemID)
        guard root.folder != nil else { throw Self.serviceError(status: 400, code: "notAFolder") }
        cacheDirectory(root, path: "/")
        return root
    }

    private func resolveDirectory(at normalizedPath: String) async throws -> OneDriveDriveItem {
        if let cached = cachedDirectory(path: normalizedPath) { return cached }
        let item = try await resolveItem(at: normalizedPath)
        guard item.folder != nil else { throw Self.serviceError(status: 400, code: "notAFolder") }
        cacheDirectory(item, path: normalizedPath)
        return item
    }

    private func resolveItem(
        at normalizedPath: String,
        waitForThrottle: Bool = false
    ) async throws -> OneDriveDriveItem {
        if normalizedPath == "/" {
            if let cached = cachedDirectory(path: "/") { return cached }
            return try await rootDirectory()
        }
        if let cached = cachedItem(path: normalizedPath) { return cached }
        return try await fetchItem(at: normalizedPath, waitForThrottle: waitForThrottle)
    }

    private func fetchItem(
        at normalizedPath: String,
        waitForThrottle: Bool = false
    ) async throws -> OneDriveDriveItem {
        if normalizedPath == "/" {
            let root = try await itemByID(config.connection.rootItemID, waitForThrottle: waitForThrottle)
            guard root.folder != nil else { throw Self.serviceError(status: 400, code: "notAFolder") }
            cacheDirectory(root, path: "/")
            return root
        }
        let encodedPath = try Self.pathComponents(normalizedPath).map(Self.encode).joined(separator: "/")
        let url = try graphURL(
            "/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(config.connection.rootItemID)):/\(encodedPath)",
            queryItems: [URLQueryItem(name: "$select", value: Self.driveItemSelect)]
        )
        let (data, _) = try await performGraph(
            method: "GET",
            url: url,
            expected: [200],
            waitForThrottle: waitForThrottle
        )
        let item = try OneDriveJSON.decode(OneDriveDriveItem.self, from: data)
        guard item.parentReference?.driveId == nil || item.parentReference?.driveId == config.connection.driveID else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        cacheItem(item, path: normalizedPath)
        return item
    }

    private func itemIfPresent(
        at normalizedPath: String,
        waitForThrottle: Bool = false,
        useCache: Bool = true
    ) async throws -> OneDriveDriveItem? {
        do {
            if useCache {
                return try await resolveItem(at: normalizedPath, waitForThrottle: waitForThrottle)
            }
            return try await fetchItem(at: normalizedPath, waitForThrottle: waitForThrottle)
        } catch {
            if OneDriveErrorClassifier.isNotFound(error) { return nil }
            throw error
        }
    }

    private func itemByID(_ id: String, waitForThrottle: Bool = false) async throws -> OneDriveDriveItem {
        let (data, _) = try await performGraph(
            method: "GET",
            url: try itemMetadataURL(id),
            expected: [200],
            waitForThrottle: waitForThrottle
        )
        let item = try OneDriveJSON.decode(OneDriveDriveItem.self, from: data)
        if id == config.connection.rootItemID, item.folder != nil {
            cacheDirectory(item, path: "/")
        }
        return item
    }

    private func cachedItem(path: String) -> OneDriveDriveItem? {
        sharedState.itemIndex.item(namespace: itemNamespace, path: path)
    }

    private func cachedDirectory(path: String) -> OneDriveDriveItem? {
        guard let item = cachedItem(path: path), item.folder != nil else { return nil }
        return item
    }

    private func cacheItem(_ item: OneDriveDriveItem, path: String) {
        sharedState.itemIndex.cache(item, namespace: itemNamespace, path: path)
    }

    private func cacheDirectory(_ item: OneDriveDriveItem, path: String) {
        guard item.folder != nil else { return }
        cacheItem(item, path: path)
    }

    private func cacheRecentlyUploadedItem(_ item: OneDriveDriveItem, path: String) {
        guard item.folder == nil else { return }
        cacheItem(item, path: path)
    }

    private func removeCachedItem(path: String) {
        sharedState.itemIndex.remove(namespace: itemNamespace, path: path)
    }

    private func removeCachedItem(id: String) {
        sharedState.itemIndex.remove(namespace: itemNamespace, id: id)
    }

    private func itemURL(_ id: String) throws -> URL {
        try graphURL("/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(id))")
    }

    private func itemMetadataURL(_ id: String) throws -> URL {
        try graphURL(
            "/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(id))",
            queryItems: [URLQueryItem(name: "$select", value: Self.driveItemSelect)]
        )
    }

    private func childrenURL(at normalizedPath: String) throws -> URL {
        if normalizedPath == "/" {
            return try graphURL(
                "/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(config.connection.rootItemID))/children",
                queryItems: [URLQueryItem(name: "$select", value: Self.driveItemSelect)]
            )
        }
        let encodedPath = try Self.encodedRelativePath(normalizedPath)
        return try graphURL(
            "/drives/\(Self.encode(config.connection.driveID))/items/\(Self.encode(config.connection.rootItemID)):/\(encodedPath):/children",
            queryItems: [URLQueryItem(name: "$select", value: Self.driveItemSelect)]
        )
    }

    private func knownFile(from item: OneDriveDriveItem, path: String) -> OneDriveKnownFile {
        OneDriveKnownFile(
            path: path,
            itemID: item.id,
            eTag: item.eTag,
            size: item.size
        )
    }

    private func delete(item: OneDriveDriveItem) async throws {
        var headers: [String: String] = [:]
        if let eTag = item.eTag { headers["If-Match"] = eTag }
        _ = try await performGraph(method: "DELETE", url: try itemURL(item.id), headers: headers, expected: [204])
    }

    private func remoteEntry(_ item: OneDriveDriveItem, path: String) -> RemoteStorageEntry {
        RemoteStorageEntry(
            path: path,
            name: item.name,
            isDirectory: item.folder != nil,
            size: item.size ?? 0,
            creationDate: OneDriveDateCodec.date(from: item.fileSystemInfo?.createdDateTime)
                ?? OneDriveDateCodec.date(from: item.createdDateTime),
            modificationDate: OneDriveDateCodec.date(from: item.fileSystemInfo?.lastModifiedDateTime)
                ?? OneDriveDateCodec.date(from: item.lastModifiedDateTime)
        )
    }

    private func waitForCopyCompletion(monitorURL: URL) async throws -> String? {
        for attempt in 0 ..< Self.copyPollLimit {
            let data: Data
            let response: HTTPURLResponse
            do {
                (data, response) = try await performUnauthenticated(
                    method: "GET",
                    url: monitorURL,
                    expected: [200, 202]
                )
            } catch {
                guard OneDriveErrorClassifier.isConnectionUnavailable(error) else { throw error }
                try await Task.sleep(for: .seconds(Self.retryDelay(attempt: attempt)))
                continue
            }
            let operation = try OneDriveJSON.decode(OneDriveAsyncOperationStatus.self, from: data)
            switch operation.status?.lowercased() {
            case "completed":
                return operation.resourceId
            case "failed", "deletefailed":
                let code = operation.error?.code
                throw Self.serviceError(status: code == "nameAlreadyExists" ? 409 : 400, code: code)
            case "cancelled":
                throw CancellationError()
            case "notstarted", "inprogress", "updating", "waiting", "cancelpending", "deletepending":
                let delay = Self.retryDelay(response: response, attempt: attempt)
                try await Task.sleep(for: .seconds(delay))
            default:
                throw OneDriveMutationOutcomeUnknownError(operation: "copy")
            }
        }
        throw OneDriveMutationOutcomeUnknownError(operation: "copy")
    }

    private func verifyCompletedCopy(
        resourceID: String?,
        destinationPath: String,
        destination: Destination
    ) async throws -> Bool {
        for attempt in 0 ..< Self.acceptedVerificationLimit {
            do {
                let copied: OneDriveDriveItem?
                if let resourceID {
                    do {
                        copied = try await itemByID(resourceID, waitForThrottle: true)
                    } catch {
                        if OneDriveErrorClassifier.isNotFound(error) {
                            copied = nil
                        } else {
                            throw error
                        }
                    }
                } else {
                    copied = try await itemIfPresent(at: destinationPath, waitForThrottle: true)
                }
                if let copied,
                   copied.name == destination.name,
                   copied.parentReference?.driveId == nil
                    || copied.parentReference?.driveId == config.connection.driveID,
                   copied.parentReference?.id == nil
                    || copied.parentReference?.id == destination.parent.id {
                    return true
                }
            } catch {
                guard OneDriveErrorClassifier.isConnectionUnavailable(error) else { throw error }
            }
            if attempt < Self.acceptedVerificationLimit - 1 {
                try await Task.sleep(for: .seconds(Self.retryDelay(attempt: attempt)))
            }
        }
        return false
    }

    private func recoverUploadOffset(
        uploadURL: URL,
        size: Int64,
        delayBeforeFirstRequest: Bool
    ) async throws -> Int64 {
        var lastError: Error?
        for attempt in 0 ..< Self.uploadRecoveryLimit {
            if delayBeforeFirstRequest || attempt > 0 {
                try await Task.sleep(for: .seconds(Self.retryDelay(attempt: attempt)))
            }
            do {
                let (statusData, _) = try await performPreauthenticated(
                    method: "GET",
                    url: uploadURL,
                    expected: [200]
                )
                let status = try OneDriveJSON.decode(OneDriveUploadStatus.self, from: statusData)
                guard let offset = Self.validatedUploadOffset(
                    from: status.nextExpectedRanges,
                    size: size
                ) else {
                    throw Self.serviceError(status: -1, code: "invalidUploadStatus")
                }
                return offset
            } catch {
                guard OneDriveErrorClassifier.isConnectionUnavailable(error) else { throw error }
                lastError = error
            }
        }
        throw lastError ?? Self.serviceError(status: -1, code: "uploadRecoveryFailed")
    }

    private func performGraph(
        method: String,
        url: URL,
        headers: [String: String] = [:],
        body: Data? = nil,
        expected: Set<Int>,
        waitForThrottle: Bool = false
    ) async throws -> (Data, HTTPURLResponse) {
        try await transport.performGraph(
            method: method,
            url: url,
            headers: headers,
            body: body,
            expected: expected,
            waitForThrottle: waitForThrottle
        )
    }

    private func performGraphDownload(
        url: URL,
        onProgress: ((Double) -> Void)?
    ) async throws -> URL {
        try await transport.performGraphDownload(url: url, onProgress: onProgress)
    }

    private func performPreauthenticated(
        method: String,
        url: URL,
        headers: [String: String] = [:],
        body: Data? = nil,
        expected: Set<Int>
    ) async throws -> (Data, HTTPURLResponse) {
        try await transport.performPreauthenticated(
            method: method,
            url: url,
            headers: headers,
            body: body,
            expected: expected
        )
    }

    private func performUnauthenticated(
        method: String,
        url: URL,
        expected: Set<Int>
    ) async throws -> (Data, HTTPURLResponse) {
        try await transport.performUnauthenticated(method: method, url: url, expected: expected)
    }

    private func graphURL(_ percentEncodedSuffix: String) throws -> URL {
        try graphURL(percentEncodedSuffix, queryItems: [])
    }

    private func graphURL(_ percentEncodedSuffix: String, queryItems: [URLQueryItem]) throws -> URL {
        var components = URLComponents(url: config.connection.cloudEnvironment.graphBaseURL, resolvingAgainstBaseURL: false)
        components?.percentEncodedPath += percentEncodedSuffix
        if !queryItems.isEmpty {
            components?.queryItems = queryItems
        }
        guard let url = components?.url else { throw RemoteStorageClientError.invalidConfiguration }
        return url
    }

    private func validatedNextLink(_ rawValue: String) throws -> URL {
        guard let url = URL(string: rawValue) else { throw RemoteStorageClientError.invalidConfiguration }
        try OneDriveGraphTransport.validateGraphURL(
            url,
            baseURL: config.connection.cloudEnvironment.graphBaseURL
        )
        return url
    }

    nonisolated private static func canonicalRelativePath(_ value: String) throws -> String {
        let components = value.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        for component in components { try validateComponent(component) }
        let result = components.isEmpty ? "/" : "/" + components.joined(separator: "/")
        guard result.count <= 400 else { throw RemoteStorageClientError.invalidConfiguration }
        return result
    }

    nonisolated private static func pathComponents(_ normalizedPath: String) throws -> [String] {
        let components = normalizedPath.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        for component in components { try validateComponent(component) }
        return components
    }

    nonisolated private static func uploadDestination(_ normalizedPath: String) throws -> UploadDestination {
        let components = try pathComponents(normalizedPath)
        guard let name = components.last else { throw RemoteStorageClientError.invalidConfiguration }
        let parentPath = components.count == 1 ? "/" : "/" + components.dropLast().joined(separator: "/")
        return UploadDestination(path: normalizedPath, parentPath: parentPath, name: name)
    }

    nonisolated private static func validateComponent(_ value: String) throws {
        guard RemoteFileNamePolicy.oneDrive.isValid(value) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
    }

    nonisolated private static let unreserved: CharacterSet = {
        var result = CharacterSet.alphanumerics
        result.insert(charactersIn: "-._~")
        return result
    }()

    nonisolated private static func encode(_ value: String) -> String {
        value.addingPercentEncoding(withAllowedCharacters: unreserved) ?? ""
    }

    nonisolated private static func encodedRelativePath(_ normalizedPath: String) throws -> String {
        try pathComponents(normalizedPath).map(encode).joined(separator: "/")
    }

    nonisolated private static func appending(_ component: String, to path: String) -> String {
        path == "/" ? "/\(component)" : "\(path)/\(component)"
    }

    nonisolated private static func readFileSlice(at url: URL, offset: Int64, length: Int64) throws -> Data {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        try handle.seek(toOffset: UInt64(offset))
        let data = try handle.read(upToCount: Int(length)) ?? Data()
        guard data.count == Int(length) else { throw CocoaError(.fileReadUnknown) }
        return data
    }

    nonisolated private static func nextUploadOffset(from ranges: [String]?) -> Int64? {
        guard let first = ranges?.first,
              !first.isEmpty else {
            return nil
        }
        let bounds = first.split(separator: "-", maxSplits: 1, omittingEmptySubsequences: false)
        guard let startValue = bounds.first,
              let start = Int64(startValue) else { return nil }
        let end = bounds.count == 2 && !bounds[1].isEmpty ? Int64(bounds[1]) : nil
        if bounds.count == 2, !bounds[1].isEmpty, end == nil { return nil }
        if let end, end < start { return nil }
        return start
    }

    nonisolated private static func validatedUploadOffset(
        from ranges: [String]?,
        size: Int64
    ) -> Int64? {
        guard let offset = nextUploadOffset(from: ranges),
              offset >= 0,
              offset < size else {
            return nil
        }
        return offset
    }

    nonisolated private static func retryDelay(response: HTTPURLResponse, attempt: Int) -> Double {
        if let date = OneDriveErrorClassifier.retryAfter(from: response) {
            return min(30, max(0.25, date.timeIntervalSinceNow))
        }
        return retryDelay(attempt: attempt)
    }

    nonisolated private static func retryDelay(attempt: Int) -> Double {
        min(5, 0.5 + Double(attempt) * 0.1)
    }

    nonisolated private static func checkCancellation(unless ignoreCancellation: Bool) throws {
        if !ignoreCancellation {
            try Task.checkCancellation()
        }
    }

    nonisolated private static func elapsedSeconds(since start: CFAbsoluteTime) -> TimeInterval {
        max(CFAbsoluteTimeGetCurrent() - start, 0)
    }

    nonisolated private static func msText(_ seconds: TimeInterval) -> String {
        String(format: "%.1f", seconds * 1_000)
    }

    nonisolated private static func serviceError(
        status: Int,
        code: String?,
        retryAfter: Date? = nil,
        claims: String? = nil
    ) -> NSError {
        OneDriveErrorClassifier.makeServiceError(
            statusCode: status,
            code: code,
            message: nil,
            retryAfter: retryAfter,
            claims: claims
        )
    }
}

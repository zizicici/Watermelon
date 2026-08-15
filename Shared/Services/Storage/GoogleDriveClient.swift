import CryptoKit
import Foundation

nonisolated private final class GoogleDriveMonotonicProgressReporter: @unchecked Sendable {
    private let lock = NSLock()
    private let callback: ((Double) -> Void)?
    private var highest = 0.0

    init(callback: ((Double) -> Void)?) {
        self.callback = callback
    }

    func report(_ value: Double) {
        let next = lock.withLock { () -> Double? in
            let clamped = min(1, max(0, value))
            guard clamped > highest else { return nil }
            highest = clamped
            return clamped
        }
        if let next { callback?(next) }
    }
}

final actor GoogleDriveClient: RemoteStorageClientProtocol,
    RemoteUploadCollisionPolicyClient,
    RemoteUploadOutcomeVerificationClient,
    RemoteLeasedNamespaceClient {
    nonisolated let allowsUnattendedLeaseConfidence = true

    struct Config: Sendable {
        let connection: CanonicalGoogleDriveConnection
    }

    private struct LocatedLockRecord: Sendable {
        let file: GoogleDriveFile
        let record: GoogleDriveLockRecord
    }

    private enum LockCursor: Sendable {
        case available(slotID: String, sequence: UInt64)
        case active(LocatedLockRecord)
    }

    private struct LockEnvelope: Sendable {
        let file: GoogleDriveFile
        let sequence: UInt64
        let nextSlotID: String
        let releaseMarkerID: String
    }

    private enum FixedIDUploadRecovery {
        case committed(GoogleDriveFile)
        case unchanged
        case unresolved
    }

    nonisolated private static let multipartUploadThreshold: Int64 = 5 * 1024 * 1024
    nonisolated private static let ordinaryFileIDBatchSize = 64
    nonisolated private static let resumableChunkSize = 8 * 1024 * 1024
    nonisolated private static let resumableRecoveryLimit = 3
    nonisolated private static let controlDirectorySettleAttempts = 6
    nonisolated private static let controlDirectorySettleDelay = Duration.milliseconds(500)
    private let config: Config
    private let credential: GoogleDriveCredentialBlob
    private let sharedState: GoogleDriveSharedState
    private let writeSessionKey: GoogleDriveWriteSessionKey
    private let transport: GoogleDriveTransport
    private var pathCache: [String: GoogleDriveFile] = [:]
    private var ordinaryFileIDPool: [String] = []
    private var verifiedLockRecordIDs: [String: String] = [:]
    private var ownedLeaseGeneration: UUID?
    private var observedLeaseGeneration: UUID?

    init(
        config: Config,
        credential: GoogleDriveCredentialBlob,
        tokenProvider: any GoogleDriveAccessTokenProviding,
        sharedState: GoogleDriveSharedState = GoogleDriveSharedState(),
        sessionConfiguration: URLSessionConfiguration? = nil
    ) {
        self.config = config
        self.credential = credential
        self.sharedState = sharedState
        writeSessionKey = GoogleDriveWriteSessionKey(
            accountSubject: config.connection.accountSubject,
            rootFolderID: config.connection.rootFolderID
        )
        transport = GoogleDriveTransport(
            clientID: config.connection.clientID,
            credential: credential,
            tokenProvider: tokenProvider,
            sharedState: sharedState,
            sessionConfiguration: sessionConfiguration
        )
    }

    nonisolated var shouldDownloadRemoteFileForNameCollision: Bool { false }

    nonisolated func shouldSetModificationDate() -> Bool { false }

    nonisolated func trustsLeaseConfidenceForDestructiveWrite() -> Bool { true }

    nonisolated func supportsLegacyV1Migration() -> Bool { false }

    nonisolated func shouldLimitUploadRetries(for error: Error) -> Bool {
        GoogleDriveErrorClassifier.isNameCollision(error) || remoteStorageIsNameCollision(error)
    }

    nonisolated func cancelActiveOperationsForAbandonment() {
        transport.cancelAll()
    }

    func connect() async throws {
        try validateIdentity()
        guard let root = try await transport.file(id: config.connection.rootFolderID),
              root.watermelonLockRootSlotID == config.connection.lockRootSlotID else {
            throw GoogleDriveAuthenticationError.accountMismatch
        }
        let generation = await synchronizeLeasedNamespaceGeneration()
        pathCache["/"] = root
        if let generation {
            await sharedState.writeSession.observe(
                root,
                path: "/",
                key: writeSessionKey,
                generation: generation
            )
        }
    }

    func disconnect() async {
        pathCache.removeAll()
        ordinaryFileIDPool.removeAll()
        observedLeaseGeneration = nil
    }

    func beginLeasedNamespaceSession() async {
        guard ownedLeaseGeneration == nil else { return }
        let generation = await sharedState.writeSession.begin(
            for: writeSessionKey,
            root: pathCache["/"]
        )
        ownedLeaseGeneration = generation
        observedLeaseGeneration = generation
        retainOnlyCachedRoot()
    }

    func endLeasedNamespaceSession() async {
        guard let generation = ownedLeaseGeneration else { return }
        ownedLeaseGeneration = nil
        await sharedState.writeSession.end(for: writeSessionKey, generation: generation)
        if observedLeaseGeneration == generation {
            observedLeaseGeneration = nil
            retainOnlyCachedRoot()
        }
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL.appendingPathComponent("about"),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [URLQueryItem(name: "fields", value: "storageQuota(limit,usage)")]
        let (data, _) = try await transport.data(for: URLRequest(url: components.url!))
        let about = try GoogleDriveJSON.decodeResponse(GoogleDriveAbout.self, from: data)
        let total = about.storageQuota?.limit.flatMap(Int64.init)
        let used = about.storageQuota?.usage.flatMap(Int64.init)
        let available: Int64?
        if let total, let used {
            available = max(0, total - used)
        } else {
            available = nil
        }
        return RemoteStorageCapacity(availableBytes: available, totalBytes: total)
    }

    func verifyWriteAccess() async throws {
        let scope = "google-drive-setup-v1"
        let writerID = Self.setupWriterID(
            clientID: config.connection.clientID,
            accountSubject: config.connection.accountSubject,
            rootFolderID: config.connection.rootFolderID
        )
        guard let lockPath = RepoLayoutLite.lockPath(basePath: "/", writerID: writerID) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        guard let lock = WriteLockService(
            basePath: "/",
            writerID: writerID,
            client: self,
            allowsFreshOwnLockTakeover: true,
            freshOwnLockTakeoverScopes: [scope],
            ownLockTakeoverScope: scope
        ) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        switch await lock.acquire(mode: .foreground) {
        case .acquired:
            let released = try await Task {
                await lock.release()
                return try await self.metadata(path: lockPath) == nil
            }.value
            guard released else { throw RemoteStorageClientError.unavailable }
            try Task.checkCancellation()
        case .blocked, .blockedByOwnLock, .skipped, .skippedByOwnLock:
            throw RemoteStorageClientError.unavailable
        case .faulted:
            throw RemoteStorageClientError.unavailable
        }
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let normalized = try Self.canonicalPath(path)
        let generation = await synchronizeLeasedNamespaceGeneration()
        let folder: GoogleDriveFile
        if let generation, !Self.requiresDistributedDirectorySettle(normalized) {
            switch await sharedState.writeSession.lookup(
                path: normalized,
                key: writeSessionKey,
                generation: generation
            ) {
            case .file(let cached):
                folder = cached
            case .missing:
                throw Self.notFoundError()
            case .busy:
                throw RemoteStorageClientError.unavailable
            case .unavailable:
                folder = try await resolveFresh(path: normalized)
            }
        } else {
            folder = try await resolveFresh(path: normalized)
        }
        guard folder.mimeType == GoogleDriveConstants.folderMIMEType else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        var files = try await listChildren(parentID: folder.id)
        if Self.isLocksDirectory(normalized) {
            let cursor = try await lockCursor(files: files)
            files.removeAll { file in
                let role = file.appProperties?[GoogleDriveConstants.rootRoleKey]
                return role == GoogleDriveConstants.lockRecordRole || role == GoogleDriveConstants.lockReleaseRole
            }
            if case .active(let active) = cursor,
               Self.parentPath(of: active.record.virtualPath) == normalized {
                verifiedLockRecordIDs[active.record.virtualPath] = active.file.id
                files.append(Self.virtualLockFile(active))
            }
        }
        let namedFiles = try files.map { file -> (String, GoogleDriveFile) in
            guard let name = file.name,
                  RemotePathBuilder.isSafePathComponent(name) else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return (name, file)
        }
        let grouped = Dictionary(grouping: namedFiles, by: { $0.0 })
        guard grouped.values.allSatisfy({ $0.count == 1 }) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        if let generation, !Self.requiresDistributedDirectorySettle(normalized) {
            try await sharedState.writeSession.install(
                path: normalized,
                folder: folder,
                childrenByName: Dictionary(uniqueKeysWithValues: namedFiles),
                key: writeSessionKey,
                generation: generation
            )
        } else {
            invalidateDirectChildren(of: normalized)
        }
        return namedFiles.map { name, file in
            let childPath = Self.appending(name, to: normalized)
            if generation == nil || Self.requiresDistributedDirectorySettle(normalized) {
                pathCache[childPath] = file
            }
            return Self.remoteEntry(file, path: childPath)
        }
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let normalized = try Self.canonicalPath(path)
        if Self.isVirtualLockPath(normalized) {
            guard case .active(let active) = try await lockCursor(),
                  active.record.virtualPath == normalized else { return nil }
            verifiedLockRecordIDs[normalized] = active.file.id
            return Self.remoteEntry(Self.virtualLockFile(active), path: normalized)
        }
        if let generation = await synchronizeLeasedNamespaceGeneration() {
            switch await sharedState.writeSession.lookup(
                path: normalized,
                key: writeSessionKey,
                generation: generation
            ) {
            case .file(let cached):
                return Self.remoteEntry(cached, path: normalized)
            case .missing:
                return nil
            case .busy:
                throw RemoteStorageClientError.unavailable
            case .unavailable:
                break
            }
        }
        do {
            let item = try await resolveFresh(path: normalized)
            return Self.remoteEntry(item, path: normalized)
        } catch {
            if GoogleDriveErrorClassifier.isNotFound(error) { return nil }
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
        let normalized = try Self.canonicalPath(remotePath)
        let operation = {
            if Self.isVirtualLockPath(normalized) {
                try await self.uploadVirtualLock(localURL: localURL, path: normalized, mode: mode)
            } else {
                try await self.performUpload(
                    localURL: localURL,
                    remotePath: normalized,
                    mode: mode,
                    onProgress: onProgress
                )
            }
        }
        if respectTaskCancellation {
            try Task.checkCancellation()
            try await operation()
            return
        }
        let task = Task { try await operation() }
        try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {}
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        let normalized = try Self.canonicalPath(path)
        guard Self.isVirtualLockPath(normalized) else { return }
        guard case .active(let active) = try await lockCursor(),
              active.record.virtualPath == normalized else {
            throw Self.notFoundError()
        }
        try await updateMetadata(id: active.file.id, values: ["modifiedTime": Self.dateString(date)])
    }

    func download(remotePath: String, localURL: URL) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        let normalized = try Self.canonicalPath(remotePath)
        if Self.isVirtualLockPath(normalized) {
            guard case .active(let active) = try await lockCursor(),
                  active.record.virtualPath == normalized else {
                throw Self.notFoundError()
            }
            verifiedLockRecordIDs[normalized] = active.file.id
            try Self.replaceLocalFile(with: active.record.lockBody, at: localURL)
            onProgress?(1)
            return
        }
        let generation = await synchronizeLeasedNamespaceGeneration()
        let leasedItem: GoogleDriveFile?
        if let generation {
            switch await sharedState.writeSession.lookup(
                path: normalized,
                key: writeSessionKey,
                generation: generation
            ) {
            case .file(let cached):
                leasedItem = cached
            case .missing:
                throw Self.notFoundError()
            case .busy:
                throw RemoteStorageClientError.unavailable
            case .unavailable:
                leasedItem = nil
            }
        } else {
            leasedItem = nil
        }
        let item: GoogleDriveFile
        if let leasedItem {
            item = leasedItem
        } else {
            let parent = try await resolve(path: Self.parentPath(of: normalized))
            let matches = try await exactChildren(parentID: parent.id, name: Self.lastComponent(of: normalized))
            guard matches.count <= 1 else { throw RemoteStorageClientError.invalidConfiguration }
            guard let current = matches.first else { throw Self.notFoundError() }
            item = current
        }
        guard item.mimeType != GoogleDriveConstants.folderMIMEType else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        do {
            try await download(item: item, localURL: localURL, onProgress: onProgress)
        } catch {
            guard GoogleDriveErrorClassifier.isNotFound(error) else { throw error }
            invalidate(path: normalized)
            let fresh = try await resolveFresh(path: normalized)
            guard fresh.mimeType != GoogleDriveConstants.folderMIMEType else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            try await download(item: fresh, localURL: localURL, onProgress: onProgress)
        }
    }

    private func download(
        item: GoogleDriveFile,
        localURL: URL,
        onProgress: ((Double) -> Void)?
    ) async throws {
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL
                .appendingPathComponent("files")
                .appendingPathComponent(item.id),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [URLQueryItem(name: "alt", value: "media")]
        let (temporaryURL, _) = try await transport.download(
            for: URLRequest(url: components.url!),
            onProgress: onProgress
        )
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        try Self.installDownloadedFile(temporaryURL, at: localURL)
        onProgress?(1)
    }

    func exists(path: String) async throws -> Bool {
        try await metadata(path: path) != nil
    }

    func remoteFileMatches(localURL: URL, remotePath: String) async throws -> Bool {
        let normalized = try Self.canonicalPath(remotePath)
        guard normalized != "/" else { return false }
        let values = try localURL.resourceValues(forKeys: [.fileSizeKey, .isRegularFileKey])
        guard values.isRegularFile == true, let fileSize = values.fileSize else { return false }
        let item = try await resolveFresh(path: normalized)
        guard item.mimeType != GoogleDriveConstants.folderMIMEType,
              item.size.flatMap(Int64.init) == Int64(fileSize) else { return false }
        return try Self.fileContentsMatch(localURL: localURL, remote: item)
    }

    func delete(path: String) async throws {
        let normalized = try Self.canonicalPath(path)
        guard normalized != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        if Self.isVirtualLockPath(normalized) {
            try await releaseVirtualLock(path: normalized)
            return
        }
        let generation = await synchronizeLeasedNamespaceGeneration()
        let item: GoogleDriveFile
        if let generation {
            switch await sharedState.writeSession.lookup(
                path: normalized,
                key: writeSessionKey,
                generation: generation
            ) {
            case .file(let cached):
                item = cached
            case .missing:
                throw Self.notFoundError()
            case .busy:
                throw RemoteStorageClientError.unavailable
            case .unavailable:
                item = try await resolveFresh(path: normalized)
            }
        } else {
            item = try await resolveFresh(path: normalized)
        }
        do {
            try await transport.deleteFile(id: item.id)
        } catch {
            let deleteError = error
            if !GoogleDriveErrorClassifier.isNotFound(deleteError) {
                guard GoogleDriveErrorClassifier.isMutationOutcomeUnknown(deleteError) else {
                    throw deleteError
                }
                let isAbsent: Bool
                do {
                    isAbsent = try await transport.file(id: item.id) == nil
                } catch {
                    throw deleteError
                }
                guard isAbsent else { throw deleteError }
            }
        }
        invalidate(path: normalized)
        if let generation {
            await sharedState.writeSession.remove(
                path: normalized,
                key: writeSessionKey,
                generation: generation
            )
        }
    }

    func createDirectory(path: String) async throws {
        let normalized = try Self.canonicalPath(path)
        if normalized == "/" { return }
        let generation = await synchronizeLeasedNamespaceGeneration()
        if let generation, !Self.requiresDistributedDirectorySettle(normalized) {
            switch await sharedState.writeSession.lookup(
                path: normalized,
                key: writeSessionKey,
                generation: generation
            ) {
            case .file(let cached):
                guard cached.mimeType == GoogleDriveConstants.folderMIMEType else {
                    throw remoteStorageNameCollisionError(path: normalized)
                }
                if !(await sharedState.writeSession.isDirectoryLoaded(
                    path: normalized,
                    key: writeSessionKey,
                    generation: generation
                )) {
                    _ = try await list(path: normalized)
                }
                return
            case .busy:
                throw RemoteStorageClientError.unavailable
            case .missing, .unavailable:
                break
            }
        }
        let key = GoogleDriveDirectoryMutationGate.Key(
            accountSubject: config.connection.accountSubject,
            rootFolderID: config.connection.rootFolderID
        )
        try await sharedState.directoryMutationGate.acquire(key)
        do {
            try Task.checkCancellation()
            try await createDirectoryUncoordinated(path: normalized)
            if let generation,
               !Self.requiresDistributedDirectorySettle(normalized),
               !(await sharedState.writeSession.isDirectoryLoaded(
                path: normalized,
                key: writeSessionKey,
                generation: generation
               )) {
                _ = try await list(path: normalized)
            }
            await sharedState.directoryMutationGate.release(key)
        } catch {
            await sharedState.directoryMutationGate.release(key)
            throw error
        }
    }

    private func createDirectoryUncoordinated(path normalized: String) async throws {
        var currentPath = "/"
        let generation = Self.requiresDistributedDirectorySettle(normalized)
            ? nil
            : observedLeaseGeneration
        var parent: GoogleDriveFile
        if let generation {
            switch await sharedState.writeSession.lookup(
                path: "/",
                key: writeSessionKey,
                generation: generation
            ) {
            case .file(let root):
                parent = root
            case .busy:
                throw RemoteStorageClientError.unavailable
            case .missing, .unavailable:
                parent = try await resolveFresh(path: "/")
            }
        } else {
            parent = try await resolveFresh(path: "/")
        }
        for component in try Self.pathComponents(normalized) {
            currentPath = Self.appending(component, to: currentPath)
            var knownMissing = false
            if let generation {
                switch await sharedState.writeSession.lookup(
                    path: currentPath,
                    key: writeSessionKey,
                    generation: generation
                ) {
                case .file(let cached):
                    guard cached.mimeType == GoogleDriveConstants.folderMIMEType else {
                        throw remoteStorageNameCollisionError(path: currentPath)
                    }
                    parent = cached
                    continue
                case .missing:
                    knownMissing = true
                case .busy:
                    throw RemoteStorageClientError.unavailable
                case .unavailable:
                    break
                }
            }
            let matches = knownMissing
                ? []
                : try await exactChildren(parentID: parent.id, name: component)
            if matches.count > 1, Self.requiresDistributedDirectorySettle(currentPath) {
                guard let oldest = matches.min(by: googleDriveFolderPrecedes) else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                let winner = try await confirmCreatedFolder(
                    oldest,
                    name: component,
                    parentID: parent.id,
                    path: currentPath
                )
                await recordObservedFile(winner, path: currentPath)
                parent = winner
                continue
            }
            if matches.count > 1 { throw RemoteStorageClientError.invalidConfiguration }
            if let existing = matches.first {
                guard existing.mimeType == GoogleDriveConstants.folderMIMEType else {
                    throw remoteStorageNameCollisionError(path: currentPath)
                }
                await recordObservedFile(existing, path: currentPath)
                parent = existing
                continue
            }
            let created = try await createFolder(
                name: component,
                parentID: parent.id
            )
            let winner = try await confirmCreatedFolder(
                created,
                name: component,
                parentID: parent.id,
                path: currentPath
            )
            await recordObservedFile(winner, path: currentPath)
            if let generation {
                try await sharedState.writeSession.install(
                    path: currentPath,
                    folder: winner,
                    childrenByName: [:],
                    key: writeSessionKey,
                    generation: generation
                )
            }
            parent = winner
        }
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        let source = try Self.canonicalPath(sourcePath)
        let destination = try Self.canonicalPath(destinationPath)
        guard source != "/", destination != "/",
              !Self.isVirtualLockPath(source), !Self.isVirtualLockPath(destination) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let generation = await synchronizeLeasedNamespaceGeneration()
        let sourceParentPath = Self.parentPath(of: source)
        let destinationParentPath = Self.parentPath(of: destination)
        let destinationName = Self.lastComponent(of: destination)
        let sourceItem: GoogleDriveFile
        let destinationParent: GoogleDriveFile
        var usedLeasedSnapshot = false
        if sourceParentPath == destinationParentPath,
           !Self.isLocksDirectory(sourceParentPath),
           let generation {
            let sourceLookup = await sharedState.writeSession.lookup(
                path: source,
                key: writeSessionKey,
                generation: generation
            )
            let destinationLookup = await sharedState.writeSession.lookup(
                path: destination,
                key: writeSessionKey,
                generation: generation
            )
            let parentLookup = await sharedState.writeSession.lookup(
                path: sourceParentPath,
                key: writeSessionKey,
                generation: generation
            )
            switch (sourceLookup, destinationLookup, parentLookup) {
            case (.file(let sourceFile), .missing, .file(let parentFile)):
                sourceItem = sourceFile
                destinationParent = parentFile
                usedLeasedSnapshot = true
            case (.missing, _, _):
                throw Self.notFoundError()
            case (_, .file, _):
                throw remoteStorageNameCollisionError(path: destination)
            case (.busy, _, _), (_, .busy, _), (_, _, .busy):
                throw RemoteStorageClientError.unavailable
            default:
                sourceItem = try await resolveFresh(path: source)
                destinationParent = try await resolveFresh(path: destinationParentPath)
            }
        } else {
            sourceItem = try await resolveFresh(path: source)
            destinationParent = try await resolveFresh(path: destinationParentPath)
        }
        guard let sourceParents = sourceItem.parents, sourceParents.count == 1,
              let sourceParentID = sourceParents.first else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        if !usedLeasedSnapshot {
            guard try await exactChildren(parentID: destinationParent.id, name: destinationName).isEmpty else {
                throw remoteStorageNameCollisionError(path: destination)
            }
        }
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL
                .appendingPathComponent("files")
                .appendingPathComponent(sourceItem.id),
            resolvingAgainstBaseURL: false
        )!
        var queryItems = [URLQueryItem(name: "fields", value: GoogleDriveConstants.fileFields)]
        if sourceParentID != destinationParent.id {
            queryItems.append(URLQueryItem(name: "addParents", value: destinationParent.id))
            queryItems.append(URLQueryItem(name: "removeParents", value: sourceParentID))
        }
        components.queryItems = queryItems
        var request = URLRequest(url: components.url!)
        request.httpMethod = "PATCH"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try Self.jsonData(["name": destinationName])
        invalidate(path: source)
        invalidate(path: destination)
        do {
            let (data, _) = try await transport.data(for: request)
            let moved = try GoogleDriveJSON.decodeResponse(GoogleDriveFile.self, from: data)
            guard moved.id == sourceItem.id,
                  moved.name == destinationName,
                  moved.parents == [destinationParent.id],
                  moved.trashed != true else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            if let generation {
                await sharedState.writeSession.applyMove(
                    moved,
                    from: source,
                    to: destination,
                    key: writeSessionKey,
                    generation: generation
                )
            } else {
                pathCache[destination] = moved
            }
        } catch {
            let moveError = error
            guard GoogleDriveErrorClassifier.isMutationOutcomeUnknown(moveError) else {
                throw moveError
            }
            if let moved = try? await transport.file(id: sourceItem.id),
               moved.name == destinationName,
               moved.parents == [destinationParent.id],
               moved.trashed != true {
                if let generation {
                    await sharedState.writeSession.applyMove(
                        moved,
                        from: source,
                        to: destination,
                        key: writeSessionKey,
                        generation: generation
                    )
                } else {
                    pathCache[destination] = moved
                }
                return
            }
            throw moveError
        }
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        let source = try Self.canonicalPath(sourcePath)
        let destination = try Self.canonicalPath(destinationPath)
        guard source != "/", destination != "/",
              !Self.isVirtualLockPath(source), !Self.isVirtualLockPath(destination) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        _ = await synchronizeLeasedNamespaceGeneration()
        let sourceItem = try await resolveFresh(path: source)
        let parentPath = Self.parentPath(of: destination)
        let name = Self.lastComponent(of: destination)
        let parent = try await resolveFresh(path: parentPath)
        guard sourceItem.mimeType != GoogleDriveConstants.folderMIMEType else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let destinationMatches = try await exactChildren(parentID: parent.id, name: name)
        if !destinationMatches.isEmpty {
            if destinationMatches.count == 1,
               Self.isValidCopyResult(
                destinationMatches[0],
                itemID: destinationMatches[0].id,
                name: name,
                parentID: parent.id,
                source: sourceItem
               ) {
                await recordObservedFile(destinationMatches[0], path: destination)
                return
            }
            throw remoteStorageNameCollisionError(path: destination)
        }
        let itemID = try await nextOrdinaryFileID()
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL
                .appendingPathComponent("files")
                .appendingPathComponent(sourceItem.id)
                .appendingPathComponent("copy"),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [URLQueryItem(name: "fields", value: GoogleDriveConstants.fileFields)]
        var request = URLRequest(url: components.url!)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try Self.jsonData(["id": itemID, "name": name, "parents": [parent.id]])
        let copied: GoogleDriveFile
        do {
            let (data, _) = try await transport.data(for: request, expectedStatusCodes: [200])
            copied = try GoogleDriveJSON.decodeResponse(GoogleDriveFile.self, from: data)
        } catch {
            let copyError = error
            guard GoogleDriveErrorClassifier.isMutationOutcomeUnknown(copyError)
                || GoogleDriveErrorClassifier.isNameCollision(copyError) else {
                throw copyError
            }
            do {
                guard let recovered = try await transport.file(id: itemID) else { throw copyError }
                copied = recovered
            } catch let recoveryError as RemoteStorageClientError {
                if case .invalidConfiguration = recoveryError { throw recoveryError }
                throw copyError
            } catch {
                throw copyError
            }
        }
        guard Self.isValidCopyResult(
            copied,
            itemID: itemID,
            name: name,
            parentID: parent.id,
            source: sourceItem
        ) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        await recordObservedFile(copied, path: destination)
    }

    private func performUpload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        onProgress: ((Double) -> Void)?
    ) async throws {
        guard remotePath != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        let values = try localURL.resourceValues(forKeys: [
            .fileSizeKey, .isRegularFileKey, .contentModificationDateKey
        ])
        guard values.isRegularFile == true, let fileSize = values.fileSize else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let parentPath = Self.parentPath(of: remotePath)
        let name = Self.lastComponent(of: remotePath)
        let generation = await synchronizeLeasedNamespaceGeneration()
        var uploadTicket: GoogleDriveWriteUploadTicket?
        var resumedBoundUpload: GoogleDriveBoundUpload?
        if let generation {
            var preparation = await sharedState.writeSession.prepareUpload(
                parentPath: parentPath,
                name: name,
                mode: mode,
                key: writeSessionKey,
                generation: generation
            )
            if case .recover(let uncertain) = preparation {
                guard uncertain.expectedSize == Int64(fileSize) else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                switch try await recoverFixedIDUpload(
                    itemID: uncertain.itemID,
                    name: uncertain.ticket.name,
                    parentID: uncertain.ticket.parent.id,
                    size: uncertain.expectedSize,
                    previous: uncertain.ticket.existing,
                    localURL: localURL
                ) {
                case .committed(let recovered):
                    guard await sharedState.writeSession.completeUpload(
                        uncertain,
                        file: recovered,
                        key: writeSessionKey
                    ) else {
                        throw RemoteStorageClientError.unavailable
                    }
                    onProgress?(1)
                    return
                case .unchanged:
                    await sharedState.writeSession.cancelUpload(
                        uncertain.ticket,
                        key: writeSessionKey
                    )
                    preparation = await sharedState.writeSession.prepareUpload(
                        parentPath: parentPath,
                        name: name,
                        mode: mode,
                        key: writeSessionKey,
                        generation: generation
                    )
                case .unresolved:
                    uploadTicket = uncertain.ticket
                    resumedBoundUpload = uncertain
                }
            }
            if resumedBoundUpload == nil {
                switch preparation {
                case .unavailable:
                    break
                case .busy, .recover:
                    throw RemoteStorageClientError.unavailable
                case .occupied:
                    throw remoteStorageNameCollisionError(path: remotePath)
                case .ready(let ticket):
                    uploadTicket = ticket
                }
            }
        }

        let parent: GoogleDriveFile
        let existing: GoogleDriveFile?
        if let uploadTicket {
            parent = uploadTicket.parent
            existing = uploadTicket.existing
        } else {
            parent = try await resolveFresh(path: parentPath)
            let matches = try await exactChildren(parentID: parent.id, name: name)
            guard matches.count <= 1 else { throw RemoteStorageClientError.invalidConfiguration }
            existing = matches.first
            if mode == .createIfAbsent, existing != nil {
                throw remoteStorageNameCollisionError(path: remotePath)
            }
        }

        let metadata: [String: Any]
        let itemID: String
        let isCreate: Bool
        if let resumedBoundUpload {
            itemID = resumedBoundUpload.itemID
            isCreate = existing == nil
            metadata = Self.uploadMetadata(
                id: isCreate ? itemID : nil,
                name: name,
                parentID: isCreate ? parent.id : nil,
                modificationDate: values.contentModificationDate,
                appProperties: nil
            )
        } else if let existing {
            guard existing.mimeType != GoogleDriveConstants.folderMIMEType else {
                throw remoteStorageNameCollisionError(path: remotePath)
            }
            itemID = existing.id
            isCreate = false
            metadata = Self.uploadMetadata(
                id: nil,
                name: name,
                parentID: nil,
                modificationDate: values.contentModificationDate,
                appProperties: nil
            )
        } else {
            do {
                itemID = try await nextOrdinaryFileID()
            } catch {
                if let uploadTicket {
                    await sharedState.writeSession.cancelUpload(uploadTicket, key: writeSessionKey)
                }
                throw error
            }
            isCreate = true
            metadata = Self.uploadMetadata(
                id: itemID,
                name: name,
                parentID: parent.id,
                modificationDate: values.contentModificationDate,
                appProperties: nil
            )
        }
        let boundUpload: GoogleDriveBoundUpload?
        if let resumedBoundUpload {
            boundUpload = resumedBoundUpload
        } else if let uploadTicket {
            guard let bound = await sharedState.writeSession.bindUpload(
                uploadTicket,
                itemID: itemID,
                expectedSize: Int64(fileSize),
                key: writeSessionKey
            ) else {
                throw RemoteStorageClientError.unavailable
            }
            boundUpload = bound
        } else {
            boundUpload = nil
        }

        let uploaded: GoogleDriveFile
        do {
            if Int64(fileSize) <= Self.multipartUploadThreshold {
                let data = try Data(contentsOf: localURL, options: .mappedIfSafe)
                uploaded = try await multipartUpload(
                    itemID: itemID,
                    isCreate: isCreate,
                    metadata: metadata,
                    content: data,
                    mimeType: "application/octet-stream",
                    expectedStatusCodes: isCreate ? [200, 201] : [200],
                    onProgress: onProgress
                )
            } else {
                uploaded = try await resumableUpload(
                    localURL: localURL,
                    size: Int64(fileSize),
                    itemID: itemID,
                    isCreate: isCreate,
                    metadata: metadata,
                    onProgress: onProgress
                )
            }
        } catch {
            if let boundUpload {
                let uploadError = error
                let shouldRecover = GoogleDriveErrorClassifier.isMutationOutcomeUnknown(uploadError)
                    || GoogleDriveErrorClassifier.isNameCollision(uploadError)
                guard shouldRecover else {
                    await sharedState.writeSession.cancelUpload(
                        boundUpload.ticket,
                        key: writeSessionKey
                    )
                    throw uploadError
                }
                let expectedMD5 = Task.isCancelled || Self.isCancellationError(uploadError)
                    ? nil
                    : try? Self.md5Hex(of: localURL)
                await sharedState.writeSession.markUploadUncertain(
                    boundUpload,
                    expectedMD5: expectedMD5,
                    key: writeSessionKey
                )
                if Task.isCancelled || Self.isCancellationError(uploadError) {
                    throw CancellationError()
                }
                do {
                    switch try await recoverFixedIDUpload(
                        itemID: boundUpload.itemID,
                        name: boundUpload.ticket.name,
                        parentID: boundUpload.ticket.parent.id,
                        size: boundUpload.expectedSize,
                        previous: boundUpload.ticket.existing,
                        localURL: localURL,
                        expectedMD5: expectedMD5
                    ) {
                    case .committed(let recovered):
                        guard await sharedState.writeSession.completeUpload(
                            boundUpload,
                            file: recovered,
                            key: writeSessionKey
                        ) else {
                            throw RemoteStorageClientError.unavailable
                        }
                        onProgress?(1)
                        return
                    case .unchanged:
                        await sharedState.writeSession.cancelUpload(
                            boundUpload.ticket,
                            key: writeSessionKey
                        )
                    case .unresolved:
                        break
                    }
                } catch let recoveryError as RemoteStorageClientError {
                    if case .invalidConfiguration = recoveryError { throw recoveryError }
                    throw uploadError
                } catch {
                    throw uploadError
                }
                if GoogleDriveErrorClassifier.isNameCollision(uploadError) {
                    throw remoteStorageNameCollisionError(path: remotePath)
                }
                throw uploadError
            }
            let uploadError = error
            if Task.isCancelled || Self.isCancellationError(uploadError) {
                throw CancellationError()
            }
            if GoogleDriveErrorClassifier.isMutationOutcomeUnknown(uploadError) {
                do {
                    switch try await recoverFixedIDUpload(
                        itemID: itemID,
                        name: name,
                        parentID: parent.id,
                        size: Int64(fileSize),
                        previous: existing,
                        localURL: localURL
                    ) {
                    case .committed(let recovered):
                        await recordObservedFile(recovered, path: remotePath)
                        onProgress?(1)
                        return
                    case .unchanged, .unresolved:
                        break
                    }
                } catch let recoveryError as RemoteStorageClientError {
                    if case .invalidConfiguration = recoveryError { throw recoveryError }
                    throw uploadError
                } catch {
                    throw uploadError
                }
            }
            if mode == .createIfAbsent, GoogleDriveErrorClassifier.isNameCollision(error) {
                let current = try await exactChildren(parentID: parent.id, name: name)
                if current.count == 1,
                   current[0].id == itemID,
                   Self.isValidUploadResult(
                    current[0],
                    itemID: itemID,
                    name: name,
                    parentID: parent.id,
                    size: Int64(fileSize)
                   ),
                   try Self.fileContentsMatch(localURL: localURL, remote: current[0]) {
                    await recordObservedFile(current[0], path: remotePath)
                    return
                }
                throw remoteStorageNameCollisionError(path: remotePath)
            }
            throw error
        }

        if let boundUpload {
            guard Self.isValidUploadResult(
                uploaded,
                itemID: itemID,
                name: name,
                parentID: parent.id,
                size: Int64(fileSize)
            ) else {
                await sharedState.writeSession.markUploadUncertain(
                    boundUpload,
                    expectedMD5: try? Self.md5Hex(of: localURL),
                    key: writeSessionKey
                )
                throw GoogleDriveAuthenticationError.invalidResponse
            }
            guard await sharedState.writeSession.completeUpload(
                boundUpload,
                file: uploaded,
                key: writeSessionKey
            ) else {
                throw RemoteStorageClientError.unavailable
            }
            onProgress?(1)
            return
        }

        pathCache.removeValue(forKey: remotePath)
        let current: [GoogleDriveFile]
        do {
            current = try await exactChildren(parentID: parent.id, name: name)
        } catch {
            let confirmationError = error
            do {
                switch try await recoverFixedIDUpload(
                    itemID: itemID,
                    name: name,
                    parentID: parent.id,
                    size: Int64(fileSize),
                    previous: existing,
                    localURL: localURL
                ) {
                case .committed(let recovered):
                    await recordObservedFile(recovered, path: remotePath)
                    onProgress?(1)
                    return
                case .unchanged, .unresolved:
                    break
                }
            } catch let recoveryError as RemoteStorageClientError {
                if case .invalidConfiguration = recoveryError { throw recoveryError }
            } catch {}
            throw confirmationError
        }
        guard current.count == 1, current[0].id == itemID else {
            if isCreate {
                try? await transport.deleteFile(id: itemID)
            }
            throw remoteStorageNameCollisionError(path: remotePath)
        }
        await recordObservedFile(current[0], path: remotePath)
        onProgress?(1)
    }

    private func recoverFixedIDUpload(
        itemID: String,
        name: String,
        parentID: String,
        size: Int64,
        previous: GoogleDriveFile?,
        localURL: URL,
        expectedMD5: String? = nil
    ) async throws -> FixedIDUploadRecovery {
        guard let recovered = try await transport.file(id: itemID) else { return .unresolved }
        if Self.isValidUploadResult(
            recovered,
            itemID: itemID,
            name: name,
            parentID: parentID,
            size: size
        ) {
            let contentsMatch: Bool
            if let expectedMD5, let remoteMD5 = recovered.md5Checksum {
                contentsMatch = remoteMD5.caseInsensitiveCompare(expectedMD5) == .orderedSame
            } else {
                contentsMatch = try Self.fileContentsMatch(localURL: localURL, remote: recovered)
            }
            if contentsMatch { return .committed(recovered) }
        }
        if let previous, recovered.hasSameContentsAndBinding(as: previous) {
            return .unchanged
        }
        throw RemoteStorageClientError.invalidConfiguration
    }

    private func uploadVirtualLock(localURL: URL, path: String, mode: RemoteUploadMode) async throws {
        let body = try Data(contentsOf: localURL)
        switch mode {
        case .createIfAbsent:
            guard case .available(let slotID, let sequence) = try await lockCursor() else {
                throw remoteStorageNameCollisionError(path: path)
            }
            let generated = try await transport.generateFileIDs(count: 2)
            let record = GoogleDriveLockRecord(
                sequence: sequence,
                virtualPath: path,
                lockBody: body,
                nextSlotID: generated[0],
                releaseMarkerID: generated[1]
            )
            let parent = try await resolve(path: Self.parentPath(of: path))
            let metadata = Self.uploadMetadata(
                id: slotID,
                name: ".gdrive-lock-record-\(sequence)",
                parentID: parent.id,
                modificationDate: Date(),
                appProperties: [
                    GoogleDriveConstants.rootRoleKey: GoogleDriveConstants.lockRecordRole,
                    GoogleDriveConstants.lockSequenceKey: String(sequence),
                    GoogleDriveConstants.lockNextSlotKey: record.nextSlotID,
                    GoogleDriveConstants.lockReleaseMarkerKey: record.releaseMarkerID
                ]
            )
            do {
                let created = try await multipartUpload(
                    itemID: slotID,
                    isCreate: true,
                    metadata: metadata,
                    content: try GoogleDriveJSON.encode(record),
                    mimeType: "application/json",
                    expectedStatusCodes: [200, 201],
                    onProgress: nil
                )
                verifiedLockRecordIDs[path] = created.id
            } catch {
                let createError = error
                guard GoogleDriveErrorClassifier.isMutationOutcomeUnknown(createError)
                    || GoogleDriveErrorClassifier.isNameCollision(createError) else {
                    throw createError
                }
                if let winner = try? await lockRecord(id: slotID), winner.record == record {
                    verifiedLockRecordIDs[path] = winner.file.id
                    return
                }
                guard GoogleDriveErrorClassifier.isNameCollision(createError) else {
                    throw createError
                }
                throw remoteStorageNameCollisionError(path: path)
            }
        case .replace:
            guard case .active(let active) = try await lockCursor(),
                  active.record.virtualPath == path,
                  googleDriveLockBodiesHaveSameIdentity(active.record.lockBody, body) else {
                throw remoteStorageNameCollisionError(path: path)
            }
            let replacement = GoogleDriveLockRecord(
                sequence: active.record.sequence,
                virtualPath: active.record.virtualPath,
                lockBody: body,
                nextSlotID: active.record.nextSlotID,
                releaseMarkerID: active.record.releaseMarkerID
            )
            do {
                _ = try await multipartUpload(
                    itemID: active.file.id,
                    isCreate: false,
                    metadata: Self.uploadMetadata(
                        id: nil,
                        name: active.file.name ?? ".gdrive-lock-record-\(active.record.sequence)",
                        parentID: nil,
                        modificationDate: Date(),
                        appProperties: nil
                    ),
                    content: try GoogleDriveJSON.encode(replacement),
                    mimeType: "application/json",
                    expectedStatusCodes: [200],
                    onProgress: nil
                )
            } catch {
                let refreshError = error
                guard GoogleDriveErrorClassifier.isMutationOutcomeUnknown(refreshError) else {
                    throw refreshError
                }
                do {
                    guard let recovered = try await lockRecord(id: active.file.id),
                          recovered.record == replacement else {
                        throw refreshError
                    }
                } catch {
                    throw refreshError
                }
            }
            verifiedLockRecordIDs[path] = active.file.id
        }
    }

    private func releaseVirtualLock(path: String) async throws {
        guard let verifiedID = verifiedLockRecordIDs[path] else {
            throw RemoteStorageClientError.unavailable
        }
        guard let located = try await lockRecord(id: verifiedID), located.record.virtualPath == path else {
            throw Self.notFoundError()
        }
        let release = GoogleDriveLockRelease(recordID: verifiedID, sequence: located.record.sequence)
        let parent = try await resolve(path: Self.parentPath(of: path))
        let metadata = Self.uploadMetadata(
            id: located.record.releaseMarkerID,
            name: ".gdrive-lock-release-\(located.record.sequence)",
            parentID: parent.id,
            modificationDate: Date(),
            appProperties: [
                GoogleDriveConstants.rootRoleKey: GoogleDriveConstants.lockReleaseRole,
                GoogleDriveConstants.lockSequenceKey: String(located.record.sequence),
                GoogleDriveConstants.lockRecordIDKey: verifiedID
            ]
        )
        do {
            _ = try await multipartUpload(
                itemID: located.record.releaseMarkerID,
                isCreate: true,
                metadata: metadata,
                content: try GoogleDriveJSON.encode(release),
                mimeType: "application/json",
                expectedStatusCodes: [200, 201],
                onProgress: nil
            )
        } catch {
            guard GoogleDriveErrorClassifier.isNameCollision(error)
                    || GoogleDriveErrorClassifier.isMutationOutcomeUnknown(error),
                  let existing = try await smallFileData(id: located.record.releaseMarkerID),
                  try GoogleDriveJSON.decode(GoogleDriveLockRelease.self, from: existing) == release else {
                throw error
            }
        }
        verifiedLockRecordIDs.removeValue(forKey: path)
    }

    private func lockCursor(files suppliedFiles: [GoogleDriveFile]? = nil) async throws -> LockCursor {
        let files: [GoogleDriveFile]
        if let suppliedFiles {
            files = suppliedFiles
        } else {
            let directory = try await resolve(path: "/.watermelon/locks")
            files = try await listChildren(parentID: directory.id)
        }
        var records: [String: LockEnvelope] = [:]
        var releases: [String: GoogleDriveFile] = [:]
        for file in files {
            let properties = file.appProperties ?? [:]
            switch properties[GoogleDriveConstants.rootRoleKey] {
            case GoogleDriveConstants.lockRecordRole:
                guard let sequenceText = properties[GoogleDriveConstants.lockSequenceKey],
                      let sequence = UInt64(sequenceText),
                      let nextSlotID = properties[GoogleDriveConstants.lockNextSlotKey],
                      !nextSlotID.isEmpty,
                      let releaseMarkerID = properties[GoogleDriveConstants.lockReleaseMarkerKey],
                      !releaseMarkerID.isEmpty,
                      records[file.id] == nil else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                records[file.id] = LockEnvelope(
                    file: file,
                    sequence: sequence,
                    nextSlotID: nextSlotID,
                    releaseMarkerID: releaseMarkerID
                )
            case GoogleDriveConstants.lockReleaseRole:
                guard releases[file.id] == nil else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                releases[file.id] = file
            default:
                continue
            }
        }
        var slotID = config.connection.lockRootSlotID
        var expectedSequence: UInt64 = 1
        var visited = Set<String>()
        while visited.insert(slotID).inserted, visited.count <= 100_000 {
            guard let envelope = records[slotID] else {
                guard visited.count == records.count + 1,
                      releases.count == records.count else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                return .available(slotID: slotID, sequence: expectedSequence)
            }
            guard envelope.sequence == expectedSequence,
                  envelope.file.trashed != true else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            if let release = releases[envelope.releaseMarkerID] {
                let properties = release.appProperties ?? [:]
                guard properties[GoogleDriveConstants.lockRecordIDKey] == slotID,
                      properties[GoogleDriveConstants.lockSequenceKey] == String(expectedSequence) else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
            } else {
                guard visited.count == records.count,
                      releases.count + 1 == records.count else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                let located = try await lockRecord(file: envelope.file)
                guard located.record.sequence == envelope.sequence,
                      located.record.nextSlotID == envelope.nextSlotID,
                      located.record.releaseMarkerID == envelope.releaseMarkerID else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                return .active(located)
            }
            slotID = envelope.nextSlotID
            expectedSequence += 1
        }
        throw RemoteStorageClientError.invalidConfiguration
    }

    private func lockRecord(id: String) async throws -> LocatedLockRecord? {
        guard let metadata = try await transport.file(id: id) else { return nil }
        return try await lockRecord(file: metadata)
    }

    private func lockRecord(file metadata: GoogleDriveFile) async throws -> LocatedLockRecord {
        guard metadata.appProperties?[GoogleDriveConstants.rootRoleKey] == GoogleDriveConstants.lockRecordRole,
              let data = try await smallFileData(id: metadata.id) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let record = try GoogleDriveJSON.decode(GoogleDriveLockRecord.self, from: data)
        guard record.schemaVersion == GoogleDriveLockRecord.currentSchemaVersion,
              !record.virtualPath.isEmpty,
              !record.nextSlotID.isEmpty,
              !record.releaseMarkerID.isEmpty else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return LocatedLockRecord(file: metadata, record: record)
    }

    private func resolve(path: String) async throws -> GoogleDriveFile {
        if let cached = pathCache[path] { return cached }
        if path == "/" {
            guard let root = try await transport.file(id: config.connection.rootFolderID), root.trashed != true else {
                throw Self.notFoundError()
            }
            pathCache[path] = root
            return root
        }
        var current = try await resolve(path: "/")
        var currentPath = "/"
        for component in try Self.pathComponents(path) {
            currentPath = Self.appending(component, to: currentPath)
            if let cached = pathCache[currentPath] {
                current = cached
                continue
            }
            let matches = try await exactChildren(parentID: current.id, name: component)
            guard matches.count <= 1 else { throw RemoteStorageClientError.invalidConfiguration }
            guard let child = matches.first else { throw Self.notFoundError() }
            pathCache[currentPath] = child
            current = child
        }
        return current
    }

    private func listChildren(parentID: String) async throws -> [GoogleDriveFile] {
        try await transport.listFiles(
            query: "'\(Self.escapeQuery(parentID))' in parents and trashed = false"
        )
    }

    private func exactChildren(parentID: String, name: String) async throws -> [GoogleDriveFile] {
        try await transport.listFiles(
            query: "'\(Self.escapeQuery(parentID))' in parents and name = '\(Self.escapeQuery(name))' and trashed = false"
        )
    }

    private func nextOrdinaryFileID() async throws -> String {
        if let id = ordinaryFileIDPool.popLast() { return id }
        let ids = try await transport.generateFileIDs(count: Self.ordinaryFileIDBatchSize)
        ordinaryFileIDPool.append(contentsOf: ids.dropFirst().reversed())
        return ids[0]
    }

    private func createFolder(name: String, parentID: String) async throws -> GoogleDriveFile {
        let id = try await nextOrdinaryFileID()
        let created: GoogleDriveFile
        do {
            created = try await transport.createFolder(id: id, name: name, parentID: parentID)
        } catch {
            let createError = error
            guard GoogleDriveErrorClassifier.isMutationOutcomeUnknown(createError)
                || GoogleDriveErrorClassifier.isNameCollision(createError) else {
                throw createError
            }
            do {
                if let recovered = try await transport.file(id: id) {
                    guard recovered.name == name,
                          recovered.mimeType == GoogleDriveConstants.folderMIMEType,
                          recovered.parents == [parentID],
                          recovered.trashed != true else {
                        throw RemoteStorageClientError.invalidConfiguration
                    }
                    return recovered
                }
            } catch let recoveryError as RemoteStorageClientError {
                if case .invalidConfiguration = recoveryError { throw recoveryError }
            }
            throw createError
        }
        guard created.id == id,
              created.name == name,
              created.mimeType == GoogleDriveConstants.folderMIMEType,
              created.parents == [parentID],
              created.trashed != true else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        return created
    }

    private func confirmCreatedFolder(
        _ created: GoogleDriveFile,
        name: String,
        parentID: String,
        path: String
    ) async throws -> GoogleDriveFile {
        if !Self.requiresDistributedDirectorySettle(path) {
            guard created.name == name,
                  created.mimeType == GoogleDriveConstants.folderMIMEType,
                  created.parents == [parentID],
                  created.trashed != true else {
                throw GoogleDriveAuthenticationError.invalidResponse
            }
            if observedLeaseGeneration == nil {
                let matches = try await exactChildren(parentID: parentID, name: name)
                guard matches.count == 1, matches[0].id == created.id else {
                    throw remoteStorageNameCollisionError(path: path)
                }
                return matches[0]
            }
            return created
        }
        var winner = created
        var previousWinnerID: String?
        for attempt in 0 ..< Self.controlDirectorySettleAttempts {
            if attempt > 0 { try await Task.sleep(for: Self.controlDirectorySettleDelay) }
            let matches = try await exactChildren(parentID: parentID, name: name)
            guard matches.allSatisfy({ $0.mimeType == GoogleDriveConstants.folderMIMEType }) else {
                throw remoteStorageNameCollisionError(path: path)
            }
            guard !matches.isEmpty else {
                previousWinnerID = nil
                continue
            }
            winner = matches.min(by: googleDriveFolderPrecedes)!
            for loser in matches where loser.id != winner.id {
                guard try await transport.hasChildren(parentID: loser.id) == false else {
                    throw RemoteStorageClientError.invalidConfiguration
                }
                do {
                    try await transport.deleteFile(id: loser.id)
                } catch {
                    if !GoogleDriveErrorClassifier.isNotFound(error) { throw error }
                }
            }
            if matches.count == 1, previousWinnerID == winner.id { return winner }
            previousWinnerID = matches.count == 1 ? winner.id : nil
        }
        throw RemoteStorageClientError.invalidConfiguration
    }

    private func updateMetadata(id: String, values: [String: Any]) async throws {
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL
                .appendingPathComponent("files")
                .appendingPathComponent(id),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [URLQueryItem(name: "fields", value: "id,modifiedTime")]
        var request = URLRequest(url: components.url!)
        request.httpMethod = "PATCH"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try Self.jsonData(values)
        _ = try await transport.data(for: request)
    }

    private func multipartUpload(
        itemID: String,
        isCreate: Bool,
        metadata: [String: Any],
        content: Data,
        mimeType: String,
        expectedStatusCodes: Set<Int>,
        onProgress: ((Double) -> Void)?
    ) async throws -> GoogleDriveFile {
        let boundary = "watermelon-\(UUID().uuidString.lowercased())"
        var body = Data()
        body.append(Data("--\(boundary)\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".utf8))
        body.append(try Self.jsonData(metadata))
        body.append(Data("\r\n--\(boundary)\r\nContent-Type: \(mimeType)\r\n\r\n".utf8))
        body.append(content)
        body.append(Data("\r\n--\(boundary)--\r\n".utf8))

        let base = isCreate
            ? GoogleDriveConstants.uploadBaseURL.appendingPathComponent("files")
            : GoogleDriveConstants.uploadBaseURL.appendingPathComponent("files").appendingPathComponent(itemID)
        var components = URLComponents(url: base, resolvingAgainstBaseURL: false)!
        components.queryItems = [
            URLQueryItem(name: "uploadType", value: "multipart"),
            URLQueryItem(name: "fields", value: GoogleDriveConstants.fileFields)
        ]
        var request = URLRequest(url: components.url!)
        request.httpMethod = isCreate ? "POST" : "PATCH"
        request.setValue("multipart/related; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        let (data, _) = try await transport.upload(
            for: request,
            body: .data(body),
            expectedStatusCodes: expectedStatusCodes,
            onProgress: onProgress
        )
        return try GoogleDriveJSON.decodeResponse(GoogleDriveFile.self, from: data)
    }

    private func resumableUpload(
        localURL: URL,
        size: Int64,
        itemID: String,
        isCreate: Bool,
        metadata: [String: Any],
        onProgress: ((Double) -> Void)?
    ) async throws -> GoogleDriveFile {
        let progress = GoogleDriveMonotonicProgressReporter(callback: onProgress)
        let base = isCreate
            ? GoogleDriveConstants.uploadBaseURL.appendingPathComponent("files")
            : GoogleDriveConstants.uploadBaseURL.appendingPathComponent("files").appendingPathComponent(itemID)
        var components = URLComponents(url: base, resolvingAgainstBaseURL: false)!
        components.queryItems = [
            URLQueryItem(name: "uploadType", value: "resumable"),
            URLQueryItem(name: "fields", value: GoogleDriveConstants.fileFields)
        ]
        var request = URLRequest(url: components.url!)
        request.httpMethod = isCreate ? "POST" : "PATCH"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/octet-stream", forHTTPHeaderField: "X-Upload-Content-Type")
        request.setValue(String(size), forHTTPHeaderField: "X-Upload-Content-Length")
        request.httpBody = try Self.jsonData(metadata)
        let (_, response) = try await transport.data(for: request, expectedStatusCodes: [200, 201])
        guard let location = response.value(forHTTPHeaderField: "Location"),
              let uploadURL = URL(string: location), uploadURL.scheme == "https" else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        let handle = try FileHandle(forReadingFrom: localURL)
        defer { try? handle.close() }
        var offset: Int64 = 0
        var noProgressRecoveryCount = 0
        while offset < size {
            try Task.checkCancellation()
            try handle.seek(toOffset: UInt64(offset))
            let requested = min(Int64(Self.resumableChunkSize), size - offset)
            guard let chunk = try handle.read(upToCount: Int(requested)),
                  Int64(chunk.count) == requested else {
                throw RemoteStorageClientError.unavailable
            }
            let end = offset + requested - 1
            var uploadRequest = URLRequest(url: uploadURL)
            uploadRequest.httpMethod = "PUT"
            uploadRequest.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
            uploadRequest.setValue(String(chunk.count), forHTTPHeaderField: "Content-Length")
            uploadRequest.setValue("bytes \(offset)-\(end)/\(size)", forHTTPHeaderField: "Content-Range")
            do {
                let (chunkData, chunkResponse) = try await transport.upload(
                    for: uploadRequest,
                    body: .data(chunk),
                    expectedStatusCodes: [200, 201, 308],
                    onProgress: { chunkProgress in
                        progress.report(
                            (Double(offset) + chunkProgress * Double(requested)) / Double(size)
                        )
                    }
                )
                guard let nextOffset = try Self.nextResumableOffset(
                    response: chunkResponse,
                    totalSize: size
                ) else {
                    progress.report(1)
                    return try GoogleDriveJSON.decodeResponse(GoogleDriveFile.self, from: chunkData)
                }
                guard nextOffset > offset else {
                    throw RemoteStorageClientError.unavailable
                }
                offset = nextOffset
                noProgressRecoveryCount = 0
                progress.report(Double(offset) / Double(size))
            } catch {
                if Task.isCancelled || Self.isCancellationError(error) { throw CancellationError() }
                guard GoogleDriveErrorClassifier.isConnectionUnavailable(error)
                    || RemoteStorageClientError.isConnectionUnavailable(error) else {
                    throw error
                }
                let originalError = error
                var recoveredOffset: Int64?
                var recoveryError: Error = originalError
                for attempt in 0 ..< Self.resumableRecoveryLimit {
                    do {
                        let status = try await queryResumableUploadStatus(uploadURL: uploadURL, size: size)
                        switch status {
                        case .incomplete(let offset):
                            recoveredOffset = offset
                        case .complete(let file):
                            progress.report(1)
                            return file
                        }
                        break
                    } catch {
                        if Task.isCancelled || Self.isCancellationError(error) { throw CancellationError() }
                        recoveryError = error
                        if GoogleDriveErrorClassifier.isNotFound(error) { break }
                        if attempt + 1 < Self.resumableRecoveryLimit {
                            try await Task.sleep(for: .seconds(pow(2, Double(attempt))))
                        }
                    }
                }
                guard let recoveredOffset else { throw recoveryError }
                guard recoveredOffset >= offset, recoveredOffset <= size else {
                    throw GoogleDriveAuthenticationError.invalidResponse
                }
                if recoveredOffset == offset {
                    noProgressRecoveryCount += 1
                    guard noProgressRecoveryCount <= Self.resumableRecoveryLimit else {
                        throw originalError
                    }
                    try await Task.sleep(
                        for: Self.resumableRecoveryDelay(attempt: noProgressRecoveryCount - 1)
                    )
                } else {
                    noProgressRecoveryCount = 0
                }
                offset = recoveredOffset
                progress.report(Double(offset) / Double(size))
            }
        }
        progress.report(1)
        guard let recovered = try await transport.file(id: itemID) else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        return recovered
    }

    private enum ResumableUploadStatus {
        case incomplete(Int64)
        case complete(GoogleDriveFile)
    }

    private func queryResumableUploadStatus(
        uploadURL: URL,
        size: Int64
    ) async throws -> ResumableUploadStatus {
        var request = URLRequest(url: uploadURL)
        request.httpMethod = "PUT"
        request.setValue("0", forHTTPHeaderField: "Content-Length")
        request.setValue("bytes */\(size)", forHTTPHeaderField: "Content-Range")
        let (data, response) = try await transport.upload(
            for: request,
            body: .data(Data()),
            expectedStatusCodes: [200, 201, 308],
            onProgress: nil
        )
        if let offset = try Self.nextResumableOffset(response: response, totalSize: size) {
            return .incomplete(offset)
        }
        return .complete(try GoogleDriveJSON.decodeResponse(GoogleDriveFile.self, from: data))
    }

    nonisolated private static func nextResumableOffset(
        response: HTTPURLResponse,
        totalSize: Int64
    ) throws -> Int64? {
        if response.statusCode == 200 || response.statusCode == 201 { return nil }
        guard response.statusCode == 308 else {
            throw GoogleDriveAuthenticationError.invalidResponse
        }
        guard let range = response.value(forHTTPHeaderField: "Range"),
              let lastText = range.split(separator: "-").last,
              let last = Int64(lastText),
              last >= 0,
              last < totalSize else {
            return 0
        }
        return last + 1
    }

    nonisolated private static func resumableRecoveryDelay(attempt: Int) -> Duration {
        let cap = pow(2, Double(max(0, attempt)))
        return .milliseconds(Int64(Double.random(in: 250 ... 500) * cap))
    }

    nonisolated private static func isCancellationError(_ error: Error) -> Bool {
        if error is CancellationError { return true }
        let ns = error as NSError
        if ns.domain == NSURLErrorDomain, ns.code == NSURLErrorCancelled { return true }
        if let storage = error as? RemoteStorageClientError, case .underlying(let inner) = storage {
            return isCancellationError(inner)
        }
        if let inner = ns.userInfo[NSUnderlyingErrorKey] as? Error {
            return isCancellationError(inner)
        }
        return false
    }

    private func smallFileData(id: String) async throws -> Data? {
        var components = URLComponents(
            url: GoogleDriveConstants.apiBaseURL
                .appendingPathComponent("files")
                .appendingPathComponent(id),
            resolvingAgainstBaseURL: false
        )!
        components.queryItems = [URLQueryItem(name: "alt", value: "media")]
        do {
            return try await transport.data(for: URLRequest(url: components.url!)).0
        } catch {
            if GoogleDriveErrorClassifier.isNotFound(error) { return nil }
            throw error
        }
    }

    private func invalidate(path: String) {
        let prefix = path == "/" ? "/" : path + "/"
        pathCache = pathCache.filter { key, _ in key != path && !key.hasPrefix(prefix) }
    }

    private func retainOnlyCachedRoot() {
        if let root = pathCache["/"] {
            pathCache = ["/": root]
        } else {
            pathCache.removeAll()
        }
    }

    private func synchronizeLeasedNamespaceGeneration() async -> UUID? {
        let generation = await sharedState.writeSession.generation(for: writeSessionKey)
        if observedLeaseGeneration != generation {
            observedLeaseGeneration = generation
            retainOnlyCachedRoot()
        }
        return generation
    }

    private func recordObservedFile(_ file: GoogleDriveFile, path: String) async {
        if let generation = observedLeaseGeneration {
            await sharedState.writeSession.observe(
                file,
                path: path,
                key: writeSessionKey,
                generation: generation
            )
        } else {
            pathCache[path] = file
        }
    }

    private func invalidateDirectChildren(of path: String) {
        pathCache = pathCache.filter { key, _ in key == path || Self.parentPath(of: key) != path }
    }

    private func resolveFresh(path: String) async throws -> GoogleDriveFile {
        if path == "/" {
            guard let root = try await transport.file(id: config.connection.rootFolderID),
                  root.watermelonLockRootSlotID == config.connection.lockRootSlotID else {
                throw Self.notFoundError()
            }
            await recordObservedFile(root, path: "/")
            return root
        }
        let parent = try await resolveFresh(path: Self.parentPath(of: path))
        let matches = try await exactChildren(parentID: parent.id, name: Self.lastComponent(of: path))
        guard matches.count <= 1 else { throw RemoteStorageClientError.invalidConfiguration }
        guard let live = matches.first else {
            invalidate(path: path)
            throw Self.notFoundError()
        }
        if observedLeaseGeneration == nil,
           let cached = pathCache[path], live.id != cached.id {
            invalidate(path: path)
        }
        await recordObservedFile(live, path: path)
        return live
    }

    private func validateIdentity() throws {
        guard credential.accountSubject == config.connection.accountSubject else {
            throw GoogleDriveAuthenticationError.accountMismatch
        }
    }

    nonisolated private static func uploadMetadata(
        id: String?,
        name: String,
        parentID: String?,
        modificationDate: Date?,
        appProperties: [String: String]?
    ) -> [String: Any] {
        var metadata: [String: Any] = ["name": name]
        if let id { metadata["id"] = id }
        if let parentID { metadata["parents"] = [parentID] }
        if let modificationDate { metadata["modifiedTime"] = dateString(modificationDate) }
        if let appProperties { metadata["appProperties"] = appProperties }
        return metadata
    }

    nonisolated private static func virtualLockFile(_ active: LocatedLockRecord) -> GoogleDriveFile {
        GoogleDriveFile(
            id: active.file.id,
            name: lastComponent(of: active.record.virtualPath),
            mimeType: "application/json",
            size: String(active.record.lockBody.count),
            md5Checksum: nil,
            createdTime: active.file.createdTime,
            modifiedTime: active.file.modifiedTime,
            parents: active.file.parents,
            trashed: false,
            appProperties: nil
        )
    }

    nonisolated private static func remoteEntry(_ file: GoogleDriveFile, path: String) -> RemoteStorageEntry {
        RemoteStorageEntry(
            path: path,
            name: file.name ?? lastComponent(of: path),
            isDirectory: file.mimeType == GoogleDriveConstants.folderMIMEType,
            size: file.size.flatMap(Int64.init) ?? 0,
            creationDate: file.createdTime,
            modificationDate: file.modifiedTime
        )
    }

    nonisolated private static func canonicalPath(_ value: String) throws -> String {
        let components = value.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        for component in components where !RemotePathBuilder.isSafePathComponent(component) {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return components.isEmpty ? "/" : "/" + components.joined(separator: "/")
    }

    nonisolated private static func pathComponents(_ path: String) throws -> [String] {
        let components = path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        for component in components where !RemotePathBuilder.isSafePathComponent(component) {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return components
    }

    nonisolated private static func parentPath(of path: String) -> String {
        let components = path.split(separator: "/", omittingEmptySubsequences: true)
        guard components.count > 1 else { return "/" }
        return "/" + components.dropLast().joined(separator: "/")
    }

    nonisolated private static func lastComponent(of path: String) -> String {
        path.split(separator: "/", omittingEmptySubsequences: true).last.map(String.init) ?? "/"
    }

    nonisolated private static func appending(_ name: String, to path: String) -> String {
        path == "/" ? "/\(name)" : "\(path)/\(name)"
    }

    nonisolated private static func isLocksDirectory(_ path: String) -> Bool {
        let components = path.split(separator: "/", omittingEmptySubsequences: true)
        return components.count >= 2 && components.suffix(2).elementsEqual([".watermelon", "locks"])
    }

    nonisolated private static func isVirtualLockPath(_ path: String) -> Bool {
        let parent = parentPath(of: path)
        return isLocksDirectory(parent) && lastComponent(of: path).hasSuffix(".lock")
    }

    nonisolated private static func requiresDistributedDirectorySettle(_ path: String) -> Bool {
        path == "/.watermelon" || isLocksDirectory(path)
    }

    nonisolated private static func escapeQuery(_ value: String) -> String {
        value.replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
    }

    nonisolated private static func dateString(_ date: Date) -> String {
        ISO8601DateFormatter().string(from: date)
    }

    nonisolated private static func setupWriterID(
        clientID: String,
        accountSubject: String,
        rootFolderID: String
    ) -> String {
        let source = "\(clientID.lowercased())\n\(accountSubject)\n\(rootFolderID)"
        let hex = SHA256.hash(data: Data(source.utf8)).prefix(16)
            .map { String(format: "%02x", $0) }
            .joined()
        return "\(hex.prefix(8))-\(hex.dropFirst(8).prefix(4))-\(hex.dropFirst(12).prefix(4))-\(hex.dropFirst(16).prefix(4))-\(hex.dropFirst(20).prefix(12))"
    }

    nonisolated private static func jsonData(_ value: Any) throws -> Data {
        guard JSONSerialization.isValidJSONObject(value) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return try JSONSerialization.data(withJSONObject: value)
    }

    nonisolated private static func notFoundError() -> NSError {
        NSError(
            domain: GoogleDriveErrorClassifier.errorDomain,
            code: 404,
            userInfo: [
                NSLocalizedDescriptionKey: String(localized: "googledrive.error.notFound"),
                GoogleDriveErrorClassifier.userInfoStatusCodeKey: 404
            ]
        )
    }

    nonisolated private static func replaceLocalFile(with data: Data, at url: URL) throws {
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        if FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.removeItem(at: url)
        }
        try data.write(to: url)
    }

    nonisolated private static func installDownloadedFile(_ source: URL, at destination: URL) throws {
        try FileManager.default.createDirectory(
            at: destination.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        if FileManager.default.fileExists(atPath: destination.path) {
            try FileManager.default.removeItem(at: destination)
        }
        try FileManager.default.moveItem(at: source, to: destination)
    }

    nonisolated private static func isValidUploadResult(
        _ file: GoogleDriveFile,
        itemID: String,
        name: String,
        parentID: String,
        size: Int64
    ) -> Bool {
        file.id == itemID
            && file.name == name
            && file.parents == [parentID]
            && file.mimeType != GoogleDriveConstants.folderMIMEType
            && file.trashed != true
            && file.size.flatMap(Int64.init) == size
    }

    nonisolated private static func isValidCopyResult(
        _ file: GoogleDriveFile,
        itemID: String,
        name: String,
        parentID: String,
        source: GoogleDriveFile
    ) -> Bool {
        guard file.id == itemID,
              file.name == name,
              file.parents == [parentID],
              file.mimeType != GoogleDriveConstants.folderMIMEType,
              file.trashed != true,
              file.size == source.size else {
            return false
        }
        guard let sourceMD5 = source.md5Checksum else { return true }
        return file.md5Checksum?.caseInsensitiveCompare(sourceMD5) == .orderedSame
    }

    nonisolated private static func fileContentsMatch(
        localURL: URL,
        remote: GoogleDriveFile
    ) throws -> Bool {
        guard let remoteMD5 = remote.md5Checksum else { return false }
        return try md5Hex(of: localURL).caseInsensitiveCompare(remoteMD5) == .orderedSame
    }

    nonisolated private static func md5Hex(of url: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var hash = Insecure.MD5()
        while let data = try handle.read(upToCount: 8 * 1024 * 1024), !data.isEmpty {
            hash.update(data: data)
        }
        return hash.finalize().map { String(format: "%02x", $0) }.joined()
    }
}

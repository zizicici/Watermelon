import Foundation
import Darwin

final actor LocalVolumeClient: RemoteStorageClientProtocol {
    struct Config {
        let rootBookmarkData: Data
        let displayPath: String?
        let onBookmarkRefreshed: ((BookmarkRefreshPayload) -> Void)?

        init(
            rootBookmarkData: Data,
            displayPath: String? = nil,
            onBookmarkRefreshed: ((BookmarkRefreshPayload) -> Void)?
        ) {
            self.rootBookmarkData = rootBookmarkData
            self.displayPath = displayPath
            self.onBookmarkRefreshed = onBookmarkRefreshed
        }
    }

    struct BookmarkRefreshPayload {
        let bookmarkData: Data
        let displayPath: String
    }

    private static let prefetchedResourceKeys: [URLResourceKey] = [
        .isDirectoryKey,
        .creationDateKey,
        .contentModificationDateKey,
        .fileSizeKey,
        .nameKey
    ]
    private static let copyBufferSize = 16 * 1024 * 1024
    private static let copyProgressStepBytes: Int64 = 8 * 1024 * 1024
    nonisolated private static let uploadTemporaryPrefix = ".watermelon-upload-"

    private struct ContainedPath {
        let url: URL
        let remotePath: String
    }

    private struct RootAnchor {
        let authorizedURL: URL
        let resolvedURL: URL
        let resourceIdentity: Data?
    }

    private struct FileArtifactIdentity: Equatable {
        let device: UInt64
        let inode: UInt64

        init(fileDescriptor: Int32) throws {
            var info = stat()
            guard Darwin.fstat(fileDescriptor, &info) == 0 else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            guard (info.st_mode & S_IFMT) == S_IFREG else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            device = UInt64(info.st_dev)
            inode = UInt64(info.st_ino)
        }

        init(pathEntryAt url: URL) throws {
            var info = stat()
            let result = url.path.withCString { Darwin.lstat($0, &info) }
            guard result == 0 else {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            guard (info.st_mode & S_IFMT) == S_IFREG else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            device = UInt64(info.st_dev)
            inode = UInt64(info.st_ino)
        }
    }

    private var config: Config
    private let bookmarkStore: SecurityScopedBookmarkStore
    nonisolated private let stagedUploadPublisher: (@Sendable (URL, URL) throws -> Void)?
    nonisolated private let securityScopeLease = SecurityScopeLease()
    private var rootURL: URL?
    private var rootAnchor: RootAnchor?

    init(
        config: Config,
        bookmarkStore: SecurityScopedBookmarkStore = SecurityScopedBookmarkStore(),
        stagedUploadPublisher: (@Sendable (URL, URL) throws -> Void)? = nil
    ) {
        self.config = config
        self.bookmarkStore = bookmarkStore
        self.stagedUploadPublisher = stagedUploadPublisher
    }

    init(
        connectedRootURL: URL,
        stagedUploadPublisher: (@Sendable (URL, URL) throws -> Void)? = nil
    ) throws {
        self.config = Config(rootBookmarkData: Data(), onBookmarkRefreshed: nil)
        self.bookmarkStore = SecurityScopedBookmarkStore()
        self.stagedUploadPublisher = stagedUploadPublisher
        self.rootURL = connectedRootURL
        self.rootAnchor = try Self.makeRootAnchor(for: connectedRootURL)
    }

    nonisolated func shouldSetModificationDate() -> Bool {
        false
    }

    nonisolated func cancelActiveOperationsForAbandonment() {
        securityScopeLease.abandon()
    }

    deinit {
        securityScopeLease.release()
    }

    func connect() async throws {
        if rootURL != nil, rootAnchor != nil { return }
        let resolved: SecurityScopedBookmarkStore.ResolvedBookmark
        do {
            resolved = try bookmarkStore.resolveBookmarkData(config.rootBookmarkData)
        } catch {
            if isLikelyBookmarkAccessFailure(error) {
                throw RemoteStorageClientError.externalStorageUnavailable
            }
            throw mapStorageError(error)
        }
        guard resolved.url.startAccessingSecurityScopedResource() else {
            throw RemoteStorageClientError.externalStorageUnavailable
        }
        guard securityScopeLease.adoptStartedAccess(to: resolved.url) else {
            if Task.isCancelled { throw CancellationError() }
            throw RemoteStorageClientError.externalStorageUnavailable
        }
        do {
            rootAnchor = try Self.makeRootAnchor(for: resolved.url)
            rootURL = resolved.url
        } catch {
            securityScopeLease.release()
            throw mapStorageError(error)
        }

        let resolvedPath = resolved.url.path
        let effectiveBookmark = resolved.refreshedBookmarkData ?? config.rootBookmarkData
        let shouldRefresh = resolved.refreshedBookmarkData != nil ||
            config.displayPath != resolvedPath
        let refreshHandler = config.onBookmarkRefreshed
        config = Config(
            rootBookmarkData: effectiveBookmark,
            displayPath: resolvedPath,
            onBookmarkRefreshed: refreshHandler
        )
        if shouldRefresh {
            refreshHandler?(BookmarkRefreshPayload(
                bookmarkData: effectiveBookmark,
                displayPath: resolvedPath
            ))
        }
    }

    func disconnect() async {
        securityScopeLease.release()
        rootURL = nil
        rootAnchor = nil
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        let root = try requireRootAnchor()
        do {
            let containedRoot = try containedPath(forRemotePath: "/", rootAnchor: root)
            let values = try containedRoot.url.resourceValues(forKeys: [
                .volumeAvailableCapacityForImportantUsageKey,
                .volumeAvailableCapacityForOpportunisticUsageKey,
                .volumeAvailableCapacityKey,
                .volumeTotalCapacityKey
            ])

            let availableImportant = values.volumeAvailableCapacityForImportantUsage.map { Int64($0) }
            let availableOpportunistic = values.volumeAvailableCapacityForOpportunisticUsage.map { Int64($0) }
            let availableLegacy = values.volumeAvailableCapacity.map { Int64($0) }

            // Some external providers report 0 for unknown capacity. Treat non-positive values as unavailable.
            let available = [availableImportant, availableOpportunistic, availableLegacy]
                .compactMap { $0 }
                .first(where: { $0 > 0 })
            let total = values.volumeTotalCapacity
                .map { Int64($0) }
                .flatMap { $0 > 0 ? $0 : nil }
            if available == nil, total == nil {
                return nil
            }
            return RemoteStorageCapacity(availableBytes: available, totalBytes: total)
        } catch {
            throw mapStorageError(error)
        }
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let root = try requireRootAnchor()
        do {
            let directory = try containedPath(forRemotePath: path, rootAnchor: root)
            let urls = try FileManager.default.contentsOfDirectory(
                at: directory.url,
                includingPropertiesForKeys: Self.prefetchedResourceKeys,
                options: []
            )
            return try urls.map { url in
                let childRemotePath = directory.remotePath == "/"
                    ? "/\(url.lastPathComponent)"
                    : "\(directory.remotePath)/\(url.lastPathComponent)"
                let child = try containedPath(forRemotePath: childRemotePath, rootAnchor: root)
                return try makeEntry(
                    fileURL: child.url,
                    remotePath: child.remotePath,
                    name: url.lastPathComponent
                )
            }
        } catch {
            throw mapStorageError(error)
        }
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let root = try requireRootAnchor()
        do {
            let contained = try containedPath(forRemotePath: path, rootAnchor: root)
            guard FileManager.default.fileExists(atPath: contained.url.path) else {
                try ensureRootReachable(root)
                return nil
            }
            return try makeEntry(fileURL: contained.url, remotePath: contained.remotePath)
        } catch {
            throw mapStorageError(error)
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
        var cleanupRemotePath: String?
        var ownedDestinationIdentity: FileArtifactIdentity?
        let root = try requireRootAnchor()
        do {
            if respectTaskCancellation {
                try Task.checkCancellation()
            }

            let destination = try containedPath(forRemotePath: remotePath, rootAnchor: root)
            let parentURL = destination.url.deletingLastPathComponent()
            try FileManager.default.createDirectory(at: parentURL, withIntermediateDirectories: true)
            if mode == .createIfAbsent {
                do {
                    try FileManager.default.copyItem(at: localURL, to: destination.url)
                    onProgress?(1)
                    return
                } catch {
                    if Self.isDestinationExistsError(error) {
                        throw remoteStorageNameCollisionError(path: remotePath)
                    }
                    throw mapStorageError(error)
                }
            }

            guard destination.remotePath != "/" else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            let parentRemotePath = Self.parentRemotePath(of: destination.remotePath)
            let temporaryRemotePath = Self.uploadTemporaryRemotePath(parentRemotePath: parentRemotePath)
            let temporaryURL = try containedPath(
                forRemotePath: temporaryRemotePath,
                rootAnchor: root
            ).url
            cleanupRemotePath = temporaryRemotePath

            if !respectTaskCancellation {
                do {
                    try FileManager.default.copyItem(at: localURL, to: temporaryURL)
                } catch {
                    if !Self.isDestinationExistsError(error) {
                        ownedDestinationIdentity = try? fileArtifactIdentity(
                            forRemotePath: temporaryRemotePath,
                            rootAnchor: root
                        )
                    }
                    throw error
                }
                ownedDestinationIdentity = try fileArtifactIdentity(
                    forRemotePath: temporaryRemotePath,
                    rootAnchor: root
                )
            } else {
                try copyFileChunked(
                    from: localURL,
                    to: temporaryURL,
                    respectTaskCancellation: true,
                    onProgress: onProgress,
                    onDestinationOpened: { ownedDestinationIdentity = $0 }
                )
            }

            guard let ownedDestinationIdentity else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            try publishStagedUpload(
                temporaryRemotePath: temporaryRemotePath,
                destinationRemotePath: destination.remotePath,
                parentRemotePath: parentRemotePath,
                rootAnchor: root,
                expectedIdentity: ownedDestinationIdentity
            )
            cleanupRemotePath = nil
            if !respectTaskCancellation {
                onProgress?(1)
            }
        } catch {
            if let cleanupRemotePath {
                removeUploadArtifactIfStillOwned(
                    remotePath: cleanupRemotePath,
                    rootAnchor: root,
                    expectedIdentity: ownedDestinationIdentity
                )
            }
            throw mapStorageError(error)
        }
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        let root = try requireRootAnchor()
        do {
            let url = try containedPath(forRemotePath: path, rootAnchor: root).url
            try FileManager.default.setAttributes([.modificationDate: date], ofItemAtPath: url.path)
        } catch {
            throw mapStorageError(error)
        }
    }

    func directReadURL(forRemotePath remotePath: String) async -> URL? {
        guard let root = rootAnchor else { return nil }
        return try? containedPath(forRemotePath: remotePath, rootAnchor: root).url
    }

    func download(remotePath: String, localURL: URL) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        let root = try requireRootAnchor()
        var shouldCleanupDestinationOnFailure = false
        do {
            try Task.checkCancellation()
            let sourceURL = try containedPath(forRemotePath: remotePath, rootAnchor: root).url
            let parentURL = localURL.deletingLastPathComponent()
            try FileManager.default.createDirectory(at: parentURL, withIntermediateDirectories: true)
            if FileManager.default.fileExists(atPath: localURL.path) {
                try FileManager.default.removeItem(at: localURL)
            }

            guard let onProgress else {
                try FileManager.default.copyItem(at: sourceURL, to: localURL)
                try Task.checkCancellation()
                return
            }

            shouldCleanupDestinationOnFailure = true
            try copyFileChunked(
                from: sourceURL,
                to: localURL,
                respectTaskCancellation: true,
                onProgress: onProgress
            )
            shouldCleanupDestinationOnFailure = false
        } catch {
            if shouldCleanupDestinationOnFailure {
                try? FileManager.default.removeItem(at: localURL)
            }
            throw mapStorageError(error)
        }
    }

    func exists(path: String) async throws -> Bool {
        let root = try requireRootAnchor()
        do {
            let url = try containedPath(forRemotePath: path, rootAnchor: root).url
            return FileManager.default.fileExists(atPath: url.path)
        } catch {
            throw mapStorageError(error)
        }
    }

    func delete(path: String) async throws {
        let root = try requireRootAnchor()
        do {
            let normalized = RemotePathBuilder.normalizePath(path)
            guard normalized != "/" else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            let url = try containedPath(forRemotePath: normalized, rootAnchor: root).url
            guard FileManager.default.fileExists(atPath: url.path) else { return }
            try FileManager.default.removeItem(at: url)
        } catch {
            throw mapStorageError(error)
        }
    }

    func createDirectory(path: String) async throws {
        let root = try requireRootAnchor()
        do {
            let normalized = RemotePathBuilder.normalizePath(path)
            guard normalized != "/" else { return }
            let url = try containedPath(forRemotePath: normalized, rootAnchor: root).url
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        } catch {
            throw mapStorageError(error)
        }
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        let root = try requireRootAnchor()
        do {
            let sourceURL = try containedPath(forRemotePath: sourcePath, rootAnchor: root).url
            let destinationURL = try containedPath(forRemotePath: destinationPath, rootAnchor: root).url
            let destinationParent = destinationURL.deletingLastPathComponent()
            try FileManager.default.createDirectory(at: destinationParent, withIntermediateDirectories: true)
            if FileManager.default.fileExists(atPath: destinationURL.path) {
                _ = try FileManager.default.replaceItemAt(
                    destinationURL,
                    withItemAt: sourceURL,
                    backupItemName: nil,
                    options: []
                )
                return
            }
            try FileManager.default.moveItem(at: sourceURL, to: destinationURL)
        } catch {
            throw mapStorageError(error)
        }
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        let root = try requireRootAnchor()
        do {
            let sourceURL = try containedPath(forRemotePath: sourcePath, rootAnchor: root).url
            let destinationURL = try containedPath(forRemotePath: destinationPath, rootAnchor: root).url
            let destinationParent = destinationURL.deletingLastPathComponent()
            try FileManager.default.createDirectory(at: destinationParent, withIntermediateDirectories: true)
            if FileManager.default.fileExists(atPath: destinationURL.path) {
                try FileManager.default.removeItem(at: destinationURL)
            }
            try FileManager.default.copyItem(at: sourceURL, to: destinationURL)
        } catch {
            throw mapStorageError(error)
        }
    }

    private func requireRootAnchor() throws -> RootAnchor {
        guard let rootAnchor else {
            throw RemoteStorageClientError.notConnected
        }
        return rootAnchor
    }

    private func containedPath(forRemotePath remotePath: String, rootAnchor: RootAnchor) throws -> ContainedPath {
        let normalized = RemotePathBuilder.normalizePath(remotePath)
        let standardizedRoot = try validateRootAnchor(rootAnchor)
        let resolvedRoot = rootAnchor.resolvedURL
        if normalized == "/" {
            return ContainedPath(url: standardizedRoot, remotePath: normalized)
        }

        let relative = String(normalized.dropFirst())
        let components = relative.split(separator: "/")
        guard !components.contains(where: { $0 == ".." || $0 == "." }) else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        var url = standardizedRoot
        for component in components where !component.isEmpty {
            url.appendPathComponent(String(component), isDirectory: false)
            do {
                _ = try url.resourceValues(forKeys: [.isSymbolicLinkKey])
            } catch {
                guard Self.isMissingFileError(error) else { throw error }
            }
            let resolved = url.resolvingSymlinksInPath().standardizedFileURL
            guard Self.isStrictDescendant(resolved, of: resolvedRoot) else {
                throw RemoteStorageClientError.invalidConfiguration
            }
        }
        return ContainedPath(url: url, remotePath: normalized)
    }

    private func makeEntry(fileURL: URL, remotePath: String, name: String? = nil) throws -> RemoteStorageEntry {
        let values = try fileURL.resourceValues(forKeys: Set(Self.prefetchedResourceKeys))
        return RemoteStorageEntry(
            path: remotePath,
            name: name ?? values.name ?? fileURL.lastPathComponent,
            isDirectory: values.isDirectory ?? false,
            size: Int64(values.fileSize ?? 0),
            creationDate: values.creationDate,
            modificationDate: values.contentModificationDate
        )
    }

    private func ensureRootReachable(_ root: RootAnchor) throws {
        _ = try containedPath(forRemotePath: "/", rootAnchor: root)
    }

    private func validateRootAnchor(_ anchor: RootAnchor) throws -> URL {
        let rootValues = try anchor.authorizedURL.resourceValues(forKeys: [
            .isDirectoryKey,
            .isSymbolicLinkKey
        ])
        guard rootValues.isDirectory == true,
              rootValues.isSymbolicLink != true else {
            throw RemoteStorageClientError.externalStorageUnavailable
        }
        let resolvedRoot = anchor.authorizedURL.resolvingSymlinksInPath().standardizedFileURL
        guard resolvedRoot == anchor.resolvedURL else {
            throw RemoteStorageClientError.externalStorageUnavailable
        }
        if let expectedIdentity = anchor.resourceIdentity {
            guard try Self.rootResourceIdentity(for: resolvedRoot) == expectedIdentity else {
                throw RemoteStorageClientError.externalStorageUnavailable
            }
        }
        return anchor.authorizedURL
    }

    nonisolated private static func makeRootAnchor(for rootURL: URL) throws -> RootAnchor {
        let authorizedURL = rootURL.standardizedFileURL
        let rootValues = try authorizedURL.resourceValues(forKeys: [
            .isDirectoryKey,
            .isSymbolicLinkKey
        ])
        guard rootValues.isDirectory == true,
              rootValues.isSymbolicLink != true else {
            throw RemoteStorageClientError.externalStorageUnavailable
        }
        let resolvedURL = authorizedURL.resolvingSymlinksInPath().standardizedFileURL
        let resolvedValues = try resolvedURL.resourceValues(forKeys: [.isDirectoryKey])
        guard resolvedValues.isDirectory == true else {
            throw RemoteStorageClientError.externalStorageUnavailable
        }
        return RootAnchor(
            authorizedURL: authorizedURL,
            resolvedURL: resolvedURL,
            resourceIdentity: try rootResourceIdentity(for: resolvedURL)
        )
    }

    nonisolated private static func rootResourceIdentity(for url: URL) throws -> Data? {
        let values = try url.resourceValues(forKeys: [
            .volumeIdentifierKey,
            .fileResourceIdentifierKey
        ])
        return SecurityScopedBookmarkStore.ephemeralLocationIdentities(
            volumeIdentifier: values.volumeIdentifier,
            fileResourceIdentifier: values.fileResourceIdentifier,
            standardizedURL: url
        ).fullIdentity
    }

    nonisolated private static func isStrictDescendant(_ candidate: URL, of root: URL) -> Bool {
        let rootPath = root.standardizedFileURL.path
        let candidatePath = candidate.standardizedFileURL.path
        let prefix = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
        return candidatePath.hasPrefix(prefix)
    }

    nonisolated private static func isMissingFileError(_ error: Error) -> Bool {
        var pending = [error as NSError]
        while let current = pending.popLast() {
            if current.domain == NSCocoaErrorDomain,
               (current.code == NSFileNoSuchFileError || current.code == NSFileReadNoSuchFileError) {
                return true
            }
            if current.domain == NSPOSIXErrorDomain, current.code == Int(ENOENT) {
                return true
            }
            if let underlying = current.userInfo[NSUnderlyingErrorKey] as? NSError {
                pending.append(underlying)
            }
        }
        return false
    }

    nonisolated private static func parentRemotePath(of remotePath: String) -> String {
        let components = RemotePathBuilder.normalizePath(remotePath)
            .split(separator: "/", omittingEmptySubsequences: true)
        guard components.count > 1 else { return "/" }
        return "/" + components.dropLast().joined(separator: "/")
    }

    nonisolated private static func uploadTemporaryRemotePath(parentRemotePath: String) -> String {
        let name = "\(uploadTemporaryPrefix)\(UUID().uuidString.lowercased()).tmp"
        return parentRemotePath == "/" ? "/\(name)" : "\(parentRemotePath)/\(name)"
    }

    private func fileArtifactIdentity(
        forRemotePath remotePath: String,
        rootAnchor: RootAnchor
    ) throws -> FileArtifactIdentity {
        let url = try containedPath(forRemotePath: remotePath, rootAnchor: rootAnchor).url
        return try FileArtifactIdentity(pathEntryAt: url)
    }

    private func publishStagedUpload(
        temporaryRemotePath: String,
        destinationRemotePath: String,
        parentRemotePath: String,
        rootAnchor: RootAnchor,
        expectedIdentity: FileArtifactIdentity
    ) throws {
        _ = try containedPath(forRemotePath: parentRemotePath, rootAnchor: rootAnchor)
        let destinationURL = try containedPath(
            forRemotePath: destinationRemotePath,
            rootAnchor: rootAnchor
        ).url
        let temporaryURL = try containedPath(
            forRemotePath: temporaryRemotePath,
            rootAnchor: rootAnchor
        ).url
        guard try FileArtifactIdentity(pathEntryAt: temporaryURL) == expectedIdentity else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        if let stagedUploadPublisher {
            try stagedUploadPublisher(temporaryURL, destinationURL)
        } else {
            try Self.atomicRename(from: temporaryURL, to: destinationURL)
        }
    }

    nonisolated private static func atomicRename(from sourceURL: URL, to destinationURL: URL) throws {
        let result = sourceURL.path.withCString { sourcePath in
            destinationURL.path.withCString { destinationPath in
                Darwin.rename(sourcePath, destinationPath)
            }
        }
        guard result == 0 else {
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
    }

    private func copyFileChunked(
        from sourceURL: URL,
        to destinationURL: URL,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?,
        onDestinationOpened: ((FileArtifactIdentity) -> Void)? = nil
    ) throws {
        guard FileManager.default.createFile(atPath: destinationURL.path, contents: nil) else {
            throw NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileWriteUnknownError,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "storage.local.createDestinationFileFailed"),
                        destinationURL.path
                    )
                ]
            )
        }

        let destinationHandle = try FileHandle(forWritingTo: destinationURL)
        var sourceHandle: FileHandle?
        var sourceClosed = false
        var destinationClosed = false
        defer {
            if !sourceClosed { try? sourceHandle?.close() }
            if !destinationClosed { try? destinationHandle.close() }
        }
        onDestinationOpened?(try FileArtifactIdentity(fileDescriptor: destinationHandle.fileDescriptor))
        let openedSourceHandle = try FileHandle(forReadingFrom: sourceURL)
        sourceHandle = openedSourceHandle

        let fileSize = ((try? FileManager.default.attributesOfItem(atPath: sourceURL.path)[.size]) as? NSNumber)?.int64Value ?? 0
        var bytesWritten: Int64 = 0
        var lastReportedBytes: Int64 = 0

        while true {
            if respectTaskCancellation {
                try Task.checkCancellation()
            }
            let chunk = try openedSourceHandle.read(upToCount: Self.copyBufferSize) ?? Data()
            if chunk.isEmpty {
                break
            }
            try destinationHandle.write(contentsOf: chunk)
            bytesWritten += Int64(chunk.count)
            if fileSize > 0 {
                let shouldReportProgress = (bytesWritten - lastReportedBytes) >= Self.copyProgressStepBytes || bytesWritten == fileSize
                if shouldReportProgress {
                    let progress = min(max(Double(bytesWritten) / Double(fileSize), 0), 1)
                    onProgress?(progress)
                    lastReportedBytes = bytesWritten
                }
            }
        }

        if respectTaskCancellation {
            try Task.checkCancellation()
        }
        try destinationHandle.synchronize()
        try destinationHandle.close()
        destinationClosed = true
        try openedSourceHandle.close()
        sourceClosed = true
        onProgress?(1)
        if respectTaskCancellation {
            try Task.checkCancellation()
        }
    }

    private func removeUploadArtifactIfStillOwned(
        remotePath: String,
        rootAnchor: RootAnchor,
        expectedIdentity: FileArtifactIdentity?
    ) {
        guard let expectedIdentity,
              let currentURL = try? containedPath(forRemotePath: remotePath, rootAnchor: rootAnchor).url,
              let currentIdentity = try? FileArtifactIdentity(pathEntryAt: currentURL),
              currentIdentity == expectedIdentity else { return }
        try? FileManager.default.removeItem(at: currentURL)
    }

    private func mapStorageError(_ error: Error) -> Error {
        if error is CancellationError {
            return error
        }
        if let storageError = error as? RemoteStorageClientError {
            return storageError
        }
        if hasLostAccessToExternalVolume(after: error) {
            return RemoteStorageClientError.externalStorageUnavailable
        }
        return RemoteStorageClientError.underlying(error)
    }

    // True when a copy failed because the destination already existed (copyItem refused it),
    // distinct from a mid-copy fault that may have left a partial. Cocoa surfaces this as
    // NSFileWriteFileExistsError, often wrapping a POSIX EEXIST.
    nonisolated private static func isDestinationExistsError(_ error: Error) -> Bool {
        var pending: [NSError] = [error as NSError]
        var visited = Set<String>()
        while let ns = pending.popLast() {
            guard visited.insert("\(ns.domain)#\(ns.code)").inserted else { continue }
            if ns.domain == NSCocoaErrorDomain, ns.code == NSFileWriteFileExistsError { return true }
            if ns.domain == NSPOSIXErrorDomain, ns.code == Int(EEXIST) { return true }
            if let underlying = ns.userInfo[NSUnderlyingErrorKey] as? NSError {
                pending.append(underlying)
            }
        }
        return false
    }

    private func isLikelyBookmarkAccessFailure(_ error: Error) -> Bool {
        if let storageError = error as? RemoteStorageClientError {
            switch storageError {
            case .externalStorageUnavailable:
                return true
            case .underlying(let underlying):
                return isLikelyBookmarkAccessFailure(underlying)
            default:
                return false
            }
        }

        let nsError = error as NSError
        guard nsError.domain == NSCocoaErrorDomain else { return false }
        let candidateCodes: Set<Int> = [
            NSFileNoSuchFileError,
            NSFileReadNoSuchFileError,
            NSFileReadNoPermissionError,
            NSFileWriteNoPermissionError,
            NSFileReadUnknownError,
            NSFileWriteUnknownError
        ]
        return candidateCodes.contains(nsError.code)
    }

    private func hasLostAccessToExternalVolume(after error: Error) -> Bool {
        let posixCodes = Self.posixErrorCodes(in: error)
        if !posixCodes.isDisjoint(with: [Int(EIO), Int(ENODEV), Int(ESTALE)]) {
            return true
        }
        if !posixCodes.isDisjoint(with: [Int(ENOENT), Int(EACCES), Int(EPERM)]) {
            return rootAnchorValidationFails()
        }

        let candidateCodes: Set<Int> = [
            NSFileNoSuchFileError,
            NSFileReadNoSuchFileError,
            NSFileReadNoPermissionError,
            NSFileWriteNoPermissionError,
            NSFileReadUnknownError,
            NSFileWriteUnknownError
        ]
        let nsError = error as NSError
        guard nsError.domain == NSCocoaErrorDomain,
              candidateCodes.contains(nsError.code) else { return false }
        return rootAnchorValidationFails()
    }

    private func rootAnchorValidationFails() -> Bool {
        guard let rootAnchor else { return false }
        do {
            _ = try validateRootAnchor(rootAnchor)
            return false
        } catch {
            return true
        }
    }

    nonisolated private static func posixErrorCodes(in error: Error) -> Set<Int> {
        var pending = [error as NSError]
        var visited = Set<String>()
        var result = Set<Int>()
        while let current = pending.popLast() {
            guard visited.insert("\(current.domain)#\(current.code)").inserted else { continue }
            if current.domain == NSPOSIXErrorDomain {
                result.insert(current.code)
            }
            if let underlying = current.userInfo[NSUnderlyingErrorKey] as? NSError {
                pending.append(underlying)
            }
        }
        return result
    }
}

private final class SecurityScopeLease: @unchecked Sendable {
    private let lock = NSLock()
    private var activeURL: URL?
    private var abandoned = false

    func adoptStartedAccess(to url: URL) -> Bool {
        let adopted = lock.withLock {
            guard !abandoned, activeURL == nil else { return false }
            activeURL = url
            return true
        }
        if !adopted {
            url.stopAccessingSecurityScopedResource()
        }
        return adopted
    }

    func release() {
        let url = lock.withLock { () -> URL? in
            let url = activeURL
            activeURL = nil
            return url
        }
        url?.stopAccessingSecurityScopedResource()
    }

    func abandon() {
        let url = lock.withLock { () -> URL? in
            guard !abandoned else { return nil }
            abandoned = true
            let url = activeURL
            activeURL = nil
            return url
        }
        url?.stopAccessingSecurityScopedResource()
    }
}

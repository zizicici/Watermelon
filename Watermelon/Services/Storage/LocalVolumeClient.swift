import Foundation

final actor LocalVolumeClient: RemoteStorageClientProtocol {
    struct Config {
        let rootBookmarkData: Data
        let onBookmarkRefreshed: ((BookmarkRefreshPayload) -> Void)?
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
    private static let uploadBufferSize = 8 * 1024 * 1024
    private static let uploadProgressStepBytes: Int64 = 8 * 1024 * 1024

    private var config: Config
    private let bookmarkStore: SecurityScopedBookmarkStore
    private var rootURL: URL?
    private var isAccessing = false

    init(
        config: Config,
        bookmarkStore: SecurityScopedBookmarkStore = SecurityScopedBookmarkStore()
    ) {
        self.config = config
        self.bookmarkStore = bookmarkStore
    }

    nonisolated func shouldSetModificationDate() -> Bool {
        false
    }

    deinit {
        if isAccessing {
            rootURL?.stopAccessingSecurityScopedResource()
        }
    }

    func connect() async throws {
        if rootURL != nil { return }
        let resolved: SecurityScopedBookmarkStore.ResolvedBookmark
        do {
            resolved = try bookmarkStore.resolveBookmarkData(config.rootBookmarkData)
        } catch {
            if isLikelyBookmarkAccessFailure(error) {
                throw RemoteStorageClientError.externalStorageUnavailable
            }
            throw mapStorageError(error)
        }
        if let refreshed = resolved.refreshedBookmarkData {
            config = Config(rootBookmarkData: refreshed, onBookmarkRefreshed: config.onBookmarkRefreshed)
            config.onBookmarkRefreshed?(BookmarkRefreshPayload(bookmarkData: refreshed, displayPath: resolved.url.path))
        }
        guard resolved.url.startAccessingSecurityScopedResource() else {
            throw RemoteStorageClientError.externalStorageUnavailable
        }
        rootURL = resolved.url
        isAccessing = true
    }

    func disconnect() async {
        guard isAccessing else { return }
        rootURL?.stopAccessingSecurityScopedResource()
        rootURL = nil
        isAccessing = false
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        let root = try requireRootURL()
        do {
            let values = try root.resourceValues(forKeys: [
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
        let root = try requireRootURL()
        do {
            let directoryURL = try remoteFileURL(forRemotePath: path, rootURL: root)
            let urls = try FileManager.default.contentsOfDirectory(
                at: directoryURL,
                includingPropertiesForKeys: Self.prefetchedResourceKeys,
                options: []
            )
            return try urls.map { url in
                try makeEntry(fileURL: url, rootURL: root)
            }
        } catch {
            throw mapStorageError(error)
        }
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let root = try requireRootURL()
        do {
            let url = try remoteFileURL(forRemotePath: path, rootURL: root)
            guard FileManager.default.fileExists(atPath: url.path) else {
                return nil
            }
            return try makeEntry(fileURL: url, rootURL: root)
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
        var destinationURLForCleanup: URL?
        var shouldCleanupDestinationOnFailure = false
        let root = try requireRootURL()
        do {
            if respectTaskCancellation {
                try Task.checkCancellation()
            }

            let destinationURL = try remoteFileURL(forRemotePath: remotePath, rootURL: root)
            destinationURLForCleanup = destinationURL
            let parentURL = destinationURL.deletingLastPathComponent()
            try FileManager.default.createDirectory(at: parentURL, withIntermediateDirectories: true)
            if FileManager.default.fileExists(atPath: destinationURL.path) {
                try FileManager.default.removeItem(at: destinationURL)
                shouldCleanupDestinationOnFailure = true
            }

            if !respectTaskCancellation {
                // Fast path: prefer system copy path only when cancellation responsiveness
                // does not matter. Large external-volume copies are otherwise impossible to interrupt.
                do {
                    shouldCleanupDestinationOnFailure = true
                    try FileManager.default.copyItem(at: localURL, to: destinationURL)
                    onProgress?(1)
                    return
                } catch {
                    let mappedError = mapStorageError(error)
                    if shouldAbortChunkedFallback(after: mappedError) {
                        throw mappedError
                    }

                    if FileManager.default.fileExists(atPath: destinationURL.path) {
                        try? FileManager.default.removeItem(at: destinationURL)
                    }
                }
            }

            shouldCleanupDestinationOnFailure = true
            guard FileManager.default.createFile(atPath: destinationURL.path, contents: nil) else {
                throw NSError(
                    domain: NSCocoaErrorDomain,
                    code: NSFileWriteUnknownError,
                    userInfo: [NSLocalizedDescriptionKey: "Failed to create destination file at \(destinationURL.path)"]
                )
            }

            let sourceHandle = try FileHandle(forReadingFrom: localURL)
            let destinationHandle = try FileHandle(forWritingTo: destinationURL)
            defer {
                try? sourceHandle.close()
                try? destinationHandle.close()
            }

            let fileSize = ((try? FileManager.default.attributesOfItem(atPath: localURL.path)[.size]) as? NSNumber)?.int64Value ?? 0
            var bytesWritten: Int64 = 0
            var lastReportedBytes: Int64 = 0

            while true {
                if respectTaskCancellation {
                    try Task.checkCancellation()
                }
                let chunk = try sourceHandle.read(upToCount: Self.uploadBufferSize) ?? Data()
                if chunk.isEmpty {
                    break
                }
                try destinationHandle.write(contentsOf: chunk)
                bytesWritten += Int64(chunk.count)
                if fileSize > 0 {
                    let shouldReportProgress = (bytesWritten - lastReportedBytes) >= Self.uploadProgressStepBytes || bytesWritten == fileSize
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
            onProgress?(1)
        } catch {
            if shouldCleanupDestinationOnFailure, let destinationURLForCleanup {
                try? FileManager.default.removeItem(at: destinationURLForCleanup)
            }
            throw mapStorageError(error)
        }
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        let root = try requireRootURL()
        do {
            let url = try remoteFileURL(forRemotePath: path, rootURL: root)
            try FileManager.default.setAttributes([.modificationDate: date], ofItemAtPath: url.path)
        } catch {
            throw mapStorageError(error)
        }
    }

    func download(remotePath: String, localURL: URL) async throws {
        let root = try requireRootURL()
        do {
            try Task.checkCancellation()
            let sourceURL = try remoteFileURL(forRemotePath: remotePath, rootURL: root)
            let parentURL = localURL.deletingLastPathComponent()
            try FileManager.default.createDirectory(at: parentURL, withIntermediateDirectories: true)
            if FileManager.default.fileExists(atPath: localURL.path) {
                try FileManager.default.removeItem(at: localURL)
            }
            try FileManager.default.copyItem(at: sourceURL, to: localURL)
            try Task.checkCancellation()
        } catch {
            throw mapStorageError(error)
        }
    }

    func exists(path: String) async throws -> Bool {
        let root = try requireRootURL()
        do {
            let url = try remoteFileURL(forRemotePath: path, rootURL: root)
            return FileManager.default.fileExists(atPath: url.path)
        } catch {
            throw mapStorageError(error)
        }
    }

    func delete(path: String) async throws {
        let root = try requireRootURL()
        do {
            let normalized = RemotePathBuilder.normalizePath(path)
            guard normalized != "/" else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            let url = try remoteFileURL(forRemotePath: normalized, rootURL: root)
            guard FileManager.default.fileExists(atPath: url.path) else { return }
            try FileManager.default.removeItem(at: url)
        } catch {
            throw mapStorageError(error)
        }
    }

    func createDirectory(path: String) async throws {
        let root = try requireRootURL()
        do {
            let normalized = RemotePathBuilder.normalizePath(path)
            guard normalized != "/" else { return }
            let url = try remoteFileURL(forRemotePath: normalized, rootURL: root)
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        } catch {
            throw mapStorageError(error)
        }
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        let root = try requireRootURL()
        do {
            let sourceURL = try remoteFileURL(forRemotePath: sourcePath, rootURL: root)
            let destinationURL = try remoteFileURL(forRemotePath: destinationPath, rootURL: root)
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

    private func requireRootURL() throws -> URL {
        guard let rootURL else {
            throw RemoteStorageClientError.notConnected
        }
        return rootURL
    }

    private func remoteFileURL(forRemotePath remotePath: String, rootURL: URL) throws -> URL {
        let normalized = RemotePathBuilder.normalizePath(remotePath)
        if normalized == "/" {
            return rootURL
        }

        let relative = String(normalized.dropFirst())
        let components = relative.split(separator: "/")
        guard !components.contains(where: { $0 == ".." || $0 == "." }) else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        var url = rootURL
        for component in components where !component.isEmpty {
            url.appendPathComponent(String(component), isDirectory: false)
        }
        return url
    }

    private func makeEntry(fileURL: URL, rootURL: URL) throws -> RemoteStorageEntry {
        let values = try fileURL.resourceValues(forKeys: Set(Self.prefetchedResourceKeys))
        let fullPath = normalizedRemotePath(for: fileURL, rootURL: rootURL)
        return RemoteStorageEntry(
            path: fullPath,
            name: values.name ?? fileURL.lastPathComponent,
            isDirectory: values.isDirectory ?? false,
            size: Int64(values.fileSize ?? 0),
            creationDate: values.creationDate,
            modificationDate: values.contentModificationDate
        )
    }

    private func normalizedRemotePath(for fileURL: URL, rootURL: URL) -> String {
        let rootPath = rootURL.standardizedFileURL.path
        let fullPath = fileURL.standardizedFileURL.path
        let rootPathWithSeparator = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
        guard fullPath == rootPath || fullPath.hasPrefix(rootPathWithSeparator) else {
            return "/"
        }

        let suffix = String(fullPath.dropFirst(rootPath.count))
        if suffix.isEmpty {
            return "/"
        }
        return RemotePathBuilder.normalizePath(suffix)
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

    private func shouldAbortChunkedFallback(after error: Error) -> Bool {
        if let storageError = error as? RemoteStorageClientError {
            switch storageError {
            case .externalStorageUnavailable, .invalidConfiguration:
                return true
            case .underlying(let underlying):
                return shouldAbortChunkedFallback(after: underlying)
            default:
                return false
            }
        }

        let nsError = error as NSError
        guard nsError.domain == NSCocoaErrorDomain else { return false }
        let fatalCodes: Set<Int> = [
            NSFileNoSuchFileError,
            NSFileReadNoSuchFileError,
            NSFileReadNoPermissionError,
            NSFileWriteNoPermissionError,
            NSFileWriteOutOfSpaceError,
            NSFileWriteVolumeReadOnlyError
        ]
        return fatalCodes.contains(nsError.code)
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
        guard candidateCodes.contains(nsError.code) else { return false }
        guard let rootURL else { return false }

        var isDirectory: ObjCBool = false
        if !FileManager.default.fileExists(atPath: rootURL.path, isDirectory: &isDirectory) {
            return true
        }

        do {
            _ = try rootURL.resourceValues(forKeys: [.isDirectoryKey])
            return false
        } catch {
            return true
        }
    }
}

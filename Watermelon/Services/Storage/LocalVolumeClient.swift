import Foundation

final actor LocalVolumeClient: RemoteStorageClientProtocol {
    struct Config {
        let rootBookmarkData: Data
    }

    private let config: Config
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

    deinit {
        if isAccessing {
            rootURL?.stopAccessingSecurityScopedResource()
        }
    }

    func connect() async throws {
        if rootURL != nil { return }
        let resolved: URL
        do {
            resolved = try bookmarkStore.resolveBookmarkData(config.rootBookmarkData)
        } catch {
            throw mapStorageError(error)
        }
        guard resolved.startAccessingSecurityScopedResource() else {
            throw RemoteStorageClientError.externalStorageUnavailable
        }
        rootURL = resolved
        isAccessing = true
    }

    func disconnect() async {
        guard isAccessing else { return }
        rootURL?.stopAccessingSecurityScopedResource()
        rootURL = nil
        isAccessing = false
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let root = try requireRootURL()
        do {
            let directoryURL = try remoteFileURL(forRemotePath: path, rootURL: root)
            let urls = try FileManager.default.contentsOfDirectory(
                at: directoryURL,
                includingPropertiesForKeys: nil,
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
        let root = try requireRootURL()
        do {
            if respectTaskCancellation {
                try Task.checkCancellation()
            }

            let destinationURL = try remoteFileURL(forRemotePath: remotePath, rootURL: root)
            let parentURL = destinationURL.deletingLastPathComponent()
            try FileManager.default.createDirectory(at: parentURL, withIntermediateDirectories: true)
            if FileManager.default.fileExists(atPath: destinationURL.path) {
                try FileManager.default.removeItem(at: destinationURL)
            }
            try FileManager.default.copyItem(at: localURL, to: destinationURL)
            onProgress?(1)
        } catch {
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
                try FileManager.default.removeItem(at: destinationURL)
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
        guard !components.contains(where: { $0 == ".." }) else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        var url = rootURL
        for component in components where !component.isEmpty {
            url.appendPathComponent(String(component), isDirectory: false)
        }
        return url
    }

    private func makeEntry(fileURL: URL, rootURL: URL) throws -> RemoteStorageEntry {
        let values = try fileURL.resourceValues(forKeys: [.isDirectoryKey, .creationDateKey, .contentModificationDateKey, .fileSizeKey, .nameKey])
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
        guard fullPath.hasPrefix(rootPath) else {
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
        if RemoteStorageClientError.isLikelyExternalStorageUnavailable(error) {
            return RemoteStorageClientError.externalStorageUnavailable
        }
        return RemoteStorageClientError.underlying(error)
    }
}

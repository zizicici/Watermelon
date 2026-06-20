import Foundation
@testable import Watermelon

// Real on-disk remote backed by a temp directory. Unlike InMemoryRemoteStorageClient it performs
// genuine FileManager I/O, so a fresh-backup run lands real bytes at real paths — used as the
// local-volume artifact evidence for the Lite cutover (LocalVolumeClient itself needs a
// security-scoped bookmark that a sandboxed test can't mint).
actor DiskBackedRemoteStorageClient: RemoteStorageClientProtocol {
    let rootURL: URL
    private let fileManager = FileManager.default

    init(rootURL: URL) {
        self.rootURL = rootURL
        try? fileManager.createDirectory(at: rootURL, withIntermediateDirectories: true)
    }

    nonisolated func shouldSetModificationDate() -> Bool { true }
    nonisolated func shouldLimitUploadRetries(for _: Error) -> Bool { false }

    func connect() async throws {}
    func disconnect() async {}
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let dir = localURL(for: path)
        var isDir: ObjCBool = false
        guard fileManager.fileExists(atPath: dir.path, isDirectory: &isDir), isDir.boolValue else {
            throw RemoteErrorFixtures.notFound
        }
        let children = try fileManager.contentsOfDirectory(
            at: dir,
            includingPropertiesForKeys: [.isDirectoryKey, .fileSizeKey, .contentModificationDateKey]
        )
        return try children.map { url in
            let values = try url.resourceValues(forKeys: [.isDirectoryKey, .fileSizeKey, .contentModificationDateKey])
            let isDirectory = values.isDirectory ?? false
            return RemoteStorageEntry(
                path: normalize(path) + "/" + url.lastPathComponent,
                name: url.lastPathComponent,
                isDirectory: isDirectory,
                size: Int64(values.fileSize ?? 0),
                creationDate: nil,
                modificationDate: values.contentModificationDate
            )
        }
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let url = localURL(for: path)
        var isDir: ObjCBool = false
        guard fileManager.fileExists(atPath: url.path, isDirectory: &isDir) else { return nil }
        let values = try? url.resourceValues(forKeys: [.fileSizeKey, .contentModificationDateKey])
        return RemoteStorageEntry(
            path: normalize(path),
            name: url.lastPathComponent,
            isDirectory: isDir.boolValue,
            size: Int64(values?.fileSize ?? 0),
            creationDate: nil,
            modificationDate: values?.contentModificationDate
        )
    }

    func upload(
        localURL source: URL,
        remotePath: String,
        respectTaskCancellation _: Bool,
        onProgress _: ((Double) -> Void)?
    ) async throws {
        try await upload(
            localURL: source,
            remotePath: remotePath,
            mode: .replace,
            respectTaskCancellation: false,
            onProgress: nil
        )
    }

    func upload(
        localURL source: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        respectTaskCancellation _: Bool,
        onProgress _: ((Double) -> Void)?
    ) async throws {
        let dest = localURL(for: remotePath)
        try fileManager.createDirectory(at: dest.deletingLastPathComponent(), withIntermediateDirectories: true)
        if fileManager.fileExists(atPath: dest.path) {
            if mode == .createIfAbsent {
                throw remoteStorageNameCollisionError(path: remotePath)
            }
            try fileManager.removeItem(at: dest)
        }
        try fileManager.copyItem(at: source, to: dest)
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try? fileManager.setAttributes([.modificationDate: date], ofItemAtPath: localURL(for: path).path)
    }

    func download(remotePath: String, localURL destination: URL) async throws {
        let source = localURL(for: remotePath)
        guard fileManager.fileExists(atPath: source.path) else { throw RemoteErrorFixtures.notFound }
        if fileManager.fileExists(atPath: destination.path) {
            try fileManager.removeItem(at: destination)
        }
        try fileManager.copyItem(at: source, to: destination)
    }

    func exists(path: String) async throws -> Bool {
        fileManager.fileExists(atPath: localURL(for: path).path)
    }

    func delete(path: String) async throws {
        let url = localURL(for: path)
        if fileManager.fileExists(atPath: url.path) {
            try fileManager.removeItem(at: url)
        }
    }

    func createDirectory(path: String) async throws {
        try fileManager.createDirectory(at: localURL(for: path), withIntermediateDirectories: true)
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        let source = localURL(for: sourcePath)
        let dest = localURL(for: destinationPath)
        guard fileManager.fileExists(atPath: source.path) else { throw RemoteErrorFixtures.notFound }
        try fileManager.createDirectory(at: dest.deletingLastPathComponent(), withIntermediateDirectories: true)
        if fileManager.fileExists(atPath: dest.path) {
            try fileManager.removeItem(at: dest)
        }
        try fileManager.moveItem(at: source, to: dest)
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        let source = localURL(for: sourcePath)
        let dest = localURL(for: destinationPath)
        try fileManager.createDirectory(at: dest.deletingLastPathComponent(), withIntermediateDirectories: true)
        if fileManager.fileExists(atPath: dest.path) {
            try fileManager.removeItem(at: dest)
        }
        try fileManager.copyItem(at: source, to: dest)
    }

    // MARK: - Helpers

    private func normalize(_ path: String) -> String {
        "/" + path.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }

    private func localURL(for path: String) -> URL {
        let relative = path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)
        return relative.reduce(rootURL) { $0.appendingPathComponent($1) }
    }
}

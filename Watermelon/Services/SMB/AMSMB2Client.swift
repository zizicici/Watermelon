import Foundation

#if canImport(AMSMB2)
import AMSMB2
#endif

final class AMSMB2Client: SMBClientProtocol {
    private let config: SMBServerConfig

    #if canImport(AMSMB2)
    private let manager: SMB2Manager
    #endif

    init(config: SMBServerConfig) throws {
        self.config = config

        #if canImport(AMSMB2)
        let normalizedHost = config.host.replacingOccurrences(of: "smb://", with: "")
        guard let url = URL(string: "smb://\(normalizedHost):\(config.port)") else {
            throw SMBClientError.invalidConfiguration
        }

        let user = [config.domain, config.username]
            .compactMap { value -> String? in
                guard let value, !value.isEmpty else { return nil }
                return value
            }
            .joined(separator: ";")

        let credential = URLCredential(user: user.isEmpty ? config.username : user, password: config.password, persistence: .forSession)

        guard let manager = SMB2Manager(url: url, credential: credential) else {
            throw SMBClientError.invalidConfiguration
        }

        self.manager = manager
        #endif
    }

    func connect() async throws {
        #if canImport(AMSMB2)
        try await manager.connectShare(name: config.shareName)
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func disconnect() async {
        #if canImport(AMSMB2)
        try? await manager.disconnectShare(gracefully: false)
        #endif
    }

    func list(path: String) async throws -> [SMBRemoteEntry] {
        #if canImport(AMSMB2)
        let remotePath = RemotePathBuilder.normalizePath(path)
        let items = try await manager.contentsOfDirectory(atPath: remotePath, recursive: false)

        return items.compactMap { values in
            let name = (values[.nameKey] as? String) ?? ""
            if name == "." || name == ".." { return nil }

            let isDirectory = (values[.isDirectoryKey] as? Bool) ?? false
            let sizeValue = values[.fileSizeKey] as? NSNumber
            let creationDate = values[.creationDateKey] as? Date
            let modificationDate = values[.contentModificationDateKey] as? Date

            let fullPath = RemotePathBuilder.normalizePath(remotePath + "/" + name)
            return SMBRemoteEntry(
                path: fullPath,
                name: name,
                isDirectory: isDirectory,
                size: sizeValue?.int64Value ?? 0,
                creationDate: creationDate,
                modificationDate: modificationDate
            )
        }
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func metadata(path: String) async throws -> SMBRemoteEntry? {
        #if canImport(AMSMB2)
        let remotePath = RemotePathBuilder.normalizePath(path)
        do {
            let values = try await manager.attributesOfItem(atPath: remotePath)
            let name = (values[.nameKey] as? String) ?? (remotePath as NSString).lastPathComponent
            let isDirectory = (values[.isDirectoryKey] as? Bool)
                ?? ((values[.fileResourceTypeKey] as? URLFileResourceType) == .directory)
            let sizeValue = values[.fileSizeKey] as? NSNumber
            let creationDate = values[.creationDateKey] as? Date
            let modificationDate = values[.contentModificationDateKey] as? Date

            return SMBRemoteEntry(
                path: remotePath,
                name: name,
                isDirectory: isDirectory,
                size: sizeValue?.int64Value ?? 0,
                creationDate: creationDate,
                modificationDate: modificationDate
            )
        } catch {
            return nil
        }
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool) async throws {
        #if canImport(AMSMB2)
        try await manager.uploadItem(
            at: localURL,
            toPath: RemotePathBuilder.normalizePath(remotePath),
            progress: { _ in
                guard respectTaskCancellation else { return true }
                return !Task.isCancelled
            }
        )
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        #if canImport(AMSMB2)
        try await manager.setAttributes(
            attributes: [.contentModificationDateKey: date],
            ofItemAtPath: RemotePathBuilder.normalizePath(path)
        )
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func download(remotePath: String, localURL: URL) async throws {
        #if canImport(AMSMB2)
        try await manager.downloadItem(
            atPath: RemotePathBuilder.normalizePath(remotePath),
            to: localURL,
            progress: { _, _ in !Task.isCancelled }
        )
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func exists(path: String) async throws -> Bool {
        #if canImport(AMSMB2)
        do {
            _ = try await manager.attributesOfItem(atPath: RemotePathBuilder.normalizePath(path))
            return true
        } catch {
            return false
        }
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func delete(path: String) async throws {
        #if canImport(AMSMB2)
        try await manager.removeItem(atPath: RemotePathBuilder.normalizePath(path))
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func createDirectory(path: String) async throws {
        #if canImport(AMSMB2)
        let normalized = RemotePathBuilder.normalizePath(path)
        guard normalized != "/" else { return }

        var runningPath = ""
        let components = normalized.split(separator: "/")
        for component in components {
            runningPath += "/\(component)"
            if try await exists(path: runningPath) {
                continue
            }
            do {
                try await manager.createDirectory(atPath: runningPath)
            } catch {
                if !(try await exists(path: runningPath)) {
                    throw error
                }
            }
        }
        #else
        throw SMBClientError.unavailable
        #endif
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        #if canImport(AMSMB2)
        try await manager.moveItem(atPath: RemotePathBuilder.normalizePath(sourcePath), toPath: RemotePathBuilder.normalizePath(destinationPath))
        #else
        throw SMBClientError.unavailable
        #endif
    }
}

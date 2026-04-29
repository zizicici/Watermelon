import Foundation

#if canImport(AMSMB2)
import AMSMB2
#endif

final class AMSMB2Client: RemoteStorageClientProtocol, @unchecked Sendable {
    private let config: SMBServerConfig

    #if canImport(AMSMB2)
    private let manager: SMB2Manager
    #endif

    init(config: SMBServerConfig) throws {
        self.config = config

        #if canImport(AMSMB2)
        let normalizedHost = config.host.replacingOccurrences(of: "smb://", with: "")
        guard let url = URL(string: "smb://\(normalizedHost):\(config.port)") else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        let user = [config.domain, config.username]
            .compactMap { value -> String? in
                guard let value, !value.isEmpty else { return nil }
                return value
            }
            .joined(separator: ";")

        let credential = URLCredential(user: user.isEmpty ? config.username : user, password: config.password, persistence: .forSession)

        guard let manager = SMB2Manager(url: url, credential: credential) else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        self.manager = manager
        #endif
    }

    func shouldSetModificationDate() -> Bool {
        true
    }

    func connect() async throws {
        #if canImport(AMSMB2)
        try await manager.connectShare(name: config.shareName)
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func disconnect() async {
        #if canImport(AMSMB2)
        try? await manager.disconnectShare(gracefully: false)
        #endif
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        #if canImport(AMSMB2)
        let queryPath = RemotePathBuilder.normalizePath(config.basePath)
        let attributes = try await manager.attributesOfFileSystem(forPath: queryPath)

        let available = (attributes[.systemFreeSize] as? NSNumber)?.int64Value
        let total = (attributes[.systemSize] as? NSNumber)?.int64Value
        if available == nil, total == nil {
            return nil
        }
        return RemoteStorageCapacity(availableBytes: available, totalBytes: total)
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
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
            return RemoteStorageEntry(
                path: fullPath,
                name: name,
                isDirectory: isDirectory,
                size: sizeValue?.int64Value ?? 0,
                creationDate: creationDate,
                modificationDate: modificationDate
            )
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
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

            return RemoteStorageEntry(
                path: remotePath,
                name: name,
                isDirectory: isDirectory,
                size: sizeValue?.int64Value ?? 0,
                creationDate: creationDate,
                modificationDate: modificationDate
            )
        } catch {
            if SMBErrorClassifier.isNotFound(error) {
                return nil
            }
            throw error
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        #if canImport(AMSMB2)
        let expectedByteCount = Self.fileSizeInBytes(for: localURL)
        let normalizedRemotePath = RemotePathBuilder.normalizePath(remotePath)
        do {
            try await manager.uploadItem(
                at: localURL,
                toPath: normalizedRemotePath,
                progress: { value in
                    if let normalized = Self.normalizedProgressValue(value, expectedByteCount: expectedByteCount) {
                        onProgress?(normalized)
                    }
                    guard respectTaskCancellation else { return true }
                    return !Task.isCancelled
                }
            )
            if respectTaskCancellation, Task.isCancelled {
                await cleanupCancelledUploadIfNeeded(
                    remotePath: normalizedRemotePath
                )
                throw CancellationError()
            }
        } catch {
            if respectTaskCancellation, Task.isCancelled {
                await cleanupCancelledUploadIfNeeded(
                    remotePath: normalizedRemotePath
                )
                throw CancellationError()
            }
            throw error
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        #if canImport(AMSMB2)
        guard Self.isSafeSMBFileDate(date) else { return }
        try await manager.setAttributes(
            attributes: [.contentModificationDateKey: date],
            ofItemAtPath: RemotePathBuilder.normalizePath(path)
        )
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    private static func normalizedProgressValue<T>(_ value: T, expectedByteCount: Int64?) -> Double? {
        if let double = value as? Double {
            return normalizedFraction(fromRawProgress: double, expectedByteCount: expectedByteCount)
        }
        if let float = value as? Float {
            return normalizedFraction(fromRawProgress: Double(float), expectedByteCount: expectedByteCount)
        }
        if let number = value as? NSNumber {
            return normalizedFraction(fromRawProgress: number.doubleValue, expectedByteCount: expectedByteCount)
        }
        return nil
    }

    private static func normalizedFraction(fromRawProgress raw: Double, expectedByteCount: Int64?) -> Double {
        let nonNegativeRaw = max(raw, 0)
        if nonNegativeRaw <= 1 {
            return min(max(nonNegativeRaw, 0), 1)
        }
        guard let expectedByteCount, expectedByteCount > 0 else {
            return min(max(nonNegativeRaw, 0), 1)
        }
        return min(max(nonNegativeRaw / Double(expectedByteCount), 0), 1)
    }

    private static func fileSizeInBytes(for fileURL: URL) -> Int64? {
        guard let values = try? fileURL.resourceValues(forKeys: [.fileSizeKey]),
              let fileSize = values.fileSize else {
            return nil
        }
        return Int64(fileSize)
    }

    private static func isSafeSMBFileDate(_ date: Date) -> Bool {
        let seconds = date.timeIntervalSince1970
        // AMSMB2 converts Date to timespec synchronously and traps on non-finite values.
        guard seconds.isFinite else { return false }

        // SMB file times are Windows FILETIME based. Stay inside that usable range so
        // libsmb2 does not underflow or receive dates many servers cannot represent.
        let windowsFileTimeMinimumSeconds: TimeInterval = -11_644_473_600 // 1601-01-01 UTC
        let conservativeMaximumSeconds: TimeInterval = 253_402_300_799 // 9999-12-31 23:59:59 UTC
        return seconds >= windowsFileTimeMinimumSeconds && seconds <= conservativeMaximumSeconds
    }

    private func cleanupCancelledUploadIfNeeded(remotePath: String) async {
        #if canImport(AMSMB2)
        do {
            let values = try await manager.attributesOfItem(atPath: remotePath)
            guard ((values[.isDirectoryKey] as? Bool) ?? false) == false else { return }
            try? await manager.removeItem(atPath: remotePath)
        } catch {
            if SMBErrorClassifier.isNotFound(error) {
                return
            }
        }
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
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func exists(path: String) async throws -> Bool {
        #if canImport(AMSMB2)
        do {
            _ = try await manager.attributesOfItem(atPath: RemotePathBuilder.normalizePath(path))
            return true
        } catch {
            if SMBErrorClassifier.isNotFound(error) {
                return false
            }
            throw error
        }
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func delete(path: String) async throws {
        #if canImport(AMSMB2)
        let normalized = RemotePathBuilder.normalizePath(path)
        guard normalized != "/" else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        do {
            try await manager.removeItem(atPath: normalized)
        } catch {
            if SMBErrorClassifier.isNotFound(error) {
                return
            }
            throw error
        }
        #else
        throw RemoteStorageClientError.unavailable
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
        throw RemoteStorageClientError.unavailable
        #endif
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        #if canImport(AMSMB2)
        try await manager.moveItem(atPath: RemotePathBuilder.normalizePath(sourcePath), toPath: RemotePathBuilder.normalizePath(destinationPath))
        #else
        throw RemoteStorageClientError.unavailable
        #endif
    }
}

enum SMBErrorClassifier {
    private static let notFoundStatusTokens: Set<String> = [
        "0XC000000F", // STATUS_NO_SUCH_FILE
        "0XC0000034", // STATUS_OBJECT_NAME_NOT_FOUND
        "0XC000003A", // STATUS_OBJECT_PATH_NOT_FOUND
        "0XC0000225", // STATUS_NOT_FOUND
        "STATUS_NO_SUCH_FILE",
        "STATUS_BAD_NETWORK_NAME",
        "STATUS_OBJECT_NAME_NOT_FOUND",
        "STATUS_OBJECT_PATH_INVALID",
        "STATUS_OBJECT_PATH_NOT_FOUND",
        "STATUS_OBJECT_PATH_SYNTAX_BAD",
        "STATUS_DFS_EXIT_PATH_FOUND",
        "STATUS_DELETE_PENDING",
        "STATUS_REDIRECTOR_NOT_STARTED",
        "STATUS_NOT_FOUND"
    ]

    private static let nameCollisionStatusTokens: Set<String> = [
        "0XC0000035", // STATUS_OBJECT_NAME_COLLISION
        "STATUS_OBJECT_NAME_COLLISION"
    ]

    private static let connectionUnavailableStatusTokens: Set<String> = [
        "0XC0000037", // STATUS_PORT_DISCONNECTED
        "0XC00000B5", // STATUS_IO_TIMEOUT
        "0XC00000C3", // STATUS_INVALID_NETWORK_RESPONSE
        "0XC00000C9", // STATUS_NETWORK_NAME_DELETED
        "0XC0000128", // STATUS_FILE_CLOSED
        "0XC000020C", // STATUS_CONNECTION_DISCONNECTED
        "0XC000020D", // STATUS_CONNECTION_RESET
        "0XC000023A", // STATUS_CONNECTION_INVALID
        "0XC0000241", // STATUS_CONNECTION_ABORTED
        "0XC000026E", // STATUS_VOLUME_DISMOUNTED
        "STATUS_PORT_DISCONNECTED",
        "STATUS_IO_TIMEOUT",
        "STATUS_INVALID_NETWORK_RESPONSE",
        "STATUS_NETWORK_NAME_DELETED",
        "STATUS_FILE_CLOSED",
        "STATUS_CONNECTION_DISCONNECTED",
        "STATUS_CONNECTION_RESET",
        "STATUS_CONNECTION_INVALID",
        "STATUS_CONNECTION_ABORTED",
        "STATUS_VOLUME_DISMOUNTED"
    ]

    private static let connectionUnavailablePOSIXCodes: Set<POSIXErrorCode> = [
        .ENETRESET,
        .ETIMEDOUT,
        .ENOTCONN,
        .ECONNRESET,
        .ECONNABORTED
    ]

    static func isNotFound(_ error: Error) -> Bool {
        if containsAnyStatusToken(notFoundStatusTokens, in: error) {
            return true
        }
        return posixErrorCodes(in: error).contains(.ENOENT)
    }

    static func isNameCollision(_ error: Error) -> Bool {
        if containsAnyStatusToken(nameCollisionStatusTokens, in: error) {
            return true
        }
        return posixErrorCodes(in: error).contains(.EEXIST)
    }

    static func isConnectionUnavailable(_ error: Error) -> Bool {
        if containsAnyStatusToken(connectionUnavailableStatusTokens, in: error) {
            return true
        }
        return !posixErrorCodes(in: error).isDisjoint(with: connectionUnavailablePOSIXCodes)
    }

    private static func containsAnyStatusToken(_ statusTokens: Set<String>, in error: Error) -> Bool {
        for description in errorDescriptions(for: error) {
            let uppercased = description.uppercased()
            if statusTokens.contains(where: { uppercased.contains($0) }) {
                return true
            }
        }
        return false
    }

    private static func posixErrorCodes(in error: Error) -> Set<POSIXErrorCode> {
        Set(nsErrors(for: error).compactMap { nsError in
            guard nsError.domain == NSPOSIXErrorDomain else { return nil }
            return POSIXErrorCode(rawValue: Int32(nsError.code))
        })
    }

    private static func errorDescriptions(for error: Error) -> [String] {
        nsErrors(for: error).flatMap { nsError -> [String] in
            var values: [String] = [nsError.localizedDescription]
            if let explicit = nsError.userInfo[NSLocalizedDescriptionKey] as? String {
                values.append(explicit)
            }
            return values
        }
    }

    private static func nsErrors(for error: Error) -> [NSError] {
        var collected: [NSError] = []
        var pending: [NSError] = [error as NSError]
        var visited: Set<String> = []

        while let current = pending.popLast() {
            let key = "\(current.domain)#\(current.code)#\(current.localizedDescription)"
            guard visited.insert(key).inserted else { continue }
            collected.append(current)

            if let underlying = current.userInfo[NSUnderlyingErrorKey] as? NSError {
                pending.append(underlying)
            } else if let underlying = current.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(underlying as NSError)
            }
        }

        return collected
    }
}

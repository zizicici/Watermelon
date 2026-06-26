import Citadel
import Crypto
import Foundation
import NIOCore
import NIOSSH

final actor SFTPClient: RemoteStorageClientProtocol {
    private nonisolated static let chunkSize = 32 * 1024
    // Citadel 0.12.1's listDirectory leaks server-side directory handles; recycle
    // the channel after N lists so the leak can't exhaust the server's budget.
    private nonisolated static let listReconnectThreshold = 32

    struct Config: Sendable {
        let host: String
        let port: Int
        let username: String
        let credential: SFTPCredentialBlob
        let expectedHostKeyFingerprintSHA256: String
    }

    private let config: Config
    private var sshClient: SSHClient?
    private var sftpClient: Citadel.SFTPClient?
    private var listOperationsSinceReconnect = 0

    init(config: Config) {
        self.config = config
    }

    func connect() async throws {
        if let active = sftpClient, active.isActive { return }
        if sftpClient != nil || sshClient != nil {
            await tearDown()
        }

        let auth = try Self.makeAuthenticationMethod(
            username: config.username,
            credential: config.credential
        )
        let validator = SSHHostKeyValidator.custom(
            HostKeyValidator(mode: .pin(expected: config.expectedHostKeyFingerprintSHA256))
        )
        let port = config.port == 0 ? 22 : config.port
        func establish(_ host: String) async throws -> SSHClient {
            try await Self.connectSSH(
                host: host,
                port: port,
                authenticationMethod: auth,
                hostKeyValidator: validator,
                reconnect: .never
            )
        }

        let ssh: SSHClient
        if let ip = await HostnameResolver.resolvedIPv4(config.host), ip != config.host {
            do {
                ssh = try await establish(ip)
            } catch {
                if error is CancellationError || Task.isCancelled { throw error }
                // A stale/wrong resolved IP can fail in any way; retry the canonical hostname (still pin-checked).
                ssh = try await establish(config.host)
            }
        } else {
            ssh = try await establish(config.host)
        }
        do {
            sftpClient = try await ssh.openSFTP()
        } catch {
            try? await ssh.close()
            throw error
        }
        sshClient = ssh
    }

    func disconnect() async {
        await tearDown()
    }

    private func tearDown() async {
        let sftp = sftpClient
        let ssh = sshClient
        sftpClient = nil
        sshClient = nil
        listOperationsSinceReconnect = 0
        if let sftp {
            try? await sftp.close()
        }
        if let ssh {
            try? await ssh.close()
        }
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        nil
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        if listOperationsSinceReconnect >= Self.listReconnectThreshold {
            await tearDown()
            try await connect()
        }
        let client = try ensureClient()
        let resolved = RemotePathBuilder.normalizePath(path)
        let names = try await client.listDirectory(atPath: resolved)
        listOperationsSinceReconnect += 1

        var results: [RemoteStorageEntry] = []
        for name in names {
            for component in name.components {
                let filename = component.filename
                if filename == "." || filename == ".." { continue }
                results.append(makeEntry(parent: resolved, name: filename, attributes: component.attributes))
            }
        }
        return results
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let client = try ensureClient()
        let resolved = RemotePathBuilder.normalizePath(path)
        do {
            let attrs = try await client.getAttributes(at: resolved)
            let parent = (resolved as NSString).deletingLastPathComponent
            let name = (resolved as NSString).lastPathComponent
            return makeEntry(parent: parent.isEmpty ? "/" : parent, name: name, attributes: attrs)
        } catch {
            if SFTPErrorClassifier.isNotFound(error) { return nil }
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
        let client = try ensureClient()
        let resolved = RemotePathBuilder.normalizePath(remotePath)
        let totalBytes = (try? FileManager.default.attributesOfItem(atPath: localURL.path)[.size] as? NSNumber)?.int64Value ?? 0

        let handle = try FileHandle(forReadingFrom: localURL)
        defer { try? handle.close() }

        let flags: SFTPOpenFileFlags = mode == .replace
            ? [.write, .create, .truncate]
            : [.write, .create, .forceCreate]
        let file: Citadel.SFTPFile
        if respectTaskCancellation { try Task.checkCancellation() }
        do {
            file = try await client.openFile(filePath: resolved, flags: flags)
        } catch {
            if error is CancellationError { throw error }
            if respectTaskCancellation, Task.isCancelled { throw CancellationError() }
            if mode == .createIfAbsent {
                if Self.isCreateIfAbsentCollision(error) {
                    throw remoteStorageNameCollisionError(path: remotePath)
                }
                if (try? await metadata(path: remotePath)) != nil {
                    throw remoteStorageNameCollisionError(path: remotePath)
                }
            }
            throw error
        }
        var offset: UInt64 = 0
        var lastProgress: Double = 0
        let allocator = ByteBufferAllocator()

        // A successful open owns this upload's destination, so write failures can clean it up.
        do {
            while true {
                if respectTaskCancellation { try Task.checkCancellation() }
                let chunk = try handle.read(upToCount: Self.chunkSize) ?? Data()
                if chunk.isEmpty { break }
                var buffer = allocator.buffer(capacity: chunk.count)
                buffer.writeBytes(chunk)
                try await file.write(buffer, at: offset)
                offset += UInt64(chunk.count)

                if let onProgress, totalBytes > 0 {
                    let progress = min(1.0, Double(offset) / Double(totalBytes))
                    if progress - lastProgress >= 0.01 || progress >= 1.0 {
                        onProgress(progress)
                        lastProgress = progress
                    }
                }
            }
            try await file.close()
        } catch {
            try? await file.close()
            // A successful open owns this destination (`.replace` truncated it, `.createIfAbsent`/
            // `.forceCreate` proved it absent), so remove the partial body for both modes — a leaked
            // half-written create-if-absent file (e.g. a write-lock claim) would block the next attempt.
            try? await client.remove(at: resolved)
            throw error
        }
        if let onProgress, totalBytes > 0, lastProgress < 1.0 {
            onProgress(1.0)
        }
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        let client = try ensureClient()
        let attrs = SFTPFileAttributes(
            accessModificationTime: SFTPFileAttributes.AccessModificationTime(
                accessTime: date,
                modificationTime: date
            )
        )
        try await client.setAttributes(at: RemotePathBuilder.normalizePath(path), to: attrs)
    }

    func download(remotePath: String, localURL: URL) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        try Task.checkCancellation()
        let client = try ensureClient()
        let resolved = RemotePathBuilder.normalizePath(remotePath)
        let totalBytes = onProgress == nil ? 0 : ((try? await metadata(path: remotePath))?.size ?? 0)
        var lastProgress = 0.0

        let parentURL = localURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: parentURL, withIntermediateDirectories: true)
        let temporaryURL = parentURL.appendingPathComponent(".sftp-download-\(UUID().uuidString).tmp")
        guard FileManager.default.createFile(atPath: temporaryURL.path, contents: nil) else {
            throw RemoteStorageClientError.unavailable
        }
        defer { try? FileManager.default.removeItem(at: temporaryURL) }

        let file = try await client.openFile(filePath: resolved, flags: [.read])
        let handle: FileHandle
        do {
            handle = try FileHandle(forWritingTo: temporaryURL)
        } catch {
            try? await file.close()
            throw error
        }

        do {
            var offset: UInt64 = 0
            while true {
                try Task.checkCancellation()
                let buffer = try await file.read(from: offset, length: UInt32(Self.chunkSize))
                let readableBytes = buffer.readableBytes
                if readableBytes == 0 { break }
                try handle.write(contentsOf: Data(buffer.readableBytesView))
                offset += UInt64(readableBytes)
                if let onProgress, totalBytes > 0 {
                    let progress = min(1.0, Double(offset) / Double(totalBytes))
                    if progress - lastProgress >= 0.01 || progress >= 1.0 {
                        onProgress(progress)
                        lastProgress = progress
                    }
                }
            }
        } catch {
            try? handle.close()
            try? await file.close()
            throw error
        }

        let remoteCloseError: Error?
        do {
            try await file.close()
            remoteCloseError = nil
        } catch {
            remoteCloseError = error
        }
        // Local close always runs (release the FD); flush errors must abort before we publish.
        try handle.close()
        if let error = remoteCloseError { throw error }

        try? FileManager.default.removeItem(at: localURL)
        try FileManager.default.moveItem(at: temporaryURL, to: localURL)
        if let onProgress, lastProgress < 1.0 {
            onProgress(1.0)
        }
    }

    func exists(path: String) async throws -> Bool {
        try await metadata(path: path) != nil
    }

    func delete(path: String) async throws {
        let client = try ensureClient()
        let resolved = RemotePathBuilder.normalizePath(path)
        guard resolved != "/" else { throw RemoteStorageClientError.invalidConfiguration }
        do {
            try await client.remove(at: resolved)
        } catch {
            if SFTPErrorClassifier.isNotFound(error) { return }
            // remove() rejects directories — only spend the extra round-trips when that's the actual cause.
            if let attrs = try? await client.getAttributes(at: resolved), Self.isDirectory(attrs) {
                do {
                    try await client.rmdir(at: resolved)
                } catch let rmdirError {
                    if SFTPErrorClassifier.isNotFound(rmdirError) { return }
                    throw rmdirError
                }
                return
            }
            throw error
        }
    }

    func createDirectory(path: String) async throws {
        let client = try ensureClient()
        let resolved = RemotePathBuilder.normalizePath(path)
        guard resolved != "/" else { return }

        var runningPath = ""
        for component in resolved.split(separator: "/") {
            runningPath += "/\(component)"
            do {
                try await client.createDirectory(atPath: runningPath)
            } catch {
                if let existing = try? await client.getAttributes(at: runningPath), Self.isDirectory(existing) {
                    continue
                }
                throw error
            }
        }
    }

    // SFTP v3 rename fails when target exists; surface verbatim so the caller's
    // .bak-dance recovery can run instead of a delete-then-rename masking layer.
    func move(from sourcePath: String, to destinationPath: String) async throws {
        let client = try ensureClient()
        try await client.rename(
            at: RemotePathBuilder.normalizePath(sourcePath),
            to: RemotePathBuilder.normalizePath(destinationPath)
        )
    }

    // SFTP has no native server-side copy — fall back to download+upload through a local temp.
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        let resolvedSource = RemotePathBuilder.normalizePath(sourcePath)
        let resolvedDest = RemotePathBuilder.normalizePath(destinationPath)
        let tmpURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("sftp-copy-\(UUID().uuidString).tmp")
        defer { try? FileManager.default.removeItem(at: tmpURL) }
        try await download(remotePath: resolvedSource, localURL: tmpURL)
        try await upload(localURL: tmpURL, remotePath: resolvedDest, respectTaskCancellation: true, onProgress: nil)
    }

    // MARK: - Helpers

    private func ensureClient() throws -> Citadel.SFTPClient {
        guard let client = sftpClient, client.isActive else {
            throw RemoteStorageClientError.notConnected
        }
        return client
    }

    private func makeEntry(parent: String, name: String, attributes: SFTPFileAttributes) -> RemoteStorageEntry {
        RemoteStorageEntry(
            path: RemotePathBuilder.absolutePath(basePath: parent, remoteRelativePath: name),
            name: name,
            isDirectory: Self.isDirectory(attributes),
            size: Int64(attributes.size ?? 0),
            creationDate: nil,
            modificationDate: attributes.accessModificationTime?.modificationTime
        )
    }

    private nonisolated static func isDirectory(_ attrs: SFTPFileAttributes) -> Bool {
        guard let permissions = attrs.permissions else { return false }
        return (permissions & 0o170000) == 0o040000
    }

    private nonisolated static func isCreateIfAbsentCollision(_ error: Error) -> Bool {
        if let storage = error as? RemoteStorageClientError, case .underlying(let inner) = storage {
            return isCreateIfAbsentCollision(inner)
        }
        let ns = error as NSError
        if ns.domain == NSPOSIXErrorDomain, ns.code == Int(EEXIST) {
            return true
        }
        if let underlying = ns.userInfo[NSUnderlyingErrorKey] as? Error {
            return isCreateIfAbsentCollision(underlying)
        }
        return false
    }

    private nonisolated static func makeAuthenticationMethod(
        username: String,
        credential: SFTPCredentialBlob
    ) throws -> SSHAuthenticationMethod {
        switch credential {
        case .password(let password):
            return .passwordBased(username: username, password: password)
        case .privateKey(let pem, let passphrase):
            let decryption = passphrase.flatMap { $0.isEmpty ? nil : Data($0.utf8) }
            let detected: SSHKeyType
            do {
                detected = try SSHKeyDetection.detectPrivateKeyType(from: pem)
            } catch {
                throw RemoteStorageClientError.invalidConfiguration
            }
            switch detected {
            case .ed25519:
                let key = try Curve25519.Signing.PrivateKey(sshEd25519: pem, decryptionKey: decryption)
                return .ed25519(username: username, privateKey: key)
            case .rsa:
                let key = try Insecure.RSA.PrivateKey(sshRsa: pem, decryptionKey: decryption)
                return .rsa(username: username, privateKey: key)
            default:
                throw SFTPUnsupportedKeyTypeError(detectedType: detected.description)
            }
        }
    }

    private enum SSHAlgorithmMode {
        case modern
        case compatible

        var algorithms: SSHAlgorithms {
            switch self {
            case .modern:
                return SSHAlgorithms()
            case .compatible:
                return .all
            }
        }
    }

    private nonisolated static func connectSSH(
        host: String,
        port: Int,
        authenticationMethod: SSHAuthenticationMethod,
        hostKeyValidator: SSHHostKeyValidator,
        reconnect: SSHReconnectMode
    ) async throws -> SSHClient {
        var lastError: Error?
        for mode in [SSHAlgorithmMode.modern, .compatible] {
            do {
                return try await SSHClient.connect(
                    host: host,
                    port: port,
                    authenticationMethod: authenticationMethod,
                    hostKeyValidator: hostKeyValidator,
                    reconnect: reconnect,
                    algorithms: mode.algorithms
                )
            } catch {
                lastError = error
                guard mode == .modern, shouldRetryWithCompatibleAlgorithms(after: error) else {
                    throw error
                }
            }
        }
        throw lastError ?? RemoteStorageClientError.unavailable
    }

    private nonisolated static func shouldRetryWithCompatibleAlgorithms(after error: Error) -> Bool {
        if error is SFTPHostKeyMismatchError { return false }
        if error is HostKeyCaptureSentinel { return false }
        if error is AuthenticationFailed { return false }
        if let sshClientError = error as? SSHClientError {
            switch sshClientError {
            case .allAuthenticationOptionsFailed, .unsupportedPasswordAuthentication,
                 .unsupportedPrivateKeyAuthentication, .unsupportedHostBasedAuthentication:
                return false
            case .channelCreationFailed:
                return true
            }
        }
        if error is NIOSSHError { return true }
        if let citadel = error as? CitadelError {
            switch citadel {
            case .unauthorized:
                return false
            default:
                return true
            }
        }
        if error is RemoteStorageClientError { return false }
        return true
    }

    // Run at save time so a broken base path surfaces in the editor instead of at first backup.
    static func verifyBasePathWritable(config: Config, basePath: String) async throws {
        let client = SFTPClient(config: config)
        do {
            try await client.connect()
            try await client.createDirectory(path: basePath)

            let probeName = ".watermelon-probe-\(UUID().uuidString)"
            let probePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: probeName)
            let tmpURL = FileManager.default.temporaryDirectory.appendingPathComponent(probeName)
            try Data("watermelon-write-probe".utf8).write(to: tmpURL)
            defer { try? FileManager.default.removeItem(at: tmpURL) }

            try await client.upload(
                localURL: tmpURL,
                remotePath: probePath,
                respectTaskCancellation: true,
                onProgress: nil
            )
            // Don't swallow — write-but-not-delete is a broken account, not transient cleanup.
            try await client.delete(path: probePath)
        } catch {
            await client.disconnect()
            throw error
        }
        await client.disconnect()
    }

    // Two-phase TOFU: aborts at host-key validation so no credential is offered until the user confirms the fingerprint.
    nonisolated static func captureHostKeyFingerprint(host: String, port: Int) async throws -> String {
        // First-trust capture must pin the canonical hostname's key — no IPv4 fast path here, or a stale A
        // record could let the user trust (and pin) the wrong server's key.
        let validator = HostKeyValidator(mode: .captureAndAbort)
        do {
            _ = try await connectSSH(
                host: host,
                port: port == 0 ? 22 : port,
                authenticationMethod: .passwordBased(username: "", password: ""),
                hostKeyValidator: SSHHostKeyValidator.custom(validator),
                reconnect: .never
            )
        } catch {
            if validator.captured == nil { throw error }
        }
        guard let fingerprint = validator.captured else {
            throw RemoteStorageClientError.unavailable
        }
        return fingerprint
    }

    fileprivate nonisolated static func computeFingerprintSHA256(of publicKey: NIOSSHPublicKey) -> String {
        var buffer = ByteBufferAllocator().buffer(capacity: 1024)
        let bytesWritten = publicKey.write(to: &buffer)
        let bytes = Array(buffer.readableBytesView.prefix(bytesWritten))
        let digest = SHA256.hash(data: bytes)
        let base64 = Data(digest).base64EncodedString().replacingOccurrences(of: "=", with: "")
        return "SHA256:\(base64)"
    }
}

struct SFTPHostKeyMismatchError: LocalizedError, Equatable {
    let actual: String

    var errorDescription: String? {
        String.localizedStringWithFormat(
            String(localized: "sftp.error.hostKeyMismatch"),
            actual
        )
    }
}

struct SFTPUnsupportedKeyTypeError: LocalizedError, Equatable {
    let detectedType: String

    var errorDescription: String? {
        String.localizedStringWithFormat(
            String(localized: "auth.sftp.validation.unsupportedKeyType"),
            detectedType
        )
    }
}

private struct HostKeyCaptureSentinel: Error, Equatable {}

private nonisolated final class HostKeyValidator: NIOSSHClientServerAuthenticationDelegate, @unchecked Sendable {
    enum Mode {
        case pin(expected: String)
        case captureAndAbort
    }

    private let mode: Mode
    private let lock = NSLock()
    private var capturedFingerprint: String?

    init(mode: Mode) {
        self.mode = mode
    }

    var captured: String? {
        lock.lock(); defer { lock.unlock() }
        return capturedFingerprint
    }

    func validateHostKey(hostKey: NIOSSHPublicKey, validationCompletePromise: EventLoopPromise<Void>) {
        let actual = SFTPClient.computeFingerprintSHA256(of: hostKey)
        switch mode {
        case .pin(let expected):
            if actual == expected {
                validationCompletePromise.succeed(())
            } else {
                validationCompletePromise.fail(SFTPHostKeyMismatchError(actual: actual))
            }
        case .captureAndAbort:
            lock.lock()
            capturedFingerprint = actual
            lock.unlock()
            validationCompletePromise.fail(HostKeyCaptureSentinel())
        }
    }
}

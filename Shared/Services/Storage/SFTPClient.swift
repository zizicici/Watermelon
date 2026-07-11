import Citadel
import Crypto
import Foundation
import NIOCore
import NIOPosix
@preconcurrency import NIOSSH

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

        var effectivePort: Int { SFTPEndpoint.effectivePort(port) }
    }

    private let config: Config
    nonisolated private let verificationTemporaryFiles = VerificationTemporaryFileRegistry()
    nonisolated private let abandonmentHandle = SFTPAbandonmentHandle()
    nonisolated private let sshTransport = SFTPSSHTransportHandle()
    private var sshClient: SSHClient?
    private var sftpClient: Citadel.SFTPClient?
    private var listOperationsSinceReconnect = 0

    init(config: Config) {
        self.config = config
    }

    nonisolated func cancelActiveOperationsForAbandonment() {
        verificationTemporaryFiles.abandon()
        abandonmentHandle.abandon()
        sshTransport.abandon()
    }

    func connect() async throws {
        if let active = sftpClient, active.isActive { return }
        if sftpClient != nil || sshClient != nil {
            await tearDown()
        }

        let validator = SSHHostKeyValidator.custom(
            HostKeyValidator(mode: .pin(expected: config.expectedHostKeyFingerprintSHA256))
        )
        guard let socketHost = RemoteHostEndpoint.socketHost(config.host) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let port = config.effectivePort
        func establish(_ host: String) async throws -> SSHClient {
            try await Self.connectSSH(
                host: host,
                port: port,
                authenticationMethod: {
                    try Self.makeAuthenticationMethod(
                        username: config.username,
                        credential: config.credential
                    )
                },
                hostKeyValidator: validator,
                transport: sshTransport
            )
        }

        let ssh: SSHClient
        if let ip = await HostnameResolver.resolvedIPv4(socketHost), ip != socketHost {
            do {
                ssh = try await establish(ip)
            } catch {
                if error is CancellationError || Task.isCancelled { throw error }
                // A stale/wrong resolved IP can fail in any way; retry the canonical hostname (still pin-checked).
                ssh = try await establish(socketHost)
            }
        } else {
            ssh = try await establish(socketHost)
        }
        do {
            let sftp = try await ssh.openSFTP()
            guard abandonmentHandle.install(sftp) else {
                throw CancellationError()
            }
            sftpClient = sftp
        } catch {
            sshTransport.closeCurrent()
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
        if let abortTask = abandonmentHandle.takeAbortTaskAndClear() {
            await abortTask.value
        }
        if let sftp {
            try? await sftp.close()
        }
        sshTransport.closeCurrent()
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
        let temporaryFiles = VerificationTemporaryFileLease(urls: [temporaryURL])
        guard verificationTemporaryFiles.register(temporaryFiles) else { throw CancellationError() }
        defer { verificationTemporaryFiles.unregister(temporaryFiles) }
        try temporaryFiles.write([(Data(), temporaryURL)])

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
        authenticationMethod: @Sendable () throws -> SSHAuthenticationMethod,
        hostKeyValidator: SSHHostKeyValidator,
        transport: SFTPSSHTransportHandle
    ) async throws -> SSHClient {
        var lastError: Error?
        for mode in [SSHAlgorithmMode.modern, .compatible] {
            do {
                try Task.checkCancellation()
                let auth = SFTPSendableAuthenticationMethod(try authenticationMethod())
                var settings = SSHClientSettings(
                    host: host,
                    port: port,
                    authenticationMethod: { auth.value },
                    hostKeyValidator: hostKeyValidator
                )
                settings.algorithms = mode.algorithms
                settings.connectTimeout = .seconds(Int64(NetworkRecoveryPolicy.connectTimeout))
                let channel = try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
                    .channelInitializer { channel in
                        guard transport.install(channel) else {
                            return channel.eventLoop.makeFailedFuture(CancellationError())
                        }
                        return channel.eventLoop.makeSucceededVoidFuture()
                    }
                    .connectTimeout(settings.connectTimeout)
                    .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
                    .channelOption(ChannelOptions.socket(SocketOptionLevel(IPPROTO_TCP), TCP_NODELAY), value: 1)
                    .connect(host: host, port: port)
                    .get()
                do {
                    return try await SSHClient.connect(on: channel, settings: settings)
                } catch {
                    transport.clearAndClose(channel)
                    throw error
                }
            } catch {
                transport.closeCurrent()
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
        if error is SFTPSSHTransportChannelClosed { return true }
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
        try await RemoteStorageWriteVerifier.verify(
            client: client,
            cleanupClientFactory: { SFTPClient(config: config) },
            basePath: basePath
        )
    }

    // Two-phase TOFU: aborts at host-key validation so no credential is offered until the user confirms the fingerprint.
    nonisolated static func captureHostKeyFingerprint(
        host: String,
        port: Int,
        timeout: TimeInterval = NetworkRecoveryPolicy.connectTimeout
    ) async throws -> String {
        guard let socketHost = RemoteHostEndpoint.socketHost(host) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let deadline = Date().addingTimeInterval(max(0, timeout))
        let transport = SFTPSSHTransportHandle()
        let outcome = await NetworkRecovery.boundedAttempt(
            deadline: deadline,
            onAbandon: { transport.abandon() },
            reap: { (_: Result<String, Error>) in transport.closeCurrent() },
            op: {
                var lastError: Error = RemoteStorageClientError.unavailable
                for mode in [SSHAlgorithmMode.modern, .compatible] {
                    do {
                        try Task.checkCancellation()
                        guard Date() < deadline else { throw RemoteStorageClientError.unavailable }
                        return .success(try await captureHostKeyFingerprintAttempt(
                            host: socketHost,
                            port: SFTPEndpoint.effectivePort(port),
                            mode: mode,
                            deadline: deadline,
                            transport: transport
                        ))
                    } catch {
                        lastError = error
                        if error is CancellationError || Task.isCancelled { return .failure(error) }
                        guard mode == .modern, shouldRetryWithCompatibleAlgorithms(after: error) else {
                            return .failure(error)
                        }
                    }
                }
                return .failure(lastError)
            }
        )
        switch outcome {
        case .completed(.success(let fingerprint)):
            return fingerprint
        case .completed(.failure(let error)):
            throw error
        case .timedOut:
            if Task.isCancelled { throw CancellationError() }
            throw RemoteStorageClientError.unavailable
        }
    }

    private nonisolated static func captureHostKeyFingerprintAttempt(
        host: String,
        port: Int,
        mode: SSHAlgorithmMode,
        deadline: Date,
        transport: SFTPSSHTransportHandle
    ) async throws -> String {
        let result = HostKeyCaptureResult()
        let validator = HostKeyValidator(mode: .captureAndAbort { result.succeed($0) })
        var configuration = SSHClientConfiguration(
            userAuthDelegate: NoCredentialUserAuthenticationDelegate(),
            serverAuthDelegate: validator
        )
        applyCaptureAlgorithms(mode.algorithms, to: &configuration)
        let connectSeconds = max(1, Int64(ceil(deadline.timeIntervalSinceNow)))
        let bootstrap = ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .channelInitializer { channel in
                channel.closeFuture.whenComplete { _ in
                    result.fail(SFTPSSHTransportChannelClosed())
                }
                guard transport.install(channel) else {
                    return channel.eventLoop.makeFailedFuture(CancellationError())
                }
                let handler = NIOSSHHandler(
                    role: .client(configuration),
                    allocator: channel.allocator,
                    inboundChildChannelInitializer: { child, _ in child.close() }
                )
                let terminal = SFTPHostKeyCaptureTerminalHandler(result: result)
                do {
                    try channel.pipeline.syncOperations.addHandlers(handler, terminal)
                    return channel.eventLoop.makeSucceededVoidFuture()
                } catch {
                    result.fail(error)
                    channel.close(promise: nil)
                    return channel.eventLoop.makeFailedFuture(error)
                }
            }
            .connectTimeout(.seconds(connectSeconds))
            .channelOption(ChannelOptions.socket(SocketOptionLevel(SOL_SOCKET), SO_REUSEADDR), value: 1)
            .channelOption(ChannelOptions.socket(SocketOptionLevel(IPPROTO_TCP), TCP_NODELAY), value: 1)

        let channel = try await bootstrap.connect(host: host, port: port).get()
        defer { transport.clearAndClose(channel) }
        return try await result.value()
    }

    private nonisolated static func applyCaptureAlgorithms(
        _ algorithms: SSHAlgorithms,
        to configuration: inout SSHClientConfiguration
    ) {
        if let modification = algorithms.transportProtectionSchemes {
            switch modification {
            case .add(let values):
                configuration.transportProtectionSchemes.append(contentsOf: values)
                values.forEach { NIOSSHAlgorithms.register(transportProtectionScheme: $0) }
            case .replace(let values):
                configuration.transportProtectionSchemes = values
                values.forEach { NIOSSHAlgorithms.register(transportProtectionScheme: $0) }
            }
        }
        if let modification = algorithms.keyExchangeAlgorithms {
            switch modification {
            case .add(let values):
                configuration.keyExchangeAlgorithms.append(contentsOf: values)
                values.forEach { NIOSSHAlgorithms.register(keyExchangeAlgorithm: $0) }
            case .replace(let values):
                configuration.keyExchangeAlgorithms = values
                values.forEach { NIOSSHAlgorithms.register(keyExchangeAlgorithm: $0) }
            }
        }
        if algorithms.publicKeyAlgorihtms != nil {
            NIOSSHAlgorithms.register(
                publicKey: Insecure.RSA.PublicKey.self,
                signature: Insecure.RSA.Signature.self
            )
        }
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

private final class SFTPAbandonmentHandle: @unchecked Sendable {
    private let lock = NSLock()
    private var client: Citadel.SFTPClient?
    private var abortTask: Task<Void, Never>?
    private var abandoned = false

    func install(_ client: Citadel.SFTPClient) -> Bool {
        lock.withLock {
            if abandoned {
                if abortTask == nil {
                    abortTask = Task.detached(priority: .utility) { try? await client.close() }
                }
                return false
            } else {
                self.client = client
                return true
            }
        }
    }

    func abandon() {
        lock.withLock {
            abandoned = true
            guard abortTask == nil, let client else { return }
            abortTask = Task.detached(priority: .utility) {
                try? await client.close()
            }
        }
    }

    func takeAbortTaskAndClear() -> Task<Void, Never>? {
        lock.withLock {
            let task = abortTask
            abortTask = nil
            client = nil
            return task
        }
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
private struct SFTPSSHTransportChannelClosed: Error {}

private struct SFTPSendableAuthenticationMethod: @unchecked Sendable {
    let value: SSHAuthenticationMethod

    init(_ value: SSHAuthenticationMethod) {
        self.value = value
    }
}

private final class SFTPHostKeyCaptureTerminalHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = Any

    private let result: HostKeyCaptureResult

    init(result: HostKeyCaptureResult) {
        self.result = result
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        result.fail(error)
        context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        result.fail(SFTPSSHTransportChannelClosed())
        context.fireChannelInactive()
    }
}

private final class SFTPSSHTransportHandle: @unchecked Sendable {
    private let lock = NSLock()
    private var channel: Channel?
    private var abandoned = false

    func install(_ channel: Channel) -> Bool {
        let shouldClose = lock.withLock {
            if abandoned { return true }
            self.channel = channel
            return false
        }
        if shouldClose { channel.close(promise: nil) }
        return !shouldClose
    }

    func abandon() {
        let channel = lock.withLock { () -> Channel? in
            guard !abandoned else { return nil }
            abandoned = true
            let channel = self.channel
            self.channel = nil
            return channel
        }
        channel?.close(promise: nil)
    }

    func clearAndClose(_ channel: Channel) {
        lock.withLock {
            if let active = self.channel,
               ObjectIdentifier(active as AnyObject) == ObjectIdentifier(channel as AnyObject) {
                self.channel = nil
            }
        }
        channel.close(promise: nil)
    }

    func closeCurrent() {
        let channel = lock.withLock { () -> Channel? in
            let channel = self.channel
            self.channel = nil
            return channel
        }
        channel?.close(promise: nil)
    }
}

private final class HostKeyCaptureResult: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<String, Error>?
    private var pending: Result<String, Error>?
    private var resolved = false

    func value() async throws -> String {
        try await withCheckedThrowingContinuation { continuation in
            let pending = lock.withLock { () -> Result<String, Error>? in
                if let pending = self.pending {
                    self.pending = nil
                    return pending
                }
                self.continuation = continuation
                return nil
            }
            if let pending { continuation.resume(with: pending) }
        }
    }

    func succeed(_ fingerprint: String) {
        resolve(.success(fingerprint))
    }

    func fail(_ error: Error) {
        resolve(.failure(error))
    }

    private func resolve(_ result: Result<String, Error>) {
        let continuation = lock.withLock { () -> CheckedContinuation<String, Error>? in
            guard !resolved else { return nil }
            resolved = true
            if let continuation = self.continuation {
                self.continuation = nil
                return continuation
            }
            pending = result
            return nil
        }
        continuation?.resume(with: result)
    }
}

private final class NoCredentialUserAuthenticationDelegate: NIOSSHClientUserAuthenticationDelegate, @unchecked Sendable {
    func nextAuthenticationType(
        availableMethods _: NIOSSHAvailableUserAuthenticationMethods,
        nextChallengePromise: EventLoopPromise<NIOSSHUserAuthenticationOffer?>
    ) {
        nextChallengePromise.succeed(nil)
    }
}

private nonisolated final class HostKeyValidator: NIOSSHClientServerAuthenticationDelegate, @unchecked Sendable {
    enum Mode {
        case pin(expected: String)
        case captureAndAbort(@Sendable (String) -> Void)
    }

    private let mode: Mode

    init(mode: Mode) {
        self.mode = mode
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
        case .captureAndAbort(let onCaptured):
            onCaptured(actual)
            validationCompletePromise.fail(HostKeyCaptureSentinel())
        }
    }
}

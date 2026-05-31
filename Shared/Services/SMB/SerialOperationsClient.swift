import Foundation

/// Enqueues before suspension so release cannot miss a waiter.
actor SerialOperationQueue {
    final class WaitHandle: @unchecked Sendable {
        private let lock = NSLock()
        private var continuation: CheckedContinuation<Bool, Never>?
        private var pendingResult: Bool?

        func resume(_ result: Bool) {
            let cont: CheckedContinuation<Bool, Never>? = lock.withLock {
                if let c = continuation {
                    continuation = nil
                    return c
                } else {
                    pendingResult = result
                    return nil
                }
            }
            cont?.resume(returning: result)
        }

        func wait() async -> Bool {
            await withCheckedContinuation { (cont: CheckedContinuation<Bool, Never>) in
                let immediate: Bool? = lock.withLock {
                    if let pending = pendingResult {
                        pendingResult = nil
                        return pending
                    } else {
                        continuation = cont
                        return nil
                    }
                }
                if let value = immediate {
                    cont.resume(returning: value)
                }
            }
        }
    }

    private var inFlight = false
    private var waiters: [WaitHandle] = []

    func run<T: Sendable>(_ body: @Sendable () async throws -> T) async throws -> T {
        try Task.checkCancellation()
        let handle = acquireOrEnqueue()
        if let handle {
            let acquired = await withTaskCancellationHandler {
                await handle.wait()
            } onCancel: {
                Task { await self.cancelWaiter(handle) }
            }
            guard acquired else { throw CancellationError() }
        }
        do {
            // Catch cancellation that arrives after queue wake-up but before body execution.
            try Task.checkCancellation()
            let result = try await body()
            release()
            return result
        } catch {
            release()
            throw error
        }
    }

    func runUncancellable<T: Sendable>(_ body: @Sendable () async -> T) async -> T {
        let handle = acquireOrEnqueue()
        if let handle {
            _ = await handle.wait()
        }
        let result = await body()
        release()
        return result
    }

    func runIgnoringCancellation<T: Sendable>(_ body: @Sendable () async throws -> T) async throws -> T {
        let handle = acquireOrEnqueue()
        if let handle {
            _ = await handle.wait()
        }
        do {
            let result = try await body()
            release()
            return result
        } catch {
            release()
            throw error
        }
    }

    private func acquireOrEnqueue() -> WaitHandle? {
        if !inFlight {
            inFlight = true
            return nil
        }
        let handle = WaitHandle()
        waiters.append(handle)
        return handle
    }

    private func cancelWaiter(_ handle: WaitHandle) {
        if let idx = waiters.firstIndex(where: { $0 === handle }) {
            waiters.remove(at: idx)
            handle.resume(false)
        }
    }

    private func release() {
        if waiters.isEmpty {
            inFlight = false
        } else {
            let next = waiters.removeFirst()
            next.resume(true)
        }
    }
}

/// Metadata-only wrapper; data uploads would lose non-Sendable progress callbacks.
final class SerialOperationsClient: RemoteStorageClientProtocol, @unchecked Sendable {
    private let underlying: any RemoteStorageClientProtocol
    private let queue = SerialOperationQueue()

    init(_ underlying: any RemoteStorageClientProtocol) {
        self.underlying = underlying
    }

    var concurrencyMode: ClientConcurrencyMode { underlying.concurrencyMode }
    var isSerialized: Bool { true }
    var dataPathOverwriteRisk: DataPathOverwriteRisk { underlying.dataPathOverwriteRisk }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { underlying.backendNameCaseSensitivity }
    var moveIfAbsentGuarantee: CreateGuarantee { underlying.moveIfAbsentGuarantee }
    var readAfterWriteGraceSeconds: TimeInterval { underlying.readAfterWriteGraceSeconds }

    func shouldSetModificationDate() -> Bool { underlying.shouldSetModificationDate() }
    func shouldLimitUploadRetries(for error: Error) -> Bool { underlying.shouldLimitUploadRetries(for: error) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool {
        try await queue.run {
            try await self.underlying.supportsExclusiveMoveIfAbsent(forDestinationPath: destinationPath)
        }
    }
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        underlying.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }

    func connect() async throws {
        try await queue.run { try await self.underlying.connect() }
    }
    func disconnect() async {
        await queue.runUncancellable { await self.underlying.disconnect() }
    }
    func verifyWriteAccess() async throws {
        try await queue.run { try await self.underlying.verifyWriteAccess() }
    }
    func storageCapacity() async throws -> RemoteStorageCapacity? {
        try await queue.run { try await self.underlying.storageCapacity() }
    }
    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await queue.run { try await self.underlying.list(path: path) }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? {
        try await queue.run { try await self.underlying.metadata(path: path) }
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        // Wrapping a data-upload client is a programming error — progress would vanish.
        // Loud in debug, silent + dropped in release (matching the prior behavior).
        assert(onProgress == nil, "SerialOperationsClient is metadata-only; data-upload clients must not be wrapped (progress would silently disappear)")
        if !respectTaskCancellation {
            return try await queue.runIgnoringCancellation {
                try await self.underlying.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: false, onProgress: nil)
            }
        }
        try await queue.run {
            try await self.underlying.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: nil)
        }
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        assert(onProgress == nil, "SerialOperationsClient is metadata-only; data-upload clients must not be wrapped (progress would silently disappear)")
        if !respectTaskCancellation {
            return try await queue.runIgnoringCancellation {
                try await self.underlying.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: false, onProgress: nil)
            }
        }
        return try await queue.run {
            try await self.underlying.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: nil)
        }
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await queue.run { try await self.underlying.setModificationDate(date, forPath: path) }
    }
    func download(remotePath: String, localURL: URL) async throws {
        try await queue.run { try await self.underlying.download(remotePath: remotePath, localURL: localURL) }
    }
    func exists(path: String) async throws -> Bool {
        try await queue.run { try await self.underlying.exists(path: path) }
    }
    func delete(path: String) async throws {
        try await queue.run { try await self.underlying.delete(path: path) }
    }
    func createDirectory(path: String) async throws {
        try await queue.run { try await self.underlying.createDirectory(path: path) }
    }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await queue.run { try await self.underlying.move(from: sourcePath, to: destinationPath) }
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await queue.run { try await self.underlying.moveIfAbsent(from: sourcePath, to: destinationPath) }
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await queue.run { try await self.underlying.copy(from: sourcePath, to: destinationPath) }
    }
}

/// Wraps `client` only when it's a `.serialOnly` backend AND not already serialized.
/// `isSerialized` lets nested wrappers (e.g., future LoggingClient) opt out of
/// double-wrapping.
func wrapIfSerial(_ client: any RemoteStorageClientProtocol) -> any RemoteStorageClientProtocol {
    if client.isSerialized { return client }
    return client.concurrencyMode == .serialOnly ? SerialOperationsClient(client) : client
}

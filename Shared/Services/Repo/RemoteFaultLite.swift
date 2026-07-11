import Foundation
#if canImport(Citadel)
import Citadel
#endif

// One place for the bounded-network-recovery policy the backup/restore paths share, so adding recovery to a
// new phase reuses these numbers instead of re-deriving them.
nonisolated enum NetworkRecoveryPolicy {
    static let foregroundWindow: TimeInterval = 150   // within the lease expiry
    static let backgroundWindow: TimeInterval = 45    // within the BG-task grace window
    static func window(background: Bool) -> TimeInterval { background ? backgroundWindow : foregroundWindow }

    static let backoffBaseNanos: UInt64 = 1_000_000_000
    static let backoffCapNanos: UInt64 = 15_000_000_000

    // Per-attempt connect bound: a single connect that overruns this is abandoned so the retry loop can try
    // again (and so one hung connect can't eat a whole recovery window or a background-task grace period).
    static let connectTimeout: TimeInterval = 30
}

// What a single recovery attempt produced. `.abandoned` is for a bounded op preempted by its own deadline/abort
// (e.g. the pool connect race timing out) — distinct from a returned fault.
nonisolated enum NetworkRecoveryStep<Success> {
    case succeeded(Success)
    case failed(Error)
    case abandoned
}

nonisolated enum NetworkRecoveryResult<Success> {
    case succeeded(Success)
    case failed(Error)        // terminal / not-found — fail fast
    case exhausted(Error)     // window elapsed while still transient — resumable pause (last fault attached)
    case cancelled
    case stopped(Error)       // shouldStop fired; carries the last fault so the caller can defer without masking
}

// The single bounded exponential-backoff recovery loop: attempt-first, classify failures (cancellation →
// cancelled; retryable → back off within `deadline`; otherwise fail fast), ending early on cancellation or
// `shouldStop`. Callers map the result to their own return/throw shape (raw error vs resumable-pause sentinel).
nonisolated enum NetworkRecovery {
    static func run<Success>(
        deadline: Date,
        shouldStop: @Sendable () async -> Bool = { false },
        isRetryable: @Sendable (Error) -> Bool = { RemoteFaultLite.classify($0) == .retryable },
        attempt: @Sendable () async -> NetworkRecoveryStep<Success>
    ) async -> NetworkRecoveryResult<Success> {
        var delayNanos = NetworkRecoveryPolicy.backoffBaseNanos
        var lastError: Error = RemoteStorageClientError.unavailable
        var firstAttempt = true
        while true {
            if Task.isCancelled { return .cancelled }
            if await shouldStop() { return .stopped(lastError) }
            if Date() >= deadline { return .exhausted(lastError) }

            if !firstAttempt {
                let remainingNanos = UInt64(max(0, deadline.timeIntervalSinceNow) * 1_000_000_000)
                let jitterNanos = UInt64.random(in: 0 ... max(1, delayNanos / 2))
                do {
                    try await Task.sleep(nanoseconds: min(delayNanos + jitterNanos, remainingNanos))
                } catch {
                    return .cancelled
                }
                delayNanos = min(delayNanos * 2, NetworkRecoveryPolicy.backoffCapNanos)
                if Task.isCancelled { return .cancelled }
                if await shouldStop() { return .stopped(lastError) }
                if Date() >= deadline { return .exhausted(lastError) }
            }
            firstAttempt = false

            switch await attempt() {
            case .succeeded(let value):
                return .succeeded(value)
            case .abandoned:
                continue   // deadline/abort preempted it — loop top resolves to .exhausted / .stopped / .cancelled
            case .failed(let error):
                lastError = error
                if Task.isCancelled { return .cancelled }
                if await shouldStop() { return .stopped(lastError) }
                if RemoteFaultLite.classify(error) == .cancelled { return .cancelled }
                if isRetryable(error) { continue }
                return .failed(error)
            }
        }
    }

    enum BoundedAttemptResult<Value: Sendable>: Sendable {
        case completed(Value)
        case timedOut
    }

    // Bounds a single async op: races it against `deadline`, cancellation, and `abortIf`. If a non-op racer wins,
    // the op is cancelled and — should it still produce a value later — handed to `reap` (e.g. disconnect a stray
    // client), so an uncooperative connect/op can neither stall the caller nor leak a resource.
    static func boundedAttempt<Value: Sendable>(
        deadline: Date,
        abortIf shouldAbort: @escaping @Sendable () async -> Bool = { false },
        onAbandon: @escaping @Sendable () -> Void = {},
        reap: @escaping @Sendable (Value) async -> Void = { _ in },
        op: @escaping @Sendable () async -> Value
    ) async -> BoundedAttemptResult<Value> {
        let gate = NetworkRaceGate()
        let continuationBox = NetworkRaceContinuationBox()
        let abandonmentBarrier = NetworkAbandonmentBarrier()
        let opTask = Task {
            let value = await op()
            if gate.claim() {
                continuationBox.resume(returning: true)
            } else {
                await abandonmentBarrier.wait()
                Task.detached(priority: .utility) { await reap(value) }
            }
            return value
        }
        let deadlineNanos = UInt64(max(0, deadline.timeIntervalSinceNow) * 1_000_000_000)
        let timerTask = Task { try? await Task.sleep(nanoseconds: deadlineNanos) }
        let abortTask = Task { () async -> Void in
            while !Task.isCancelled {
                if await shouldAbort() { return }
                try? await Task.sleep(nanoseconds: 150_000_000)
            }
        }
        let opWon: Bool = await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
                continuationBox.install(continuation)
                Task {
                    _ = await timerTask.value
                    if gate.claim() { continuationBox.resume(returning: false) }
                }
                Task {
                    _ = await abortTask.value
                    if gate.claim() { continuationBox.resume(returning: false) }
                }
            }
        } onCancel: {
            opTask.cancel()
            timerTask.cancel()
            abortTask.cancel()
            if gate.cancelAndClaim() {
                continuationBox.resume(returning: false)
            }
        }
        timerTask.cancel()
        abortTask.cancel()
        if opWon, !gate.wasCancelled, !Task.isCancelled {
            return .completed(await opTask.value)
        }
        opTask.cancel()
        onAbandon()
        abandonmentBarrier.open()
        if opWon {
            Task.detached(priority: .utility) {
                await reap(await opTask.value)
            }
        }
        return .timedOut
    }

    // Bounds a single client.connect(): SMB/SFTP have no transport-level connect timeout, so a half-open connect
    // is abandoned at the deadline rather than stalling the caller, and a late success is disconnected. Throws the
    // connect error on failure, or .unavailable (retryable) when the deadline/cancellation wins.
    static func boundedConnect(_ client: any RemoteStorageClientProtocol, deadline: Date) async throws {
        let result = await boundedAttempt(
            deadline: deadline,
            onAbandon: { client.cancelActiveOperationsForAbandonment() },
            reap: { (_: Error?) in await client.reapAbandonedOperations() },
            op: { () async -> Error? in
                do { try await client.connect(); return nil } catch { return error }
            }
        )
        switch result {
        case .completed(let connectError):
            if let connectError { throw connectError }
        case .timedOut:
            throw RemoteStorageClientError.unavailable
        }
    }

    // Rides out transient connect faults within `deadline`: a fresh client per attempt, each connect bounded so a
    // half-open one is abandoned and retried. Returns the recovery result so the caller maps exhaustion to its own
    // shape (a resumable-pause sentinel vs a raw throw).
    static func connectRidingOut(
        deadline: Date,
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol
    ) async -> NetworkRecoveryResult<any RemoteStorageClientProtocol> {
        await run(deadline: deadline) {
            let clientHandle = NetworkAttemptClientHandle()
            // Cap the per-attempt connect at the cumulative deadline so the last attempt can't overrun the
            // recovery window (or a background-task grace period) by a full connectTimeout.
            let bounded = await boundedAttempt(
                deadline: min(deadline, Date().addingTimeInterval(NetworkRecoveryPolicy.connectTimeout)),
                onAbandon: { clientHandle.abandon() },
                reap: { (_: Result<any RemoteStorageClientProtocol, Error>) in await clientHandle.reap() },
                op: { () async -> Result<any RemoteStorageClientProtocol, Error> in
                    do {
                        let client = try makeClient()
                        guard clientHandle.install(client) else { throw CancellationError() }
                        try await client.connect()
                        return .success(client)
                    } catch {
                        return .failure(error)
                    }
                }
            )
            switch bounded {
            case .completed(.success(let client)):
                return .succeeded(client)
            case .completed(.failure(let error)):
                return .failed(error)
            case .timedOut:
                return .failed(RemoteStorageClientError.unavailable)   // per-attempt timeout → retry a fresh client
            }
        }
    }
}

// Single-resume guard so the racing child tasks can pick a winner synchronously without awaiting an actor.
nonisolated final class NetworkRaceGate: @unchecked Sendable {
    private let lock = NSLock()
    private var claimed = false
    private var cancelled = false

    var wasCancelled: Bool { lock.withLock { cancelled } }

    func claim() -> Bool {
        lock.withLock {
            if claimed { return false }
            claimed = true
            return true
        }
    }

    func cancelAndClaim() -> Bool {
        lock.withLock {
            cancelled = true
            if claimed { return false }
            claimed = true
            return true
        }
    }
}

nonisolated final class NetworkRaceContinuationBox: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Bool, Never>?
    private var pendingValue: Bool?

    func install(_ continuation: CheckedContinuation<Bool, Never>) {
        let pending = lock.withLock { () -> Bool? in
            if let pendingValue {
                self.pendingValue = nil
                return pendingValue
            }
            self.continuation = continuation
            return nil
        }
        if let pending {
            continuation.resume(returning: pending)
        }
    }

    func resume(returning value: Bool) {
        let continuation = lock.withLock { () -> CheckedContinuation<Bool, Never>? in
            if let continuation = self.continuation {
                self.continuation = nil
                return continuation
            }
            pendingValue = value
            return nil
        }
        continuation?.resume(returning: value)
    }
}

nonisolated final class NetworkAbandonmentBarrier: @unchecked Sendable {
    private let lock = NSLock()
    private var isOpen = false
    private var continuation: CheckedContinuation<Void, Never>?

    func wait() async {
        await withCheckedContinuation { continuation in
            let resumeNow = lock.withLock {
                if isOpen { return true }
                self.continuation = continuation
                return false
            }
            if resumeNow {
                continuation.resume()
            }
        }
    }

    func open() {
        let continuation = lock.withLock { () -> CheckedContinuation<Void, Never>? in
            guard !isOpen else { return nil }
            isOpen = true
            let continuation = self.continuation
            self.continuation = nil
            return continuation
        }
        continuation?.resume()
    }
}

nonisolated final class NetworkAttemptClientHandle: @unchecked Sendable {
    private let lock = NSLock()
    private var client: (any RemoteStorageClientProtocol)?
    private var abandoned = false

    func install(_ client: any RemoteStorageClientProtocol) -> Bool {
        let shouldAbort = lock.withLock {
            self.client = client
            return abandoned
        }
        if shouldAbort {
            client.cancelActiveOperationsForAbandonment()
        }
        return !shouldAbort
    }

    func abandon() {
        let client = lock.withLock { () -> (any RemoteStorageClientProtocol)? in
            guard !abandoned else { return nil }
            abandoned = true
            return self.client
        }
        client?.cancelActiveOperationsForAbandonment()
    }

    func reap() async {
        let client = lock.withLock { self.client }
        if let client {
            await client.reapAbandonedOperations()
        }
    }
}

// Shared fault classifier for lock/version/month probes. Collapses a remote error into one of four
// intentions so call sites can tell "the file really isn't there" from "the network blinked" without
// re-deriving backend-specific shapes.
nonisolated enum RemoteFaultLite {
    enum Category: Equatable, Sendable {
        case notFound
        case retryable
        case cancelled
        case terminal
    }

    static func classify(_ error: Error) -> Category {
        let chain = errorChain(error)
        if chain.contains(where: isCancellationNode) {
            return .cancelled
        }
        // Retryable is tested before notFound: a chain carrying a transient backend/session/transport
        // fault must not be read as object absence just because it also carries an ambiguous not-found
        // token. A clear absence (no retryable signal anywhere in the chain) still falls through to
        // .notFound. Fail closed toward "transient" when both are present.
        if isRetryable(chain) {
            return .retryable
        }
        if isNotFound(chain) {
            return .notFound
        }
        return .terminal
    }

    // MARK: - Cancellation

    private static func isCancellationNode(_ node: Error) -> Bool {
        if node is CancellationError { return true }
        if let liteError = node as? LiteRepoError, liteError.isCancellation { return true }
        let ns = node as NSError
        return ns.domain == NSURLErrorDomain && ns.code == NSURLErrorCancelled
    }

    // MARK: - Intentional absence

    private static func isNotFound(_ chain: [Error]) -> Bool {
        if s3IsNotFound(chain) { return true }
        if sftpIsNoSuchFile(chain) { return true }
        for node in chain {
            if SMBErrorClassifier.isNotFound(node) { return true }
            let ns = node as NSError
            if ns.domain == WebDAVClient.errorDomain, ns.code == 404 { return true }
            if ns.domain == NSPOSIXErrorDomain, ns.code == Int(ENOENT) { return true }
            if ns.domain == NSCocoaErrorDomain,
               ns.code == NSFileNoSuchFileError || ns.code == NSFileReadNoSuchFileError {
                return true
            }
        }
        return false
    }

    private static func s3IsNotFound(_ chain: [Error]) -> Bool {
        for node in chain {
            let ns = node as NSError
            guard ns.domain == S3ErrorClassifier.errorDomain else { continue }
            let serverCode = ns.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String
            if serverCode == "NoSuchKey" || serverCode == "NotFound" { return true }
            // NoSuchBucket is a configuration fault, not a missing object — leave it for `.terminal`.
            if serverCode == nil {
                let status = ns.userInfo[S3ErrorClassifier.userInfoStatusCodeKey] as? Int
                if status == 404 || ns.code == 404 { return true }
            }
        }
        return false
    }

    private static func sftpIsNoSuchFile(_ chain: [Error]) -> Bool {
        #if canImport(Citadel)
        for node in chain {
            if SFTPErrorClassifier.isNotFound(node) {
                return true
            }
        }
        #endif
        return false
    }

    // MARK: - Transport / probe uncertainty

    private static func isRetryable(_ chain: [Error]) -> Bool {
        if s3IsRetryable(chain) { return true }
        for node in chain {
            if let storage = node as? RemoteStorageClientError {
                switch storage {
                case .notConnected, .unavailable:
                    return true
                default:
                    break
                }
            }
            if SMBErrorClassifier.isConnectionUnavailable(node) { return true }
            if SFTPErrorClassifier.isConnectionUnavailable(node) { return true }
            let ns = node as NSError
            if ns.domain == NSURLErrorDomain, S3ErrorClassifier.isConnectionUnavailableURLErrorCode(ns.code) { return true }
            // WebDAV transport faults surface as NSURLErrorDomain (above); transient server statuses
            // (gateway / unavailable / rate-limit — not an ambiguous 500) surface on its own domain.
            if ns.domain == WebDAVClient.errorDomain, webdavRetryableHTTPStatuses.contains(ns.code) { return true }
            // A watchdog-detected stalled transfer (dead socket) is recoverable by reconnecting.
            if WebDAVClient.isStalledTransferTimeout(node) { return true }
            if S3Client.isStalledTransferTimeout(node) { return true }
        }
        return false
    }

    private static let retryableHTTPStatuses: Set<Int> = [408, 429, 500, 502, 503, 504]
    // 408 is a server-side request timeout (transient); 500 omitted as too ambiguous to auto-retry.
    private static let webdavRetryableHTTPStatuses: Set<Int> = [408, 429, 502, 503, 504]

    private static func s3IsRetryable(_ chain: [Error]) -> Bool {
        for node in chain {
            let ns = node as NSError
            guard ns.domain == S3ErrorClassifier.errorDomain else { continue }
            let status = (ns.userInfo[S3ErrorClassifier.userInfoStatusCodeKey] as? Int) ?? ns.code
            if let serverCode = ns.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String,
               let code = S3ErrorClassifier.S3ErrorCode(rawValue: serverCode) {
                // SlowDown / ServiceUnavailable / InternalError / throttles are the transient server codes;
                // a recognized client fault stays terminal even when wrapped in a 200 envelope.
                if !code.isClientFault { return true }
                continue
            }
            // An S3 embedded error (HTTP 200 carrying <Error>) with no/unrecognized code is a server-side
            // completion hiccup (e.g. multipart complete) — retry it rather than reading 200 as success.
            if status == 200 { return true }
            if retryableHTTPStatuses.contains(status) { return true }
        }
        return false
    }

    // MARK: - Error chain

    // Unwraps both RemoteStorageClientError.underlying and NSUnderlyingErrorKey so every node can be
    // inspected on its own terms. Depth-capped because a malformed userInfo could cycle.
    private static func errorChain(_ error: Error, maxDepth: Int = 32) -> [Error] {
        var result: [Error] = []
        var pending: [Error] = [error]
        while let next = pending.popLast(), result.count < maxDepth {
            result.append(next)
            if let storage = next as? RemoteStorageClientError, case .underlying(let inner) = storage {
                pending.append(inner)
                continue
            }
            let ns = next as NSError
            if let underlying = ns.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(underlying)
            }
        }
        return result
    }
}

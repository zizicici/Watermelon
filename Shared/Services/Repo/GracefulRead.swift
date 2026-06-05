import Foundation

/// Why a grace-backend read concluded absence. Lets callers distinguish "the object is genuinely
/// gone" from "the backend hasn't made our recent write visible yet" without each site re-deriving it.
nonisolated enum AbsenceKind: Sendable, Equatable {
    /// Confirmed absent: zero-grace backend, or the read-after-write deadline elapsed without the object appearing.
    case genuinelyAbsent
    /// Absent so far but still inside the backend's read-after-write grace budget — may be visibility lag.
    case visibilityLag
}

nonisolated enum GracefulReadResult<Value: Sendable>: Sendable {
    case found(Value)
    case absent(AbsenceKind)

    var value: Value? {
        if case .found(let v) = self { return v }
        return nil
    }
}

/// One place that encodes the read-after-write grace loop every metadata reader used to hand-write:
/// `guard grace > 0 else { zero-grace is authoritative }`, then retry until
/// `metadataReadAfterWriteDeadline` before promoting absence to `.genuinelyAbsent`.
///
/// Two surfaces:
/// - `read` for the common shape: a self-write read where final absence is a value (`.absent`).
/// - `retryWithinGrace` for sites that map the outcome themselves — byte-equality verdicts, or
///   fail-closed readers that must RETHROW a retained not-found past the deadline rather than
///   return absence. Those keep their own backoff schedule and error policy.
nonisolated enum GracefulRead {
    static let defaultPollIntervalMs = 150

    /// Backoff between attempts. `.fixed` polls at a constant interval; `.exponential` doubles the
    /// base each attempt up to `maxShift` doublings, matching sites that ramp `200 * (1 << n)`.
    enum Backoff: Sendable {
        case fixed(ms: Int)
        case exponential(baseMs: Int, maxShift: Int)

        func delayMs(attempt: Int) -> Int {
            switch self {
            case .fixed(let ms):
                return ms
            case .exponential(let baseMs, let maxShift):
                return baseMs * (1 << min(attempt, maxShift))
            }
        }
    }

    /// Read an object that may not be visible yet after our own write. `operation` returns the value,
    /// or `nil` / throws a not-found error to signal absence. Non-not-found errors propagate.
    /// Zero-grace backends are authoritative on the first miss (`.genuinelyAbsent`).
    static func read<Value: Sendable>(
        client: any RemoteStorageClientProtocol,
        floorSeconds: TimeInterval = 1,
        pollIntervalMs: Int = defaultPollIntervalMs,
        isNotFound: @Sendable (Error) -> Bool = { isStorageNotFoundError($0) },
        operation: () async throws -> Value?
    ) async throws -> GracefulReadResult<Value> {
        let first: Value?
        do {
            first = try await operation()
        } catch {
            guard isNotFound(error) else { throw error }
            first = nil
        }
        if let first { return .found(first) }

        guard client.readAfterWriteGraceSeconds > 0 else { return .absent(.genuinelyAbsent) }

        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: floorSeconds)
        while Date() < deadline {
            try Task.checkCancellation()
            try await Task.sleep(for: .milliseconds(pollIntervalMs))
            do {
                if let value = try await operation() { return .found(value) }
            } catch {
                // Mirror the first attempt: only not-found keeps polling, everything else propagates.
                guard isNotFound(error) else { throw error }
            }
        }
        return .absent(.genuinelyAbsent)
    }
    /// Low-level retry primitive for sites that map the result themselves. Invokes `attempt` until it
    /// returns a non-nil value or the grace deadline elapses; zero-grace backends attempt exactly once.
    /// Cancellation propagates. Errors thrown by `attempt` propagate unchanged — the caller decides what
    /// counts as retryable absence and whether to rethrow a retained error past the deadline.
    static func retryWithinGrace<Value: Sendable>(
        client: any RemoteStorageClientProtocol,
        floorSeconds: TimeInterval = 1,
        backoff: Backoff = .fixed(ms: defaultPollIntervalMs),
        attempt: () async throws -> Value?
    ) async throws -> Value? {
        if let value = try await attempt() { return value }
        guard client.readAfterWriteGraceSeconds > 0 else { return nil }

        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: floorSeconds)
        var n = 0
        while Date() < deadline {
            try Task.checkCancellation()
            try await Task.sleep(for: .milliseconds(backoff.delayMs(attempt: n)))
            n += 1
            if let value = try await attempt() { return value }
        }
        return nil
    }

    /// Post-create readback retry: like `retryWithinGrace`, but always retries for at least
    /// `floorSeconds`, even on zero-grace backends. Use for authoritative create readback where
    /// the retry budget is a hard floor independent of backend visibility semantics.
    static func retryWithFloor<Value: Sendable>(
        client: any RemoteStorageClientProtocol,
        floorSeconds: TimeInterval = 3,
        backoff: Backoff = .exponential(baseMs: 200, maxShift: 3),
        attempt: () async throws -> Value?
    ) async throws -> Value? {
        if let value = try await attempt() { return value }

        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: floorSeconds)
        var n = 0
        while Date() < deadline {
            try Task.checkCancellation()
            try await Task.sleep(for: .milliseconds(backoff.delayMs(attempt: n)))
            n += 1
            if let value = try await attempt() { return value }
        }
        return nil
    }
}

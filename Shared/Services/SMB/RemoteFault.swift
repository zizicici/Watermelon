import Foundation

/// One classification at the storage boundary so V2 Repo consumers stop re-deriving
/// not-found vs transient vs cancelled from raw NSError. Buckets mirror the only
/// distinctions V2 callers act on; permission/quota/auth/config all terminate the same.
nonisolated enum RemoteFault: Sendable, Equatable {
    /// Object reported absent. Callers decide visibility-lag vs genuine via `GracefulRead`.
    case notFound
    /// Connection-unavailable / transient server failure — safe to retry or pause.
    case retryable
    /// Operation was cancelled; never collapse this into absence or failure.
    case cancelled
    /// Permanent failure (permission, quota, auth, config, unsupported, unknown). Terminate.
    case terminal

    var isRetryable: Bool { self == .retryable }

    /// Single boundary classifier built on the existing primitives — it does not re-walk
    /// NSError itself, so adding a backend error code only updates the primitive it routes to.
    static func classify(_ error: Error) -> RemoteFault {
        if RemoteWriteClassifier.isCancellation(error) { return .cancelled }
        if isStorageNotFoundError(error) { return .notFound }
        if RemoteWriteClassifier.isTransientVerifyFailure(error) { return .retryable }
        return .terminal
    }
}

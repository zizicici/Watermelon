import Foundation

enum BackupFlushFailureClassification: Sendable, Equatable {
    case concurrentFlushRejected
    /// Raw NSURLErrorCancelled outside FlushError is NOT cancelled — today's per-site
    /// predicate only matches `error is CancellationError` or `FlushError.cancellationCause`.
    case cancelled
    case connectionUnavailable
    case snapshotWriteFailedPartial
    case other

    static func classify(_ error: Error, on profile: ServerProfileRecord) -> BackupFlushFailureClassification {
        if let flushError = error as? V2MonthSession.FlushError,
           case .concurrentFlushRejected = flushError {
            return .concurrentFlushRejected
        }
        if error is CancellationError
            || (error as? V2MonthSession.FlushError)?.cancellationCause != nil {
            return .cancelled
        }
        if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
            return .connectionUnavailable
        }
        if let flushError = error as? V2MonthSession.FlushError,
           case .snapshotWriteFailed = flushError {
            return .snapshotWriteFailedPartial
        }
        return .other
    }

    enum ForegroundIntervalAction: Sendable, Equatable {
        case continueAssetLoopAndResetCounter
        case pauseAndBreakAssetLoop
        case abortMonthBreakAssetLoop
        case logWarningAndContinue
    }

    var foregroundIntervalAction: ForegroundIntervalAction {
        switch self {
        case .concurrentFlushRejected: return .continueAssetLoopAndResetCounter
        case .cancelled:               return .pauseAndBreakAssetLoop
        case .connectionUnavailable:   return .abortMonthBreakAssetLoop
        // Partial and other lump at foreground interval — only foregroundEndOfMonth distinguishes.
        case .snapshotWriteFailedPartial,
             .other:                   return .logWarningAndContinue
        }
    }

    enum ForegroundEndOfMonthAction: Sendable, Equatable {
        case ignoreConcurrentReject
        case pauseAndBreakMonthLoop
        case abortMonthBreakMonthLoop
        case logErrorTryDeferDurableSnapshotSuppressRethrow
        case logErrorTryDeferDurableSnapshotOrRethrow
    }

    var foregroundEndOfMonthAction: ForegroundEndOfMonthAction {
        switch self {
        case .concurrentFlushRejected:    return .ignoreConcurrentReject
        case .cancelled:                  return .pauseAndBreakMonthLoop
        case .connectionUnavailable:      return .abortMonthBreakMonthLoop
        case .snapshotWriteFailedPartial: return .logErrorTryDeferDurableSnapshotSuppressRethrow
        case .other:                      return .logErrorTryDeferDurableSnapshotOrRethrow
        }
    }

    enum BackgroundIntervalAction: Sendable, Equatable {
        case continueAssetLoopAndResetCounter
        case ignoreSilently
        case abortProfileLogError
        case logErrorAndContinue
    }

    var backgroundIntervalAction: BackgroundIntervalAction {
        switch self {
        case .concurrentFlushRejected: return .continueAssetLoopAndResetCounter
        case .cancelled:               return .ignoreSilently
        case .connectionUnavailable:   return .abortProfileLogError
        // Partial and other lump at background interval — only foregroundEndOfMonth distinguishes.
        case .snapshotWriteFailedPartial,
             .other:                   return .logErrorAndContinue
        }
    }

    enum BackgroundEndOfMonthAction: Sendable, Equatable {
        case continueMonthLoop
        case ignoreSilently
        case abortProfileLogError
        case recordReasonLogError
    }

    var backgroundEndOfMonthAction: BackgroundEndOfMonthAction {
        switch self {
        case .concurrentFlushRejected: return .continueMonthLoop
        case .cancelled:               return .ignoreSilently
        case .connectionUnavailable:   return .abortProfileLogError
        // Partial and other lump at background end-of-month — only foregroundEndOfMonth distinguishes.
        case .snapshotWriteFailedPartial,
             .other:                   return .recordReasonLogError
        }
    }
}

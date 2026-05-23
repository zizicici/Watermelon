import Foundation

enum BackupFlushFailureClassification: Sendable, Equatable {
    case concurrentFlushRejected
    /// Raw NSURLErrorCancelled outside FlushError is NOT cancelled — today's per-site
    /// predicate only matches `error is CancellationError` or `FlushError.cancellationCause`.
    case cancelled
    case connectionUnavailable
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
        case .other:                   return .logWarningAndContinue
        }
    }

    enum ForegroundEndOfMonthAction: Sendable, Equatable {
        case ignoreConcurrentReject
        case pauseAndBreakMonthLoop
        case abortMonthBreakMonthLoop
        case logErrorAndRethrow
    }

    var foregroundEndOfMonthAction: ForegroundEndOfMonthAction {
        switch self {
        case .concurrentFlushRejected: return .ignoreConcurrentReject
        case .cancelled:               return .pauseAndBreakMonthLoop
        case .connectionUnavailable:   return .abortMonthBreakMonthLoop
        case .other:                   return .logErrorAndRethrow
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
        case .other:                   return .logErrorAndContinue
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
        case .other:                   return .recordReasonLogError
        }
    }
}

extension BackupFlushFailureClassification {
    enum ForegroundIntervalPartialAction: Sendable, Equatable {
        case pauseAndBreakAssetLoop
        case abortMonthBreakAssetLoop
        case logWarningAndContinue
    }
    struct ForegroundIntervalPartialDispatch {
        let action: ForegroundIntervalPartialAction
        let displayError: V2MonthSession.FlushError
    }

    enum ForegroundEndOfMonthPartialAction: Sendable, Equatable {
        case pauseAndBreakMonthLoop
        case abortMonthBreakMonthLoop
        case logErrorAndEmitDeferred
        case logErrorOnly
    }
    struct ForegroundEndOfMonthPartialDispatch {
        let action: ForegroundEndOfMonthPartialAction
        let displayError: V2MonthSession.FlushError
    }

    enum BackgroundIntervalPartialAction: Sendable, Equatable {
        case ignoreSilently
        case abortProfileLogError
        case logErrorAndContinue
    }
    struct BackgroundIntervalPartialDispatch {
        let action: BackgroundIntervalPartialAction
        let displayError: V2MonthSession.FlushError
    }

    enum BackgroundEndOfMonthPartialAction: Sendable, Equatable {
        case ignoreSilently
        case abortProfileLogError
        case recordReasonLogError
    }
    struct BackgroundEndOfMonthPartialDispatch {
        let action: BackgroundEndOfMonthPartialAction
        let displayError: V2MonthSession.FlushError
    }

    static func foregroundIntervalPartialDispatch(
        outcome: V2MonthFlushOutcome,
        profile: ServerProfileRecord
    ) -> ForegroundIntervalPartialDispatch? {
        guard case .commitDurableSnapshotDeferred(_, let flushError) = outcome else { return nil }
        let action: ForegroundIntervalPartialAction
        if outcome.cancellationCause != nil {
            action = .pauseAndBreakAssetLoop
        } else if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(flushError) {
            action = .abortMonthBreakAssetLoop
        } else {
            action = .logWarningAndContinue
        }
        return .init(action: action, displayError: flushError)
    }

    static func foregroundEndOfMonthPartialDispatch(
        outcome: V2MonthFlushOutcome,
        profile: ServerProfileRecord,
        shouldFinishMonth: Bool
    ) -> ForegroundEndOfMonthPartialDispatch? {
        guard case .commitDurableSnapshotDeferred(_, let flushError) = outcome else { return nil }
        let action: ForegroundEndOfMonthPartialAction
        if outcome.cancellationCause != nil {
            action = .pauseAndBreakMonthLoop
        } else if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(flushError) {
            action = .abortMonthBreakMonthLoop
        } else if shouldFinishMonth {
            action = .logErrorAndEmitDeferred
        } else {
            action = .logErrorOnly
        }
        return .init(action: action, displayError: flushError)
    }

    static func backgroundIntervalPartialDispatch(
        outcome: V2MonthFlushOutcome,
        profile: ServerProfileRecord
    ) -> BackgroundIntervalPartialDispatch? {
        guard case .commitDurableSnapshotDeferred(_, let flushError) = outcome else { return nil }
        let action: BackgroundIntervalPartialAction
        if outcome.cancellationCause != nil {
            action = .ignoreSilently
        } else if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(flushError) {
            action = .abortProfileLogError
        } else {
            action = .logErrorAndContinue
        }
        return .init(action: action, displayError: flushError)
    }

    static func backgroundEndOfMonthPartialDispatch(
        outcome: V2MonthFlushOutcome,
        profile: ServerProfileRecord
    ) -> BackgroundEndOfMonthPartialDispatch? {
        guard case .commitDurableSnapshotDeferred(_, let flushError) = outcome else { return nil }
        let action: BackgroundEndOfMonthPartialAction
        if outcome.cancellationCause != nil {
            action = .ignoreSilently
        } else if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(flushError) {
            action = .abortProfileLogError
        } else {
            action = .recordReasonLogError
        }
        return .init(action: action, displayError: flushError)
    }
}

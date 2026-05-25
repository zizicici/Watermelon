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
    enum PartialOutcomeCategory: Sendable, Equatable {
        case cancelled
        case connectionUnavailable
        case other
    }

    struct PartialOutcomeClassification {
        let category: PartialOutcomeCategory
        let flushError: V2MonthSession.FlushError
    }

    static func classifyPartialOutcome(
        _ outcome: V2MonthFlushOutcome,
        on profile: ServerProfileRecord
    ) -> PartialOutcomeClassification? {
        guard case .commitDurableSnapshotDeferred(_, let flushError) = outcome else { return nil }
        let category: PartialOutcomeCategory
        if outcome.cancellationCause != nil {
            category = .cancelled
        } else if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(flushError) {
            category = .connectionUnavailable
        } else {
            category = .other
        }
        return PartialOutcomeClassification(category: category, flushError: flushError)
    }

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
        guard let classified = classifyPartialOutcome(outcome, on: profile) else { return nil }
        let action: ForegroundIntervalPartialAction
        switch classified.category {
        case .cancelled:             action = .pauseAndBreakAssetLoop
        case .connectionUnavailable: action = .abortMonthBreakAssetLoop
        case .other:                 action = .logWarningAndContinue
        }
        return .init(action: action, displayError: classified.flushError)
    }

    static func foregroundEndOfMonthPartialDispatch(
        outcome: V2MonthFlushOutcome,
        profile: ServerProfileRecord,
        shouldFinishMonth: Bool
    ) -> ForegroundEndOfMonthPartialDispatch? {
        guard let classified = classifyPartialOutcome(outcome, on: profile) else { return nil }
        let action: ForegroundEndOfMonthPartialAction
        switch classified.category {
        case .cancelled:             action = .pauseAndBreakMonthLoop
        case .connectionUnavailable: action = .abortMonthBreakMonthLoop
        case .other:                 action = shouldFinishMonth ? .logErrorAndEmitDeferred : .logErrorOnly
        }
        return .init(action: action, displayError: classified.flushError)
    }

    static func backgroundIntervalPartialDispatch(
        outcome: V2MonthFlushOutcome,
        profile: ServerProfileRecord
    ) -> BackgroundIntervalPartialDispatch? {
        guard let classified = classifyPartialOutcome(outcome, on: profile) else { return nil }
        let action: BackgroundIntervalPartialAction
        switch classified.category {
        case .cancelled:             action = .ignoreSilently
        case .connectionUnavailable: action = .abortProfileLogError
        case .other:                 action = .logErrorAndContinue
        }
        return .init(action: action, displayError: classified.flushError)
    }

    static func backgroundEndOfMonthPartialDispatch(
        outcome: V2MonthFlushOutcome,
        profile: ServerProfileRecord
    ) -> BackgroundEndOfMonthPartialDispatch? {
        guard let classified = classifyPartialOutcome(outcome, on: profile) else { return nil }
        let action: BackgroundEndOfMonthPartialAction
        switch classified.category {
        case .cancelled:             action = .ignoreSilently
        case .connectionUnavailable: action = .abortProfileLogError
        case .other:                 action = .recordReasonLogError
        }
        return .init(action: action, displayError: classified.flushError)
    }

    enum AssetErrorCategory: Sendable, Equatable {
        case cancelled
        case connectionUnavailable
        case other
    }

    struct AssetErrorClassification {
        let category: AssetErrorCategory
        let error: Error
    }

    static func classifyAssetProcessError(
        _ error: Error,
        on profile: ServerProfileRecord
    ) -> AssetErrorClassification {
        let category: AssetErrorCategory
        if error is CancellationError {
            category = .cancelled
        } else if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
            category = .connectionUnavailable
        } else {
            category = .other
        }
        return AssetErrorClassification(category: category, error: error)
    }

    enum ForegroundAssetErrorAction: Sendable, Equatable {
        case pauseAndBreakAssetLoop
        case abortMonthDataConnectionLossBreakAssetLoop
        case logGenericFailureAndContinue
    }
    struct ForegroundAssetErrorDispatch {
        let action: ForegroundAssetErrorAction
        let error: Error
    }

    static func foregroundAssetErrorDispatch(
        error: Error,
        profile: ServerProfileRecord
    ) -> ForegroundAssetErrorDispatch {
        let classified = classifyAssetProcessError(error, on: profile)
        let action: ForegroundAssetErrorAction
        switch classified.category {
        case .cancelled:             action = .pauseAndBreakAssetLoop
        case .connectionUnavailable: action = .abortMonthDataConnectionLossBreakAssetLoop
        case .other:                 action = .logGenericFailureAndContinue
        }
        return ForegroundAssetErrorDispatch(action: action, error: classified.error)
    }

    enum BackgroundAssetErrorAction: Sendable, Equatable {
        case breakAssetLoop
        case abortMonthConnectionUnavailableBreakAssetLoop
        case logGenericFailureAndContinue
    }
    struct BackgroundAssetErrorDispatch {
        let action: BackgroundAssetErrorAction
        let error: Error
    }

    static func backgroundAssetErrorDispatch(
        error: Error,
        profile: ServerProfileRecord
    ) -> BackgroundAssetErrorDispatch {
        let classified = classifyAssetProcessError(error, on: profile)
        let action: BackgroundAssetErrorAction
        switch classified.category {
        case .cancelled:             action = .breakAssetLoop
        case .connectionUnavailable: action = .abortMonthConnectionUnavailableBreakAssetLoop
        case .other:                 action = .logGenericFailureAndContinue
        }
        return BackgroundAssetErrorDispatch(action: action, error: classified.error)
    }
}

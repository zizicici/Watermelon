import Foundation

enum V2MonthFlushOutcome {
    case completed(BackupMonthFlushDelta)
    /// Commit landed on remote; a subsequent post-commit operation failed. `delta` carries the
    /// durable fingerprints as a value; `flushError` is the `postCommitFailed(underlying:)` wrapper
    /// so dispatch can preserve `userFacingStorageErrorMessage` / `appendErrorLog(..., unless: error)`
    /// / fatal-error propagation semantics exactly.
    case commitDurablePartial(delta: BackupMonthFlushDelta, flushError: V2MonthSession.FlushError)

    var delta: BackupMonthFlushDelta {
        switch self {
        case .completed(let d), .commitDurablePartial(let d, _):
            return d
        }
    }

    /// Original FlushError wrapper for display/logging/propagation. Production consumes it via the
    /// per-site partial-dispatch struct, not directly.
    var displayError: V2MonthSession.FlushError? {
        guard case .commitDurablePartial(_, let flushError) = self else { return nil }
        return flushError
    }

    /// Delegates to `V2MonthSession.FlushError.cancellationCause` so the NSURLErrorCancelled /
    /// CancellationError walk stays in one place.
    var cancellationCause: CancellationError? {
        displayError?.cancellationCause
    }
}

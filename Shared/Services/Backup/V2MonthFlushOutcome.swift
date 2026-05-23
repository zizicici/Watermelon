import Foundation

enum V2MonthFlushOutcome {
    case completed(BackupMonthFlushDelta)
    /// Commit landed on remote; the in-month snapshot write failed. `flushError` is the original
    /// `V2MonthSession.FlushError.snapshotWriteFailed(...)` wrapper so dispatch can preserve
    /// `userFacingStorageErrorMessage` / `appendErrorLog(..., unless: error)` / fatal-error
    /// propagation semantics exactly.
    case commitDurableSnapshotDeferred(delta: BackupMonthFlushDelta, flushError: V2MonthSession.FlushError)

    var delta: BackupMonthFlushDelta {
        switch self {
        case .completed(let d), .commitDurableSnapshotDeferred(let d, _):
            return d
        }
    }

    /// Original FlushError wrapper for display/logging/propagation. Production consumes it via the
    /// per-site partial-dispatch struct, not directly.
    var displayError: V2MonthSession.FlushError? {
        guard case .commitDurableSnapshotDeferred(_, let flushError) = self else { return nil }
        return flushError
    }

    /// Delegates to `V2MonthSession.FlushError.cancellationCause` so the NSURLErrorCancelled /
    /// CancellationError walk stays in one place.
    var cancellationCause: CancellationError? {
        displayError?.cancellationCause
    }
}

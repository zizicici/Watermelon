import Foundation

/// Arch-VII A-II B5b: control signal returned by month-loop phases extracted out of
/// `runParallelMonthWorker`. Swift `break` cannot cross a function boundary, so an extracted region
/// that previously `break`ed the month loop returns `.breakMonthLoop` and the caller (still inside
/// the loop) reproduces the break verbatim. `throw` propagates unchanged via the helper's `throws`.
enum MonthLoopFlow {
    case proceed
    case breakMonthLoop
}

/// Result of processing one asset in the inner loop. Swift `break`/`continue` cannot cross a
/// helper's function boundary, so the caller acts on this instead of a `shouldBreak` boolean:
/// `.breakAssetLoop` -> `break`, `.skippedEmpty`/`.processed` -> next iteration.
enum AssetLoopFlow {
    case processed
    case skippedEmpty
    case breakAssetLoop
}

/// Result of one batch. `.breakBatchLoop` mirrors the inline `if workUnit.paused || hasFatal { break }`.
enum BatchLoopFlow {
    case completed
    case breakBatchLoop
}

/// Arch-VII A-II B5: explicit representation of the four cross-phase lifecycle flags that
/// `runParallelMonthWorker` previously tracked as loose mutable `var`s. Each old flag maps to one
/// field and each old assignment maps to one named transition, so the control flow stays byte-identical.
///
/// Scope split mirrors the original declarations exactly:
/// - `paused` and `clientReusable` are worker-scoped (survive every month iteration).
/// - `fatalError` and `shouldFlushAfterDataConnectionLoss` are month-scoped (reset per month via
///   `beginMonth()`).
///
/// Precedence preserved from the original code: `fatalError` dominates `paused` at finish — the
/// worker throws the fatal before honoring a pause break (see `runParallelMonthWorker` finish block).
struct MonthWorkUnit {
    // Worker-scoped.
    private(set) var paused: Bool = false
    private(set) var clientReusable: Bool = true

    // Month-scoped.
    private(set) var fatalError: Error?
    private(set) var shouldFlushAfterDataConnectionLoss: Bool = false

    /// Reset the month-scoped fields at the start of each month; worker-scoped fields persist.
    mutating func beginMonth() {
        fatalError = nil
        shouldFlushAfterDataConnectionLoss = false
    }

    // MARK: - Transitions (each maps 1:1 to an original flag assignment)

    /// `workerState.paused = true` (cancellation, pause-classified failure, finalization cancel).
    mutating func markPaused() {
        paused = true
    }

    /// `clientReusable = false; monthFatalError = error` (abort-classified flush/EOM failure).
    mutating func markFatal(_ error: Error) {
        clientReusable = false
        fatalError = error
    }

    /// `clientReusable = false; monthFatalError = error; shouldFlushAfterDataConnectionLoss = true`
    /// — the data-connection-loss-during-asset abort site, which additionally arms the EOM flush.
    mutating func markDataConnectionLost(_ error: Error) {
        clientReusable = false
        fatalError = error
        shouldFlushAfterDataConnectionLoss = true
    }

    /// `monthFatalError = error` WITHOUT touching `clientReusable` — the onMonthUploaded `.failed`
    /// site, which is the only fatal assignment that left `clientReusable` untouched.
    mutating func markFatalKeepingClient(_ error: Error) {
        fatalError = error
    }

    /// `clientReusable = false` at the outer catch when the thrown error is connection-unavailable.
    mutating func markClientNotReusable() {
        clientReusable = false
    }

    // MARK: - Reads

    var hasFatal: Bool { fatalError != nil }

    /// Project back into the public `WorkerRunState` return shape (unchanged).
    var workerRunState: WorkerRunState {
        WorkerRunState(paused: paused)
    }
}

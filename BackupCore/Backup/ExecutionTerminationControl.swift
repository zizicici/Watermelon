import Foundation

enum ExecutionTerminationIntent: Sendable {
    case none
    case pause
    case stop
}

final class ExecutionTerminationControl: @unchecked Sendable {
    private let lock = NSLock()
    private var intent: ExecutionTerminationIntent = .none

    var terminationIntent: ExecutionTerminationIntent {
        lock.withLock { intent }
    }

    var shouldDrain: Bool {
        switch terminationIntent {
        case .none:
            return false
        case .pause, .stop:
            return true
        }
    }

    func checkDrainRequested() throws {
        if shouldDrain {
            throw CancellationError()
        }
    }

    func request(_ requestedIntent: ExecutionTerminationIntent) {
        if case .none = requestedIntent { return }

        lock.withLock {
            switch (intent, requestedIntent) {
            case (.stop, _), (_, .none), (.pause, .pause):
                break
            case (.none, .pause), (.none, .stop), (.pause, .stop):
                intent = requestedIntent
            }
        }
    }
}

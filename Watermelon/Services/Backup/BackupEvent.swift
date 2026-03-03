import Foundation

enum BackupEvent: Sendable {
    case progress(BackupProgress)
    case log(String)
    case transferState(BackupTransferState)
    case monthChanged(MonthChangeEvent)
    case remoteIndexSynced(RemoteIndexSyncEvent)
    case started(totalAssets: Int)
    case finished(BackupExecutionResult)
    case failed(Error)

    var isTerminal: Bool {
        switch self {
        case .finished, .failed:
            return true
        default:
            return false
        }
    }
}

struct MonthChangeEvent: Sendable {
    let year: Int
    let month: Int
    let action: MonthAction

    enum MonthAction: Sendable {
        case started
        case flushed
        case flushFailed(String)
    }
}

struct RemoteIndexSyncEvent: Sendable {
    let resourceCount: Int
    let assetCount: Int
    let changedMonths: Int
    let removedMonths: Int
}

final class BackupEventStream: @unchecked Sendable {
    let stream: AsyncStream<BackupEvent>

    private let lock = NSLock()
    private var continuation: AsyncStream<BackupEvent>.Continuation?
    private var finished = false

    init() {
        var captured: AsyncStream<BackupEvent>.Continuation?
        stream = AsyncStream { continuation in
            captured = continuation
        }
        precondition(captured != nil, "BackupEventStream continuation was not initialized.")
        continuation = captured
    }

    func emit(_ event: BackupEvent) {
        let target: AsyncStream<BackupEvent>.Continuation?
        lock.lock()
        if finished {
            target = nil
        } else {
            target = continuation
        }
        lock.unlock()
        target?.yield(event)
    }

    func finish() {
        let target: AsyncStream<BackupEvent>.Continuation?
        lock.lock()
        if finished {
            target = nil
        } else {
            finished = true
            target = continuation
            continuation = nil
        }
        lock.unlock()
        target?.finish()
    }
}

import Foundation

enum BackupEvent: Sendable {
    case progress(BackupProgress)
    case log(String)
    case transferState(BackupTransferState)
    case assetCompleted(AssetCompletionEvent)
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

struct AssetCompletionEvent: Sendable {
    let assetLocalIdentifier: String
    let assetFingerprint: Data?
    let displayName: String
    let status: BackupItemStatus
    let reason: String?
    let resourceSummary: String?
    let position: Int
    let total: Int
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
    private let continuation: AsyncStream<BackupEvent>.Continuation
    private let lock = NSLock()
    private var finished = false

    init() {
        var captured: AsyncStream<BackupEvent>.Continuation?
        let stream = AsyncStream<BackupEvent> { continuation in
            captured = continuation
        }
        guard let continuation = captured else {
            preconditionFailure("BackupEventStream continuation was not initialized.")
        }
        self.stream = stream
        self.continuation = continuation
    }

    func emit(_ event: BackupEvent) {
        lock.lock()
        let isFinished = finished
        lock.unlock()
        guard !isFinished else { return }
        continuation.yield(event)
    }

    func finish() {
        lock.lock()
        guard !finished else {
            lock.unlock()
            return
        }
        finished = true
        lock.unlock()
        continuation.finish()
    }
}

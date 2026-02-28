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
    private let lock = NSLock()
    private var continuations: [UUID: AsyncStream<BackupEvent>.Continuation] = [:]
    private var finished = false

    func makeStream() -> AsyncStream<BackupEvent> {
        AsyncStream { continuation in
            let id = UUID()

            lock.lock()
            if finished {
                lock.unlock()
                continuation.finish()
                return
            }
            continuations[id] = continuation
            lock.unlock()

            continuation.onTermination = { [weak self] _ in
                self?.removeContinuation(id: id)
            }
        }
    }

    func emit(_ event: BackupEvent) {
        lock.lock()
        guard !finished else {
            lock.unlock()
            return
        }
        let targets = Array(continuations.values)
        lock.unlock()

        for continuation in targets {
            continuation.yield(event)
        }
    }

    func finish() {
        lock.lock()
        guard !finished else {
            lock.unlock()
            return
        }
        finished = true
        let targets = Array(continuations.values)
        continuations.removeAll()
        lock.unlock()

        for continuation in targets {
            continuation.finish()
        }
    }

    private func removeContinuation(id: UUID) {
        lock.lock()
        continuations[id] = nil
        lock.unlock()
    }
}

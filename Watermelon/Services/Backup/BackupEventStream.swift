import Foundation

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
        let target: AsyncStream<BackupEvent>.Continuation? = lock.withLock {
            finished ? nil : continuation
        }
        target?.yield(event)
    }

    func emitLog(
        _ message: String,
        level: ExecutionLogLevel = .info
    ) {
        emit(.log(message, level: level))
    }

    func finish() {
        let target: AsyncStream<BackupEvent>.Continuation? = lock.withLock {
            guard !finished else { return nil }
            finished = true
            let c = continuation
            continuation = nil
            return c
        }
        target?.finish()
    }
}

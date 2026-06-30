import Foundation

// A cancellation-aware counting semaphore for async code. `wait()` returns false instead of acquiring
// if the task is cancelled while parked, so callers can bail cleanly. Used by the remote browser to
// bound concurrent connection use to the pool size — so the pool never has to park a waiter (the
// browser does its waiting here, where cancellation IS observed). A resumed waiter is resumed exactly
// once: signal() and the cancel handler both remove it under the lock, so whoever removes it wins.
final class AsyncSemaphore: @unchecked Sendable {
    private struct Waiter {
        let id: UUID
        let continuation: CheckedContinuation<Bool, Never>
    }

    private let lock = NSLock()
    private var count: Int
    private var waiters: [Waiter] = []

    init(value: Int) {
        count = max(0, value)
    }

    // Returns true once a slot is acquired; false if the task was cancelled before/while waiting.
    func wait() async -> Bool {
        if Task.isCancelled { return false }
        let id = UUID()
        return await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Bool, Never>) in
                lock.lock()
                // Re-check under the lock: closes the race where onCancel ran before this body parked
                // the waiter (it would otherwise stay parked forever with no future signal).
                if Task.isCancelled {
                    lock.unlock()
                    continuation.resume(returning: false)
                } else if count > 0 {
                    count -= 1
                    lock.unlock()
                    continuation.resume(returning: true)
                } else {
                    waiters.append(Waiter(id: id, continuation: continuation))
                    lock.unlock()
                }
            }
        } onCancel: {
            lock.lock()
            guard let index = waiters.firstIndex(where: { $0.id == id }) else {
                lock.unlock()
                return
            }
            let waiter = waiters.remove(at: index)
            lock.unlock()
            waiter.continuation.resume(returning: false)
        }
    }

    func signal() {
        lock.lock()
        if waiters.isEmpty {
            count += 1
            lock.unlock()
        } else {
            let waiter = waiters.removeFirst()
            lock.unlock()
            waiter.continuation.resume(returning: true)
        }
    }
}

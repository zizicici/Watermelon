import Foundation

final class BackupCancellationController: @unchecked Sendable {
    private let lock = NSLock()
    private var isCancelledFlag = false
    private var handlers: [UUID: () -> Void] = [:]

    var isCancelled: Bool {
        lock.lock()
        let value = isCancelledFlag
        lock.unlock()
        return value
    }

    func throwIfCancelled() throws {
        if isCancelled {
            throw CancellationError()
        }
    }

    @discardableResult
    func addCancellationHandler(_ handler: @escaping () -> Void) -> UUID? {
        let id = UUID()

        lock.lock()
        if isCancelledFlag {
            lock.unlock()
            handler()
            return nil
        }
        handlers[id] = handler
        lock.unlock()

        return id
    }

    func removeCancellationHandler(_ id: UUID) {
        lock.lock()
        handlers[id] = nil
        lock.unlock()
    }

    func cancel() {
        let callbacks: [() -> Void]

        lock.lock()
        guard !isCancelledFlag else {
            lock.unlock()
            return
        }
        isCancelledFlag = true
        callbacks = Array(handlers.values)
        handlers.removeAll()
        lock.unlock()

        for callback in callbacks {
            callback()
        }
    }
}

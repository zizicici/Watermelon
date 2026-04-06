import Foundation

final class BackupCancellationController: @unchecked Sendable {
    private let lock = NSLock()
    private var isCancelledFlag = false
    private var handlers: [UUID: () -> Void] = [:]

    var isCancelled: Bool {
        lock.withLock { isCancelledFlag }
    }

    func throwIfCancelled() throws {
        if isCancelled {
            throw CancellationError()
        }
    }

    @discardableResult
    func addCancellationHandler(_ handler: @escaping () -> Void) -> UUID? {
        let id = UUID()

        let alreadyCancelled = lock.withLock { () -> Bool in
            if isCancelledFlag { return true }
            handlers[id] = handler
            return false
        }

        if alreadyCancelled {
            handler()
            return nil
        }

        return id
    }

    func removeCancellationHandler(_ id: UUID) {
        lock.withLock { handlers[id] = nil }
    }

    func cancel() {
        let callbacks: [() -> Void] = lock.withLock {
            guard !isCancelledFlag else { return [] }
            isCancelledFlag = true
            let values = Array(handlers.values)
            handlers.removeAll()
            return values
        }

        for callback in callbacks {
            callback()
        }
    }
}

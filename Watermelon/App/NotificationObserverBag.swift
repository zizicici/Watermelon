import Foundation

final class NotificationObserverBag: @unchecked Sendable {
    private let lock = NSLock()
    private var tokens: [NSObjectProtocol] = []

    func insert(_ token: NSObjectProtocol) {
        lock.withLock { tokens.append(token) }
    }

    func removeAll() {
        let removed = lock.withLock {
            let removed = tokens
            tokens.removeAll()
            return removed
        }
        removed.forEach { NotificationCenter.default.removeObserver($0) }
    }

    deinit {
        removeAll()
    }
}

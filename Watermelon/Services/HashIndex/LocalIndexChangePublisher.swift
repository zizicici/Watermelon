import Foundation

/// Backup execution intentionally does not publish here — it already drives Home
/// via its own per-month / per-asset callbacks, and a publish would double-fire.
final class LocalIndexChangePublisher: @unchecked Sendable {
    enum Change: Sendable {
        case touched(assetIDs: Set<String>)
        case bulkInvalidation
    }

    private let lock = NSLock()
    private var observers: [UUID: @Sendable (Change) -> Void] = [:]

    init() {}

    @discardableResult
    func addObserver(_ block: @escaping @Sendable (Change) -> Void) -> UUID {
        let id = UUID()
        lock.withLock { observers[id] = block }
        return id
    }

    func removeObserver(_ id: UUID) {
        lock.withLock { _ = observers.removeValue(forKey: id) }
    }

    func publish(_ change: Change) {
        let snapshot = lock.withLock { Array(observers.values) }
        for block in snapshot {
            block(change)
        }
    }
}

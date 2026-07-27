import Foundation

final class ChangeSignal<Value: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var observers: [UUID: @Sendable (Value) -> Void] = [:]

    @discardableResult
    func addObserver(_ observer: @escaping @Sendable (Value) -> Void) -> UUID {
        let id = UUID()
        lock.withLock { observers[id] = observer }
        return id
    }

    func removeObserver(_ id: UUID) {
        lock.withLock { _ = observers.removeValue(forKey: id) }
    }

    func publish(_ value: Value) {
        let current = lock.withLock { Array(observers.values) }
        current.forEach { $0(value) }
    }
}

extension ChangeSignal where Value == Void {
    @discardableResult
    func addObserver(_ observer: @escaping @Sendable () -> Void) -> UUID {
        addObserver { _ in observer() }
    }

    func publish() {
        publish(())
    }
}

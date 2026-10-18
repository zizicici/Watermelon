import Foundation

final class ChangeSignal<Value: Sendable>: @unchecked Sendable {
    private struct Observer {
        let receive: @Sendable (Value) -> Void
        let cancel: (@Sendable () -> Void)?
    }

    private let lock = NSLock()
    private var observers: [UUID: Observer] = [:]

    @discardableResult
    func addObserver(_ observer: @escaping @Sendable (Value) -> Void) -> UUID {
        addObserver(observer, onRemoval: nil)
    }

    private func addObserver(
        _ observer: @escaping @Sendable (Value) -> Void,
        onRemoval: (@Sendable () -> Void)?
    ) -> UUID {
        let id = UUID()
        lock.withLock { observers[id] = Observer(receive: observer, cancel: onRemoval) }
        return id
    }

    func removeObserver(_ id: UUID) {
        let removed = lock.withLock { observers.removeValue(forKey: id) }
        removed?.cancel?()
    }

    func publish(_ value: Value) {
        let current = lock.withLock { Array(observers.values) }
        current.forEach { $0.receive(value) }
    }

    deinit {
        for observer in observers.values { observer.cancel?() }
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

    @discardableResult
    func addCoalescedMainActorObserver(_ observer: @escaping @MainActor @Sendable () -> Void) -> UUID {
        let delivery = MainActorDelivery(observer)
        return addObserver({ _ in delivery.schedule() }, onRemoval: { delivery.cancel() })
    }
}

private final class MainActorDelivery: @unchecked Sendable {
    private let lock = NSLock()
    private let observer: @MainActor @Sendable () -> Void
    private var scheduled = false
    private var cancelled = false

    init(_ observer: @escaping @MainActor @Sendable () -> Void) { self.observer = observer }

    func schedule() {
        let enqueue = lock.withLock {
            guard !scheduled, !cancelled else { return false }
            scheduled = true
            return true
        }
        guard enqueue else { return }
        DispatchQueue.main.async { [self] in
            let deliver = lock.withLock {
                scheduled = false
                return !cancelled
            }
            if deliver { observer() }
        }
    }

    func cancel() { lock.withLock { cancelled = true } }
}

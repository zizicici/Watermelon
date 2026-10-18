import UIKit

@MainActor
final class AppForegroundActivity {
    private let onForeground: (() -> Void)?
    private let onChange: (Bool) -> Void
    private let observers = NotificationObserverBag()
    private(set) var isActive = false

    init(onForeground: (() -> Void)? = nil, onChange: @escaping (Bool) -> Void) {
        self.onForeground = onForeground
        self.onChange = onChange
        observe(UIApplication.willResignActiveNotification) { [weak self] in
            self?.setActive(false)
        }
        observe(UIApplication.didBecomeActiveNotification) { [weak self] in
            guard let self, !self.isActive else { return }
            self.onForeground?()
            self.setActive(true)
        }
    }

    func start() {
        setActive(UIApplication.shared.applicationState == .active)
    }

    private func setActive(_ active: Bool) {
        guard isActive != active else { return }
        isActive = active
        onChange(active)
    }

    private func observe(_ name: Notification.Name, handler: @escaping @MainActor () -> Void) {
        observers.insert(NotificationCenter.default.addObserver(forName: name, object: nil, queue: .main) { _ in
            MainActor.assumeIsolated { handler() }
        })
    }
}

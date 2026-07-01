import Foundation

@MainActor
final class HomeMonthGroupingTimeZoneChangeObserver {
    struct Hooks {
        let requestLocalIndexReload: @MainActor () -> Void
    }

    private let hooks: Hooks
    private let notificationCenter: NotificationCenter
    private var observers: [NSObjectProtocol] = []

    init(notificationCenter: NotificationCenter = .default, hooks: Hooks) {
        self.notificationCenter = notificationCenter
        self.hooks = hooks
        observe(.MonthGroupingTimeZonePreferenceDidChange) { [weak self] in
            self?.hooks.requestLocalIndexReload()
        }
        observe(Notification.Name.NSSystemTimeZoneDidChange) { [weak self] in
            _ = MonthGroupingTimeZonePreference.currentSystemTimeZone()
            guard MonthGroupingTimeZonePreference.current.mode == .system else { return }
            self?.hooks.requestLocalIndexReload()
        }
    }

    deinit {
        for observer in observers {
            notificationCenter.removeObserver(observer)
        }
    }

    private func observe(_ name: Notification.Name, handler: @escaping @MainActor () -> Void) {
        let observer = notificationCenter.addObserver(
            forName: name,
            object: nil,
            queue: .main
        ) { _ in
            MainActor.assumeIsolated {
                handler()
            }
        }
        observers.append(observer)
    }
}

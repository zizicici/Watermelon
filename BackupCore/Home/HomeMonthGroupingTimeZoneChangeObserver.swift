import Foundation

nonisolated private final class MonthGroupingObservationStorage: @unchecked Sendable {
    let notificationCenter: NotificationCenter
    var observers: [NSObjectProtocol] = []

    init(notificationCenter: NotificationCenter) {
        self.notificationCenter = notificationCenter
    }

    deinit {
        for observer in observers {
            notificationCenter.removeObserver(observer)
        }
    }
}

@MainActor
final class HomeMonthGroupingTimeZoneChangeObserver {
    struct Hooks {
        let requestLocalIndexReload: @MainActor () -> Void
    }

    private let hooks: Hooks
    private let observationStorage: MonthGroupingObservationStorage

    init(notificationCenter: NotificationCenter = .default, hooks: Hooks) {
        observationStorage = MonthGroupingObservationStorage(
            notificationCenter: notificationCenter
        )
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

    private func observe(_ name: Notification.Name, handler: @escaping @MainActor () -> Void) {
        let observer = observationStorage.notificationCenter.addObserver(
            forName: name,
            object: nil,
            queue: .main
        ) { _ in
            MainActor.assumeIsolated {
                handler()
            }
        }
        observationStorage.observers.append(observer)
    }
}

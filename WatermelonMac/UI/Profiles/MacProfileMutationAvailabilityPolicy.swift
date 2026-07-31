import Foundation

enum MacProfileMutationError: LocalizedError {
    case taskInProgress

    var errorDescription: String? {
        String(
            localized: "home.alert.maintenanceInProgress"
        )
    }
}

enum MacProfileMutationAvailabilityPolicy {
    static func canMutate(
        executionActive: Bool,
        maintenanceActive: Bool,
        connectionActive: Bool
    ) -> Bool {
        !executionActive
            && !maintenanceActive
            && !connectionActive
    }
}

@MainActor
final class MacProfileMutationActivityObserver {
    private let notificationCenter: NotificationCenter
    nonisolated(unsafe) private var observers:
        [NSObjectProtocol] = []

    init(
        notificationCenter: NotificationCenter = .default,
        onChange: @escaping @MainActor () -> Void
    ) {
        self.notificationCenter = notificationCenter
        for name in [
            Notification.Name.ExecutionLifecycleDidChange,
            .RemoteMaintenanceDidChange,
            .ConnectionLifecycleDidChange
        ] {
            observers.append(
                notificationCenter.addObserver(
                    forName: name,
                    object: nil,
                    queue: .main
                ) { _ in
                    MainActor.assumeIsolated {
                        onChange()
                    }
                }
            )
        }
    }

    deinit {
        observers.forEach {
            notificationCenter.removeObserver($0)
        }
    }
}

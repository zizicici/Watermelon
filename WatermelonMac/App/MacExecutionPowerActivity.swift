import Foundation

@MainActor
final class MacExecutionPowerActivity {
    typealias BeginActivity = (
        ProcessInfo.ActivityOptions,
        String
    ) -> NSObjectProtocol
    typealias EndActivity = (NSObjectProtocol) -> Void

    private let appRuntimeFlags: AppRuntimeFlags
    private let notificationCenter: NotificationCenter
    private let beginActivity: BeginActivity
    private let endActivity: EndActivity
    nonisolated(unsafe) private var lifecycleObserver:
        NSObjectProtocol?
    private var activityToken: NSObjectProtocol?
    private var isInvalidated = false

    convenience init(
        appRuntimeFlags: AppRuntimeFlags,
        processInfo: ProcessInfo = .processInfo,
        notificationCenter: NotificationCenter = .default
    ) {
        self.init(
            appRuntimeFlags: appRuntimeFlags,
            notificationCenter: notificationCenter,
            beginActivity: { options, reason in
                processInfo.beginActivity(
                    options: options,
                    reason: reason
                )
            },
            endActivity: {
                processInfo.endActivity($0)
            }
        )
    }

    init(
        appRuntimeFlags: AppRuntimeFlags,
        notificationCenter: NotificationCenter = .default,
        beginActivity: @escaping BeginActivity,
        endActivity: @escaping EndActivity
    ) {
        self.appRuntimeFlags = appRuntimeFlags
        self.notificationCenter = notificationCenter
        self.beginActivity = beginActivity
        self.endActivity = endActivity
        lifecycleObserver = notificationCenter.addObserver(
            forName: .ExecutionLifecycleDidChange,
            object: appRuntimeFlags,
            queue: .main
        ) { [weak self] _ in
            MainActor.assumeIsolated {
                self?.synchronize()
            }
        }
        synchronize()
    }

    func invalidate() {
        guard !isInvalidated else { return }
        isInvalidated = true
        if let lifecycleObserver {
            notificationCenter.removeObserver(lifecycleObserver)
            self.lifecycleObserver = nil
        }
        endCurrentActivity()
    }

    private func synchronize() {
        guard !isInvalidated else { return }
        if appRuntimeFlags.isExecuting {
            guard activityToken == nil else { return }
            activityToken = beginActivity(
                [.userInitiated, .idleSystemSleepDisabled],
                "Watermelon Backup task"
            )
        } else {
            endCurrentActivity()
        }
    }

    private func endCurrentActivity() {
        guard let activityToken else { return }
        self.activityToken = nil
        endActivity(activityToken)
    }
}

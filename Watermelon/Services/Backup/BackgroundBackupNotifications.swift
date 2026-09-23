import Foundation
import UserNotifications

enum BackgroundBackupNotifications {
    enum Result {
        case success(transferred: Int)
        case failure(Error)
    }

    // Fixed id: `add` replaces the delivered banner, so rounds never stack. A silent round leaves
    // the previous one standing — there is nothing newer to say.
    static let identifier = "background-backup.summary"

    struct Summary {
        private struct Failure {
            let name: String
            let message: String
        }

        private var successes: [String] = []
        private var failures: [Failure] = []

        mutating func record(_ result: Result, for profile: ServerProfileRecord) {
            switch result {
            case .success(let transferred):
                // A run that moved nothing is not worth waking the user for.
                guard profile.backgroundBackupNotifyOnSuccess, transferred > 0 else { return }
                successes.append(profile.name)
            case .failure(let error):
                guard profile.backgroundBackupNotifyOnFailure,
                      !(error is CancellationError),
                      !(error is BackupRunSkipped),
                      RemoteFaultLite.classify(error) != .cancelled else { return }
                failures.append(Failure(name: profile.name, message: profile.userFacingStorageErrorMessage(error)))
            }
        }

        func content(taskIsCancelled: Bool) -> UNMutableNotificationContent? {
            guard !taskIsCancelled, !successes.isEmpty || !failures.isEmpty else { return nil }
            let content = UNMutableNotificationContent()
            if successes.count == 1, failures.isEmpty {
                content.title = String(localized: "backgroundBackup.notification.success.title")
                content.body = String(format: String(localized: "backgroundBackup.notification.success.body"), successes[0])
            } else if failures.count == 1, successes.isEmpty {
                content.title = String(localized: "backgroundBackup.notification.failure.title")
                content.body = String(
                    format: String(localized: "backgroundBackup.notification.failure.body"),
                    failures[0].name,
                    failures[0].message
                )
            } else {
                content.title = String(localized: "backgroundBackup.notification.summary.title")
                var lines: [String] = []
                if !successes.isEmpty {
                    lines.append(String(
                        format: String(localized: "backgroundBackup.notification.summary.success.body"),
                        ListFormatter.localizedString(byJoining: successes)
                    ))
                }
                lines.append(contentsOf: failures.map { failure in
                    String(
                        format: String(localized: "backgroundBackup.notification.summary.failure.body"),
                        failure.name,
                        failure.message
                    )
                })
                content.body = lines.joined(separator: "\n")
            }
            content.sound = .default
            content.threadIdentifier = "background-backup"
            return content
        }
    }

    static func send(_ summary: Summary) async throws {
        guard let content = summary.content(taskIsCancelled: Task.isCancelled) else { return }
        let center = UNUserNotificationCenter.current()
        let settings = await center.notificationSettings()
        guard canDeliver(authorization: settings.authorizationStatus), !Task.isCancelled else { return }
        try await center.add(UNNotificationRequest(
            identifier: identifier,
            content: content,
            trigger: nil
        ))
    }

    static func canDeliver(authorization: UNAuthorizationStatus) -> Bool {
        switch authorization {
        case .authorized, .provisional, .ephemeral: true
        case .notDetermined, .denied: false
        @unknown default: false
        }
    }
}

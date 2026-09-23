import MoreKit
import Photos
import UIKit
import UserNotifications

@MainActor
final class PermissionSettings {
    private var notificationAuthorization: UNAuthorizationStatus?
    private var photosAuthorization: PHAuthorizationStatus?
    private let observers = NotificationObserverBag()

    init() {
        observers.insert(NotificationCenter.default.addObserver(
            forName: UIApplication.didBecomeActiveNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor [weak self] in await self?.refresh() }
        })
        Task { [weak self] in await self?.refresh() }
    }

    var photosValue: String {
        Self.photosValue(for: PHPhotoLibrary.authorizationStatus(for: .readWrite))
    }

    var notificationsValue: String? {
        guard let notificationAuthorization else { return nil }
        return Self.notificationsValue(for: notificationAuthorization)
    }

    static func photosValue(for status: PHAuthorizationStatus) -> String {
        switch status {
        case .notDetermined: String(localized: "settings.permission.notRequested")
        case .authorized: String(localized: "settings.permission.allowed")
        case .limited: String(localized: "settings.permission.limitedPhotos")
        case .denied: String(localized: "settings.permission.denied")
        case .restricted: String(localized: "settings.permission.restricted")
        @unknown default: String(localized: "settings.permission.restricted")
        }
    }

    static func notificationsValue(for status: UNAuthorizationStatus) -> String {
        switch status {
        case .notDetermined: String(localized: "settings.permission.notRequested")
        case .authorized, .ephemeral: String(localized: "settings.permission.allowed")
        case .provisional: String(localized: "settings.permission.quietNotifications")
        case .denied: String(localized: "settings.permission.denied")
        @unknown default: String(localized: "settings.permission.restricted")
        }
    }

    func openPhotos() async {
        if PHPhotoLibrary.authorizationStatus(for: .readWrite) == .notDetermined {
            _ = await PHPhotoLibrary.requestAuthorization(for: .readWrite)
            await refresh()
        } else if let url = URL(string: UIApplication.openSettingsURLString) {
            await UIApplication.shared.open(url)
        }
    }

    func openNotifications(from presenter: UIViewController) async {
        let center = UNUserNotificationCenter.current()
        let settings = await center.notificationSettings()
        if settings.authorizationStatus == .notDetermined {
            do {
                _ = try await center.requestAuthorization(options: [.alert, .sound])
            } catch {
                let alert = UIAlertController(
                    title: String(localized: "common.error"),
                    message: error.localizedDescription,
                    preferredStyle: .alert
                )
                alert.addAction(UIAlertAction(title: String(localized: "common.ok"), style: .default))
                presenter.present(alert, animated: ConsideringUser.animated)
            }
            await refresh()
        } else if let url = URL(string: UIApplication.openNotificationSettingsURLString) {
            await UIApplication.shared.open(url)
        }
    }

    private func refresh() async {
        let notifications = await UNUserNotificationCenter.current().notificationSettings().authorizationStatus
        let photos = PHPhotoLibrary.authorizationStatus(for: .readWrite)
        guard notificationAuthorization != notifications || photosAuthorization != photos else { return }
        notificationAuthorization = notifications
        photosAuthorization = photos
        NotificationCenter.default.post(name: .SettingsUpdate, object: nil)
    }
}

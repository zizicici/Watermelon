import AppKit
import Foundation

enum MacExternalVolumeLifecyclePolicy {
    static func shouldDisconnect(
        profile: ServerProfileRecord?,
        unmountedVolumeURL: URL
    ) -> Bool {
        guard let profile,
              profile.resolvedStorageType == .externalVolume,
              let displayPath =
                profile.externalVolumeParams?.displayPath,
              !displayPath.isEmpty else {
            return false
        }

        let rootComponents = URL(
            fileURLWithPath: displayPath,
            isDirectory: true
        ).standardizedFileURL.pathComponents
        let volumeComponents =
            unmountedVolumeURL.standardizedFileURL.pathComponents
        guard rootComponents.count >= volumeComponents.count else {
            return false
        }
        return zip(volumeComponents, rootComponents).allSatisfy(==)
    }
}

@MainActor
final class MacExternalVolumeLifecycleMonitor {
    private let notificationCenter: NotificationCenter
    private let activeProfile: () -> ServerProfileRecord?
    private let disconnect: () -> Void
    private let refreshReachability: () -> Void
    nonisolated(unsafe) private var observers: [NSObjectProtocol] = []

    init(
        notificationCenter: NotificationCenter =
            NSWorkspace.shared.notificationCenter,
        activeProfile: @escaping () -> ServerProfileRecord?,
        disconnect: @escaping () -> Void,
        refreshReachability: @escaping () -> Void
    ) {
        self.notificationCenter = notificationCenter
        self.activeProfile = activeProfile
        self.disconnect = disconnect
        self.refreshReachability = refreshReachability
    }

    deinit {
        observers.forEach(notificationCenter.removeObserver)
    }

    func start() {
        guard observers.isEmpty else { return }
        observers.append(
            notificationCenter.addObserver(
                forName: NSWorkspace.didUnmountNotification,
                object: nil,
                queue: .main
            ) { [weak self] notification in
                let volumeURL = notification.userInfo?[
                    NSWorkspace.volumeURLUserInfoKey
                ] as? URL
                MainActor.assumeIsolated {
                    self?.handleUnmount(volumeURL)
                }
            }
        )
        observers.append(
            notificationCenter.addObserver(
                forName: NSWorkspace.didMountNotification,
                object: nil,
                queue: .main
            ) { [weak self] _ in
                MainActor.assumeIsolated {
                    self?.refreshReachability()
                }
            }
        )
    }

    func stop() {
        observers.forEach(notificationCenter.removeObserver)
        observers.removeAll()
    }

    private func handleUnmount(_ volumeURL: URL?) {
        defer { refreshReachability() }
        guard let volumeURL,
            MacExternalVolumeLifecyclePolicy.shouldDisconnect(
                profile: activeProfile(),
                unmountedVolumeURL: volumeURL
            ) else {
            return
        }
        disconnect()
    }
}

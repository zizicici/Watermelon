import Foundation

// Read from multiple threads (e.g. the browser's presence rebuild reads `activeProfile` off the main thread),
// so the two fields are NSLock-guarded. Writes stay main-driven; notifications fire outside the lock.
final class AppSession: @unchecked Sendable {
    private let lock = NSLock()
    private var _activeProfile: ServerProfileRecord?
    private var _activePassword: String?

    private var _onSessionChanged: ((ServerProfileRecord?) -> Void)?

    var activeProfile: ServerProfileRecord? { lock.withLock { _activeProfile } }
    var activePassword: String? { lock.withLock { _activePassword } }

    // Lock-guarded like the fields so the `@unchecked Sendable` claim holds for every stored property.
    var onSessionChanged: ((ServerProfileRecord?) -> Void)? {
        get { lock.withLock { _onSessionChanged } }
        set { lock.withLock { _onSessionChanged = newValue } }
    }

    func activate(profile: ServerProfileRecord, password: String) {
        // Snapshot the callback under the same lock as the write, then invoke it outside the lock.
        let callback: ((ServerProfileRecord?) -> Void)? = lock.withLock {
            _activeProfile = profile
            _activePassword = password
            return _onSessionChanged
        }
        callback?(profile)
        NotificationCenter.default.post(name: .AppSessionChanged, object: nil)
    }

    func clear() {
        let callback: ((ServerProfileRecord?) -> Void)? = lock.withLock {
            _activeProfile = nil
            _activePassword = nil
            return _onSessionChanged
        }
        callback?(nil)
        NotificationCenter.default.post(name: .AppSessionChanged, object: nil)
    }

    func setActiveBackgroundBackupEnabled(_ enabled: Bool, profileID: Int64) {
        lock.withLock {
            guard _activeProfile?.id == profileID else { return }
            _activeProfile?.backgroundBackupEnabled = enabled
        }
    }

    func setActiveGenerateRemoteThumbnails(_ enabled: Bool, profileID: Int64) {
        lock.withLock {
            guard _activeProfile?.id == profileID else { return }
            _activeProfile?.generateRemoteThumbnails = enabled
        }
    }
}

extension Notification.Name {
    static let BackgroundBackupProfileChanged = Notification.Name("Watermelon.BackgroundBackupProfileChanged")
    static let ProfileListChanged = Notification.Name("Watermelon.ProfileListChanged")
    // Posted when the active remote session is established or cleared (drives live UI like browser tabs).
    static let AppSessionChanged = Notification.Name("Watermelon.AppSessionChanged")
}

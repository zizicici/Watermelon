import Foundation

final class AppSession {
    private(set) var activeProfile: ServerProfileRecord?
    private(set) var activePassword: String?

    var onSessionChanged: ((ServerProfileRecord?) -> Void)?

    func activate(profile: ServerProfileRecord, password: String) {
        activeProfile = profile
        activePassword = password
        onSessionChanged?(profile)
        NotificationCenter.default.post(name: .AppSessionChanged, object: nil)
    }

    func clear() {
        activeProfile = nil
        activePassword = nil
        onSessionChanged?(nil)
        NotificationCenter.default.post(name: .AppSessionChanged, object: nil)
    }

    func setActiveBackgroundBackupEnabled(_ enabled: Bool, profileID: Int64) {
        guard activeProfile?.id == profileID else { return }
        activeProfile?.backgroundBackupEnabled = enabled
    }

    func setActiveGenerateRemoteThumbnails(_ enabled: Bool, profileID: Int64) {
        guard activeProfile?.id == profileID else { return }
        activeProfile?.generateRemoteThumbnails = enabled
    }
}

extension Notification.Name {
    static let BackgroundBackupProfileChanged = Notification.Name("Watermelon.BackgroundBackupProfileChanged")
    static let ProfileListChanged = Notification.Name("Watermelon.ProfileListChanged")
    // Posted when the active remote session is established or cleared (drives live UI like browser tabs).
    static let AppSessionChanged = Notification.Name("Watermelon.AppSessionChanged")
}

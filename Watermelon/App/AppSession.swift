import Foundation

final class AppSession {
    private(set) var activeProfile: ServerProfileRecord?
    private(set) var activePassword: String?

    var onSessionChanged: ((ServerProfileRecord?) -> Void)?

    func activate(profile: ServerProfileRecord, password: String) {
        activeProfile = profile
        activePassword = password
        onSessionChanged?(profile)
    }

    func clear() {
        activeProfile = nil
        activePassword = nil
        onSessionChanged?(nil)
    }
}

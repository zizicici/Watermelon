import Foundation

final class BackupSessionConnectionResolver {
    private let appSession: AppSession
    private let databaseManager: DatabaseManager

    init(appSession: AppSession, databaseManager: DatabaseManager) {
        self.appSession = appSession
        self.databaseManager = databaseManager
    }

    func resolveActiveConnection() -> (profile: ServerProfileRecord, password: String)? {
        guard let profile = appSession.activeProfile,
              let password = resolvePassword(for: profile) else {
            return nil
        }
        return (profile, password)
    }

    func handleExternalStorageUnavailableIfNeeded(_ error: Error, for profile: ServerProfileRecord) {
        guard profile.isExternalStorageUnavailableError(error),
              appSession.activeProfile?.id == profile.id else { return }
        try? databaseManager.setActiveServerProfileID(nil)
        appSession.clear()
    }

    private func resolvePassword(for profile: ServerProfileRecord) -> String? {
        if profile.storageProfile.requiresPassword {
            guard let activePassword = appSession.activePassword, !activePassword.isEmpty else {
                return nil
            }
            return activePassword
        }
        return appSession.activePassword ?? ""
    }
}

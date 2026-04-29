import Combine
import Foundation

@MainActor
final class MacDependencyContainer: ObservableObject {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let storageClientFactory: StorageClientFactory
    let profileStore: ProfileStore

    init() {
        let db: DatabaseManager
        do {
            db = try DatabaseManager()
        } catch {
            fatalError("Failed to initialize database: \(error)")
        }
        self.databaseManager = db
        let keychain = KeychainService()
        self.keychainService = keychain
        self.appSession = AppSession()
        self.storageClientFactory = StorageClientFactory(databaseManager: db)
        self.profileStore = ProfileStore(databaseManager: db, keychainService: keychain)

        ExecutionLogFileStore.prepareForBackgroundUse()
        Task.detached(priority: .utility) {
            ExecutionLogFileStore.purgeExpired()
        }
    }

    var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
    }
}

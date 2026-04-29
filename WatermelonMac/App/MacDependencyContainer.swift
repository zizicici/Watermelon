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
            Self.purgeStaleLegacyTempDirs()
        }
    }

    /// Removes leftover legacy-scan temp directories from previous runs that crashed before
    /// their `defer { remove tempDir }` could fire. Anything older than 24 hours is fair game.
    private nonisolated static func purgeStaleLegacyTempDirs() {
        let tmp = FileManager.default.temporaryDirectory
        guard let entries = try? FileManager.default.contentsOfDirectory(
            at: tmp,
            includingPropertiesForKeys: [.contentModificationDateKey],
            options: [.skipsHiddenFiles]
        ) else { return }

        let cutoff = Date().addingTimeInterval(-24 * 3600)
        for url in entries where url.lastPathComponent.hasPrefix("watermelon-legacy-") {
            let mtime = (try? url.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate) ?? .distantPast
            if mtime < cutoff {
                try? FileManager.default.removeItem(at: url)
            }
        }
    }

    var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
    }
}

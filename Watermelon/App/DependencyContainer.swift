import Foundation

final class DependencyContainer {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let storageClientFactory: StorageClientFactory
    let photoLibraryService: PhotoLibraryService
    let metadataService: MetadataService
    let backupExecutor: BackupExecutor
    let restoreService: RestoreService

    init() {
        do {
            databaseManager = try DatabaseManager()
        } catch {
            fatalError("Failed to initialize database: \(error)")
        }

        keychainService = KeychainService()
        appSession = AppSession()
        storageClientFactory = StorageClientFactory()
        photoLibraryService = PhotoLibraryService()
        metadataService = MetadataService()
        backupExecutor = BackupExecutor(
            databaseManager: databaseManager,
            photoLibraryService: photoLibraryService,
            storageClientFactory: storageClientFactory
        )
        restoreService = RestoreService(
            databaseManager: databaseManager,
            storageClientFactory: storageClientFactory
        )
    }

    var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
    }
}

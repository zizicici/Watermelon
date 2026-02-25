import Foundation

final class DependencyContainer {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let photoLibraryService: PhotoLibraryService
    let metadataService: MetadataService
    let manifestSyncService: ManifestSyncService
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
        photoLibraryService = PhotoLibraryService()
        metadataService = MetadataService()
        manifestSyncService = ManifestSyncService(databaseManager: databaseManager)
        backupExecutor = BackupExecutor(
            databaseManager: databaseManager,
            photoLibraryService: photoLibraryService,
            manifestSyncService: manifestSyncService
        )
        restoreService = RestoreService(databaseManager: databaseManager)
    }

    var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
    }
}

import Foundation

final class DependencyContainer {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let storageClientFactory: StorageClientFactory
    let photoLibraryService: PhotoLibraryService
    let hashIndexRepository: ContentHashIndexRepository
    let localHashIndexBuildService: LocalHashIndexBuildService
    let backupCoordinator: BackupCoordinator
    let restoreService: RestoreService

    init() {
        do {
            databaseManager = try DatabaseManager()
        } catch {
            fatalError("Failed to initialize database: \(error)")
        }

        keychainService = KeychainService()
        appSession = AppSession()
        storageClientFactory = StorageClientFactory(databaseManager: databaseManager)
        photoLibraryService = PhotoLibraryService()
        hashIndexRepository = ContentHashIndexRepository(databaseManager: databaseManager)
        localHashIndexBuildService = LocalHashIndexBuildService(
            photoLibraryService: photoLibraryService,
            repository: hashIndexRepository
        )

        backupCoordinator = BackupCoordinator(
            photoLibraryService: photoLibraryService,
            storageClientFactory: storageClientFactory,
            hashIndexRepository: hashIndexRepository
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

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

    convenience init() {
        do {
            try self.init(databaseManager: DatabaseManager())
        } catch {
            fatalError("Failed to initialize database: \(error)")
        }
    }

    private init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager

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

    static func makeForBackgroundTask() throws -> DependencyContainer {
        try DependencyContainer(databaseManager: DatabaseManager())
    }

    var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
    }
}

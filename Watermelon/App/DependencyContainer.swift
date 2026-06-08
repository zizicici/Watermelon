import Foundation

final class DependencyContainer {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let storageClientFactory: StorageClientFactory
    let photoLibraryService: PhotoLibraryService
    let hashIndexRepository: ContentHashIndexRepository
    let localHashIndexBuildService: LocalHashIndexBuildService
    let localIndexChangePublisher: LocalIndexChangePublisher
    let localIndexBuildCoordinator: LocalIndexBuildCoordinator
    let backupCoordinator: BackupCoordinator
    let restoreService: RestoreService
    let appRuntimeFlags: AppRuntimeFlags
    let remoteMaintenanceController: RemoteMaintenanceController
    let profileReachabilityService: ProfileReachabilityService
    // Internal, default-off Repo V2 (Lite) cutover switch. No UI sets it; it stays false in production.
    let liteRepoEnabled: Bool

    convenience init() {
        do {
            try self.init(databaseManager: DatabaseManager())
        } catch {
            fatalError("Failed to initialize database: \(error)")
        }
    }

    private init(databaseManager: DatabaseManager, liteRepoEnabled: Bool = false) {
        self.databaseManager = databaseManager
        self.liteRepoEnabled = liteRepoEnabled

        keychainService = KeychainService()
        appSession = AppSession()
        storageClientFactory = StorageClientFactory(databaseManager: databaseManager)
        photoLibraryService = PhotoLibraryService()
        hashIndexRepository = ContentHashIndexRepository(databaseManager: databaseManager)
        localHashIndexBuildService = LocalHashIndexBuildService(
            photoLibraryService: photoLibraryService,
            repository: hashIndexRepository
        )
        let localIndexChangePublisher = LocalIndexChangePublisher()
        self.localIndexChangePublisher = localIndexChangePublisher
        localIndexBuildCoordinator = LocalIndexBuildCoordinator(
            buildService: localHashIndexBuildService,
            photoLibraryService: photoLibraryService,
            hashIndexRepository: hashIndexRepository,
            changePublisher: localIndexChangePublisher
        )

        let backupCoordinator = BackupCoordinator(
            photoLibraryService: photoLibraryService,
            storageClientFactory: storageClientFactory,
            hashIndexRepository: hashIndexRepository,
            liteRepoEnabled: liteRepoEnabled
        )
        self.backupCoordinator = backupCoordinator

        restoreService = RestoreService(
            databaseManager: databaseManager,
            storageClientFactory: storageClientFactory
        )

        let appRuntimeFlags = AppRuntimeFlags()
        self.appRuntimeFlags = appRuntimeFlags
        self.remoteMaintenanceController = RemoteMaintenanceController(
            backupCoordinator: backupCoordinator,
            appRuntimeFlags: appRuntimeFlags,
            databaseManager: databaseManager
        )
        let profileReachabilityService = ProfileReachabilityService()
        profileReachabilityService.start()
        self.profileReachabilityService = profileReachabilityService
    }

    static func makeForBackgroundTask() throws -> DependencyContainer {
        try DependencyContainer(databaseManager: DatabaseManager())
    }

    var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
    }
}

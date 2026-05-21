import Foundation

final class DependencyContainer {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let storageClientFactory: StorageClientFactory
    let photoLibraryService: PhotoLibraryService
    let hashIndexRepository: ContentHashIndexRepository
    let localHashIndexBuildService: LocalHashIndexBuildService
    let restoredAssetFingerprintVerifier: RestoredAssetFingerprintVerifier
    let localIndexChangePublisher: LocalIndexChangePublisher
    let localIndexBuildCoordinator: LocalIndexBuildCoordinator
    let backupCoordinator: BackupCoordinator
    let restoreService: RestoreService
    let appRuntimeFlags: AppRuntimeFlags
    let remoteMaintenanceController: RemoteMaintenanceController
    let profileReachabilityService: ProfileReachabilityService

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
        let localHashIndexBuildService = LocalHashIndexBuildService(
            photoLibraryService: photoLibraryService,
            repository: hashIndexRepository
        )
        self.localHashIndexBuildService = localHashIndexBuildService
        let hashIndexRepositoryForVerifier = hashIndexRepository
        restoredAssetFingerprintVerifier = RestoredAssetFingerprintVerifier(
            buildIndex: { ids in
                try await localHashIndexBuildService.buildIndex(
                    for: ids,
                    workerCount: 1,
                    allowNetworkAccess: false
                )
            },
            fetchRecords: { ids in
                try hashIndexRepositoryForVerifier.fetchAssetFingerprintRecords(assetIDs: ids)
            }
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
            databaseManager: databaseManager
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

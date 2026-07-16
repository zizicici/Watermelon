import Foundation

final class DependencyContainer {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let storageClientFactory: StorageClientFactory
    let oneDriveCredentialLifecycleService: OneDriveCredentialLifecycleService
    let oneDriveProfileSetupCoordinator: OneDriveProfileSetupCoordinator
    let storageProfileConnectionService: StorageProfileConnectionService
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

    convenience init() {
        do {
            try self.init(databaseManager: DatabaseManager())
        } catch {
            fatalError("Failed to initialize database: \(error)")
        }
    }

    private init(
        databaseManager: DatabaseManager,
        startProfileReachability: Bool = true,
        reconcileOneDriveAccounts: Bool = true
    ) {
        self.databaseManager = databaseManager

        keychainService = KeychainService()
        appSession = AppSession()
        let oneDriveAuthenticationService = OneDriveMSALService()
        let oneDriveSharedState = OneDriveSharedState()
        let oneDriveCredentialLifecycleService = OneDriveCredentialLifecycleService(
            databaseManager: databaseManager,
            keychainService: keychainService,
            authenticationService: oneDriveAuthenticationService
        )
        self.oneDriveCredentialLifecycleService = oneDriveCredentialLifecycleService
        if reconcileOneDriveAccounts {
            oneDriveCredentialLifecycleService.reconcileCachedAccounts()
        }
        let oneDriveAppFolderBootstrapService = OneDriveAppFolderBootstrapService(
            tokenProvider: oneDriveAuthenticationService,
            sharedState: oneDriveSharedState
        )
        oneDriveProfileSetupCoordinator = OneDriveProfileSetupCoordinator(
            authenticationService: oneDriveAuthenticationService,
            bootstrapService: oneDriveAppFolderBootstrapService,
            sharedState: oneDriveSharedState,
            credentialLifecycleService: oneDriveCredentialLifecycleService
        )
        storageClientFactory = StorageClientFactory(
            databaseManager: databaseManager,
            oneDriveTokenProvider: oneDriveAuthenticationService,
            oneDriveSharedState: oneDriveSharedState
        )
        storageProfileConnectionService = StorageProfileConnectionService(
            databaseManager: databaseManager
        )
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
        if startProfileReachability {
            profileReachabilityService.start()
        }
        self.profileReachabilityService = profileReachabilityService
    }

    static func makeForBackgroundTask() throws -> DependencyContainer {
        try DependencyContainer(
            databaseManager: DatabaseManager(),
            startProfileReachability: false,
            reconcileOneDriveAccounts: false
        )
    }

    var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0"
    }
}

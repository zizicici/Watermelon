import Foundation

final class DependencyContainer {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let storageClientFactory: StorageClientFactory
    private let oneDriveDependencyProvider: OneDriveDependencyProvider
    var oneDriveCredentialLifecycleService: OneDriveCredentialLifecycleService {
        oneDriveDependencyProvider.credentialLifecycleService
    }
    @MainActor var oneDriveProfileSetupCoordinator: OneDriveProfileSetupCoordinator {
        oneDriveDependencyProvider.profileSetupCoordinator
    }
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
        let oneDriveDependencyProvider = OneDriveDependencyProvider(
            databaseManager: databaseManager,
            keychainService: keychainService
        )
        self.oneDriveDependencyProvider = oneDriveDependencyProvider
        if reconcileOneDriveAccounts {
            oneDriveDependencyProvider.reconcileCachedAccountsIfOneDriveProfileExists()
        }
        storageClientFactory = StorageClientFactory(
            databaseManager: databaseManager,
            oneDriveClientContextProvider: {
                oneDriveDependencyProvider.clientContext()
            }
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

private final class OneDriveDependencyProvider: @unchecked Sendable {
    private let databaseManager: DatabaseManager
    private let keychainService: KeychainService
    private let lock = NSLock()
    private var scope: OneDriveDependencyScope?

    init(
        databaseManager: DatabaseManager,
        keychainService: KeychainService
    ) {
        self.databaseManager = databaseManager
        self.keychainService = keychainService
    }

    var credentialLifecycleService: OneDriveCredentialLifecycleService {
        resolveScope().credentialLifecycleService
    }

    @MainActor
    var profileSetupCoordinator: OneDriveProfileSetupCoordinator {
        resolveScope().profileSetupCoordinator
    }

    func clientContext() -> StorageClientFactory.OneDriveClientContext {
        resolveScope().clientContext
    }

    func reconcileCachedAccountsIfOneDriveProfileExists() {
        guard (try? databaseManager.fetchServerProfiles().contains {
            $0.resolvedStorageType == .onedrive
        }) == true else { return }
        resolveScope().credentialLifecycleService.reconcileCachedAccounts()
    }

    private func resolveScope() -> OneDriveDependencyScope {
        lock.withLock {
            if let scope { return scope }
            let newScope = OneDriveDependencyScope(
                databaseManager: databaseManager,
                keychainService: keychainService
            )
            scope = newScope
            return newScope
        }
    }
}

private final class OneDriveDependencyScope: @unchecked Sendable {
    let authenticationService: OneDriveMSALService
    let sharedState: OneDriveSharedState
    let credentialLifecycleService: OneDriveCredentialLifecycleService
    let profileSetupCoordinator: OneDriveProfileSetupCoordinator

    init(
        databaseManager: DatabaseManager,
        keychainService: KeychainService
    ) {
        let authenticationService = OneDriveMSALService()
        let sharedState = OneDriveSharedState()
        let credentialLifecycleService = OneDriveCredentialLifecycleService(
            databaseManager: databaseManager,
            keychainService: keychainService,
            authenticationService: authenticationService
        )
        let appFolderBootstrapService = OneDriveAppFolderBootstrapService(
            tokenProvider: authenticationService,
            sharedState: sharedState
        )

        self.authenticationService = authenticationService
        self.sharedState = sharedState
        self.credentialLifecycleService = credentialLifecycleService
        profileSetupCoordinator = OneDriveProfileSetupCoordinator(
            authenticationService: authenticationService,
            bootstrapService: appFolderBootstrapService,
            sharedState: sharedState,
            credentialLifecycleService: credentialLifecycleService
        )
    }

    var clientContext: StorageClientFactory.OneDriveClientContext {
        StorageClientFactory.OneDriveClientContext(
            tokenProvider: authenticationService,
            sharedState: sharedState
        )
    }
}

import Combine
import Foundation

@MainActor
final class MacDependencyContainer: ObservableObject {
    let databaseManager: DatabaseManager
    let keychainService: KeychainService
    let appSession: AppSession
    let oneDriveAuthenticationService: OneDriveMSALService
    let oneDriveSharedState: OneDriveSharedState
    let oneDriveCredentialLifecycleService:
        OneDriveCredentialLifecycleService
    let oneDriveProfileSetupCoordinator:
        OneDriveProfileSetupCoordinator
    let storageClientFactory: StorageClientFactory
    let storageProfileConnectionService: StorageProfileConnectionService
    let profileReachabilityService: ProfileReachabilityService
    let remoteIndexService: RemoteIndexSyncService
    let remoteLibraryReadService: RemoteLibraryReadService
    let profileStore: ProfileStore
    let photoLibraryService: PhotoLibraryService
    let hashIndexRepository: ContentHashIndexRepository
    let localHashIndexBuildService: LocalHashIndexBuildService
    let localIndexChangePublisher: LocalIndexChangePublisher
    let localIndexBuildCoordinator: LocalIndexBuildCoordinator
    let remoteThumbnailMaintenanceService:
        MacRemoteThumbnailMaintenanceService
    let backupCoordinator: BackupCoordinator
    let restoreService: RestoreService
    let downloadWorkflowHelper: DownloadWorkflowHelper
    let appRuntimeFlags: AppRuntimeFlags
    let remoteMaintenanceController: RemoteMaintenanceController
    let photoLibraryMonthlyIndexWorker: PhotoLibraryMonthlyIndexWorker
    let photoLibraryAuthorizationProvider: any PhotoLibraryAuthorizationProviding

    init() {
        let db: DatabaseManager
        do {
            db = try DatabaseManager()
        } catch {
            fatalError("Failed to initialize database: \(error)")
        }
        self.databaseManager = db
        let keychain = KeychainService()
        let appRuntimeFlags = AppRuntimeFlags()
        self.keychainService = keychain
        self.appRuntimeFlags = appRuntimeFlags
        self.appSession = AppSession()
        let oneDriveAuthenticationService = OneDriveMSALService()
        let oneDriveSharedState = OneDriveSharedState()
        let oneDriveCredentialLifecycleService =
            OneDriveCredentialLifecycleService(
                databaseManager: db,
                keychainService: keychain,
                authenticationService:
                    oneDriveAuthenticationService
            )
        let oneDriveBootstrapService =
            OneDriveAppFolderBootstrapService(
                tokenProvider: oneDriveAuthenticationService,
                sharedState: oneDriveSharedState
            )
        self.oneDriveAuthenticationService =
            oneDriveAuthenticationService
        self.oneDriveSharedState = oneDriveSharedState
        self.oneDriveCredentialLifecycleService =
            oneDriveCredentialLifecycleService
        self.oneDriveProfileSetupCoordinator =
            OneDriveProfileSetupCoordinator(
                authenticationService:
                    oneDriveAuthenticationService,
                bootstrapService: oneDriveBootstrapService,
                sharedState: oneDriveSharedState,
                credentialLifecycleService:
                    oneDriveCredentialLifecycleService
            )
        let storageClientFactory = StorageClientFactory(
            databaseManager: db,
            oneDriveTokenProvider:
                oneDriveAuthenticationService,
            oneDriveSharedState: oneDriveSharedState
        )
        self.storageClientFactory = storageClientFactory
        self.storageProfileConnectionService = StorageProfileConnectionService(
            databaseManager: db
        )
        let profileReachabilityService = ProfileReachabilityService()
        profileReachabilityService.start()
        self.profileReachabilityService = profileReachabilityService
        let remoteIndexService = RemoteIndexSyncService()
        self.remoteIndexService = remoteIndexService
        self.remoteLibraryReadService = RemoteLibraryReadService(
            storageClientFactory: storageClientFactory,
            remoteIndexService: remoteIndexService
        )
        self.profileStore = ProfileStore(
            databaseManager: db,
            keychainService: keychain,
            appRuntimeFlags: appRuntimeFlags,
            oneDriveCredentialLifecycleService:
                oneDriveCredentialLifecycleService
        )
        let photoLibraryService = PhotoLibraryService()
        let hashIndexRepository = ContentHashIndexRepository(databaseManager: db)
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.localHashIndexBuildService = LocalHashIndexBuildService(
            photoLibraryService: photoLibraryService,
            repository: hashIndexRepository
        )
        let localIndexChangePublisher = LocalIndexChangePublisher()
        self.localIndexChangePublisher = localIndexChangePublisher
        self.localIndexBuildCoordinator = LocalIndexBuildCoordinator(
            buildService: localHashIndexBuildService,
            photoLibraryService: photoLibraryService,
            hashIndexRepository: hashIndexRepository,
            changePublisher: localIndexChangePublisher,
            canRunAutomaticRevalidation: {
                !appRuntimeFlags.isExecuting
            }
        )
        self.remoteThumbnailMaintenanceService =
            MacRemoteThumbnailMaintenanceService(
                storageClientFactory: storageClientFactory,
                hashIndexRepository: hashIndexRepository,
                photoLibraryService: photoLibraryService
            )
        self.backupCoordinator = BackupCoordinator(
            photoLibraryService: photoLibraryService,
            storageClientFactory: storageClientFactory,
            hashIndexRepository: hashIndexRepository,
            databaseManager: db,
            remoteIndexService: remoteIndexService,
            thumbnailRenderer: MacThumbnailRenderer()
        )
        let restoreService = RestoreService(
            databaseManager: db,
            storageClientFactory: storageClientFactory
        )
        self.restoreService = restoreService
        self.downloadWorkflowHelper = DownloadWorkflowHelper(
            restoreService: restoreService,
            hashIndexRepository: hashIndexRepository
        )
        self.remoteMaintenanceController = RemoteMaintenanceController(
            backupCoordinator: backupCoordinator,
            appRuntimeFlags: appRuntimeFlags,
            databaseManager: db
        )
        self.photoLibraryMonthlyIndexWorker = PhotoLibraryMonthlyIndexWorker(
            photoLibraryService: photoLibraryService,
            contentHashIndexRepository: hashIndexRepository
        )
        self.photoLibraryAuthorizationProvider = photoLibraryService

        if (try? db.fetchServerProfiles().contains {
            $0.resolvedStorageType == .onedrive
        }) == true {
            oneDriveCredentialLifecycleService
                .reconcileCachedAccounts()
        }

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

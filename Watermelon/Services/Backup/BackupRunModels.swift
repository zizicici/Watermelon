import Foundation

enum BackupRunMode: Sendable {
    case full
    case scoped(assetIDs: Set<String>)
    case retry(assetIDs: Set<String>)

    var isRetry: Bool {
        if case .retry = self {
            return true
        }
        return false
    }

    var targetAssetIdentifiers: Set<String>? {
        switch self {
        case .full:
            return nil
        case .retry(let assetIDs):
            return assetIDs
        case .scoped(let assetIDs):
            return assetIDs
        }
    }
}

enum BackupTerminationIntent: Sendable {
    case none
    case pause
    case stop
}

enum BackupMonthFinalizationResult: Sendable {
    case success
    case failed(String)
    case cancelled
}

// Carried into the upload finalizer so it can reuse the run's live write lease for verification instead
// of acquiring/releasing an independent same-writer maintenance session (which would drop the outer
// lock). `liteSession` is nil under V1; `manifestLayout` selects the verify manifest path.
struct BackupMonthUploadContext: Sendable {
    let liteSession: LiteWriteSession?
    let manifestLayout: MonthManifestStore.ManifestLayout
}

typealias BackupMonthFinalizer = @Sendable @MainActor (LibraryMonthKey, BackupMonthUploadContext) async -> BackupMonthFinalizationResult

struct BackupRunConfigurationOverride: Sendable {
    let workerCountOverride: Int?
    let iCloudPhotoBackupMode: ICloudPhotoBackupMode
}

struct BackupRunRequest: Sendable {
    let profile: ServerProfileRecord
    let password: String
    let onlyAssetLocalIdentifiers: Set<String>?
    let workerCountOverride: Int?
    let iCloudPhotoBackupMode: ICloudPhotoBackupMode
    let onMonthUploaded: BackupMonthFinalizer?

    init(
        profile: ServerProfileRecord,
        password: String,
        onlyAssetLocalIdentifiers: Set<String>?,
        workerCountOverride: Int? = nil,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode = .disable,
        onMonthUploaded: BackupMonthFinalizer? = nil
    ) {
        self.profile = profile
        self.password = password
        self.onlyAssetLocalIdentifiers = onlyAssetLocalIdentifiers
        self.workerCountOverride = workerCountOverride
        self.iCloudPhotoBackupMode = iCloudPhotoBackupMode
        self.onMonthUploaded = onMonthUploaded
    }
}

struct BackupRunState: Sendable {
    var total: Int = 0
    var succeeded: Int = 0
    var failed: Int = 0
    var skipped: Int = 0
    var paused: Bool = false

    var processed: Int {
        succeeded + failed + skipped
    }
}

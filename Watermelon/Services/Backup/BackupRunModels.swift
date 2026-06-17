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
    case fatal(String, LiteRepoError)
    case cancelled
}

struct RepoWriteMode: Sendable {
    let leaseSession: RepoLeaseSession
    let liteMonthsListing: LiteMonthsListingSnapshot?

    static func lite(_ session: RepoLeaseSession, _ monthsListing: LiteMonthsListingSnapshot?) -> RepoWriteMode {
        RepoWriteMode(leaseSession: session, liteMonthsListing: monthsListing)
    }

    var manifestLayout: MonthManifestStore.ManifestLayout {
        .lite
    }

    // Write-tier per-month lease gate (load + manifest flush, in-run verify): read-only ownership proof,
    // never writes the lock. The refresh task remains the sole lock writer.
    var leaseProvenAssertion: MonthManifestOwnershipAssertion? {
        { try await leaseSession.assertLeaseProvenForWrite() }
    }

    func stopAndRelease() async {
        await leaseSession.stopAndRelease()
    }
}

// Carried into the upload finalizer so it can reuse the run's live write lease for verification instead
// of acquiring/releasing an independent same-writer maintenance session (which would drop the outer
// lock).
struct BackupMonthUploadContext: Sendable {
    let writeMode: RepoWriteMode
}

typealias BackupMonthFinalizer = @Sendable @MainActor (LibraryMonthKey, BackupMonthUploadContext) async -> BackupMonthFinalizationResult

struct BackupDownloadVerificationPlan: Sendable {
    private let verifyMonth: @Sendable (LibraryMonthKey) async throws -> Void

    init(verifyMonth: @escaping @Sendable (LibraryMonthKey) async throws -> Void) {
        self.verifyMonth = verifyMonth
    }

    func verify(month: LibraryMonthKey) async throws {
        try await verifyMonth(month)
    }
}

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

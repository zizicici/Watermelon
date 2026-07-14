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
    let session: AnyRepoWriteSession
    let liteMonthsListing: LiteMonthsListingSnapshot?

    static func lite<Session: RepoWriteSession>(
        _ session: Session,
        _ monthsListing: LiteMonthsListingSnapshot?
    ) -> RepoWriteMode {
        RepoWriteMode(session: AnyRepoWriteSession(session), liteMonthsListing: monthsListing)
    }

    var manifestLayout: MonthManifestStore.ManifestLayout {
        .lite
    }

    // Write-tier per-month lease gate (load + manifest flush, in-run verify): read-only ownership proof,
    // never writes the lock. The refresh task remains the sole lock writer.
    var controlWriteAssertion: MonthManifestOwnershipAssertion? {
        { try await session.assertControlWriteAllowed(now: Date()) }
    }

    func stopAndRelease() async {
        await session.release()
    }
}

struct RepoMaintenancePlan: Sendable {
    let layout: MonthManifestStore.ManifestLayout
    let session: AnyRepoWriteSession?
    let monthsListing: LiteMonthsListingSnapshot?

    init(_ plan: RemoteLiteRepoGateway.MaintenancePlan) {
        layout = plan.layout
        session = plan.session.map(AnyRepoWriteSession.init)
        monthsListing = plan.monthsListing
    }

    init(_ plan: LocalVolumeRepoGateway.MaintenancePlan) {
        layout = plan.layout
        session = plan.session.map(AnyRepoWriteSession.init)
        monthsListing = plan.monthsListing
    }

    init(
        layout: MonthManifestStore.ManifestLayout,
        session: AnyRepoWriteSession?,
        monthsListing: LiteMonthsListingSnapshot?
    ) {
        self.layout = layout
        self.session = session
        self.monthsListing = monthsListing
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
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference

    init(
        workerCountOverride: Int?,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode,
        monthGroupingTimeZone: MonthGroupingTimeZonePreference
    ) {
        self.workerCountOverride = workerCountOverride
        self.iCloudPhotoBackupMode = iCloudPhotoBackupMode
        self.monthGroupingTimeZone = monthGroupingTimeZone
    }
}

// Limits a run to a subset of months. `.recentMonths` scopes both the asset fetch and the remote-index
// sync so a scoped run never materializes the full library snapshot.
enum BackupMonthScope: Sendable {
    case all
    case recentMonths(Int)
}

// Background declines safely pre-lock (`.skip`); foreground never does. Selects the lease gateway in prepareRun.
enum BackupLeaseMode: Sendable {
    case foreground
    case background
}

// Month processing order. `.balanced` (LPT: largest months first) maximizes multi-worker throughput.
// `.newestMonthFirst` ensures a single-worker, time-boxed run (background under BG-task expiration) makes
// progress on the most recent month before older, possibly larger ones can starve it.
enum BackupMonthOrdering: Sendable {
    case balanced
    case newestMonthFirst
}

// prepareRun throws this when a background run declines the lease safely — distinct from a failure so it
// is never logged as a prepare error.
struct BackupRunSkipped: Error {}

typealias BackupMonthAssetIDsProvider = @Sendable () async -> [MonthKey: [String]]

actor BackupMonthAssetIDsCache {
    private let loader: @Sendable () -> [MonthKey: [String]]
    private var cached: [MonthKey: [String]]?

    init(loader: @escaping @Sendable () -> [MonthKey: [String]]) {
        self.loader = loader
    }

    func load() -> [MonthKey: [String]] {
        if let cached { return cached }
        let loaded = loader()
        cached = loaded
        return loaded
    }
}

// Thrown when a worker's bounded network recovery (reconnect + backoff) is exhausted. The reducer maps it
// to a resumable pause (not failed): the network is down, not the data, so resume continues uncommitted work.
struct BackupNetworkRecoveryExhausted: Error {
    let underlying: Error
}

struct BackupRunRequest: Sendable {
    let profile: ServerProfileRecord
    let password: String
    let onlyAssetLocalIdentifiers: Set<String>?
    let workerCountOverride: Int?
    let iCloudPhotoBackupMode: ICloudPhotoBackupMode
    let monthScope: BackupMonthScope
    let monthAssetIDsProvider: BackupMonthAssetIDsProvider?
    let monthOrdering: BackupMonthOrdering
    let leaseMode: BackupLeaseMode
    let incrementalFlushInterval: Int?
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference
    let monthScopeNow: Date
    let onMonthUploaded: BackupMonthFinalizer?

    init(
        profile: ServerProfileRecord,
        password: String,
        onlyAssetLocalIdentifiers: Set<String>?,
        workerCountOverride: Int? = nil,
        iCloudPhotoBackupMode: ICloudPhotoBackupMode = .disable,
        monthScope: BackupMonthScope = .all,
        monthAssetIDsProvider: BackupMonthAssetIDsProvider? = nil,
        monthOrdering: BackupMonthOrdering = .balanced,
        leaseMode: BackupLeaseMode = .foreground,
        incrementalFlushInterval: Int? = nil,
        monthGroupingTimeZone: MonthGroupingTimeZonePreference,
        monthScopeNow: Date = Date(),
        onMonthUploaded: BackupMonthFinalizer? = nil
    ) {
        self.profile = profile
        self.password = password
        self.onlyAssetLocalIdentifiers = onlyAssetLocalIdentifiers
        self.workerCountOverride = workerCountOverride
        self.iCloudPhotoBackupMode = iCloudPhotoBackupMode
        self.monthScope = monthScope
        self.monthAssetIDsProvider = monthAssetIDsProvider
        self.monthOrdering = monthOrdering
        self.leaseMode = leaseMode
        self.incrementalFlushInterval = incrementalFlushInterval
        self.monthGroupingTimeZone = monthGroupingTimeZone
        self.monthScopeNow = monthScopeNow
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

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

struct BackupFinalizationFailure: @unchecked Sendable {
    let message: String
    let underlyingError: Error?

    init(message: String, underlyingError: Error? = nil) {
        self.message = message
        self.underlyingError = underlyingError
    }
}

enum BackupMonthFinalizationResult: Sendable {
    case success
    case incomplete(BackupMonthIncompleteSummary)
    case failed(BackupFinalizationFailure)
    case cancelled
}

enum BackupMonthIncompleteSummaryRenderer {
    static func message(
        for summary: BackupMonthIncompleteSummary,
        month: LibraryMonthKey
    ) -> String {
        let parts = messageParts(for: summary, month: month)
        return parts.isEmpty ? String(localized: "home.execution.partialFailed") : parts.joined(separator: ". ")
    }

    static func messageParts(
        for summary: BackupMonthIncompleteSummary,
        month: LibraryMonthKey
    ) -> [String] {
        var parts: [String] = []
        let issues = summary.downloadIssues
        if issues.skippedIncompleteCount > 0 {
            parts.append(String.localizedStringWithFormat(
                String(localized: "restore.log.skippedIncomplete"),
                month.displayText,
                issues.skippedIncompleteCount
            ))
        }
        if issues.fingerprintMismatchCount > 0 {
            parts.append(String.localizedStringWithFormat(
                String(localized: "restore.log.fingerprintMismatch"),
                month.displayText,
                issues.fingerprintMismatchCount
            ))
        }
        if issues.localFingerprintVerificationIncompleteCount > 0 {
            parts.append(String.localizedStringWithFormat(
                String(localized: "restore.log.unverifiedFingerprint"),
                month.displayText,
                issues.localFingerprintVerificationIncompleteCount
            ))
        }
        if let message = summary.metadataSnapshotDeferredMessage, !message.isEmpty {
            parts.append(message)
        }
        return parts
    }
}

typealias BackupMonthFinalizer = @Sendable @MainActor (LibraryMonthKey) async -> BackupMonthFinalizationResult

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

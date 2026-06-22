import Foundation

// Fail-closed faults for Repo V2 (Lite) routing and ownership. These surface in the execution log via
// prepareFailed, so descriptions are localized.
enum LiteRepoError: LocalizedError, Equatable, Sendable {
    case repoDamaged                           // .damaged: Lite data with no committed/usable version
    case repoUnsupported(minAppVersion: String? = nil) // .unsupported: future/foreign committed format
    case repoMaintenanceUnavailable            // pure read / verify on a not-yet-committed fresh repo
    case probeFault(RemoteFaultLite.Category)  // router probe could not be resolved
    case lockConflict                          // foreground lock blocked by another live writer
    case ownLockConflict(WriteLockService.OwnLockBlock? = nil) // previous same-writer session is still fresh
    case lockFault(RemoteFaultLite.Category)   // lock acquire transport fault
    case writerIdentityUnavailable             // profile has no usable writerID
    case versionCommitFailed                   // version.json write/read-back failed
    case leaseConfidenceLost                   // pre-upload gate: lease no longer confidently held
    case ownershipLost                         // pre-flush gate: ownership could not be re-asserted
    case existingLiteManifestConflict(month: String)
    case v1MonthManifestUnreadable(month: String)
    case v1SourceChangedDuringMigration

    static func ownLockConflictRetryTimeText(_ date: Date) -> String {
        DateFormatter.localizedString(from: date, dateStyle: .none, timeStyle: .short)
    }

    static func ownLockConflictReasonText(_ reason: WriteLockService.OwnLockBlock.Reason) -> String {
        switch reason {
        case .stillFresh:
            return String(localized: "backup.repo.ownLockConflict.reason.stillFresh")
        case .missingTimeEvidence:
            return String(localized: "backup.repo.ownLockConflict.reason.missingTimeEvidence")
        case .changedDuringConfirmation:
            return String(localized: "backup.repo.ownLockConflict.reason.changedDuringConfirmation")
        case .ownershipUnverified:
            return String(localized: "backup.repo.ownLockConflict.reason.ownershipUnverified")
        }
    }

    struct Disposition: Equatable, Sendable {
        let isCancellation: Bool
        let isLeaseOwnershipLoss: Bool
        let shouldAbortRemoteIndexSync: Bool
        let shouldContinueDownloadVerify: Bool

        var isUploadFailFast: Bool { isLeaseOwnershipLoss }
        var preservesOriginalDuringVersionCommit: Bool { isLeaseOwnershipLoss }
    }

    var disposition: Disposition {
        switch self {
        case .repoDamaged, .repoUnsupported, .repoMaintenanceUnavailable,
             .writerIdentityUnavailable, .versionCommitFailed,
             .existingLiteManifestConflict, .v1MonthManifestUnreadable, .v1SourceChangedDuringMigration:
            return Disposition(
                isCancellation: false,
                isLeaseOwnershipLoss: false,
                shouldAbortRemoteIndexSync: true,
                shouldContinueDownloadVerify: false
            )
        case .probeFault(let category), .lockFault(let category):
            return Self.disposition(forRemoteFault: category)
        case .lockConflict, .ownLockConflict(_):
            return Disposition(
                isCancellation: false,
                isLeaseOwnershipLoss: false,
                shouldAbortRemoteIndexSync: true,
                shouldContinueDownloadVerify: true
            )
        case .leaseConfidenceLost, .ownershipLost:
            return Disposition(
                isCancellation: false,
                isLeaseOwnershipLoss: true,
                shouldAbortRemoteIndexSync: true,
                shouldContinueDownloadVerify: false
            )
        }
    }

    private static func disposition(forRemoteFault category: RemoteFaultLite.Category) -> Disposition {
        switch category {
        case .cancelled:
            return Disposition(
                isCancellation: true,
                isLeaseOwnershipLoss: false,
                shouldAbortRemoteIndexSync: true,
                shouldContinueDownloadVerify: false
            )
        case .retryable:
            return Disposition(
                isCancellation: false,
                isLeaseOwnershipLoss: false,
                shouldAbortRemoteIndexSync: true,
                shouldContinueDownloadVerify: true
            )
        case .notFound, .terminal:
            return Disposition(
                isCancellation: false,
                isLeaseOwnershipLoss: false,
                shouldAbortRemoteIndexSync: true,
                shouldContinueDownloadVerify: false
            )
        }
    }

    var isCancellation: Bool {
        disposition.isCancellation
    }

    // A transient transport fault during repo probe / lock acquire. classify() can't see the category inside a
    // LiteRepoError, so callers that decide pause-vs-fail must consult this to avoid hard-failing on a wobble.
    var isRetryableTransportFault: Bool {
        switch self {
        case .probeFault(let category), .lockFault(let category):
            return category == .retryable
        default:
            return false
        }
    }

    var isLeaseOwnershipLoss: Bool {
        disposition.isLeaseOwnershipLoss
    }

    var isUploadFailFast: Bool {
        disposition.isUploadFailFast
    }

    var preservesOriginalDuringVersionCommit: Bool {
        disposition.preservesOriginalDuringVersionCommit
    }

    var shouldAbortRemoteIndexSync: Bool {
        disposition.shouldAbortRemoteIndexSync
    }

    var shouldContinueDownloadVerify: Bool {
        disposition.shouldContinueDownloadVerify
    }

    var errorDescription: String? {
        switch self {
        case .repoDamaged:
            return String(localized: "backup.repo.damaged")
        case .repoUnsupported(let minAppVersion):
            if let minAppVersion, !minAppVersion.isEmpty {
                return String.localizedStringWithFormat(
                    String(localized: "compatibility.error.remoteFormatUnsupported.versioned"),
                    AppName.localized,
                    minAppVersion
                )
            }
            return String.localizedStringWithFormat(
                String(localized: "profileFormatUnsupported"),
                AppName.localized
            )
        case .repoMaintenanceUnavailable:
            return String(localized: "backup.repo.maintenanceUnavailable")
        case .probeFault(let category):
            return String.localizedStringWithFormat(
                String(localized: "backup.repo.probeFault"),
                String(describing: category)
            )
        case .lockConflict:
            return String(localized: "lockedByAnotherDevice")
        case .ownLockConflict(let block):
            let reason = Self.ownLockConflictReasonText(block?.reason ?? .ownershipUnverified)
            if let retryAfter = block?.retryAfter {
                return String.localizedStringWithFormat(
                    String(localized: "backup.repo.ownLockConflict.retryAfter"),
                    reason,
                    Self.ownLockConflictRetryTimeText(retryAfter)
                )
            }
            return String.localizedStringWithFormat(
                String(localized: "backup.repo.ownLockConflict"),
                reason
            )
        case .lockFault(let category):
            return String.localizedStringWithFormat(
                String(localized: "backup.repo.lockFault"),
                String(describing: category)
            )
        case .writerIdentityUnavailable:
            return String(localized: "backup.repo.writerIdentityUnavailable")
        case .versionCommitFailed:
            return String(localized: "backup.repo.versionCommitFailed")
        case .leaseConfidenceLost:
            return String(localized: "backup.repo.leaseConfidenceLost")
        case .ownershipLost:
            return String(localized: "backup.repo.ownershipLost")
        case .existingLiteManifestConflict(let month):
            return String.localizedStringWithFormat(
                String(localized: "backup.repo.existingLiteManifestConflict"),
                month
            )
        case .v1MonthManifestUnreadable(let month):
            return String.localizedStringWithFormat(
                String(localized: "backup.repo.v1MonthManifestUnreadable"),
                month
            )
        case .v1SourceChangedDuringMigration:
            return String(localized: "backup.repo.v1SourceChangedDuringMigration")
        }
    }
}

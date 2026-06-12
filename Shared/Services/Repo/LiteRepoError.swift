import Foundation

// Fail-closed faults for Repo V2 (Lite) routing and ownership. These surface in the execution log via
// prepareFailed, so descriptions are localized.
enum LiteRepoError: LocalizedError, Equatable, Sendable {
    case repoDamaged                           // .damaged: Lite data with no committed/usable version
    case repoUnsupported(minAppVersion: String? = nil) // .unsupported: future/foreign format or dev-marker dirs
    case repoMaintenanceUnavailable            // pure read / verify on a not-yet-committed fresh repo
    case probeFault(RemoteFaultLite.Category)  // router probe could not be resolved
    case lockConflict                          // foreground lock blocked by another live writer
    case ownLockConflict                       // previous same-writer session is still fresh
    case lockFault(RemoteFaultLite.Category)   // lock acquire transport fault
    case writerIdentityUnavailable             // profile has no usable writerID
    case versionCommitFailed                   // version.json write/read-back failed
    case leaseConfidenceLost                   // pre-upload gate: lease no longer confidently held
    case ownershipLost                         // pre-flush gate: ownership could not be re-asserted
    case existingLiteManifestConflict(month: String)
    case v1MonthManifestUnreadable(month: String)
    case v1SourceChangedDuringMigration

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
        case .ownLockConflict:
            return String(localized: "backup.repo.ownLockConflict")
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

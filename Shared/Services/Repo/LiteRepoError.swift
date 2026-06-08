import Foundation

// Fail-closed faults for the Repo V2 (Lite) cutover. When the flag is on these surface in the
// execution log via prepareFailed, so descriptions are localized.
enum LiteRepoError: LocalizedError, Equatable {
    case repoDamaged                           // .damaged: Lite data with no committed/usable version
    case repoUnsupported                       // .unsupported: future/foreign format or dev-marker dirs
    case probeFault(RemoteFaultLite.Category)  // router probe could not be resolved
    case lockConflict                          // foreground lock blocked by another live writer
    case lockFault(RemoteFaultLite.Category)   // lock acquire transport fault
    case writerIdentityUnavailable             // profile has no usable writerID
    case versionCommitFailed                   // version.json write/read-back failed
    case leaseConfidenceLost                   // pre-upload gate: lease no longer confidently held
    case ownershipLost                         // pre-flush gate: ownership could not be re-asserted

    var errorDescription: String? {
        switch self {
        case .repoDamaged:
            return String(localized: "backup.repo.damaged")
        case .repoUnsupported:
            return String.localizedStringWithFormat(
                String(localized: "profileFormatUnsupported"),
                AppName.localized
            )
        case .probeFault(let category):
            return String.localizedStringWithFormat(
                String(localized: "backup.repo.probeFault"),
                String(describing: category)
            )
        case .lockConflict:
            return String(localized: "lockedByAnotherDevice")
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
        }
    }
}

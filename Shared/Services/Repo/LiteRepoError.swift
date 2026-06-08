import Foundation

// Fail-closed faults for the dormant Repo V2 (Lite) cutover. These only ever surface when the internal
// `liteRepoEnabled` flag is on, which has no UI exposure, so the messages are developer-facing and
// intentionally not localized — a shipped (flag-off) build never reaches them.
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
            return "Repo V2 (Lite): remote repo is damaged; aborting without writes."
        case .repoUnsupported:
            return "Repo V2 (Lite): remote repo format is unsupported; aborting without writes."
        case .probeFault(let category):
            return "Repo V2 (Lite): could not classify remote repo (\(category)); aborting without writes."
        case .lockConflict:
            return "Repo V2 (Lite): another writer holds the lock; aborting without writes."
        case .lockFault(let category):
            return "Repo V2 (Lite): write-lock acquisition failed (\(category)); aborting without writes."
        case .writerIdentityUnavailable:
            return "Repo V2 (Lite): no writer identity available for this profile; aborting without writes."
        case .versionCommitFailed:
            return "Repo V2 (Lite): version.json could not be committed; aborting without writes."
        case .leaseConfidenceLost:
            return "Repo V2 (Lite): write lease confidence lost; aborting before remote write."
        case .ownershipLost:
            return "Repo V2 (Lite): write ownership lost; aborting before manifest flush."
        }
    }
}

import Foundation

enum BackupEvent: Sendable {
    case progress(BackupProgress)
    case log(String)
    case transferState(BackupTransferState)
    case monthChanged(MonthChangeEvent)
    case remoteIndexSynced(RemoteIndexSyncEvent)
    case started(totalAssets: Int)
    case finished(BackupExecutionResult)

    var isTerminal: Bool {
        switch self {
        case .finished:
            return true
        default:
            return false
        }
    }
}

struct MonthChangeEvent: Sendable {
    let year: Int
    let month: Int
    let action: MonthAction

    enum MonthAction: Sendable {
        case started
        case checkpointSaved
        case completed
        case checkpointFailed(String)
    }
}

struct RemoteIndexSyncEvent: Sendable {
    let resourceCount: Int
    let assetCount: Int
}

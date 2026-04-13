import Foundation

enum BackupEvent: Sendable {
    case progress(BackupProgress)
    case log(String)
    case transferState(BackupTransferState)
    case monthChanged(MonthChangeEvent)
    case started(totalAssets: Int)
    case finished(BackupExecutionResult)
}

struct MonthChangeEvent: Sendable {
    let year: Int
    let month: Int
    let action: MonthAction

    enum MonthAction: Sendable {
        case started
        case completed
    }
}

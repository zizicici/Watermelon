import Foundation

enum BackupEvent: Sendable {
    case progress(BackupProgress)
    case log(String, level: ExecutionLogLevel)
    case transferState(BackupTransferState)
    case monthChanged(MonthChangeEvent)
    case started(totalAssets: Int, totalBytes: Int64?)
    case finished(BackupExecutionResult)
}

struct MonthChangeEvent: Sendable {
    let year: Int
    let month: Int
    let action: MonthAction

    enum MonthAction: Sendable {
        case started
        case completed
        // Month manifest flush never reached a durable verified commit (read-back failed): its uploaded
        // assets are unpublished. Un-marks them as resume-complete; a positive count also fails the month.
        case uploadFailed(resumableAssetLocalIdentifiers: Set<String>, failedItemCount: Int)
    }
}

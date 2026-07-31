import Foundation

struct LegacyImportTotals: Equatable, Sendable {
    var bundlesPlanned: Int = 0
    var bundlesProcessed: Int = 0
    var bundlesImported: Int = 0
    var bundlesSkippedFingerprintExists: Int = 0
    var resourcesUploaded: Int = 0
    var resourcesSkippedHashExists: Int = 0
    var bytesUploaded: Int64 = 0
    var bundlesFailed: Int = 0
    var monthsTotal: Int = 0
    var monthsDone: Int = 0
    var monthsCommitted: Int = 0
    var monthsFailed: Int = 0
}

enum LegacyImportEvent: Sendable {
    case started(totals: LegacyImportTotals)
    case monthStarted(month: LibraryMonthKey, bundleCount: Int)
    case bundleResult(month: LibraryMonthKey, bundle: LegacyAssetBundle, outcome: LegacyImportBundleOutcome)
    case monthCompleted(month: LibraryMonthKey)
    case monthFailed(month: LibraryMonthKey, reason: String)
    case logMessage(String)
    case progress(totals: LegacyImportTotals)
    case finished(totals: LegacyImportTotals)
    case cancelled(totals: LegacyImportTotals)
    case failed(error: Error, totals: LegacyImportTotals)
}

enum LegacyMigrationTerminalPolicy {
    static func event(
        for error: Error,
        totals: LegacyImportTotals
    ) -> LegacyImportEvent {
        if error is CancellationError
            || RemoteFaultLite.classify(error) == .cancelled {
            return .cancelled(totals: totals)
        }
        return .failed(error: error, totals: totals)
    }

    static func shouldRefreshRemoteSnapshot(
        after totals: LegacyImportTotals
    ) -> Bool {
        totals.bundlesImported > 0 || totals.monthsCommitted > 0
    }
}

enum LegacyImportBundleOutcome: Equatable, Sendable {
    case imported(bytesUploaded: Int64, resourcesUploaded: Int, resourcesSkippedHashExists: Int)
    case skippedFingerprintExists
    case failed(reason: String)
}

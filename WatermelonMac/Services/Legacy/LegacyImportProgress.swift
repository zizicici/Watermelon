import Foundation

struct LegacyImportTotals: Equatable {
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
}

enum LegacyImportEvent {
    case started(totals: LegacyImportTotals)
    case monthStarted(month: LibraryMonthKey, bundleCount: Int)
    case bundleResult(month: LibraryMonthKey, bundle: LegacyAssetBundle, outcome: LegacyImportBundleOutcome)
    case monthCompleted(month: LibraryMonthKey)
    case logMessage(String)
    case progress(totals: LegacyImportTotals)
    case finished(totals: LegacyImportTotals)
    case failed(error: Error, totals: LegacyImportTotals)
}

enum LegacyImportBundleOutcome: Equatable {
    case imported(bytesUploaded: Int64, resourcesUploaded: Int, resourcesSkippedHashExists: Int)
    case skippedFingerprintExists
    case failed(reason: String)
}

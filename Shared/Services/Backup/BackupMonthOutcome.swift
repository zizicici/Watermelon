import Foundation

struct DownloadIssueSummary: Equatable, Sendable {
    var skippedIncompleteCount: Int = 0
    var fingerprintMismatchCount: Int = 0
    var localFingerprintVerificationIncompleteCount: Int = 0

    var isEmpty: Bool {
        skippedIncompleteCount == 0 &&
            fingerprintMismatchCount == 0 &&
            localFingerprintVerificationIncompleteCount == 0
    }

    mutating func mergeObserved(_ other: DownloadIssueSummary) {
        skippedIncompleteCount = max(skippedIncompleteCount, other.skippedIncompleteCount)
        fingerprintMismatchCount = max(fingerprintMismatchCount, other.fingerprintMismatchCount)
        localFingerprintVerificationIncompleteCount = max(
            localFingerprintVerificationIncompleteCount,
            other.localFingerprintVerificationIncompleteCount
        )
    }
}

struct BackupMonthIncompleteSummary: Equatable, Sendable {
    var downloadIssues: DownloadIssueSummary = .init()
    var metadataSnapshotDeferredMessage: String?

    var isEmpty: Bool {
        downloadIssues.isEmpty && metadataSnapshotDeferredMessage == nil
    }

    mutating func mergeObserved(_ other: BackupMonthIncompleteSummary) {
        downloadIssues.mergeObserved(other.downloadIssues)
        if let message = other.metadataSnapshotDeferredMessage {
            metadataSnapshotDeferredMessage = message
        }
    }
}

struct DownloadMonthOutcome: Equatable, Sendable {
    var restoredCount: Int
    var issues: DownloadIssueSummary
}

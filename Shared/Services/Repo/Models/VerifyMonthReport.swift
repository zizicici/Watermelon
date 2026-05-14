import Foundation

enum VerifyMonthReportKind: String, Sendable {
    case phantomAsset
    case partiallyMissing
    case allResourcesGone
    case metadataOnlyLeft
    case fingerprintMismatch
    case verificationIncomplete
}

struct VerifyMonthReportItem: Sendable, Hashable {
    let kind: VerifyMonthReportKind
    let assetFingerprint: Data
    let detail: String?

    var allowsCleanup: Bool {
        kind == .phantomAsset || kind == .allResourcesGone || kind == .metadataOnlyLeft
    }
}

struct VerifyMonthReport: Sendable {
    let month: LibraryMonthKey
    let items: [VerifyMonthReportItem]
    var didMutateRemote: Bool = false

    var cleanupCandidates: [VerifyMonthReportItem] {
        items.filter { $0.allowsCleanup }
    }

    var reportOnly: [VerifyMonthReportItem] {
        items.filter { !$0.allowsCleanup }
    }
}

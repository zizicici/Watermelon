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

    var allowsCleanup: Bool { kind.allowsCleanup }
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

extension VerifyMonthReportKind {
    // Single source of truth: AssetIntegrityState.allowsCleanup and
    // VerifyMonthReportItem.allowsCleanup both resolve through this set so the
    // predicate cannot drift across the state ↔ report vocabularies.
    static let cleanupAllowingKinds: Set<VerifyMonthReportKind> = [
        .phantomAsset,
        .allResourcesGone,
        .metadataOnlyLeft,
    ]

    var allowsCleanup: Bool { Self.cleanupAllowingKinds.contains(self) }

    init?(from state: AssetIntegrityState) {
        switch state {
        case .healthy:
            return nil
        case .phantom:
            self = .phantomAsset
        case .fullyMissing:
            self = .allResourcesGone
        case .metadataOnlyLeft:
            self = .metadataOnlyLeft
        case .fingerprintMismatch:
            self = .fingerprintMismatch
        case .partiallyMissing:
            self = .partiallyMissing
        }
    }
}

extension VerifyMonthReportItem {
    static func from(
        state: AssetIntegrityState,
        fingerprint: Data,
        linkCount: Int
    ) -> VerifyMonthReportItem? {
        guard let kind = VerifyMonthReportKind(from: state) else { return nil }
        let detail: String
        switch state {
        case .healthy:
            return nil
        case .phantom:
            detail = "no asset_resources rows; fingerprint=\(fingerprint.hexString)"
        case .fullyMissing:
            detail = "all \(linkCount) resources missing on remote"
        case .metadataOnlyLeft:
            detail = "only adjustment-data roles remain"
        case .fingerprintMismatch:
            detail = "stored fp does not match recomputed from \(linkCount) link(s)"
        case .partiallyMissing(let missing):
            detail = "\(missing.count)/\(linkCount) resources missing"
        }
        return VerifyMonthReportItem(
            kind: kind,
            assetFingerprint: fingerprint,
            detail: detail
        )
    }
}

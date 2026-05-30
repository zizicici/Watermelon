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
    let assetFingerprint: AssetFingerprint
    let detail: String?

    var allowsCleanup: Bool { kind.allowsCleanup }
}

/// Verdict that distinguishes genuine unrepaired damage from routine budget-incomplete
/// verification, so callers can withhold a "verified OK" timestamp only when damage was found.
enum MonthVerifyOutcome: Equatable, Sendable {
    case clean
    case mutated
    case damaged(kinds: Set<VerifyMonthReportKind>)

    // Report-only kinds that represent genuine unrepaired damage. verificationIncomplete is
    // routine content-trust budget exhaustion and is deliberately excluded.
    static let damageKinds: Set<VerifyMonthReportKind> = [.partiallyMissing, .fingerprintMismatch]

    var isDamaged: Bool {
        if case .damaged = self { return true }
        return false
    }

    /// Aggregation: damage dominates (kinds merge), mutated outranks clean, clean is identity.
    func combined(with other: MonthVerifyOutcome) -> MonthVerifyOutcome {
        switch (self, other) {
        case let (.damaged(a), .damaged(b)):
            return .damaged(kinds: a.union(b))
        case (.damaged, _):
            return self
        case (_, .damaged):
            return other
        case (.mutated, _), (_, .mutated):
            return .mutated
        default:
            return .clean
        }
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

    var outcome: MonthVerifyOutcome {
        let damage = Set(items.map(\.kind)).intersection(MonthVerifyOutcome.damageKinds)
        if !damage.isEmpty { return .damaged(kinds: damage) }
        return didMutateRemote ? .mutated : .clean
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

    /// Tombstone reason for cleanup-eligible kinds; nil for kinds that `allowsCleanup` filters out.
    /// Always nil ⟺ `allowsCleanup` is false (pinned by adapter parity test).
    var tombstoneReason: CommitTombstoneBody.Reason? {
        switch self {
        case .phantomAsset, .metadataOnlyLeft:
            return .manifestOrphan
        case .allResourcesGone:
            return .verifyFailed
        case .partiallyMissing, .fingerprintMismatch, .verificationIncomplete:
            return nil
        }
    }
}

extension VerifyMonthReportItem {
    static func from(
        state: AssetIntegrityState,
        fingerprint: AssetFingerprint,
        linkCount: Int
    ) -> VerifyMonthReportItem? {
        guard let kind = VerifyMonthReportKind(from: state) else { return nil }
        let detail: String
        switch state {
        case .healthy:
            return nil
        case .phantom:
            detail = "no asset_resources rows; fingerprint=\(fingerprint)"
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

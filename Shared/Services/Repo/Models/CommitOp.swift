import Foundation

struct CommitHeader: Equatable, Sendable {
    static let currentVersion = 1
    static let bodyKindPlain = "plain"
    let version: Int
    let repoID: String
    let writerID: String
    let seq: UInt64
    let runID: String
    let scope: String
    let clockMin: UInt64
    let clockMax: UInt64
    let bodyKind: String

    static func monthScope(_ month: LibraryMonthKey) -> String {
        "month:\(month.text)"
    }

    static func parseMonthScope(_ scope: String) -> LibraryMonthKey? {
        guard scope.hasPrefix("month:") else { return nil }
        let text = String(scope.dropFirst("month:".count))
        let parts = text.split(separator: "-")
        guard parts.count == 2,
              let y = Int(parts[0]),
              let m = Int(parts[1]),
              (1...12).contains(m) else { return nil }
        return LibraryMonthKey(year: y, month: m)
    }
}

struct CommitResourceEntry: Equatable, Sendable {
    let physicalRemotePath: String
    let logicalName: String
    let contentHash: Data
    let fileSize: Int64
    let resourceType: Int
    let role: Int
    let slot: Int
    let crypto: ResourceCryptoMetadata?
}

struct CommitAddAssetBody: Equatable, Sendable {
    let assetFingerprint: Data
    let creationDateMs: Int64?
    let backedUpAtMs: Int64
    let resources: [CommitResourceEntry]
}

/// Prevents observation tombstones from deleting content healed after the scan.
struct TombstoneObservationBasis: Equatable, Sendable {
    let perWriterMaxSeq: [String: UInt64]
    let lamportWatermark: UInt64
}

/// Baseline-load seeds LWW stamps so stale replay cannot overwrite newer baked-in rows.
struct OpStamp: Hashable, Sendable {
    let writerID: String
    let seq: UInt64
    let clock: UInt64
}

/// Matches the cross-commit tiebreak; opSeq is intra-commit only.
func opStampPrecedes(_ a: OpStamp, _ b: OpStamp) -> Bool {
    if a.clock != b.clock { return a.clock < b.clock }
    if a.writerID != b.writerID { return a.writerID < b.writerID }
    return a.seq < b.seq
}

struct CommitTombstoneBody: Equatable, Sendable {
    enum Reason: String, Sendable {
        case userDeleted
        case verifyFailed
        case manifestOrphan
    }
    let assetFingerprint: Data
    let reason: Reason
    let observedBasis: TombstoneObservationBasis

    init(assetFingerprint: Data, reason: Reason, observedBasis: TombstoneObservationBasis) {
        self.assetFingerprint = assetFingerprint
        self.reason = reason
        self.observedBasis = observedBasis
    }
}

enum CommitOpKind: String, Sendable {
    case addAsset
    case tombstoneAsset
}

struct CommitOp: Equatable, Sendable {
    let opSeq: Int
    let clock: UInt64
    let body: CommitOpBody
}

enum CommitOpBody: Equatable, Sendable {
    case addAsset(CommitAddAssetBody)
    case tombstoneAsset(CommitTombstoneBody)

    var kind: CommitOpKind {
        switch self {
        case .addAsset: return .addAsset
        case .tombstoneAsset: return .tombstoneAsset
        }
    }
}

struct CommitFile: Equatable, Sendable {
    let header: CommitHeader
    let ops: [CommitOp]
    let sha256Hex: String
    let rowCount: Int
}

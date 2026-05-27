import Foundation

struct BackupMonthFlushDelta: Sendable {
    let didFlush: Bool
    let committedAssetFingerprints: Set<AssetFingerprint>
    let committedTombstoneFingerprints: Set<AssetFingerprint>

    static let none = BackupMonthFlushDelta(
        didFlush: false,
        committedAssetFingerprints: [],
        committedTombstoneFingerprints: []
    )
}

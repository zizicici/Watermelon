import Foundation

struct BackupMonthFlushDelta: Sendable {
    let didFlush: Bool
    let committedAssetFingerprints: Set<Data>
    let committedTombstoneFingerprints: Set<Data>

    static let none = BackupMonthFlushDelta(
        didFlush: false,
        committedAssetFingerprints: [],
        committedTombstoneFingerprints: []
    )
}

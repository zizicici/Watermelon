import Foundation

enum BackupV2Constants {
    static let batchFlushInterval = 200
    // A2 aggressive checkpoint thresholds: smaller baselines fold sooner so the commit-delete path opens
    // earlier and replay fan-out stays bounded.
    static let checkpointCommitThreshold = 1_000
    static let checkpointByteThreshold: Int64 = 4 * 1024 * 1024
    // Pre-A2 conservative thresholds, retained for `RepoCompactionPolicy.conservative`.
    static let conservativeCheckpointCommitThreshold = 5_000
    static let conservativeCheckpointByteThreshold: Int64 = 16 * 1024 * 1024
    static let snapshotFallbackKeepCount = 2
    static let snapshotGCMarginFileCount = 2
}

import Foundation

enum BackupV2Constants {
    static let batchFlushInterval = 10
    static let checkpointCommitThreshold = 5_000
    static let checkpointByteThreshold: Int64 = 16 * 1024 * 1024
    static let minimumCheckpointIntervalSeconds = 6 * 60 * 60
    static let retentionStalenessThresholdSeconds = 24 * 60 * 60
    static let unknownRetentionCapabilityGraceSeconds = 7 * 24 * 60 * 60
    static let snapshotFallbackKeepCount = 2
}

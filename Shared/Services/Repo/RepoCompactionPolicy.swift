import Foundation

struct RepoCompactionPolicy: Equatable, Sendable {
    var checkpointCommitThreshold: Int
    var checkpointByteThreshold: Int64
    var minimumCheckpointIntervalSeconds: Int
    var retentionStalenessThresholdSeconds: Int
    var legacyClientGraceSeconds: Int
    var snapshotFallbackKeepCount: Int

    static var `default`: RepoCompactionPolicy {
        RepoCompactionPolicy(
            checkpointCommitThreshold: BackupV2Constants.checkpointCommitThreshold,
            checkpointByteThreshold: BackupV2Constants.checkpointByteThreshold,
            minimumCheckpointIntervalSeconds: BackupV2Constants.minimumCheckpointIntervalSeconds,
            retentionStalenessThresholdSeconds: BackupV2Constants.retentionStalenessThresholdSeconds,
            legacyClientGraceSeconds: BackupV2Constants.legacyClientGraceSeconds,
            snapshotFallbackKeepCount: BackupV2Constants.snapshotFallbackKeepCount
        )
    }

    func conservativeDeletePrefixByWriter(covered: CoveredRanges) -> [String: UInt64] {
        covered.conservativeContiguousPrefixByWriter()
    }
}

extension CoveredRanges {
    // Absence represents no seq-1 prefix; zero is never a deletion watermark.
    func conservativeContiguousPrefixByWriter() -> [String: UInt64] {
        var result: [String: UInt64] = [:]
        for (writerID, ranges) in rangesByWriter {
            guard let first = ranges.first, first.low == 1 else { continue }
            result[writerID] = first.high
        }
        return result
    }
}

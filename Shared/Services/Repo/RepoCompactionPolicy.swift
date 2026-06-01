import Foundation

struct RepoCompactionPolicy: Equatable, Sendable {
    var checkpointCommitThreshold: Int
    var checkpointByteThreshold: Int64
    var snapshotFallbackKeepCount: Int
    var snapshotGCMarginFileCount: Int

    static var `default`: RepoCompactionPolicy {
        RepoCompactionPolicy(
            checkpointCommitThreshold: BackupV2Constants.checkpointCommitThreshold,
            checkpointByteThreshold: BackupV2Constants.checkpointByteThreshold,
            snapshotFallbackKeepCount: BackupV2Constants.snapshotFallbackKeepCount,
            snapshotGCMarginFileCount: BackupV2Constants.snapshotGCMarginFileCount
        )
    }

    func conservativeDeletePrefixByWriter(covered: CoveredRanges) -> [String: UInt64] {
        covered.conservativeContiguousPrefixByWriter()
    }

    // Hysteresis margin so light months never churn; only months strictly past keepN+margin run GC.
    var snapshotGCTriggerFileCount: Int {
        max(0, snapshotFallbackKeepCount) + max(0, snapshotGCMarginFileCount)
    }

    func shouldRunSnapshotGC(snapshotFileCount: Int) -> Bool {
        snapshotFileCount > snapshotGCTriggerFileCount
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

import Foundation

enum RetentionInvariantEvaluator {
    enum PostDeleteOutcome: Sendable, Equatable {
        case passed
        case failed(reason: RepoRetentionPostDeleteVerificationFailure)
    }

    static func evaluatePostDeleteContract(
        evidence: RepoRetentionPostDeleteVerificationEvidence,
        afterState: RepoSnapshotState,
        month: LibraryMonthKey,
        contract: RepoRetentionPostDeleteEquivalenceContract
    ) -> PostDeleteOutcome {
        guard evidence.acceptedSnapshot.covered.superset(of: contract.acceptedSnapshotCovered) else {
            return .failed(reason: .acceptedSnapshotCoverageRegression(filename: evidence.acceptedSnapshot.filename))
        }
        guard evidence.acceptedSnapshot.covered.superset(of: contract.retainedBarrierUnionCovered) else {
            return .failed(reason: .retainedBarrierCoverageRegression(filename: evidence.acceptedSnapshot.filename))
        }
        guard evidence.acceptedSnapshot.covered.superset(of: coveredRangesFrom(prefixes: contract.expectedDeletePrefixByWriter)) else {
            return .failed(reason: .deletePrefixCoverageRegression(filename: evidence.acceptedSnapshot.filename))
        }
        guard stateIsRetentionSuperset(before: contract.preDeleteState, after: afterState, month: month) else {
            return .failed(reason: .stateNotRetentionSuperset)
        }
        guard evidence.materializedCovered.superset(of: contract.preDeleteCovered) else {
            return .failed(reason: .coveredRangeRegression)
        }
        for writerID in contract.requiredObservedSeqByWriter.keys.sorted() {
            let expected = contract.requiredObservedSeqByWriter[writerID] ?? 0
            let observed = evidence.observedSeqByWriter[writerID] ?? 0
            guard observed >= expected else {
                return .failed(reason: .observedSeqRegression(
                    writerID: writerID,
                    expectedAtLeast: expected,
                    observed: observed
                ))
            }
        }
        guard evidence.observedClock >= contract.preDeleteState.observedClock else {
            return .failed(reason: .observedClockRegression(
                expectedAtLeast: contract.preDeleteState.observedClock,
                observed: evidence.observedClock
            ))
        }
        return .passed
    }

    static func coveredRangesFrom(prefixes: [String: UInt64]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: prefixes.compactMapValues { prefix in
            prefix > 0 ? [ClosedSeqRange(low: 1, high: prefix)] : nil
        })
    }

    static func stateIsRetentionSuperset(
        before: RepoSnapshotState,
        after: RepoSnapshotState,
        month: LibraryMonthKey
    ) -> Bool {
        guard let beforeMonth = before.months[month] else { return true }
        guard let afterMonth = after.months[month] else { return false }
        return monthStateIsSuperset(before: beforeMonth, after: afterMonth)
    }

    static func monthStateIsSuperset(before: RepoMonthState, after: RepoMonthState) -> Bool {
        dictionaryIsSuperset(before: before.assets, after: after.assets) &&
        dictionaryIsSuperset(before: before.resources, after: after.resources) &&
        dictionaryIsSuperset(before: before.assetResources, after: after.assetResources) &&
        dictionaryIsSuperset(before: before.deletedAssetStamps, after: after.deletedAssetStamps)
    }

    static func dictionaryIsSuperset<K: Hashable, V: Equatable>(before: [K: V], after: [K: V]) -> Bool {
        for (key, value) in before where after[key] != value {
            return false
        }
        return true
    }
}

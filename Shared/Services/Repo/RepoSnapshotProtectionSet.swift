import Foundation

/// Deterministic, pure rules deciding which snapshot files MUST NOT be deleted.
/// Inputs are explicit so the caller (preflight service) is responsible for sourcing
/// them; outputs are testable without storage mocks.
enum RepoSnapshotProtectionSet {
    struct Input: Equatable, Sendable {
        let acceptedBaselineFilename: String
        let acceptedBaselineCovered: CoveredRanges
        let barrierReferencedFilenames: Set<String>
        let parseableSnapshotsForMonth: [Parseable]
        let snapshotKeepCount: Int

        struct Parseable: Equatable, Sendable {
            let filename: String
            let lamport: UInt64
            let writerID: String
            let covered: CoveredRanges
        }
    }

    struct Output: Equatable, Sendable {
        let protectedFilenames: Set<String>
        let deleteCandidateFilenames: [String]
    }

    /// `protected` always contains the accepted baseline and every barrier-referenced
    /// snapshot. Then the N most-recent parseable snapshots by `(lamport desc, filename asc)`
    /// are added — this count includes the accepted baseline itself, so keepN=1 protects only
    /// the baseline, keepN=2 protects baseline + one more, etc. Covered-dominance (not lamport)
    /// determines deletion eligibility: a snapshot whose covered is a superset of the accepted
    /// baseline's covered is never a candidate, even if its lamport is lower.
    static func compute(_ input: Input) -> Output {
        var protected: Set<String> = []
        protected.insert(input.acceptedBaselineFilename)
        protected.formUnion(input.barrierReferencedFilenames)

        let keepN = max(0, input.snapshotKeepCount)
        if keepN > 0 {
            let sorted = input.parseableSnapshotsForMonth.sorted { lhs, rhs in
                if lhs.lamport != rhs.lamport { return lhs.lamport > rhs.lamport }
                return lhs.filename < rhs.filename
            }
            for entry in sorted.prefix(keepN) {
                protected.insert(entry.filename)
            }
        }

        let candidates = input.parseableSnapshotsForMonth
            .filter { input.acceptedBaselineCovered.superset(of: $0.covered) }
            .filter { !protected.contains($0.filename) }
            .sorted { lhs, rhs in
                if lhs.lamport != rhs.lamport { return lhs.lamport < rhs.lamport }
                return lhs.filename < rhs.filename
            }
            .map(\.filename)

        return Output(protectedFilenames: protected, deleteCandidateFilenames: candidates)
    }
}

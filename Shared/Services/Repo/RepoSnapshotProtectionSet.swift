import Foundation

enum RepoSnapshotProtectionSet {
    struct Input: Equatable, Sendable {
        let acceptedBaselineFilename: String
        let acceptedBaselineCovered: CoveredRanges
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

    static func compute(_ input: Input) -> Output {
        var protected: Set<String> = []
        protected.insert(input.acceptedBaselineFilename)

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
            // Strict domination only: equal coverage is not dominated and must be retained.
            .filter {
                input.acceptedBaselineCovered.superset(of: $0.covered)
                    && input.acceptedBaselineCovered != $0.covered
            }
            .filter { !protected.contains($0.filename) }
            .sorted { lhs, rhs in
                if lhs.lamport != rhs.lamport { return lhs.lamport < rhs.lamport }
                return lhs.filename < rhs.filename
            }
            .map(\.filename)

        return Output(protectedFilenames: protected, deleteCandidateFilenames: candidates)
    }
}

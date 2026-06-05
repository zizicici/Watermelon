import Foundation

/// Shared internal representation for the two V2 metadata-delete families (commit-prefix GC and
/// snapshot GC). Eligibility, revalidation, and post-delete verification stay family-specific; this
/// only unifies the candidate/summary/failure plumbing both families already shared.
enum RepoMetadataDeleteKind: Equatable, Sendable {
    case commit(seq: UInt64)
    case snapshot(lamport: UInt64, runIDPrefix: String)
}

struct RepoMetadataDeleteCandidate: Equatable, Sendable {
    let kind: RepoMetadataDeleteKind
    let filename: String
    let path: String
    let month: LibraryMonthKey
    let writerID: String
    let size: Int64
    let sha256Hex: String
    let rowCount: Int
}

extension RepoMetadataDeleteCandidate {
    var commitSeq: UInt64? {
        if case .commit(let seq) = kind { return seq }
        return nil
    }
    var snapshotLamport: UInt64? {
        if case .snapshot(let lamport, _) = kind { return lamport }
        return nil
    }
    var snapshotRunIDPrefix: String? {
        if case .snapshot(_, let runIDPrefix) = kind { return runIDPrefix }
        return nil
    }
}

struct RepoMetadataDeleteSummary: Equatable, Sendable {
    let month: LibraryMonthKey
    let repoID: String
    let candidateCount: Int
    var attempted: [RepoMetadataDeleteCandidate] = []
    var deleted: [RepoMetadataDeleteCandidate] = []
    var alreadyMissing: [RepoMetadataDeleteCandidate] = []

    var attemptedCount: Int { attempted.count }
    var deletedCount: Int { deleted.count }
    var alreadyMissingCount: Int { alreadyMissing.count }
}

enum RepoMetadataDeleteFailure: Equatable, Sendable {
    case cancelled
    case other(String)
}

import Foundation

enum RepoCrossRepoIndexSchema {
    static let currentVersion: Int = 1
}

struct RepoCrossRepoIndexHeader: Equatable, Sendable {
    let schemaVersion: Int
    let repoID: String
    let writerID: String
    let lamport: UInt64
    let runIDPrefix: String
    let observedClock: UInt64
    let coveredByMonth: [LibraryMonthKey: CoveredRanges]
}

struct RepoCrossRepoIndexAcceptedSnapshotInfo: Equatable, Sendable {
    let filename: String
    let lamport: UInt64
    let writerID: String
    let runIDPrefix: String
    let covered: CoveredRanges
}

struct RepoCrossRepoIndexTail: Equatable, Sendable {
    let observedSeqByWriter: [String: UInt64]
    let acceptedSnapshotBaselinesByMonthAtIndexTime: [LibraryMonthKey: RepoCrossRepoIndexAcceptedSnapshotInfo]
    let corruptedSnapshotMonthsAtIndexTime: Set<LibraryMonthKey>
}

struct RepoCrossRepoIndexMonthSection: Equatable, Sendable {
    let month: LibraryMonthKey
    let assets: [SnapshotAssetRow]
    let resources: [SnapshotResourceRow]
    let assetResources: [SnapshotAssetResourceRow]
    let deletedKeys: [SnapshotDeletedKeyRow]
}

enum RepoCrossRepoIndexRow: Equatable, Sendable {
    case header(RepoCrossRepoIndexHeader)
    case monthBegin(LibraryMonthKey)
    case monthEnd(LibraryMonthKey)
    case asset(SnapshotAssetRow)
    case resource(SnapshotResourceRow)
    case assetResource(SnapshotAssetResourceRow)
    case deletedKey(SnapshotDeletedKeyRow)
    case tail(RepoCrossRepoIndexTail)
    case end(sha256Hex: String, rowCount: Int)
}

struct RepoCrossRepoIndexFile: Equatable, Sendable {
    let header: RepoCrossRepoIndexHeader
    let monthSections: [RepoCrossRepoIndexMonthSection]
    let tail: RepoCrossRepoIndexTail
    let sha256Hex: String
    let rowCount: Int
}

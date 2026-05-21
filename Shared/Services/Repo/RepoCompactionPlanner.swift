import Foundation

struct RepoCompactionPlanner: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    let policy: RepoCompactionPolicy

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        policy: RepoCompactionPolicy = .default
    ) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
        self.policy = policy
    }

    func makeReport(
        expectedRepoID: String?,
        preMaterialized: RepoMaterializer.MaterializeOutput? = nil
    ) async throws -> RepoCompactionReport {
        async let commitEntries = listMetadataEntries(path: RepoLayout.commitsDirectoryPath(base: basePath))
        async let snapshotEntries = listMetadataEntries(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        let materialized: RepoMaterializer.MaterializeOutput
        if let preMaterialized {
            materialized = preMaterialized
        } else {
            materialized = try await RepoMaterializer(client: client, basePath: basePath)
                .materialize(expectedRepoID: expectedRepoID)
        }

        let commits = try await commitEntries
        let snapshots = try await snapshotEntries
        return makeReport(materialized: materialized, commits: commits, snapshots: snapshots)
    }

    private func listMetadataEntries(path: String) async throws -> [RemoteStorageEntry] {
        do {
            return try await client.list(path: path).filter { !$0.isDirectory && $0.name.hasSuffix(".jsonl") }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            do {
                let metadata = try await client.metadata(path: path)
                if metadata == nil { return [] }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            }
            throw error
        }
    }

    private func makeReport(
        materialized: RepoMaterializer.MaterializeOutput,
        commits: [RemoteStorageEntry],
        snapshots: [RemoteStorageEntry]
    ) -> RepoCompactionReport {
        let commitStats = CommitMetadataStats(entries: commits)
        let snapshotStats = SnapshotMetadataStats(entries: snapshots)
        var months = Set<LibraryMonthKey>()
        months.formUnion(commitStats.byMonth.keys)
        months.formUnion(commitStats.fileCountByMonth.keys)
        months.formUnion(snapshotStats.byMonth.keys)
        months.formUnion(snapshotStats.fileCountByMonth.keys)
        months.formUnion(materialized.coveredByMonth.keys)
        months.formUnion(materialized.acceptedSnapshotBaselinesByMonth.keys)

        let monthReports = months.sorted().map { month in
            makeMonthReport(
                month: month,
                commitStats: commitStats,
                snapshotStats: snapshotStats,
                materialized: materialized
            )
        }
        return RepoCompactionReport(months: monthReports, totals: RepoCompactionTotals(months: monthReports))
    }

    private func makeMonthReport(
        month: LibraryMonthKey,
        commitStats: CommitMetadataStats,
        snapshotStats: SnapshotMetadataStats,
        materialized: RepoMaterializer.MaterializeOutput
    ) -> RepoCompactionMonthReport {
        let commits = commitStats.byMonth[month] ?? []
        let acceptedSnapshot = materialized.acceptedSnapshotBaselinesByMonth[month]
        let acceptedSnapshotCovered = acceptedSnapshot?.covered ?? .empty
        let finalCovered = materialized.coveredByMonth[month] ?? .empty
        let deletePrefixByWriter = acceptedSnapshot == nil
            ? [:]
            : policy.conservativeDeletePrefixByWriter(covered: acceptedSnapshotCovered)

        var replayedSinceCheckpointCommitCount = 0
        var replayedSinceCheckpointBytes: Int64 = 0
        var checkpointCoveredPrefixCandidateCount = 0
        var checkpointCoveredPrefixCandidateBytes: Int64 = 0
        var checkpointCoveredButOutsidePrefixCount = 0
        var notCheckpointCoveredCommitCount = 0

        for commit in commits {
            let checkpointCovered = acceptedSnapshotCovered.contains(writerID: commit.writerID, seq: commit.seq)
            if finalCovered.contains(writerID: commit.writerID, seq: commit.seq), !checkpointCovered {
                replayedSinceCheckpointCommitCount += 1
                replayedSinceCheckpointBytes += commit.size
            }
            if let prefix = deletePrefixByWriter[commit.writerID], commit.seq <= prefix {
                checkpointCoveredPrefixCandidateCount += 1
                checkpointCoveredPrefixCandidateBytes += commit.size
            } else if checkpointCovered {
                checkpointCoveredButOutsidePrefixCount += 1
            }
            if !checkpointCovered {
                notCheckpointCoveredCommitCount += 1
            }
        }

        return RepoCompactionMonthReport(
            month: month,
            commitFileCount: commitStats.fileCountByMonth[month] ?? 0,
            parseableCommitFileCount: commits.count,
            unparseableCommitFileCount: commitStats.unparseableCountByMonth[month] ?? 0,
            commitBytes: commitStats.bytesByMonth[month] ?? 0,
            snapshotFileCount: snapshotStats.fileCountByMonth[month] ?? 0,
            parseableSnapshotFileCount: snapshotStats.parseableCountByMonth[month] ?? 0,
            unparseableSnapshotFileCount: snapshotStats.unparseableCountByMonth[month] ?? 0,
            acceptedSnapshot: acceptedSnapshot,
            acceptedSnapshotCovered: acceptedSnapshotCovered,
            finalCovered: finalCovered,
            replayedSinceCheckpointCommitCount: replayedSinceCheckpointCommitCount,
            replayedSinceCheckpointBytes: replayedSinceCheckpointBytes,
            checkpointRecommended: replayedSinceCheckpointCommitCount >= policy.checkpointCommitThreshold
                || replayedSinceCheckpointBytes >= policy.checkpointByteThreshold,
            deletePrefixByWriter: deletePrefixByWriter,
            checkpointCoveredPrefixCandidateCount: checkpointCoveredPrefixCandidateCount,
            checkpointCoveredPrefixCandidateBytes: checkpointCoveredPrefixCandidateBytes,
            checkpointCoveredButOutsidePrefixCount: checkpointCoveredButOutsidePrefixCount,
            notCheckpointCoveredCommitCount: notCheckpointCoveredCommitCount,
            protectedUnparseableFilenameCount: commitStats.unparseableCountByMonth[month] ?? 0
        )
    }
}

struct RepoCompactionReport: Equatable, Sendable {
    let months: [RepoCompactionMonthReport]
    let totals: RepoCompactionTotals
}

struct RepoCompactionMonthReport: Equatable, Sendable {
    let month: LibraryMonthKey
    let commitFileCount: Int
    let parseableCommitFileCount: Int
    let unparseableCommitFileCount: Int
    let commitBytes: Int64
    let snapshotFileCount: Int
    let parseableSnapshotFileCount: Int
    let unparseableSnapshotFileCount: Int
    let acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo?
    let acceptedSnapshotCovered: CoveredRanges
    let finalCovered: CoveredRanges
    let replayedSinceCheckpointCommitCount: Int
    let replayedSinceCheckpointBytes: Int64
    let checkpointRecommended: Bool
    let deletePrefixByWriter: [String: UInt64]
    let checkpointCoveredPrefixCandidateCount: Int
    let checkpointCoveredPrefixCandidateBytes: Int64
    let checkpointCoveredButOutsidePrefixCount: Int
    let notCheckpointCoveredCommitCount: Int
    let protectedUnparseableFilenameCount: Int
}

struct RepoCompactionTotals: Equatable, Sendable {
    let commitFileCount: Int
    let parseableCommitFileCount: Int
    let unparseableCommitFileCount: Int
    let commitBytes: Int64
    let snapshotFileCount: Int
    let parseableSnapshotFileCount: Int
    let unparseableSnapshotFileCount: Int
    let replayedSinceCheckpointCommitCount: Int
    let replayedSinceCheckpointBytes: Int64
    let checkpointRecommendedMonthCount: Int
    let checkpointCoveredPrefixCandidateCount: Int
    let checkpointCoveredPrefixCandidateBytes: Int64
    let checkpointCoveredButOutsidePrefixCount: Int
    let notCheckpointCoveredCommitCount: Int
    let protectedUnparseableFilenameCount: Int
    let identityScanUpperBoundFileCount: Int

    init(months: [RepoCompactionMonthReport]) {
        commitFileCount = months.reduce(0) { $0 + $1.commitFileCount }
        parseableCommitFileCount = months.reduce(0) { $0 + $1.parseableCommitFileCount }
        unparseableCommitFileCount = months.reduce(0) { $0 + $1.unparseableCommitFileCount }
        commitBytes = months.reduce(0) { $0 + $1.commitBytes }
        snapshotFileCount = months.reduce(0) { $0 + $1.snapshotFileCount }
        parseableSnapshotFileCount = months.reduce(0) { $0 + $1.parseableSnapshotFileCount }
        unparseableSnapshotFileCount = months.reduce(0) { $0 + $1.unparseableSnapshotFileCount }
        replayedSinceCheckpointCommitCount = months.reduce(0) { $0 + $1.replayedSinceCheckpointCommitCount }
        replayedSinceCheckpointBytes = months.reduce(0) { $0 + $1.replayedSinceCheckpointBytes }
        checkpointRecommendedMonthCount = months.filter(\.checkpointRecommended).count
        checkpointCoveredPrefixCandidateCount = months.reduce(0) { $0 + $1.checkpointCoveredPrefixCandidateCount }
        checkpointCoveredPrefixCandidateBytes = months.reduce(0) { $0 + $1.checkpointCoveredPrefixCandidateBytes }
        checkpointCoveredButOutsidePrefixCount = months.reduce(0) { $0 + $1.checkpointCoveredButOutsidePrefixCount }
        notCheckpointCoveredCommitCount = months.reduce(0) { $0 + $1.notCheckpointCoveredCommitCount }
        protectedUnparseableFilenameCount = months.reduce(0) { $0 + $1.protectedUnparseableFilenameCount }
        identityScanUpperBoundFileCount = parseableCommitFileCount + parseableSnapshotFileCount
    }
}

private struct ParsedCommitMetadata: Equatable {
    let writerID: String
    let seq: UInt64
    let size: Int64
}

private struct CommitMetadataStats {
    var byMonth: [LibraryMonthKey: [ParsedCommitMetadata]] = [:]
    var fileCountByMonth: [LibraryMonthKey: Int] = [:]
    var unparseableCountByMonth: [LibraryMonthKey: Int] = [:]
    var bytesByMonth: [LibraryMonthKey: Int64] = [:]

    init(entries: [RemoteStorageEntry]) {
        for entry in entries {
            if let parsed = RepoLayout.parseCommitFilename(entry.name) {
                fileCountByMonth[parsed.month, default: 0] += 1
                bytesByMonth[parsed.month, default: 0] += entry.size
                byMonth[parsed.month, default: []].append(ParsedCommitMetadata(
                    writerID: parsed.writerID,
                    seq: parsed.seq,
                    size: entry.size
                ))
            } else if let month = monthPrefix(from: entry.name) {
                fileCountByMonth[month, default: 0] += 1
                unparseableCountByMonth[month, default: 0] += 1
                bytesByMonth[month, default: 0] += entry.size
            }
        }
    }
}

private struct SnapshotMetadataStats {
    var fileCountByMonth: [LibraryMonthKey: Int] = [:]
    var parseableCountByMonth: [LibraryMonthKey: Int] = [:]
    var unparseableCountByMonth: [LibraryMonthKey: Int] = [:]
    var byMonth: [LibraryMonthKey: Bool] = [:]

    init(entries: [RemoteStorageEntry]) {
        for entry in entries {
            if let parsed = RepoLayout.parseSnapshotFilename(entry.name) {
                byMonth[parsed.month] = true
                fileCountByMonth[parsed.month, default: 0] += 1
                parseableCountByMonth[parsed.month, default: 0] += 1
            } else if let month = monthPrefix(from: entry.name) {
                fileCountByMonth[month, default: 0] += 1
                unparseableCountByMonth[month, default: 0] += 1
            }
        }
    }
}

private func monthPrefix(from filename: String) -> LibraryMonthKey? {
    guard filename.count >= 7 else { return nil }
    let prefix = String(filename.prefix(7))
    let parts = prefix.split(separator: "-")
    guard parts.count == 2,
          let year = Int(parts[0]),
          let month = Int(parts[1]),
          (1...12).contains(month) else {
        return nil
    }
    return LibraryMonthKey(year: year, month: month)
}

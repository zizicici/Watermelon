import XCTest
@testable import Watermelon

final class RepoCompactionPlannerTests: XCTestCase {
    func testMaterializerExposesAcceptedSnapshotBaselineSeparatelyFromFinalCoverage() async throws {
        let client = try await makeClient()
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        _ = try await commitWriter.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerA,
                seq: 6,
                runID: runID,
                month: month
            ),
            ops: [],
            month: month,
            respectTaskCancellation: false
        )
        let noSnapshot = try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: repoID)
        XCTAssertEqual(noSnapshot.acceptedSnapshotBaselinesByMonth, [:])
        XCTAssertTrue(noSnapshot.coveredByMonth[month, default: .empty].contains(writerID: writerA, seq: 6))

        let covered = coveredRanges(writerA: [(1, 5)])
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: covered
            ),
            assets: [],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: runID,
            respectTaskCancellation: false
        )

        let output = try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: repoID)
        let accepted = try XCTUnwrap(output.acceptedSnapshotBaselinesByMonth[month])
        XCTAssertEqual(accepted.filename, RepoLayout.snapshotFileName(month: month, lamport: 10, writerID: writerA, runID: runID))
        XCTAssertEqual(accepted.month, month)
        XCTAssertEqual(accepted.lamport, 10)
        XCTAssertEqual(accepted.writerID, writerA)
        XCTAssertEqual(accepted.runIDPrefix, RepoLayout.runIDPrefix(runID))
        XCTAssertEqual(accepted.covered, covered)
        XCTAssertEqual(output.state, RepoSnapshotState(months: [month: .empty], observedClock: 10))
        XCTAssertEqual(output.observedSeqByWriter, [writerA: 6])
        XCTAssertEqual(output.coveredByMonth[month], coveredRanges(writerA: [(1, 6)]))
        XCTAssertEqual(output.corruptedSnapshotMonths, [])
        XCTAssertEqual(output.repoID, repoID)
    }

    func testNoSnapshotNeverProducesPrefixCandidatesEvenWhenFinalFoldCovered() async throws {
        let client = try await makeClient()
        await injectCommitFiles(client: client, writerID: writerA, seqs: 1...5_000)
        let output = materializedOutput(
            finalCovered: coveredRanges(writerA: [(1, 5_000)]),
            acceptedSnapshot: nil
        )

        let report = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: output)
        let monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.commitFileCount, 5_000)
        XCTAssertEqual(monthReport.parseableCommitFileCount, 5_000)
        XCTAssertEqual(monthReport.replayedSinceCheckpointCommitCount, 5_000)
        XCTAssertTrue(monthReport.checkpointRecommended)
        XCTAssertEqual(monthReport.deletePrefixByWriter, [:])
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateCount, 0)
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateBytes, 0)
        XCTAssertEqual(monthReport.notCheckpointCoveredCommitCount, 5_000)
    }

    func testExactFiveThousandFanoutAfterCheckpointAndAppend() async throws {
        let policy = RepoCompactionPolicy(
            checkpointCommitThreshold: 5_000,
            checkpointByteThreshold: 500,
            minimumCheckpointIntervalSeconds: 0,
            retentionStalenessThresholdSeconds: 86_400,
            snapshotFallbackKeepCount: 2
        )
        let client = try await makeClient()
        await injectCommitFiles(client: client, writerID: writerA, seqs: 1...5_000)
        await injectSnapshotMarker(client: client, lamport: 10, writerID: writerA)
        let checkpointed = materializedOutput(
            finalCovered: coveredRanges(writerA: [(1, 5_000)]),
            acceptedSnapshot: acceptedSnapshotInfo(covered: coveredRanges(writerA: [(1, 5_000)]))
        )

        var report = try await RepoCompactionPlanner(client: client, basePath: basePath, policy: policy)
            .makeReport(expectedRepoID: repoID, preMaterialized: checkpointed)
        var monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateCount, 5_000)
        XCTAssertEqual(monthReport.replayedSinceCheckpointCommitCount, 0)
        XCTAssertFalse(monthReport.checkpointRecommended)

        await injectCommitFiles(client: client, writerID: writerA, seqs: 5_001...5_500)
        let appended = materializedOutput(
            finalCovered: coveredRanges(writerA: [(1, 5_500)]),
            acceptedSnapshot: acceptedSnapshotInfo(covered: coveredRanges(writerA: [(1, 5_000)]))
        )
        report = try await RepoCompactionPlanner(client: client, basePath: basePath, policy: policy)
            .makeReport(expectedRepoID: repoID, preMaterialized: appended)
        monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateCount, 5_000)
        XCTAssertEqual(monthReport.replayedSinceCheckpointCommitCount, 500)
        XCTAssertEqual(monthReport.replayedSinceCheckpointBytes, 500)
        XCTAssertTrue(monthReport.checkpointRecommended)
    }

    func testMultiWriterAndGapCoveredRangesUseConservativePrefixes() async throws {
        let client = try await makeClient()
        await injectCommitFiles(client: client, writerID: writerA, seqs: 1...100)
        await injectCommitFiles(client: client, writerID: writerB, seqs: 1...50)
        await injectCommitFiles(client: client, writerID: writerB, seqs: 100...200)
        await injectSnapshotMarker(client: client, lamport: 10, writerID: writerA)
        let acceptedCovered = CoveredRanges(rangesByWriter: [
            writerA: [ClosedSeqRange(low: 1, high: 100)],
            writerB: [ClosedSeqRange(low: 1, high: 50), ClosedSeqRange(low: 100, high: 200)]
        ])
        let output = materializedOutput(
            finalCovered: acceptedCovered,
            acceptedSnapshot: acceptedSnapshotInfo(covered: acceptedCovered)
        )

        let report = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: output)
        let monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.deletePrefixByWriter[writerA], 100)
        XCTAssertEqual(monthReport.deletePrefixByWriter[writerB], 50)
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateCount, 150)
        XCTAssertEqual(monthReport.checkpointCoveredButOutsidePrefixCount, 101)
        XCTAssertEqual(monthReport.notCheckpointCoveredCommitCount, 0)
    }

    func testCoveredGapAndNonOneStartStayProtectedOutsidePrefix() async throws {
        let gapClient = try await makeClient()
        await injectCommitFiles(client: gapClient, writerID: writerA, seqs: 1...100)
        await injectCommitFiles(client: gapClient, writerID: writerA, seqs: 200...300)
        await injectSnapshotMarker(client: gapClient, lamport: 10, writerID: writerA)
        let gapCovered = coveredRanges(writerA: [(1, 100), (200, 300)])
        let gapOutput = materializedOutput(
            finalCovered: gapCovered,
            acceptedSnapshot: acceptedSnapshotInfo(covered: gapCovered)
        )
        var report = try await RepoCompactionPlanner(client: gapClient, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: gapOutput)
        var monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateCount, 100)
        XCTAssertEqual(monthReport.checkpointCoveredButOutsidePrefixCount, 101)

        let nonOneClient = try await makeClient()
        await injectCommitFiles(client: nonOneClient, writerID: writerA, seqs: 5...10)
        await injectSnapshotMarker(client: nonOneClient, lamport: 10, writerID: writerA)
        let nonOneCovered = coveredRanges(writerA: [(5, 10)])
        let nonOneOutput = materializedOutput(
            finalCovered: nonOneCovered,
            acceptedSnapshot: acceptedSnapshotInfo(covered: nonOneCovered)
        )
        report = try await RepoCompactionPlanner(client: nonOneClient, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: nonOneOutput)
        monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.deletePrefixByWriter, [:])
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateCount, 0)
        XCTAssertEqual(monthReport.checkpointCoveredButOutsidePrefixCount, 6)
    }

    func testUnparseableAndUntrustedParseableFilesRemainProtected() async throws {
        let client = try await makeClient()
        await injectCommitFiles(client: client, writerID: writerA, seqs: 1...3)
        await client.injectFile(
            path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/\(month.text)--not-a-writer--0000000000000004.jsonl",
            data: Data([0x00])
        )
        await injectCommitFiles(client: client, writerID: writerA, seqs: 99...99)
        await injectSnapshotMarker(client: client, lamport: 10, writerID: writerA)
        let acceptedCovered = coveredRanges(writerA: [(1, 3)])
        let output = materializedOutput(
            finalCovered: acceptedCovered,
            acceptedSnapshot: acceptedSnapshotInfo(covered: acceptedCovered)
        )

        let report = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: output)
        let monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.commitFileCount, 5)
        XCTAssertEqual(monthReport.unparseableCommitFileCount, 1)
        XCTAssertEqual(monthReport.protectedUnparseableFilenameCount, 1)
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateCount, 3)
        XCTAssertEqual(monthReport.notCheckpointCoveredCommitCount, 1)
    }

    func testMonthPrefixedUnparseableOnlyMetadataStillProducesMonthReport() async throws {
        let client = try await makeClient()
        await client.injectFile(
            path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/\(month.text)--not-a-writer--0000000000000004.jsonl",
            data: Data([0x00, 0x01])
        )
        await client.injectFile(
            path: "\(RepoLayout.snapshotsDirectoryPath(base: basePath))/\(month.text)--bad-snapshot.jsonl",
            data: Data([0x02])
        )
        let output = RepoMaterializer.MaterializeOutput(
            state: RepoSnapshotState(months: [:], observedClock: 0),
            observedSeqByWriter: [:],
            coveredByMonth: [:],
            acceptedSnapshotBaselinesByMonth: [:],
            corruptedSnapshotMonths: [],
            repoID: repoID
        )

        let report = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: output)
        let monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(report.months.map(\.month), [month])
        XCTAssertEqual(monthReport.commitFileCount, 1)
        XCTAssertEqual(monthReport.parseableCommitFileCount, 0)
        XCTAssertEqual(monthReport.unparseableCommitFileCount, 1)
        XCTAssertEqual(monthReport.commitBytes, 2)
        XCTAssertEqual(monthReport.snapshotFileCount, 1)
        XCTAssertEqual(monthReport.parseableSnapshotFileCount, 0)
        XCTAssertEqual(monthReport.unparseableSnapshotFileCount, 1)
        XCTAssertEqual(monthReport.protectedUnparseableFilenameCount, 1)
        XCTAssertEqual(report.totals.unparseableCommitFileCount, 1)
        XCTAssertEqual(report.totals.unparseableSnapshotFileCount, 1)
        XCTAssertEqual(report.totals.protectedUnparseableFilenameCount, 1)
    }

    func testNonCanonicalCommitFilenameIsProtectedAsUnparseable() async throws {
        let client = try await makeClient()
        await client.injectFile(
            path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/\(month.text)--\(writerA)--1.jsonl",
            data: Data([0x01, 0x02, 0x03])
        )
        let output = materializedOutput(finalCovered: .empty, acceptedSnapshot: nil)

        let report = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: output)
        let monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.commitFileCount, 1)
        XCTAssertEqual(monthReport.parseableCommitFileCount, 0)
        XCTAssertEqual(monthReport.unparseableCommitFileCount, 1)
        XCTAssertEqual(monthReport.protectedUnparseableFilenameCount, 1)
        XCTAssertEqual(monthReport.commitBytes, 3)
    }

    func testForeignRepoCommitRemainsProtectedOnMaterializedPath() async throws {
        let client = try await makeClient()
        await injectCommitFiles(client: client, writerID: writerA, seqs: 1...3)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        _ = try await snapshotWriter.write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerA,
                repoID: repoID,
                covered: coveredRanges(writerA: [(1, 3)])
            ),
            assets: [],
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: 10,
            runID: runID,
            respectTaskCancellation: false
        )
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        _ = try await commitWriter.write(
            header: TestFixtures.makeCommitHeader(
                repoID: "bbbbbbbb-cccc-dddd-eeee-ffffffffffff",
                writerID: writerA,
                seq: 99,
                runID: runID,
                month: month
            ),
            ops: [],
            month: month,
            respectTaskCancellation: false
        )

        let report = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID)
        let monthReport = try XCTUnwrap(report.months.first)
        XCTAssertEqual(monthReport.checkpointCoveredPrefixCandidateCount, 3)
        XCTAssertEqual(monthReport.notCheckpointCoveredCommitCount, 1)
        XCTAssertEqual(monthReport.replayedSinceCheckpointCommitCount, 0)
    }

    func testPreMaterializedPathDoesNotMaterializeAgain() async throws {
        let client = try await makeClient()
        await injectCommitFiles(client: client, writerID: writerA, seqs: 1...1)
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await client.injectPersistentDownloadError(.transport, for: path)
        let output = materializedOutput(
            finalCovered: coveredRanges(writerA: [(1, 1)]),
            acceptedSnapshot: nil
        )

        let report = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: output)
        XCTAssertEqual(report.totals.parseableCommitFileCount, 1)
    }

    func testPlannerDoesNotMutateRemoteWithOrWithoutPreMaterializedOutput() async throws {
        let client = try await makeClient()
        let writer = CommitLogWriter(client: client, basePath: basePath)
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerA,
                seq: 1,
                runID: runID,
                month: month
            ),
            ops: [],
            month: month,
            respectTaskCancellation: false
        )
        let beforeWithoutPre = await client.snapshotFiles()
        _ = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID)
        let afterWithoutPre = await client.snapshotFiles()
        XCTAssertEqual(afterWithoutPre, beforeWithoutPre)

        let output = materializedOutput(
            finalCovered: coveredRanges(writerA: [(1, 1)]),
            acceptedSnapshot: nil
        )
        let beforeWithPre = await client.snapshotFiles()
        _ = try await RepoCompactionPlanner(client: client, basePath: basePath)
            .makeReport(expectedRepoID: repoID, preMaterialized: output)
        let afterWithPre = await client.snapshotFiles()
        XCTAssertEqual(afterWithPre, beforeWithPre)
    }

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerA)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        return client
    }

    private func injectCommitFiles(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        seqs: ClosedRange<UInt64>
    ) async {
        for seq in seqs {
            await client.injectFile(
                path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: seq),
                data: Data([0x00])
            )
        }
    }

    private func injectSnapshotMarker(
        client: InMemoryRemoteStorageClient,
        lamport: UInt64,
        writerID: String
    ) async {
        await client.injectFile(
            path: RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: lamport, writerID: writerID, runID: runID),
            data: Data([0x00])
        )
    }

    private func materializedOutput(
        finalCovered: CoveredRanges,
        acceptedSnapshot: RepoMaterializer.AcceptedSnapshotBaselineInfo?
    ) -> RepoMaterializer.MaterializeOutput {
        RepoMaterializer.MaterializeOutput(
            state: RepoSnapshotState(months: [:], observedClock: 0),
            observedSeqByWriter: [writerA: 0, writerB: 0],
            coveredByMonth: [month: finalCovered],
            acceptedSnapshotBaselinesByMonth: acceptedSnapshot.map { [month: $0] } ?? [:],
            corruptedSnapshotMonths: [],
            repoID: repoID
        )
    }

    private func acceptedSnapshotInfo(covered: CoveredRanges) -> RepoMaterializer.AcceptedSnapshotBaselineInfo {
        RepoMaterializer.AcceptedSnapshotBaselineInfo(
            filename: RepoLayout.snapshotFileName(month: month, lamport: 10, writerID: writerA, runID: runID),
            month: month,
            lamport: 10,
            writerID: writerA,
            runIDPrefix: RepoLayout.runIDPrefix(runID),
            covered: covered
        )
    }

    private func coveredRanges(writerA ranges: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [
            self.writerA: ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        ])
    }

    func testListCancellationPropagates() async throws {
        let commitsDir = RepoLayout.commitsDirectoryPath(base: basePath)
        let snapshotsDir = RepoLayout.snapshotsDirectoryPath(base: basePath)
        let output = materializedOutput(finalCovered: .empty, acceptedSnapshot: nil)

        let snapshotClient = try await makeClient()
        await snapshotClient.injectListWrappedURLCancellation(for: snapshotsDir)
        do {
            _ = try await RepoCompactionPlanner(client: snapshotClient, basePath: basePath)
                .makeReport(expectedRepoID: repoID, preMaterialized: output)
            XCTFail("expected cancellation from snapshots list")
        } catch is CancellationError {}

        let commitClient = try await makeClient()
        await commitClient.injectListWrappedURLCancellation(for: commitsDir)
        do {
            _ = try await RepoCompactionPlanner(client: commitClient, basePath: basePath)
                .makeReport(expectedRepoID: repoID, preMaterialized: output)
            XCTFail("expected cancellation from commits list")
        } catch is CancellationError {}
    }

    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerA = "11111111-1111-1111-1111-111111111111"
    private let writerB = "22222222-2222-2222-2222-222222222222"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let month = LibraryMonthKey(year: 2026, month: 5)
}

import XCTest
import os
@testable import Watermelon

final class RepoCompactionServiceTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let otherWriterID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-compaction-test"
    private let year = 2026
    private let monthValue = 5
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: monthValue) }
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    // MARK: - Snapshot GC disposition tests

    func testStartupSnapshotGCSkippedBelowThreshold() async throws {
        // Only the single baseline snapshot exists (file count 1 ≤ keepN+margin=4), so the
        // threshold-gated startup path skips snapshot GC without paying for a fresh materialize.
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactStartupMonths()

        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "startup compaction must process the month with checkpoint-eligible commits")
        XCTAssertEqual(monthResult.snapshotGC, .skipped(.skippedBelowThreshold))
    }

    func testStartupSnapshotGCSkippedAtThreshold() async throws {
        // Exactly keepN+margin (4) snapshot files: the strict `>` gate must NOT run snapshot GC.
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4])
        try await writeCommits(client: client, seqs: 1...4)
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "covered-prefix candidates must select the month for startup compaction")
        XCTAssertEqual(monthResult.snapshotGC, .skipped(.skippedBelowThreshold))
    }

    func testStartupSnapshotGCRunsAboveThresholdDeletingDominated() async throws {
        // Five nested snapshots (file count 5 > 4): the gate passes and the three oldest, strictly
        // dominated snapshots are deleted while the accepted baseline + newest keepN are retained.
        let client = try await makeConnectedClient()
        // Empty-but-covered nested snapshots (no commit prefix behind them): a post-GC empty month, so
        // snapshot-GC domination is exercised without an under-representing baseline over real commits.
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4, 5])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "snapshot file count above threshold must select the month for startup compaction")
        guard case .ran(.completed(let summary, _, _)) = monthResult.snapshotGC else {
            return XCTFail("expected snapshot GC to run and complete, got \(monthResult.snapshotGC)")
        }
        // keepN=2 retains lamports 50 (accepted) and 40; the three dominated snapshots go.
        XCTAssertEqual(summary.deleted.compactMap(\.snapshotLamport).sorted(), [10, 20, 30])
        XCTAssertFalse(summary.deleted.contains { $0.snapshotLamport == 40 || $0.snapshotLamport == 50 },
            "accepted baseline and newest keepN must never be deleted")
    }

    func testStartupCompactionFailsClosedOnSurvivingPeerMigrationMarker() async throws {
        // Same fixture as testStartupSnapshotGCRunsAboveThresholdDeletingDominated (which deletes
        // lamports 10/20/30): a surviving peer migration marker must now fail-close every delete path.
        // Cleanup-only open clears only the selected owner's marker, so a peer marker can still be
        // advertised when startup maintenance runs immediately after open.
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4, 5])
        try await MigrationMarkerStore(client: client, basePath: basePath)
            .writePhase(writerID: otherWriterID, phase: .phase3, runID: "peer-run")
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "snapshot threshold still selects the month even when the delete is blocked")

        guard case .preflightBlocked(let commitBlockers, _) = monthResult.commitCleanup else {
            return XCTFail("commit GC must fail closed on a migration marker, got \(String(describing: monthResult.commitCleanup))")
        }
        XCTAssertEqual(commitBlockers, [.migrationInProgress])

        guard case .ran(let snapResult) = monthResult.snapshotGC,
              case .preflightBlocked(let snapshotBlockers, _) = snapResult else {
            return XCTFail("snapshot GC must fail closed on a migration marker, got \(monthResult.snapshotGC)")
        }
        XCTAssertEqual(snapshotBlockers, [.migrationInProgress])

        // .preflightBlocked is decided before the delete executor runs, so nothing was removed; the
        // peer marker survives for the next inspection to resolve.
        let markerStillPresent = try await MigrationMarkerStore(client: client, basePath: basePath).existsAny()
        XCTAssertTrue(markerStillPresent, "compaction must not touch the migration marker")
    }

    func testStartupSnapshotGCIgnoresCorruptSiblingWhileDeletingDominated() async throws {
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4, 5])
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 60, writerID: otherWriterID, runID: "bad-sibling"
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "snapshot threshold must still select the month")
        guard case .ran(.completed(let summary, let report, _)) = monthResult.snapshotGC else {
            return XCTFail("expected snapshot GC to complete around corrupt sibling, got \(monthResult.snapshotGC)")
        }
        XCTAssertEqual(summary.deleted.compactMap(\.snapshotLamport).sorted(), [10, 20, 30])
        XCTAssertEqual(report.candidateScan?.protectedSummary.corruptOrUntrustedCandidateCount, 1)
        let survived = await client.hasFile(corruptPath)
        XCTAssertTrue(survived, "corrupt sibling is ignored for authority but not deleted by snapshot GC")
    }

    func testSnapshotDeleteCandidateScannerBlocksOnListedSnapshotDownloadNotFound() async throws {
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4, 5])
        let racingPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 40, writerID: writerID, runID: runID
        )
        await client.injectPersistentDownloadError(.notFound, for: racingPath)

        let accepted = RepoMaterializer.AcceptedSnapshotBaselineInfo(
            filename: RepoLayout.snapshotFileName(month: monthKey, lamport: 50, writerID: writerID, runID: runID),
            month: monthKey,
            lamport: 50,
            writerID: writerID,
            runIDPrefix: RepoLayout.runIDPrefix(runID),
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 5)]])
        )
        let scan = try await SnapshotDeleteCandidateScanner(
            client: client,
            basePath: basePath,
            policy: .default
        ).scan(month: monthKey, expectedRepoID: repoID, acceptedBaseline: accepted)

        XCTAssertTrue(scan.blockers.contains { if case .candidateReadFailed = $0 { return true } else { return false } },
            "listed snapshot download notFound is a read race, not bad metadata")
    }

    func testStartupSnapshotGCIgnoresPoisonedFilenameLamportSibling() async throws {
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4, 5])
        try await writeChainSnapshot(
            client: client,
            high: 100,
            lamport: LamportClock.maxAdoptableValue,
            runID: "poison-lamport"
        )
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey])
        guard case .ran(.completed(let summary, let report, _)) = monthResult.snapshotGC else {
            return XCTFail("expected snapshot GC to complete around poisoned filename-lamport sibling, got \(monthResult.snapshotGC)")
        }
        XCTAssertEqual(summary.deleted.compactMap(\.snapshotLamport).sorted(), [10, 20, 30])
        XCTAssertEqual(report.candidateScan?.protectedSummary.corruptOrUntrustedCandidateCount, 1)
    }

    func testStartupSnapshotGCRunsWhenOnlySnapshotThresholdSelectsMonth() async throws {
        // Snapshots cover [3,N] (no seq-1 prefix ⇒ no deletable commit prefix) and a single in-range
        // commit supplies content but no checkpoint recommendation. The month is selected for startup
        // ONLY because snapshotFileCount (5) exceeds keepN+margin, and snapshot GC still runs.
        let client = try await makeConnectedClient()
        for high in UInt64(3)...UInt64(7) {
            try await writeChainSnapshot(client: client, low: 3, high: high, lamport: high * 10)
        }
        try await writeAddCommit(client: client, seq: 3, clock: 3, assetByte: 3)
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "snapshot accumulation alone must select the month for startup compaction")
        // No deletable commit prefix exists, so commit GC blocks — proving the month was selected
        // purely by the snapshot threshold, not by a commit-GC signal.
        if case .preflightBlocked = monthResult.commitCleanup {} else {
            XCTFail("expected commit GC to find nothing deletable, got \(String(describing: monthResult.commitCleanup))")
        }
        guard case .ran(.completed(let summary, _, _)) = monthResult.snapshotGC else {
            return XCTFail("expected snapshot GC to run and complete, got \(monthResult.snapshotGC)")
        }
        // Accepted [3,7] + newest keepN ([3,7],[3,6]) retained; the three oldest dominated go.
        XCTAssertEqual(summary.deleted.compactMap(\.snapshotLamport).sorted(), [30, 40, 50])
    }

    func testStartupSnapshotGCIgnoresUnparseableSnapshotFilenameWhileDeletingDominated() async throws {
        let client = try await makeConnectedClient()
        // Empty-but-covered nested snapshots only (no real commit prefix to body-retention-block on).
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4, 5])
        let garbagePath = RepoLayout.snapshotsDirectoryPath(base: basePath) + "/\(monthKey.text)--garbage.jsonl"
        await client.injectFile(path: garbagePath, data: Data("not-a-snapshot\n".utf8))
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey])
        guard case .ran(.completed(let summary, let report, _)) = monthResult.snapshotGC else {
            return XCTFail("expected snapshot GC to complete around unparseable filename, got \(monthResult.snapshotGC)")
        }
        XCTAssertEqual(summary.deleted.compactMap(\.snapshotLamport).sorted(), [10, 20, 30])
        XCTAssertEqual(report.candidateScan?.protectedSummary.unparseableSnapshotsForMonth, 1)
        let garbageSurvived = await client.hasFile(garbagePath)
        XCTAssertTrue(garbageSurvived,
            "unparseable same-month metadata is ignored for authority but not deleted by snapshot GC")
    }

    func testStartupSnapshotGCIgnoresDirectoryShapedSnapshotFilenameWhileDeletingDominated() async throws {
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4, 5])
        let garbagePath = RepoLayout.snapshotsDirectoryPath(base: basePath) + "/\(monthKey.text)--directory.jsonl"
        try await client.createDirectory(path: garbagePath)
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey])
        guard case .ran(.completed(let summary, let report, _)) = monthResult.snapshotGC else {
            return XCTFail("expected snapshot GC to complete around directory-shaped filename, got \(monthResult.snapshotGC)")
        }
        XCTAssertEqual(summary.deleted.compactMap(\.snapshotLamport).sorted(), [10, 20, 30])
        XCTAssertEqual(report.candidateScan?.protectedSummary.unparseableSnapshotsForMonth, 1)
        let garbageMetadata = try await client.metadata(path: garbagePath)
        XCTAssertEqual(garbageMetadata?.isDirectory, true,
            "directory-shaped same-month metadata is ignored for authority but not deleted by snapshot GC")
    }

    func testUserMaintenanceSnapshotGCRunsBelowThresholdDeletingDominated() async throws {
        // `.always` (user maintenance) ignores the startup threshold gate: a three-snapshot month
        // (below keepN+margin) still runs and deletes the single dominated snapshot.
        let client = try await makeConnectedClient()
        // Empty-but-covered nested snapshots only (no real commit prefix to body-retention-block on).
        try await writeSnapshotChain(client: client, highs: [1, 2, 3])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)
        guard case .ran(.completed(let summary, _, _)) = result.snapshotGC else {
            return XCTFail("expected user-maintenance snapshot GC to complete, got \(result.snapshotGC)")
        }
        XCTAssertEqual(summary.deleted.compactMap(\.snapshotLamport), [10],
            "only the oldest dominated snapshot is deleted; accepted + keepN are retained")
    }

    func testSnapshotGCSkipsAmbiguousIncomparableMonth() async throws {
        // Two trusted snapshots with incomparable coverage make the month ambiguous; snapshot GC
        // must skip the whole month rather than pick a winner and delete.
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3])
        // A different writer's snapshot whose coverage neither dominates nor is dominated.
        try await writeChainSnapshot(client: client, high: 2, lamport: 35, writer: otherWriterID, runID: "run-other")
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)
        if case .ran = result.snapshotGC {
            XCTFail("ambiguous month must not run destructive snapshot GC, got \(result.snapshotGC)")
        }
        XCTAssertNil(result.commitCleanup, "ambiguous outcome must short-circuit before any deletion")
    }

    // Bug-X P17 R03 (CodexReviewerB/CodexChecker): the snapshot-GC sibling of the commit-GC trap. A
    // body-bearing snapshot covering [1,2] is dominated by newer EMPTY snapshots covering [1,3]; the
    // covered-max accepted baseline is therefore empty. Deleting the dominated body-bearing snapshot on
    // coverage authority alone would drop the last copy of that asset row, so the body-retention guard
    // must fail closed and retain it. (The existing empty-dominated-snapshot GC tests above still delete,
    // proving the guard does not over-block legitimate cleanup.)
    func testSnapshotGCBlocksDeletingBodyBearingSnapshotUnderEmptyAcceptedBaseline() async throws {
        let client = try await makeConnectedClient()
        try await writeBodiedSnapshot(client: client, low: 1, high: 2, lamport: 11, assetByte: 0x42)
        try await writeChainSnapshot(client: client, low: 1, high: 3, lamport: 20)
        try await writeChainSnapshot(client: client, low: 1, high: 3, lamport: 22)
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        guard case .ran(.stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _)) = result.snapshotGC else {
            return XCTFail("snapshot GC must fail closed with bodyRetentionUnproven, got \(result.snapshotGC)")
        }
        XCTAssertFalse(summary.deleted.contains { $0.snapshotLamport == 11 },
            "snapshot GC must not delete a body-bearing snapshot the accepted body fails to retain")
        let bodiedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 11, writerID: writerID, runID: runID
        )
        let survived = await client.hasFile(bodiedPath)
        XCTAssertTrue(survived,
            "the body-bearing dominated snapshot must survive — its asset is not retained by the empty accepted body")
    }

    // Bug-X P17 R04 (CodexReviewerB): snapshot-GC sibling of the partial-resource trap. A dominated
    // snapshot carries the full two-resource asset body; the covered-max accepted (dominating) snapshots
    // keep the same fingerprint/stamp but only one resource/link. Deleting the dominated snapshot would
    // drop the last copy of the omitted resource/link, so the guard must fail closed and retain it.
    func testSnapshotGCBlocksWhenAcceptedBodyOmitsOneDominatedResourceLink() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x66)
        let hashA = TestFixtures.fingerprint(0xA2)
        let hashB = TestFixtures.fingerprint(0xB2)
        let pathA = String(format: "%04d/%02d/asset-66-a.jpg", year, monthValue)
        let pathB = String(format: "%04d/%02d/asset-66-b.jpg", year, monthValue)
        // Dominated [1,1] snapshot carries the FULL two-resource asset body.
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 11, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: pathA, hash: hashA),
            (role: ResourceTypeCode.photo, slot: 1, path: pathB, hash: hashB),
        ])
        // Two dominating [1,2] snapshots keep the same fingerprint/stamp but only the first link; the
        // covered-max accepted baseline is therefore partial. (keepN protects @20/@22; [1,1]@11 is the candidate.)
        for lamport in [UInt64(20), UInt64(22)] {
            try await writeAssetSnapshot(client: client, low: 1, high: 2, lamport: lamport, fp: fp, stampSeq: 1, resources: [
                (role: ResourceTypeCode.photo, slot: 0, path: pathA, hash: hashA),
            ])
        }
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        guard case .ran(.stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _)) = result.snapshotGC else {
            return XCTFail("snapshot GC must fail closed when a dominated resource/link is unretained, got \(result.snapshotGC)")
        }
        XCTAssertFalse(summary.deleted.contains { $0.snapshotLamport == 11 },
            "the dominated full-body snapshot must not be deleted under a partial accepted baseline")
        let dominatedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 11, writerID: writerID, runID: runID
        )
        let survived = await client.hasFile(dominatedPath)
        XCTAssertTrue(survived, "the dominated snapshot holding the full two-resource body must survive")
    }

    // Bug-X P17 R06 (CodexChecker): the R04 guard compared link hashes but not logicalName. A dominated
    // snapshot can carry the faithful original filename while the accepted dominating baseline keeps the
    // SAME fingerprint/stamp/(role,slot,hash) under a mutated logicalName. Same stamp == same op, so the
    // guard must fail closed rather than delete the only snapshot body with the correct filename.
    func testSnapshotGCBlocksWhenAcceptedBodyMutatesSameStampLogicalName() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x88)
        let hash = TestFixtures.fingerprint(0xC2)
        let path = String(format: "%04d/%02d/asset-88.jpg", year, monthValue)
        // Dominated [1,1] snapshot carries the faithful original filename.
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 11, fp: fp, stampSeq: 1, logicalName: "IMG_0001.JPG", resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Two dominating [1,2] snapshots keep the same fingerprint/stamp/hash but a different logicalName.
        for lamport in [UInt64(20), UInt64(22)] {
            try await writeAssetSnapshot(client: client, low: 1, high: 2, lamport: lamport, fp: fp, stampSeq: 1, logicalName: "wrong.JPG", resources: [
                (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
            ])
        }
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        guard case .ran(.stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _)) = result.snapshotGC else {
            return XCTFail("snapshot GC must fail closed when same-stamp logicalName is mutated, got \(result.snapshotGC)")
        }
        XCTAssertFalse(summary.deleted.contains { $0.snapshotLamport == 11 },
            "the dominated snapshot holding the faithful filename must not be deleted")
        let dominatedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 11, writerID: writerID, runID: runID
        )
        let survived = await client.hasFile(dominatedPath)
        XCTAssertTrue(survived, "the dominated snapshot holding the correct original filename must survive")
    }

    // Bug-X P17 R07 (CodexReviewerB/CodexChecker): snapshot GC never proved the candidate's resource rows.
    // A dominated snapshot can carry the faithful physicalRemotePath while the accepted dominating baseline
    // keeps the same fingerprint/stamp/(role,slot,hash)/logicalName but records the resource at a different
    // path. After deletion the faithful path→hash association (restore download source, physical-presence
    // probe) is lost, so snapshot GC must fail closed.
    func testSnapshotGCBlocksWhenAcceptedBodyMutatesSameStampResourcePath() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0xAA)
        let hash = TestFixtures.fingerprint(0xD2)
        let faithfulPath = String(format: "%04d/%02d/asset-aa.jpg", year, monthValue)
        let mutatedPath = String(format: "%04d/%02d/asset-aa-missing.jpg", year, monthValue)
        // Dominated [1,1] snapshot records the faithful resource path.
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 11, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: faithfulPath, hash: hash),
        ])
        // Two dominating [1,2] snapshots keep the same fingerprint/stamp/hash/logicalName but a mutated path.
        for lamport in [UInt64(20), UInt64(22)] {
            try await writeAssetSnapshot(client: client, low: 1, high: 2, lamport: lamport, fp: fp, stampSeq: 1, resources: [
                (role: ResourceTypeCode.photo, slot: 0, path: mutatedPath, hash: hash),
            ])
        }
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        guard case .ran(.stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _)) = result.snapshotGC else {
            return XCTFail("snapshot GC must fail closed when the same-stamp resource path is mutated, got \(result.snapshotGC)")
        }
        XCTAssertFalse(summary.deleted.contains { $0.snapshotLamport == 11 },
            "the dominated snapshot holding the faithful resource path must not be deleted")
        let dominatedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 11, writerID: writerID, runID: runID
        )
        let survived = await client.hasFile(dominatedPath)
        XCTAssertTrue(survived, "the dominated snapshot recording the correct resource path must survive")
    }

    // Bug-X P17 R08 (CodexReviewerB): snapshot-GC sibling of the asset-row metadata gap. A dominated
    // snapshot can carry the faithful creationDateMs while the accepted dominating baseline keeps the same
    // fingerprint/stamp/link/resource but mutates the same-stamp asset row, so snapshot GC must fail closed.
    func testSnapshotGCBlocksWhenAcceptedBodyMutatesSameStampAssetCreationDate() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0xDD)
        let hash = TestFixtures.fingerprint(0xE3)
        let path = String(format: "%04d/%02d/asset-dd.jpg", year, monthValue)
        // Dominated [1,1] snapshot records the faithful creation date.
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 11, fp: fp, stampSeq: 1, creationDateMs: 1234, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Two dominating [1,2] snapshots keep the same fingerprint/stamp/link/resource but a mutated creationDateMs.
        for lamport in [UInt64(20), UInt64(22)] {
            try await writeAssetSnapshot(client: client, low: 1, high: 2, lamport: lamport, fp: fp, stampSeq: 1, creationDateMs: nil, resources: [
                (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
            ])
        }
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        guard case .ran(.stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _)) = result.snapshotGC else {
            return XCTFail("snapshot GC must fail closed when same-stamp asset creationDateMs is mutated, got \(result.snapshotGC)")
        }
        XCTAssertFalse(summary.deleted.contains { $0.snapshotLamport == 11 },
            "the dominated snapshot holding the faithful creation date must not be deleted")
        let dominatedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 11, writerID: writerID, runID: runID
        )
        let survived = await client.hasFile(dominatedPath)
        XCTAssertTrue(survived, "the dominated snapshot recording the correct creation date must survive")
    }

    // Bug-X P17 R08 (CodexChecker): snapshot-GC sibling of the strictly-older resource-row gap. A dominated
    // snapshot carries the faithful resource row stamped at (2), while the accepted dominating baseline
    // keeps the asset/link at stamp (2) but its backing resource row at a STRICTLY-OLDER stamp (1).
    func testSnapshotGCBlocksWhenAcceptedResourceRowIsStrictlyOlderThanDeleted() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0xEE)
        let hash = TestFixtures.fingerprint(0xE4)
        let path = String(format: "%04d/%02d/asset-ee.jpg", year, monthValue)
        // Dominated [1,2] snapshot carries the faithful resource row stamped at (writer, 2, 2).
        try await writeAssetSnapshot(client: client, low: 1, high: 2, lamport: 11, fp: fp, stampSeq: 2, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Two dominating [1,3] snapshots keep the asset at stamp (2) and the same link, but their backing
        // resource row carries a STRICTLY-OLDER stamp (1).
        for lamport in [UInt64(20), UInt64(22)] {
            try await writeAssetSnapshot(client: client, low: 1, high: 3, lamport: lamport, fp: fp, stampSeq: 2, resourceStampSeq: 1, resources: [
                (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
            ])
        }
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        guard case .ran(.stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _)) = result.snapshotGC else {
            return XCTFail("snapshot GC must fail closed when the accepted resource row is strictly older, got \(result.snapshotGC)")
        }
        XCTAssertFalse(summary.deleted.contains { $0.snapshotLamport == 11 },
            "the dominated snapshot holding the newer faithful resource row must not be deleted")
        let dominatedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 11, writerID: writerID, runID: runID
        )
        let survived = await client.hasFile(dominatedPath)
        XCTAssertTrue(survived, "the dominated snapshot recording the newer resource row must survive")
    }

    // Bug-X P17 R09 (CodexReviewerB): snapshot-GC sibling of the same-stamp resource-timestamp gap. A
    // dominated snapshot carries the faithful resource backedUpAtMs while the accepted dominating baseline
    // keeps the same fingerprint/stamp/link/path/hash but mutates only that freshness timestamp.
    func testSnapshotGCBlocksWhenAcceptedBodyMutatesSameStampResourceBackedUpAt() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0xCD)
        let hash = TestFixtures.fingerprint(0xF2)
        let path = String(format: "%04d/%02d/asset-cd.jpg", year, monthValue)
        // Dominated [1,1] snapshot records the faithful resource backedUpAtMs (== 1 for stampSeq 1).
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 11, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Two dominating [1,2] snapshots keep the same fingerprint/stamp/link/path/hash but a mutated
        // resource backedUpAtMs.
        for lamport in [UInt64(20), UInt64(22)] {
            try await writeAssetSnapshot(client: client, low: 1, high: 2, lamport: lamport, fp: fp, stampSeq: 1, resourceBackedUpAtMs: 999, resources: [
                (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
            ])
        }
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        guard case .ran(.stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _)) = result.snapshotGC else {
            return XCTFail("snapshot GC must fail closed when same-stamp resource backedUpAtMs is mutated, got \(result.snapshotGC)")
        }
        XCTAssertFalse(summary.deleted.contains { $0.snapshotLamport == 11 },
            "the dominated snapshot holding the faithful resource freshness timestamp must not be deleted")
        let dominatedPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 11, writerID: writerID, runID: runID
        )
        let survived = await client.hasFile(dominatedPath)
        XCTAssertTrue(survived, "the dominated snapshot recording the correct resource freshness must survive")
    }

    func testReportOnlyEntryReturnsSkippedReportOnly() async throws {
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .reportSnapshotGC()

        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "report-only entry must process the month with checkpoint-eligible commits")
        XCTAssertEqual(monthResult.snapshotGC, .skipped(.skippedReportOnly))
    }

    func testUserMaintenanceEntryDoesNotSkipDisabled() async throws {
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        // User maintenance uses .always, so it should NOT be .skippedDisabled.
        if case .skipped(.skippedDisabled) = result.snapshotGC {
            XCTFail("user maintenance entry must not use .skippedDisabled")
        }
    }

    // MARK: - Commit GC authority tests

    func testCommitGCRunsAfterCheckpointWithPostCheckpointAccepted() async throws {
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactMonth(monthKey)

        // Checkpoint must succeed and commit GC must run (non-nil).
        XCTAssertEqual(result.outcome, .checkpointWritten)
        XCTAssertNotNil(result.commitCleanup,
            "commit GC must run when post-checkpoint accepted is present")
    }

    func testCompactMonthWithoutCheckpointRecommendationStillAttemptsCommitGC() async throws {
        // Baseline covers seq 1-3; only 1 replay commit — below default threshold of 5.
        let client = try await makeClientWithBaselineAndCommits(replayCount: 1)
        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactMonth(monthKey)

        // R7-2: commit GC is no longer gated on a fresh checkpoint — it runs off the existing
        // accepted baseline. Here there are no deletable commit files, so it blocks rather than skips.
        XCTAssertNotEqual(result.outcome, .checkpointWritten)
        let cleanup = try XCTUnwrap(result.commitCleanup,
            "commit GC must run even when checkpoint is not recommended")
        guard case .preflightBlocked = cleanup else {
            return XCTFail("expected commit GC to find no deletable candidates, got \(cleanup)")
        }
    }

    func testStartupCompactionRunsCommitGCOnResidualPrefixWithoutFreshCheckpoint() async throws {
        // Phase 1: 6 replay commits cross the threshold, writing a checkpoint covering [1,9].
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)
        let service = RepoCompactionService(services: services)
        _ = try await service.compactMonth(monthKey)

        // Phase 2: no new commits, so no fresh checkpoint is recommended, but the accepted
        // snapshot now dominates residual commit files. R7-2 must still select the month via
        // checkpointCoveredPrefixCandidateCount and delete the residual prefix.
        let result = try await service.compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey],
            "startup compaction must select the month via covered-prefix candidates without a fresh checkpoint")
        XCTAssertNotEqual(monthResult.outcome, .checkpointWritten,
            "phase 2 must not write a fresh checkpoint")
        let cleanup = try XCTUnwrap(monthResult.commitCleanup,
            "commit GC must run on the residual deletable prefix")
        guard case .completed(let summary, _, _) = cleanup else {
            return XCTFail("expected residual commit GC to complete, got \(cleanup)")
        }
        XCTAssertFalse(summary.deleted.isEmpty, "residual commits must actually be deleted")
    }

    func testCommitGCIgnoresBadSnapshotSiblingAndSnapshotGCStillRuns() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x31)
        let hash = TestFixtures.fingerprint(0x32)
        let path = String(format: "%04d/%02d/asset-31.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: client, seq: 1, fp: fp, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        for high in UInt64(1)...UInt64(4) {
            try await writeChainSnapshot(client: client, low: 1, high: high, lamport: high + 4)
        }
        try await writeAssetSnapshot(client: client, low: 1, high: 5, lamport: 9, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 20, writerID: otherWriterID, runID: "bad-sibling"
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .completed(let commitSummary, _, let verification) = cleanup else {
            return XCTFail("commit GC must complete around persistent bad snapshot sibling, got \(cleanup)")
        }
        XCTAssertEqual(commitSummary.deletedCount, 1)
        if case .passed = verification {
            // expected
        } else {
            XCTFail("commit-GC post-delete verification should pass around bad snapshot sibling, got \(verification)")
        }
        guard case .ran(.completed(let snapshotSummary, let report, _)) = result.snapshotGC else {
            return XCTFail("snapshot GC should still run after commit-GC passes around bad sibling, got \(result.snapshotGC)")
        }
        XCTAssertFalse(snapshotSummary.deleted.isEmpty, "snapshot GC should delete dominated snapshots")
        XCTAssertEqual(report.candidateScan?.protectedSummary.corruptOrUntrustedCandidateCount, 1)
        let corruptSiblingSurvived = await client.hasFile(corruptPath)
        XCTAssertTrue(corruptSiblingSurvived,
            "bad sibling is ignored for authority but not deleted by snapshot GC")
    }

    func testCommitGCListedSnapshotNotFoundMakesVerificationInconclusiveAndSkipsSnapshotGC() async throws {
        let inner = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x33)
        let hash = TestFixtures.fingerprint(0x34)
        let path = String(format: "%04d/%02d/asset-33.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: inner, seq: 1, fp: fp, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        for high in UInt64(1)...UInt64(4) {
            try await writeChainSnapshot(client: inner, low: 1, high: high, lamport: high + 4)
        }
        try await writeAssetSnapshot(client: inner, low: 1, high: 5, lamport: 9, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let racingPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 8, writerID: writerID, runID: runID
        )
        let wrapped = SnapshotDownloadNotFoundAfterCommitDeleteClient(
            inner: inner,
            targetSnapshotPath: racingPath,
            commitsDirPrefix: RepoLayout.commitsDirectoryPath(base: basePath) + "/"
        )
        let services = try await makeServices(client: inner, metadataClientOverride: wrapped)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .verificationInconclusive(let summary, _, _, let verification) = cleanup else {
            return XCTFail("commit GC must report inconclusive on listed snapshot notFound race, got \(cleanup)")
        }
        XCTAssertEqual(summary.deletedCount, 1,
            "the read race is discovered by post-delete verification after the commit delete")
        if case .inconclusive(reason: .materializerReadFailed) = verification {
            // expected
        } else {
            XCTFail("expected carried verification to be .inconclusive(.materializerReadFailed), got \(verification)")
        }
        XCTAssertEqual(result.snapshotGC, .skipped(.skippedAfterCommitCleanupVerificationInconclusive),
            "snapshot GC must not run after commit-GC post-delete verification is inconclusive")
    }

    // Bug-X P17 R03 (CodexReviewerB/CodexChecker): coverage-only authority must not let commit GC
    // delete commits whose asset rows the accepted snapshot body does not actually retain. A parseable
    // snapshot declaring covered=[1,3] with an empty body would otherwise fold the month clean and
    // drive deletion of the real seq 1…3 asset commits — silent data loss. The body-retention guard
    // must fail closed and leave the commits in place.
    func testCommitGCBlocksDeletingCommitsAnEmptyAcceptedBodyFailsToRetain() async throws {
        let client = try await makeConnectedClient()
        try await writeCommits(client: client, seqs: 1...3)
        try await writeChainSnapshot(client: client, low: 1, high: 3, lamport: 30)
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _) = cleanup else {
            return XCTFail("commit GC must fail closed with bodyRetentionUnproven, got \(cleanup)")
        }
        XCTAssertTrue(summary.deleted.isEmpty, "no commit may be deleted once retention is unproven")
        for seq in UInt64(1)...3 {
            let path = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: seq)
            let survived = await client.hasFile(path)
            XCTAssertTrue(survived,
                "commit seq \(seq) must survive — its assets are not retained by the empty accepted body")
        }
    }

    // Bug-X P17 R04 (CodexReviewerB): the R03 guard proved asset/tombstone retention but ignored
    // subordinate resource/link metadata. A self-consistent accepted snapshot that keeps the SAME asset
    // fingerprint+stamp but omits one of a multi-resource asset's links passes makeBaseline and the
    // asset-level proof, so commit GC could delete the last commit holding the omitted resource/link.
    // The strengthened guard must fail closed when any per-(role,slot) link is unretained.
    func testCommitGCBlocksWhenAcceptedBodyOmitsOneOfTwoResourceLinks() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x55)
        let hashA = TestFixtures.fingerprint(0xA1)
        let hashB = TestFixtures.fingerprint(0xB1)
        let pathA = String(format: "%04d/%02d/asset-55-a.jpg", year, monthValue)
        let pathB = String(format: "%04d/%02d/asset-55-b.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: client, seq: 1, fp: fp, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: pathA, hash: hashA),
            (role: ResourceTypeCode.photo, slot: 1, path: pathB, hash: hashB),
        ])
        // Accepted snapshot covering [1,1] keeps the same fingerprint/stamp but only the first link.
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 5, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: pathA, hash: hashA),
        ])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _) = cleanup else {
            return XCTFail("commit GC must fail closed when a resource/link is unretained, got \(cleanup)")
        }
        XCTAssertTrue(summary.deleted.isEmpty, "no commit may be deleted once a resource/link is unproven")
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 1)
        let survived = await client.hasFile(commitPath)
        XCTAssertTrue(survived, "the two-resource commit holding the omitted resource/link must survive")
    }

    // Bug-X P17 R06 (CodexChecker): the R04 guard proved per-(role,slot) link hashes but ignored
    // subordinate metadata. An accepted snapshot can keep the SAME asset fingerprint/stamp/(role,slot,hash)
    // while mutating logicalName — the original filename restore/index consume (HomeAlbumMatching). A
    // same-stamp accepted row is the SAME op, so a differing logicalName means the accepted body is not
    // faithful; commit GC must fail closed rather than delete the last commit with the correct filename.
    func testCommitGCBlocksWhenAcceptedBodyMutatesSameStampLogicalName() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x77)
        let hash = TestFixtures.fingerprint(0xC1)
        let path = String(format: "%04d/%02d/asset-77.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: client, seq: 1, fp: fp, logicalName: "IMG_0001.JPG", resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Accepted snapshot covering [1,1] keeps the same fingerprint/stamp/(role,slot,hash) but a
        // different logicalName, so the commit holds the only faithful original filename.
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 5, fp: fp, stampSeq: 1, logicalName: "wrong.JPG", resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _) = cleanup else {
            return XCTFail("commit GC must fail closed when same-stamp logicalName is mutated, got \(cleanup)")
        }
        XCTAssertTrue(summary.deleted.isEmpty, "no commit may be deleted once a same-stamp logicalName is unretained")
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 1)
        let survived = await client.hasFile(commitPath)
        XCTAssertTrue(survived, "the commit holding the faithful original filename must survive")
    }

    // Bug-X P17 R07 (CodexReviewerB/CodexChecker): the R06 guard proved the link's hash + logicalName but
    // not the backing resource row. An accepted snapshot can keep the same fingerprint/stamp/(role,slot,hash)
    // and logicalName while mutating the resource row's fileSize, which restore validates downloaded bytes
    // against (RestoreService.contentMismatchReason). Same stamp == same op, so commit GC must fail closed
    // rather than delete the last commit recording the correct size.
    func testCommitGCBlocksWhenAcceptedBodyMutatesSameStampResourceFileSize() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x99)
        let hash = TestFixtures.fingerprint(0xD1)
        let path = String(format: "%04d/%02d/asset-99.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: client, seq: 1, fp: fp, fileSize: 100, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Accepted snapshot covering [1,1] keeps the same fingerprint/stamp/(role,slot,hash)/logicalName but
        // a different resource fileSize, so the commit holds the only faithful size metadata.
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 5, fp: fp, stampSeq: 1, fileSize: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _) = cleanup else {
            return XCTFail("commit GC must fail closed when same-stamp resource fileSize is mutated, got \(cleanup)")
        }
        XCTAssertTrue(summary.deleted.isEmpty, "no commit may be deleted once a same-stamp resource row is unretained")
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 1)
        let survived = await client.hasFile(commitPath)
        XCTAssertTrue(survived, "the commit holding the faithful resource size must survive")
    }

    // Bug-X P17 R08 (CodexReviewerB): the R06/R07 guard proved links + resource rows but not the asset
    // row's projected metadata. An accepted snapshot can keep the same fingerprint/stamp, links, and
    // resource rows while mutating the asset row's creationDateMs — which restore writes into the Photos
    // asset (RestoreService request.creationDate). Same stamp == same op, so commit GC must fail closed
    // rather than delete the last commit recording the correct creation date.
    func testCommitGCBlocksWhenAcceptedBodyMutatesSameStampAssetCreationDate() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0xBB)
        let hash = TestFixtures.fingerprint(0xE1)
        let path = String(format: "%04d/%02d/asset-bb.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: client, seq: 1, fp: fp, creationDateMs: 1234, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Accepted snapshot covering [1,1] keeps the same fingerprint/stamp/link/resource but a different
        // asset-row creationDateMs, so the commit holds the only faithful creation date.
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 5, fp: fp, stampSeq: 1, creationDateMs: nil, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _) = cleanup else {
            return XCTFail("commit GC must fail closed when same-stamp asset creationDateMs is mutated, got \(cleanup)")
        }
        XCTAssertTrue(summary.deleted.isEmpty, "no commit may be deleted once a same-stamp asset row is unretained")
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 1)
        let survived = await client.hasFile(commitPath)
        XCTAssertTrue(survived, "the commit holding the faithful creation date must survive")
    }

    // Bug-X P17 R08 (CodexChecker): the R07 retainsResourceRow accepted a STRICTLY-OLDER accepted resource
    // row as retaining a newer deleted resource row (it compared fields only on equal stamp and never
    // required the accepted row's stamp to be non-earlier). Path-keyed resource rows are LWW, so an older
    // row is neither the same op nor a later supersession; commit GC must fail closed.
    func testCommitGCBlocksWhenAcceptedResourceRowIsStrictlyOlderThanDeleted() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0xCC)
        let hash = TestFixtures.fingerprint(0xE2)
        let path = String(format: "%04d/%02d/asset-cc.jpg", year, monthValue)
        // Faithful commit at seq=2: asset + backing resource row stamped at (writer, 2, 2).
        try await writeMultiResourceCommit(client: client, seq: 2, fp: fp, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Accepted snapshot covering [1,2] keeps the asset at the same stamp (2) and the same link, but its
        // backing resource row carries a STRICTLY-OLDER stamp (1) — a stale, non-superseding copy.
        try await writeAssetSnapshot(client: client, low: 1, high: 2, lamport: 5, fp: fp, stampSeq: 2, resourceStampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _) = cleanup else {
            return XCTFail("commit GC must fail closed when the accepted resource row is strictly older, got \(cleanup)")
        }
        XCTAssertTrue(summary.deleted.isEmpty, "no commit may be deleted when only a stale older resource row is retained")
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 2)
        let survived = await client.hasFile(commitPath)
        XCTAssertTrue(survived, "the commit holding the newer faithful resource row must survive")
    }

    // Bug-X P17 R09 (CodexReviewerB): the R07/R08 resource-row proof compared same-stamp fileSize/type/crypto
    // but omitted creationDateMs/backedUpAtMs. RepoVerifyMonthService gates whole-month-404 cleanup on each
    // resource's backedUpAtMs (within grace ⇒ inconclusive, else ⇒ missing/cleanup-eligible), so a mutated
    // same-stamp backedUpAtMs can drive a wrongful tombstone after GC deletes the faithful body. Same stamp
    // == same op, so commit GC must fail closed.
    func testCommitGCBlocksWhenAcceptedBodyMutatesSameStampResourceBackedUpAt() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0xBC)
        let hash = TestFixtures.fingerprint(0xF1)
        let path = String(format: "%04d/%02d/asset-bc.jpg", year, monthValue)
        // Commit seq=1 ⇒ body.backedUpAtMs == 1, materialized into the resource row at stamp (writer,1,1).
        try await writeMultiResourceCommit(client: client, seq: 1, fp: fp, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        // Accepted snapshot covering [1,1] matches the asset row and the resource path/hash/stamp/size, but
        // mutates only the backing resource row's backedUpAtMs (the freshness authority verify-month reads).
        try await writeAssetSnapshot(client: client, low: 1, high: 1, lamport: 5, fp: fp, stampSeq: 1, resourceBackedUpAtMs: 999, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)

        let cleanup = try XCTUnwrap(result.commitCleanup, "commit GC must run on the covered prefix")
        guard case .stopped(let summary, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _) = cleanup else {
            return XCTFail("commit GC must fail closed when same-stamp resource backedUpAtMs is mutated, got \(cleanup)")
        }
        XCTAssertTrue(summary.deleted.isEmpty, "no commit may be deleted once a same-stamp resource timestamp is unretained")
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 1)
        let survived = await client.hasFile(commitPath)
        XCTAssertTrue(survived, "the commit holding the faithful resource freshness timestamp must survive")
    }

    // MARK: - Materialize reuse

    func testStartupCompactionReusesInitialMaterializeOutput() async throws {
        // Accepted baseline [1,3] + one below-threshold commit outside the delete prefix ⇒ zero
        // candidate months, so candidate selection is the only materialize work in startup.
        let client = try await makeClientWithBaselineAndCommits(replayCount: 1)
        let snapshotsDir = RepoLayout.snapshotsDirectoryPath(base: basePath)

        // Without reuse: the planner runs its own full materialize on top of its own listing.
        let servicesNoBox = try await makeServices(client: client)
        let before0 = await client.listAttemptCount(for: snapshotsDir)
        _ = try await RepoCompactionService(services: servicesNoBox).compactStartupMonths()
        let listsWithoutReuse = await client.listAttemptCount(for: snapshotsDir) - before0

        // With reuse: the box-supplied materialize is passed through, so only the planner's own
        // single listing touches the snapshots directory.
        let preMaterialized = try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: repoID)
        let servicesWithBox = try await makeServices(client: client, initialMaterialize: preMaterialized)
        let before1 = await client.listAttemptCount(for: snapshotsDir)
        _ = try await RepoCompactionService(services: servicesWithBox).compactStartupMonths()
        let listsWithReuse = await client.listAttemptCount(for: snapshotsDir) - before1

        XCTAssertEqual(listsWithReuse, 1,
            "candidate selection must list snapshots once (planner only, no second full materialize)")
        XCTAssertLessThan(listsWithReuse, listsWithoutReuse,
            "reusing initialMaterializeOutput must avoid the planner's second full materialize")
    }

    func testCorruptBaselineSkipsCompactionAndCommitGC() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        // Write a corrupt snapshot (not valid jsonl) so materialize outcome is .corrupt.
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 5, writerID: writerID, runID: runID
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))

        // Write commits so the month has content.
        for i in 0..<6 {
            let seq = UInt64(1 + i)
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }

        let services = try await makeServices(client: client)
        let result = try await RepoCompactionService(services: services)
            .compactMonth(monthKey)

        // Corrupt outcome → compaction skipped entirely, commit GC did not run.
        XCTAssertNotEqual(result.outcome, .checkpointWritten)
        XCTAssertNil(result.commitCleanup,
            "commit GC must not run on corrupt baseline")
    }

    func testRepairCorruptSnapshotBaselineRecoversMonthToClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // The only snapshot for the month is body-corrupt but ATTESTS covering [1..3], and the surviving
        // commits cover exactly [1..3] — its authenticated coverage is globally recorded, so repair is
        // provably safe. The data-intact terminal-corrupt case: un-writable/un-maintainable until repaired.
        for i in 0..<3 {
            let seq = UInt64(1 + i)
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }
        _ = try await TestFixtures.injectAttestedCorruptSnapshot(
            client, basePath: basePath, month: monthKey, writerID: writerID, repoID: repoID,
            lamport: 5, runID: runID,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 3)]])
        )

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .corrupt,
            "precondition: corrupt-only snapshot makes the month terminal-corrupt")

        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 1, "the corrupt-snapshot month must be repaired exactly once")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .clean,
            "after repair the month re-materializes clean and is writable/maintainable again")
        XCTAssertEqual(after.state.months[monthKey]?.assets.count, 3,
            "replay state must survive the repair baseline write")
        XCTAssertFalse(after.corruptedSnapshotMonths.contains(monthKey),
            "the month must no longer be flagged corrupt-snapshot after a fresh baseline is accepted")
    }

    /// A corrupt-snapshot month whose surviving replay carries a forged identity (an asset whose link set
    /// does not recompute to its fingerprint) must NOT be repaired: repair is a baseline-cementing write, so
    /// blessing it `.clean` would launder the forged identity into a fresh attested baseline — exactly what
    /// the compaction/GC identity gates already refuse. Completeness is otherwise provable (attested coverage
    /// [1..1] globally recorded, replay complete), so only the identity gate can keep the month corrupt.
    func testRepairCorruptSnapshotBaselineSkipsMonthWithForgedAssetIdentity() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // Forged addAsset at seq 1: a valid 32-byte fingerprint that is NOT the recompute of its
        // (role, slot, contentHash) link set. The structural-trust materialize still folds it into replay.
        let mismatchedFP = TestFixtures.assetFingerprint(0x7E)
        let resourceHash = TestFixtures.fingerprint(0x7F)
        let forgedPath = String(format: "%04d/%02d/forged.jpg", year, monthValue)
        let op = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: mismatchedFP,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [CommitResourceEntry(
                physicalRemotePath: forgedPath, logicalName: "forged.jpg", contentHash: resourceHash,
                fileSize: 100, resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
            )]
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerID, seq: 1, runID: runID, month: monthKey, clockMin: 1, clockMax: 1
            ),
            ops: [op], month: monthKey, respectTaskCancellation: true
        )
        // Sole snapshot is body-corrupt but attests covering [1..1]; the surviving commit covers exactly
        // [1..1], so every completeness guard passes and only the identity gate can refuse the repair.
        _ = try await TestFixtures.injectAttestedCorruptSnapshot(
            client, basePath: basePath, month: monthKey, writerID: writerID, repoID: repoID,
            lamport: 5, runID: runID,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 1)]])
        )

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .corrupt,
            "precondition: corrupt-only snapshot makes the month terminal-corrupt")
        XCTAssertNotNil(
            RepoMonthStateValidator.assetFingerprintLinkMismatch(
                assets: (before.state.months[monthKey] ?? .empty).assets,
                assetResources: (before.state.months[monthKey] ?? .empty).assetResources
            ),
            "precondition: the surviving replay carries the forged identity")

        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 0,
            "a corrupt month whose replay carries a forged identity must NOT be repaired")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .corrupt,
            "the month must stay corrupt so the forged identity is never laundered into a clean baseline")
        XCTAssertTrue(after.corruptedSnapshotMonths.contains(monthKey),
            "no fresh baseline may be written cementing the forged identity")
    }

    /// Startup maintenance repairs the corrupt-snapshot month, but the open-time materialize output is
    /// boxed for the post-open sync to reuse. If that stale pre-repair output is published, the repaired
    /// month stays in `nonCleanOutcomeMonths` and the committed view fails closed against a now-clean
    /// month. The runner must invalidate the box after a repair so the sync re-materializes.
    func testStartupRepairInvalidatesPreRepairMaterializeSoCommittedViewIsClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // Recoverable corrupt-snapshot-only month: sole snapshot is body-corrupt but attests [1..3] and
        // the surviving commits cover exactly [1..3] (mirrors the recovery precondition above).
        for i in 0..<3 {
            let seq = UInt64(1 + i)
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }
        _ = try await TestFixtures.injectAttestedCorruptSnapshot(
            client, basePath: basePath, month: monthKey, writerID: writerID, repoID: repoID,
            lamport: 5, runID: runID,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 3)]])
        )

        // What BackupV2RepoOpenService materializes and boxes before startup maintenance runs.
        let preRepair = try await RepoMaterializer(client: client, basePath: basePath)
            .materialize(expectedRepoID: repoID)
        XCTAssertEqual(preRepair.outcomeByMonth[monthKey], .corrupt,
            "precondition: month is non-clean only because every snapshot baseline is corrupt")

        let services = try await makeServices(client: client, initialMaterialize: preRepair)
        let diagnostic = try await RepoMaintenanceStartupRunner.runStartupRetentionIfEnabled(
            services: services, mode: .enabled
        )
        XCTAssertEqual(diagnostic.repairedCount, 1, "startup repair must heal the corrupt-snapshot month")

        let boxedAfterRepair = await services.initialMaterializeOutput.peek()
        XCTAssertNil(boxedAfterRepair,
            "repair changed the month corrupt→clean, so the stale pre-repair materialize must be dropped")

        let profile = TestFixtures.makeServerProfile(storageType: .webdav, basePath: basePath, writerID: writerID)
        let remoteIndex = RemoteIndexSyncService()
        _ = try await remoteIndex.syncIndex(
            client: client, profile: profile,
            preMaterialized: boxedAfterRepair, expectV2: true, localRepoID: repoID
        )
        XCTAssertTrue(remoteIndex.nonCleanOutcomeMonths().isEmpty,
            "the repaired month must not remain non-clean in the post-open committed view")
    }

    func testRepairCorruptSnapshotBaselineSkipsMonthWithGCdCommitPrefix() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // The month's only snapshot is corrupt, AND the surviving commits start at seq 4 — i.e. the
        // covered prefix seq 1…3 was already commit-GC'd, with that corrupt snapshot as its sole record.
        // Commit replay can only rebuild the seq 4…5 suffix, so re-blessing the month `.clean` would
        // silently drop the seq 1…3 assets and erase the `.corrupt` signal.
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 5, writerID: writerID, runID: runID
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))
        for seq in UInt64(4)...UInt64(5) {
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .corrupt,
            "precondition: corrupt snapshot with a GC'd prefix is a terminal-corrupt month")
        XCTAssertEqual(before.state.months[monthKey]?.assets.count, 2,
            "precondition: replay sees only the surviving seq 4…5 suffix")

        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 0,
            "a corrupt month whose surviving replay is incomplete must NOT be repaired")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .corrupt,
            "the month must stay corrupt so the data loss keeps surfacing as non-clean")
        XCTAssertTrue(after.corruptedSnapshotMonths.contains(monthKey),
            "no fresh baseline may be written that launders the incomplete replay as clean")
    }

    func testRepairCorruptSnapshotBaselineSkipsMonthWithLostInteriorCommit() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 7, writerID: writerID, runID: runID
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))
        for seq in [UInt64(1), 2, 3, 5, 6] {
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .corrupt)
        XCTAssertEqual(before.state.months[monthKey]?.assets.count, 5,
            "precondition: replay sees the surviving commits but not the lost interior seq")

        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 0,
            "a corrupt month with an interior coverage hole must not be re-blessed clean")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .corrupt)
        XCTAssertTrue(after.corruptedSnapshotMonths.contains(monthKey))
    }

    func testRepairCorruptSnapshotBaselineSkipsMonthWithDanglingReplayLink() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 5, writerID: writerID, runID: runID
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))
        let sharedPath = String(format: "%04d/%02d/shared-conflict.jpg", year, monthValue)
        try await writeAddCommit(
            client: client,
            seq: 1,
            clock: 1,
            assetByte: 0xA1,
            path: sharedPath,
            hash: TestFixtures.fingerprint(0xB1)
        )
        try await writeAddCommit(
            client: client,
            seq: 2,
            clock: 2,
            assetByte: 0xA2,
            path: sharedPath,
            hash: TestFixtures.fingerprint(0xB2)
        )

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .corrupt)
        XCTAssertFalse(before.corruptedSnapshotMonths.contains(monthKey),
            "dangling replay state is not complete replay and must not be repair-eligible")

        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 0,
            "repair must not write a baseline that makeBaseline will reject again on the next materialize")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .corrupt)
        XCTAssertFalse(after.corruptedSnapshotMonths.contains(monthKey))
    }

    func testRepairCrossWriterCorruptSnapshotRecoversToClean() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // The month's only snapshot is body-corrupt and authored by a peer writer, but it ATTESTS covering
        // writerID [1..3] — globally recorded by the surviving commits. With no trusted baseline it still
        // enters corrupt-snapshot repair, and the repaired trusted baseline is no longer vetoed by the
        // lingering bad sibling.
        for seq in UInt64(1)...UInt64(3) {
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }
        _ = try await TestFixtures.injectAttestedCorruptSnapshot(
            client, basePath: basePath, month: monthKey, writerID: otherWriterID, repoID: repoID,
            lamport: 5, runID: "run-peer",
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 3)]])
        )

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .corrupt,
            "precondition: a corrupt-only snapshot makes the month terminal-corrupt")

        let services = try await makeServices(client: client)
        _ = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .clean,
            "a repaired trusted baseline must not stay non-clean because of a cross-writer corrupt sibling")
    }

    func testRepairSkipsCorruptSnapshotMonthWithOutOfMonthReplayOp() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // The month's only snapshot is corrupt AND a COVERED commit (seq 1) carries an addAsset whose
        // resource path lies outside the month. The projector skips that op (asset dropped) but the
        // commit stays covered, so a repair baseline would absorb seq 1 and silence the out-of-month
        // signal, laundering the dropped asset to clean and freeing commit GC to delete its only record.
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 5, writerID: writerID, runID: runID
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))

        let assetFP = TestFixtures.assetFingerprint(0x10)
        let hash = TestFixtures.fingerprint(0x11)
        // monthKey is 2026/05; this resource path is in 2026/02 — out-of-month.
        let outOfMonthResource = [CommitResourceEntry(
            physicalRemotePath: String(format: "%04d/02/wrong-month.jpg", year),
            logicalName: "asset.jpg",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            role: ResourceTypeCode.photo,
            slot: 0,
            crypto: nil
        )]
        let op = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: assetFP, creationDateMs: nil, backedUpAtMs: 1, resources: outOfMonthResource
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerID, seq: 1, runID: runID,
                month: monthKey, clockMin: 1, clockMax: 1
            ),
            ops: [op], month: monthKey, respectTaskCancellation: true
        )

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .corrupt,
            "precondition: corrupt snapshot + out-of-month covered op is a terminal-corrupt month")
        XCTAssertFalse(before.corruptedSnapshotMonths.contains(monthKey),
            "an out-of-month-tainted month must be excluded from the repair-eligible corrupt-snapshot set")

        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 0,
            "repair must not launder a month whose replay dropped a covered out-of-month op")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .corrupt,
            "the month must stay corrupt so commit GC cannot delete the only record of the dropped asset")
    }

    func testRepairCorruptSnapshotBaselineIsNoOpWithoutCorruptMonths() async throws {
        // A healthy month must not trigger any repair write.
        let client = try await makeClientWithBaselineAndCommits(replayCount: 1)
        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 0, "no corrupt-snapshot months means no repair writes")
    }

    func testRepairSkipsLegacyBodyCorruptSnapshotWithCompleteReplay() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // A legacy (no coverage attestation) body-corrupt snapshot. Commit replay rebuilds full state
        // (seq 1…3), but the snapshot's covered is unauthenticated — it could have been the sole record of
        // a now-GC'd prefix, so repair must fail closed rather than launder unknown coverage to clean.
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 5, writerID: writerID, runID: runID
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))
        for seq in UInt64(1)...UInt64(3) {
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }

        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 0,
            "a legacy body-corrupt snapshot with unknown coverage must not auto-repair clean")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .corrupt,
            "the month stays corrupt because the corrupt snapshot's coverage cannot be authenticated")
    }

    func testRepairSkipsAttestedCorruptWhenAuthenticatedCoverageNotGloballyRecorded() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // D-001: the body-corrupt snapshot ATTESTS covering [1..5], but only the seq 4..5 suffix survives as
        // commits — the seq 1..3 prefix was GC'd with this snapshot as its sole record. Its authenticated
        // coverage is NOT globally recorded by surviving coverage, so repair must stay closed rather than
        // launder the lost prefix into a verified-clean baseline.
        for seq in UInt64(4)...UInt64(5) {
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }
        _ = try await TestFixtures.injectAttestedCorruptSnapshot(
            client, basePath: basePath, month: monthKey, writerID: writerID, repoID: repoID,
            lamport: 5, runID: runID,
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 5)]])
        )

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .corrupt,
            "precondition: corrupt snapshot with a GC'd attested prefix is terminal-corrupt")
        XCTAssertEqual(before.authenticatedCorruptCoverageByMonth[monthKey]?.rangesByWriter,
            [writerID: [ClosedSeqRange(low: 1, high: 5)]],
            "the attested coverage is recovered as repair evidence")

        let services = try await makeServices(client: client)
        let repaired = try await RepoCompactionService(services: services).repairCorruptSnapshotBaselines()
        XCTAssertEqual(repaired, 0,
            "attested coverage not globally recorded by surviving coverage must not be repaired")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .corrupt,
            "the month stays corrupt so the GC'd-prefix data loss keeps surfacing as non-clean")
    }

    // MARK: - Snapshot GC cancellation propagation

    /// Snapshot GC cancellation must propagate out of `compactMonth` like checkpoint and commit GC do,
    /// not be swallowed into a `.skipped(.skippedCancellation)` disposition that returns normally.
    /// Otherwise a cancellation landing on the last startup month's snapshot-GC phase lets
    /// `compactStartupMonths` return normally, so `runStartupRetentionIfEnabled` never reaches its
    /// cancel-path `services.shutdown()`.
    func testSnapshotGCCancellationPropagatesOutOfCompactMonth() async throws {
        let inner = try await makeConnectedClient()
        // Empty-but-covered nested snapshots only (no real commit prefix to body-retention-block on).
        try await writeSnapshotChain(client: inner, highs: [1, 2, 3])
        // The snapshot-GC delete reports cancellation; the executor maps it to `.deleteFailed(_, .cancelled)`,
        // so `runSnapshotGC` throws CancellationError — deterministic, without racing Task.cancel().
        let wrapped = CancelOnSnapshotDeleteClient(
            inner: inner,
            snapshotsDirPrefix: RepoLayout.snapshotsDirectoryPath(base: basePath)
        )
        let services = try await makeServices(client: inner, metadataClientOverride: wrapped)

        do {
            _ = try await RepoCompactionService(services: services).compactMonthForUserMaintenance(monthKey)
            XCTFail("snapshot-GC cancellation must propagate, not be swallowed into a skipped disposition")
        } catch is CancellationError {
            // expected — matches checkpoint and commit GC cancellation handling
        }
    }

    /// A cancellation landing during the compaction planner's directory listing must propagate out of
    /// `compactMonth`, not be swallowed by the planner `try?` into a normal skip. Otherwise a last-month
    /// planner-phase cancellation lets the startup loop finish normally and `runStartupRetentionIfEnabled`
    /// never reaches its cancel-path `services.shutdown()` — the same invariant the snapshot-GC fix upholds.
    func testPlannerCancellationPropagatesOutOfCompactMonth() async throws {
        let inner = try await makeConnectedClient()
        try await writeSnapshotChain(client: inner, highs: [1, 2, 3])
        try await writeCommits(client: inner, seqs: 1...3)
        // materializeMonth lists the commits directory once, then the planner lists it again; throwing
        // cancellation on the SECOND commits-dir list isolates the planner phase (materialize succeeds).
        let wrapped = CancelOnSecondCommitsListClient(
            inner: inner,
            commitsDir: RepoLayout.commitsDirectoryPath(base: basePath)
        )
        let services = try await makeServices(client: inner, metadataClientOverride: wrapped)

        do {
            _ = try await RepoCompactionService(services: services).compactMonthForUserMaintenance(monthKey)
            XCTFail("planner-phase cancellation must propagate, not be swallowed into a skipped result")
        } catch is CancellationError {
            // expected — matches checkpoint / commit GC / snapshot GC cancellation handling
        }
    }

    // MARK: - S2b-a shared metadata-delete model (differential)

    func testCommitScannerMapsValidCommitPrefixCandidateToSharedModel() async throws {
        let client = try await makeConnectedClient()
        try await writeCommits(client: client, seqs: 1...3)

        let scan = try await RepoRetentionDeleteCandidateScanner(client: client, basePath: basePath)
            .scan(month: monthKey, expectedRepoID: repoID, deletePrefixByWriter: [writerID: 3])

        XCTAssertTrue(scan.blockers.isEmpty, "a valid commit prefix must raise no blockers")
        XCTAssertEqual(scan.candidates.count, 3)

        for seq in UInt64(1)...3 {
            let candidate = try XCTUnwrap(scan.candidates.first { $0.commitSeq == seq })
            XCTAssertEqual(candidate.kind, .commit(seq: seq))
            XCTAssertEqual(candidate.month, monthKey)
            XCTAssertEqual(candidate.writerID, writerID)
            XCTAssertEqual(
                candidate.path,
                RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: seq)
            )
            XCTAssertEqual(candidate.sha256Hex, candidate.sha256Hex.lowercased())
            let commit = try await CommitLogReader(client: client, basePath: basePath)
                .read(remotePath: candidate.path)
            XCTAssertEqual(candidate.sha256Hex, commit.sha256Hex.lowercased())
            XCTAssertEqual(candidate.rowCount, commit.rowCount)
            XCTAssertGreaterThan(candidate.rowCount, 0)
        }

        // Existing writer-then-seq ordering must be preserved.
        XCTAssertEqual(scan.candidates.compactMap(\.commitSeq), [1, 2, 3])
    }

    func testSnapshotScannerMapsValidSnapshotCandidateToSharedModel() async throws {
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3])

        let accepted = RepoMaterializer.AcceptedSnapshotBaselineInfo(
            filename: RepoLayout.snapshotFileName(month: monthKey, lamport: 30, writerID: writerID, runID: runID),
            month: monthKey,
            lamport: 30,
            writerID: writerID,
            runIDPrefix: RepoLayout.runIDPrefix(runID),
            covered: CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 3)]])
        )
        let scan = try await SnapshotDeleteCandidateScanner(client: client, basePath: basePath, policy: .default)
            .scan(month: monthKey, expectedRepoID: repoID, acceptedBaseline: accepted)

        XCTAssertTrue(scan.blockers.isEmpty, "a valid snapshot set must raise no blockers")
        XCTAssertTrue(scan.acceptedBaselineListed)
        XCTAssertEqual(scan.candidates.count, 3)

        for lamport in [UInt64(10), 20, 30] {
            let candidate = try XCTUnwrap(scan.candidates.first { $0.snapshotLamport == lamport })
            XCTAssertEqual(candidate.kind, .snapshot(lamport: lamport, runIDPrefix: RepoLayout.runIDPrefix(runID)))
            XCTAssertEqual(candidate.month, monthKey)
            XCTAssertEqual(candidate.writerID, writerID)
            XCTAssertEqual(
                candidate.path,
                RepoLayout.snapshotFilePath(base: basePath, month: monthKey, lamport: lamport, writerID: writerID, runID: runID)
            )
            XCTAssertEqual(candidate.sha256Hex, candidate.sha256Hex.lowercased())
            let file = try await SnapshotReader(client: client, basePath: basePath).read(filename: candidate.filename)
            XCTAssertEqual(candidate.sha256Hex, file.sha256Hex.lowercased())
            XCTAssertEqual(candidate.rowCount, file.rowCount)
        }

        // Existing lamport-then-filename ordering must be preserved.
        XCTAssertEqual(scan.candidates.compactMap(\.snapshotLamport), [10, 20, 30])
    }

    func testCommitGCSummaryReportsSharedCandidateCountsAndArrays() async throws {
        let client = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x31)
        let hash = TestFixtures.fingerprint(0x32)
        let path = String(format: "%04d/%02d/asset-31.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: client, seq: 1, fp: fp, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        for high in UInt64(1)...UInt64(4) {
            try await writeChainSnapshot(client: client, low: 1, high: high, lamport: high + 4)
        }
        try await writeAssetSnapshot(client: client, low: 1, high: 5, lamport: 9, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)
        let cleanup = try XCTUnwrap(result.commitCleanup)
        guard case .completed(let summary, _, _) = cleanup else {
            return XCTFail("expected commit GC to complete, got \(cleanup)")
        }
        XCTAssertEqual(summary.candidateCount, 1)
        XCTAssertEqual(summary.attemptedCount, 1)
        XCTAssertEqual(summary.deletedCount, 1)
        XCTAssertEqual(summary.alreadyMissingCount, 0)
        XCTAssertEqual(summary.attempted, summary.deleted)
        XCTAssertTrue(summary.alreadyMissing.isEmpty)
        XCTAssertEqual(summary.deleted.compactMap(\.commitSeq), [1])
    }

    func testSnapshotGCSummaryReportsSharedCandidateCountsAndArrays() async throws {
        let client = try await makeConnectedClient()
        try await writeSnapshotChain(client: client, highs: [1, 2, 3, 4, 5])
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey])
        guard case .ran(.completed(let summary, _, _)) = monthResult.snapshotGC else {
            return XCTFail("expected snapshot GC to complete, got \(monthResult.snapshotGC)")
        }
        XCTAssertEqual(summary.candidateCount, 3)
        XCTAssertEqual(summary.attemptedCount, 3)
        XCTAssertEqual(summary.deletedCount, 3)
        XCTAssertEqual(summary.alreadyMissingCount, 0)
        XCTAssertEqual(summary.attempted, summary.deleted)
        XCTAssertTrue(summary.alreadyMissing.isEmpty)
        XCTAssertEqual(summary.deleted.compactMap(\.snapshotLamport).sorted(), [10, 20, 30])
    }

    func testSnapshotGCSkippedAfterCommitCleanupStopped() async throws {
        let client = try await makeConnectedClient()
        try await writeCommits(client: client, seqs: 1...3)
        try await writeChainSnapshot(client: client, low: 1, high: 3, lamport: 30)
        let services = try await makeServices(client: client)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)
        let cleanup = try XCTUnwrap(result.commitCleanup)
        guard case .stopped(_, .preDeleteRevalidationFailed(_, .bodyRetentionUnproven), _, _) = cleanup else {
            return XCTFail("expected commit GC to stop with bodyRetentionUnproven, got \(cleanup)")
        }
        XCTAssertEqual(result.snapshotGC, .skipped(.skippedAfterCommitCleanupStopped),
            "snapshot GC must stay skipped after a commit cleanup stop")
    }

    /// Commit-GC sibling of `testSnapshotGCCancellationPropagatesOutOfCompactMonth`: a delete that reports
    /// cancellation maps to `.deleteFailed(_, .cancelled)`, so `runCommitGC` throws CancellationError and it
    /// propagates out of `compactMonth` rather than being swallowed into a completed/blocked result.
    func testCommitGCCancellationPropagatesOutOfCompactMonth() async throws {
        let inner = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x31)
        let hash = TestFixtures.fingerprint(0x32)
        let path = String(format: "%04d/%02d/asset-31.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: inner, seq: 1, fp: fp, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        for high in UInt64(1)...UInt64(4) {
            try await writeChainSnapshot(client: inner, low: 1, high: high, lamport: high + 4)
        }
        try await writeAssetSnapshot(client: inner, low: 1, high: 5, lamport: 9, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let wrapped = CancelOnCommitDeleteClient(
            inner: inner,
            commitsDirPrefix: RepoLayout.commitsDirectoryPath(base: basePath) + "/"
        )
        let services = try await makeServices(client: inner, metadataClientOverride: wrapped)

        do {
            _ = try await RepoCompactionService(services: services).compactMonthForUserMaintenance(monthKey)
            XCTFail("commit-GC cancellation must propagate, not be swallowed into a non-cancel result")
        } catch is CancellationError {
            // expected — matches the snapshot-GC cancellation handling
        }
    }

    // MARK: - S2b-b shared metadata-delete transaction (differential + oracles)

    /// The shared commit scanner must order candidates writerID-then-seq across writers regardless of the
    /// listing order — the ordering the shared transaction then walks. Two writer IDs with interleaved
    /// write order isolates the sort from any incidental directory-listing order.
    func testCommitScannerOrdersCandidatesByWriterThenSeqAcrossWriters() async throws {
        let client = try await makeConnectedClient()
        // Interleaved write order: prove the scanner sorts rather than echoing insertion order.
        try await writeAddCommit(client: client, seq: 2, clock: 2, assetByte: 0xB2, writer: otherWriterID)
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xA1)
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB1, writer: otherWriterID)
        try await writeAddCommit(client: client, seq: 2, clock: 2, assetByte: 0xA2)

        let scan = try await RepoRetentionDeleteCandidateScanner(client: client, basePath: basePath)
            .scan(
                month: monthKey,
                expectedRepoID: repoID,
                deletePrefixByWriter: [writerID: 2, otherWriterID: 2]
            )

        XCTAssertTrue(scan.blockers.isEmpty, "a valid multi-writer prefix must raise no blockers")
        XCTAssertEqual(scan.candidates.count, 4)
        // writerID (aaaa…) sorts before otherWriterID (bbbb…); within a writer, ascending seq.
        XCTAssertEqual(
            scan.candidates.map(\.writerID),
            [writerID, writerID, otherWriterID, otherWriterID]
        )
        XCTAssertEqual(scan.candidates.compactMap(\.commitSeq), [1, 2, 1, 2])
    }

    /// Commit-GC oracle: a `delete` that reports not-found (file already gone) must be classified as
    /// `alreadyMissing`, not `deleted`, while `attempted` still records the candidate and post-delete
    /// verification stays intact because the file is genuinely absent.
    func testCommitGCClassifiesNotFoundDeleteAsAlreadyMissing() async throws {
        let inner = try await makeConnectedClient()
        let fp = TestFixtures.assetFingerprint(0x31)
        let hash = TestFixtures.fingerprint(0x32)
        let path = String(format: "%04d/%02d/asset-31.jpg", year, monthValue)
        try await writeMultiResourceCommit(client: inner, seq: 1, fp: fp, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        for high in UInt64(1)...UInt64(4) {
            try await writeChainSnapshot(client: inner, low: 1, high: high, lamport: high + 4)
        }
        try await writeAssetSnapshot(client: inner, low: 1, high: 5, lamport: 9, fp: fp, stampSeq: 1, resources: [
            (role: ResourceTypeCode.photo, slot: 0, path: path, hash: hash),
        ])
        let wrapped = DeleteThenReportNotFoundClient(
            inner: inner,
            dirPrefix: RepoLayout.commitsDirectoryPath(base: basePath)
        )
        let services = try await makeServices(client: inner, metadataClientOverride: wrapped)

        let result = try await RepoCompactionService(services: services)
            .compactMonthForUserMaintenance(monthKey)
        let cleanup = try XCTUnwrap(result.commitCleanup)
        guard case .completed(let summary, _, _) = cleanup else {
            return XCTFail("expected commit GC to complete despite not-found delete, got \(cleanup)")
        }
        XCTAssertEqual(summary.candidateCount, 1)
        XCTAssertEqual(summary.attemptedCount, 1, "attempted is appended before the delete attempt")
        XCTAssertEqual(summary.deletedCount, 0, "a not-found delete must not be counted as deleted")
        XCTAssertEqual(summary.alreadyMissingCount, 1)
        XCTAssertEqual(summary.alreadyMissing.compactMap(\.commitSeq), [1])
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 1)
        let stillPresent = await inner.hasFile(commitPath)
        XCTAssertFalse(stillPresent, "the commit file is genuinely gone, so post-delete verification holds")
    }

    /// Snapshot-GC oracle: the same not-found delete classification holds for the snapshot family — every
    /// dominated snapshot whose delete reports not-found lands in `alreadyMissing`, none in `deleted`, and
    /// the GC still completes with verification intact.
    func testSnapshotGCClassifiesNotFoundDeleteAsAlreadyMissing() async throws {
        let inner = try await makeConnectedClient()
        try await writeSnapshotChain(client: inner, highs: [1, 2, 3, 4, 5])
        let wrapped = DeleteThenReportNotFoundClient(
            inner: inner,
            dirPrefix: RepoLayout.snapshotsDirectoryPath(base: basePath)
        )
        let services = try await makeServices(client: inner, metadataClientOverride: wrapped)

        let result = try await RepoCompactionService(services: services).compactStartupMonths()
        let monthResult = try XCTUnwrap(result.monthResults[monthKey])
        guard case .ran(.completed(let summary, _, _)) = monthResult.snapshotGC else {
            return XCTFail("expected snapshot GC to complete despite not-found deletes, got \(monthResult.snapshotGC)")
        }
        XCTAssertEqual(summary.candidateCount, 3)
        XCTAssertEqual(summary.attemptedCount, 3)
        XCTAssertEqual(summary.deletedCount, 0, "not-found deletes must not be counted as deleted")
        XCTAssertEqual(summary.alreadyMissingCount, 3)
        XCTAssertEqual(summary.alreadyMissing.compactMap(\.snapshotLamport).sorted(), [10, 20, 30])
    }

    // MARK: - Startup maintenance diagnostics

    /// A non-cancellation startup-maintenance failure must stay best-effort (runner does not throw)
    /// AND be observable in the returned diagnostic. On HEAD the runner returns Void, so the swallowed
    /// failure is invisible — this fails to compile until the diagnostic is exposed.
    func testStartupRunner_nonCancellationFailure_isNonFatalAndVisibleInDiagnostic() async throws {
        let client = try await makeConnectedClient()
        // Fail the repair pass's open-time materialize with a non-cancellation transport error.
        await client.injectListError(.transport, for: RepoLayout.snapshotsDirectoryPath(base: basePath))
        let services = try await makeServices(client: client)

        let diagnostic = try await RepoMaintenanceStartupRunner.runStartupRetentionIfEnabled(
            services: services, mode: .enabled
        )

        XCTAssertTrue(diagnostic.ran, "enabled startup maintenance must record that it ran")
        XCTAssertEqual(diagnostic.failureStage, .repair,
            "a repair-phase non-cancellation failure must be captured at the repair stage")
        XCTAssertNotNil(diagnostic.failureDescription,
            "the swallowed failure must be observable via the diagnostic, not silently dropped")
        let disconnects = await client.disconnectCount
        XCTAssertEqual(disconnects, 0,
            "a non-cancellation failure is best-effort and must not shut down the client")
    }

    /// Cancellation in startup maintenance must stay fatal: rethrow CancellationError and shut down the
    /// owned metadata client exactly once, never captured into a returned diagnostic.
    func testStartupRunner_cancellation_throwsAndShutsDownOwnedMetadataClient() async throws {
        let inner = try await makeConnectedClient()
        // Five snapshots cross the startup threshold so snapshot GC runs; its delete reports cancellation.
        try await writeSnapshotChain(client: inner, highs: [1, 2, 3, 4, 5])
        let wrapped = CancelOnSnapshotDeleteClient(
            inner: inner,
            snapshotsDirPrefix: RepoLayout.snapshotsDirectoryPath(base: basePath)
        )
        let services = try await makeServices(client: inner, metadataClientOverride: wrapped)

        do {
            _ = try await RepoMaintenanceStartupRunner.runStartupRetentionIfEnabled(
                services: services, mode: .enabled
            )
            XCTFail("cancellation in startup maintenance must propagate, not be captured into a diagnostic")
        } catch is CancellationError {
            // expected
        }
        let disconnects = await inner.disconnectCount
        XCTAssertEqual(disconnects, 1,
            "cancellation must shut down owned metadata services exactly once")
    }

    /// Disabled mode must not run any maintenance and must record a no-op diagnostic.
    func testStartupRunner_disabledMode_recordsNoOpDiagnosticAndRunsNothing() async throws {
        let inner = try await makeConnectedClient()
        try await writeSnapshotChain(client: inner, highs: [1, 2, 3, 4, 5])
        let services = try await makeServices(client: inner)

        let diagnostic = try await RepoMaintenanceStartupRunner.runStartupRetentionIfEnabled(
            services: services, mode: .disabled(.test)
        )

        XCTAssertEqual(diagnostic.mode, .disabled(.test))
        XCTAssertFalse(diagnostic.ran, "disabled mode must not run maintenance")
        XCTAssertNil(diagnostic.repairedCount)
        XCTAssertNil(diagnostic.startupResult)
        XCTAssertNil(diagnostic.failureStage)
        // No deletion happened: every snapshot the threshold would have GC'd is still present.
        for lamport in [UInt64(10), 20, 30, 40, 50] {
            let path = RepoLayout.snapshotFilePath(
                base: basePath, month: monthKey, lamport: lamport, writerID: writerID, runID: runID
            )
            let present = await inner.hasFile(path)
            XCTAssertTrue(present, "disabled startup maintenance must not delete snapshot @\(lamport)")
        }
    }

    /// Successful startup maintenance preserves existing compaction/repair behavior and records success.
    func testStartupRunner_success_preservesBehaviorAndRecordsSuccess() async throws {
        let client = try await makeClientWithBaselineAndCommits(replayCount: 6)
        let services = try await makeServices(client: client)

        let diagnostic = try await RepoMaintenanceStartupRunner.runStartupRetentionIfEnabled(
            services: services, mode: .enabled
        )

        XCTAssertTrue(diagnostic.ran)
        XCTAssertNil(diagnostic.failureStage, "a successful pass records no failure")
        XCTAssertNil(diagnostic.failureDescription)
        XCTAssertEqual(diagnostic.repairedCount, 0, "no corrupt-snapshot months means no repairs")
        let startup = try XCTUnwrap(diagnostic.startupResult,
            "successful startup maintenance must record its result")
        let monthResult = try XCTUnwrap(startup.monthResults[monthKey],
            "startup compaction must process the checkpoint-eligible month, as before")
        XCTAssertEqual(monthResult.snapshotGC, .skipped(.skippedBelowThreshold),
            "diagnostic must preserve the existing below-threshold snapshot-GC behavior")
    }

    // MARK: - Helpers

    func testCompactionSkipsMonthWhenAssetFingerprintDoesNotMatchLinkSet() async throws {
        let client = try await makeConnectedClient()
        // A forged/foreign commit: a syntactically valid 32-byte fingerprint that is NOT the recompute of
        // its own (role, slot, contentHash) link set. The materializer still folds the month .clean
        // (structural-trust model unchanged); the compaction gate is what must refuse destructive maintenance.
        let mismatchedFP = TestFixtures.assetFingerprint(0x7E)
        let hash = TestFixtures.fingerprint(0x7F)
        let path = String(format: "%04d/%02d/forged.jpg", year, monthValue)
        let op = CommitOp(opSeq: 0, clock: 1, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: mismatchedFP,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [CommitResourceEntry(
                physicalRemotePath: path, logicalName: "forged.jpg", contentHash: hash,
                fileSize: 100, resourceType: ResourceTypeCode.photo, role: ResourceTypeCode.photo, slot: 0, crypto: nil
            )]
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerID, seq: 1, runID: runID, month: monthKey, clockMin: 1, clockMax: 1
            ),
            ops: [op], month: monthKey, respectTaskCancellation: true
        )
        let services = try await makeServices(client: client)

        let outcome = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID).outcomeByMonth[monthKey]
        XCTAssertEqual(outcome, .clean, "the structural-trust materialize is unchanged; mismatch is a maintenance gate")

        let result = try await RepoCompactionService(services: services).compactMonthForUserMaintenance(monthKey)

        XCTAssertEqual(result.checkpoint.outcome, .skippedEmptyFold,
                       "a fingerprint/link-set mismatch must skip checkpoint so the forged identity is not baselined")
        XCTAssertNil(result.commitCleanup, "commit GC must not delete commits on a fingerprint-mismatched month")
        let snapshotCount = await client.snapshotFiles().keys.filter { $0.contains("/.watermelon/snapshots/") }.count
        XCTAssertEqual(snapshotCount, 0, "no checkpoint baseline should be written for a mismatched month")
    }

    private func makeConnectedClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        return client
    }

    /// Writes a snapshot covering [low,high] whose body carries asset `fp` (stamped at `stampSeq`) with
    /// exactly the given per-(role,slot) resources/links. Used to build full vs. under-representing
    /// (partial) accepted bodies that share the same fingerprint/stamp.
    private func writeAssetSnapshot(
        client: InMemoryRemoteStorageClient,
        low: UInt64,
        high: UInt64,
        lamport: UInt64,
        fp: AssetFingerprint,
        stampSeq: UInt64,
        logicalName: String = "asset.jpg",
        fileSize: Int64 = 100,
        creationDateMs: Int64? = nil,
        resourceStampSeq: UInt64? = nil,
        resourceBackedUpAtMs: Int64? = nil,
        resources: [(role: Int, slot: Int, path: String, hash: Data)]
    ) async throws {
        let stamp = OpStamp(writerID: writerID, seq: stampSeq, clock: stampSeq)
        let resourceStampValue = resourceStampSeq ?? stampSeq
        let resourceStamp = OpStamp(writerID: writerID, seq: resourceStampValue, clock: resourceStampValue)
        // Identity must recompute from the link set or the materializer's fingerprint/link-set gate folds
        // the month non-clean. Derive `fp` from the resources rather than trusting the opaque parameter.
        _ = fp
        let canonicalFP = TestFixtures.computedFingerprint(
            for: resources.map { (role: $0.role, slot: $0.slot, contentHash: $0.hash) }
        )
        var state = RepoMonthState.empty
        var totalSize: Int64 = 0
        for r in resources {
            state.resources[RemotePhysicalPathKey(r.path)] = SnapshotResourceRow(
                physicalRemotePath: r.path, contentHash: r.hash, fileSize: fileSize,
                resourceType: ResourceTypeCode.photo, creationDateMs: creationDateMs,
                backedUpAtMs: resourceBackedUpAtMs ?? Int64(stampSeq),
                crypto: nil, stamp: resourceStamp
            )
            state.assetResources[AssetResourceKey(assetFingerprint: canonicalFP, role: r.role, slot: r.slot)] =
                SnapshotAssetResourceRow(
                    assetFingerprint: canonicalFP, role: r.role, slot: r.slot, resourceHash: r.hash, logicalName: logicalName
                )
            totalSize += fileSize
        }
        state.assets[canonicalFP] = SnapshotAssetRow(
            assetFingerprint: canonicalFP, creationDateMs: creationDateMs, backedUpAtMs: Int64(stampSeq),
            resourceCount: resources.count, totalFileSizeBytes: totalSize, stamp: stamp
        )
        let covered = CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: low, high: high)]])
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion, scope: CommitHeader.monthScope(monthKey),
            writerID: writerID, repoID: repoID, covered: covered, createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: state)
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header, assets: parts.assets, resources: parts.resources,
            assetResources: parts.assetResources, deletedKeys: parts.deletedKeys,
            month: monthKey, lamport: lamport, runID: runID, respectTaskCancellation: true
        )
    }

    /// Writes an addAsset commit at `seq` carrying `fp` with the given per-(role,slot) resources/links.
    private func writeMultiResourceCommit(
        client: InMemoryRemoteStorageClient,
        seq: UInt64,
        fp: AssetFingerprint,
        logicalName: String = "asset.jpg",
        fileSize: Int64 = 100,
        creationDateMs: Int64? = nil,
        resources: [(role: Int, slot: Int, path: String, hash: Data)]
    ) async throws {
        let entries = resources.map { r in
            CommitResourceEntry(
                physicalRemotePath: r.path, logicalName: logicalName, contentHash: r.hash,
                fileSize: fileSize, resourceType: ResourceTypeCode.photo, role: r.role, slot: r.slot, crypto: nil
            )
        }
        _ = fp
        let canonicalFP = TestFixtures.computedFingerprint(
            for: resources.map { (role: $0.role, slot: $0.slot, contentHash: $0.hash) }
        )
        let op = CommitOp(opSeq: 0, clock: seq, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: canonicalFP, creationDateMs: creationDateMs, backedUpAtMs: Int64(seq), resources: entries
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerID, seq: seq, runID: runID,
                month: monthKey, clockMin: seq, clockMax: seq
            ),
            ops: [op], month: monthKey, respectTaskCancellation: true
        )
    }

    /// Writes one empty snapshot covering seq [low,high] for `writer` at `lamport`.
    private func writeChainSnapshot(
        client: InMemoryRemoteStorageClient,
        low: UInt64 = 1,
        high: UInt64,
        lamport: UInt64,
        writer: String? = nil,
        runID overrideRunID: String? = nil
    ) async throws {
        let wID = writer ?? writerID
        let rID = overrideRunID ?? runID
        let covered = CoveredRanges(rangesByWriter: [wID: [ClosedSeqRange(low: low, high: high)]])
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(monthKey),
            writerID: wID,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: .empty)
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: monthKey,
            lamport: lamport,
            runID: rID,
            respectTaskCancellation: true
        )
    }

    /// Writes a snapshot covering [low,high] whose body carries one real asset (with its resource +
    /// link) stamped at `high`. Used to build a dominated-but-body-bearing snapshot that coverage-only
    /// authority would wrongly delete under an empty covered-max baseline.
    private func writeBodiedSnapshot(
        client: InMemoryRemoteStorageClient,
        low: UInt64,
        high: UInt64,
        lamport: UInt64,
        assetByte: UInt8
    ) async throws {
        let hash = TestFixtures.fingerprint(assetByte &+ 1)
        let fp = TestFixtures.computedFingerprint(for: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)])
        let path = String(format: "%04d/%02d/asset-%02x.jpg", year, monthValue, assetByte)
        let stamp = OpStamp(writerID: writerID, seq: high, clock: high)
        var state = RepoMonthState.empty
        state.assets[fp] = SnapshotAssetRow(
            assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: Int64(high),
            resourceCount: 1, totalFileSizeBytes: 100, stamp: stamp
        )
        state.resources[RemotePhysicalPathKey(path)] = SnapshotResourceRow(
            physicalRemotePath: path, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: Int64(high),
            crypto: nil, stamp: stamp
        )
        state.assetResources[AssetResourceKey(assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0)] =
            SnapshotAssetResourceRow(
                assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0,
                resourceHash: hash, logicalName: "asset.jpg"
            )
        let covered = CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: low, high: high)]])
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(monthKey),
            writerID: writerID,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: state)
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: monthKey,
            lamport: lamport,
            runID: runID,
            respectTaskCancellation: true
        )
    }

    /// Writes a nested chain of snapshots covering [1,high] for each high, with lamport = high*10
    /// so the highest-covered snapshot is also the newest (the covered-max accepted baseline).
    private func writeSnapshotChain(client: InMemoryRemoteStorageClient, highs: [UInt64]) async throws {
        for high in highs {
            try await writeChainSnapshot(client: client, high: high, lamport: high * 10)
        }
    }

    private func writeCommits(client: InMemoryRemoteStorageClient, seqs: ClosedRange<UInt64>) async throws {
        for seq in seqs {
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }
    }

    /// Writes an accepted snapshot covering seq [1,3] then `replayCount` additional commits
    /// at seq 4..(4+replayCount-1). Default threshold is 5, so replayCount >= 5 triggers recommendation.
    private func makeClientWithBaselineAndCommits(replayCount: Int) async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Post-delete verification reads the remote identity, so a realistic repo needs both files.
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)

        // Accepted baseline snapshot covering seq [1,3].
        let covered = CoveredRanges(rangesByWriter: [
            writerID: [ClosedSeqRange(low: 1, high: 3)]
        ])
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(monthKey),
            writerID: writerID,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: .empty)
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: monthKey,
            lamport: 10,
            runID: runID,
            respectTaskCancellation: true
        )

        // Additional commits beyond the baseline covered range.
        for i in 0..<replayCount {
            let seq = UInt64(4 + i)
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }

        return client
    }

    private func writeAddCommit(
        client: InMemoryRemoteStorageClient,
        seq: UInt64,
        clock: UInt64,
        assetByte: UInt8,
        path: String? = nil,
        hash: Data? = nil,
        writer: String? = nil
    ) async throws {
        let wID = writer ?? writerID
        let resourceHash = hash ?? TestFixtures.fingerprint(assetByte &+ 1)
        let assetFP = TestFixtures.computedFingerprint(for: [(role: ResourceTypeCode.photo, slot: 0, contentHash: resourceHash)])
        let resources = [CommitResourceEntry(
            physicalRemotePath: path ?? String(format: "%04d/%02d/asset-%02x.jpg", year, monthValue, assetByte),
            logicalName: "asset.jpg",
            contentHash: resourceHash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            role: ResourceTypeCode.photo,
            slot: 0,
            crypto: nil
        )]
        let op = CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: assetFP,
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: resources
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: wID,
                seq: seq,
                runID: runID,
                month: monthKey,
                clockMin: clock,
                clockMax: clock
            ),
            ops: [op],
            month: monthKey,
            respectTaskCancellation: true
        )
    }

    private static let testPolicy = RepoCompactionPolicy(
        checkpointCommitThreshold: 5,
        checkpointByteThreshold: Int64.max,
        snapshotFallbackKeepCount: 2,
        snapshotGCMarginFileCount: 2
    )

    private func makeServices(
        client: InMemoryRemoteStorageClient,
        initialMaterialize: RepoMaterializer.MaterializeOutput? = nil,
        metadataClientOverride: (any RemoteStorageClientProtocol)? = nil
    ) async throws -> BackupV2RuntimeServices {
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav
        )
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: repoID,
            runID: runID,
            basePath: basePath,
            postOpenSyncInspection: .v2(formatVersion: RepoLayout.currentSupportedFormatVersion),
            database: databaseManager,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: commitWriter,
            snapshotWriter: snapshotWriter,
            compactionPolicy: Self.testPolicy,
            isLocalVolume: true,
            metadataClient: metadataClientOverride ?? client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(initialMaterialize),
        )
    }
}

/// Forwards everything to the inner client but makes `delete` of any commits-directory path report
/// cancellation, so the commit-GC executor maps it to a `.deleteFailed(_, .cancelled)` stop reason —
/// deterministically exercising the commit-GC cancellation path without racing Task.cancel().
private struct CancelOnCommitDeleteClient: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let commitsDirPrefix: String

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }

    func delete(path: String) async throws {
        if path.hasPrefix(commitsDirPrefix) { throw CancellationError() }
        try await inner.delete(path: path)
    }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Forwards everything to the inner client but makes `delete` of any snapshot-directory path report
/// cancellation, so the snapshot-GC executor maps it to a `.deleteFailed(_, .cancelled)` stop reason —
/// deterministically exercising the snapshot-GC cancellation path without racing Task.cancel().
private struct CancelOnSnapshotDeleteClient: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let snapshotsDirPrefix: String

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }

    func delete(path: String) async throws {
        if path.hasPrefix(snapshotsDirPrefix) { throw CancellationError() }
        try await inner.delete(path: path)
    }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Forwards everything to the inner client but makes the SECOND `list` of the commits directory throw
/// cancellation. The first commits-dir list is `materializeMonth`'s; the second is the compaction
/// planner's, so this deterministically exercises cancellation inside `RepoCompactionPlanner.makeReport`
/// without racing Task.cancel().
private struct CancelOnSecondCommitsListClient: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let commitsDir: String
    private let commitsListCount = OSAllocatedUnfairLock(initialState: 0)

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        if path == commitsDir {
            let count = commitsListCount.withLock { state -> Int in
                state += 1
                return state
            }
            if count >= 2 { throw CancellationError() }
        }
        return try await inner.list(path: path)
    }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

private struct SnapshotDownloadNotFoundAfterCommitDeleteClient: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let targetSnapshotPath: String
    let commitsDirPrefix: String
    private let armed = OSAllocatedUnfairLock(initialState: false)

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }

    func delete(path: String) async throws {
        try await inner.delete(path: path)
        if path.hasPrefix(commitsDirPrefix) {
            armed.withLock { $0 = true }
        }
    }

    func download(remotePath: String, localURL: URL) async throws {
        if remotePath == targetSnapshotPath, armed.withLock({ $0 }) {
            throw RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileNoSuchFileError,
                userInfo: [NSLocalizedDescriptionKey: "no such file"]
            ))
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }

    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Forwards everything to the inner client, but for `delete` of any path under `dirPrefix` it performs the
/// real delete and THEN reports not-found — so the metadata-delete transaction classifies the candidate as
/// `alreadyMissing` while the file is genuinely gone, leaving post-delete verification intact.
private struct DeleteThenReportNotFoundClient: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let dirPrefix: String

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }

    func delete(path: String) async throws {
        try await inner.delete(path: path)
        if path.hasPrefix(dirPrefix) {
            throw RemoteStorageClientError.underlying(NSError(
                domain: NSCocoaErrorDomain,
                code: NSFileNoSuchFileError,
                userInfo: [NSLocalizedDescriptionKey: "no such file"]
            ))
        }
    }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

import Foundation
import XCTest
@testable import Watermelon

final class RetentionMaintenanceOrchestratorTests: XCTestCase {

    // MARK: - runMonthCommitPrefixDelete integration

    func testRunMonthCommitPrefixDelete_HappyPath_DelegatesToExecutorAndReturnsCompleted() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xA1)
        let services = try await makeServices(
            client: client,
            policy: policy(checkpointCommitThreshold: 99, retentionStalenessThresholdSeconds: 60)
        )
        _ = try await writeCheckpointBarrier(client: client, services: services, createdAtMs: 1)

        let orchestrator = RetentionMaintenanceOrchestrator(services: services, nowMs: { 120_000 })
        let result = try await orchestrator.runMonthCommitPrefixDelete(month: month)

        guard case .completed(let summary, _, .passed(_)) = result else {
            return XCTFail("expected completed deletion, got \(result)")
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
    }

    func testRunMonthCommitPrefixDelete_PreflightBlocked_ReturnsResultWithoutThrowing() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xA2)
        // No barrier published → preflight produces .emptyBarrierSet, executor returns .preflightBlocked.
        let services = try await makeServices(client: client, policy: policy())
        let orchestrator = RetentionMaintenanceOrchestrator(services: services, nowMs: { 1 })
        let result = try await orchestrator.runMonthCommitPrefixDelete(month: month)

        guard case .preflightBlocked(let blockers, _) = result else {
            return XCTFail("expected preflight blocked, got \(result)")
        }
        XCTAssertTrue(blockers.contains(.emptyBarrierSet))
    }

    func testRunMonthCommitPrefixDelete_CancellationInResult_ThrowsCancellationError() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xA3)
        let services = try await makeServices(client: client, policy: policy())
        let orchestrator = RetentionMaintenanceOrchestrator(services: services, nowMs: { 1 })

        // Pre-cancel the task before invocation so executor.execute throws CancellationError
        // synchronously at its `try Task.checkCancellation()` entry guard. Orchestrator
        // re-throws cleanly. The cancellation-in-result path (executor returns .stopped(.cancelled)
        // without throwing) is covered by the containsCancellation unit tests below.
        let task = Task { try await orchestrator.runMonthCommitPrefixDelete(month: month) }
        task.cancel()

        do {
            _ = try await task.value
            XCTFail("expected cancellation throw")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    // MARK: - runStartupCommitPrefixSweep integration

    func testRunStartupCommitPrefixSweep_HappyPath_IteratesCandidateMonths() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB1)
        let services = try await makeServices(
            client: client,
            policy: policy(checkpointCommitThreshold: 99, retentionStalenessThresholdSeconds: 60)
        )
        _ = try await writeCheckpointBarrier(client: client, services: services, createdAtMs: 1)

        let orchestrator = RetentionMaintenanceOrchestrator(services: services, nowMs: { 120_000 })
        let results = try await orchestrator.runStartupCommitPrefixSweep()

        guard case .completed(let summary, _, .passed(_))? = results[month] else {
            return XCTFail("expected startup sweep deletion, got \(String(describing: results[month]))")
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
    }

    func testRunStartupCommitPrefixSweep_EmptyCandidateList_ReturnsEmptyMap() async throws {
        let client = try await makeClient()
        // No barrier published → candidate-month loader returns empty.
        let services = try await makeServices(client: client, policy: policy(retentionStalenessThresholdSeconds: 60))
        let orchestrator = RetentionMaintenanceOrchestrator(services: services, nowMs: { 120_000 })
        let results = try await orchestrator.runStartupCommitPrefixSweep()
        XCTAssertTrue(results.isEmpty)
    }

    func testRunStartupCommitPrefixSweep_CancellationMidLoop_ThrowsCancellationError() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB2)
        let services = try await makeServices(
            client: client,
            policy: policy(checkpointCommitThreshold: 99, retentionStalenessThresholdSeconds: 60)
        )
        _ = try await writeCheckpointBarrier(client: client, services: services, createdAtMs: 1)

        let orchestrator = RetentionMaintenanceOrchestrator(services: services, nowMs: { 120_000 })
        let task = Task { try await orchestrator.runStartupCommitPrefixSweep() }
        task.cancel()
        do {
            _ = try await task.value
            XCTFail("expected cancellation throw")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    // MARK: - containsCancellation static helper

    func testContainsCancellation_StoppedWithCancelledReason_ReturnsTrue() {
        let result = RepoRetentionCommitDeleteResult.stopped(
            summary: emptySummary(),
            reason: .cancelled(candidate: nil),
            report: emptyReport(),
            verification: nil
        )
        XCTAssertTrue(RetentionMaintenanceOrchestrator.containsCancellation(result))
    }

    func testContainsCancellation_StoppedWithDeleteFailedCancelled_ReturnsTrue() {
        let result = RepoRetentionCommitDeleteResult.stopped(
            summary: emptySummary(),
            reason: .deleteFailed(candidate: makeCandidate(), failure: .cancelled),
            report: emptyReport(),
            verification: nil
        )
        XCTAssertTrue(RetentionMaintenanceOrchestrator.containsCancellation(result))
    }

    func testContainsCancellation_VerificationInconclusiveCancelled_ReturnsTrue() {
        let result = RepoRetentionCommitDeleteResult.verificationInconclusive(
            summary: emptySummary(),
            stopReason: nil,
            report: emptyReport(),
            verification: .inconclusive(reason: .cancelled)
        )
        XCTAssertTrue(RetentionMaintenanceOrchestrator.containsCancellation(result))
    }

    func testContainsCancellation_CompletedNoCancellation_ReturnsFalse() {
        let result = RepoRetentionCommitDeleteResult.completed(
            summary: emptySummary(),
            report: emptyReport(),
            verification: .inconclusive(reason: .materializerReadFailed)
        )
        XCTAssertFalse(RetentionMaintenanceOrchestrator.containsCancellation(result))
    }

    func testContainsCancellation_VerificationFailedNonCancellation_ReturnsFalse() {
        let result = RepoRetentionCommitDeleteResult.verificationFailed(
            summary: emptySummary(),
            stopReason: nil,
            report: emptyReport(),
            verification: .failed(
                reason: .stateNotRetentionSuperset,
                evidence: nil
            )
        )
        XCTAssertFalse(RetentionMaintenanceOrchestrator.containsCancellation(result))
    }

    // MARK: - Fixtures (mirror RepoCheckpointBarrierHookTests for integration parity)

    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerID = "11111111-1111-1111-1111-111111111111"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let month = LibraryMonthKey(year: 2026, month: 5)

    private func emptySummary() -> RepoRetentionCommitDeleteSummary {
        RepoRetentionCommitDeleteSummary(
            month: month,
            repoID: repoID,
            candidateCount: 0
        )
    }

    private func emptyReport() -> RepoRetentionDeletePreflightReport {
        RepoRetentionDeletePreflightReport(
            month: month,
            repoID: repoID,
            mode: .dryRun,
            evaluatedAtMs: 0
        )
    }

    private func makeCandidate() -> RepoRetentionDeleteCandidate {
        RepoRetentionDeleteCandidate(
            filename: "x",
            path: "/x",
            month: month,
            writerID: writerID,
            seq: 1,
            size: 0,
            sha256Hex: "",
            rowCount: 0
        )
    }

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        return client
    }

    private func writeAddCommit(
        client: any RemoteStorageClientProtocol,
        seq: UInt64,
        clock: UInt64,
        assetByte: UInt8
    ) async throws {
        let hash = TestFixtures.fingerprint(assetByte)
        let body = CommitAddAssetBody(
            assetFingerprint: assetFingerprint(hash: hash),
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: []
        )
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
                seq: seq,
                runID: runID,
                month: month,
                clockMin: clock,
                clockMax: clock
            ),
            ops: [CommitOp(opSeq: 0, clock: clock, body: .addAsset(body))],
            month: month,
            respectTaskCancellation: true
        )
    }

    private func assetFingerprint(hash: Data) -> Data {
        BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
    }

    private func makeServices(
        client: any RemoteStorageClientProtocol,
        policy: RepoCompactionPolicy
    ) async throws -> BackupV2RuntimeServices {
        let dbURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("retention-orchestrator-\(UUID().uuidString).sqlite")
        let database = try DatabaseManager(databaseURL: dbURL)
        let profileID = try TestFixtures.insertServerProfile(
            in: database,
            writerID: writerID,
            basePath: basePath,
            storageType: .webdav
        )
        let identity = RepoIdentity(database: database)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: database, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: database, profileID: profileID, repoID: repoID, initial: 0)
        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: repoID,
            runID: runID,
            basePath: basePath,
            postOpenSyncInspection: .v2(formatVersion: RepoLayout.currentSupportedFormatVersion),
            database: database,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: CommitLogWriter(client: client, basePath: basePath),
            snapshotWriter: SnapshotWriter(client: client, basePath: basePath),
            liveness: LivenessTracker(client: client, basePath: basePath, writerID: writerID, isLocalVolume: true),
            compactionPolicy: policy,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }

    @discardableResult
    private func writeCheckpointBarrier(
        client: InMemoryRemoteStorageClient,
        services: BackupV2RuntimeServices,
        createdAtMs: Int64
    ) async throws -> RepoRetentionBarrierPublishResult {
        let checkpoint = try await RepoCheckpointService(
            client: client,
            basePath: basePath,
            repoID: repoID,
            writerID: writerID,
            runID: runID,
            clock: services.lamport,
            policy: services.compactionPolicy
        ).checkpointMonth(month, mode: .force, respectTaskCancellation: true)
        return try await RepoRetentionBarrierService(
            client: client,
            basePath: basePath,
            repoID: repoID,
            writerID: writerID,
            runID: runID,
            policy: services.compactionPolicy,
            nowMs: { createdAtMs }
        ).publishBarrier(for: checkpoint, respectTaskCancellation: true)
    }

    private func policy(
        checkpointCommitThreshold: Int = 1,
        retentionStalenessThresholdSeconds: Int = 86_400
    ) -> RepoCompactionPolicy {
        RepoCompactionPolicy(
            checkpointCommitThreshold: checkpointCommitThreshold,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: retentionStalenessThresholdSeconds,
            snapshotFallbackKeepCount: 2
        )
    }
}

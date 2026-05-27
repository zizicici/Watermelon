import Foundation
import XCTest
@testable import Watermelon

/// Coordinator gating contract: Phase C runs only when Phase B reaches
/// `.preflightBlocked` (no mutation) or `.completed` (verified mutation). Phase B
/// `.stopped` / `.verificationFailed` / `.verificationInconclusive` must skip
/// Phase C with the matching typed disposition. The five Phase-B outcomes share
/// the same gate, so we cover the two "allow" branches and one "block" branch;
/// the other two block-branches reuse the same code path verified by `.stopped`.
final class RepoMaintenanceCoordinatorTests: XCTestCase {

    // MARK: - Phase B .preflightBlocked → Phase C runs

    func testPhaseB_PreflightBlocked_PhaseC_Runs() async throws {
        let client = try await makeClient()
        // No barrier published → Phase B preflight returns `.emptyBarrierSet`.
        // No commits either → Phase A skips with `.skippedEmptyFold`.
        let services = try await makeServices(client: client, policy: makePolicy())

        let result = try await RepoMaintenanceCoordinator(services: services, nowMs: { 1 })
            .runForMonth(month)

        XCTAssertEqual(result.month, month)
        XCTAssertEqual(result.outcome, .skippedEmptyFold)
        guard case .preflightBlocked? = result.commitCleanup else {
            return XCTFail("expected Phase B .preflightBlocked, got \(String(describing: result.commitCleanup))")
        }
        // Phase C MUST run because Phase B did not mutate.
        guard case .ran(let gc) = result.snapshotGC else {
            return XCTFail("expected Phase C to run, got \(result.snapshotGC)")
        }
        // Without a barrier or accepted baseline, Phase C also blocks at preflight.
        guard case .preflightBlocked = gc else {
            return XCTFail("expected Phase C .preflightBlocked, got \(gc)")
        }
    }

    // MARK: - Phase B .completed → Phase C runs

    func testPhaseB_Completed_PhaseC_Runs() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xA1)
        let services = try await makeServices(
            client: client,
            policy: makePolicy(checkpointCommitThreshold: 99, retentionStalenessThresholdSeconds: 60)
        )
        _ = try await writeCheckpointBarrier(client: client, services: services, createdAtMs: 1)

        let result = try await RepoMaintenanceCoordinator(services: services, nowMs: { 120_000 })
            .runForMonth(month)

        guard case .completed = result.commitCleanup else {
            return XCTFail("expected Phase B .completed, got \(String(describing: result.commitCleanup))")
        }
        guard case .ran = result.snapshotGC else {
            return XCTFail("expected Phase C to run, got \(result.snapshotGC)")
        }
    }

    // MARK: - Phase B .stopped → Phase C skipped

    func testPhaseB_Stopped_PhaseC_SkippedWithTypedReason() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xA2)
        let services = try await makeServices(
            client: client,
            policy: makePolicy(checkpointCommitThreshold: 99, retentionStalenessThresholdSeconds: 60)
        )
        _ = try await writeCheckpointBarrier(client: client, services: services, createdAtMs: 1)
        // Inject a delete failure on the commit file path so Phase B reaches `.stopped`
        // rather than `.completed`.
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectDeleteError(.transport, for: commitPath)

        let result = try await RepoMaintenanceCoordinator(services: services, nowMs: { 120_000 })
            .runForMonth(month)

        guard case .stopped = result.commitCleanup else {
            return XCTFail("expected Phase B .stopped, got \(String(describing: result.commitCleanup))")
        }
        guard case .skipped(.skippedAfterCommitCleanupStopped) = result.snapshotGC else {
            return XCTFail("expected Phase C skippedAfterCommitCleanupStopped, got \(result.snapshotGC)")
        }
    }

    // MARK: - Phase B .verificationFailed → Phase C skipped (override seam)

    func testPhaseB_VerificationFailed_PhaseC_SkippedWithTypedReason() async throws {
        let client = try await makeClient()
        let services = try await makeServices(client: client, policy: makePolicy())

        let synthesized: RepoRetentionCommitDeleteResult = .verificationFailed(
            summary: emptySummary(),
            stopReason: nil,
            report: emptyReport(nowMs: 1),
            verification: .failed(reason: .coveredRangeRegression, evidence: nil)
        )

        let result = try await RepoMaintenanceCoordinator(
            services: services,
            nowMs: { 1 },
            commitCleanupOverride: { _ in synthesized }
        ).runForMonth(month)

        guard case .verificationFailed = result.commitCleanup else {
            return XCTFail("expected Phase B .verificationFailed, got \(String(describing: result.commitCleanup))")
        }
        guard case .skipped(.skippedAfterCommitCleanupVerificationFailed) = result.snapshotGC else {
            return XCTFail("expected Phase C skippedAfterCommitCleanupVerificationFailed, got \(result.snapshotGC)")
        }
    }

    // MARK: - Phase B .verificationInconclusive → Phase C skipped (override seam)

    func testPhaseB_VerificationInconclusive_PhaseC_SkippedWithTypedReason() async throws {
        let client = try await makeClient()
        let services = try await makeServices(client: client, policy: makePolicy())

        let synthesized: RepoRetentionCommitDeleteResult = .verificationInconclusive(
            summary: emptySummary(),
            stopReason: nil,
            report: emptyReport(nowMs: 1),
            verification: .inconclusive(reason: .materializerReadFailed)
        )

        let result = try await RepoMaintenanceCoordinator(
            services: services,
            nowMs: { 1 },
            commitCleanupOverride: { _ in synthesized }
        ).runForMonth(month)

        guard case .verificationInconclusive = result.commitCleanup else {
            return XCTFail("expected Phase B .verificationInconclusive, got \(String(describing: result.commitCleanup))")
        }
        guard case .skipped(.skippedAfterCommitCleanupVerificationInconclusive) = result.snapshotGC else {
            return XCTFail("expected Phase C skippedAfterCommitCleanupVerificationInconclusive, got \(result.snapshotGC)")
        }
    }

    // MARK: - Override-seam fixture helpers

    private func emptySummary() -> RepoRetentionCommitDeleteSummary {
        RepoRetentionCommitDeleteSummary(month: month, repoID: repoID, candidateCount: 0)
    }

    private func emptyReport(nowMs: Int64) -> RepoRetentionDeletePreflightReport {
        RepoRetentionDeletePreflightReport(
            month: month,
            repoID: repoID,
            mode: .dryRun,
            evaluatedAtMs: nowMs
        )
    }

    // MARK: - Fixtures (mirror RetentionMaintenanceOrchestratorTests)

    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerID = "11111111-1111-1111-1111-111111111111"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let month = LibraryMonthKey(year: 2026, month: 5)

    private func makePolicy(
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

    private func assetFingerprint(hash: Data) -> AssetFingerprint {
        BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
    }

    private func makeServices(
        client: any RemoteStorageClientProtocol,
        policy: RepoCompactionPolicy
    ) async throws -> BackupV2RuntimeServices {
        let dbURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("coordinator-\(UUID().uuidString).sqlite")
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
}

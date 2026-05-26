import XCTest
@testable import Watermelon

final class RetentionDeletionSafetyGateTests: XCTestCase {
    private let nowMs: Int64 = 1_800_000_000_000
    private let writerA = "11111111-1111-1111-1111-111111111111"
    private let writerB = "22222222-2222-2222-2222-222222222222"

    func testEmptyCompleteViewAllowsDeletionSafetyDecision() {
        let decision = evaluate(peers: [])

        XCTAssertTrue(decision.allowed)
        XCTAssertEqual(decision.blockers, [])
        XCTAssertEqual(decision.evaluatedAtMs, nowMs)
    }

    func testLocalVolumeViewAllowsWithoutLivenessBlockers() {
        let decision = evaluate(
            peers: [peer(writerA, .unknown(reason: .readFailed), nil)],
            isLocalVolume: true
        )

        XCTAssertTrue(decision.allowed)
        XCTAssertEqual(decision.blockers, [])
    }

    func testActiveCapablePeerBlocks() {
        let decision = evaluate(peers: [
            peer(writerA, .active(lastSeenMs: nowMs - 1_000), capable)
        ])

        XCTAssertEqual(decision.blockers, [.activePeer(writerID: writerA)])
        XCTAssertFalse(decision.allowed)
    }

    func testActivePeerStillBlocksWhenManifestDoesNotRequireNoActiveWriters() {
        let decision = evaluate(
            peers: [peer(writerA, .active(lastSeenMs: nowMs - 1_000), capable)],
            manifestGate: gate(requiredNoActive: false)
        )

        XCTAssertEqual(decision.blockers, [.activePeer(writerID: writerA)])
    }

    func testUnknownPeerAndIncompleteViewBlock() {
        let decision = evaluate(peers: [
            peer(writerA, .unknown(reason: .readFailed), nil)
        ])

        XCTAssertEqual(decision.blockers, [
            .incompleteView,
            .unknownPeer(writerID: writerA)
        ])
    }

    func testListLevelIncompleteViewBlocksWhenRequired() {
        let decision = evaluate(
            view: RetentionPeerStatusView(peers: [], listComplete: false),
            manifestGate: gate(requiredComplete: true)
        )

        XCTAssertEqual(decision.blockers, [.incompleteView])
    }

    func testListLevelIncompleteViewDoesNotBlockWhenNotRequired() {
        let decision = evaluate(
            view: RetentionPeerStatusView(peers: [], listComplete: false),
            manifestGate: gate(requiredComplete: false)
        )

        XCTAssertTrue(decision.allowed)
    }

    func testStaleCapablePeerWithinRetentionThresholdBlocks() {
        let decision = evaluate(peers: [
            peer(writerA, .stale(lastSeenMs: nowMs - msMinutes(6)), capable)
        ])

        XCTAssertEqual(decision.blockers, [
            .stalePeerWithinRetentionThreshold(writerID: writerA, lastSeenMs: nowMs - msMinutes(6))
        ])
    }

    func testStaleCapablePeerAfterRetentionThresholdAllows() {
        let policy = policy(retentionSeconds: secDays(1))
        let decision = evaluate(
            peers: [peer(writerA, .stale(lastSeenMs: nowMs - msDays(1)), capable)],
            policy: policy
        )

        XCTAssertTrue(decision.allowed)
    }

    func testStaleLegacyPeerBlocks() {
        let decision = evaluate(peers: [
            peer(writerA, .stale(lastSeenMs: nowMs - msDays(3)), nil)
        ])

        XCTAssertEqual(decision.blockers, [
            .legacyPeer(writerID: writerA, lastSeenMs: nowMs - msDays(3))
        ])
    }

    func testStaleLegacyPeerAfterGraceAllows() {
        let decision = evaluate(peers: [
            peer(writerA, .stale(lastSeenMs: nowMs - msDays(8)), nil)
        ])

        XCTAssertTrue(decision.allowed)
    }

    func testManifestGateCanRaiseUnknownCapabilityGrace() {
        let decision = evaluate(
            peers: [peer(writerA, .stale(lastSeenMs: nowMs - msDays(10)), nil)],
            manifestGate: gate(unknownCapabilityGraceMs: msDays(14))
        )

        XCTAssertEqual(decision.blockers, [
            .legacyPeer(writerID: writerA, lastSeenMs: nowMs - msDays(10))
        ])
    }

    func testUnsupportedAndMalformedCapabilityBlockAsLegacyPeers() throws {
        let unsupported = try LivenessHeartbeat.decode(data([
            "ts": nowMs - msDays(3),
            "retention": [
                "version": 2,
                "barrier_aware_session_refresh": true,
                "checkpoint_barrier_hook": true
            ]
        ]))
        let malformed = try LivenessHeartbeat.decode(data([
            "ts": nowMs - msDays(3),
            "retention": [
                "version": 1,
                "barrier_aware_session_refresh": true
            ]
        ]))

        let decision = evaluate(peers: [
            peer(writerA, .stale(lastSeenMs: unsupported.timestampMs), unsupported.retention),
            peer(writerB, .stale(lastSeenMs: malformed.timestampMs), malformed.retention)
        ])

        XCTAssertEqual(decision.blockers, [
            .legacyPeer(writerID: writerA, lastSeenMs: nowMs - msDays(3)),
            .legacyPeer(writerID: writerB, lastSeenMs: nowMs - msDays(3))
        ])
    }

    func testDecisionIsEquatable() {
        let lhs = evaluate(peers: [peer(writerA, .active(lastSeenMs: nowMs), capable)])
        let rhs = evaluate(peers: [peer(writerA, .active(lastSeenMs: nowMs), capable)])

        XCTAssertEqual(lhs, rhs)
    }

    func testDeletionProductionEnablementStaysBehindSafetyGateAndOrchestrator() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let gateSource = try source(root, "Shared/Services/Repo/RetentionDeletionSafetyGate.swift")
        XCTAssertFalse(gateSource.contains("client.delete("))
        XCTAssertFalse(gateSource.contains("LivenessTracker.staleThreshold"))
        XCTAssertFalse(gateSource.contains("300"))
        XCTAssertFalse(gateSource.contains("300000"))

        let orchestratorSource = try source(root, "Shared/Services/Repo/RetentionMaintenanceOrchestrator.swift")
        XCTAssertTrue(orchestratorSource.contains("RepoRetentionCommitDeleteExecutor("))

        for path in try FileManager.default.subpathsOfDirectory(atPath: root.path)
        where path.hasSuffix(".swift") && !path.hasPrefix("WatermelonTests/") {
            let text = try source(root, path)
            XCTAssertFalse(text.contains("commitPrefixDeletionEnabled"), "commitPrefixDeletionEnabled unexpectedly present in \(path)")
        }

        // Production backup runtime callers must not carry maintenance-off overrides.
        // The lease (Shared/Services/Repo/BackupV2RuntimeLease.swift) is the single
        // place that selects maintenanceStartupMode, including the verify factory's
        // .disabled(.verifyMonthTombstoneApply) value.
        let productionBuildCallers = [
            "Watermelon/Services/Backup/BackupRunPreparation.swift",
            "Watermelon/Services/Backup/BackgroundBackupRunner.swift"
        ]
        for path in productionBuildCallers {
            let text = try source(root, path)
            XCTAssertFalse(text.contains("retentionRuntimeMode:"), "retentionRuntimeMode unexpectedly passed in \(path)")
            XCTAssertFalse(text.contains("RepoRetentionRuntimeMode"), "retention runtime mode unexpectedly referenced in \(path)")
            XCTAssertFalse(text.contains("runMaintenanceTasks:"), "runMaintenanceTasks unexpectedly passed in \(path)")
            XCTAssertFalse(text.contains("maintenanceStartupMode:"), "production caller unexpectedly overrides maintenance startup in \(path); the lease owns this axis now")
        }
        let lease = try source(root, "Shared/Services/Repo/BackupV2RuntimeLease.swift")
        XCTAssertEqual(
            lease.components(separatedBy: "maintenanceStartupMode: .disabled(.verifyMonthTombstoneApply)").count - 1,
            1
        )
        XCTAssertFalse(lease.contains("maintenanceStartupMode: .disabled(.test)"))
        XCTAssertFalse(lease.contains("retentionRuntimeMode:"))
        XCTAssertFalse(lease.contains("runMaintenanceTasks:"))

        for path in [
            "Shared/Services/Backup/V2MonthSession.swift",
            "Shared/Services/Backup/V2RetentionBarrierRefresh.swift",
            "Shared/Services/Repo/OrphanMetadataCleanup.swift",
            "Shared/Services/Repo/RepoCheckpointBarrierHook.swift"
        ] {
            let text = try source(root, path)
            XCTAssertFalse(text.contains("RetentionDeletionSafetyGate"), "gate unexpectedly wired in \(path)")
        }
    }

    private var capable: RetentionPeerCapability {
        RetentionPeerCapability(barrierAwareSessionRefresh: true, checkpointBarrierHook: false)
    }

    private func evaluate(
        peers: [RetentionPeerStatus],
        policy: RepoCompactionPolicy = .default,
        manifestGate: RetentionLivenessGate? = nil,
        isLocalVolume: Bool = false
    ) -> RetentionDeletionSafetyDecision {
        evaluate(
            view: RetentionPeerStatusView(peers: peers, listComplete: true),
            policy: policy,
            manifestGate: manifestGate,
            isLocalVolume: isLocalVolume
        )
    }

    private func evaluate(
        view: RetentionPeerStatusView,
        policy: RepoCompactionPolicy = .default,
        manifestGate: RetentionLivenessGate? = nil,
        isLocalVolume: Bool = false
    ) -> RetentionDeletionSafetyDecision {
        RetentionDeletionSafetyGate.evaluate(
            peerStatusView: view,
            policy: policy,
            manifestGate: manifestGate ?? gate(),
            nowMs: nowMs,
            isLocalVolume: isLocalVolume
        )
    }

    private func peer(
        _ writerID: String,
        _ status: LivenessTracker.PeerStatus,
        _ capability: RetentionPeerCapability?
    ) -> RetentionPeerStatus {
        RetentionPeerStatus(writerID: writerID, status: status, capability: capability)
    }

    private func policy(
        retentionSeconds: Int = BackupV2Constants.retentionStalenessThresholdSeconds
    ) -> RepoCompactionPolicy {
        RepoCompactionPolicy(
            checkpointCommitThreshold: 1,
            checkpointByteThreshold: 1,
            minimumCheckpointIntervalSeconds: 1,
            retentionStalenessThresholdSeconds: retentionSeconds,
            snapshotFallbackKeepCount: 2
        )
    }

    private func gate(
        requiredComplete: Bool = true,
        requiredNoActive: Bool = true,
        unknownCapabilityGraceMs: Int64 = Int64(BackupV2Constants.unknownRetentionCapabilityGraceSeconds) * 1000
    ) -> RetentionLivenessGate {
        RetentionLivenessGate(
            requiredCompleteView: requiredComplete,
            requiredNoActiveNonSelfWriters: requiredNoActive,
            legacyClientGraceMs: unknownCapabilityGraceMs
        )
    }

    private func data(_ object: [String: Any]) throws -> Data {
        try JSONSerialization.data(withJSONObject: object)
    }

    private func source(_ root: URL, _ path: String) throws -> String {
        try String(contentsOf: root.appendingPathComponent(path), encoding: .utf8)
    }
}

private func msMinutes(_ value: Int) -> Int64 {
    Int64(value) * 60 * 1_000
}

private func secDays(_ value: Int) -> Int {
    value * 24 * 60 * 60
}

private func msDays(_ value: Int) -> Int64 {
    Int64(secDays(value)) * 1_000
}

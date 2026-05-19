import XCTest
@testable import Watermelon

final class RetentionLivenessCapabilityTests: XCTestCase {
    private let basePath = "/repo"
    private let selfWriter = "00000000-0000-0000-0000-000000000000"
    private let peerWriter = "11111111-1111-1111-1111-111111111111"
    private let staleWriter = "22222222-2222-2222-2222-222222222222"
    private let corruptWriter = "33333333-3333-3333-3333-333333333333"
    private let vanishedWriter = "44444444-4444-4444-4444-444444444444"

    func testDecodeLegacyHeartbeat() throws {
        let heartbeat = try LivenessHeartbeat.decode(data(["ts": 123]))

        XCTAssertEqual(heartbeat, LivenessHeartbeat(timestampMs: 123, retention: nil))
    }

    func testDecodeNewHeartbeatWithCapability() throws {
        let heartbeat = try LivenessHeartbeat.decode(data([
            "ts": 123,
            "retention": [
                "version": 1,
                "barrier_aware_session_refresh": true,
                "checkpoint_barrier_hook": false
            ]
        ]))

        XCTAssertEqual(heartbeat.retention, RetentionPeerCapability(
            barrierAwareSessionRefresh: true,
            checkpointBarrierHook: false
        ))
    }

    func testDecodeIgnoresUnknownFields() throws {
        let heartbeat = try LivenessHeartbeat.decode(data([
            "ts": 123,
            "future": "ignored",
            "retention": [
                "version": 1,
                "barrier_aware_session_refresh": true,
                "checkpoint_barrier_hook": true,
                "extra": 42
            ]
        ]))

        XCTAssertEqual(heartbeat.retention, RetentionPeerCapability(
            barrierAwareSessionRefresh: true,
            checkpointBarrierHook: true
        ))
    }

    func testDecodeUnsupportedOrMalformedRetentionAsMissingCapability() throws {
        let unsupported = try LivenessHeartbeat.decode(data([
            "ts": 123,
            "retention": [
                "version": 2,
                "barrier_aware_session_refresh": true,
                "checkpoint_barrier_hook": true
            ]
        ]))
        let malformed = try LivenessHeartbeat.decode(data([
            "ts": 123,
            "retention": [
                "version": 1,
                "barrier_aware_session_refresh": "yes",
                "checkpoint_barrier_hook": true
            ]
        ]))
        let wrongType = try LivenessHeartbeat.decode(data([
            "ts": 123,
            "retention": ["array"]
        ]))
        let nullValue = try LivenessHeartbeat.decode(data([
            "ts": 123,
            "retention": NSNull()
        ]))

        XCTAssertNil(unsupported.retention)
        XCTAssertNil(malformed.retention)
        XCTAssertNil(wrongType.retention)
        XCTAssertNil(nullValue.retention)
    }

    func testDecodeRejectsUnreadableTimestamp() throws {
        XCTAssertThrowsError(try LivenessHeartbeat.decode(data(["retention": [:]])))
        XCTAssertThrowsError(try LivenessHeartbeat.decode(data(["ts": true])))
    }

    func testDecodeAcceptsNegativeTimestamp() throws {
        let heartbeat = try LivenessHeartbeat.decode(data(["ts": -1]))

        XCTAssertEqual(heartbeat.timestampMs, -1)
    }

    func testEncodeLegacyHeartbeatContainsNoRetentionKey() throws {
        let encoded = try LivenessHeartbeat(timestampMs: 123, retention: nil).encode()
        let text = String(decoding: encoded, as: UTF8.self)
        let decoded = try LivenessHeartbeat.decode(encoded)

        XCTAssertEqual(text, #"{"ts":123}"#)
        XCTAssertFalse(text.contains("retention"))
        XCTAssertEqual(decoded, LivenessHeartbeat(timestampMs: 123, retention: nil))
    }

    func testEncodeCapabilityUsesStableWireKeys() throws {
        let encoded = try LivenessHeartbeat(
            timestampMs: 123,
            retention: RetentionPeerCapability(
                barrierAwareSessionRefresh: true,
                checkpointBarrierHook: false
            )
        ).encode()
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: encoded) as? [String: Any])
        let retention = try XCTUnwrap(object["retention"] as? [String: Any])

        XCTAssertEqual(retention["version"] as? Int, 1)
        XCTAssertEqual(retention["barrier_aware_session_refresh"] as? Bool, true)
        XCTAssertEqual(retention["checkpoint_barrier_hook"] as? Bool, false)
    }

    func testRuntimeModeCapabilityReflectsActiveModeOnly() {
        let policy = RepoCompactionPolicy.default

        XCTAssertNil(RepoRetentionRuntimeMode.disabled.retentionPeerCapability)
        XCTAssertEqual(
            RepoRetentionRuntimeMode.barrierAwareSessionRefreshOnly.retentionPeerCapability,
            RetentionPeerCapability(barrierAwareSessionRefresh: true, checkpointBarrierHook: false)
        )
        XCTAssertEqual(
            RepoRetentionRuntimeMode.checkpointBarrierHookOnly(policy: policy).retentionPeerCapability,
            RetentionPeerCapability(barrierAwareSessionRefresh: true, checkpointBarrierHook: true)
        )
    }

    func testTrackerWritesLegacyHeartbeatWhenCapabilityDisabled() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let tracker = LivenessTracker(
            client: client,
            basePath: basePath,
            writerID: selfWriter,
            isLocalVolume: false
        )

        await tracker.start()
        let body = try await waitForHeartbeat(client: client, writerID: selfWriter)
        await tracker.stopAndWait()

        let text = String(decoding: body, as: UTF8.self)
        XCTAssertFalse(text.contains("retention"))
        _ = try LivenessHeartbeat.decode(body).timestampMs
    }

    func testTrackerWritesCapabilityHeartbeatWhenEnabled() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let capability = RetentionPeerCapability(
            barrierAwareSessionRefresh: true,
            checkpointBarrierHook: true
        )
        let tracker = LivenessTracker(
            client: client,
            basePath: basePath,
            writerID: selfWriter,
            isLocalVolume: false,
            retentionCapability: capability
        )

        await tracker.start()
        let body = try await waitForHeartbeat(client: client, writerID: selfWriter)
        await tracker.stopAndWait()

        XCTAssertEqual(try LivenessHeartbeat.decode(body).retention, capability)
    }

    func testSnapshotRetentionPeerStatusesReportsCapabilitiesSeparatelyFromSweepView() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
        let staleMs = nowMs - Int64((LivenessTracker.staleThreshold + 60) * 1000)
        let capability = RetentionPeerCapability(
            barrierAwareSessionRefresh: true,
            checkpointBarrierHook: false
        )
        await injectHeartbeat(client: client, writerID: peerWriter, ts: nowMs, capability: capability)
        await injectHeartbeat(client: client, writerID: staleWriter, ts: staleMs, capability: nil)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let sweepView = try await tracker.snapshotPeerStatuses()
        let retentionView = try await tracker.snapshotRetentionPeerStatuses()

        XCTAssertEqual(sweepView.activePeerIDs, [peerWriter])
        XCTAssertEqual(sweepView.stalePeerIDs, [staleWriter])
        XCTAssertEqual(sweepView.unknownPeerIDs, [])
        XCTAssertEqual(retentionView.peers.count, 2)
        XCTAssertEqual(retentionView.peer(peerWriter)?.capability, capability)
        XCTAssertNil(retentionView.peer(staleWriter)?.capability)
    }

    func testSnapshotRetentionPeerStatusesReportsUnknownPeersWithoutCapability() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setReadAfterWriteGrace(30)
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        let corruptPath = RepoLayout.livenessFilePath(base: basePath, writerID: corruptWriter)
        await client.injectFile(path: corruptPath, data: Data([0x01]))
        await injectListEntryWith404Download(client: client, writerID: vanishedWriter)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let retentionView = try await tracker.snapshotRetentionPeerStatuses()

        XCTAssertFalse(retentionView.isComplete)
        XCTAssertEqual(retentionView.peer(corruptWriter)?.status, .unknown(reason: .readFailed))
        XCTAssertEqual(retentionView.peer(vanishedWriter)?.status, .unknown(reason: .vanishedWithinGrace))
        XCTAssertNil(retentionView.peer(corruptWriter)?.capability)
        XCTAssertNil(retentionView.peer(vanishedWriter)?.capability)
    }

    func testSnapshotRetentionPeerStatusesTreatsMalformedRetentionAsMissingCapability() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
        let malformedRetention = try data([
            "ts": nowMs,
            "retention": [
                "version": 1,
                "barrier_aware_session_refresh": true
            ]
        ])
        let unreadableTimestamp = try data(["ts": true])
        await client.injectFile(
            path: RepoLayout.livenessFilePath(base: basePath, writerID: peerWriter),
            data: malformedRetention
        )
        await client.injectFile(
            path: RepoLayout.livenessFilePath(base: basePath, writerID: corruptWriter),
            data: unreadableTimestamp
        )

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let retentionView = try await tracker.snapshotRetentionPeerStatuses()

        XCTAssertEqual(retentionView.peer(peerWriter)?.status, .active(lastSeenMs: nowMs))
        XCTAssertNil(retentionView.peer(peerWriter)?.capability)
        XCTAssertEqual(retentionView.peer(corruptWriter)?.status, .unknown(reason: .readFailed))
    }

    func testLocalVolumeRetentionPeerViewIsEmpty() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: true)

        let view = try await tracker.snapshotRetentionPeerStatuses()

        XCTAssertEqual(view, .empty)
    }

    func testRetentionPeerSnapshotPropagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: peerWriter)
        await injectHeartbeat(client: client, writerID: peerWriter, ts: 123, capability: nil)
        await client.injectDownloadCancellation(for: path)
        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)

        do {
            _ = try await tracker.snapshotRetentionPeerStatuses()
            XCTFail("expected cancellation")
        } catch is CancellationError {
        }
    }

    func testSameWriterClaimProbeReadsNewHeartbeatShape() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let futureMs = Int64(Date().timeIntervalSince1970 * 1000) + 60_000
        let capability = RetentionPeerCapability(
            barrierAwareSessionRefresh: true,
            checkpointBarrierHook: true
        )
        await injectHeartbeat(client: client, writerID: selfWriter, ts: futureMs, capability: capability)
        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)

        await tracker.start()
        try await Task.sleep(for: .milliseconds(200))
        await tracker.stopAndWait()

        let body = try await waitForHeartbeat(client: client, writerID: selfWriter)
        let heartbeat = try LivenessHeartbeat.decode(body)
        XCTAssertEqual(heartbeat.timestampMs, futureMs)
        XCTAssertEqual(heartbeat.retention, capability)
    }

    private func data(_ object: [String: Any]) throws -> Data {
        try JSONSerialization.data(withJSONObject: object)
    }

    private func waitForHeartbeat(
        client: InMemoryRemoteStorageClient,
        writerID: String
    ) async throws -> Data {
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        for _ in 0..<40 {
            if let body = await client.snapshotFiles()[path] {
                return body
            }
            try await Task.sleep(for: .milliseconds(50))
        }
        XCTFail("heartbeat was not written")
        return Data()
    }

    private func injectHeartbeat(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        ts: Int64,
        capability: RetentionPeerCapability?
    ) async {
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        let body = try! LivenessHeartbeat(timestampMs: ts, retention: capability).encode()
        await client.injectFile(path: path, data: body)
    }

    private func injectListEntryWith404Download(client: InMemoryRemoteStorageClient, writerID: String) async {
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        await client.injectFile(path: path, data: Data([0x01]))
        await client.injectDownloadError(.notFound, for: path)
    }
}

private extension RetentionPeerStatusView {
    func peer(_ writerID: String) -> RetentionPeerStatus? {
        peers.first { $0.writerID == writerID }
    }
}

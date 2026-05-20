import XCTest
@testable import Watermelon

final class LivenessTrackerTests: XCTestCase {
    private let basePath = "/repo"

    // writerIDs are 36-char lowercase hex UUIDs per `RepoLayout.isValidWriterID`; tests use stable fixtures.
    private let selfWriter = "00000000-0000-0000-0000-000000000000"
    private let activeWriter = "11111111-1111-1111-1111-111111111111"
    private let staleWriter  = "22222222-2222-2222-2222-222222222222"
    private let phantomWriter = "33333333-3333-3333-3333-333333333333"
    private let goneWriter = "44444444-4444-4444-4444-444444444444"
    private let flakyWriter = "55555555-5555-5555-5555-555555555555"


    func testSnapshotPeerStatuses_classifiesActiveAndStale() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))

        let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
        let staleMs = nowMs - Int64((LivenessTracker.staleThreshold + 60) * 1000)
        await injectHeartbeat(client: client, writerID: activeWriter, ts: nowMs)
        await injectHeartbeat(client: client, writerID: staleWriter, ts: staleMs)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let view = try await tracker.snapshotPeerStatuses()

        XCTAssertEqual(view.activePeerIDs, [activeWriter])
        XCTAssertEqual(view.stalePeerIDs, [staleWriter])
        XCTAssertEqual(view.unknownPeerIDs, [])
        XCTAssertTrue(view.isComplete)
    }

    func testSnapshotPeerStatuses_404WithGrace_yieldsUnknown_vanishedWithinGrace() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setReadAfterWriteGrace(30)
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))

        await injectListEntryWith404Download(client: client, writerID: phantomWriter)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let view = try await tracker.snapshotPeerStatuses()

        XCTAssertEqual(view.unknownPeerIDs, [phantomWriter])
        XCTAssertTrue(view.activePeerIDs.isEmpty)
        XCTAssertFalse(view.isComplete, "any unknown peer must block cleanup")
        XCTAssertTrue(view.sweepProtectionSet.contains(phantomWriter),
                      "unknown peers must be in sweepProtectionSet so their staging files are preserved")
    }

    func testSnapshotPeerStatuses_404WithoutGrace_omitsPeer() async throws {
        let client = InMemoryRemoteStorageClient()
        // Default readAfterWriteGraceSeconds = 0.
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))

        await injectListEntryWith404Download(client: client, writerID: goneWriter)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let view = try await tracker.snapshotPeerStatuses()

        XCTAssertEqual(view.activePeerIDs, [])
        XCTAssertEqual(view.stalePeerIDs, [])
        XCTAssertEqual(view.unknownPeerIDs, [], "strong-consistency 404 = truly gone, not unknown")
        XCTAssertTrue(view.isComplete)
    }

    func testSnapshotPeerStatuses_persistentReadFailure_yieldsUnknown_readFailed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))

        let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
        await injectHeartbeat(client: client, writerID: flakyWriter, ts: nowMs)
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: flakyWriter)
        await client.injectPersistentDownloadError(.transport, for: path)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let view = try await tracker.snapshotPeerStatuses()

        XCTAssertEqual(view.unknownPeerIDs, [flakyWriter])
        XCTAssertTrue(view.activePeerIDs.isEmpty)
        XCTAssertFalse(view.isComplete)
    }

    func testSnapshotPeerStatuses_directoryAtHeartbeatPath_yieldsUnknown() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        // Create a directory at the canonical heartbeat path for a peer.
        let dirPath = RepoLayout.livenessFilePath(base: basePath, writerID: activeWriter)
        try await client.createDirectory(path: dirPath)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let view = try await tracker.snapshotPeerStatuses()

        XCTAssertEqual(view.unknownPeerIDs, [activeWriter],
                       "directory at heartbeat path must be unknown, not silently skipped")
        XCTAssertTrue(view.activePeerIDs.isEmpty)
        XCTAssertFalse(view.isComplete, "directory-shaped heartbeat must block cleanup")
        XCTAssertTrue(view.sweepProtectionSet.contains(activeWriter))
    }

    func testSnapshotRetentionPeerStatuses_directoryAtHeartbeatPath_yieldsUnknown() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        let dirPath = RepoLayout.livenessFilePath(base: basePath, writerID: activeWriter)
        try await client.createDirectory(path: dirPath)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let view = try await tracker.snapshotRetentionPeerStatuses()

        XCTAssertEqual(view.peers.count, 1)
        XCTAssertEqual(view.peers.first?.writerID, activeWriter)
        if case .unknown = view.peers.first?.status {
        } else {
            XCTFail("directory-shaped heartbeat must yield unknown status")
        }
        XCTAssertTrue(view.listComplete, "listComplete reflects successful list; unknown is in the peer data")
    }

    func testSnapshotPeerStatuses_booleanTimestamp_yieldsUnknown_blocksSweep() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))

        let path = RepoLayout.livenessFilePath(base: basePath, writerID: phantomWriter)
        let body: [String: Any] = ["ts": true]
        let data = try JSONSerialization.data(withJSONObject: body)
        await client.injectFile(path: path, data: data)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let view = try await tracker.snapshotPeerStatuses()

        XCTAssertEqual(view.unknownPeerIDs, [phantomWriter])
        XCTAssertTrue(view.activePeerIDs.isEmpty)
        XCTAssertTrue(view.stalePeerIDs.isEmpty,
                      "boolean ts must NOT bridge to 1ms and classify as ancient-stale")
        XCTAssertFalse(view.isComplete, "any unparseable peer heartbeat must block cleanup")
        XCTAssertTrue(view.sweepProtectionSet.contains(phantomWriter))
    }

    func testSnapshotPeerStatuses_negativeTimestamp_yieldsUnknown() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))

        let path = RepoLayout.livenessFilePath(base: basePath, writerID: phantomWriter)
        let body: [String: Any] = ["ts": -1]
        let data = try JSONSerialization.data(withJSONObject: body)
        await client.injectFile(path: path, data: data)

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        let view = try await tracker.snapshotPeerStatuses()

        XCTAssertEqual(view.unknownPeerIDs, [phantomWriter])
        XCTAssertTrue(view.activePeerIDs.isEmpty)
        XCTAssertTrue(view.stalePeerIDs.isEmpty,
                      "negative ts must NOT classify as stale and unblock cleanup")
        XCTAssertFalse(view.isComplete)
        XCTAssertTrue(view.sweepProtectionSet.contains(phantomWriter))
    }

    func testSnapshotPeerStatuses_listFailure_propagates() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        await client.injectListError(.transport, for: RepoLayout.livenessDirectoryPath(base: basePath))

        let tracker = LivenessTracker(client: client, basePath: basePath, writerID: selfWriter, isLocalVolume: false)
        do {
            _ = try await tracker.snapshotPeerStatuses()
            XCTFail("list failure must throw so caller skips cleanup")
        } catch {
            // expected
        }
    }


    func testActiveWritersView_isCompleteOnlyWhenNoUnknown() {
        let v1 = LivenessTracker.ActiveWritersView(activePeerIDs: ["a"], stalePeerIDs: ["b"], unknownPeerIDs: [])
        XCTAssertTrue(v1.isComplete)

        let v2 = LivenessTracker.ActiveWritersView(activePeerIDs: ["a"], stalePeerIDs: [], unknownPeerIDs: ["c"])
        XCTAssertFalse(v2.isComplete, "single unknown blocks cleanup")
    }

    func testActiveWritersView_sweepProtectionSet_unionsActiveAndUnknown() {
        let view = LivenessTracker.ActiveWritersView(
            activePeerIDs: ["a", "b"],
            stalePeerIDs: ["s"],
            unknownPeerIDs: ["u"]
        )
        XCTAssertEqual(view.sweepProtectionSet, ["a", "b", "u"],
                       "unknown peers MUST be protected — their staging may belong to a live writer we couldn't reach")
        XCTAssertFalse(view.sweepProtectionSet.contains("s"),
                       "stale peers' staging is fair game to sweep")
    }


    func testIsStale_appliesBackendGracePeriod() {
        let now = Date()
        let baseThresholdMs = Int64(LivenessTracker.staleThreshold * 1000)
        // Heartbeat that is `staleThreshold + 10s` old: stale under grace=0, fresh under grace=30.
        let tsMs = Int64(now.timeIntervalSince1970 * 1000) - baseThresholdMs - 10_000

        XCTAssertTrue(LivenessTracker.isStale(timestampMs: tsMs, now: now, gracePeriodSec: 0))
        XCTAssertFalse(LivenessTracker.isStale(timestampMs: tsMs, now: now, gracePeriodSec: 30),
                       "grace must extend the stale boundary so eventual-consistency lag doesn't false-positive a live peer")
    }


    private func injectHeartbeat(client: InMemoryRemoteStorageClient, writerID: String, ts: Int64) async {
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        let body: [String: Any] = ["ts": ts]
        let data = try! JSONSerialization.data(withJSONObject: body)
        await client.injectFile(path: path, data: data)
    }

    private func injectListEntryWith404Download(client: InMemoryRemoteStorageClient, writerID: String) async {
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        await client.injectFile(path: path, data: Data([0x01]))
        await client.injectDownloadError(.notFound, for: path)
    }
}

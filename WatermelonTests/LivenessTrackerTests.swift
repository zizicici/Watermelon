import XCTest
@testable import Watermelon

/// `LivenessTracker.snapshotPeerStatuses` is the cleanup gate: classifies every
/// peer's heartbeat as active / stale / unknown, and only when zero peers are
/// unknown does the builder allow `OrphanMetadataCleanup.sweep` to run. These
/// tests pin the boundary cases so a future refactor can't silently re-introduce
/// "any error suppressed = peer dropped from active set = staging deleted."
final class LivenessTrackerTests: XCTestCase {
    private let basePath = "/repo"

    // writerIDs are 36-char lowercase hex UUIDs per `RepoLayout.isValidWriterID`; tests use stable fixtures.
    private let selfWriter = "00000000-0000-0000-0000-000000000000"
    private let activeWriter = "11111111-1111-1111-1111-111111111111"
    private let staleWriter  = "22222222-2222-2222-2222-222222222222"
    private let phantomWriter = "33333333-3333-3333-3333-333333333333"
    private let goneWriter = "44444444-4444-4444-4444-444444444444"
    private let flakyWriter = "55555555-5555-5555-5555-555555555555"

    // MARK: - snapshotPeerStatuses

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

    /// Backend with `livenessConsistencyGraceSeconds > 0` (e.g., R2/MinIO/WebDAV-behind-cache):
    /// a 404 may be post-write visibility lag, not a truly absent peer. Must classify as unknown.
    func testSnapshotPeerStatuses_404WithGrace_yieldsUnknown_vanishedWithinGrace() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setLivenessConsistencyGrace(30)
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

    /// Strong-consistency backend (`grace == 0`, default): a confirmed 404 means
    /// the peer is truly gone and is omitted from all sets — no cleanup gate.
    func testSnapshotPeerStatuses_404WithoutGrace_omitsPeer() async throws {
        let client = InMemoryRemoteStorageClient()
        // Default livenessConsistencyGrace = 0.
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

    /// A transport error that persists past the retry budget must classify as
    /// `.unknown(.readFailed)`, not silently drop the peer. Otherwise sweep could
    /// nuke an active peer's staging during a transient network blip.
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

    /// `client.list` errors propagate (no view at all) — caller must skip cleanup.
    /// Prior `try?` behavior in the builder is preserved by this throw.
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

    // MARK: - ActiveWritersView semantics

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

    // MARK: - isStale grace period

    func testIsStale_appliesBackendGracePeriod() {
        let now = Date()
        let baseThresholdMs = Int64(LivenessTracker.staleThreshold * 1000)
        // Heartbeat that is `staleThreshold + 10s` old: stale under grace=0, fresh under grace=30.
        let tsMs = Int64(now.timeIntervalSince1970 * 1000) - baseThresholdMs - 10_000

        XCTAssertTrue(LivenessTracker.isStale(timestampMs: tsMs, now: now, gracePeriodSec: 0))
        XCTAssertFalse(LivenessTracker.isStale(timestampMs: tsMs, now: now, gracePeriodSec: 30),
                       "grace must extend the stale boundary so eventual-consistency lag doesn't false-positive a live peer")
    }

    // MARK: - Helpers

    private func injectHeartbeat(client: InMemoryRemoteStorageClient, writerID: String, ts: Int64) async {
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        let body: [String: Any] = ["ts": ts]
        let data = try! JSONSerialization.data(withJSONObject: body)
        await client.injectFile(path: path, data: data)
    }

    /// Simulates the listing race we're testing against: peer's filename is in
    /// `list` output, but its `download` 404s — either because the peer just
    /// renewed and we caught the swap window, or because the listing was cached.
    private func injectListEntryWith404Download(client: InMemoryRemoteStorageClient, writerID: String) async {
        let path = RepoLayout.livenessFilePath(base: basePath, writerID: writerID)
        await client.injectFile(path: path, data: Data([0x01]))
        await client.injectDownloadError(.notFound, for: path)
    }
}

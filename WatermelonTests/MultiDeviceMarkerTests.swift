import XCTest
@testable import Watermelon

// P08 (P08-MaintenanceCleanup): multi-device diagnostic marker. WriteLockService.acquire fires a
// best-effort hook whenever it observes another writer's lock (fresh / unknown / stale, in either the
// initial or post-write scan), never for the caller's own lock, and never letting the hook change the
// acquire outcome. The marker is diagnostic only — it does not alter verify's fail-closed semantics.
final class MultiDeviceMarkerTests: XCTestCase {
    private let basePath = "/photos"
    private let base = Date(timeIntervalSince1970: 1_700_000_000)
    private var keepAlive: [AnyObject] = []

    override func tearDown() {
        keepAlive.removeAll()
        super.tearDown()
    }

    private func newWriterID() -> String { UUID().uuidString.lowercased() }

    private func makeService(writerID: String, client: InMemoryRemoteStorageClient, marker: MarkerRecorder) -> WriteLockService {
        guard let service = WriteLockService(
            basePath: basePath, writerID: writerID, client: client,
            onForeignWriterObserved: { await marker.record() }
        ) else {
            preconditionFailure("canonical writer ID must build a service")
        }
        return service
    }

    private func makeDatabaseManager() throws -> DatabaseManager {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-marker-db-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let dbm = try DatabaseManager(databaseURL: dir.appendingPathComponent("test.sqlite"))
        keepAlive.append(dbm)
        return dbm
    }

    // MARK: - acquire fires the marker for any other writer

    func testMarkerFiresForFreshOtherLock() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: newWriterID(), modificationDate: base.addingTimeInterval(-60))
        let marker = MarkerRecorder()
        let service = makeService(writerID: newWriterID(), client: client, marker: marker)

        let result = await service.acquire(mode: .foreground, now: base)
        let count = await marker.count
        XCTAssertEqual(result, .blocked)
        XCTAssertGreaterThanOrEqual(count, 1, "a fresh other lock must fire the marker")
    }

    func testMarkerFiresForUnknownMtimeOtherLock() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: newWriterID(), modificationDate: nil)
        let marker = MarkerRecorder()
        let service = makeService(writerID: newWriterID(), client: client, marker: marker)

        let result = await service.acquire(mode: .foreground, now: base)
        let count = await marker.count
        XCTAssertEqual(result, .blocked)
        XCTAssertGreaterThanOrEqual(count, 1, "an unknown-mtime other lock must fire the marker")
    }

    func testMarkerFiresForStaleOtherLock() async {
        let client = InMemoryRemoteStorageClient()
        // Beyond expiry + skew so the stranger lock is unambiguously stale (not in the skew-fresh band).
        await client.seedLock(basePath: basePath, writerID: newWriterID(), modificationDate: base.addingTimeInterval(-(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 60)))
        await client.setPendingUploadModificationDate(base)
        let marker = MarkerRecorder()
        let service = makeService(writerID: newWriterID(), client: client, marker: marker)

        let result = await service.acquire(mode: .foreground, now: base)
        let count = await marker.count
        XCTAssertEqual(result, .acquired, "foreground takes over a stale stranger lock")
        XCTAssertGreaterThanOrEqual(count, 1, "a stale other lock must still fire the marker")
    }

    func testMarkerFiresOnPostWriteConfirmationScan() async {
        // Pre-write LIST is empty (no marker yet); post-write LIST surfaces a fresh other writer.
        let client = InMemoryRemoteStorageClient()
        let me = newWriterID()
        let other = newWriterID()
        await client.enqueueListResult([])
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base),
            makeLockEntry(basePath: basePath, writerID: other, modificationDate: base.addingTimeInterval(-60))
        ])
        let marker = MarkerRecorder()
        let service = makeService(writerID: me, client: client, marker: marker)

        let result = await service.acquire(mode: .foreground, now: base)
        let count = await marker.count
        XCTAssertEqual(result, .blocked)
        XCTAssertEqual(count, 1, "marker fires on the post-write confirmation scan")
    }

    // MARK: - Own lock never fires; marker never changes behavior

    func testMarkerDoesNotFireForOwnLockOnly() async {
        let client = InMemoryRemoteStorageClient()
        let me = newWriterID()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: base)
        await client.setPendingUploadModificationDate(base)
        let marker = MarkerRecorder()
        let service = makeService(writerID: me, client: client, marker: marker)

        let result = await service.acquire(mode: .foreground, now: base)
        let count = await marker.count
        XCTAssertEqual(result, .acquired)
        XCTAssertEqual(count, 0, "the caller's own lock must never fire the marker")
    }

    func testMarkerFiringDoesNotChangeContendedOutcome() async {
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: newWriterID(), modificationDate: base.addingTimeInterval(-60))
        let me = newWriterID()
        let service = WriteLockService(
            basePath: basePath, writerID: me, client: client,
            onForeignWriterObserved: {}   // present but inert
        )!

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        XCTAssertEqual(result, .blocked, "marker presence must not change the fail-closed outcome")
        XCTAssertFalse(uploaded.contains(RepoLayoutLite.lockPath(basePath: basePath, writerID: me)!), "a contended acquire still writes no own lock")
    }

    // MARK: - DatabaseManager marker storage

    func testDatabaseMarkerRoundTrip() throws {
        let dbm = try makeDatabaseManager()
        XCTAssertNil(try dbm.multiDeviceObservedAt(profileID: 7))
        try dbm.setMultiDeviceObserved(base, profileID: 7)
        let read = try dbm.multiDeviceObservedAt(profileID: 7)
        XCTAssertEqual(read?.timeIntervalSince1970 ?? -1, base.timeIntervalSince1970, accuracy: 0.001)
        XCTAssertNil(try dbm.multiDeviceObservedAt(profileID: 8), "the marker is per-profile")
    }

    func testForeignLockDuringAcquireRecordsDatabaseMarker() async throws {
        let dbm = try makeDatabaseManager()
        let profileID: Int64 = 42
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: newWriterID(), modificationDate: base.addingTimeInterval(-60))
        let marker: @Sendable () async -> Void = { try? dbm.setMultiDeviceObserved(Date(), profileID: profileID) }
        let service = WriteLockService(basePath: basePath, writerID: newWriterID(), client: client, onForeignWriterObserved: marker)!

        let result = await service.acquire(mode: .foreground, now: base)

        XCTAssertEqual(result, .blocked)
        XCTAssertNotNil(try dbm.multiDeviceObservedAt(profileID: profileID), "a foreign writer must record a diagnostic DB marker")
    }

    // MARK: - Verify semantics unchanged (diagnostic-only)

    func testMaintenanceForeignLockStillFailsClosedWhileMarking() async throws {
        let client = InMemoryRemoteStorageClient()
        let manifest = VersionManifestLite.makeManifest(createdAt: "2026-01-01T00:00:00Z", createdBy: "seed")
        await client.seedFile(path: RepoLayoutLite.versionPath(basePath: basePath), data: try VersionManifestLite.encode(manifest))
        await client.seedLock(basePath: basePath, writerID: newWriterID(), modificationDate: base.addingTimeInterval(-60))
        let marker = MarkerRecorder()

        do {
            _ = try await LiteRepoGateway.prepareMaintenance(
                client: client, basePath: basePath, writerID: newWriterID(), now: base,
                onForeignWriterObserved: { await marker.record() }
            )
            XCTFail("verify must still fail closed against a fresh foreign lock")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .lockConflict, "the marker must not change verify's fail-closed semantics")
        }
        let count = await marker.count
        XCTAssertGreaterThanOrEqual(count, 1, "the foreign lock is still observed diagnostically")
    }
}

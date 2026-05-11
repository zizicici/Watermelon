import XCTest
@testable import Watermelon

/// Crashed metadata writes leave `.staging-<uuid>` files behind that no future
/// run will ever reference (UUID is unique per call). Without cleanup, the
/// snapshot dir accumulates orphans indefinitely. Active-writer set + mtime
/// gate prevent racing peers' in-flight stagings from getting swept.
final class OrphanMetadataCleanupTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let month = LibraryMonthKey(year: 2026, month: 5)

    private func stagingName(for month: LibraryMonthKey, lamport: UInt64, writerID: String, runID: String) -> String {
        "\(RepoLayout.snapshotFileName(month: month, lamport: lamport, writerID: writerID, runID: runID)).staging-\(UUID().uuidString)"
    }

    func testSweep_oldOrphan_belongingToInactiveWriter_isDeleted() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let name = stagingName(for: month, lamport: 1, writerID: writerB, runID: "run-x")
        let path = "\(basePath)/.watermelon/snapshots/\(name)"
        await client.injectFile(path: path, contents: "orphan")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)

        let deleted = await OrphanMetadataCleanup.sweepSnapshots(
            client: client, basePath: basePath,
            activeWriters: [writerA] // writer B not active
        )
        XCTAssertEqual(deleted, 1)
        let entries = try await client.list(path: "\(basePath)/.watermelon/snapshots")
        XCTAssertTrue(entries.allSatisfy { !$0.name.contains(".staging-") })
    }

    func testSweep_recentOrphan_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let name = stagingName(for: month, lamport: 2, writerID: writerB, runID: "run-y")
        let path = "\(basePath)/.watermelon/snapshots/\(name)"
        await client.injectFile(path: path, contents: "fresh")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -60), path: path)

        let deleted = await OrphanMetadataCleanup.sweepSnapshots(
            client: client, basePath: basePath,
            activeWriters: []
        )
        XCTAssertEqual(deleted, 0, "mtime within threshold must keep the staging file (writer may still be mid-flight)")
    }

    func testSweep_oldOrphan_butWriterStillActive_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let name = stagingName(for: month, lamport: 3, writerID: writerB, runID: "run-z")
        let path = "\(basePath)/.watermelon/snapshots/\(name)"
        await client.injectFile(path: path, contents: "old-but-active")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)

        let deleted = await OrphanMetadataCleanup.sweepSnapshots(
            client: client, basePath: basePath,
            activeWriters: [writerB]
        )
        XCTAssertEqual(deleted, 0, "active-writer gate must protect a peer's staging file from sweep")
    }

    func testSweep_orphanWithoutMtime_failClosed_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let name = stagingName(for: month, lamport: 5, writerID: writerB, runID: "run-nomtime")
        let path = "\(basePath)/.watermelon/snapshots/\(name)"
        await client.injectFile(path: path, contents: "no-mtime")
        // Deliberately skip setModificationDateForTest → listing reports nil mtime.

        let deleted = await OrphanMetadataCleanup.sweepSnapshots(
            client: client, basePath: basePath,
            activeWriters: []
        )
        XCTAssertEqual(deleted, 0, "fail-closed: nil mtime must keep the file (some backends omit mtime)")
    }

    func testSweep_nonStagingFile_isUntouched() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // A real snapshot file (no .staging- suffix) — must NOT be deleted.
        let realName = RepoLayout.snapshotFileName(month: month, lamport: 4, writerID: writerB, runID: "run-q")
        let path = "\(basePath)/.watermelon/snapshots/\(realName)"
        await client.injectFile(path: path, contents: "real snapshot")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -86400), path: path)

        let deleted = await OrphanMetadataCleanup.sweepSnapshots(
            client: client, basePath: basePath,
            activeWriters: []
        )
        XCTAssertEqual(deleted, 0)
        let entries = try await client.list(path: "\(basePath)/.watermelon/snapshots")
        XCTAssertEqual(entries.count, 1, "non-staging snapshot files must survive sweep regardless of age")
    }
}

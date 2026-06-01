import XCTest
@testable import Watermelon

/// Crashed metadata writes leave `.staging-<uuid>` files behind that no future
/// run will ever reference (UUID is unique per call). The cleanup path only
/// reclaims staging attributed to the current writer; peer-writer and
/// unattributable staging are skipped regardless of age.
final class OrphanMetadataCleanupTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let month = LibraryMonthKey(year: 2026, month: 5)

    private func stagingName(for month: LibraryMonthKey, lamport: UInt64, writerID: String, runID: String) -> String {
        "\(RepoLayout.snapshotFileName(month: month, lamport: lamport, writerID: writerID, runID: runID)).staging-\(UUID().uuidString)"
    }

    // MARK: - sweepOwnStagings

    func testSweep_oldStaging_forCurrentWriter_isDeleted() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let name = stagingName(for: month, lamport: 1, writerID: writerA, runID: "run-x")
        let path = "\(basePath)/.watermelon/snapshots/\(name)"
        await client.injectFile(path: path, contents: "orphan")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)

        let deleted = try await OrphanMetadataCleanup.sweepOwnStagings(
            client: client, basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 1)
        let entries = try await client.list(path: "\(basePath)/.watermelon/snapshots")
        XCTAssertTrue(entries.allSatisfy { !$0.name.contains(".staging-") })
    }

    func testSweep_oldStaging_forPeerWriter_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let name = stagingName(for: month, lamport: 1, writerID: writerB, runID: "run-x")
        let path = "\(basePath)/.watermelon/snapshots/\(name)"
        await client.injectFile(path: path, contents: "peer orphan")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)

        // Current writer is writerA; writerB is a peer — staging must not be touched.
        let deleted = try await OrphanMetadataCleanup.sweepOwnStagings(
            client: client, basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 0, "peer-writer staging must be skipped regardless of age")
    }

    func testSweep_recentStaging_forCurrentWriter_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let name = stagingName(for: month, lamport: 2, writerID: writerA, runID: "run-y")
        let path = "\(basePath)/.watermelon/snapshots/\(name)"
        await client.injectFile(path: path, contents: "fresh")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -60), path: path)

        let deleted = try await OrphanMetadataCleanup.sweepOwnStagings(
            client: client, basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 0, "mtime within threshold must keep the staging file")
    }

    func testSweep_orphanWithoutMtime_failClosed_isSkipped() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let name = stagingName(for: month, lamport: 5, writerID: writerA, runID: "run-nomtime")
        let path = "\(basePath)/.watermelon/snapshots/\(name)"
        await client.injectFile(path: path, contents: "no-mtime")
        // Deliberately skip setModificationDateForTest → listing reports nil mtime.

        let deleted = try await OrphanMetadataCleanup.sweepOwnStagings(
            client: client, basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 0, "fail-closed: nil mtime must keep the file (some backends omit mtime)")
    }

    func testSweep_nonStagingFile_isUntouched() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // A real snapshot file (no .staging- suffix) — must NOT be deleted.
        let realName = RepoLayout.snapshotFileName(month: month, lamport: 4, writerID: writerA, runID: "run-q")
        let path = "\(basePath)/.watermelon/snapshots/\(realName)"
        await client.injectFile(path: path, contents: "real snapshot")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -86400), path: path)

        let deleted = try await OrphanMetadataCleanup.sweepOwnStagings(
            client: client, basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 0)
        let entries = try await client.list(path: "\(basePath)/.watermelon/snapshots")
        XCTAssertEqual(entries.count, 1, "non-staging snapshot files must survive sweep regardless of age")
    }

    // MARK: - Standard sweep directories

    /// Each `SweepDirectory` carries a writer-extractor matched to that dir's filename
    /// shape. If a future refactor desyncs a parser from its directory, the per-writer
    /// activeWriters gate silently downgrades to mtime-only protection for that dir.
    /// Pin each (dir, sample filename, expected writerID) so drift fails loud.
    func testStandardSweepDirectories_parsersMatchFilenameShapes() {
        let dirs = OrphanMetadataCleanup.standardSweepDirectories(basePath: basePath)
        XCTAssertEqual(dirs.count, 5, "standardSweepDirectories shape changed; update test cases too")

        let cases: [(dirPath: String, sampleName: String, expectedWriter: String?)] = [
            (
                RepoLayout.commitsDirectoryPath(base: basePath),
                RepoLayout.commitFileName(month: month, writerID: writerA, seq: 7),
                writerA
            ),
            (
                RepoLayout.snapshotsDirectoryPath(base: basePath),
                RepoLayout.snapshotFileName(month: month, lamport: 1, writerID: writerA, runID: "run-x"),
                writerA
            ),
            (
                RepoLayout.identityDirectoryPath(base: basePath),
                "\(writerA).json",
                writerA
            ),
            (
                RepoLayout.migrationsDirectoryPath(base: basePath),
                "\(writerA).json",
                writerA
            ),
            (
                RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]),
                "version.json",
                nil   // root .watermelon/ pairing is mtime-only (nil parser)
            )
        ]
        for testCase in cases {
            guard let dir = dirs.first(where: { $0.path == testCase.dirPath }) else {
                XCTFail("missing dir entry for \(testCase.dirPath)")
                continue
            }
            let parsed = dir.parseWriter(testCase.sampleName)
            XCTAssertEqual(parsed, testCase.expectedWriter,
                           "wrong writer parsed for \(testCase.sampleName) under \(testCase.dirPath)")
        }
    }

    /// Behavioral pin: peer-writer staging must be skipped across all standard directories.
    /// The cleanup path only reclaims current-writer staging; peer staging
    /// must remain protected regardless of age.
    func testStandardSweepDirectories_peerStaging_protectedAcrossAllDirs() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        // Plant old peer-writer staging in each directory that has a parser.
        let peerStagings: [(dirPath: String, fileName: String)] = [
            (
                RepoLayout.commitsDirectoryPath(base: basePath),
                "\(RepoLayout.commitFileName(month: month, writerID: writerB, seq: 9)).staging-\(UUID().uuidString)"
            ),
            (
                RepoLayout.snapshotsDirectoryPath(base: basePath),
                "\(RepoLayout.snapshotFileName(month: month, lamport: 2, writerID: writerB, runID: "run-z")).staging-\(UUID().uuidString)"
            ),
            (
                RepoLayout.identityDirectoryPath(base: basePath),
                "\(writerB).json.staging-\(UUID().uuidString)"
            ),
            (
                RepoLayout.migrationsDirectoryPath(base: basePath),
                "\(writerB).json.staging-\(UUID().uuidString)"
            )
        ]
        for staging in peerStagings {
            let path = RepoLayout.normalize(joining: [staging.dirPath, staging.fileName])
            await client.injectFile(path: path, contents: "old-peer-staging")
            await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)
        }

        // Sweep as writerA — peer (writerB) staging must not be touched.
        let deleted = try await OrphanMetadataCleanup.sweepOwnStagings(
            client: client,
            basePath: basePath,
            writerID: writerA,
            ageThresholdSeconds: 3600,
            now: Date()
        )
        XCTAssertEqual(deleted, 0,
                       "peer-writer staging must be skipped across all directories regardless of age")
    }
}

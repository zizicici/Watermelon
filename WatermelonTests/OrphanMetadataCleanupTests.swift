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


    /// Each `SweepDirectory` carries a writer-extractor matched to that dir's filename
    /// shape. If a future refactor desyncs a parser from its directory (e.g. swaps
    /// commit parser into snapshots dir), the per-writer activeWriters gate silently
    /// downgrades to mtime-only protection for that dir — a Medium hazard masquerading
    /// as a Low test gap. Pin each (dir, sample filename, expected writerID) so drift
    /// fails loud.
    func testStandardSweepDirectories_parsersMatchFilenameShapes() {
        let dirs = OrphanMetadataCleanup.standardSweepDirectories(basePath: basePath)
        XCTAssertEqual(dirs.count, 6, "standardSweepDirectories shape changed; update test cases too")

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
                RepoLayout.livenessDirectoryPath(base: basePath),
                "\(writerA).json",
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
                "repo.json",
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

    /// Behavioral pin: across all six standard directories, the active-writer gate
    /// must protect a peer's `.staging-` orphan whose canonical-half names that peer,
    /// while old orphans from inactive writers get swept.
    func testStandardSweepDirectories_activeWriterGate_protectsAcrossAllDirs() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let dirs = OrphanMetadataCleanup.standardSweepDirectories(basePath: basePath)

        // For each dir with a parser, plant an old staging file whose canonical-half
        // names writerB (active). For the nil-parser root dir, plant a similar file —
        // it should NOT be protected by writer gate (parser returns nil), age-gated only.
        let stagings: [(dirPath: String, fileName: String, isWriterParsed: Bool)] = [
            (
                RepoLayout.commitsDirectoryPath(base: basePath),
                "\(RepoLayout.commitFileName(month: month, writerID: writerB, seq: 9)).staging-\(UUID().uuidString)",
                true
            ),
            (
                RepoLayout.snapshotsDirectoryPath(base: basePath),
                "\(RepoLayout.snapshotFileName(month: month, lamport: 2, writerID: writerB, runID: "run-z")).staging-\(UUID().uuidString)",
                true
            ),
            (
                RepoLayout.livenessDirectoryPath(base: basePath),
                "\(writerB).json.staging-\(UUID().uuidString)",
                true
            ),
            (
                RepoLayout.identityDirectoryPath(base: basePath),
                "\(writerB).json.staging-\(UUID().uuidString)",
                true
            ),
            (
                RepoLayout.migrationsDirectoryPath(base: basePath),
                "\(writerB).json.staging-\(UUID().uuidString)",
                true
            )
        ]
        for staging in stagings {
            let path = RepoLayout.normalize(joining: [staging.dirPath, staging.fileName])
            await client.injectFile(path: path, contents: "old-but-active")
            await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)
        }

        let deleted = await OrphanMetadataCleanup.sweep(
            client: client,
            directories: dirs,
            activeWriters: [writerB],
            ageThresholdSeconds: 3600,
            now: Date()
        )
        XCTAssertEqual(deleted, 0,
                       "every dir's parser must surface writerB so the active-writer gate protects all peer stagings")

        // Now flip writerB to inactive: every staging in a writer-parsed dir should sweep.
        let deletedAfterInactive = await OrphanMetadataCleanup.sweep(
            client: client,
            directories: dirs,
            activeWriters: [],
            ageThresholdSeconds: 3600,
            now: Date()
        )
        XCTAssertEqual(deletedAfterInactive, stagings.count,
                       "with no active writers, every old peer staging across all parser-backed dirs must sweep")
    }


    /// Own writerID is unconditionally inserted into `activeWriters` for the general
    /// sweep, so own-writer liveness stagings from prior-crash ticks are immortal
    /// through that path. The targeted self-sweep must drop them even when our
    /// writerID is "active" (this run).
    func testSweepOwnLivenessStagings_oldOwnStaging_isDeleted() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let fileName = "\(writerA).json.staging-\(UUID().uuidString).tmp"
        let path = "\(basePath)/.watermelon/liveness/\(fileName)"
        await client.injectFile(path: path, contents: "stranded heartbeat")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)

        let deleted = try await OrphanMetadataCleanup.sweepOwnLivenessStagings(
            client: client,
            basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 1, "stranded own-writer liveness staging must be reclaimed by the targeted sweep")
        let entries = try await client.list(path: "\(basePath)/.watermelon/liveness")
        XCTAssertTrue(entries.allSatisfy { !$0.name.contains(".staging-") })
    }

    /// Same-process residue protection: a brand-new staging (mtime within threshold)
    /// must survive even at the self-sweep entry point. Pre-`liveness.start()` this
    /// is defense-in-depth, but it ensures the helper can't accidentally clobber a
    /// concurrent same-writerID instance's in-flight heartbeat.
    func testSweepOwnLivenessStagings_freshOwnStaging_isPreserved() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let fileName = "\(writerA).json.staging-\(UUID().uuidString).tmp"
        let path = "\(basePath)/.watermelon/liveness/\(fileName)"
        await client.injectFile(path: path, contents: "in-flight heartbeat")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -60), path: path)

        let deleted = try await OrphanMetadataCleanup.sweepOwnLivenessStagings(
            client: client,
            basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 0, "fresh same-writer staging must survive the targeted sweep")
    }

    /// Fail-closed parity with the general sweep: a backend that omits
    /// `modificationDate` from list entries (some WebDAV servers, the in-memory
    /// fake until `setModificationDateForTest` is called) must NOT cause the
    /// self-sweep to clobber an own-writer staging it can't age-check. A
    /// concurrent same-writerID instance's in-flight tick would otherwise lose.
    func testSweepOwnLivenessStagings_nilMtime_failClosed_isPreserved() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let fileName = "\(writerA).json.staging-\(UUID().uuidString).tmp"
        let path = "\(basePath)/.watermelon/liveness/\(fileName)"
        await client.injectFile(path: path, contents: "no-mtime staging")
        // Deliberately skip setModificationDateForTest → listing reports nil mtime.

        let deleted = try await OrphanMetadataCleanup.sweepOwnLivenessStagings(
            client: client,
            basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 0, "fail-closed: nil mtime must keep the file — can't distinguish orphan from concurrent in-flight tick")
        let stillThere = await client.hasFile(path)
        XCTAssertTrue(stillThere)
    }

    /// Targeted scope guard: a peer's old liveness staging is NOT in scope — that
    /// remains the general sweep's job (with its active-writer gate). The self-sweep
    /// must leave foreign stagings alone, otherwise an aged-but-still-active peer
    /// gets clobbered.
    func testSweepOwnLivenessStagings_peerStaging_isUntouched() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let fileName = "\(writerB).json.staging-\(UUID().uuidString).tmp"
        let path = "\(basePath)/.watermelon/liveness/\(fileName)"
        await client.injectFile(path: path, contents: "peer heartbeat")
        await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)

        let deleted = try await OrphanMetadataCleanup.sweepOwnLivenessStagings(
            client: client,
            basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 0, "self-sweep must not touch peer-owned liveness stagings")
        let entries = try await client.list(path: "\(basePath)/.watermelon/liveness")
        XCTAssertEqual(entries.count, 1)
    }

    /// Scope guard across dirs: an old own staging under commits/snapshots/identity/
    /// migrations must NOT be reclaimed by the liveness-only self-sweep. Those dirs
    /// remain protected by `activeWriters` in the general sweep. Confirms the helper
    /// doesn't accidentally generalize beyond liveness.
    func testSweepOwnLivenessStagings_doesNotReachNonLivenessDirs() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Plant aged own stagings under each non-liveness dir.
        let commitsPath = "\(basePath)/.watermelon/commits/\(RepoLayout.commitFileName(month: month, writerID: writerA, seq: 3)).staging-\(UUID().uuidString)"
        let snapshotsPath = "\(basePath)/.watermelon/snapshots/\(RepoLayout.snapshotFileName(month: month, lamport: 9, writerID: writerA, runID: "run-a")).staging-\(UUID().uuidString)"
        let identityPath = "\(basePath)/.watermelon/identity/\(writerA).json.staging-\(UUID().uuidString).tmp"
        let migrationsPath = "\(basePath)/.watermelon/migrations/\(writerA).json.staging-\(UUID().uuidString).tmp"
        for path in [commitsPath, snapshotsPath, identityPath, migrationsPath] {
            await client.injectFile(path: path, contents: "old own staging")
            await client.setModificationDateForTest(Date(timeIntervalSinceNow: -7200), path: path)
        }

        let deleted = try await OrphanMetadataCleanup.sweepOwnLivenessStagings(
            client: client,
            basePath: basePath,
            writerID: writerA
        )
        XCTAssertEqual(deleted, 0, "self-sweep must be liveness-only")
        for path in [commitsPath, snapshotsPath, identityPath, migrationsPath] {
            let stillThere = await client.hasFile(path)
            XCTAssertTrue(stillThere, "non-liveness own staging must survive self-sweep at \(path)")
        }
    }
}

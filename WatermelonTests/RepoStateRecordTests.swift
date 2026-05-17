import XCTest
@testable import Watermelon

final class RepoStateRecordTests: XCTestCase {
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    func testRepoIdentityLazyEnsureWriterIDIdempotent() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager)
        let identity = RepoIdentity(database: databaseManager)
        let first = try await identity.lazyEnsureWriterID(profileID: profileID)
        let second = try await identity.lazyEnsureWriterID(profileID: profileID)
        XCTAssertEqual(first, second)
        XCTAssertFalse(first.isEmpty)
    }

    func testRepoIdentityLazyEnsureRepoStateInsertsRow() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "writer-1")
        let identity = RepoIdentity(database: databaseManager)
        let s1 = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "repo-A", writerID: "writer-1")
        XCTAssertEqual(s1.lastSeq, 0)
        XCTAssertEqual(s1.lastClock, 0)
        XCTAssertEqual(s1.migrationCompleted, 0)

        let s2 = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "repo-A", writerID: "writer-1")
        XCTAssertEqual(s2.lastSeq, 0)
    }

    func testSeqAllocatorIsMonotonicAndPersists() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        let a = try await allocator.allocate()
        let b = try await allocator.allocate()
        let c = try await allocator.allocate()
        XCTAssertEqual(a, 1)
        XCTAssertEqual(b, 2)
        XCTAssertEqual(c, 3)

        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, 3)
    }

    /// A stale in-memory allocator must (a) never return a seq that's already been
    /// allocated, and (b) never regress the persisted lastSeq. With the read-then-write
    /// transaction, BG sees the actual DB value at allocate-time and continues from
    /// there — so its `allocate()` returns 6 (not stale 1) and DB advances to 6.
    func testSeqAllocatorReadsDBOnAllocateToAvoidStaleSeq() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let foreground = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        for _ in 0 ..< 5 { _ = try await foreground.allocate() }

        let background = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        let bgAllocated = try await background.allocate()

        XCTAssertEqual(bgAllocated, 6,
                       "BG initialized with stale current=0 must read DB=5 at allocate-time and return 6, not collide on seq=1")
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, 6,
                       "lastSeq must monotonically advance, never regress")
    }

    func testSeqAllocatorObserveRemoteMaxDoesNotRegressDB() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let foreground = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await foreground.observeRemoteMax(100)

        let background = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await background.observeRemoteMax(50)

        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, 100,
                       "observe with smaller value must not regress DB")
    }

    /// A stale BG clock must (a) never return a range overlapping FG's, and (b) never
    /// regress lastClock. Read-then-write transaction in `tickRange` reads DB at
    /// allocate-time so BG's range starts at 1001 (after FG's 1..1000), DB → 1010.
    func testPersistedLamportClockReadsDBOnTickToAvoidOverlap() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let foreground = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        _ = try await foreground.tickRange(count: 1000)

        let background = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        let bgRange = try await background.tickRange(count: 10)

        XCTAssertEqual(bgRange.low, 1001,
                       "BG initialized with stale current=0 must read DB=1000 at tick-time and start at 1001, not overlap FG's 1..1000")
        XCTAssertEqual(bgRange.high, 1010)
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastClock, 1010,
                       "lastClock must monotonically advance, never regress")
    }

    func testPersistedLamportClockObserveDoesNotRegressDB() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let foreground = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await foreground.observe(500)

        let background = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await background.observe(100)

        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastClock, 500,
                       "observing a smaller value must not regress persisted lastClock")
    }

    /// observe on an actor whose `current` is below DB high-water must adopt the
    /// DB value, not leave `value()` reading the smaller external. Pre-fix the
    /// rejected conditional UPDATE silently left `current = external < dbValue`,
    /// so `value()` could under-report — fragile if any future caller bases a
    /// decision on `value()` rather than re-reading the DB.
    func testPersistedLamportClockObserveAdoptsDBHighWaterAfterRejectedUpdate() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let foreground = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await foreground.observe(500)

        let background = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await background.observe(100)

        let bgValue = await background.value()
        XCTAssertEqual(bgValue, 500,
                       "observe must lift actor-local current to DB high-water when the conditional UPDATE is rejected")
    }

    /// Symmetric to PersistedLamportClock: SeqAllocator.observeRemoteMax must lift
    /// actor-local `current` to the DB high-water if the conditional UPDATE is
    /// rejected, so future `value()` reads don't under-report.
    func testSeqAllocatorObserveRemoteMaxAdoptsDBHighWaterAfterRejectedUpdate() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let foreground = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await foreground.observeRemoteMax(100)

        let background = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await background.observeRemoteMax(50)

        let bgValue = await background.value()
        XCTAssertEqual(bgValue, 100,
                       "observeRemoteMax must lift actor-local current to DB high-water when the conditional UPDATE is rejected")
    }

    /// Profile delete must cascade to `repo_state` — otherwise a recycled profileID
    /// inherits stale lastSeq/lastClock from the previous owner.
    func testDeleteServerProfileCascadesToRepoState() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        try databaseManager.deleteServerProfile(id: profileID)

        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertNil(reloaded, "repo_state row must be removed with its parent profile")
    }

    /// `findRepoStateByProfile` prefers the migration-completed row over any
    /// half-baked alternates so resume sessions grab the canonical repoID.
    func testFindRepoStateByProfilePrefersMigrationCompleted() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")

        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "old-completed",
                writerID: "w", lastClock: 100, lastSeq: 5,
                migrationCompleted: 1
            ).insert(db)
            try RepoStateRecord(
                profileID: profileID, repoID: "new-incomplete",
                writerID: "w", lastClock: 200, lastSeq: 10,
                migrationCompleted: 0
            ).insert(db)
        }

        let identity = RepoIdentity(database: databaseManager)
        let resolved = try await identity.findRepoStateByProfile(profileID: profileID)
        XCTAssertEqual(resolved?.repoID, "old-completed",
                       "completed row wins regardless of higher lastSeq on incomplete row")
    }

    func testSeqAllocatorObserveRemoteMaxAdvances() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await allocator.observeRemoteMax(100)
        let next = try await allocator.allocate()
        XCTAssertEqual(next, 101)
    }
}

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

    /// Peer-poisoned Lamport values (e.g. a hostile commit with `clock=UInt64.max`
    /// or a snapshot filename with `lamport=ffffffffffffffff`) used to crash every
    /// later writer via `fatalError` inside `tickRange`. `observe` now ignores
    /// values above the safe ceiling and `tickRange` throws a typed error instead
    /// of crashing.
    func testPersistedLamportClockObserveIgnoresValueAboveSafeCeiling() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await clock.observe(UInt64.max)

        let inMemory = await clock.value()
        XCTAssertEqual(inMemory, 0, "observe must reject UInt64.max so the actor-local mirror stays at 0")
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastClock, 0, "rejected observe must not persist UInt64.max")

        // Subsequent tickRange must still succeed — the prior `fatalError` path
        // would have aborted the process before this call could complete.
        let range = try await clock.tickRange(count: 4)
        XCTAssertEqual(range.low, 1)
        XCTAssertEqual(range.high, 4)
    }

    /// Even if a legitimate observe happens to land near the ceiling (paranoid
    /// case — would take ~9e18 prior ticks), `tickRange` surfaces a typed error
    /// instead of crashing. Planted value is the max non-poison high-water
    /// (`maxAdoptableValue - 1`) so the actor loads it as-is rather than
    /// resetting it during init's poison detection.
    func testPersistedLamportClockTickRangeAtCeilingThrowsRatherThanCrashing() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let nearCeiling = LamportClock.maxAdoptableValue - 1
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: nearCeiling), profileID, "r"]
            )
        }
        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: nearCeiling)

        do {
            _ = try await clock.tickRange(count: 5)
            XCTFail("expected advanceExhausted")
        } catch PersistedLamportClockError.advanceExhausted(let current, let requested) {
            XCTAssertEqual(current, nearCeiling)
            XCTAssertEqual(requested, 5)
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    /// In-memory LamportClock mirrors the persisted clock's safe-ceiling behavior
    /// so any test or alternate path using it has the same crash-resistance.
    func testLamportClockObserveIgnoresValueAboveSafeCeilingAndTickRangeThrows() async throws {
        let clock = LamportClock(initial: 0)
        await clock.observe(UInt64.max)
        let val = await clock.value()
        XCTAssertEqual(val, 0)

        let clock2 = LamportClock(initial: LamportClock.maxAdvanceableValue)
        do {
            _ = try await clock2.tickRange(count: 1)
            XCTFail("expected advanceExhausted")
        } catch LamportClockError.advanceExhausted(let current, let requested) {
            XCTAssertEqual(current, LamportClock.maxAdvanceableValue)
            XCTAssertEqual(requested, 1)
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    /// Exact-ceiling values are also rejected by `observe` — the prior `<=`
    /// guard let a peer's `clock == maxAdvanceableValue` get persisted, after
    /// which every subsequent `tickRange` threw `advanceExhausted`. Strict-`<`
    /// guarantees observed values always leave at least one tick of headroom.
    func testPersistedLamportClockObserveRejectsExactCeiling() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await clock.observe(LamportClock.maxAdvanceableValue)

        let inMemory = await clock.value()
        XCTAssertEqual(inMemory, 0)
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastClock, 0)

        // After the strict rejection, allocation must still work.
        let range = try await clock.tickRange(count: 2)
        XCTAssertEqual(range.low, 1)
        XCTAssertEqual(range.high, 2)
    }

    func testLamportClockObserveRejectsExactCeiling() async throws {
        let clock = LamportClock(initial: 0)
        await clock.observe(LamportClock.maxAdvanceableValue)
        let val = await clock.value()
        XCTAssertEqual(val, 0)

        // Subsequent ticks must still allocate.
        let range = try await clock.tickRange(count: 3)
        XCTAssertEqual(range.low, 1)
        XCTAssertEqual(range.high, 3)
    }

    /// Pre-fix installs may have a poisoned `repo_state.lastClock` row (e.g.
    /// observed a remote `UInt64.max`-class clock and persisted it raw before
    /// this round's `observe` guard landed). The next allocation must auto-
    /// repair the row in the same write transaction so the writer can resume
    /// instead of throwing `advanceExhausted` forever. Recovery resets the
    /// in-memory + DB high-water to 0; a sanitized `observe` from the
    /// materializer (applied upstream in normal startup) then pushes both
    /// back up to a real high-water before the first tick.
    func testPersistedLamportClockTickRangeRepairsPoisonedDBHighWater() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        // Plant a poisoned DB value directly (simulates a pre-fix install).
        let poison = UInt64.max
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: poison), profileID, "r"]
            )
        }

        // Init detects poison and resets `current` to 0. tickRange then
        // detects the poisoned DB row and resets dbCurrent to 0 as well,
        // overwriting the row when it writes the new `high`.
        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: poison)
        let initial = await clock.value()
        XCTAssertEqual(initial, 0, "init must reset poisoned mirror to 0")

        let range = try await clock.tickRange(count: 3)
        XCTAssertEqual(range.low, 1)
        XCTAssertEqual(range.high, 3)

        // The DB row has been overwritten with the new high — poison is gone.
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let recovered = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertEqual(recovered, 3)
    }

    /// On a legit recovery path the sanitized observedClock from materialize
    /// is applied via observe before the first tick — verify that flow works
    /// even when the DB started poisoned.
    func testPersistedLamportClockPoisonRecoveryWithObserveRestoresHighWater() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let poison = UInt64.max - 10
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: poison), profileID, "r"]
            )
        }

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: poison)
        try await clock.observe(500)
        let afterObserve = await clock.value()
        XCTAssertEqual(afterObserve, 500, "observe must overwrite poisoned DB row with sanitized clock")

        let range = try await clock.tickRange(count: 2)
        XCTAssertEqual(range.low, 501)
        XCTAssertEqual(range.high, 502)
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

    /// `maxObservableValue` is one below `maxAdvanceableValue` so the prior
    /// "accepted but unadvanceable" choke point is gone: a peer planting
    /// `maxAdvanceableValue - 1` (the OLD observe ceiling minus one) is now
    /// rejected by observe, and an observed value up to `maxObservableValue - 1`
    /// still leaves at least one tick of self-progression headroom.
    func testPersistedLamportClockObserveRejectsCeilingMinusOne() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await clock.observe(LamportClock.maxAdvanceableValue - 1)

        let inMemory = await clock.value()
        XCTAssertEqual(inMemory, 0, "values at/above maxObservableValue must be rejected")
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastClock, 0, "rejected observe must not persist")
    }

    func testLamportClockObserveRejectsCeilingMinusOne() async throws {
        let clock = LamportClock(initial: 0)
        await clock.observe(LamportClock.maxAdvanceableValue - 1)
        let val = await clock.value()
        XCTAssertEqual(val, 0)
    }

    /// Codex Reviewer 1 (loop II-VII final convergence) — direct regression
    /// for the dead-end at the highest-accepted observe value. Pre-fix,
    /// observe accepted `maxAdoptableValue - 1` (the highest emittable AND
    /// adoptable value) but `tickRange(count: 1)` then threw `advanceExhausted`
    /// because its headroom guard used the same strict-`<` ceiling as
    /// observe — adopting the highest tickable value left zero headroom.
    /// The fix loosens tick to use the absolute `maxObservableValue` emit
    /// ceiling (one above the adopt ceiling), so adopting the highest
    /// observable value still has at least one tick of self-progression
    /// headroom. Persisted poison detection follows the looser ceiling too,
    /// so the resulting emit at `maxAdoptableValue` survives restart.
    func testPersistedLamportClockObserveAtBoundaryThenTickRangeSucceeds() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        let boundary = LamportClock.maxAdoptableValue - 1
        try await clock.observe(boundary)

        let range = try await clock.tickRange(count: 1)
        XCTAssertEqual(range.low, LamportClock.maxAdoptableValue,
                       "tick from the highest-adopted value must produce `maxAdoptableValue` — one above the adopt ceiling, which only peers cannot adopt")
        XCTAssertEqual(range.high, LamportClock.maxAdoptableValue)

        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let stored = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertEqual(stored, LamportClock.maxAdoptableValue,
                       "writer's legitimate boundary emit must survive in DB — poison detection uses the looser `maxObservableValue` ceiling")

        // The boundary emit is the writer's last legitimate tick; the next
        // `tickRange(1)` throws because `current == maxAdoptableValue` would
        // emit `maxAdoptableValue + 1 == maxObservableValue` which violates
        // the tick-emit strict-`<` ceiling. End-of-clock dead-end is the
        // expected terminal state for a writer that ticked all the way.
        do {
            _ = try await clock.tickRange(count: 1)
            XCTFail("expected advanceExhausted at the absolute emit boundary")
        } catch PersistedLamportClockError.advanceExhausted {
            // Expected: writer is now at `maxAdoptableValue`, one tick from
            // `maxObservableValue` (the absolute emit ceiling).
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    /// Observing `maxAdoptableValue - 2` (one below the highest adopt)
    /// permits a tick that emits `maxAdoptableValue - 1` — still
    /// peer-adoptable. Sanity check that the relaxed tick ceiling
    /// doesn't change behavior strictly below the boundary.
    func testPersistedLamportClockObserveOneBelowBoundaryAllowsOneTick() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        let boundary = LamportClock.maxAdoptableValue - 2
        try await clock.observe(boundary)

        let range = try await clock.tickRange(count: 1)
        XCTAssertEqual(range.low, boundary + 1)
        XCTAssertEqual(range.high, boundary + 1)
        XCTAssertEqual(range.high, LamportClock.maxAdoptableValue - 1,
                       "tick result is still peer-adoptable (strictly below `maxAdoptableValue`)")
    }

    /// `observe(maxAdoptableValue)` must be rejected — the value sits at
    /// the adopt strict-`<` ceiling so accepting it would consume the
    /// writer's last tick of headroom. The persisted clock stays at 0,
    /// the actor-local mirror stays at 0, and the next `tickRange(1)`
    /// emits 1.
    func testObserveRejectsValueAtAdoptCeiling() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        let peerBoundary = LamportClock.maxAdoptableValue
        try await clock.observe(peerBoundary)

        // observe rejects the dead-end value — the persisted clock stays
        // at 0 and tickRange(1) succeeds.
        let valueAfterObserve = await clock.value()
        XCTAssertEqual(valueAfterObserve, 0,
                       "values at/above maxAdoptableValue must be rejected by observe")
        let range = try await clock.tickRange(count: 1)
        XCTAssertEqual(range.high, 1,
                       "tickRange must succeed after observe rejects the dead-end value")
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let stored = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertEqual(stored, 1,
                       "DB row reflects the new tick, never the unobservable peer value")
    }

    /// A pre-fix install could have persisted `maxObservableValue` via the
    /// old tickRange cap (which emitted up to that value). Init must now
    /// treat it as poison and the builder repair path must sanitise the
    /// row, otherwise the writer is permanently dead-ended (every later
    /// tickRange would throw `advanceExhausted`).
    func testPersistedLamportClockInitTreatsMaxObservableValueAsPoison() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let prefixEmit = LamportClock.maxObservableValue
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: prefixEmit), profileID, "r"]
            )
        }

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: prefixEmit)
        let inMemory = await clock.value()
        XCTAssertEqual(inMemory, 0, "init must reset persisted maxObservableValue to 0 as an unobservable dead-end")

        try await clock.repairPoisonedDBIfNeeded()
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let recovered = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertEqual(recovered, 0, "repair must overwrite the unobservable dead-end DB row with sanitized 0")

        let range = try await clock.tickRange(count: 1)
        XCTAssertEqual(range.low, 1)
        XCTAssertEqual(range.high, 1)
    }

    /// An exact-ceiling poisoned DB row (Int64.max == maxAdvanceableValue)
    /// stores as positive in SQLite, so the legacy `lastClock < ?` predicate
    /// couldn't repair it on a sanitized observe — the row stayed poisoned
    /// across restarts. persist now branches to an unconditional UPDATE on
    /// the poisoned branch.
    func testPersistedLamportClockObserveRepairsExactCeilingDBPoison() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let poison = LamportClock.maxAdvanceableValue
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: poison), profileID, "r"]
            )
        }

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: poison)
        try await clock.observe(500)

        let afterObserve = await clock.value()
        XCTAssertEqual(afterObserve, 500, "sanitized observe must adopt the new high-water")
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let recovered = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertEqual(recovered, 500,
                       "DB row must be overwritten with the sanitized value — exact-ceiling poison no longer survives")
    }

    /// When observedClock is 0 (e.g. the only peer ops were poisoned and
    /// skipped), `observe(0)` is a no-op — but the poisoned DB row must
    /// still be repaired before the runtime is handed out. observe now
    /// triggers `repairPoisonedDBIfNeeded` on the no-op path while
    /// `dbPoisonedAtInit` is set.
    func testPersistedLamportClockObserveZeroRepairsPoisonedDBRow() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let poison = UInt64.max
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: poison), profileID, "r"]
            )
        }

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: poison)
        try await clock.observe(0)

        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let recovered = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertEqual(recovered, 0, "no-op observe under poisoned-init flag must still repair the DB row")
    }

    /// Direct cover for `repairPoisonedDBIfNeeded` — idempotent across
    /// multiple calls and no-op when the row is already sane.
    func testPersistedLamportClockRepairPoisonedDBIsIdempotent() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let poison = LamportClock.maxAdvanceableValue
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: poison), profileID, "r"]
            )
        }

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: poison)
        try await clock.repairPoisonedDBIfNeeded()
        try await clock.repairPoisonedDBIfNeeded()

        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let recovered = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertEqual(recovered, 0, "init mirror was reset to 0, so repair overwrites the row with 0")

        // No-op when sane.
        try await clock.repairPoisonedDBIfNeeded()
        let still = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(still.map { UInt64(bitPattern: $0.lastClock) }, 0)
    }
}

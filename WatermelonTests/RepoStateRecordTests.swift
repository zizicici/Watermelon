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

    func testSeqAllocatorIgnoresRemoteSeqAbovePersistableCeiling() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await allocator.observeRemoteMax(RepoStateAuthority.maxPersistableSeq + 1)

        let ignoredValue = await allocator.value()
        XCTAssertEqual(ignoredValue, 0)
        var reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, 0, "above-ceiling remote seq must not be written as a negative SQLite INTEGER")

        let next = try await allocator.allocate()
        XCTAssertEqual(next, 1, "ignored remote poison must not re-enter actor-local state through the persist return path")
        reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, 1)
    }

    func testSeqAllocatorCanAllocateMaxPersistableSeqAfterObservation() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await allocator.observeRemoteMax(RepoStateAuthority.maxPersistableSeq - 1)
        let next = try await allocator.allocate()

        XCTAssertEqual(next, RepoStateAuthority.maxPersistableSeq)
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, Int64.max)
    }

    func testSeqAllocatorIgnoresRemoteSeqAtPersistableCeiling() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await allocator.observeRemoteMax(RepoStateAuthority.maxPersistableSeq)

        let ignoredValue = await allocator.value()
        XCTAssertEqual(ignoredValue, 0)
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, 0, "ceiling remote seq must not be persisted")

        let next = try await allocator.allocate()
        XCTAssertEqual(next, 1, "ignored ceiling must not block allocation")
    }

    func testSeqAllocatorThrowsWhenNextSeqExceedsPersistableCeiling() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await allocator.observeRemoteMax(RepoStateAuthority.maxPersistableSeq - 1)
        let ceiling = try await allocator.allocate()
        XCTAssertEqual(ceiling, RepoStateAuthority.maxPersistableSeq)

        do {
            _ = try await allocator.allocate()
            XCTFail("expected exhausted")
        } catch SeqAllocator.SeqAllocatorError.exhausted {
            let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
            XCTAssertEqual(reloaded?.lastSeq, Int64.max)
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

    func testSanitizeRemoteSeqObservation_rejectsPersistableCeiling() async throws {
        let result = RepoStateAuthority.sanitizeRemoteSeqObservation(
            RepoStateAuthority.maxPersistableSeq, writerID: "w"
        )
        if case .ignoredAsUntrusted = result {
            // correct
        } else {
            XCTFail("maxPersistableSeq must be ignoredAsUntrusted, got \(result)")
        }

        let belowCeiling = RepoStateAuthority.sanitizeRemoteSeqObservation(
            RepoStateAuthority.maxPersistableSeq - 1, writerID: "w"
        )
        if case .accepted(let seq) = belowCeiling {
            XCTAssertEqual(seq, RepoStateAuthority.maxPersistableSeq - 1)
        } else {
            XCTFail("maxPersistableSeq - 1 must be accepted, got \(belowCeiling)")
        }
    }

    func testSeqAllocatorSanitizesNegativePersistedSeqOnAllocate() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastSeq = -1 WHERE profileID = ? AND repoID = ?",
                arguments: [profileID, "r"]
            )
        }

        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        let next = try await allocator.allocate()

        XCTAssertEqual(next, 1)
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, 1)
    }

    func testSeqAllocatorRebuildsContinuityFromSafeObservationAfterNegativePersistedSeq() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastSeq = -1 WHERE profileID = ? AND repoID = ?",
                arguments: [profileID, "r"]
            )
        }
        let poisonedRow = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let poisoned = try XCTUnwrap(poisonedRow)
        let counters = RepoStateAuthority.counters(from: poisoned)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: "r", initial: counters.lastSeq)

        try await allocator.observeRemoteMax(7)
        let next = try await allocator.allocate()

        XCTAssertEqual(next, 8)
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastSeq, 8)
    }

    func testSeqAllocatorSanitizesUntrustedInitialSeqAboveCeiling() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let allocator = SeqAllocator(
            database: databaseManager,
            profileID: profileID,
            repoID: "r",
            initial: RepoStateAuthority.maxPersistableSeq + 1
        )

        let initialValue = await allocator.value()
        XCTAssertEqual(initialValue, 0)
        let next = try await allocator.allocate()
        XCTAssertEqual(next, 1)
    }

    func testDeleteServerProfileCascadesToRepoState() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        try databaseManager.deleteServerProfile(id: profileID)

        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertNil(reloaded, "repo_state row must be removed with its parent profile")
    }

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

    func testFindRepoStateByProfileDoesNotPreferNegativeLastSeqPoison() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")

        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "poison",
                writerID: "w", lastClock: 0, lastSeq: -1,
                migrationCompleted: 1
            ).insert(db)
            try RepoStateRecord(
                profileID: profileID, repoID: "sane",
                writerID: "w", lastClock: 0, lastSeq: 10,
                migrationCompleted: 1
            ).insert(db)
        }

        let identity = RepoIdentity(database: databaseManager)
        let resolved = try await identity.findRepoStateByProfile(profileID: profileID)
        XCTAssertEqual(resolved?.repoID, "sane")
    }

    func testFindRepoStateByProfileDoesNotPreferInt64MaxLastSeqPoison() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")

        try databaseManager.write { db in
            try RepoStateRecord(
                profileID: profileID, repoID: "positive-poison",
                writerID: "w", lastClock: 0, lastSeq: Int64.max,
                migrationCompleted: 1
            ).insert(db)
            try RepoStateRecord(
                profileID: profileID, repoID: "sane",
                writerID: "w", lastClock: 0, lastSeq: 10,
                migrationCompleted: 1
            ).insert(db)
        }

        let identity = RepoIdentity(database: databaseManager)
        let resolved = try await identity.findRepoStateByProfile(profileID: profileID)
        XCTAssertEqual(resolved?.repoID, "sane")
    }

    func testRepoStateAuthorityDecodesNegativeSeqWithoutTreatingItAsUInt64Max() {
        let row = RepoStateRecord(
            profileID: 1,
            repoID: "r",
            writerID: "w",
            lastClock: -1,
            lastSeq: -1,
            migrationCompleted: 0
        )

        let counters = RepoStateAuthority.counters(from: row)
        XCTAssertEqual(counters.lastSeq, 0)
        XCTAssertEqual(counters.lastClock, UInt64.max)
    }

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

    func testPersistedLamportClockTickRangeRepairsPoisonedDBHighWater() async throws {
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

    func testPersistedLamportClockObserveRejectsCeilingMinusOne() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        try await clock.observe(LamportClock.maxAdvanceableValue - 1)

        let inMemory = await clock.value()
        XCTAssertEqual(inMemory, 0, "values at/above maxAdoptableValue must be rejected")
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        XCTAssertEqual(reloaded?.lastClock, 0, "rejected observe must not persist")
    }

    func testLamportClockObserveRejectsCeilingMinusOne() async throws {
        let clock = LamportClock(initial: 0)
        await clock.observe(LamportClock.maxAdvanceableValue - 1)
        let val = await clock.value()
        XCTAssertEqual(val, 0)
    }

    func testPersistedLamportClockObserveAtBoundaryThenTickRangeThrowsExhausted() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: 0)
        let boundary = LamportClock.maxAdoptableValue - 1
        try await clock.observe(boundary)

        // Emission ceiling equals reader acceptance ceiling; emitting
        // `maxAdoptableValue` would produce an unusable value rejected by
        // materializer/checkpoint readers, so the allocator must throw.
        do {
            _ = try await clock.tickRange(count: 1)
            XCTFail("expected advanceExhausted because emitting maxAdoptableValue would be rejected by readers")
        } catch PersistedLamportClockError.advanceExhausted {
            // Expected: the writer observed the highest-adoptable value and
            // cannot emit a value readers would accept.
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }

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

    func testPersistedLamportClockInitTreatsMaxAdoptableValueAsPoison() async throws {
        let profileID = try TestFixtures.insertServerProfile(in: databaseManager, writerID: "w")
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: "r", writerID: "w")

        let poison = LamportClock.maxAdoptableValue
        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE \(RepoStateRecord.databaseTableName) SET lastClock = ? WHERE profileID = ? AND repoID = ?",
                arguments: [Int64(bitPattern: poison), profileID, "r"]
            )
        }

        let clock = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: "r", initial: poison)
        let inMemory = await clock.value()
        XCTAssertEqual(inMemory, 0, "init must reset persisted maxAdoptableValue to 0 as an unobservable dead-end")

        try await clock.repairPoisonedDBIfNeeded()
        let reloaded = try await identity.loadRepoState(profileID: profileID, repoID: "r")
        let recovered = reloaded.map { UInt64(bitPattern: $0.lastClock) }
        XCTAssertEqual(recovered, 0, "repair must overwrite the unobservable dead-end DB row with sanitized 0")

        let range = try await clock.tickRange(count: 1)
        XCTAssertEqual(range.low, 1)
        XCTAssertEqual(range.high, 1)
    }

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

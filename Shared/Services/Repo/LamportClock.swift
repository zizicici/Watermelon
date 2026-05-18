import Foundation
import GRDB
import os.log

private let lamportLog = Logger(subsystem: "com.zizicici.watermelon", category: "LamportClock")

actor LamportClock {
    private var current: UInt64

    /// Signed-positive cap. `Int64.max ≈ 9.22 × 10^18`. Any persisted value at
    /// or above is corruption (a pre-fix install observed a `UInt64.max`-class
    /// peer clock). Kept distinct from `maxObservableValue` only as documentation
    /// of the underlying signed-projection invariant. Tick-emit + persisted
    /// poison detection use `maxObservableValue`; adopt (observe + materializer
    /// quarantine) uses the tighter `maxAdoptableValue` — so a legitimate value
    /// never reaches this constant.
    static let maxAdvanceableValue: UInt64 = UInt64.max >> 1

    /// Strict-`<` cap for *tick emission*. `tickRange` keeps
    /// `high = effective + count` strictly below this constant, so max
    /// emit = `maxObservableValue - 1` (= `maxAdoptableValue`). One
    /// above `maxAdoptableValue` so any adopted value still has at least
    /// one tick of self-progression headroom — emit can produce the
    /// max-adopt boundary, but adopting that boundary would itself leave
    /// no headroom (the prior `< maxAdoptableValue` emit ceiling and
    /// `< maxAdoptableValue` adopt ceiling were structurally symmetric,
    /// so adopting the highest emittable value dead-ended the next
    /// `tickRange(count: 1)`; see codex-reviewer-1 loop II-VII final
    /// convergence trace).
    static let maxObservableValue: UInt64 = maxAdvanceableValue - 1

    /// Strict-`<` cap used at every *adoption* site (observe acceptance,
    /// materializer filename / op / row-stamp quarantine). The writer's
    /// own emit can reach exactly this value (`tickRange` uses the looser
    /// `maxObservableValue` ceiling), but peers — and the writer on
    /// adopt-paths — refuse to accept it because doing so would consume
    /// the last tick of headroom. A writer who ticks all the way to
    /// `maxAdoptableValue` is permanently at boundary: their own next
    /// `tickRange(count: 1)` throws `advanceExhausted` and their final
    /// emit is unobservable to peers (this is the inherent end-of-clock
    /// state). Persisted poison detection uses the looser
    /// `maxObservableValue` so the writer's legitimate boundary emit
    /// survives restart rather than getting reset to 0.
    static let maxAdoptableValue: UInt64 = maxObservableValue - 1

    init(initial: UInt64 = 0) {
        self.current = initial
    }

    func value() -> UInt64 {
        current
    }

    func observe(_ external: UInt64) {
        guard external < LamportClock.maxAdoptableValue else {
            lamportLog.warning("ignore observed clock at/above safe ceiling external=\(external, privacy: .public)")
            return
        }
        if external > current {
            current = external
        }
    }

    func tick() throws -> UInt64 {
        try ensureHeadroom(current: current, count: 1)
        let (next, _) = current.addingReportingOverflow(1)
        current = next
        return current
    }

    struct Range: Equatable, Sendable {
        let low: UInt64
        let high: UInt64
    }

    func tickRange(count: Int) throws -> Range {
        precondition(count > 0, "tickRange count must be positive")
        try ensureHeadroom(current: current, count: UInt64(count))
        let (low, _) = current.addingReportingOverflow(1)
        let (high, _) = current.addingReportingOverflow(UInt64(count))
        current = high
        return Range(low: low, high: high)
    }

    private func ensureHeadroom(current: UInt64, count: UInt64) throws {
        guard count > 0 else { return }
        // Emitted `high = current + count` must stay strictly below
        // `maxObservableValue` (the absolute tick-emit ceiling). Loose
        // by one from `maxAdoptableValue` so adopting the max-adopt
        // value (`maxAdoptableValue - 1`) still has one tick of
        // headroom for our own progression — without this asymmetry,
        // a peer at the highest emittable value was observed into our
        // persisted clock, and `tickRange(1)` immediately threw
        // `advanceExhausted` and the writer was permanently stuck.
        if count >= LamportClock.maxObservableValue
            || current >= LamportClock.maxObservableValue
            || current >= LamportClock.maxObservableValue - count {
            throw LamportClockError.advanceExhausted(current: current, requested: Int(min(count, UInt64(Int.max))))
        }
    }
}

enum LamportClockError: Error, Equatable {
    case advanceExhausted(current: UInt64, requested: Int)
}

actor PersistedLamportClock {
    private let database: DatabaseManager
    private let profileID: Int64
    private let repoID: String
    private var current: UInt64
    /// True when init substituted a poisoned `repo_state.lastClock` with 0.
    /// Forces the next observe (even no-op) to repair the row so the poison
    /// doesn't survive a session where no tick fires (e.g. observedClock == 0
    /// on a remote with no surviving peer ops).
    private var dbPoisonedAtInit: Bool

    init(database: DatabaseManager, profileID: Int64, repoID: String, initial: UInt64) {
        self.database = database
        self.profileID = profileID
        self.repoID = repoID
        // Defensive: if the persisted store was previously poisoned by a corrupt
        // remote (e.g. a pre-fix install observed a `UInt64.max`-class clock or
        // a pre-fix tickRange emitted at/above the new headroom ceiling),
        // reset the actor-local mirror to 0 so allocation can resume.
        // Threshold uses `maxObservableValue` (the absolute tick-emit
        // ceiling) — values at exactly `maxAdoptableValue` are the
        // writer's legitimate boundary emit and must survive restart, so
        // poison detection only triggers strictly above the tick ceiling.
        if initial >= LamportClock.maxObservableValue {
            self.current = 0
            self.dbPoisonedAtInit = true
        } else {
            self.current = initial
            self.dbPoisonedAtInit = false
        }
    }

    func value() -> UInt64 {
        current
    }

    func observe(_ external: UInt64) throws {
        guard external < LamportClock.maxAdoptableValue else {
            lamportLog.warning("ignore observed clock at/above safe ceiling external=\(external, privacy: .public)")
            return
        }
        guard external > current else {
            // observe is a no-op for this value, but if init detected poison
            // and nothing has cleared it yet, force a repair so the DB row
            // doesn't survive across a session where no tick fires.
            if dbPoisonedAtInit {
                try repairPoisonedDBIfNeeded()
            }
            return
        }
        // persist returns the post-write DB high-water; a concurrent peer may have
        // already pushed it past `external`, in which case our actor-local `current`
        // must adopt that higher value rather than regress to `external`.
        let dbHighWater = try persist(value: external)
        current = max(external, dbHighWater)
        dbPoisonedAtInit = false
    }

    /// Idempotent DB-row repair for installs whose `repo_state.lastClock` is
    /// poisoned. Needed because `observe(value)` no-ops when `value <= current`
    /// (so the DB poison survives the session if the materializer's clock is
    /// 0), and `persist`'s legacy `lastClock < ?` predicate couldn't overwrite
    /// an exact-ceiling positive value with a smaller sanitized one.
    func repairPoisonedDBIfNeeded() throws {
        try database.write { [profileID, repoID, current] db in
            guard let raw = try Self.readPersistedClock(db: db, profileID: profileID, repoID: repoID) else {
                throw PersistedLamportClockError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            guard raw >= LamportClock.maxObservableValue else { return }
            let signed = Int64(bitPattern: current)
            try db.execute(
                sql: """
                UPDATE \(RepoStateRecord.databaseTableName)
                SET lastClock = ?
                WHERE profileID = ? AND repoID = ?
                """,
                arguments: [signed, profileID, repoID]
            )
            lamportLog.warning("repaired poisoned persisted lastClock=\(raw, privacy: .public) to sanitized=\(current, privacy: .public) for repo=\(repoID, privacy: .public)")
        }
        dbPoisonedAtInit = false
    }

    /// Allocates `count` ticks under a single write-transaction read-then-write so
    /// concurrent clocks (FG + BG on the same profile) can't both hand out overlapping
    /// ranges from stale local state. Conditional UPDATE alone wasn't enough — see
    /// SeqAllocator.allocate for the same race.
    func tickRange(count: Int) throws -> LamportClock.Range {
        precondition(count > 0, "tickRange count must be positive")
        let range: LamportClock.Range = try database.write { [profileID, repoID, current] db in
            guard let dbCurrentRaw = try Self.readPersistedClock(db: db, profileID: profileID, repoID: repoID) else {
                throw PersistedLamportClockError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            // If the DB row was poisoned by a pre-fix install (e.g. observed a
            // remote `UInt64.max`-class clock and persisted it raw, or a pre-
            // fix tickRange emitted at/above the `maxObservableValue`
            // headroom ceiling which is now an unobservable dead-end), reset
            // the local high-water to 0 so allocation can resume. The new
            // `high` value written at the end of this transaction overwrites
            // the poisoned row. Loses the (corrupted) history, but the
            // sanitized materializer's observedClock — applied via `observe`
            // upstream of any allocation in normal startup — pushes us back
            // up before the first tick on the happy path.
            let dbCurrent: UInt64
            if dbCurrentRaw >= LamportClock.maxObservableValue {
                lamportLog.warning("reset poisoned persisted lastClock=\(dbCurrentRaw, privacy: .public) to 0 for repo=\(repoID, privacy: .public); next observe restores real high-water")
                dbCurrent = 0
            } else {
                dbCurrent = dbCurrentRaw
            }
            let effective = max(current, dbCurrent)
            let countU = UInt64(count)
            // Headroom check replaces a prior `fatalError` — a single corrupt or
            // hostile peer metadata value used to crash every later writer.
            // Emitted `high = effective + count` must stay strictly below
            // `maxObservableValue` (the absolute tick-emit ceiling). The
            // emitted high may equal `maxAdoptableValue` (one above the
            // adopt ceiling) — peers will refuse to adopt it, but the
            // emit itself is the writer's legitimate boundary tick.
            guard countU < LamportClock.maxObservableValue,
                  effective < LamportClock.maxObservableValue,
                  effective < LamportClock.maxObservableValue - countU else {
                throw PersistedLamportClockError.advanceExhausted(current: effective, requested: count)
            }
            let (low, _) = effective.addingReportingOverflow(1)
            let (high, _) = effective.addingReportingOverflow(countU)
            let signed = Int64(bitPattern: high)
            try db.execute(
                sql: """
                UPDATE \(RepoStateRecord.databaseTableName)
                SET lastClock = ?
                WHERE profileID = ? AND repoID = ?
                """,
                arguments: [signed, profileID, repoID]
            )
            return LamportClock.Range(low: low, high: high)
        }
        current = range.high
        dbPoisonedAtInit = false
        return range
    }

    private func persist(value: UInt64) throws -> UInt64 {
        // Bumps the persisted clock only if `value` is higher — used by `observe`
        // where conditional advance (no regression) is the desired semantic.
        // Returns the post-write DB value so observe can match in-memory `current`
        // to it; otherwise a rejected UPDATE (DB already higher) would leave
        // `current < dbValue` and future `value()` reads would under-report.
        let signed = Int64(bitPattern: value)
        return try database.write { [profileID, repoID] db in
            guard let beforeRaw = try Self.readPersistedClock(db: db, profileID: profileID, repoID: repoID) else {
                throw PersistedLamportClockError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            // A poisoned `before` reading (pre-fix install left a value above
            // the safe ceiling, OR persisted at/above `maxObservableValue`
            // from a pre-fix tickRange emit that is now an unobservable
            // dead-end) must not be returned as the high-water: `observe`
            // would then adopt it into the actor-local mirror and get stuck.
            // The conditional `lastClock < ?` UPDATE only fires when the
            // stored value is smaller, which holds for negative-signed
            // (`UInt64.max`-class) poison but NOT for an exact-ceiling
            // positive row. Force an unconditional overwrite on the
            // poisoned branch.
            let before: UInt64
            if beforeRaw >= LamportClock.maxObservableValue {
                before = value
                try db.execute(
                    sql: """
                    UPDATE \(RepoStateRecord.databaseTableName)
                    SET lastClock = ?
                    WHERE profileID = ? AND repoID = ?
                    """,
                    arguments: [signed, profileID, repoID]
                )
            } else {
                before = beforeRaw
                try db.execute(
                    sql: """
                    UPDATE \(RepoStateRecord.databaseTableName)
                    SET lastClock = ?
                    WHERE profileID = ? AND repoID = ? AND lastClock < ?
                    """,
                    arguments: [signed, profileID, repoID, signed]
                )
            }
            return max(before, value)
        }
    }

    private static func readPersistedClock(db: Database, profileID: Int64, repoID: String) throws -> UInt64? {
        guard let row = try Row.fetchOne(
            db,
            sql: "SELECT lastClock FROM \(RepoStateRecord.databaseTableName) WHERE profileID = ? AND repoID = ?",
            arguments: [profileID, repoID]
        ),
              let signed = row["lastClock"] as? Int64 else {
            return nil
        }
        return UInt64(bitPattern: signed)
    }
}

enum PersistedLamportClockError: Error, Equatable {
    case missingRepoState(profileID: Int64, repoID: String)
    case advanceExhausted(current: UInt64, requested: Int)
}

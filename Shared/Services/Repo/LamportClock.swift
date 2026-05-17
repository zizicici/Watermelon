import Foundation
import GRDB

actor LamportClock {
    private var current: UInt64

    init(initial: UInt64 = 0) {
        self.current = initial
    }

    func value() -> UInt64 {
        current
    }

    func observe(_ external: UInt64) {
        if external > current {
            current = external
        }
    }

    func tick() -> UInt64 {
        let (next, overflow) = current.addingReportingOverflow(1)
        guard !overflow else {
            fatalError("Lamport clock overflow at tick: current=\(current)")
        }
        current = next
        return current
    }

    struct Range: Equatable, Sendable {
        let low: UInt64
        let high: UInt64
    }

    func tickRange(count: Int) -> Range {
        precondition(count > 0, "tickRange count must be positive")
        let (low, lowOverflow) = current.addingReportingOverflow(1)
        let (high, highOverflow) = current.addingReportingOverflow(UInt64(count))
        guard !lowOverflow && !highOverflow else {
            fatalError("Lamport clock overflow at tickRange(count: \(count)): current=\(current)")
        }
        current = high
        return Range(low: low, high: high)
    }
}

actor PersistedLamportClock {
    private let database: DatabaseManager
    private let profileID: Int64
    private let repoID: String
    private var current: UInt64

    init(database: DatabaseManager, profileID: Int64, repoID: String, initial: UInt64) {
        self.database = database
        self.profileID = profileID
        self.repoID = repoID
        self.current = initial
    }

    func value() -> UInt64 {
        current
    }

    func observe(_ external: UInt64) throws {
        guard external > current else { return }
        // persist returns the post-write DB high-water; a concurrent peer may have
        // already pushed it past `external`, in which case our actor-local `current`
        // must adopt that higher value rather than regress to `external`.
        let dbHighWater = try persist(value: external)
        current = max(external, dbHighWater)
    }

    /// Allocates `count` ticks under a single write-transaction read-then-write so
    /// concurrent clocks (FG + BG on the same profile) can't both hand out overlapping
    /// ranges from stale local state. Conditional UPDATE alone wasn't enough — see
    /// SeqAllocator.allocate for the same race.
    func tickRange(count: Int) throws -> LamportClock.Range {
        precondition(count > 0, "tickRange count must be positive")
        let range: LamportClock.Range = try database.write { [profileID, repoID, current] db in
            guard let dbCurrent = try Self.readPersistedClock(db: db, profileID: profileID, repoID: repoID) else {
                throw PersistedLamportClockError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            let effective = max(current, dbCurrent)
            let (low, lowOverflow) = effective.addingReportingOverflow(1)
            let (high, highOverflow) = effective.addingReportingOverflow(UInt64(count))
            guard !lowOverflow && !highOverflow else {
                fatalError("Persisted Lamport clock overflow at tickRange(count: \(count)): effective=\(effective)")
            }
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
            guard let before = try Self.readPersistedClock(db: db, profileID: profileID, repoID: repoID) else {
                throw PersistedLamportClockError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            try db.execute(
                sql: """
                UPDATE \(RepoStateRecord.databaseTableName)
                SET lastClock = ?
                WHERE profileID = ? AND repoID = ? AND lastClock < ?
                """,
                arguments: [signed, profileID, repoID, signed]
            )
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

enum PersistedLamportClockError: Error {
    case missingRepoState(profileID: Int64, repoID: String)
}

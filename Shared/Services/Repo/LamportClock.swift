import Foundation
import GRDB
import os.log

private let lamportLog = Logger(subsystem: "com.zizicici.watermelon", category: "LamportClock")

actor LamportClock {
    private var current: UInt64

    /// Signed persistence ceiling; values at or above it are corrupt peer metadata.
    static let maxAdvanceableValue: UInt64 = UInt64.max >> 1

    /// Tick emission keeps one more slot than adoption so a max-adopted peer clock can still advance.
    static let maxObservableValue: UInt64 = maxAdvanceableValue - 1

    /// Adoption is stricter than emission so accepting a peer clock cannot consume the last tick.
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
        // Emission uses the looser ceiling so an adopted boundary value still leaves one local tick.
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
    /// Forces no-op observe to repair a poisoned DB row before session exit.
    private var dbPoisonedAtInit: Bool

    init(database: DatabaseManager, profileID: Int64, repoID: String, initial: UInt64) {
        self.database = database
        self.profileID = profileID
        self.repoID = repoID
        // Reset persisted poison above the emit ceiling; a legitimate boundary emit must survive restart.
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
            // Even a no-op observe must repair a poisoned DB row before the session exits.
            if dbPoisonedAtInit {
                try repairPoisonedDBIfNeeded()
            }
            return
        }
        // Adopt the post-write DB high-water so concurrent writers cannot make this actor regress.
        let dbHighWater = try persist(value: external)
        current = max(external, dbHighWater)
        dbPoisonedAtInit = false
    }

    /// Repairs poisoned DB rows even when observe is otherwise a no-op.
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

    /// Allocates inside one read-then-write transaction so concurrent runners cannot overlap ranges.
    func tickRange(count: Int) throws -> LamportClock.Range {
        precondition(count > 0, "tickRange count must be positive")
        let range: LamportClock.Range = try database.write { [profileID, repoID, current] db in
            guard let dbCurrentRaw = try Self.readPersistedClock(db: db, profileID: profileID, repoID: repoID) else {
                throw PersistedLamportClockError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            // Treat persisted values above the emit ceiling as poison so allocation can resume.
            let dbCurrent: UInt64
            if dbCurrentRaw >= LamportClock.maxObservableValue {
                lamportLog.warning("reset poisoned persisted lastClock=\(dbCurrentRaw, privacy: .public) to 0 for repo=\(repoID, privacy: .public); next observe restores real high-water")
                dbCurrent = 0
            } else {
                dbCurrent = dbCurrentRaw
            }
            let effective = max(current, dbCurrent)
            let countU = UInt64(count)
            // Throw on exhausted headroom; corrupt peer clocks must not crash every future writer.
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
        // Return the DB high-water so observe keeps actor state aligned after a rejected update.
        let signed = Int64(bitPattern: value)
        return try database.write { [profileID, repoID] db in
            guard let beforeRaw = try Self.readPersistedClock(db: db, profileID: profileID, repoID: repoID) else {
                throw PersistedLamportClockError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            // Force-overwrite poisoned rows because conditional advance cannot lower an exact-ceiling value.
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

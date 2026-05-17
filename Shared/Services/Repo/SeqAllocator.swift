import Foundation
import GRDB

actor SeqAllocator {
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

    func observeRemoteMax(_ remoteMax: UInt64) throws {
        guard remoteMax > current else { return }
        // persist returns the post-write DB high-water; if a concurrent allocator
        // already pushed it past `remoteMax`, our actor-local `current` must adopt
        // the higher value rather than regress to `remoteMax`.
        let dbHighWater = try persist(value: remoteMax)
        current = max(remoteMax, dbHighWater)
    }

    /// Allocates the next seq under a single write-transaction read-then-write so
    /// concurrent allocators (FG + BG on the same profile) can't both return the same
    /// seq from stale local state. The conditional UPDATE alone wasn't enough — its
    /// "WHERE lastSeq < ?" silently skips when DB is ahead, but we'd still bump our
    /// local `current` and return a seq someone else already used.
    func allocate() throws -> UInt64 {
        let next = try database.write { [profileID, repoID, current] db in
            guard let dbCurrent = try Self.readPersistedSeq(db: db, profileID: profileID, repoID: repoID) else {
                throw SeqAllocatorError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            let effective = max(current, dbCurrent)
            // `&+ 1` would wrap to 0 and overwrite an earlier commit at the same `(writerID, seq)` path.
            guard effective < UInt64.max else {
                throw SeqAllocatorError.exhausted
            }
            let next = effective + 1
            let signed = Int64(bitPattern: next)
            try db.execute(
                sql: """
                UPDATE \(RepoStateRecord.databaseTableName)
                SET lastSeq = ?
                WHERE profileID = ? AND repoID = ?
                """,
                arguments: [signed, profileID, repoID]
            )
            return next
        }
        current = next
        return next
    }

    enum SeqAllocatorError: Error {
        case missingRepoState(profileID: Int64, repoID: String)
        case exhausted
    }

    private func persist(value: UInt64) throws -> UInt64 {
        // Bumps the persisted seq if `value` is higher than what the DB currently holds.
        // Used by `observeRemoteMax` where conditional advance is the desired semantic.
        // Returns the post-write DB value so the caller can match in-memory `current`
        // to it; otherwise a rejected UPDATE would leave `current < dbValue` and
        // future `value()` reads would under-report.
        let signed = Int64(bitPattern: value)
        return try database.write { [profileID, repoID] db in
            guard let before = try Self.readPersistedSeq(db: db, profileID: profileID, repoID: repoID) else {
                throw SeqAllocatorError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            try db.execute(
                sql: """
                UPDATE \(RepoStateRecord.databaseTableName)
                SET lastSeq = ?
                WHERE profileID = ? AND repoID = ? AND lastSeq < ?
                """,
                arguments: [signed, profileID, repoID, signed]
            )
            return max(before, value)
        }
    }

    private static func readPersistedSeq(db: Database, profileID: Int64, repoID: String) throws -> UInt64? {
        guard let row = try Row.fetchOne(
            db,
            sql: "SELECT lastSeq FROM \(RepoStateRecord.databaseTableName) WHERE profileID = ? AND repoID = ?",
            arguments: [profileID, repoID]
        ),
              let signed = row["lastSeq"] as? Int64 else {
            return nil
        }
        return UInt64(bitPattern: signed)
    }
}

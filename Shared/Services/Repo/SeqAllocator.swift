import Foundation
import GRDB
import os.log

private let seqAllocatorLog = Logger(subsystem: "com.zizicici.watermelon", category: "SeqAllocator")

actor SeqAllocator {
    private let database: DatabaseManager
    private let profileID: Int64
    private let repoID: String
    private var current: UInt64

    init(database: DatabaseManager, profileID: Int64, repoID: String, initial: UInt64) {
        self.database = database
        self.profileID = profileID
        self.repoID = repoID
        self.current = RepoStateAuthority.sanitizeInitialSeq(initial).value
    }

    func value() -> UInt64 {
        current
    }

    func observeRemoteMax(_ remoteMax: UInt64) throws {
        let repoID = self.repoID
        guard remoteMax <= RepoStateAuthority.maxPersistableSeq else {
            seqAllocatorLog.warning("ignore remote seq above persistable ceiling repo=\(repoID, privacy: .public) seq=\(remoteMax, privacy: .public)")
            return
        }
        guard remoteMax > current else { return }
        let dbHighWater = try persist(value: remoteMax)
        current = max(remoteMax, dbHighWater)
    }

    func allocate() throws -> UInt64 {
        let next = try database.write { [profileID, repoID, current] db in
            guard let dbCurrent = try Self.readPersistedSeq(db: db, profileID: profileID, repoID: repoID) else {
                throw SeqAllocatorError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            let effective = max(current, dbCurrent)
            guard effective < RepoStateAuthority.maxPersistableSeq else {
                throw SeqAllocatorError.exhausted
            }
            let next = effective + 1
            let signed = try RepoStateAuthority.encodeSeq(next)
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
        return try database.write { [profileID, repoID] db in
            guard let before = try Self.readPersistedSeq(db: db, profileID: profileID, repoID: repoID) else {
                throw SeqAllocatorError.missingRepoState(profileID: profileID, repoID: repoID)
            }
            guard value <= RepoStateAuthority.maxPersistableSeq else {
                seqAllocatorLog.warning("ignore persist seq above persistable ceiling repo=\(repoID, privacy: .public) seq=\(value, privacy: .public)")
                return before
            }
            guard value > before else {
                return before
            }
            let signed = try RepoStateAuthority.encodeSeq(value)
            try db.execute(
                sql: """
                UPDATE \(RepoStateRecord.databaseTableName)
                SET lastSeq = ?
                WHERE profileID = ? AND repoID = ?
                """,
                arguments: [signed, profileID, repoID]
            )
            return value
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
        return RepoStateAuthority.decodePersistedSeq(signed).value
    }
}

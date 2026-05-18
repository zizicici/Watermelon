import Foundation
import GRDB

actor RepoIdentity {
    enum RepoIdentityError: Error {
        case profileNotFound(profileID: Int64)
    }

    private let database: DatabaseManager

    init(database: DatabaseManager) {
        self.database = database
    }

    func lazyEnsureWriterID(profileID: Int64) throws -> String {
        try database.write { db in
            guard var profile = try ServerProfileRecord.fetchOne(db, key: profileID) else {
                throw RepoIdentityError.profileNotFound(profileID: profileID)
            }
            if let existing = profile.writerID, !existing.isEmpty {
                return existing
            }
            let generated = UUID().uuidString.lowercased()
            profile.writerID = generated
            try profile.update(db)
            return generated
        }
    }

    func lazyEnsureRepoState(profileID: Int64, repoID: String, writerID: String) throws -> RepoStateRecord {
        try database.write { db in
            if let existing = try RepoStateRecord
                .filter(Column("profileID") == profileID)
                .filter(Column("repoID") == repoID)
                .fetchOne(db) {
                return existing
            }
            let fresh = RepoStateRecord(
                profileID: profileID,
                repoID: repoID,
                writerID: writerID,
                lastClock: 0,
                lastSeq: 0,
                migrationCompleted: 0
            )
            try fresh.insert(db)
            return fresh
        }
    }

    func loadRepoState(profileID: Int64, repoID: String) throws -> RepoStateRecord? {
        try database.read { db in
            try RepoStateRecord
                .filter(Column("profileID") == profileID)
                .filter(Column("repoID") == repoID)
                .fetchOne(db)
        }
    }

    // Allocator state is keyed by repoID; reuse the local row when remote repo.json is missing/unreachable.
    func findRepoStateByProfile(profileID: Int64) throws -> RepoStateRecord? {
        try database.read { db in
            let rows = try RepoStateRecord
                .filter(Column("profileID") == profileID)
                .order(Column("migrationCompleted").desc)
                .fetchAll(db)
            let candidates = rows.filter { row in
                RepoStateAuthority.isTrustedFallbackSeq(row.lastSeq)
            }
            return candidates.max { lhs, rhs in
                if lhs.migrationCompleted != rhs.migrationCompleted {
                    return lhs.migrationCompleted < rhs.migrationCompleted
                }
                let lhsSeq = RepoStateAuthority.decodePersistedSeq(lhs.lastSeq).value
                let rhsSeq = RepoStateAuthority.decodePersistedSeq(rhs.lastSeq).value
                if lhsSeq != rhsSeq {
                    return lhsSeq < rhsSeq
                }
                return lhs.repoID < rhs.repoID
            }
        }
    }

    func setMigrationCompleted(profileID: Int64, repoID: String) throws {
        try database.write { db in
            try db.execute(
                sql: """
                UPDATE \(RepoStateRecord.databaseTableName)
                SET migrationCompleted = 1
                WHERE profileID = ? AND repoID = ?
                """,
                arguments: [profileID, repoID]
            )
        }
    }

    nonisolated static func newRunID() -> String {
        UUID().uuidString.lowercased()
    }
}

import Foundation
import GRDB

final class ContentHashIndexRepository {
    private let databaseManager: DatabaseManager

    init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
    }

    func upsert(
        assetLocalIdentifier: String,
        resourceLocalIdentifier: String,
        contentHash: Data
    ) throws {
        try databaseManager.write { db in
            try db.execute(
                sql: """
                INSERT INTO content_hash_index (
                    assetLocalIdentifier,
                    resourceLocalIdentifier,
                    contentHash
                ) VALUES (?, ?, ?)
                ON CONFLICT(assetLocalIdentifier, resourceLocalIdentifier) DO UPDATE SET
                    contentHash = excluded.contentHash
                """,
                arguments: [assetLocalIdentifier, resourceLocalIdentifier, contentHash]
            )
        }
    }

    func fetchHashMapByAsset() throws -> [String: [Data]] {
        try databaseManager.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, contentHash
                FROM content_hash_index
                """
            )
            var result: [String: [Data]] = [:]
            result.reserveCapacity(rows.count)
            for row in rows {
                let assetID: String = row["assetLocalIdentifier"]
                let hash: Data = row["contentHash"]
                result[assetID, default: []].append(hash)
            }
            return result
        }
    }

    func fetchAllHashes() throws -> Set<Data> {
        try databaseManager.read { db in
            Set(try Data.fetchAll(db, sql: "SELECT contentHash FROM content_hash_index"))
        }
    }

    func clearAll() throws {
        try databaseManager.write { db in
            try db.execute(sql: "DELETE FROM content_hash_index")
        }
    }
}

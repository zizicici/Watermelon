import Foundation
import GRDB

final class ContentHashIndexRepository {
    private let databaseManager: DatabaseManager

    init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
    }

    func upsertAssetResource(
        assetLocalIdentifier: String,
        role: Int,
        slot: Int,
        contentHash: Data
    ) throws {
        try databaseManager.write { db in
            try db.execute(
                sql: """
                INSERT INTO local_asset_resources (
                    assetLocalIdentifier,
                    role,
                    slot,
                    contentHash
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(assetLocalIdentifier, role, slot) DO UPDATE SET
                    contentHash = excluded.contentHash
                """,
                arguments: [assetLocalIdentifier, role, slot, contentHash]
            )
        }
    }

    func upsertAssetFingerprint(
        assetLocalIdentifier: String,
        assetFingerprint: Data,
        resourceCount: Int
    ) throws {
        try databaseManager.write { db in
            try db.execute(
                sql: """
                INSERT INTO local_assets (
                    assetLocalIdentifier,
                    assetFingerprint,
                    resourceCount,
                    updatedAt
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(assetLocalIdentifier) DO UPDATE SET
                    assetFingerprint = excluded.assetFingerprint,
                    resourceCount = excluded.resourceCount,
                    updatedAt = excluded.updatedAt
                """,
                arguments: [
                    assetLocalIdentifier,
                    assetFingerprint,
                    resourceCount,
                    Date()
                ]
            )
        }
    }

    func fetchHashMapByAsset() throws -> [String: [Data]] {
        try databaseManager.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, contentHash
                FROM local_asset_resources
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

    func fetchHashToAssetMap() throws -> [Data: String] {
        try databaseManager.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, contentHash
                FROM local_asset_resources
                ORDER BY assetLocalIdentifier
                """
            )
            var result: [Data: String] = [:]
            result.reserveCapacity(rows.count)
            for row in rows {
                let assetID: String = row["assetLocalIdentifier"]
                let hash: Data = row["contentHash"]
                if result[hash] == nil {
                    result[hash] = assetID
                }
            }
            return result
        }
    }

    func fetchAssetFingerprintsByAsset() throws -> [String: Data] {
        try databaseManager.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, assetFingerprint
                FROM local_assets
                """
            )
            var result: [String: Data] = [:]
            result.reserveCapacity(rows.count)
            for row in rows {
                let assetID: String = row["assetLocalIdentifier"]
                let fingerprint: Data = row["assetFingerprint"]
                result[assetID] = fingerprint
            }
            return result
        }
    }

    func fetchAllHashes() throws -> Set<Data> {
        try databaseManager.read { db in
            Set(try Data.fetchAll(db, sql: "SELECT contentHash FROM local_asset_resources"))
        }
    }

    func fetchAllAssetFingerprints() throws -> Set<Data> {
        try databaseManager.read { db in
            Set(try Data.fetchAll(db, sql: "SELECT assetFingerprint FROM local_assets"))
        }
    }

    func clearAll() throws {
        try databaseManager.write { db in
            try db.execute(sql: "DELETE FROM local_asset_resources")
            try db.execute(sql: "DELETE FROM local_assets")
        }
    }
}

import Foundation
import GRDB

struct AssetResourceRoleSlot: Hashable {
    let role: Int
    let slot: Int
}

struct LocalAssetHashCache {
    let assetFingerprint: Data
    let resourceCount: Int
    let updatedAt: Date
    var hashesByRoleSlot: [AssetResourceRoleSlot: Data]
}

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

    func fetchHashMapByAsset(assetIDs: Set<String>) throws -> [String: [Data]] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            let sortedIDs = assetIDs.sorted()
            let placeholders = Array(repeating: "?", count: sortedIDs.count).joined(separator: ", ")
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, contentHash
                FROM local_asset_resources
                WHERE assetLocalIdentifier IN (\(placeholders))
                """,
                arguments: StatementArguments(sortedIDs)
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

    func fetchAssetFingerprintsByAsset(assetIDs: Set<String>) throws -> [String: Data] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            let sortedIDs = assetIDs.sorted()
            let placeholders = Array(repeating: "?", count: sortedIDs.count).joined(separator: ", ")
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, assetFingerprint
                FROM local_assets
                WHERE assetLocalIdentifier IN (\(placeholders))
                """,
                arguments: StatementArguments(sortedIDs)
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

    func fetchAssetHashCaches() throws -> [String: LocalAssetHashCache] {
        try databaseManager.read { db in
            let assetRows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, assetFingerprint, resourceCount, updatedAt
                FROM local_assets
                """
            )

            var result: [String: LocalAssetHashCache] = [:]
            result.reserveCapacity(assetRows.count)
            for row in assetRows {
                let assetID: String = row["assetLocalIdentifier"]
                result[assetID] = LocalAssetHashCache(
                    assetFingerprint: row["assetFingerprint"],
                    resourceCount: Int(row["resourceCount"] as Int64),
                    updatedAt: row["updatedAt"],
                    hashesByRoleSlot: [:]
                )
            }

            let resourceRows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, role, slot, contentHash
                FROM local_asset_resources
                """
            )

            for row in resourceRows {
                let assetID: String = row["assetLocalIdentifier"]
                guard var cache = result[assetID] else { continue }
                let role = Int(row["role"] as Int64)
                let slot = Int(row["slot"] as Int64)
                let key = AssetResourceRoleSlot(role: role, slot: slot)
                cache.hashesByRoleSlot[key] = row["contentHash"]
                result[assetID] = cache
            }

            return result
        }
    }
}

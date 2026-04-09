import Foundation
import GRDB

struct AssetResourceRoleSlot: Hashable {
    let role: Int
    let slot: Int
}

struct LocalAssetHashCache {
    let assetFingerprint: Data
    let resourceCount: Int
    let totalFileSizeBytes: Int64
    let updatedAt: Date
    var hashesByRoleSlot: [AssetResourceRoleSlot: Data]
}

struct LocalAssetResourceHashRecord: Sendable {
    let role: Int
    let slot: Int
    let contentHash: Data
    let fileSize: Int64
}

struct LocalHashIndexStats: Sendable {
    let assetCount: Int
    let resourceCount: Int
    let totalFileSizeBytes: Int64
    let oldestUpdatedAt: Date?
    let newestUpdatedAt: Date?
}

final class ContentHashIndexRepository: @unchecked Sendable {
    private let databaseManager: DatabaseManager

    init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
    }

    func upsertAssetHashSnapshot(
        assetLocalIdentifier: String,
        assetFingerprint: Data,
        resources: [LocalAssetResourceHashRecord],
        totalFileSizeBytes: Int64
    ) throws {
        try databaseManager.write { db in
            try db.execute(
                sql: """
                INSERT INTO local_assets (
                    assetLocalIdentifier,
                    assetFingerprint,
                    resourceCount,
                    totalFileSizeBytes,
                    updatedAt
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(assetLocalIdentifier) DO UPDATE SET
                    assetFingerprint = excluded.assetFingerprint,
                    resourceCount = excluded.resourceCount,
                    totalFileSizeBytes = excluded.totalFileSizeBytes,
                    updatedAt = excluded.updatedAt
                """,
                arguments: [
                    assetLocalIdentifier,
                    assetFingerprint,
                    resources.count,
                    totalFileSizeBytes,
                    Date()
                ]
            )

            try db.execute(
                sql: "DELETE FROM local_asset_resources WHERE assetLocalIdentifier = ?",
                arguments: [assetLocalIdentifier]
            )

            for resource in resources {
                try db.execute(
                    sql: """
                    INSERT INTO local_asset_resources (
                        assetLocalIdentifier,
                        role,
                        slot,
                        contentHash,
                        fileSize
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        assetLocalIdentifier,
                        resource.role,
                        resource.slot,
                        resource.contentHash,
                        resource.fileSize
                    ]
                )
            }
        }
    }

    func upsertAssetFingerprint(
        assetLocalIdentifier: String,
        assetFingerprint: Data,
        resourceCount: Int,
        totalFileSizeBytes: Int64
    ) throws {
        try databaseManager.write { db in
            try db.execute(
                sql: """
                INSERT INTO local_assets (
                    assetLocalIdentifier,
                    assetFingerprint,
                    resourceCount,
                    totalFileSizeBytes,
                    updatedAt
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(assetLocalIdentifier) DO UPDATE SET
                    assetFingerprint = excluded.assetFingerprint,
                    resourceCount = excluded.resourceCount,
                    totalFileSizeBytes = excluded.totalFileSizeBytes,
                    updatedAt = excluded.updatedAt
                """,
                arguments: [
                    assetLocalIdentifier,
                    assetFingerprint,
                    resourceCount,
                    totalFileSizeBytes,
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

    func fetchAssetHashCaches(assetIDs: Set<String>) throws -> [String: LocalAssetHashCache] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            let sortedIDs = assetIDs.sorted()
            let chunkSize = 400
            var result: [String: LocalAssetHashCache] = [:]

            for chunkStart in stride(from: 0, to: sortedIDs.count, by: chunkSize) {
                let chunk = Array(sortedIDs[chunkStart ..< min(chunkStart + chunkSize, sortedIDs.count)])
                let placeholders = Array(repeating: "?", count: chunk.count).joined(separator: ", ")

                let assetRows = try Row.fetchAll(
                    db,
                    sql: """
                    SELECT assetLocalIdentifier, assetFingerprint, resourceCount, totalFileSizeBytes, updatedAt
                    FROM local_assets
                    WHERE assetLocalIdentifier IN (\(placeholders))
                    """,
                    arguments: StatementArguments(chunk)
                )

                for row in assetRows {
                    let assetID: String = row["assetLocalIdentifier"]
                    result[assetID] = LocalAssetHashCache(
                        assetFingerprint: row["assetFingerprint"],
                        resourceCount: Int(row["resourceCount"] as Int64),
                        totalFileSizeBytes: row["totalFileSizeBytes"],
                        updatedAt: row["updatedAt"],
                        hashesByRoleSlot: [:]
                    )
                }

                let resourceRows = try Row.fetchAll(
                    db,
                    sql: """
                    SELECT assetLocalIdentifier, role, slot, contentHash
                    FROM local_asset_resources
                    WHERE assetLocalIdentifier IN (\(placeholders))
                    """,
                    arguments: StatementArguments(chunk)
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
            }

            return result
        }
    }

    func fetchFileSizeByAsset() throws -> [String: Int64] {
        try databaseManager.read { db in
            var result: [String: Int64] = [:]
            let rows = try Row.fetchAll(
                db,
                sql: "SELECT assetLocalIdentifier, totalFileSizeBytes FROM local_assets WHERE totalFileSizeBytes > 0"
            )
            for row in rows {
                let id: String = row["assetLocalIdentifier"]
                let size: Int64 = row["totalFileSizeBytes"]
                result[id] = size
            }
            return result
        }
    }

    func fetchTotalFileSizeBytes(assetIDs: Set<String>) throws -> Int64 {
        guard !assetIDs.isEmpty else { return 0 }

        return try databaseManager.read { db in
            let sortedIDs = assetIDs.sorted()
            let chunkSize = 400
            var totalBytes: Int64 = 0

            for chunkStart in stride(from: 0, to: sortedIDs.count, by: chunkSize) {
                let chunk = Array(sortedIDs[chunkStart ..< min(chunkStart + chunkSize, sortedIDs.count)])
                let placeholders = Array(repeating: "?", count: chunk.count).joined(separator: ", ")
                let chunkBytes = try Int64.fetchOne(
                    db,
                    sql: """
                    SELECT COALESCE(SUM(totalFileSizeBytes), 0)
                    FROM local_assets
                    WHERE assetLocalIdentifier IN (\(placeholders))
                    """,
                    arguments: StatementArguments(chunk)
                ) ?? 0
                totalBytes += max(chunkBytes, 0)
            }

            return totalBytes
        }
    }

    func fetchLocalHashIndexStats() throws -> LocalHashIndexStats {
        try databaseManager.read { db in
            let assetCount = try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM local_assets"
            ) ?? 0
            let resourceCount = try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM local_asset_resources"
            ) ?? 0
            let oldest = try Date.fetchOne(
                db,
                sql: "SELECT MIN(updatedAt) FROM local_assets"
            )
            let newest = try Date.fetchOne(
                db,
                sql: "SELECT MAX(updatedAt) FROM local_assets"
            )
            let totalFileSizeBytes = try Int64.fetchOne(
                db,
                sql: "SELECT COALESCE(SUM(totalFileSizeBytes), 0) FROM local_assets"
            ) ?? 0
            return LocalHashIndexStats(
                assetCount: assetCount,
                resourceCount: resourceCount,
                totalFileSizeBytes: totalFileSizeBytes,
                oldestUpdatedAt: oldest,
                newestUpdatedAt: newest
            )
        }
    }

    func clearLocalHashIndex() throws {
        try databaseManager.write { db in
            try db.execute(sql: "DELETE FROM local_asset_resources")
            try db.execute(sql: "DELETE FROM local_assets")
        }
    }

    func fetchIndexedAssetIDs() throws -> [String] {
        try databaseManager.read { db in
            try String.fetchAll(
                db,
                sql: "SELECT assetLocalIdentifier FROM local_assets"
            )
        }
    }

    func deleteIndexEntries(assetIDs: [String]) throws {
        guard !assetIDs.isEmpty else { return }
        let chunkSize = 400
        try databaseManager.write { db in
            for chunkStart in stride(from: 0, to: assetIDs.count, by: chunkSize) {
                let chunk = Array(assetIDs[chunkStart ..< min(chunkStart + chunkSize, assetIDs.count)])
                let placeholders = Array(repeating: "?", count: chunk.count).joined(separator: ",")
                try db.execute(
                    sql: "DELETE FROM local_asset_resources WHERE assetLocalIdentifier IN (\(placeholders))",
                    arguments: StatementArguments(chunk)
                )
                try db.execute(
                    sql: "DELETE FROM local_assets WHERE assetLocalIdentifier IN (\(placeholders))",
                    arguments: StatementArguments(chunk)
                )
            }
        }
    }
}


import Foundation
import GRDB
import os.log

private let contentHashIndexLog = Logger(subsystem: "com.zizicici.watermelon", category: "ContentHashIndex")

struct AssetResourceRoleSlot: Hashable, Sendable {
    let role: Int
    let slot: Int
}

struct LocalAssetHashCache: Sendable {
    let assetFingerprint: Data
    let resourceCount: Int
    let totalFileSizeBytes: Int64
    let updatedAt: Date
    var hashesByRoleSlot: [AssetResourceRoleSlot: Data]
    let selectionVersion: Int
    let resourceSignature: Data?
}

struct IndexedAssetRow: Sendable {
    let assetLocalIdentifier: String
    let assetFingerprint: Data
    let totalFileSizeBytes: Int64
    let updatedAt: Date
    let selectionVersion: Int
    let resourceSignature: Data?
}

struct LocalAssetFingerprintRecord: Sendable, Equatable {
    let fingerprint: Data
    let updatedAt: Date
    let selectionVersion: Int
    let resourceSignature: Data?
}

struct AssetSizeSnapshot: Sendable {
    let totalFileSizeBytes: Int64
    let modificationDateMs: Int64
}

struct AssetSizeUpdate: Sendable {
    let assetLocalIdentifier: String
    let totalFileSizeBytes: Int64
    let modificationDateMs: Int64
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
    // Under SQLITE_MAX_VARIABLE_NUMBER and small enough to keep prepared-statement cost flat.
    private static let idChunkSize = 400
    private static let sha256DigestLength = 32

    private let databaseManager: DatabaseManager

    init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
    }

    private static func forEachIDChunk<C: Collection>(
        _ ids: C,
        body: (_ chunk: [String], _ placeholders: String) throws -> Void
    ) rethrows where C.Element == String {
        var buffer: [String] = []
        buffer.reserveCapacity(idChunkSize)
        for id in ids {
            buffer.append(id)
            if buffer.count == idChunkSize {
                let placeholders = Array(repeating: "?", count: buffer.count).joined(separator: ", ")
                try body(buffer, placeholders)
                buffer.removeAll(keepingCapacity: true)
            }
        }
        if !buffer.isEmpty {
            let placeholders = Array(repeating: "?", count: buffer.count).joined(separator: ", ")
            try body(buffer, placeholders)
        }
    }

    func upsertAssetHashSnapshot(
        assetLocalIdentifier: String,
        assetFingerprint: Data,
        resources: [LocalAssetResourceHashRecord],
        totalFileSizeBytes: Int64,
        modificationDateMs: Int64?,
        selectionVersion: Int,
        resourceSignature: Data
    ) throws {
        try databaseManager.write { db in
            try Self.writeLocalAssetRow(
                db,
                assetLocalIdentifier: assetLocalIdentifier,
                assetFingerprint: assetFingerprint,
                resourceCount: resources.count,
                totalFileSizeBytes: totalFileSizeBytes,
                modificationDateMs: modificationDateMs,
                selectionVersion: selectionVersion,
                resourceSignature: resourceSignature
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
        totalFileSizeBytes: Int64,
        modificationDateMs: Int64?
    ) throws {
        try databaseManager.write { db in
            try Self.writeLocalAssetRow(
                db,
                assetLocalIdentifier: assetLocalIdentifier,
                assetFingerprint: assetFingerprint,
                resourceCount: resourceCount,
                totalFileSizeBytes: totalFileSizeBytes,
                modificationDateMs: modificationDateMs,
                selectionVersion: nil,
                resourceSignature: nil
            )
        }
    }

    // COALESCE preserves existing mtime / selectionVersion when callers pass nil — a hash write must not null out fields a prior size-only scan populated.
    private static func writeLocalAssetRow(
        _ db: Database,
        assetLocalIdentifier: String,
        assetFingerprint: Data,
        resourceCount: Int,
        totalFileSizeBytes: Int64,
        modificationDateMs: Int64?,
        selectionVersion: Int?,
        resourceSignature: Data?
    ) throws {
        try db.execute(
            sql: """
            INSERT INTO local_assets (
                assetLocalIdentifier,
                assetFingerprint,
                resourceCount,
                totalFileSizeBytes,
                modificationDateMs,
                selectionVersion,
                resourceSignature,
                updatedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(assetLocalIdentifier) DO UPDATE SET
                assetFingerprint = excluded.assetFingerprint,
                resourceCount = excluded.resourceCount,
                totalFileSizeBytes = excluded.totalFileSizeBytes,
                modificationDateMs = COALESCE(excluded.modificationDateMs, modificationDateMs),
                selectionVersion = CASE WHEN excluded.selectionVersion > 0 THEN excluded.selectionVersion ELSE selectionVersion END,
                resourceSignature = COALESCE(excluded.resourceSignature, resourceSignature),
                updatedAt = excluded.updatedAt
            """,
            arguments: [
                assetLocalIdentifier,
                assetFingerprint,
                resourceCount,
                totalFileSizeBytes,
                modificationDateMs,
                selectionVersion ?? 0,
                resourceSignature,
                Date()
            ]
        )
    }

    func fetchAssetFingerprintRecords() throws -> [String: LocalAssetFingerprintRecord] {
        try databaseManager.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, assetFingerprint, updatedAt, selectionVersion, resourceSignature
                FROM local_assets
                WHERE assetFingerprint IS NOT NULL
                """
            )
            var result: [String: LocalAssetFingerprintRecord] = [:]
            result.reserveCapacity(rows.count)
            for row in rows {
                let assetID: String = row["assetLocalIdentifier"]
                result[assetID] = LocalAssetFingerprintRecord(
                    fingerprint: row["assetFingerprint"],
                    updatedAt: row["updatedAt"],
                    selectionVersion: Int(row["selectionVersion"] as Int64? ?? 0),
                    resourceSignature: row["resourceSignature"] as Data?
                )
            }
            return result
        }
    }

    func fetchAssetFingerprintRecords(assetIDs: Set<String>) throws -> [String: LocalAssetFingerprintRecord] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            var result: [String: LocalAssetFingerprintRecord] = [:]
            result.reserveCapacity(assetIDs.count)
            try Self.forEachIDChunk(assetIDs) { chunk, placeholders in
                let rows = try Row.fetchAll(
                    db,
                    sql: """
                    SELECT assetLocalIdentifier, assetFingerprint, updatedAt, selectionVersion, resourceSignature
                    FROM local_assets
                    WHERE assetLocalIdentifier IN (\(placeholders))
                      AND assetFingerprint IS NOT NULL
                    """,
                    arguments: StatementArguments(chunk)
                )
                for row in rows {
                    let assetID: String = row["assetLocalIdentifier"]
                    result[assetID] = LocalAssetFingerprintRecord(
                        fingerprint: row["assetFingerprint"],
                        updatedAt: row["updatedAt"],
                        selectionVersion: Int(row["selectionVersion"] as Int64? ?? 0),
                        resourceSignature: row["resourceSignature"] as Data?
                    )
                }
            }
            return result
        }
    }

    func fetchValidIndexedRows(assetIDs: Set<String>) throws -> [String: IndexedAssetRow] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            var result: [String: IndexedAssetRow] = [:]
            try Self.forEachIDChunk(assetIDs) { chunk, placeholders in
                let rows = try Row.fetchAll(
                    db,
                    sql: """
                    SELECT assetLocalIdentifier, assetFingerprint, totalFileSizeBytes, updatedAt, selectionVersion, resourceSignature
                    FROM local_assets
                    WHERE assetLocalIdentifier IN (\(placeholders))
                      AND assetFingerprint IS NOT NULL
                    """,
                    arguments: StatementArguments(chunk)
                )
                for row in rows {
                    let assetID: String = row["assetLocalIdentifier"]
                    result[assetID] = IndexedAssetRow(
                        assetLocalIdentifier: assetID,
                        assetFingerprint: row["assetFingerprint"],
                        totalFileSizeBytes: row["totalFileSizeBytes"],
                        updatedAt: row["updatedAt"],
                        selectionVersion: Int(row["selectionVersion"] as Int64? ?? 0),
                        resourceSignature: row["resourceSignature"] as Data?
                    )
                }
            }
            return result
        }
    }

    func fetchAssetHashCaches(assetIDs: Set<String>) throws -> [String: LocalAssetHashCache] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            var result: [String: LocalAssetHashCache] = [:]
            try Self.forEachIDChunk(assetIDs) { chunk, placeholders in
                let assetRows = try Row.fetchAll(
                    db,
                    sql: """
                    SELECT assetLocalIdentifier, assetFingerprint, resourceCount, totalFileSizeBytes, updatedAt, selectionVersion, resourceSignature
                    FROM local_assets
                    WHERE assetLocalIdentifier IN (\(placeholders))
                      AND assetFingerprint IS NOT NULL
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
                        hashesByRoleSlot: [:],
                        selectionVersion: Int(row["selectionVersion"] as Int64? ?? 0),
                        resourceSignature: row["resourceSignature"] as Data?
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

    func fetchAssetSizes() throws -> [String: AssetSizeSnapshot] {
        try databaseManager.read { db in
            var result: [String: AssetSizeSnapshot] = [:]
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, totalFileSizeBytes, modificationDateMs
                FROM local_assets
                WHERE totalFileSizeBytes > 0 AND modificationDateMs IS NOT NULL
                """
            )
            result.reserveCapacity(rows.count)
            for row in rows {
                let id: String = row["assetLocalIdentifier"]
                let size: Int64 = row["totalFileSizeBytes"]
                let mtime: Int64 = row["modificationDateMs"]
                result[id] = AssetSizeSnapshot(
                    totalFileSizeBytes: size,
                    modificationDateMs: mtime
                )
            }
            return result
        }
    }

    func upsertAssetSizes(_ entries: [AssetSizeUpdate]) throws {
        guard !entries.isEmpty else { return }
        try databaseManager.write { db in
            let now = Date()
            // Size-only writes must not bump `updatedAt` (reuse-decision key vs PHAsset.modificationDate) and must reject older mtimes (out-of-order detached writers).
            let statement = try db.makeStatement(sql: """
                INSERT INTO local_assets (
                    assetLocalIdentifier,
                    totalFileSizeBytes,
                    modificationDateMs,
                    updatedAt
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(assetLocalIdentifier) DO UPDATE SET
                    totalFileSizeBytes = excluded.totalFileSizeBytes,
                    modificationDateMs = excluded.modificationDateMs
                WHERE modificationDateMs IS NULL
                   OR excluded.modificationDateMs >= modificationDateMs
                """)
            for entry in entries {
                try statement.setArguments([
                    entry.assetLocalIdentifier,
                    entry.totalFileSizeBytes,
                    entry.modificationDateMs,
                    now
                ])
                try statement.execute()
            }
        }
    }

    func fetchTotalFileSizeBytes(assetIDs: Set<String>) throws -> Int64 {
        guard !assetIDs.isEmpty else { return 0 }

        return try databaseManager.read { db in
            var totalBytes: Int64 = 0
            try Self.forEachIDChunk(assetIDs) { chunk, placeholders in
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
                sql: "SELECT COUNT(*) FROM local_assets WHERE assetFingerprint IS NOT NULL"
            ) ?? 0
            let resourceCount = try Int.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM local_asset_resources"
            ) ?? 0
            let oldest = try Date.fetchOne(
                db,
                sql: "SELECT MIN(updatedAt) FROM local_assets WHERE assetFingerprint IS NOT NULL"
            )
            let newest = try Date.fetchOne(
                db,
                sql: "SELECT MAX(updatedAt) FROM local_assets WHERE assetFingerprint IS NOT NULL"
            )
            let totalFileSizeBytes = try Int64.fetchOne(
                db,
                sql: "SELECT COALESCE(SUM(totalFileSizeBytes), 0) FROM local_assets WHERE assetFingerprint IS NOT NULL"
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
                sql: "SELECT assetLocalIdentifier FROM local_assets WHERE assetFingerprint IS NOT NULL"
            )
        }
    }

    func writeHashIndex(
        assetLocalIdentifier: String,
        remoteAssetFingerprint: Data,
        instances: [RemoteAssetResourceInstance]
    ) throws {
        guard !instances.isEmpty else {
            try deleteIndexEntries(assetIDs: [assetLocalIdentifier])
            return
        }
        let records = instances.map { instance in
            LocalAssetResourceHashRecord(
                role: instance.role,
                slot: instance.slot,
                contentHash: instance.resourceHash,
                fileSize: instance.fileSize
            )
        }
        let totalSize = instances.reduce(Int64(0)) { partial, instance in
            partial + instance.fileSize
        }
        if instances.allSatisfy({ $0.resourceHash.count == Self.sha256DigestLength }) {
            let importedFingerprint = BackupAssetResourcePlanner.assetFingerprint(
                resourceRoleSlotHashes: instances.map { instance in
                    (role: instance.role, slot: instance.slot, contentHash: instance.resourceHash)
                }
            )
            if importedFingerprint != remoteAssetFingerprint {
                contentHashIndexLog.info("[HashIndex] fingerprint mismatch for \(assetLocalIdentifier, privacy: .private): preserving remote restore identity")
            }
        } else {
            contentHashIndexLog.info("[HashIndex] legacy restore identity for \(assetLocalIdentifier, privacy: .private): preserving remote fingerprint without full local hashes")
        }
        let signature = BackupAssetResourcePlanner.resourceSignature(
            roleSlots: instances.map { (role: $0.role, slot: $0.slot) }
        )
        try upsertAssetHashSnapshot(
            assetLocalIdentifier: assetLocalIdentifier,
            assetFingerprint: remoteAssetFingerprint,
            resources: records,
            totalFileSizeBytes: totalSize,
            modificationDateMs: nil,
            selectionVersion: BackupAssetResourcePlanner.currentSelectionVersion,
            resourceSignature: signature
        )
    }

    func deleteIndexEntries(assetIDs: [String]) throws {
        guard !assetIDs.isEmpty else { return }
        try databaseManager.write { db in
            try Self.forEachIDChunk(assetIDs) { chunk, placeholders in
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

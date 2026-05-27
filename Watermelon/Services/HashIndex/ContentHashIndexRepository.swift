import Foundation
import GRDB

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
    let assetLocalIdentifier: PhotoKitLocalIdentifier
    let assetFingerprint: Data
    let totalFileSizeBytes: Int64
    let updatedAt: Date
    let selectionVersion: Int
    let resourceSignature: Data?
}

struct DuplicateIndexedAssetRow: Sendable {
    let assetLocalIdentifier: PhotoKitLocalIdentifier
    let assetFingerprint: Data
    let updatedAt: Date
    let selectionVersion: Int
    let resourceSignature: Data?
}

struct DuplicateIndexedAssetCandidate: Sendable {
    let assetFingerprint: Data
    let rows: [DuplicateIndexedAssetRow]
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
    let assetLocalIdentifier: PhotoKitLocalIdentifier
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

    private let databaseManager: DatabaseManager

    init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
    }

    // Raw String chunker — the SQLite codec binds String, conversion happens at the public API boundary.
    private static func forEachRawIDChunk(
        _ ids: [String],
        body: (_ chunk: [String], _ placeholders: String) throws -> Void
    ) rethrows {
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
        assetLocalIdentifier: PhotoKitLocalIdentifier,
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
                assetLocalIdentifier: assetLocalIdentifier.rawValue,
                assetFingerprint: assetFingerprint,
                resourceCount: resources.count,
                totalFileSizeBytes: totalFileSizeBytes,
                modificationDateMs: modificationDateMs,
                selectionVersion: selectionVersion,
                resourceSignature: resourceSignature
            )

            try db.execute(
                sql: "DELETE FROM local_asset_resources WHERE assetLocalIdentifier = ?",
                arguments: [assetLocalIdentifier.rawValue]
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
                        assetLocalIdentifier.rawValue,
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
        assetLocalIdentifier: PhotoKitLocalIdentifier,
        assetFingerprint: Data,
        resourceCount: Int,
        totalFileSizeBytes: Int64,
        modificationDateMs: Int64?
    ) throws {
        try databaseManager.write { db in
            try Self.writeLocalAssetRow(
                db,
                assetLocalIdentifier: assetLocalIdentifier.rawValue,
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

    func fetchAssetFingerprintRecords() throws -> [PhotoKitLocalIdentifier: LocalAssetFingerprintRecord] {
        try databaseManager.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, assetFingerprint, updatedAt, selectionVersion, resourceSignature
                FROM local_assets
                WHERE assetFingerprint IS NOT NULL
                """
            )
            var result: [PhotoKitLocalIdentifier: LocalAssetFingerprintRecord] = [:]
            result.reserveCapacity(rows.count)
            for row in rows {
                let assetID = PhotoKitLocalIdentifier(rawValue: row["assetLocalIdentifier"])
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

    func fetchAssetFingerprintRecords(assetIDs: Set<PhotoKitLocalIdentifier>) throws -> [PhotoKitLocalIdentifier: LocalAssetFingerprintRecord] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            var result: [PhotoKitLocalIdentifier: LocalAssetFingerprintRecord] = [:]
            result.reserveCapacity(assetIDs.count)
            try Self.forEachRawIDChunk(Array(assetIDs.rawValues)) { chunk, placeholders in
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
                    let assetID = PhotoKitLocalIdentifier(rawValue: row["assetLocalIdentifier"])
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

    func fetchValidIndexedRows(assetIDs: Set<PhotoKitLocalIdentifier>) throws -> [PhotoKitLocalIdentifier: IndexedAssetRow] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            var result: [PhotoKitLocalIdentifier: IndexedAssetRow] = [:]
            try Self.forEachRawIDChunk(Array(assetIDs.rawValues)) { chunk, placeholders in
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
                    let assetID = PhotoKitLocalIdentifier(rawValue: row["assetLocalIdentifier"])
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

    func fetchPotentiallyUsableIndexedAssetCount(minSelectionVersion: Int) throws -> Int {
        try databaseManager.read { db in
            try Int.fetchOne(
                db,
                sql: """
                SELECT COUNT(*)
                FROM local_assets
                WHERE assetFingerprint IS NOT NULL
                  AND resourceSignature IS NOT NULL
                  AND selectionVersion >= ?
                """,
                arguments: [minSelectionVersion]
            ) ?? 0
        }
    }

    func fetchDuplicateIndexedAssetCandidates(
        minSelectionVersion: Int
    ) throws -> [DuplicateIndexedAssetCandidate] {
        try databaseManager.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                WITH candidate_fingerprints AS (
                    SELECT assetFingerprint
                    FROM local_assets
                    WHERE assetFingerprint IS NOT NULL
                      AND resourceSignature IS NOT NULL
                      AND selectionVersion >= ?
                    GROUP BY assetFingerprint
                    HAVING COUNT(*) > 1
                )
                SELECT
                    la.assetLocalIdentifier,
                    la.assetFingerprint,
                    la.updatedAt,
                    la.selectionVersion,
                    la.resourceSignature
                FROM local_assets la
                JOIN candidate_fingerprints cf
                  ON la.assetFingerprint = cf.assetFingerprint
                WHERE la.assetFingerprint IS NOT NULL
                  AND la.resourceSignature IS NOT NULL
                  AND la.selectionVersion >= ?
                ORDER BY la.assetFingerprint, la.assetLocalIdentifier
                """,
                arguments: [minSelectionVersion, minSelectionVersion]
            )

            var candidates: [DuplicateIndexedAssetCandidate] = []
            var currentFingerprint: Data?
            var currentRows: [DuplicateIndexedAssetRow] = []

            func flush() {
                guard let fingerprint = currentFingerprint, !currentRows.isEmpty else { return }
                candidates.append(DuplicateIndexedAssetCandidate(
                    assetFingerprint: fingerprint,
                    rows: currentRows
                ))
                currentRows.removeAll(keepingCapacity: true)
            }

            for row in rows {
                let fingerprint: Data = row["assetFingerprint"]
                if currentFingerprint != fingerprint {
                    flush()
                    currentFingerprint = fingerprint
                }
                currentRows.append(DuplicateIndexedAssetRow(
                    assetLocalIdentifier: PhotoKitLocalIdentifier(rawValue: row["assetLocalIdentifier"]),
                    assetFingerprint: fingerprint,
                    updatedAt: row["updatedAt"],
                    selectionVersion: Int(row["selectionVersion"] as Int64? ?? 0),
                    resourceSignature: row["resourceSignature"] as Data?
                ))
            }
            flush()

            return candidates
        }
    }

    func fetchAssetHashCaches(assetIDs: Set<PhotoKitLocalIdentifier>) throws -> [PhotoKitLocalIdentifier: LocalAssetHashCache] {
        guard !assetIDs.isEmpty else { return [:] }

        return try databaseManager.read { db in
            var result: [PhotoKitLocalIdentifier: LocalAssetHashCache] = [:]
            try Self.forEachRawIDChunk(Array(assetIDs.rawValues)) { chunk, placeholders in
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
                    let assetID = PhotoKitLocalIdentifier(rawValue: row["assetLocalIdentifier"])
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
                    let assetID = PhotoKitLocalIdentifier(rawValue: row["assetLocalIdentifier"])
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

    func fetchAssetSizes() throws -> [PhotoKitLocalIdentifier: AssetSizeSnapshot] {
        try databaseManager.read { db in
            var result: [PhotoKitLocalIdentifier: AssetSizeSnapshot] = [:]
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
                let id = PhotoKitLocalIdentifier(rawValue: row["assetLocalIdentifier"])
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
                    entry.assetLocalIdentifier.rawValue,
                    entry.totalFileSizeBytes,
                    entry.modificationDateMs,
                    now
                ])
                try statement.execute()
            }
        }
    }

    func fetchTotalFileSizeBytes(assetIDs: Set<PhotoKitLocalIdentifier>) throws -> Int64 {
        guard !assetIDs.isEmpty else { return 0 }

        return try databaseManager.read { db in
            var totalBytes: Int64 = 0
            try Self.forEachRawIDChunk(Array(assetIDs.rawValues)) { chunk, placeholders in
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

    func fetchIndexedAssetIDs() throws -> [PhotoKitLocalIdentifier] {
        try databaseManager.read { db in
            try String.fetchAll(
                db,
                sql: "SELECT assetLocalIdentifier FROM local_assets WHERE assetFingerprint IS NOT NULL"
            ).map { PhotoKitLocalIdentifier(rawValue: $0) }
        }
    }

    func deleteIndexEntries(assetIDs: [PhotoKitLocalIdentifier]) throws {
        guard !assetIDs.isEmpty else { return }
        try databaseManager.write { db in
            try Self.forEachRawIDChunk(assetIDs.rawValues) { chunk, placeholders in
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

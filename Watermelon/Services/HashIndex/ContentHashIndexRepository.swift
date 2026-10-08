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
}

struct IndexedAssetRow: Sendable {
    let assetLocalIdentifier: String
    let assetFingerprint: Data
    let totalFileSizeBytes: Int64
    let updatedAt: Date
}

struct LocalAssetFingerprintRecord: Sendable, Equatable {
    let fingerprint: Data
    let updatedAt: Date
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

struct LocalAssetResourceHashRecord: Sendable, Codable {
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
    // Conservative bound well under SQLITE_MAX_VARIABLE_NUMBER (default 32766 on iOS); large
    // IN lists also balloon SQL parse + prepared-statement bind cost, so staying around 400
    // gives predictable per-chunk latency.
    private static let idChunkSize = 400

    private let databaseManager: DatabaseManager

    init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
    }

    /// Iterate `ids` in chunks of `idChunkSize`, invoking `body` with the chunk array and
    /// the matching "?, ?, ?" placeholder string. No sort — callers key results by asset ID.
    private static func forEachIDChunk<C: Collection>(
        _ ids: C,
        body: (_ chunk: [C.Element], _ placeholders: String) throws -> Void
    ) rethrows where C.Element: DatabaseValueConvertible {
        var buffer: [C.Element] = []
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
        modificationDateMs: Int64?
    ) throws {
        try databaseManager.write { db in
            try Self.writeLocalAssetRow(
                db,
                assetLocalIdentifier: assetLocalIdentifier,
                assetFingerprint: assetFingerprint,
                resourceCount: resources.count,
                totalFileSizeBytes: totalFileSizeBytes,
                modificationDateMs: modificationDateMs
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
            try Self.validatePendingRestoreOrigins(db, assetID: assetLocalIdentifier,
                fingerprint: assetFingerprint, resources: resources, modificationDateMs: modificationDateMs)
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
                modificationDateMs: modificationDateMs
            )
        }
    }

    // COALESCE preserves the existing modificationDateMs when callers can't supply one
    // (writeHashIndex from the remote-restore path passes nil). Size-only scans write
    // mtime via upsertAssetSizes; a subsequent hash write must not null it out.
    private static func writeLocalAssetRow(
        _ db: Database,
        assetLocalIdentifier: String,
        assetFingerprint: Data,
        resourceCount: Int,
        totalFileSizeBytes: Int64,
        modificationDateMs: Int64?
    ) throws {
        try db.execute(
            sql: """
            INSERT INTO local_assets (
                assetLocalIdentifier,
                assetFingerprint,
                resourceCount,
                totalFileSizeBytes,
                modificationDateMs,
                updatedAt
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(assetLocalIdentifier) DO UPDATE SET
                assetFingerprint = excluded.assetFingerprint,
                resourceCount = excluded.resourceCount,
                totalFileSizeBytes = excluded.totalFileSizeBytes,
                modificationDateMs = COALESCE(excluded.modificationDateMs, modificationDateMs),
                updatedAt = excluded.updatedAt
            """,
            arguments: [
                assetLocalIdentifier,
                assetFingerprint,
                resourceCount,
                totalFileSizeBytes,
                modificationDateMs,
                Date()
            ]
        )
    }

    func fetchAssetFingerprintRecords() throws -> [String: LocalAssetFingerprintRecord] {
        try databaseManager.read { db in
            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, assetFingerprint, updatedAt
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
                    updatedAt: row["updatedAt"]
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
                    SELECT assetLocalIdentifier, assetFingerprint, updatedAt
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
                        updatedAt: row["updatedAt"]
                    )
                }
            }
            return result
        }
    }

    // Reverse map fingerprint -> a local asset identifier still present in the library, built in one
    // pass for the remote browser to render thumbnails locally without a remote download. Duplicates
    // (same content, multiple assets) resolve to an arbitrary first match.
    func fetchLocalIdentifiersByFingerprint() throws -> [Data: String] {
        try databaseManager.read { db in
            var result: [Data: String] = [:]
            let cursor = try Row.fetchCursor(
                db,
                sql: """
                SELECT assetLocalIdentifier, assetFingerprint
                FROM local_assets
                WHERE assetFingerprint IS NOT NULL
                """
            )
            while let row = try cursor.next() {
                let fingerprint: Data = row["assetFingerprint"]
                let identifier: String = row["assetLocalIdentifier"]
                if result[fingerprint] == nil {
                    result[fingerprint] = identifier
                }
            }
            return result
        }
    }

    // All rows per fingerprint, newest-updated first — the batch validator's fallback when the reverse
    // map's arbitrary candidate fails the current-bytes check (an older stale row can shadow a current one).
    func fetchAssetIDsByFingerprints(_ fingerprints: Set<Data>) throws -> [Data: [String]] {
        guard !fingerprints.isEmpty else { return [:] }

        return try databaseManager.read { db in
            var result: [Data: [String]] = [:]
            try Self.forEachIDChunk(fingerprints) { chunk, placeholders in
                let rows = try Row.fetchAll(
                    db,
                    sql: """
                    SELECT assetLocalIdentifier, assetFingerprint
                    FROM local_assets
                    WHERE assetFingerprint IN (\(placeholders))
                    ORDER BY updatedAt DESC
                    """,
                    arguments: StatementArguments(chunk)
                )
                for row in rows {
                    let fingerprint: Data = row["assetFingerprint"]
                    let identifier: String = row["assetLocalIdentifier"]
                    result[fingerprint, default: []].append(identifier)
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
                    SELECT assetLocalIdentifier, assetFingerprint, totalFileSizeBytes, updatedAt
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
                        updatedAt: row["updatedAt"]
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
                    SELECT assetLocalIdentifier, assetFingerprint, resourceCount, totalFileSizeBytes, updatedAt
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
            // On conflict:
            // - Don't touch assetFingerprint/resourceCount: hash-indexed rows keep their identity.
            // - Don't touch updatedAt: it tracks "last hash write", which AssetProcessor and
            //   LocalHashIndexBuildService compare against PHAsset.modificationDate to decide
            //   whether to reuse a cached hash. Bumping it from a size-only write would make
            //   a post-modification scan look "newer than the photo edit" and silently revive
            //   a stale hash.
            // - Reject stale mtime writes: fire-and-forget detached write-backs are not ordered
            //   across scans, so guard against an older batch overwriting a newer one.
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

    func recordRestoreImport(
        profileKey: String,
        remoteFingerprint: Data,
        assetLocalIdentifier: String,
        instances: [RemoteAssetResourceInstance],
        isIncomplete: Bool,
        importedAt: Date = Date()
    ) throws {
        let resources = instances.map {
            LocalAssetResourceHashRecord(role: $0.role, slot: $0.slot, contentHash: $0.resourceHash, fileSize: $0.fileSize)
        }
        let fingerprint = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: resources.map {
            (role: $0.role, slot: $0.slot, contentHash: $0.contentHash)
        })
        let complete = !isIncomplete && !resources.isEmpty && fingerprint == remoteFingerprint
            && resources.allSatisfy { $0.contentHash.count == 32 }
        let encoded = try JSONEncoder().encode(resources)
        try databaseManager.write { db in
            try db.execute(sql: """
                INSERT INTO restore_origins
                (profileKey, remoteFingerprint, assetLocalIdentifier, sourceResources, completeCandidate, importedAtMs)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(profileKey, remoteFingerprint, assetLocalIdentifier) DO NOTHING
                """, arguments: [profileKey, remoteFingerprint, assetLocalIdentifier, encoded, complete, importedAt.millisecondsSinceEpoch])
        }
    }

    private static func validatePendingRestoreOrigins(
        _ db: Database,
        assetID: String,
        fingerprint: Data,
        resources: [LocalAssetResourceHashRecord],
        modificationDateMs: Int64?
    ) throws {
        let pending = try Row.fetchAll(db, sql: """
            SELECT profileKey, remoteFingerprint, sourceResources, completeCandidate, importedAtMs
            FROM restore_origins WHERE assetLocalIdentifier = ? AND localFingerprint IS NULL
            """, arguments: [assetID])
        for row in pending {
            let encoded: Data = row["sourceResources"]
            let expected = try JSONDecoder().decode([LocalAssetResourceHashRecord].self, from: encoded)
            let importedAtMs: Int64 = row["importedAtMs"]
            let complete: Bool = row["completeCandidate"]
            let unchanged = modificationDateMs.map { $0 <= importedAtMs } ?? false
            let equivalent = complete && unchanged && restoredResourcesMatch(expected: expected, actual: resources)
            try db.execute(sql: """
                UPDATE restore_origins SET localFingerprint = ?, isEquivalent = ?
                WHERE profileKey = ? AND remoteFingerprint = ? AND assetLocalIdentifier = ?
                """, arguments: [fingerprint, equivalent, row["profileKey"] as String, row["remoteFingerprint"] as Data, assetID])
        }
    }

    static func restoredResourcesMatch(expected: [LocalAssetResourceHashRecord], actual: [LocalAssetResourceHashRecord]) -> Bool {
        guard expected.count == actual.count, !expected.isEmpty else { return false }
        let expectedKeys = Set(expected.map { AssetResourceRoleSlot(role: $0.role, slot: $0.slot) })
        let actualKeys = Set(actual.map { AssetResourceRoleSlot(role: $0.role, slot: $0.slot) })
        guard expectedKeys.count == expected.count, expectedKeys == actualKeys else { return false }
        return expected.allSatisfy { source in
            guard let imported = actual.first(where: { $0.role == source.role && $0.slot == source.slot }) else { return false }
            // PhotoKit rewrites adjustmentData during an import owned by this app.
            return source.role == ResourceTypeCode.adjustmentData
                || source.contentHash == imported.contentHash
        }
    }

    func pendingRestoreAssetIDs(profileKey: String, remoteFingerprint: Data) throws -> [String] {
        try databaseManager.read { db in
            try String.fetchAll(db, sql: """
                SELECT assetLocalIdentifier FROM restore_origins
                WHERE profileKey = ? AND remoteFingerprint = ? AND completeCandidate = 1 AND localFingerprint IS NULL
                ORDER BY importedAtMs
                """, arguments: [profileKey, remoteFingerprint])
        }
    }

    func fetchRestoreOrigins(profileKey: String?, assetIDs: Set<String>? = nil) throws -> [RestoreOrigin] {
        try fetchRestoreOrigins(profileKey: profileKey, filterColumn: "assetLocalIdentifier",
            filterValues: assetIDs?.map(\.databaseValue))
    }

    func fetchRestoreOrigins(profileKey: String?, remoteFingerprints: Set<Data>) throws -> [RestoreOrigin] {
        try fetchRestoreOrigins(profileKey: profileKey, filterColumn: "remoteFingerprint",
            filterValues: remoteFingerprints.map(\.databaseValue))
    }

    private func fetchRestoreOrigins(
        profileKey: String?,
        filterColumn: String,
        filterValues: [DatabaseValue]?
    ) throws -> [RestoreOrigin] {
        guard let profileKey else { return [] }
        if let filterValues, filterValues.isEmpty { return [] }
        return try databaseManager.read { db in
            let query = """
                SELECT o.assetLocalIdentifier, o.localFingerprint, o.remoteFingerprint
                FROM restore_origins o JOIN local_assets a ON a.assetLocalIdentifier = o.assetLocalIdentifier
                WHERE o.profileKey = ? AND o.isEquivalent = 1 AND o.localFingerprint = a.assetFingerprint
                """
            var rows: [Row] = []
            if let filterValues {
                try Self.forEachIDChunk(filterValues) { chunk, placeholders in
                    rows += try Row.fetchAll(db, sql: query + " AND o.\(filterColumn) IN (\(placeholders))",
                        arguments: StatementArguments([profileKey.databaseValue] + chunk))
                }
            } else {
                rows = try Row.fetchAll(db, sql: query, arguments: [profileKey])
            }
            return rows.map { RestoreOrigin(assetLocalIdentifier: $0["assetLocalIdentifier"],
                localFingerprint: $0["localFingerprint"], remoteFingerprint: $0["remoteFingerprint"]) }
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
        instances: [RemoteAssetResourceInstance],
        modificationDateMs: Int64? = nil
    ) throws {
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
        let onDeviceFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: instances.lazy.map {
                (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash)
            }
        )
        try upsertAssetHashSnapshot(
            assetLocalIdentifier: assetLocalIdentifier,
            assetFingerprint: onDeviceFingerprint,
            resources: records,
            totalFileSizeBytes: totalSize,
            modificationDateMs: modificationDateMs
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

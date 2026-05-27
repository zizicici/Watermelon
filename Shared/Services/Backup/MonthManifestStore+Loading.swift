import Foundation
import GRDB
import os.log

private let manifestLoadLog = Logger(subsystem: "com.zizicici.watermelon", category: "MonthManifestStore")

extension MonthManifestStore {
    /// V1-only entry point: V2-mode workers go through `V2MonthSession.loadOrCreate` instead.
    /// `seed` is used by legacy import (LegacyMigrationExecutor) — when provided, skips
    /// the remote manifest download and seeds the local sqlite directly.
    static func loadOrCreate(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        seed: Seed? = nil,
        stepLogger: MonthManifestStepLogger? = nil
    ) async throws -> MonthManifestStore {
        if let seed {
            return try await loadSeeded(
                client: client,
                basePath: basePath,
                year: year,
                month: month,
                seed: seed,
                stepLogger: stepLogger
            )
        }

        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        do {
            try await client.createDirectory(path: monthAbsolutePath)
        } catch {
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.createMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }

        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbsolutePath)
        } catch {
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.listMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }
        let manifestExists = entries.contains { $0.name == Self.manifestFileName && !$0.isDirectory }
        let remoteFilesByName = Self.dedupedRemoteFilesByName(entries: entries, year: year, month: month)

        let localURL = Self.makeLocalManifestURL(year: year, month: month)

        let manifestAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRelativePath + "/" + Self.manifestFileName
        )

        if manifestExists {
            do {
                try await client.download(remotePath: manifestAbsolutePath, localURL: localURL)
            } catch {
                try? FileManager.default.removeItem(at: localURL)
                throw NSError(
                    domain: "MonthManifestStore",
                    code: -31,
                    userInfo: [
                        NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                            String(localized: "backup.manifest.error.downloadExistingManifest"),
                            monthRelativePath
                        ),
                        NSUnderlyingErrorKey: error
                    ]
                )
            }
        }

        let prepared: PreparedManifestQueue
        do {
            prepared = try Self.prepareLocalManifest(
                localURL: localURL,
                origin: manifestExists ? .downloadedFromRemote : .freshlyCreated
            )
        } catch {
            try? FileManager.default.removeItem(at: localURL)
            if manifestExists {
                throw NSError(
                    domain: "MonthManifestStore",
                    code: -32,
                    userInfo: [
                        NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                            String(localized: "backup.manifest.error.existingManifestInvalid"),
                            monthRelativePath
                        ),
                        NSUnderlyingErrorKey: error
                    ]
                )
            }
            prepared = try Self.prepareLocalManifest(
                localURL: localURL,
                origin: .freshlyCreated
            )
        }

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: prepared.queue,
            remoteFilesByName: remoteFilesByName,
            dirty: prepared.requiresRemoteSync,
            stepLogger: stepLogger
        )

        // reloadCache doubles as the integrity check: corruption surfaces
        // here before flush can overwrite remote with a bad manifest.
        try store.reloadCache()

        _ = try await store.reconcileWithRemoteListing(Set(remoteFilesByName.keys))

        return store
    }

    static func loadSeeded(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        seed: Seed,
        stepLogger: MonthManifestStepLogger? = nil
    ) async throws -> MonthManifestStore {
        let localURL = Self.makeLocalManifestURL(year: year, month: month)

        let dbQueue = try DatabaseQueue(path: localURL.path)
        try Self.migrate(dbQueue)

        // Fresh months don't exist on backends like SMB/WebDAV/SFTP until createDirectory.
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        do {
            try await client.createDirectory(path: monthAbsolutePath)
        } catch {
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.createMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }
        // List actual remote directory to detect orphaned files (uploaded but
        // not recorded in manifest due to crash / force-kill).
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbsolutePath)
        } catch {
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.listMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }
        let remoteFilesByName = Self.dedupedRemoteFilesByName(entries: entries, year: year, month: month)

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: dbQueue,
            remoteFilesByName: remoteFilesByName,
            dirty: false,
            stepLogger: stepLogger
        )
        try store.seedDatabase(seed)
        try store.reloadCache()
        // V1 sqlite schema doesn't persist crypto — overlay it back so the next snapshot
        // round-trip preserves Stage-2 metadata. Skipped when crypto is nil (the common case).
        store.overlaySeedCrypto(seed.resources)

        _ = try await store.reconcileWithRemoteListing(Set(remoteFilesByName.keys))

        return store
    }

    static func loadManifestOnlyIfExists(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int
    ) async throws -> MonthManifestStore? {
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let manifestAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRelativePath + "/" + Self.manifestFileName
        )

        guard let manifestEntry = try await client.metadata(path: manifestAbsolutePath),
              manifestEntry.isDirectory == false else {
            return nil
        }

        return try await loadManifestDirect(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            manifestAbsolutePath: manifestAbsolutePath
        )
    }

    /// Downloads and loads a manifest directly, skipping the existence check.
    /// Use when the caller has already confirmed the manifest exists (e.g., via scanManifestDigests).
    /// Pass `pushSchemaUpgrade: false` when reading a manifest you don't own (e.g. legacy-import
    /// scanning a backup folder) so a schema migration doesn't trigger flushToRemote on the source.
    ///
    /// Returns nil ONLY when the manifest is confirmed absent (metadata not found).
    /// Transient transport / permission errors throw — V1MigrationService.runPhase1
    /// would otherwise treat them as "no manifest", skip the month, phase3 deletes the
    /// real V1 manifests on remote → silent data loss.
    static func loadManifestDirect(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        manifestAbsolutePath: String? = nil,
        pushSchemaUpgrade: Bool = true
    ) async throws -> MonthManifestStore? {
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let absPath = manifestAbsolutePath ?? {
            return RemotePathBuilder.absolutePath(
                basePath: basePath,
                remoteRelativePath: monthRelativePath + "/" + Self.manifestFileName
            )
        }()

        // metadata() distinguishes "not found" (legitimate nil) from transport failures
        // that download() would otherwise mask.
        guard try await client.metadata(path: absPath) != nil else {
            return nil
        }

        let localURL = Self.makeLocalManifestURL(year: year, month: month)

        do {
            try await client.download(remotePath: absPath, localURL: localURL)
        } catch {
            try? FileManager.default.removeItem(at: localURL)
            throw error
        }

        let prepared: PreparedManifestQueue
        do {
            prepared = try Self.prepareLocalManifest(
                localURL: localURL,
                origin: .downloadedFromRemote
            )
        } catch {
            try? FileManager.default.removeItem(at: localURL)
            throw NSError(
                domain: "MonthManifestStore",
                code: -34,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadedManifestInvalid"),
                        monthRelativePath
                    ),
                    NSUnderlyingErrorKey: error
                ]
            )
        }

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: prepared.queue,
            remoteFilesByName: [:],
            dirty: prepared.requiresRemoteSync
        )

        do {
            try store.reloadCache()
        } catch {
            throw NSError(
                domain: "MonthManifestStore",
                code: -35,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "backup.manifest.error.downloadedManifestInvalid"),
                        monthRelativePath
                    ),
                    NSUnderlyingErrorKey: error
                ]
            )
        }

        if prepared.requiresRemoteSync && pushSchemaUpgrade {
            try await store.flushToRemote()
        }

        return store
    }

    func seedDatabase(_ seed: Seed) throws {
        try dbQueue.write { db in
            try db.execute(sql: "DELETE FROM asset_resources")
            try db.execute(sql: "DELETE FROM assets")
            try db.execute(sql: "DELETE FROM resources")

            for resource in seed.resources {
                try db.execute(
                    sql: """
                    INSERT INTO resources (
                        fileName,
                        contentHash,
                        fileSize,
                        resourceType,
                        creationDateMs,
                        backedUpAtMs
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        resource.logicalName,
                        resource.contentHash,
                        resource.fileSize,
                        resource.resourceType,
                        resource.creationDateMs,
                        resource.backedUpAtMs
                    ]
                )
            }

            for asset in seed.assets {
                try db.execute(
                    sql: """
                    INSERT INTO assets (
                        assetFingerprint,
                        creationDateMs,
                        backedUpAtMs,
                        resourceCount,
                        totalFileSizeBytes
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        asset.assetFingerprint.rawValue,
                        asset.creationDateMs,
                        asset.backedUpAtMs,
                        asset.resourceCount,
                        asset.totalFileSizeBytes
                    ]
                )
            }

            for link in seed.assetResourceLinks.sorted(by: { lhs, rhs in
                if lhs.assetFingerprint != rhs.assetFingerprint {
                    return lhs.assetFingerprint.rawValue.lexicographicallyPrecedes(rhs.assetFingerprint.rawValue)
                }
                if lhs.role != rhs.role { return lhs.role < rhs.role }
                return lhs.slot < rhs.slot
            }) {
                try db.execute(
                    sql: """
                    INSERT INTO asset_resources (
                        assetFingerprint,
                        resourceHash,
                        role,
                        slot
                    ) VALUES (?, ?, ?, ?)
                    """,
                    arguments: [
                        link.assetFingerprint.rawValue,
                        link.resourceHash,
                        link.role,
                        link.slot
                    ]
                )
            }
        }
    }

    func reloadCache() throws {
        let resourceRows = try dbQueue.read { db in
            try Row.fetchAll(
                db,
                sql: """
                SELECT fileName, contentHash, fileSize, resourceType, creationDateMs, backedUpAtMs
                FROM resources
                """
            )
        }

        var resourcesByName: [String: RemoteManifestResource] = [:]
        resourcesByName.reserveCapacity(resourceRows.count)
        var resourcesByHash: [Data: String] = [:]
        resourcesByHash.reserveCapacity(resourceRows.count)

        let monthRel = String(format: "%04d/%02d", year, month)
        for row in resourceRows {
            let leaf: String = row["fileName"]
            let item = RemoteManifestResource(
                year: year,
                month: month,
                physicalRemotePath: monthRel + "/" + leaf,
                contentHash: row["contentHash"],
                fileSize: row["fileSize"],
                resourceType: Int(row["resourceType"] as Int64),
                creationDateMs: row["creationDateMs"],
                backedUpAtMs: row["backedUpAtMs"]
            )
            resourcesByName[item.logicalName] = item
            resourcesByHash[item.contentHash] = item.logicalName
        }

        let assetRows = try dbQueue.read { db in
            try Row.fetchAll(
                db,
                sql: """
                SELECT assetFingerprint, creationDateMs, backedUpAtMs, resourceCount, totalFileSizeBytes
                FROM assets
                """
            )
        }

        var assetsByFingerprint: [AssetFingerprint: RemoteManifestAsset] = [:]
        assetsByFingerprint.reserveCapacity(assetRows.count)

        for row in assetRows {
            let blob: Data = row["assetFingerprint"]
            guard let fingerprint = AssetFingerprint(decoding: blob) else {
                throw Self.invalidAssetFingerprintBlobError(table: "assets", actualByteCount: blob.count)
            }
            let asset = RemoteManifestAsset(
                year: year,
                month: month,
                assetFingerprint: fingerprint,
                creationDateMs: row["creationDateMs"],
                backedUpAtMs: row["backedUpAtMs"],
                resourceCount: Int(row["resourceCount"] as Int64),
                totalFileSizeBytes: row["totalFileSizeBytes"]
            )
            assetsByFingerprint[fingerprint] = asset
        }

        let linkRows = try dbQueue.read { db in
            try Row.fetchAll(
                db,
                sql: """
                SELECT assetFingerprint, resourceHash, role, slot
                FROM asset_resources
                ORDER BY assetFingerprint, role, slot
                """
            )
        }

        var linksByFingerprint: [AssetFingerprint: [RemoteAssetResourceLink]] = [:]
        linksByFingerprint.reserveCapacity(assetsByFingerprint.count)

        for row in linkRows {
            let resourceHash: Data = row["resourceHash"]
            let blob: Data = row["assetFingerprint"]
            guard let fingerprint = AssetFingerprint(decoding: blob) else {
                throw Self.invalidAssetFingerprintBlobError(table: "asset_resources", actualByteCount: blob.count)
            }
            let link = RemoteAssetResourceLink(
                year: year,
                month: month,
                assetFingerprint: fingerprint,
                resourceHash: resourceHash,
                role: Int(row["role"] as Int64),
                slot: Int(row["slot"] as Int64),
                logicalName: resourcesByHash[resourceHash] ?? ""
            )
            linksByFingerprint[link.assetFingerprint, default: []].append(link)
        }

        itemsByFileName = resourcesByName
        itemsByHash = resourcesByHash
        self.assetsByFingerprint = assetsByFingerprint
        assetLinksByFingerprint = linksByFingerprint
        existingFileNameSet = Set(resourcesByName.keys).union(remoteFilesByName.keys)
        rebuildLinkIndexes()
        invalidateCollisionKeyCache()
    }

    // Fail-closed manifest blob corruption — short/non-32-byte assetFingerprint must not vanish silently;
    // surface as a manifest-corruption error so callers route it through the existing legacy-quarantine
    // and re-load paths (V1MigrationResidueQuarantine, MonthManifestStore.loadOrInitialize catch).
    static func invalidAssetFingerprintBlobError(table: String, actualByteCount: Int) -> NSError {
        NSError(
            domain: "MonthManifestStore",
            code: -42,
            userInfo: [
                NSLocalizedDescriptionKey: "Invalid assetFingerprint blob in \(table): expected 32 bytes, got \(actualByteCount)"
            ]
        )
    }

    // Defensive: case-insensitive volumes / buggy listings can surface the same name twice.
    static func dedupedRemoteFilesByName(
        entries: [RemoteStorageEntry],
        year: Int,
        month: Int
    ) -> [String: RemoteFileMetadata] {
        var result: [String: RemoteFileMetadata] = [:]
        result.reserveCapacity(entries.count)
        for entry in entries where !entry.isDirectory && entry.name != Self.manifestFileName {
            if let existing = result[entry.name] {
                manifestLoadLog.error("[MonthManifestStore] duplicate remote entry month=\(year)-\(month) name=\(entry.name, privacy: .public) existingSize=\(existing.size) duplicateSize=\(entry.size)")
                continue
            }
            result[entry.name] = RemoteFileMetadata(size: entry.size)
        }
        return result
    }
}

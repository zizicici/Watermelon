import Foundation
import GRDB

extension MonthManifestStore {
    static func loadOrCreate(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        seed: Seed? = nil
    ) async throws -> MonthManifestStore {
        if let seed {
            return try await loadSeeded(
                client: client,
                basePath: basePath,
                year: year,
                month: month,
                seed: seed
            )
        }

        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        try await client.createDirectory(path: monthAbsolutePath)

        let entries = try await client.list(path: monthAbsolutePath)
        let manifestExists = entries.contains { $0.name == Self.manifestFileName && !$0.isDirectory }
        let remoteFilesByName = Dictionary(
            uniqueKeysWithValues: entries
                .filter { !$0.isDirectory && $0.name != Self.manifestFileName }
                .map {
                    (
                        $0.name,
                        RemoteFileMetadata(size: $0.size)
                    )
                }
        )

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
            dirty: prepared.requiresRemoteSync
        )

        // reloadCache doubles as the integrity check: corruption surfaces
        // here before flush can overwrite remote with a bad manifest.
        try store.reloadCache()

        if prepared.requiresRemoteSync {
            try await store.flushToRemote()
        }

        return store
    }

    static func loadSeeded(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        seed: Seed
    ) async throws -> MonthManifestStore {
        let localURL = Self.makeLocalManifestURL(year: year, month: month)

        let dbQueue = try DatabaseQueue(path: localURL.path)
        try Self.migrate(dbQueue)

        // List actual remote directory to detect orphaned files (uploaded but
        // not recorded in manifest due to crash / force-kill). Without this,
        // prepareUpload misses disk-level collisions and upload fails with
        // STATUS_OBJECT_NAME_COLLISION.
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
        let entries = try await client.list(path: monthAbsolutePath)
        let remoteFilesByName = Dictionary(
            uniqueKeysWithValues: entries
                .filter { !$0.isDirectory && $0.name != Self.manifestFileName }
                .map { ($0.name, RemoteFileMetadata(size: $0.size)) }
        )

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: dbQueue,
            remoteFilesByName: remoteFilesByName,
            dirty: false
        )
        try store.seedDatabase(seed)
        try store.reloadCache()
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
    static func loadManifestDirect(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        manifestAbsolutePath: String? = nil
    ) async throws -> MonthManifestStore? {
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let absPath = manifestAbsolutePath ?? {
            return RemotePathBuilder.absolutePath(
                basePath: basePath,
                remoteRelativePath: monthRelativePath + "/" + Self.manifestFileName
            )
        }()

        let localURL = Self.makeLocalManifestURL(year: year, month: month)

        do {
            try await client.download(remotePath: absPath, localURL: localURL)
        } catch {
            try? FileManager.default.removeItem(at: localURL)
            return nil
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

        if prepared.requiresRemoteSync {
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
                        resource.fileName,
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
                        asset.assetFingerprint,
                        asset.creationDateMs,
                        asset.backedUpAtMs,
                        asset.resourceCount,
                        asset.totalFileSizeBytes
                    ]
                )
            }

            for link in seed.assetResourceLinks.sorted(by: { lhs, rhs in
                if lhs.assetFingerprint != rhs.assetFingerprint {
                    return lhs.assetFingerprint.lexicographicallyPrecedes(rhs.assetFingerprint)
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
                        link.assetFingerprint,
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

        for row in resourceRows {
            let item = RemoteManifestResource(
                year: year,
                month: month,
                fileName: row["fileName"],
                contentHash: row["contentHash"],
                fileSize: row["fileSize"],
                resourceType: Int(row["resourceType"] as Int64),
                creationDateMs: row["creationDateMs"],
                backedUpAtMs: row["backedUpAtMs"]
            )
            resourcesByName[item.fileName] = item
            resourcesByHash[item.contentHash] = item.fileName
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

        var assetsByFingerprint: [Data: RemoteManifestAsset] = [:]
        assetsByFingerprint.reserveCapacity(assetRows.count)

        for row in assetRows {
            let fingerprint: Data = row["assetFingerprint"]
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

        var linksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]
        linksByFingerprint.reserveCapacity(assetsByFingerprint.count)

        for row in linkRows {
            let link = RemoteAssetResourceLink(
                year: year,
                month: month,
                assetFingerprint: row["assetFingerprint"],
                resourceHash: row["resourceHash"],
                role: Int(row["role"] as Int64),
                slot: Int(row["slot"] as Int64)
            )
            linksByFingerprint[link.assetFingerprint, default: []].append(link)
        }

        itemsByFileName = resourcesByName
        itemsByHash = resourcesByHash
        self.assetsByFingerprint = assetsByFingerprint
        assetLinksByFingerprint = linksByFingerprint
        existingFileNameSet = Set(resourcesByName.keys).union(remoteFilesByName.keys)
    }
}

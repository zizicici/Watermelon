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
                        NSLocalizedDescriptionKey: "Failed to download existing month manifest for \(monthRelativePath).",
                        NSUnderlyingErrorKey: error
                    ]
                )
            }
        }

        var dbQueue: DatabaseQueue?
        do {
            let queue = try DatabaseQueue(path: localURL.path)
            dbQueue = queue
            try Self.migrate(queue)
            _ = try await queue.read { db in
                try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM resources") ?? 0
            }
        } catch {
            Self.closeAndRemoveLocalManifest(at: localURL, queue: dbQueue)
            dbQueue = nil
            if manifestExists {
                throw NSError(
                    domain: "MonthManifestStore",
                    code: -32,
                    userInfo: [
                        NSLocalizedDescriptionKey: "Existing month manifest for \(monthRelativePath) is corrupted and cannot be loaded.",
                        NSUnderlyingErrorKey: error
                    ]
                )
            }
            let queue = try DatabaseQueue(path: localURL.path)
            try Self.migrate(queue)
            dbQueue = queue
        }

        guard let dbQueue else {
            throw NSError(
                domain: "MonthManifestStore",
                code: -33,
                userInfo: [NSLocalizedDescriptionKey: "Failed to initialize month manifest database queue."]
            )
        }

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: dbQueue,
            remoteFilesByName: remoteFilesByName,
            dirty: !manifestExists
        )

        try store.reloadCache()

        if !manifestExists {
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
        let absPath = manifestAbsolutePath ?? {
            let monthRelativePath = String(format: "%04d/%02d", year, month)
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

        var dbQueue: DatabaseQueue?
        do {
            let queue = try DatabaseQueue(path: localURL.path)
            dbQueue = queue
            try Self.migrate(queue)
            _ = try await queue.read { db in
                try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM resources") ?? 0
            }
        } catch {
            Self.closeAndRemoveLocalManifest(at: localURL, queue: dbQueue)
            return nil
        }

        guard let dbQueue else {
            Self.closeAndRemoveLocalManifest(at: localURL, queue: nil)
            return nil
        }

        let store = MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: dbQueue,
            remoteFilesByName: [:],
            dirty: false
        )

        do {
            try store.reloadCache()
            return store
        } catch {
            return nil
        }
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
                        creationDateNs,
                        backedUpAtNs
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        resource.fileName,
                        resource.contentHash,
                        resource.fileSize,
                        resource.resourceType,
                        resource.creationDateNs,
                        resource.backedUpAtNs
                    ]
                )
            }

            for asset in seed.assets {
                try db.execute(
                    sql: """
                    INSERT INTO assets (
                        assetFingerprint,
                        creationDateNs,
                        backedUpAtNs,
                        resourceCount,
                        totalFileSizeBytes
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        asset.assetFingerprint,
                        asset.creationDateNs,
                        asset.backedUpAtNs,
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
                SELECT fileName, contentHash, fileSize, resourceType, creationDateNs, backedUpAtNs
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
                creationDateNs: row["creationDateNs"],
                backedUpAtNs: row["backedUpAtNs"]
            )
            resourcesByName[item.fileName] = item
            resourcesByHash[item.contentHash] = item.fileName
        }

        let assetRows = try dbQueue.read { db in
            try Row.fetchAll(
                db,
                sql: """
                SELECT assetFingerprint, creationDateNs, backedUpAtNs, resourceCount, totalFileSizeBytes
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
                creationDateNs: row["creationDateNs"],
                backedUpAtNs: row["backedUpAtNs"],
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

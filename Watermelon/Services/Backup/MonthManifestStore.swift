import Foundation
import GRDB

final class MonthManifestStore {
    static let manifestFileName = ".watermelon_manifest.sqlite"

    struct Seed {
        let resources: [RemoteManifestResource]
        let assets: [RemoteManifestAsset]
        let assetResourceLinks: [RemoteAssetResourceLink]
    }

    let year: Int
    let month: Int

    var monthKey: String {
        String(format: "%04d-%02d", year, month)
    }

    var monthRelativePath: String {
        String(format: "%04d/%02d", year, month)
    }

    var monthAbsolutePath: String {
        RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
    }

    var manifestAbsolutePath: String {
        RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath + "/" + Self.manifestFileName)
    }

    private let client: RemoteStorageClientProtocol
    private let basePath: String
    private let localManifestURL: URL
    private let dbQueue: DatabaseQueue

    private(set) var itemsByFileName: [String: RemoteManifestResource] = [:]
    private(set) var itemsByHash: [Data: RemoteManifestResource] = [:]

    private(set) var assetsByFingerprint: [Data: RemoteManifestAsset] = [:]
    private(set) var assetLinksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]

    private(set) var remoteFilesByName: [String: RemoteStorageEntry] = [:]
    private(set) var existingFileNameSet: Set<String> = []
    private(set) var dirty: Bool = false

    private init(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        localManifestURL: URL,
        dbQueue: DatabaseQueue,
        remoteFilesByName: [String: RemoteStorageEntry],
        dirty: Bool
    ) {
        self.client = client
        self.basePath = basePath
        self.year = year
        self.month = month
        self.localManifestURL = localManifestURL
        self.dbQueue = dbQueue
        self.remoteFilesByName = remoteFilesByName
        existingFileNameSet = Set(remoteFilesByName.keys)
        self.dirty = dirty
    }

    deinit {
        Self.closeAndRemoveLocalManifest(at: localManifestURL, queue: dbQueue)
    }

    static func loadOrCreate(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        seed: Seed? = nil
    ) async throws -> MonthManifestStore {
        if let seed {
            return try loadSeeded(
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
                .map { ($0.name, $0) }
        )

        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("month_manifest_\(year)_\(month)_\(UUID().uuidString).sqlite")
        try? FileManager.default.removeItem(at: localURL)

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

    private static func loadSeeded(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        seed: Seed
    ) throws -> MonthManifestStore {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("month_manifest_\(year)_\(month)_\(UUID().uuidString).sqlite")
        try? FileManager.default.removeItem(at: localURL)

        let dbQueue = try DatabaseQueue(path: localURL.path)
        try Self.migrate(dbQueue)

        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let remoteFilesByName: [String: RemoteStorageEntry] = Dictionary(uniqueKeysWithValues: seed.resources.map { resource in
            let remotePath = RemotePathBuilder.absolutePath(
                basePath: basePath,
                remoteRelativePath: monthRelativePath + "/" + resource.fileName
            )
            let createdAt = Self.dateFromEpochNs(resource.creationDateNs)
            return (
                resource.fileName,
                RemoteStorageEntry(
                    path: remotePath,
                    name: resource.fileName,
                    isDirectory: false,
                    size: resource.fileSize,
                    creationDate: createdAt,
                    modificationDate: createdAt
                )
            )
        })

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

    static func loadIfExists(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int
    ) async throws -> MonthManifestStore? {
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)

        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: monthAbsolutePath)
        } catch {
            return nil
        }

        guard entries.contains(where: { $0.name == Self.manifestFileName && !$0.isDirectory }) else {
            return nil
        }

        let remoteFilesByName = Dictionary(
            uniqueKeysWithValues: entries
                .filter { !$0.isDirectory && $0.name != Self.manifestFileName }
                .map { ($0.name, $0) }
        )

        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("month_manifest_\(year)_\(month)_\(UUID().uuidString).sqlite")
        try? FileManager.default.removeItem(at: localURL)

        let manifestAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: monthRelativePath + "/" + Self.manifestFileName
        )

        do {
            try await client.download(remotePath: manifestAbsolutePath, localURL: localURL)
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
            remoteFilesByName: remoteFilesByName,
            dirty: false
        )

        do {
            try store.reloadCache()
            return store
        } catch {
            return nil
        }
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

        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("month_manifest_\(year)_\(month)_\(UUID().uuidString).sqlite")
        try? FileManager.default.removeItem(at: localURL)

        do {
            try await client.download(remotePath: manifestAbsolutePath, localURL: localURL)
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

    func containsAssetFingerprint(_ fingerprint: Data) -> Bool {
        assetsByFingerprint[fingerprint] != nil
    }

    func findByFileName(_ fileName: String) -> RemoteManifestResource? {
        itemsByFileName[fileName]
    }

    func findResourceByHash(_ hash: Data) -> RemoteManifestResource? {
        itemsByHash[hash]
    }

    func links(forAssetFingerprint fingerprint: Data) -> [RemoteAssetResourceLink] {
        assetLinksByFingerprint[fingerprint] ?? []
    }

    func remoteEntry(named fileName: String) -> RemoteStorageEntry? {
        remoteFilesByName[fileName]
    }

    func existingFileNames() -> Set<String> {
        existingFileNameSet
    }

    func allItems() -> [RemoteManifestResource] {
        itemsByFileName.values.sorted { lhs, rhs in
            if lhs.backedUpAtNs == rhs.backedUpAtNs {
                return lhs.fileName < rhs.fileName
            }
            return lhs.backedUpAtNs < rhs.backedUpAtNs
        }
    }

    func allAssets() -> [RemoteManifestAsset] {
        assetsByFingerprint.values.sorted { lhs, rhs in
            if lhs.backedUpAtNs == rhs.backedUpAtNs {
                return lhs.assetFingerprintHex < rhs.assetFingerprintHex
            }
            return lhs.backedUpAtNs < rhs.backedUpAtNs
        }
    }

    @discardableResult
    func upsertResource(_ item: RemoteManifestResource) throws -> RemoteManifestResource {
        if let existing = itemsByHash[item.contentHash] {
            return existing
        }

        try dbQueue.write { db in
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
                ON CONFLICT(fileName) DO UPDATE SET
                    contentHash = excluded.contentHash,
                    fileSize = excluded.fileSize,
                    resourceType = excluded.resourceType,
                    creationDateNs = excluded.creationDateNs,
                    backedUpAtNs = excluded.backedUpAtNs
                """,
                arguments: [
                    item.fileName,
                    item.contentHash,
                    item.fileSize,
                    item.resourceType,
                    item.creationDateNs,
                    item.backedUpAtNs
                ]
            )
        }

        if let old = itemsByFileName[item.fileName], old.contentHash != item.contentHash {
            itemsByHash[old.contentHash] = nil
        }

        itemsByFileName[item.fileName] = item
        itemsByHash[item.contentHash] = item
        existingFileNameSet.insert(item.fileName)
        dirty = true

        return item
    }

    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink]
    ) throws {
        for link in links where itemsByHash[link.resourceHash] == nil {
            throw NSError(
                domain: "MonthManifestStore",
                code: -11,
                userInfo: [NSLocalizedDescriptionKey: "Missing referenced resource hash for asset link."]
            )
        }

        let normalizedLinks = links.sorted { lhs, rhs in
            if lhs.role != rhs.role { return lhs.role < rhs.role }
            if lhs.slot != rhs.slot { return lhs.slot < rhs.slot }
            return lhs.resourceHash.lexicographicallyPrecedes(rhs.resourceHash)
        }

        try dbQueue.write { db in
            try db.execute(
                sql: """
                INSERT INTO assets (
                    assetFingerprint,
                    creationDateNs,
                    backedUpAtNs,
                    resourceCount,
                    totalFileSizeBytes
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(assetFingerprint) DO UPDATE SET
                    creationDateNs = excluded.creationDateNs,
                    backedUpAtNs = excluded.backedUpAtNs,
                    resourceCount = excluded.resourceCount,
                    totalFileSizeBytes = excluded.totalFileSizeBytes
                """,
                arguments: [
                    asset.assetFingerprint,
                    asset.creationDateNs,
                    asset.backedUpAtNs,
                    asset.resourceCount,
                    asset.totalFileSizeBytes
                ]
            )

            try db.execute(
                sql: "DELETE FROM asset_resources WHERE assetFingerprint = ?",
                arguments: [asset.assetFingerprint]
            )

            for link in normalizedLinks {
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

        assetsByFingerprint[asset.assetFingerprint] = asset
        assetLinksByFingerprint[asset.assetFingerprint] = normalizedLinks
        dirty = true
    }

    func markRemoteFile(name: String, size: Int64, creationDate: Date?) {
        let path = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath + "/" + name)
        remoteFilesByName[name] = RemoteStorageEntry(
            path: path,
            name: name,
            isDirectory: false,
            size: size,
            creationDate: creationDate,
            modificationDate: Date()
        )
        existingFileNameSet.insert(name)
    }

    @discardableResult
    func flushToRemote(ignoreCancellation: Bool = false) async throws -> Bool {
        guard dirty else { return false }
        if !ignoreCancellation {
            try Task.checkCancellation()
        }
        try await client.createDirectory(path: monthAbsolutePath)
        if !ignoreCancellation {
            try Task.checkCancellation()
        }

        let finalPath = manifestAbsolutePath
        let tempRemotePath = finalPath + ".tmp.\(UUID().uuidString)"

        do {
            try await client.upload(
                localURL: localManifestURL,
                remotePath: tempRemotePath,
                respectTaskCancellation: !ignoreCancellation,
                onProgress: nil
            )
            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            try await moveReplacingExistingManifest(
                tempRemotePath: tempRemotePath,
                finalPath: finalPath,
                ignoreCancellation: ignoreCancellation
            )
        } catch {
            if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                throw CancellationError()
            }
            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            if (try? await client.exists(path: tempRemotePath)) == true {
                try? await client.delete(path: tempRemotePath)
            }
            throw error
        }

        if !ignoreCancellation {
            try Task.checkCancellation()
        }
        if (try? await client.exists(path: tempRemotePath)) == true {
            try? await client.delete(path: tempRemotePath)
        }

        if !ignoreCancellation {
            try Task.checkCancellation()
        }
        dirty = false
        return true
    }

    private func moveReplacingExistingManifest(
        tempRemotePath: String,
        finalPath: String,
        ignoreCancellation: Bool
    ) async throws {
        do {
            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            try await client.move(from: tempRemotePath, to: finalPath)
            return
        } catch {
            if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                throw CancellationError()
            }
            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            let finalExists = (try? await client.exists(path: finalPath)) ?? false
            guard finalExists else {
                throw error
            }

            let backupPath = finalPath + ".bak.\(UUID().uuidString)"
            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            try await client.move(from: finalPath, to: backupPath)

            do {
                if !ignoreCancellation {
                    try Task.checkCancellation()
                }
                try await client.move(from: tempRemotePath, to: finalPath)
            } catch {
                if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                    throw CancellationError()
                }
                if (try? await client.exists(path: finalPath)) == true {
                    try? await client.delete(path: finalPath)
                }
                if (try? await client.exists(path: backupPath)) == true {
                    try? await client.move(from: backupPath, to: finalPath)
                }
                throw error
            }

            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            if (try? await client.exists(path: backupPath)) == true {
                try? await client.delete(path: backupPath)
            }
        }
    }

    private static func migrate(_ queue: DatabaseQueue) throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("month_manifest_v3_dev_schema_reset") { db in
            try db.execute(sql: "DROP INDEX IF EXISTS idx_asset_resources_hash")
            try db.execute(sql: "DROP INDEX IF EXISTS idx_asset_resources_asset")
            try db.execute(sql: "DROP INDEX IF EXISTS idx_resources_contentHash")
            try db.execute(sql: "DROP TABLE IF EXISTS asset_resources")
            try db.execute(sql: "DROP TABLE IF EXISTS assets")
            try db.execute(sql: "DROP TABLE IF EXISTS resources")
            try ensureSchemaBaseline(db)
        }
        try migrator.migrate(queue)
    }

    private static func ensureSchemaBaseline(_ db: Database) throws {
        try db.execute(
            sql: """
            CREATE TABLE IF NOT EXISTS resources (
              fileName TEXT PRIMARY KEY NOT NULL,
              contentHash BLOB NOT NULL,
              fileSize INTEGER NOT NULL,
              resourceType INTEGER NOT NULL,
              creationDateNs INTEGER,
              backedUpAtNs INTEGER NOT NULL
            )
            """
        )
        try db.execute(
            sql: """
            CREATE TABLE IF NOT EXISTS assets (
              assetFingerprint BLOB PRIMARY KEY NOT NULL,
              creationDateNs INTEGER,
              backedUpAtNs INTEGER NOT NULL,
              resourceCount INTEGER NOT NULL,
              totalFileSizeBytes INTEGER NOT NULL
            )
            """
        )
        try db.execute(
            sql: """
            CREATE TABLE IF NOT EXISTS asset_resources (
              assetFingerprint BLOB NOT NULL,
              resourceHash BLOB NOT NULL,
              role INTEGER NOT NULL,
              slot INTEGER NOT NULL,
              PRIMARY KEY(assetFingerprint, role, slot)
            )
            """
        )
        try db.execute(sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_resources_contentHash ON resources(contentHash)")
        try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_asset_resources_asset ON asset_resources(assetFingerprint)")
        try db.execute(sql: "CREATE INDEX IF NOT EXISTS idx_asset_resources_hash ON asset_resources(resourceHash)")
    }

    private static func closeAndRemoveLocalManifest(at localURL: URL, queue: DatabaseQueue?) {
        if let queue {
            try? queue.close()
        }
        try? FileManager.default.removeItem(at: localURL)
    }

    private static func dateFromEpochNs(_ ns: Int64?) -> Date? {
        guard let ns else { return nil }
        return Date(timeIntervalSince1970: Double(ns) / 1_000_000_000)
    }

    private func seedDatabase(_ seed: Seed) throws {
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

    private func reloadCache() throws {
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
        var resourcesByHash: [Data: RemoteManifestResource] = [:]
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
            resourcesByHash[item.contentHash] = item
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

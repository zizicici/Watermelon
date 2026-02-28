import Foundation
import GRDB

final class MonthManifestStore {
    static let manifestFileName = ".watermelon_manifest.sqlite"

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

    private let client: SMBClientProtocol
    private let basePath: String
    private let localManifestURL: URL
    private let dbQueue: DatabaseQueue

    private(set) var itemsByFileName: [String: RemoteManifestResource] = [:]
    private(set) var itemsByHash: [Data: RemoteManifestResource] = [:]

    private(set) var assetsByFingerprint: [Data: RemoteManifestAsset] = [:]
    private(set) var assetLinksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]

    private(set) var remoteFilesByName: [String: SMBRemoteEntry] = [:]
    private(set) var existingFileNameSet: Set<String> = []
    private(set) var dirty: Bool = false

    private init(
        client: SMBClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        localManifestURL: URL,
        dbQueue: DatabaseQueue,
        remoteFilesByName: [String: SMBRemoteEntry],
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
        try? FileManager.default.removeItem(at: localManifestURL)
    }

    static func loadOrCreate(
        client: SMBClientProtocol,
        basePath: String,
        year: Int,
        month: Int
    ) async throws -> MonthManifestStore {
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
            }
        }

        var dbQueue: DatabaseQueue
        do {
            dbQueue = try DatabaseQueue(path: localURL.path)
            try Self.migrate(dbQueue)
            _ = try await dbQueue.read { db in
                try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM resources") ?? 0
            }
        } catch {
            try? FileManager.default.removeItem(at: localURL)
            dbQueue = try DatabaseQueue(path: localURL.path)
            try Self.migrate(dbQueue)
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

    static func loadIfExists(
        client: SMBClientProtocol,
        basePath: String,
        year: Int,
        month: Int
    ) async throws -> MonthManifestStore? {
        let monthRelativePath = String(format: "%04d/%02d", year, month)
        let monthAbsolutePath = RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)

        let entries: [SMBRemoteEntry]
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

        let dbQueue: DatabaseQueue
        do {
            dbQueue = try DatabaseQueue(path: localURL.path)
            try Self.migrate(dbQueue)
            _ = try await dbQueue.read { db in
                try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM resources") ?? 0
            }
        } catch {
            try? FileManager.default.removeItem(at: localURL)
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
        client: SMBClientProtocol,
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

        let dbQueue: DatabaseQueue
        do {
            dbQueue = try DatabaseQueue(path: localURL.path)
            try Self.migrate(dbQueue)
            _ = try await dbQueue.read { db in
                try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM resources") ?? 0
            }
        } catch {
            try? FileManager.default.removeItem(at: localURL)
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

    func remoteEntry(named fileName: String) -> SMBRemoteEntry? {
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
                    resourceCount
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(assetFingerprint) DO UPDATE SET
                    creationDateNs = excluded.creationDateNs,
                    backedUpAtNs = excluded.backedUpAtNs,
                    resourceCount = excluded.resourceCount
                """,
                arguments: [
                    asset.assetFingerprint,
                    asset.creationDateNs,
                    asset.backedUpAtNs,
                    asset.resourceCount
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
        remoteFilesByName[name] = SMBRemoteEntry(
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
    func flushToRemote() async throws -> Bool {
        guard dirty else { return false }
        try await client.createDirectory(path: monthAbsolutePath)

        let finalPath = manifestAbsolutePath
        let tempRemotePath = finalPath + ".tmp.\(UUID().uuidString)"

        try await client.upload(
            localURL: localManifestURL,
            remotePath: tempRemotePath,
            respectTaskCancellation: false,
            onProgress: nil
        )

        if try await client.exists(path: finalPath) {
            try? await client.delete(path: finalPath)
        }

        do {
            try await client.move(from: tempRemotePath, to: finalPath)
        } catch {
            if try await client.exists(path: finalPath) {
                try? await client.delete(path: finalPath)
                try await client.move(from: tempRemotePath, to: finalPath)
            } else {
                throw error
            }
        }

        if (try? await client.exists(path: tempRemotePath)) == true {
            try? await client.delete(path: tempRemotePath)
        }

        dirty = false
        return true
    }

    private static func migrate(_ queue: DatabaseQueue) throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("month_manifest_v2_reset_schema") { db in
            let candidateTables = ["manifest_items", "resources", "assets", "asset_resources"]
            for tableName in candidateTables where try db.tableExists(tableName) {
                try db.drop(table: tableName)
            }

            try db.create(table: "resources") { table in
                table.column("fileName", .text).notNull().primaryKey()
                table.column("contentHash", .blob).notNull()
                table.column("fileSize", .integer).notNull()
                table.column("resourceType", .integer).notNull()
                table.column("creationDateNs", .integer)
                table.column("backedUpAtNs", .integer).notNull()
                table.uniqueKey(["contentHash"])
            }

            try db.create(table: "assets") { table in
                table.column("assetFingerprint", .blob).notNull().primaryKey()
                table.column("creationDateNs", .integer)
                table.column("backedUpAtNs", .integer).notNull()
                table.column("resourceCount", .integer).notNull()
            }

            try db.create(table: "asset_resources") { table in
                table.column("assetFingerprint", .blob).notNull()
                table.column("resourceHash", .blob).notNull()
                table.column("role", .integer).notNull()
                table.column("slot", .integer).notNull()
                table.primaryKey(["assetFingerprint", "role", "slot"])
            }

            try db.create(index: "idx_resources_contentHash", on: "resources", columns: ["contentHash"], unique: true)
            try db.create(index: "idx_asset_resources_asset", on: "asset_resources", columns: ["assetFingerprint"])
            try db.create(index: "idx_asset_resources_hash", on: "asset_resources", columns: ["resourceHash"])
        }
        try migrator.migrate(queue)
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
                SELECT assetFingerprint, creationDateNs, backedUpAtNs, resourceCount
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
                resourceCount: Int(row["resourceCount"] as Int64)
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

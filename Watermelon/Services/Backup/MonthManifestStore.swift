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
    private(set) var hashes = Set<Data>()
    private(set) var remoteFilesByName: [String: SMBRemoteEntry] = [:]
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
                try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM manifest_items") ?? 0
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

    func containsHash(_ hash: Data) -> Bool {
        hashes.contains(hash)
    }

    func findByFileName(_ fileName: String) -> RemoteManifestResource? {
        itemsByFileName[fileName]
    }

    func remoteEntry(named fileName: String) -> SMBRemoteEntry? {
        remoteFilesByName[fileName]
    }

    func existingFileNames() -> Set<String> {
        Set(itemsByFileName.keys).union(remoteFilesByName.keys)
    }

    func allItems() -> [RemoteManifestResource] {
        itemsByFileName.values.sorted { lhs, rhs in
            if lhs.backedUpAtNs == rhs.backedUpAtNs {
                return lhs.fileName < rhs.fileName
            }
            return lhs.backedUpAtNs < rhs.backedUpAtNs
        }
    }

    func upsertItem(_ item: RemoteManifestResource) throws {
        try dbQueue.write { db in
            try db.execute(
                sql: """
                INSERT INTO manifest_items (
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
            hashes.remove(old.contentHash)
        }
        itemsByFileName[item.fileName] = item
        hashes.insert(item.contentHash)
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
            respectTaskCancellation: false
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
        migrator.registerMigration("month_manifest_v1") { db in
            try db.create(table: "manifest_items") { table in
                table.column("fileName", .text).notNull().primaryKey()
                table.column("contentHash", .blob).notNull()
                table.column("fileSize", .integer).notNull()
                table.column("resourceType", .integer).notNull()
                table.column("creationDateNs", .integer)
                table.column("backedUpAtNs", .integer).notNull()
            }
            try db.create(index: "idx_manifest_items_contentHash", on: "manifest_items", columns: ["contentHash"], unique: true)
        }
        try migrator.migrate(queue)
    }

    private func reloadCache() throws {
        let rows = try dbQueue.read { db in
            try Row.fetchAll(
                db,
                sql: """
                SELECT fileName, contentHash, fileSize, resourceType, creationDateNs, backedUpAtNs
                FROM manifest_items
                """
            )
        }

        var fileMap: [String: RemoteManifestResource] = [:]
        fileMap.reserveCapacity(rows.count)
        var hashSet = Set<Data>()
        hashSet.reserveCapacity(rows.count)

        for row in rows {
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
            fileMap[item.fileName] = item
            hashSet.insert(item.contentHash)
        }

        itemsByFileName = fileMap
        hashes = hashSet
    }
}

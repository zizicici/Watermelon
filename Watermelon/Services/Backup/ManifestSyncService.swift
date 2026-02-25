import Foundation
import GRDB

enum RemoteManifestRefreshResult {
    case pulled
    case remoteMissingClearedLocal
    case remoteMissingKeptLocal
}

private enum ManifestSyncError: LocalizedError {
    case invalidFingerprintHex(String)
    case invalidFingerprintBlobSize(Int)

    var errorDescription: String? {
        switch self {
        case .invalidFingerprintHex(let value):
            return "Invalid fingerprint hex value: \(value)"
        case .invalidFingerprintBlobSize(let count):
            return "Invalid fingerprint blob size: \(count), expected 32 bytes."
        }
    }
}

private struct LocalManifestItemSource: FetchableRecord, Decodable {
    let assetLocalIdentifier: String
    let creationDate: Date?
    let resourceType: String
    let originalFilename: String
    let fileSize: Int64
    let fingerprint: String
    let remoteRelativePath: String
    let backedUpAt: Date
}

private struct ManifestStorageItemRecord: FetchableRecord, Decodable {
    let assetLocalIdentifier: String
    let creationDateNs: Int64?
    let resourceType: Int64
    let originalFilename: String
    let fileSize: Int64
    let fingerprint: Data
    let remoteRelativePath: String
    let backedUpAtNs: Int64
}

private struct ManifestItemRecord {
    let assetLocalIdentifier: String
    let creationDate: Date?
    let resourceType: String
    let originalFilename: String
    let fileSize: Int64
    let fingerprint: String
    let remoteRelativePath: String
    let backedUpAt: Date
}

final class ManifestSyncService {
    static let manifestFileName = ".watermelon_manifest.sqlite"
    private static let manifestItemsTableName = "manifest_items"

    private let databaseManager: DatabaseManager

    init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
    }

    func manifestRemotePath(basePath: String) -> String {
        RemotePathBuilder.normalizePath("\(basePath)/\(Self.manifestFileName)")
    }

    func pullRemoteManifestIfNeeded(client: SMBClientProtocol, basePath: String) async throws {
        guard let remotePath = try await resolveExistingManifestPath(client: client, basePath: basePath) else {
            return
        }

        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent("manifest_remote_\(UUID().uuidString).sqlite")
        try? FileManager.default.removeItem(at: tempURL)
        try await client.download(remotePath: remotePath, localURL: tempURL)

        defer {
            try? FileManager.default.removeItem(at: tempURL)
        }

        let remoteQueue = try DatabaseQueue(path: tempURL.path)
        let remoteItems = try await remoteQueue.read { db in
            try Self.loadManifestItems(db: db)
        }

        try databaseManager.write { db in
            for item in remoteItems {
                let normalizedItem = ManifestItemRecord(
                    assetLocalIdentifier: item.assetLocalIdentifier,
                    creationDate: item.creationDate,
                    resourceType: item.resourceType,
                    originalFilename: item.originalFilename,
                    fileSize: item.fileSize,
                    fingerprint: item.fingerprint,
                    remoteRelativePath: RemotePathBuilder.storedPathToRelative(
                        basePath: basePath,
                        storedPath: item.remoteRelativePath
                    ),
                    backedUpAt: item.backedUpAt
                )
                try Self.upsertAsset(from: normalizedItem, db: db)
                try Self.upsertResource(from: normalizedItem, db: db)
            }
        }

        try databaseManager.setSyncState(key: "remote_manifest_pull", value: ISO8601DateFormatter().string(from: Date()))
    }

    func refreshFromRemote(
        client: SMBClientProtocol,
        basePath: String,
        clearLocalWhenMissing: Bool
    ) async throws -> RemoteManifestRefreshResult {
        guard let _ = try await resolveExistingManifestPath(client: client, basePath: basePath) else {
            if clearLocalWhenMissing {
                let shouldClearLocal = try await isRemoteBaseEffectivelyEmpty(client: client, basePath: basePath)
                if shouldClearLocal {
                    try databaseManager.write { db in
                        try db.execute(sql: "DELETE FROM resources")
                        try db.execute(sql: "DELETE FROM assets")
                    }
                    return .remoteMissingClearedLocal
                }
            }
            return .remoteMissingKeptLocal
        }

        try await pullRemoteManifestIfNeeded(client: client, basePath: basePath)
        return .pulled
    }

    private func resolveExistingManifestPath(client: SMBClientProtocol, basePath: String) async throws -> String? {
        let primary = manifestRemotePath(basePath: basePath)
        if try await client.exists(path: primary) {
            return primary
        }
        return nil
    }

    private func isRemoteBaseEffectivelyEmpty(client: SMBClientProtocol, basePath: String) async throws -> Bool {
        do {
            let entries = try await client.list(path: basePath)
            return !entries.contains { entry in
                let name = entry.name
                let isManifestArtifact = name == Self.manifestFileName
                    || name.hasPrefix(Self.manifestFileName + ".")
                return !isManifestArtifact
            }
        } catch {
            // If remote listing fails, keep local cache to avoid destructive false clears.
            return false
        }
    }

    func pushLocalManifest(client: SMBClientProtocol, basePath: String, appVersion: String) async throws {
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent("manifest_local_\(UUID().uuidString).sqlite")
        try? FileManager.default.removeItem(at: tempURL)

        var configuration = Configuration()
        configuration.prepareDatabase { db in
            try db.execute(sql: "PRAGMA journal_mode=DELETE")
        }
        let manifestQueue = try DatabaseQueue(path: tempURL.path, configuration: configuration)
        try migrateManifestDatabase(queue: manifestQueue)

        let localItems = try databaseManager.read { db in
            try LocalManifestItemSource.fetchAll(db, sql: """
            SELECT r.assetLocalIdentifier AS assetLocalIdentifier,
                   a.creationDate AS creationDate,
                   r.resourceType AS resourceType,
                   r.originalFilename AS originalFilename,
                   r.fileSize AS fileSize,
                   r.fingerprint AS fingerprint,
                   r.remoteRelativePath AS remoteRelativePath,
                   r.backedUpAt AS backedUpAt
            FROM resources r
            LEFT JOIN assets a ON a.localIdentifier = r.assetLocalIdentifier
            """)
        }

        try await manifestQueue.write { db in
            for item in localItems {
                let relativePath = RemotePathBuilder.storedPathToRelative(
                    basePath: basePath,
                    storedPath: item.remoteRelativePath
                )
                try db.execute(
                    sql: """
                    INSERT INTO \(Self.manifestItemsTableName) (
                        assetLocalIdentifier, creationDateNs, resourceType, originalFilename,
                        fileSize, fingerprint, remoteRelativePath, backedUpAtNs
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    arguments: [
                        item.assetLocalIdentifier,
                        Self.nanosecondsSinceEpoch(item.creationDate),
                        Self.resourceTypeCode(from: item.resourceType),
                        item.originalFilename,
                        item.fileSize,
                        try Self.fingerprintBlob(fromHex: item.fingerprint),
                        relativePath,
                        Self.nanosecondsSinceEpoch(item.backedUpAt) ?? 0
                    ]
                )
            }

            var meta = RemoteManifestMeta(version: 4, generatedAt: Date(), appVersion: appVersion)
            try meta.save(db)
        }

        let finalPath = manifestRemotePath(basePath: basePath)
        let tempRemotePath = finalPath + ".tmp.\(UUID().uuidString)"

        // Cleanup stale artifacts from previous implementation.
        for stalePath in [finalPath + ".tmp", finalPath + ".bak", finalPath + ".bak.old"] {
            if (try? await client.exists(path: stalePath)) == true {
                try? await client.delete(path: stalePath)
            }
        }

        try await client.upload(
            localURL: tempURL,
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

        try? FileManager.default.removeItem(at: tempURL)
        try databaseManager.setSyncState(key: "remote_manifest_push", value: ISO8601DateFormatter().string(from: Date()))
    }

    private func migrateManifestDatabase(queue: DatabaseQueue) throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("manifest_v1") { db in
            try db.create(table: Self.manifestItemsTableName) { table in
                table.column("assetLocalIdentifier", .text).notNull()
                table.column("creationDateNs", .integer)
                table.column("resourceType", .integer).notNull()
                table.column("originalFilename", .text).notNull()
                table.column("fileSize", .integer).notNull()
                table.column("fingerprint", .blob).notNull()
                table.column("remoteRelativePath", .text).notNull()
                table.column("backedUpAtNs", .integer).notNull()
                table.uniqueKey(["assetLocalIdentifier", "fingerprint"])
            }

            try db.create(table: RemoteManifestMeta.databaseTableName) { table in
                table.column("version", .integer).notNull().primaryKey()
                table.column("generatedAt", .datetime).notNull()
                table.column("appVersion", .text).notNull()
            }
        }
        try migrator.migrate(queue)
    }

    private static func loadManifestItems(db: Database) throws -> [ManifestItemRecord] {
        let storageItems = try ManifestStorageItemRecord.fetchAll(
            db,
            sql: """
            SELECT assetLocalIdentifier, creationDateNs, resourceType, originalFilename,
                   fileSize, fingerprint, remoteRelativePath, backedUpAtNs
            FROM \(Self.manifestItemsTableName)
            """
        )
        return try storageItems.map { try fromStorage($0) }
    }

    private static func fromStorage(_ storage: ManifestStorageItemRecord) throws -> ManifestItemRecord {
        ManifestItemRecord(
            assetLocalIdentifier: storage.assetLocalIdentifier,
            creationDate: dateFromEpochNanoseconds(storage.creationDateNs),
            resourceType: resourceTypeName(from: storage.resourceType),
            originalFilename: storage.originalFilename,
            fileSize: storage.fileSize,
            fingerprint: try fingerprintHex(fromBlob: storage.fingerprint),
            remoteRelativePath: storage.remoteRelativePath,
            backedUpAt: dateFromEpochNanoseconds(storage.backedUpAtNs) ?? Date(timeIntervalSince1970: 0)
        )
    }

    private static func upsertAsset(from item: ManifestItemRecord, db: Database) throws {
        try db.execute(
            sql: """
            INSERT INTO assets (
                localIdentifier, mediaType, creationDate, modificationDate, locationJSON,
                pixelWidth, pixelHeight, duration, isLivePhoto, lastSeenAt
            ) VALUES (?, 'unknown', ?, NULL, NULL, 0, 0, 0, 0, ?)
            ON CONFLICT(localIdentifier) DO UPDATE SET
                creationDate = COALESCE(excluded.creationDate, assets.creationDate),
                lastSeenAt = excluded.lastSeenAt
            """,
            arguments: [
                item.assetLocalIdentifier,
                item.creationDate,
                Date()
            ]
        )
    }

    private static func upsertResource(from item: ManifestItemRecord, db: Database) throws {
        let syntheticResourceID = "\(item.resourceType)|\(item.fingerprint)"
        try db.execute(
            sql: """
            INSERT INTO resources (
                assetLocalIdentifier, resourceLocalIdentifier, resourceType, uti, originalFilename,
                fileSize, fingerprint, remoteRelativePath, backedUpAt, checksum
            ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(assetLocalIdentifier, resourceLocalIdentifier) DO UPDATE SET
                resourceType = excluded.resourceType,
                originalFilename = excluded.originalFilename,
                fileSize = excluded.fileSize,
                fingerprint = excluded.fingerprint,
                remoteRelativePath = excluded.remoteRelativePath,
                backedUpAt = excluded.backedUpAt
            """,
            arguments: [
                item.assetLocalIdentifier,
                syntheticResourceID,
                item.resourceType,
                item.originalFilename,
                item.fileSize,
                item.fingerprint,
                item.remoteRelativePath,
                item.backedUpAt
            ]
        )
    }

    private static func resourceTypeCode(from value: String) -> Int64 {
        switch value {
        case "photo": return 1
        case "video": return 2
        case "audio": return 3
        case "alternatePhoto": return 4
        case "fullSizePhoto": return 5
        case "fullSizeVideo": return 6
        case "pairedVideo": return 7
        case "adjustmentData": return 8
        case "adjustmentBasePhoto": return 9
        case "photoProxy": return 10
        default:
            if value.hasPrefix("other_"), let raw = Int64(value.dropFirst(6)) {
                return 1000 + raw
            }
            return 0
        }
    }

    private static func resourceTypeName(from code: Int64) -> String {
        switch code {
        case 1: return "photo"
        case 2: return "video"
        case 3: return "audio"
        case 4: return "alternatePhoto"
        case 5: return "fullSizePhoto"
        case 6: return "fullSizeVideo"
        case 7: return "pairedVideo"
        case 8: return "adjustmentData"
        case 9: return "adjustmentBasePhoto"
        case 10: return "photoProxy"
        default:
            if code >= 1000 {
                return "other_\(code - 1000)"
            }
            return "unknown"
        }
    }

    private static func nanosecondsSinceEpoch(_ date: Date?) -> Int64? {
        guard let date else { return nil }
        return Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())
    }

    private static func dateFromEpochNanoseconds(_ value: Int64?) -> Date? {
        guard let value else { return nil }
        return Date(timeIntervalSince1970: Double(value) / 1_000_000_000)
    }

    private static func fingerprintBlob(fromHex hex: String) throws -> Data {
        guard hex.count == 64 else {
            throw ManifestSyncError.invalidFingerprintHex(hex)
        }
        var data = Data(capacity: 32)
        var index = hex.startIndex
        while index < hex.endIndex {
            let next = hex.index(index, offsetBy: 2)
            let byteString = hex[index..<next]
            guard let byte = UInt8(byteString, radix: 16) else {
                throw ManifestSyncError.invalidFingerprintHex(hex)
            }
            data.append(byte)
            index = next
        }
        return data
    }

    private static func fingerprintHex(fromBlob blob: Data) throws -> String {
        guard blob.count == 32 else {
            throw ManifestSyncError.invalidFingerprintBlobSize(blob.count)
        }
        return blob.map { String(format: "%02x", $0) }.joined()
    }
}

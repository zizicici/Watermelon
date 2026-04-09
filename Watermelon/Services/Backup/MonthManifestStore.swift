import Foundation
import GRDB

final class MonthManifestStore {
    static let manifestFileName = ".watermelon_manifest.sqlite"
    static let tempFilePrefix = "month_manifest_"
    static let tempFileExtension = "sqlite"
    static let staleTempFileAge: TimeInterval = 24 * 60 * 60
    static let staleTempCleanupLock = NSLock()
    static var hasPurgedStaleTempFiles = false

    struct RemoteFileMetadata {
        let size: Int64
    }

    struct Seed {
        let resources: [RemoteManifestResource]
        let assets: [RemoteManifestAsset]
        let assetResourceLinks: [RemoteAssetResourceLink]
    }

    let year: Int
    let month: Int

    var monthRelativePath: String {
        String(format: "%04d/%02d", year, month)
    }

    var monthAbsolutePath: String {
        RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
    }

    var manifestAbsolutePath: String {
        RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath + "/" + Self.manifestFileName)
    }

    let client: RemoteStorageClientProtocol
    let basePath: String
    let localManifestURL: URL
    let dbQueue: DatabaseQueue

    var itemsByFileName: [String: RemoteManifestResource] = [:]
    var itemsByHash: [Data: String] = [:]

    var assetsByFingerprint: [Data: RemoteManifestAsset] = [:]
    var assetLinksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]

    var remoteFilesByName: [String: RemoteFileMetadata] = [:]
    var existingFileNameSet: Set<String> = []
    private(set) var dirty: Bool = false

    init(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        localManifestURL: URL,
        dbQueue: DatabaseQueue,
        remoteFilesByName: [String: RemoteFileMetadata],
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

    func containsAssetFingerprint(_ fingerprint: Data) -> Bool {
        assetsByFingerprint[fingerprint] != nil
    }

    func findByFileName(_ fileName: String) -> RemoteManifestResource? {
        itemsByFileName[fileName]
    }

    func findResourceByHash(_ hash: Data) -> RemoteManifestResource? {
        guard let fileName = itemsByHash[hash] else { return nil }
        return itemsByFileName[fileName]
    }

    func links(forAssetFingerprint fingerprint: Data) -> [RemoteAssetResourceLink] {
        assetLinksByFingerprint[fingerprint] ?? []
    }

    func remoteFileSize(named fileName: String) -> Int64? {
        remoteFilesByName[fileName]?.size
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
        if let existingFileName = itemsByHash[item.contentHash],
           let existing = itemsByFileName[existingFileName] {
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
        itemsByHash[item.contentHash] = item.fileName
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

    func markRemoteFile(name: String, size: Int64) {
        remoteFilesByName[name] = RemoteFileMetadata(size: size)
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
        evictCaches()
        return true
    }

    func evictCaches() {
        itemsByFileName.removeAll()
        itemsByHash.removeAll()
        assetsByFingerprint.removeAll()
        assetLinksByFingerprint.removeAll()
    }

    private var cacheEvicted: Bool {
        itemsByFileName.isEmpty && assetsByFingerprint.isEmpty
    }

    private func ensureCacheLoaded() throws {
        if cacheEvicted {
            try reloadCache()
        }
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
}

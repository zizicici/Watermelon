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

    /// Existing asset whose link set ⊇ the given (role, slot, hash) tuples — used by legacy
    /// import to skip bundles already represented by an Asset with extra (adjustment, etc.) roles.
    func findEnclosingAssetFingerprint(
        forResources resources: [(role: Int, slot: Int, hash: Data)]
    ) -> Data? {
        guard !resources.isEmpty else { return nil }
        let needed = Self.linkKeySet(fromTuples: resources)
        for (fingerprint, links) in assetLinksByFingerprint {
            if links.count < needed.count { continue }
            let have = Self.linkKeySet(fromLinks: links)
            if have.isSuperset(of: needed) { return fingerprint }
        }
        return nil
    }

    /// Existing assets whose link set is a strict subset of the given (role, slot, hash) tuples
    /// — used by legacy import to find older partial Assets that the incoming bundle supersedes.
    func findStrictSubsetAssetFingerprints(
        forResources resources: [(role: Int, slot: Int, hash: Data)]
    ) -> [Data] {
        guard !resources.isEmpty else { return [] }
        let needed = Self.linkKeySet(fromTuples: resources)
        var result: [Data] = []
        for (fingerprint, links) in assetLinksByFingerprint {
            if links.isEmpty { continue }
            if links.count >= needed.count { continue }
            let have = Self.linkKeySet(fromLinks: links)
            if needed.isSuperset(of: have) { result.append(fingerprint) }
        }
        return result
    }

    private struct LinkKey: Hashable {
        let role: Int
        let slot: Int
        let hash: Data
    }

    private static func linkKeySet(fromTuples tuples: [(role: Int, slot: Int, hash: Data)]) -> Set<LinkKey> {
        Set(tuples.map { LinkKey(role: $0.role, slot: $0.slot, hash: $0.hash) })
    }

    private static func linkKeySet(fromLinks links: [RemoteAssetResourceLink]) -> Set<LinkKey> {
        Set(links.map { LinkKey(role: $0.role, slot: $0.slot, hash: $0.resourceHash) })
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

    /// Unsorted bulk export for snapshotCache — avoids sorting overhead
    /// since replaceMonth builds its own dictionaries by key.
    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        (
            resources: Array(itemsByFileName.values),
            assets: Array(assetsByFingerprint.values),
            links: assetLinksByFingerprint.values.flatMap { $0 }
        )
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
                    creationDateMs,
                    backedUpAtMs
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(fileName) DO UPDATE SET
                    contentHash = excluded.contentHash,
                    fileSize = excluded.fileSize,
                    resourceType = excluded.resourceType,
                    creationDateMs = excluded.creationDateMs,
                    backedUpAtMs = excluded.backedUpAtMs
                """,
                arguments: [
                    item.fileName,
                    item.contentHash,
                    item.fileSize,
                    item.resourceType,
                    item.creationDateMs,
                    item.backedUpAtMs
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
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<Data> = []
    ) throws {
        for link in links where itemsByHash[link.resourceHash] == nil {
            throw NSError(
                domain: "MonthManifestStore",
                code: -11,
                userInfo: [NSLocalizedDescriptionKey: String(localized: "backup.manifest.error.missingResourceHash")]
            )
        }

        let normalizedLinks = links.sorted { lhs, rhs in
            if lhs.role != rhs.role { return lhs.role < rhs.role }
            if lhs.slot != rhs.slot { return lhs.slot < rhs.slot }
            return lhs.resourceHash.lexicographicallyPrecedes(rhs.resourceHash)
        }

        let subsetsToDelete = replacingSubsetFingerprints.subtracting([asset.assetFingerprint])

        try dbQueue.write { db in
            for fingerprint in subsetsToDelete {
                try db.execute(
                    sql: "DELETE FROM asset_resources WHERE assetFingerprint = ?",
                    arguments: [fingerprint]
                )
                try db.execute(
                    sql: "DELETE FROM assets WHERE assetFingerprint = ?",
                    arguments: [fingerprint]
                )
            }

            try db.execute(
                sql: """
                INSERT INTO assets (
                    assetFingerprint,
                    creationDateMs,
                    backedUpAtMs,
                    resourceCount,
                    totalFileSizeBytes
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(assetFingerprint) DO UPDATE SET
                    creationDateMs = excluded.creationDateMs,
                    backedUpAtMs = excluded.backedUpAtMs,
                    resourceCount = excluded.resourceCount,
                    totalFileSizeBytes = excluded.totalFileSizeBytes
                """,
                arguments: [
                    asset.assetFingerprint,
                    asset.creationDateMs,
                    asset.backedUpAtMs,
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

        for fingerprint in subsetsToDelete {
            assetsByFingerprint[fingerprint] = nil
            assetLinksByFingerprint[fingerprint] = nil
        }
        assetsByFingerprint[asset.assetFingerprint] = asset
        assetLinksByFingerprint[asset.assetFingerprint] = normalizedLinks
        dirty = true
    }

    func markRemoteFile(name: String, size: Int64) {
        remoteFilesByName[name] = RemoteFileMetadata(size: size)
        existingFileNameSet.insert(name)
    }

    /// Phantom asset (assets row with no asset_resources rows) counts as incomplete.
    func isAssetIncomplete(_ fingerprint: Data) -> Bool {
        guard let asset = assetsByFingerprint[fingerprint] else { return false }
        let links = assetLinksByFingerprint[fingerprint] ?? []
        return Self.isAssetIncomplete(
            links: links,
            isResourceAvailable: { itemsByHash[$0] != nil },
            assetFingerprint: asset.assetFingerprint
        )
    }

    struct CleanupMissingResourcesResult {
        let removedResourceCount: Int
        let removedAssetCount: Int
        let removedOrphanLinkCount: Int
    }

    /// Also integrally deletes any asset left fully-orphan or metadata-only (role 7) after the
    /// resource removal. Throws → manifest unchanged.
    func cleanupMissingResources(missingHashes: Set<Data>) throws -> CleanupMissingResourcesResult {
        let actualMissing = missingHashes.intersection(itemsByHash.keys)
        let metadataOnlyRoles: Set<Int> = [ResourceTypeCode.adjustmentData]

        // Iterate assetsByFingerprint.keys to cover phantom assets (no link entries).
        var assetsToRemove: Set<Data> = []
        for fingerprint in assetsByFingerprint.keys {
            let links = assetLinksByFingerprint[fingerprint] ?? []
            var hasNonMetadataKeptLink = false
            var hasAnyKeptLink = false
            for link in links {
                let kept = !actualMissing.contains(link.resourceHash) && itemsByHash[link.resourceHash] != nil
                if !kept { continue }
                hasAnyKeptLink = true
                if !metadataOnlyRoles.contains(link.role) {
                    hasNonMetadataKeptLink = true
                    break
                }
            }
            if !hasAnyKeptLink || !hasNonMetadataKeptLink {
                assetsToRemove.insert(fingerprint)
            }
        }

        return try applyDeletions(assetsToRemove: assetsToRemove, missingHashes: actualMissing)
    }

    func reconcileWithRemoteListing(_ remoteFileNames: Set<String>) async throws -> CleanupMissingResourcesResult {
        let missing = itemsByFileName.values
            .filter { !remoteFileNames.contains($0.fileName) }
            .map(\.contentHash)
        let result = try cleanupMissingResources(missingHashes: Set(missing))
        if dirty {
            try await flushToRemote()
        }
        return result
    }

    /// Strict counterpart to `cleanupMissingResources`: also deletes assets whose
    /// fingerprint no longer matches their link set, with no metadata-only allowance.
    /// Backup-time inline reconcile must keep using the lenient form.
    func reconcileMonth(
        missingFileNames: Set<String> = [],
        missingHashes: Set<Data> = []
    ) throws -> CleanupMissingResourcesResult {
        var allMissingHashes = missingHashes
        for name in missingFileNames {
            if let res = itemsByFileName[name] {
                allMissingHashes.insert(res.contentHash)
            }
        }
        let actualMissing = allMissingHashes.intersection(itemsByHash.keys)
        let availableHashes = Set(itemsByHash.keys).subtracting(actualMissing)

        var assetsToRemove: Set<Data> = []
        for (fingerprint, asset) in assetsByFingerprint {
            let links = assetLinksByFingerprint[fingerprint] ?? []
            if Self.isAssetIncomplete(
                links: links,
                isResourceAvailable: { availableHashes.contains($0) },
                assetFingerprint: asset.assetFingerprint
            ) {
                assetsToRemove.insert(fingerprint)
            }
        }

        let orphanLinkFingerprints = Set(assetLinksByFingerprint.keys)
            .subtracting(assetsByFingerprint.keys)

        return try applyDeletions(
            assetsToRemove: assetsToRemove,
            orphanLinkFingerprints: orphanLinkFingerprints,
            missingHashes: actualMissing
        )
    }

    private func applyDeletions(
        assetsToRemove: Set<Data>,
        orphanLinkFingerprints: Set<Data> = [],
        missingHashes actualMissing: Set<Data>
    ) throws -> CleanupMissingResourcesResult {
        let linkFingerprintsToDelete = assetsToRemove.union(orphanLinkFingerprints)
        guard !actualMissing.isEmpty || !linkFingerprintsToDelete.isEmpty else {
            return CleanupMissingResourcesResult(
                removedResourceCount: 0,
                removedAssetCount: 0,
                removedOrphanLinkCount: 0
            )
        }

        try dbQueue.write { db in
            if !linkFingerprintsToDelete.isEmpty {
                try Self.forEachDataChunk(linkFingerprintsToDelete) { chunk, placeholders in
                    try db.execute(
                        sql: "DELETE FROM asset_resources WHERE assetFingerprint IN (\(placeholders))",
                        arguments: StatementArguments(chunk)
                    )
                }
            }
            if !assetsToRemove.isEmpty {
                try Self.forEachDataChunk(assetsToRemove) { chunk, placeholders in
                    try db.execute(
                        sql: "DELETE FROM assets WHERE assetFingerprint IN (\(placeholders))",
                        arguments: StatementArguments(chunk)
                    )
                }
            }
            if !actualMissing.isEmpty {
                try Self.forEachDataChunk(actualMissing) { chunk, placeholders in
                    try db.execute(
                        sql: "DELETE FROM resources WHERE contentHash IN (\(placeholders))",
                        arguments: StatementArguments(chunk)
                    )
                }
            }
        }

        for fingerprint in assetsToRemove {
            assetsByFingerprint.removeValue(forKey: fingerprint)
            assetLinksByFingerprint.removeValue(forKey: fingerprint)
        }
        for fingerprint in orphanLinkFingerprints {
            assetLinksByFingerprint.removeValue(forKey: fingerprint)
        }
        for hash in actualMissing {
            guard let fileName = itemsByHash.removeValue(forKey: hash) else { continue }
            itemsByFileName.removeValue(forKey: fileName)
            if remoteFilesByName[fileName] == nil {
                existingFileNameSet.remove(fileName)
            }
        }
        dirty = true

        return CleanupMissingResourcesResult(
            removedResourceCount: actualMissing.count,
            removedAssetCount: assetsToRemove.count,
            removedOrphanLinkCount: orphanLinkFingerprints.count
        )
    }

    // SQLite default SQLITE_MAX_VARIABLE_NUMBER is 999; chunk to stay safely below.
    private static func forEachDataChunk<C: Collection>(
        _ values: C,
        body: (_ chunk: [Data], _ placeholders: String) throws -> Void
    ) rethrows where C.Element == Data {
        let chunkSize = 400
        var buffer: [Data] = []
        buffer.reserveCapacity(chunkSize)
        for value in values {
            buffer.append(value)
            if buffer.count == chunkSize {
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
            let finalExists = try await client.exists(path: finalPath)
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

extension MonthManifestStore {
    /// Catches four failure modes: phantom (no links), broken link (resource gone),
    /// fingerprint-vs-link-set divergence, and metadata-only (only `adjustmentData`
    /// remaining). Latter two are invisible to a pure phantom/missing-resource check.
    static func isAssetIncomplete(
        links: [RemoteAssetResourceLink],
        isResourceAvailable: (Data) -> Bool,
        assetFingerprint: Data
    ) -> Bool {
        if links.isEmpty { return true }
        if links.contains(where: { !isResourceAvailable($0.resourceHash) }) {
            return true
        }
        let recomputed = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: links.map {
                (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash)
            }
        )
        if recomputed != assetFingerprint { return true }
        let metadataOnlyRoles: Set<Int> = [ResourceTypeCode.adjustmentData]
        return !links.contains { !metadataOnlyRoles.contains($0.role) }
    }
}

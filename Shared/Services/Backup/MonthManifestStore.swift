import Foundation
import GRDB

typealias MonthManifestStepLogger = @Sendable (String) -> Void

extension MonthManifestStore {
    /// Selects where a month's manifest sqlite is stored. Data/resource files (`YYYY/MM/...`) are
    /// identical across layouts — only the manifest, its temp upload, and its rename-backup move.
    enum ManifestLayout: Sendable, Equatable {
        case v1     // <YYYY>/<MM>/.watermelon_manifest.sqlite — current production layout
        case lite   // .watermelon/months/<YYYY-MM>.sqlite — dormant Repo V2 layout

        func manifestAbsolutePath(basePath: String, year: Int, month: Int) -> String {
            switch self {
            case .v1:
                return RemotePathBuilder.absolutePath(
                    basePath: basePath,
                    remoteRelativePath: String(format: "%04d/%02d/%@", year, month, MonthManifestStore.manifestFileName)
                )
            case .lite:
                return RepoLayoutLite.monthPath(basePath: basePath, month: LibraryMonthKey(year: year, month: month))
            }
        }

        func manifestDirectoryAbsolutePath(basePath: String, year: Int, month: Int) -> String {
            switch self {
            case .v1:
                return RemotePathBuilder.absolutePath(
                    basePath: basePath,
                    remoteRelativePath: String(format: "%04d/%02d", year, month)
                )
            case .lite:
                return RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
            }
        }
    }
}

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

    /// Where the per-month manifest sqlite lives. Data/resource paths are layout-independent;
    /// only the manifest file and its temp/backup siblings move between layouts.
    let layout: ManifestLayout

    var monthRelativePath: String {
        String(format: "%04d/%02d", year, month)
    }

    var monthAbsolutePath: String {
        RemotePathBuilder.absolutePath(basePath: basePath, remoteRelativePath: monthRelativePath)
    }

    var manifestAbsolutePath: String {
        layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
    }

    /// Directory that holds the manifest + its temp/backup siblings. Equals `monthAbsolutePath`
    /// for `.v1`; the shared `.watermelon/months` directory for `.lite`.
    var manifestDirectoryAbsolutePath: String {
        layout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)
    }

    let client: RemoteStorageClientProtocol
    let basePath: String
    let localManifestURL: URL
    let dbQueue: DatabaseQueue

    var itemsByFileName: [String: RemoteManifestResource] = [:]
    var itemsByHash: [Data: String] = [:]

    var assetsByFingerprint: [Data: RemoteManifestAsset] = [:]
    var assetLinksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]

    // Indexes derived from assetLinksByFingerprint. Maintained incrementally on upsertAsset
    // and applyDeletions; rebuilt wholesale on reloadCache. Without these, the legacy-import
    // finders below would scan all assets and rebuild link Sets per call → O(N²) per month
    // when the manifest fills up.
    private var linkKeySetByFingerprint: [Data: Set<LinkKey>] = [:]
    private var assetsByResourceHash: [Data: Set<Data>] = [:]

    var remoteFilesByName: [String: RemoteFileMetadata] = [:]
    var existingFileNameSet: Set<String> = []
    /// Lazily-built fold of `existingFileNameSet` through `RemoteFileNaming.collisionKey`.
    /// Built on first read, incrementally updated on insert, invalidated on removal.
    /// Without this cache, processBundle re-folds the entire set per bundle → O(N²) per month.
    private var collisionKeysCache: Set<String>?
    private(set) var dirty: Bool = false

    let stepLogger: MonthManifestStepLogger?

    init(
        client: RemoteStorageClientProtocol,
        basePath: String,
        year: Int,
        month: Int,
        localManifestURL: URL,
        dbQueue: DatabaseQueue,
        remoteFilesByName: [String: RemoteFileMetadata],
        dirty: Bool,
        layout: ManifestLayout = .v1,
        stepLogger: MonthManifestStepLogger? = nil
    ) {
        self.client = client
        self.basePath = basePath
        self.year = year
        self.month = month
        self.layout = layout
        self.localManifestURL = localManifestURL
        self.dbQueue = dbQueue
        self.remoteFilesByName = remoteFilesByName
        existingFileNameSet = Set(remoteFilesByName.keys)
        self.dirty = dirty
        self.stepLogger = stepLogger
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
        // Candidates = fingerprints whose links cover ALL of resources' hashes.
        // Intersect over each hash bucket; if any hash has no asset, no enclosing exists.
        var candidates: Set<Data>?
        for r in resources {
            guard let bucket = assetsByResourceHash[r.hash], !bucket.isEmpty else { return nil }
            if let existing = candidates {
                let inter = existing.intersection(bucket)
                if inter.isEmpty { return nil }
                candidates = inter
            } else {
                candidates = bucket
            }
        }
        guard let candidates else { return nil }
        let needed = Self.linkKeySet(fromTuples: resources)
        for fingerprint in candidates {
            guard let have = linkKeySetByFingerprint[fingerprint] else { continue }
            if have.count < needed.count { continue }
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
        // Candidates = fingerprints sharing AT LEAST ONE hash with resources. A strict subset's
        // links are a subset of resources' tuples → its hashes are necessarily a subset too.
        var candidates: Set<Data> = []
        for r in resources {
            if let bucket = assetsByResourceHash[r.hash] { candidates.formUnion(bucket) }
        }
        if candidates.isEmpty { return [] }
        let needed = Self.linkKeySet(fromTuples: resources)
        var result: [Data] = []
        for fingerprint in candidates {
            guard let have = linkKeySetByFingerprint[fingerprint] else { continue }
            if have.isEmpty { continue }
            if have.count >= needed.count { continue }
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

    /// Rebuilds the link/hash indexes from `assetLinksByFingerprint`. Call after wholesale
    /// reloads (`reloadCache`, `seedDatabase`) where assetLinksByFingerprint is replaced en masse.
    func rebuildLinkIndexes() {
        linkKeySetByFingerprint.removeAll(keepingCapacity: true)
        assetsByResourceHash.removeAll(keepingCapacity: true)
        linkKeySetByFingerprint.reserveCapacity(assetLinksByFingerprint.count)
        for (fingerprint, links) in assetLinksByFingerprint {
            linkKeySetByFingerprint[fingerprint] = Self.linkKeySet(fromLinks: links)
            for link in links {
                assetsByResourceHash[link.resourceHash, default: []].insert(fingerprint)
            }
        }
    }

    private func indexAddAsset(fingerprint: Data, links: [RemoteAssetResourceLink]) {
        linkKeySetByFingerprint[fingerprint] = Self.linkKeySet(fromLinks: links)
        for link in links {
            assetsByResourceHash[link.resourceHash, default: []].insert(fingerprint)
        }
    }

    private func indexRemoveAsset(fingerprint: Data) {
        if let oldLinks = assetLinksByFingerprint[fingerprint] {
            for link in oldLinks {
                assetsByResourceHash[link.resourceHash]?.remove(fingerprint)
                if assetsByResourceHash[link.resourceHash]?.isEmpty == true {
                    assetsByResourceHash.removeValue(forKey: link.resourceHash)
                }
            }
        }
        linkKeySetByFingerprint.removeValue(forKey: fingerprint)
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

    /// Folded view of existingFileNames for Unicode-insensitive collision checks. Cached.
    func existingCollisionKeys() -> Set<String> {
        if let cache = collisionKeysCache { return cache }
        var built = Set<String>()
        built.reserveCapacity(existingFileNameSet.count)
        for name in existingFileNameSet {
            built.insert(RemoteFileNaming.collisionKey(for: name))
        }
        collisionKeysCache = built
        return built
    }

    func invalidateCollisionKeyCache() {
        collisionKeysCache = nil
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
        collisionKeysCache?.insert(RemoteFileNaming.collisionKey(for: item.fileName))
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
            indexRemoveAsset(fingerprint: fingerprint)
            assetsByFingerprint[fingerprint] = nil
            assetLinksByFingerprint[fingerprint] = nil
        }
        indexRemoveAsset(fingerprint: asset.assetFingerprint)
        assetsByFingerprint[asset.assetFingerprint] = asset
        assetLinksByFingerprint[asset.assetFingerprint] = normalizedLinks
        indexAddAsset(fingerprint: asset.assetFingerprint, links: normalizedLinks)
        dirty = true
    }

    func markRemoteFile(name: String, size: Int64) {
        remoteFilesByName[name] = RemoteFileMetadata(size: size)
        existingFileNameSet.insert(name)
        collisionKeysCache?.insert(RemoteFileNaming.collisionKey(for: name))
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

    /// `assertOwnership`, when provided (Lite write lease), must confirm ownership before this load-time
    /// reconcile/schema-sync flush — the first remote manifest write a Lite worker performs. A `false`
    /// result fails closed (mirrors `RemoteIndexSyncService.verifyMonth`) so a lost/foreign lease never
    /// overwrites a foreign writer's manifest.
    func reconcileWithRemoteListing(
        _ remoteFileNames: Set<String>,
        assertOwnership: (@Sendable () async -> Bool)? = nil
    ) async throws -> CleanupMissingResourcesResult {
        let missing = itemsByFileName.values
            .filter { !remoteFileNames.contains($0.fileName) }
            .map(\.contentHash)
        let result = try cleanupMissingResources(missingHashes: Set(missing))
        if dirty {
            if let assertOwnership, await assertOwnership() == false {
                throw LiteRepoError.ownershipLost
            }
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
            indexRemoveAsset(fingerprint: fingerprint)
            assetsByFingerprint.removeValue(forKey: fingerprint)
            assetLinksByFingerprint.removeValue(forKey: fingerprint)
        }
        for fingerprint in orphanLinkFingerprints {
            indexRemoveAsset(fingerprint: fingerprint)
            assetLinksByFingerprint.removeValue(forKey: fingerprint)
        }
        var anyFileNameRemoved = false
        for hash in actualMissing {
            guard let fileName = itemsByHash.removeValue(forKey: hash) else { continue }
            itemsByFileName.removeValue(forKey: fileName)
            if remoteFilesByName[fileName] == nil {
                existingFileNameSet.remove(fileName)
                anyFileNameRemoved = true
            }
        }
        if anyFileNameRemoved { collisionKeysCache = nil }
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
        let manifestDirectory = manifestDirectoryAbsolutePath
        do {
            try await client.createDirectory(path: manifestDirectory)
        } catch {
            stepLogger?(String.localizedStringWithFormat(
                String(localized: "backup.manifest.diagnostic.createMonthDirFailed"),
                monthRelativePath,
                error.localizedDescription
            ))
            throw error
        }
        if !ignoreCancellation {
            try Task.checkCancellation()
        }

        // Upload an integrity-checked snapshot, not the live DB: the live file can be mid-write or
        // hold WAL pages, and we want a stable byte image to read back and verify against.
        let exportURL = Self.makeLocalManifestURL(year: year, month: month)
        defer { Self.removeScratchFile(at: exportURL) }
        let exportedData = try exportVerifiedManifestCopy(to: exportURL)
        if !ignoreCancellation {
            try Task.checkCancellation()
        }

        let finalPath = manifestAbsolutePath
        // Avoid dot-prefix + `.sqlite` here: some NAS AV/extension filters reject those with STATUS_OBJECT_NAME_NOT_FOUND.
        let tempRemotePath = manifestDirectory + "/manifest_\(UUID().uuidString).tmp"

        do {
            do {
                try await client.upload(
                    localURL: exportURL,
                    remotePath: tempRemotePath,
                    respectTaskCancellation: !ignoreCancellation,
                    onProgress: nil
                )
            } catch {
                if !(error is CancellationError) {
                    stepLogger?(String.localizedStringWithFormat(
                        String(localized: "backup.manifest.diagnostic.uploadManifestTempFailed"),
                        monthRelativePath,
                        error.localizedDescription
                    ))
                }
                throw error
            }
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
        // Confirm the persisted manifest is byte-identical to what we uploaded before declaring the
        // flush durable. A read-back mismatch leaves `dirty` set so the next flush re-uploads.
        try await verifyRemoteManifestBytes(
            at: finalPath,
            expected: exportedData,
            ignoreCancellation: ignoreCancellation
        )

        if !ignoreCancellation {
            try Task.checkCancellation()
        }
        dirty = false
        return true
    }

    /// `VACUUM INTO` a fresh file, `PRAGMA quick_check` it, and return its bytes. The export is a
    /// self-contained, defragmented copy with no attached WAL, so its bytes are stable for read-back.
    private func exportVerifiedManifestCopy(to exportURL: URL) throws -> Data {
        Self.removeScratchFile(at: exportURL)   // VACUUM INTO refuses to overwrite an existing file.
        try dbQueue.vacuum(into: exportURL.path)
        try Self.runQuickCheck(on: exportURL)
        return try Data(contentsOf: exportURL)
    }

    private static func runQuickCheck(on url: URL) throws {
        let queue = try DatabaseQueue(path: url.path)
        defer { try? queue.close() }
        let results = try queue.read { db in
            try String.fetchAll(db, sql: "PRAGMA quick_check")
        }
        guard results == ["ok"] else {
            throw NSError(
                domain: "MonthManifestStore",
                code: -37,
                userInfo: [NSLocalizedDescriptionKey: "Manifest integrity check failed before upload: \(results.joined(separator: "; "))"]
            )
        }
    }

    private func verifyRemoteManifestBytes(
        at finalPath: String,
        expected: Data,
        ignoreCancellation: Bool
    ) async throws {
        let verifyURL = Self.makeLocalManifestURL(year: year, month: month)
        defer { Self.removeScratchFile(at: verifyURL) }
        do {
            try await client.download(remotePath: finalPath, localURL: verifyURL)
        } catch {
            if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                throw CancellationError()
            }
            // Manifest already committed above; only an actual read-back cancellation is exempt — a
            // non-cancellation error stays a hard -36 even when the task is already cancelled.
            if ignoreCancellation, error is CancellationError {
                return
            }
            throw NSError(
                domain: "MonthManifestStore",
                code: -36,
                userInfo: [
                    NSLocalizedDescriptionKey: "Failed to read back manifest for verification: \(error.localizedDescription)",
                    NSUnderlyingErrorKey: error
                ]
            )
        }
        let actual = (try? Data(contentsOf: verifyURL)) ?? Data()
        guard actual == expected else {
            throw NSError(
                domain: "MonthManifestStore",
                code: -36,
                userInfo: [NSLocalizedDescriptionKey: "Manifest read-back mismatch for \(monthRelativePath): uploaded \(expected.count) bytes, remote returned \(actual.count) bytes"]
            )
        }
    }

    private static func removeScratchFile(at url: URL) {
        try? FileManager.default.removeItem(at: url)
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
                stepLogger?(String.localizedStringWithFormat(
                    String(localized: "backup.manifest.diagnostic.renameManifestFailed"),
                    monthRelativePath,
                    error.localizedDescription
                ))
                throw error
            }

            let backupPath = manifestDirectoryAbsolutePath + "/manifest_\(UUID().uuidString).bak"
            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            do {
                try await client.move(from: finalPath, to: backupPath)
            } catch {
                await Task {
                    try? await client.move(from: backupPath, to: finalPath)
                }.value
                throw error
            }

            do {
                if !ignoreCancellation {
                    try Task.checkCancellation()
                }
                try await client.move(from: tempRemotePath, to: finalPath)
            } catch {
                if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                    await Task {
                        try? await client.delete(path: finalPath)
                        try? await client.move(from: backupPath, to: finalPath)
                    }.value
                    throw CancellationError()
                }
                await Task {
                    try? await client.delete(path: finalPath)
                    try? await client.move(from: backupPath, to: finalPath)
                }.value
                stepLogger?(String.localizedStringWithFormat(
                    String(localized: "backup.manifest.diagnostic.renameManifestFailed"),
                    monthRelativePath,
                    error.localizedDescription
                ))
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

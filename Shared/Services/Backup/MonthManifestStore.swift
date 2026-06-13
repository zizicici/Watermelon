import Foundation
import GRDB

typealias MonthManifestStepLogger = @Sendable (String) -> Void

// Re-asserts the Lite write lease against the backend. Carried by a store opened for an owned Lite
// write so the flush primitive — not a caller convention — gates every dirty Lite manifest write.
typealias MonthManifestOwnershipAssertion = @Sendable () async throws -> Void

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

    /// Lite write lease, owned by the store. Present only on an owned Lite write path; `nil` for V1 and
    /// read-only loads. `flushToRemote` fails closed for a `.lite` store whenever this is absent or returns
    /// false, so a lost/foreign lease can never overwrite a foreign writer's manifest.
    private let liteWriteOwnership: MonthManifestOwnershipAssertion?
    private let liteMonthsListing: LiteMonthsListingSnapshot?

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
        layout: ManifestLayout,
        liteWriteOwnership: MonthManifestOwnershipAssertion? = nil,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil,
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
        self.liteWriteOwnership = liteWriteOwnership
        self.liteMonthsListing = liteMonthsListing
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

    func manifestFileNames() -> Set<String> {
        Set(itemsByFileName.keys)
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

    /// Load-time reconcile/schema-sync: prune resources missing from the remote listing, then push the
    /// change. This is the first remote manifest write a Lite worker performs; the dirty flush re-asserts
    /// the store-owned Lite write lease inside `flushToRemote` and fails closed if it is lost, so a
    /// lost/foreign lease never overwrites a foreign writer's manifest.
    func reconcileWithRemoteListing(
        _ remoteFileNames: Set<String>
    ) async throws -> CleanupMissingResourcesResult {
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
        try await assertLiteWriteOwnership(ignoreCancellation: ignoreCancellation)
        if !ignoreCancellation {
            try Task.checkCancellation()
        }
        // A directory introduced at the canonical month path after load is damaged/foreign control state:
        // fail closed before publishing. loadOrCreate's guard runs only at load, and RemoteMoveReplace is
        // type-blind, so a post-load directory would otherwise be moved aside and deleted by the publish.
        if layout == .lite {
            try await assertLiteCanonicalPathNotDirectory(ignoreCancellation: ignoreCancellation)
        }
        let manifestDirectory = manifestDirectoryAbsolutePath
        let remoteClient = client
        do {
            try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
                try await remoteClient.createDirectory(path: manifestDirectory)
            }
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
        let tempRemotePath = scratchManifestPath(suffix: "tmp")
        let backupRemotePath: String

        do {
            do {
                try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
                    try await remoteClient.upload(
                        localURL: exportURL,
                        remotePath: tempRemotePath,
                        respectTaskCancellation: !ignoreCancellation,
                        onProgress: nil
                    )
                }
                if layout == .lite {
                    await liteMonthsListing?.noteScratchCreated(path: tempRemotePath, basePath: basePath)
                }
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
            backupRemotePath = try await moveReplacingExistingManifest(
                tempRemotePath: tempRemotePath,
                finalPath: finalPath,
                ignoreCancellation: ignoreCancellation
            )
            if layout == .lite {
                await liteMonthsListing?.invalidate(basePath: basePath)
            }
        } catch {
            if layout == .lite {
                await liteMonthsListing?.invalidate(basePath: basePath)
            }
            if !ignoreCancellation, Task.isCancelled || error is CancellationError {
                throw CancellationError()
            }
            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            if (try? await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation, {
                try await remoteClient.exists(path: tempRemotePath)
            })) == true {
                do {
                    try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
                        try await remoteClient.delete(path: tempRemotePath)
                    }
                    await liteMonthsListing?.noteDeleted(path: tempRemotePath)
                } catch {}
            }
            throw error
        }

        if !ignoreCancellation {
            try Task.checkCancellation()
        }
        if (try? await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation, {
            try await remoteClient.exists(path: tempRemotePath)
        })) == true {
            do {
                try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
                    try await remoteClient.delete(path: tempRemotePath)
                }
                await liteMonthsListing?.noteDeleted(path: tempRemotePath)
            } catch {}
        }

        if !ignoreCancellation {
            try Task.checkCancellation()
        }
        // Confirm the persisted manifest is byte-identical to what we uploaded before declaring the
        // flush durable. A read-back mismatch leaves `dirty` set so the next flush re-uploads.
        do {
            try await verifyRemoteManifestBytes(
                at: finalPath,
                expected: exportedData,
                ignoreCancellation: ignoreCancellation
            )
        } catch {
            // The replacement did not read back byte-exact, so it is not durable. If we overwrote an existing
            // canonical, revert to the prior good manifest we backed up — a valid-but-mismatched replacement
            // must not be left canonical (cleanup would otherwise reclaim the backup as redundant scratch).
            await restorePriorCanonicalFromBackup(
                backupPath: backupRemotePath,
                finalPath: finalPath,
                ignoreCancellation: ignoreCancellation
            )
            throw error
        }

        // Read-back proved the replacement durable, so the prior canonical we backed up is now redundant.
        // Drop it inline (like the temp above) so a surviving month `.bak` always means an unverified
        // replacement — letting cleanup preserve it instead of reclaiming the last verified-good copy.
        if (try? await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation, {
            try await remoteClient.exists(path: backupRemotePath)
        })) == true {
            do {
                try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
                    try await remoteClient.delete(path: backupRemotePath)
                }
                await liteMonthsListing?.noteDeleted(path: backupRemotePath)
            } catch {}
        }

        if !ignoreCancellation {
            try Task.checkCancellation()
        }
        dirty = false
        return true
    }

    /// Fails a dirty `.lite` flush closed unless the store-owned write lease is present and still valid.
    /// V1 and read-only stores hold no lease and pass through. Runs before any remote mutation.
    private func assertLiteWriteOwnership(ignoreCancellation: Bool = false) async throws {
        guard layout == .lite else { return }
        guard let liteWriteOwnership else {
            throw LiteRepoError.ownershipLost
        }
        if ignoreCancellation {
            try await Task { try await liteWriteOwnership() }.value
        } else {
            try await liteWriteOwnership()
        }
    }

    /// Directory check at the canonical Lite month path before publish. An unresolved type probe is not
    /// proof the slot is safe to replace: propagate the fault so the flush fails closed (matching
    /// loadOrCreate's load-time guard), rather than letting the type-blind publish move/delete a directory
    /// we couldn't rule out. A genuine absence (`metadata == nil`) is a safe fresh/file slot and proceeds.
    private func assertLiteCanonicalPathNotDirectory(ignoreCancellation: Bool) async throws {
        let probe = try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
            try await self.client.metadata(path: self.manifestAbsolutePath)
        }
        if probe?.isDirectory == true {
            throw LiteRepoError.existingLiteManifestConflict(
                month: LibraryMonthKey(year: year, month: month).text
            )
        }
    }

    /// `VACUUM INTO` a fresh file, `PRAGMA quick_check` it, and return its bytes. The export is a
    /// self-contained, defragmented copy with no attached WAL, so its bytes are stable for read-back.
    private func exportVerifiedManifestCopy(to exportURL: URL) throws -> Data {
        Self.removeScratchFile(at: exportURL)   // VACUUM INTO refuses to overwrite an existing file.
        try dbQueue.vacuum(into: exportURL.path)
        try Self.runQuickCheck(on: exportURL)
        return try Data(contentsOf: exportURL)
    }

    static func runQuickCheck(on url: URL) throws {
        let results = try RemoteSqliteValidator.quickCheckResults(at: url)
        guard results == ["ok"] else {
            throw makeManifestQuickCheckError(results: results)
        }
    }

    private func verifyRemoteManifestBytes(
        at finalPath: String,
        expected: Data,
        ignoreCancellation: Bool
    ) async throws {
        let verifyURL = Self.makeLocalManifestURL(year: year, month: month)
        let remoteClient = client
        defer { Self.removeScratchFile(at: verifyURL) }

        var lastFailure: Error?
        for attempt in 0..<2 {
            Self.removeScratchFile(at: verifyURL)
            do {
                try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
                    try await remoteClient.download(remotePath: finalPath, localURL: verifyURL)
                }
            } catch {
                let isCancellationError = RemoteFaultLite.classify(error) == .cancelled
                if !ignoreCancellation && (Task.isCancelled || isCancellationError) {
                    throw CancellationError()
                }
                if isCancellationError {
                    throw Self.makeReadBackDownloadError(
                        manifestPath: monthRelativePath,
                        underlying: error
                    )
                }
                lastFailure = Self.makeReadBackDownloadError(
                    manifestPath: monthRelativePath,
                    underlying: error
                )
                if attempt == 0 { continue }
                throw lastFailure!
            }
            if !ignoreCancellation {
                try Task.checkCancellation()
            }
            let actual = (try? Data(contentsOf: verifyURL)) ?? Data()
            guard actual == expected else {
                lastFailure = Self.makeReadBackMismatchError(
                    manifestPath: monthRelativePath,
                    expectedByteCount: expected.count,
                    actualByteCount: actual.count
                )
                if attempt == 0 { continue }
                throw lastFailure!
            }
            return
        }
        throw lastFailure ?? Self.makeReadBackVerificationError(manifestPath: monthRelativePath)
    }

    static func isReadBackVerificationError(_ error: Error) -> Bool {
        let ns = error as NSError
        return ns.domain == "MonthManifestStore" && ns.code == -36
    }

    static func makeManifestQuickCheckError(results: [String]) -> NSError {
        NSError(
            domain: "MonthManifestStore",
            code: -37,
            userInfo: [
                NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                    String(localized: "backup.manifest.error.quickCheckBeforeUploadFailed"),
                    results.joined(separator: "; ")
                )
            ]
        )
    }

    static func makeReadBackDownloadError(
        manifestPath: String,
        underlying: Error
    ) -> NSError {
        makeReadBackVerificationError(
            description: String.localizedStringWithFormat(
                String(localized: "backup.manifest.error.readBackDownloadFailed"),
                manifestPath,
                underlying.localizedDescription
            ),
            underlying: underlying
        )
    }

    static func makeReadBackMismatchError(
        manifestPath: String,
        expectedByteCount: Int,
        actualByteCount: Int
    ) -> NSError {
        makeReadBackVerificationError(
            description: String.localizedStringWithFormat(
                String(localized: "backup.manifest.error.readBackMismatch"),
                manifestPath,
                expectedByteCount,
                actualByteCount
            )
        )
    }

    static func makeReadBackVerificationError(manifestPath: String) -> NSError {
        makeReadBackVerificationError(
            description: String.localizedStringWithFormat(
                String(localized: "backup.manifest.error.readBackVerificationFailed"),
                manifestPath
            )
        )
    }

    private static func makeReadBackVerificationError(
        description: String,
        underlying: Error? = nil
    ) -> NSError {
        var userInfo: [String: Any] = [NSLocalizedDescriptionKey: description]
        if let underlying {
            userInfo[NSUnderlyingErrorKey] = underlying
        }
        return NSError(domain: "MonthManifestStore", code: -36, userInfo: userInfo)
    }

    private static func removeScratchFile(at url: URL) {
        try? FileManager.default.removeItem(at: url)
    }

    // Scratch sibling for the manifest upload/backup dance. Lite names are final-derived
    // ("<YYYY-MM>.sqlite.<uuid>.tmp"/".bak") so repair-first cleanup can recover the intended canonical
    // month; V1 keeps its opaque "manifest_<uuid>" name. Neither is dot-prefixed nor ends in `.sqlite`,
    // which some NAS AV/extension filters reject with STATUS_OBJECT_NAME_NOT_FOUND.
    private func scratchManifestPath(suffix: String) -> String {
        let scratchSuffix: RepoLayoutLite.ScratchSuffix = suffix == "bak" ? .backup : .temp
        switch layout {
        case .v1:
            return RepoLayoutLite.v1OpaqueMonthScratchPath(
                directory: manifestDirectoryAbsolutePath,
                suffix: scratchSuffix
            )
        case .lite:
            return RepoLayoutLite.liteMonthScratchPath(
                basePath: basePath,
                month: LibraryMonthKey(year: year, month: month),
                suffix: scratchSuffix
            )
        }
    }

    /// Returns the backup path it used: when an existing canonical was overwritten it now holds the prior
    /// manifest, so `flushToRemote` can revert to it if the post-replace read-back fails.
    private func moveReplacingExistingManifest(
        tempRemotePath: String,
        finalPath: String,
        ignoreCancellation: Bool
    ) async throws -> String {
        let backupPath = scratchManifestPath(suffix: "bak")
        try await RemoteMoveReplace.moveReplacing(
            client: client,
            tempPath: tempRemotePath,
            finalPath: finalPath,
            backupPath: backupPath,
            ignoreCancellation: ignoreCancellation,
            assertOwnership: { try await self.assertLiteWriteOwnership(ignoreCancellation: ignoreCancellation) },
            // Read-back validation runs in flushToRemote after this returns, so an existing canonical month
            // manifest must be backed up before overwrite (even on overwrite-permitting backends) so a failed
            // read-back can recover the prior good copy.
            backupExistingFinal: true,
            onRenameFailure: { [stepLogger, month = monthRelativePath] error in
                stepLogger?(String.localizedStringWithFormat(
                    String(localized: "backup.manifest.diagnostic.renameManifestFailed"),
                    month,
                    error.localizedDescription
                ))
            }
        )
        return backupPath
    }

    // Reverts the canonical to the prior manifest we backed up before this flush's overwrite, after a
    // read-back mismatch proved the replacement is not durable. A valid-but-byte-mismatched replacement must
    // not be left canonical, or cleanup could later reclaim the backup as redundant scratch behind it.
    // Ownership-gated + cancellation-shielded; a no-op when no backup was made (fresh month). If the revert
    // can't complete, the backup survives for OrphanCleanupLite's repair-first restore.
    private func restorePriorCanonicalFromBackup(
        backupPath: String,
        finalPath: String,
        ignoreCancellation: Bool
    ) async {
        do {
            guard try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation, {
                try await self.client.exists(path: backupPath)
            }) else { return }
            try await assertLiteWriteOwnership(ignoreCancellation: ignoreCancellation)
            if (try? await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation, {
                try await self.client.exists(path: finalPath)
            })) == true {
                // Re-prove after the awaited probe so a lease lost during it cannot delete a successor's canonical.
                try await assertLiteWriteOwnership(ignoreCancellation: ignoreCancellation)
                try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
                    try await self.client.delete(path: finalPath)
                }
            }
            try await assertLiteWriteOwnership(ignoreCancellation: ignoreCancellation)
            try await Self.shieldedRemoteOperation(ignoreCancellation: ignoreCancellation) {
                try await self.client.move(from: backupPath, to: finalPath)
            }
        } catch {
            // Best-effort: a lost lease / fault leaves the backup in place for OrphanCleanupLite to restore.
        }
        if layout == .lite {
            await liteMonthsListing?.invalidate(basePath: basePath)
        }
    }

    private static func shieldedRemoteOperation<T: Sendable>(
        ignoreCancellation: Bool,
        _ operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        if ignoreCancellation {
            return try await Task { try await operation() }.value
        }
        return try await operation()
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

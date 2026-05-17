import Foundation

/// In-memory month state cache for `V2MonthSession`. Owns the materialized
/// projection of repo state plus pending op tracking; presence gating is the
/// only filter — nothing is dropped from the snapshot-emit path.
///
/// Single-writer (the backup worker's serial queue per month) — no internal locking.
final class V2MonthIndexes {
    let year: Int
    let month: Int

    // Materialized state — keyed by physicalRemotePath so multi-path same-hash is natural.
    private var resourcesByPath: [String: RemoteManifestResource]
    private var assetsByFingerprint: [Data: RemoteManifestAsset]
    private var linksByFingerprint: [Data: [RemoteAssetResourceLink]]
    /// `findResourceByHash` returns lex-min over present paths only; missing-path lookup would bind metadata to undownloadable bytes.
    private var pathsByHash: [Data: Set<String>]
    /// Reverse name indexes keep upload preparation from scanning every resource in a month.
    private var resourcesByLeafName: [String: [RemoteManifestResource]] = [:]
    private var resourcesByCollisionKey: [String: [RemoteManifestResource]] = [:]
    private var collisionKeysCache: Set<String>?
    /// Per-path presence; missing/inconclusive paths are excluded from find ops, hash-missing
    /// is derived on demand for cache-wide consumers (Home / download / health / resume).
    private var presenceMap: RemoteMonthPresenceMap

    // Existing remote files from start-of-month directory listing (collision rename input).
    private var remoteFilesByName: [String: MonthManifestStore.RemoteFileMetadata]
    private var existingFileNameSet: Set<String>

    /// Mirror of `RepoMonthState.deletedAssetStamps`; survives flushes so snapshot
    /// emits deletedKey rows. Legacy unstamped tombstones live in the Set side.
    private(set) var deletedAssetStamps: [Data: OpStamp]
    private(set) var legacyDeletedAssetFingerprints: Set<Data>

    /// Resource rows projected from committed `addAsset` bodies, keyed by path.
    /// Seeded unconditionally from `materializedState.resources` (which is itself
    /// `fold(commits in covered)`) and updated by `recordCommit`. Snapshot emit
    /// reads directly from here — never from the live `resourcesByPath` — so an
    /// in-session `upsertResource` overwriting a committed path with a different
    /// hash cannot leak the replacement bytes into the snapshot baseline.
    private var committedResourceByPath: [String: SnapshotResourceRow]

    // Pending V2 ops since last flush.
    private(set) var pendingV2AssetFingerprints: Set<Data> = []
    private(set) var pendingV2TombstoneFingerprints: Set<Data> = []

    private let nameCase: BackendNameCaseSensitivity

    var hasUncommittedOps: Bool {
        !pendingV2AssetFingerprints.isEmpty || !pendingV2TombstoneFingerprints.isEmpty
    }

    var hasAnyAsset: Bool { !assetsByFingerprint.isEmpty }

    init(
        year: Int,
        month: Int,
        materializedState: RepoMonthState,
        remoteFilesByName: [String: MonthManifestStore.RemoteFileMetadata],
        verifiedMissingHashes: Set<Data>?,
        nameCase: BackendNameCaseSensitivity
    ) {
        self.year = year
        self.month = month
        self.nameCase = nameCase
        self.remoteFilesByName = remoteFilesByName
        self.existingFileNameSet = Set(remoteFilesByName.keys)

        // Faithful projection; filtering here would leak into snapshot writes and break the covered-range invariant.
        var resourcesByPath: [String: RemoteManifestResource] = [:]
        var pathsByHash: [Data: Set<String>] = [:]
        var resourcesByLeafName: [String: [RemoteManifestResource]] = [:]
        var resourcesByCollisionKey: [String: [RemoteManifestResource]] = [:]
        var presenceMap = RemoteMonthPresenceMap()
        var sizesByPresenceKey: [String: Set<Int64>] = [:]
        for (name, meta) in remoteFilesByName {
            sizesByPresenceKey[nameCase.presenceKey(for: name), default: []].insert(meta.size)
        }
        for row in materializedState.resources.values {
            let logicalName = (row.physicalRemotePath as NSString).lastPathComponent
            let key = nameCase.presenceKey(for: logicalName)
            let listedSizeMatches = sizesByPresenceKey[key]?.contains(row.fileSize) == true
            let presence: RemoteResourcePresence
            if let verifiedMissingHashes {
                if !listedSizeMatches || verifiedMissingHashes.contains(row.contentHash) {
                    presence = .missing
                } else {
                    presence = .listedSizeMatched
                }
            } else if listedSizeMatches {
                // Listing already confirmed name + size — same trust level as the overlay
                // grants any resource it can't SHA-verify. Treating it as `.inconclusive` here
                // breaks dedup and re-uploads everything when no overlay probe has run.
                presence = .listedSizeMatched
            } else {
                presence = .missing
            }
            presenceMap.mark(path: row.physicalRemotePath, presence)
            let resource = RemoteManifestResource(
                year: year,
                month: month,
                physicalRemotePath: row.physicalRemotePath,
                contentHash: row.contentHash,
                fileSize: row.fileSize,
                resourceType: row.resourceType,
                creationDateMs: row.creationDateMs,
                backedUpAtMs: row.backedUpAtMs,
                crypto: row.crypto
            )
            resourcesByPath[row.physicalRemotePath] = resource
            pathsByHash[row.contentHash, default: []].insert(row.physicalRemotePath)
            let leaf = (row.physicalRemotePath as NSString).lastPathComponent
            resourcesByLeafName[leaf, default: []].append(resource)
            resourcesByCollisionKey[RemoteFileNaming.collisionKey(for: leaf), default: []].append(resource)
        }
        self.resourcesByPath = resourcesByPath
        self.pathsByHash = pathsByHash
        self.resourcesByLeafName = resourcesByLeafName
        self.resourcesByCollisionKey = resourcesByCollisionKey
        self.presenceMap = presenceMap

        var assetsByFingerprint: [Data: RemoteManifestAsset] = [:]
        for row in materializedState.assets.values {
            assetsByFingerprint[row.assetFingerprint] = RemoteManifestAsset(
                year: year,
                month: month,
                assetFingerprint: row.assetFingerprint,
                creationDateMs: row.creationDateMs,
                backedUpAtMs: row.backedUpAtMs,
                resourceCount: row.resourceCount,
                totalFileSizeBytes: row.totalFileSizeBytes,
                stamp: row.stamp
            )
        }
        self.assetsByFingerprint = assetsByFingerprint

        var linksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]
        for row in materializedState.assetResources.values {
            linksByFingerprint[row.assetFingerprint, default: []].append(RemoteAssetResourceLink(
                year: year,
                month: month,
                assetFingerprint: row.assetFingerprint,
                resourceHash: row.resourceHash,
                role: row.role,
                slot: row.slot,
                logicalName: row.logicalName
            ))
        }
        self.linksByFingerprint = linksByFingerprint

        self.deletedAssetStamps = materializedState.deletedAssetStamps
        self.legacyDeletedAssetFingerprints = materializedState.deletedAssetFingerprints
            .subtracting(materializedState.deletedAssetStamps.keys)
        // Faithful seed: every row from `materializedState.resources` is in
        // `fold(commits in covered)` already, so preserving them on the next
        // snapshot emit keeps `state == fold(covered)`. Tombstoned-asset orphan
        // rows are part of fold(covered) (RepoMaterializer keeps them) and must
        // survive forward.
        self.committedResourceByPath = materializedState.resources
    }

    // MARK: - Read

    func containsAssetFingerprint(_ fingerprint: Data) -> Bool {
        assetsByFingerprint[fingerprint] != nil
    }

    func isAssetIncomplete(_ fingerprint: Data) -> Bool {
        guard let asset = assetsByFingerprint[fingerprint] else { return false }
        let links = linksByFingerprint[fingerprint] ?? []
        // Filter gates actionability only; materialized state stays faithful to the commit log.
        return MonthManifestStore.isAssetIncomplete(
            links: links,
            isResourceAvailable: { hash in
                self.anyPresentPath(forHash: hash) != nil
            },
            assetFingerprint: asset.assetFingerprint
        )
    }

    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource? {
        // Lex-min over all paths would let a missing path shadow a present one and bind metadata to undownloadable bytes.
        guard let chosen = anyPresentPath(forHash: contentHash) else { return nil }
        return resourcesByPath[chosen]
    }

    func findByFileName(_ logicalName: String) -> RemoteManifestResource? {
        let leafName = logicalName
            .split(separator: "/", omittingEmptySubsequences: true)
            .last
            .map(String.init) ?? logicalName
        let candidates: [RemoteManifestResource]
        if nameCase.foldsCaseForCollisionAvoidance {
            let key = RemoteFileNaming.collisionKey(for: leafName)
            candidates = resourcesByCollisionKey[key] ?? []
        } else {
            candidates = resourcesByLeafName[leafName] ?? []
        }
        return candidates
            .filter { self.presenceMap.isUsableCandidate($0.physicalRemotePath) }
            .min { $0.physicalRemotePath < $1.physicalRemotePath }
    }

    /// Lex-min of present paths for `hash`; nil if no path's file is on remote.
    private func anyPresentPath(forHash hash: Data) -> String? {
        guard let paths = pathsByHash[hash], !paths.isEmpty else { return nil }
        return paths.lazy
            .filter { self.presenceMap.isUsableCandidate($0) }
            .min()
    }

    func existingFileNames() -> Set<String> {
        existingFileNameSet
    }

    func existingCollisionKeys() -> Set<String> {
        if let cache = collisionKeysCache { return cache }
        let built = RemoteFileNaming.collisionKeySet(from: existingFileNameSet)
        collisionKeysCache = built
        return built
    }

    func remoteFileSize(named logicalName: String) -> Int64? {
        remoteFilesByName[logicalName]?.size
    }

    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        let resources = Array(resourcesByPath.values)
        let assets = Array(assetsByFingerprint.values)
        let links = linksByFingerprint.values.flatMap { $0 }
        return (resources, assets, links)
    }

    func physicallyMissingHashesSnapshot() -> Set<Data> {
        presenceMap.fullyMissingHashes(pathsByHash: pathsByHash)
    }

    // MARK: - Write

    @discardableResult
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource {
        // If the same path is being repurposed to a different content hash, drop the
        // stale (oldHash → path) entry first; otherwise findResourceByHash(oldHash)
        // would still return this slot and serve up the new content under the wrong key.
        if let existing = resourcesByPath[resource.physicalRemotePath],
           existing.contentHash != resource.contentHash {
            let oldHash = existing.contentHash
            pathsByHash[oldHash]?.remove(resource.physicalRemotePath)
            if pathsByHash[oldHash]?.isEmpty == true {
                pathsByHash.removeValue(forKey: oldHash)
            }
        }
        if let existing = resourcesByPath[resource.physicalRemotePath] {
            removeNameIndexes(for: existing)
        }
        resourcesByPath[resource.physicalRemotePath] = resource
        pathsByHash[resource.contentHash, default: []].insert(resource.physicalRemotePath)
        addNameIndexes(for: resource)
        if !existingFileNameSet.contains(resource.logicalName) {
            existingFileNameSet.insert(resource.logicalName)
            collisionKeysCache?.insert(RemoteFileNaming.collisionKey(for: resource.logicalName))
        }
        presenceMap.mark(path: resource.physicalRemotePath, .hashVerified)
        return resource
    }

    private func addNameIndexes(for resource: RemoteManifestResource) {
        let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
        resourcesByLeafName[leaf, default: []].append(resource)
        resourcesByCollisionKey[RemoteFileNaming.collisionKey(for: leaf), default: []].append(resource)
    }

    private func removeNameIndexes(for resource: RemoteManifestResource) {
        let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
        if var bucket = resourcesByLeafName[leaf] {
            bucket.removeAll { $0.physicalRemotePath == resource.physicalRemotePath }
            if bucket.isEmpty {
                resourcesByLeafName.removeValue(forKey: leaf)
            } else {
                resourcesByLeafName[leaf] = bucket
            }
        }
        let collisionKey = RemoteFileNaming.collisionKey(for: leaf)
        guard var foldedBucket = resourcesByCollisionKey[collisionKey] else { return }
        foldedBucket.removeAll { $0.physicalRemotePath == resource.physicalRemotePath }
        if foldedBucket.isEmpty {
            resourcesByCollisionKey.removeValue(forKey: collisionKey)
        } else {
            resourcesByCollisionKey[collisionKey] = foldedBucket
        }
    }

    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<Data>
    ) throws {
        // Validate every link's resourceHash has a matching resource on file
        // AND its physical file is present — otherwise flush would emit a commit
        // body with empty resources[] and the snapshot covering that seq would
        // break `state == fold(covered)`.
        for link in links {
            guard anyPresentPath(forHash: link.resourceHash) != nil else {
                throw NSError(
                    domain: "V2MonthSession",
                    code: -11,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "backup.manifest.error.missingResourceHash")]
                )
            }
        }

        // Subset replacement — older partial assets that are strict subsets of this one
        // get tombstoned. Mirrors MonthManifestStore behavior for legacy import.
        for sub in replacingSubsetFingerprints where sub != asset.assetFingerprint {
            assetsByFingerprint.removeValue(forKey: sub)
            linksByFingerprint.removeValue(forKey: sub)
            pendingV2AssetFingerprints.remove(sub)
            pendingV2TombstoneFingerprints.insert(sub)
        }

        assetsByFingerprint[asset.assetFingerprint] = asset
        linksByFingerprint[asset.assetFingerprint] = links
        pendingV2AssetFingerprints.insert(asset.assetFingerprint)
        pendingV2TombstoneFingerprints.remove(asset.assetFingerprint)
        // Resurrect: mirrors RepoMaterializer's apply-addAsset gate so the snapshot
        // baseline doesn't carry both an asset row and its historical tombstone.
        deletedAssetStamps.removeValue(forKey: asset.assetFingerprint)
        legacyDeletedAssetFingerprints.remove(asset.assetFingerprint)
    }

    func markRemoteFile(name: String, size: Int64) {
        remoteFilesByName[name] = MonthManifestStore.RemoteFileMetadata(size: size)
        if !existingFileNameSet.contains(name) {
            existingFileNameSet.insert(name)
            collisionKeysCache?.insert(RemoteFileNaming.collisionKey(for: name))
        }
    }

    // MARK: - Flush coordination

    /// Look up a resource by hash for commit-op construction. Throws if the resource has
    /// been lost between upsert and flush — dropping a link would emit a commit body with
    /// fewer resources than in-memory and break the snapshot covered-range invariant.
    func resourceForCommitOp(hash: Data) throws -> RemoteManifestResource {
        guard let resource = findResourceByHash(hash) else {
            throw NSError(
                domain: "V2MonthSession",
                code: -13,
                userInfo: [NSLocalizedDescriptionKey:
                    "flush aborted: link hash \(hash.hexString) lost its resource between upsert and flush"]
            )
        }
        return resource
    }

    /// Pop pending fingerprints in deterministic order; caller stamps committed rows
    /// via `recordCommit(...)` after the commit log write succeeds.
    func snapshotPending() -> (assets: [Data], tombstones: [Data]) {
        let assets = pendingV2AssetFingerprints.sorted(by: { $0.lexicographicallyPrecedes($1) })
        let tombstones = pendingV2TombstoneFingerprints.sorted(by: { $0.lexicographicallyPrecedes($1) })
        return (assets, tombstones)
    }

    func asset(forFingerprint fp: Data) -> RemoteManifestAsset? {
        assetsByFingerprint[fp]
    }

    func links(forFingerprint fp: Data) -> [RemoteAssetResourceLink]? {
        linksByFingerprint[fp]
    }

    /// Stamps committed rows so the snapshot baseline matches what a future replay would derive
    /// (LWW gate). Clears pending sets — commit is durable, so a snapshot failure must not re-emit
    /// the same ops in the next flush. `committedResources` carries the full snapshot row for
    /// every path embedded in an `addAsset` body so the baseline survives a later same-path
    /// `upsertResource` overwrite. Rows must be built by the flusher with the same field
    /// projection RepoMaterializer uses on replay (asset body's creationDateMs/backedUpAtMs,
    /// resource entry's physicalRemotePath/contentHash/fileSize/resourceType/crypto).
    /// `committedResourceClocks` maps each committed path to the clock of the producing
    /// addAsset op; this method stamps each row with `OpStamp(writerID, seq, clock)` so the
    /// path-level LWW gate in `RepoMaterializer` can skip a stale cross-writer overwrite.
    func recordCommit(
        assetClocks: [Data: UInt64],
        tombstoneClocks: [Data: UInt64],
        committedResources: [String: SnapshotResourceRow],
        committedResourceClocks: [String: UInt64],
        writerID: String,
        seq: UInt64
    ) {
        for (fp, clock) in assetClocks {
            guard let asset = assetsByFingerprint[fp] else { continue }
            assetsByFingerprint[fp] = RemoteManifestAsset(
                year: asset.year,
                month: asset.month,
                assetFingerprint: asset.assetFingerprint,
                creationDateMs: asset.creationDateMs,
                backedUpAtMs: asset.backedUpAtMs,
                resourceCount: asset.resourceCount,
                totalFileSizeBytes: asset.totalFileSizeBytes,
                stamp: OpStamp(writerID: writerID, seq: seq, clock: clock)
            )
        }
        for (fp, clock) in tombstoneClocks {
            deletedAssetStamps[fp] = OpStamp(writerID: writerID, seq: seq, clock: clock)
            legacyDeletedAssetFingerprints.remove(fp)
        }
        for (path, row) in committedResources {
            let stamp = committedResourceClocks[path].map { OpStamp(writerID: writerID, seq: seq, clock: $0) }
            committedResourceByPath[path] = SnapshotResourceRow(
                physicalRemotePath: row.physicalRemotePath,
                contentHash: row.contentHash,
                fileSize: row.fileSize,
                resourceType: row.resourceType,
                creationDateMs: row.creationDateMs,
                backedUpAtMs: row.backedUpAtMs,
                crypto: row.crypto,
                stamp: stamp
            )
        }
        pendingV2AssetFingerprints.removeAll()
        pendingV2TombstoneFingerprints.removeAll()
    }

    /// Project our in-memory bookkeeping back into a `RepoMonthState` shape — the
    /// fold-of-covered-commits truth that `RepoSnapshotBuilder` requires. No
    /// listing-based filtering (that lives in the session-view layer).
    func currentMaterializedState() -> RepoMonthState {
        var state = RepoMonthState.empty
        for (fp, asset) in assetsByFingerprint {
            state.assets[fp] = SnapshotAssetRow(
                assetFingerprint: asset.assetFingerprint,
                creationDateMs: asset.creationDateMs,
                backedUpAtMs: asset.backedUpAtMs,
                resourceCount: asset.resourceCount,
                totalFileSizeBytes: asset.totalFileSizeBytes,
                stamp: asset.stamp
            )
        }
        // Snapshot resources are read from the committed map directly, NOT from
        // `resourcesByPath` — the live indexes may have been overwritten by an
        // in-session `upsertResource` for a path that was previously committed at
        // a different hash; using the committed row keeps the snapshot baseline
        // equal to `fold(commits in covered)`.
        for (path, row) in committedResourceByPath {
            state.resources[path] = row
        }
        for (fp, links) in linksByFingerprint {
            for link in links {
                let key = AssetResourceKey(assetFingerprint: fp, role: link.role, slot: link.slot)
                state.assetResources[key] = SnapshotAssetResourceRow(
                    assetFingerprint: fp,
                    role: link.role,
                    slot: link.slot,
                    resourceHash: link.resourceHash,
                    logicalName: link.logicalName
                )
            }
        }
        state.deletedAssetStamps = deletedAssetStamps
        state.deletedAssetFingerprints = legacyDeletedAssetFingerprints.union(deletedAssetStamps.keys)
        return state
    }
}

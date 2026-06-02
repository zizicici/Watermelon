import Foundation

final class V2MonthIndexes {
    struct StrictSubsetQueryStats: Equatable {
        let incomingKeyCount: Int
        let hashBucketLookups: Int
        let candidateCount: Int
        let predicateChecks: Int
        let stoppedAfterFirstMatch: Bool
    }

    private struct StrictSubsetQueryResult {
        let fingerprints: [AssetFingerprint]
        let hasMatch: Bool
        let stats: StrictSubsetQueryStats
    }

    let year: Int
    let month: Int

    private var resourcesByPath: [RemotePhysicalPathKey: RemoteManifestResource]
    private var assetsByFingerprint: [AssetFingerprint: RemoteManifestAsset]
    private var linksByFingerprint: [AssetFingerprint: [RemoteAssetResourceLink]]
    /// Derived from linksByFingerprint; future link-state mutators must use the index helpers.
    private var linkKeySetByFingerprint: [AssetFingerprint: Set<AssetResourceLinkKey>] = [:]
    private var fingerprintsByResourceHash: [Data: Set<AssetFingerprint>] = [:]
    /// `findResourceByHash` returns lex-min over present paths only; missing-path lookup would bind metadata to undownloadable bytes.
    /// Byte-exact keys so same-hash NFC/NFD twins don't fold a present path into a missing one (see `RemotePhysicalPathKey`).
    private var pathsByHash: [Data: Set<RemotePhysicalPathKey>]
    /// Reverse name index keeps upload preparation from scanning every resource in a month.
    /// Keyed by leaf name on case-sensitive backends, collision key on case-folding backends.
    private var resourcesByNameKey: [String: [RemoteManifestResource]] = [:]
    private var collisionKeysCache: Set<String>?
    /// Missing/inconclusive paths are excluded from find ops, not snapshot emission.
    private var presenceMap: RemoteMonthPresenceMap

    private var remoteFilesByName: [String: MonthManifestStore.RemoteFileMetadata]
    private var existingFileNameSet: Set<String>

    /// Mirror of `RepoMonthState.deletedAssetStamps`; survives flushes so snapshot emits deletedKey rows.
    private(set) var deletedAssetStamps: [AssetFingerprint: OpStamp]

    /// Snapshot emission reads committed rows so live same-path overwrites cannot enter the covered baseline.
    private var committedResourceByPath: [RemotePhysicalPathKey: SnapshotResourceRow]

    private(set) var pendingV2AssetFingerprints: Set<AssetFingerprint> = []
    private(set) var pendingV2TombstoneFingerprints: Set<AssetFingerprint> = []

    private let nameCase: BackendNameCaseSensitivity

    var hasUncommittedOps: Bool {
        !pendingV2AssetFingerprints.isEmpty || !pendingV2TombstoneFingerprints.isEmpty
    }

    var pendingOpsCount: Int {
        pendingV2AssetFingerprints.count + pendingV2TombstoneFingerprints.count
    }

    var hasAnyAsset: Bool { !assetsByFingerprint.isEmpty }

    init(
        year: Int,
        month: Int,
        materializedState: RepoMonthState,
        remoteFilesByName: [String: MonthManifestStore.RemoteFileMetadata],
        listedSizesByPresenceKey: [String: Set<Int64>]? = nil,
        verifiedMissingHashes: Set<Data>?,
        nameCase: BackendNameCaseSensitivity
    ) {
        self.year = year
        self.month = month
        self.nameCase = nameCase
        self.remoteFilesByName = remoteFilesByName
        self.existingFileNameSet = Set(remoteFilesByName.keys)

        // Faithful projection; filtering here would leak into snapshot writes and break the covered-range invariant.
        var resourcesByPath: [RemotePhysicalPathKey: RemoteManifestResource] = [:]
        var pathsByHash: [Data: Set<RemotePhysicalPathKey>] = [:]
        var resourcesByNameKey: [String: [RemoteManifestResource]] = [:]
        var presenceMap = RemoteMonthPresenceMap()
        // `remoteFilesByName` ([String:…]) folds NFC/NFD twins; loadOrCreate passes a byte-exact map
        // built from the raw listing so a present twin isn't computed `.missing`. Derive only when absent.
        let sizesByPresenceKey: [String: Set<Int64>]
        if let listedSizesByPresenceKey {
            sizesByPresenceKey = listedSizesByPresenceKey
        } else {
            var derived: [String: Set<Int64>] = [:]
            for (name, meta) in remoteFilesByName {
                derived[nameCase.presenceKey(for: name), default: []].insert(meta.size)
            }
            sizesByPresenceKey = derived
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
                // Treat listed size matches as usable or no-probe startup re-uploads every resource.
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
            resourcesByPath[RemotePhysicalPathKey(row.physicalRemotePath)] = resource
            pathsByHash[row.contentHash, default: []].insert(RemotePhysicalPathKey(row.physicalRemotePath))
            let leaf = (row.physicalRemotePath as NSString).lastPathComponent
            let nameKey = nameCase.foldsCaseForCollisionAvoidance
                ? RemoteFileNaming.collisionKey(for: leaf)
                : leaf
            resourcesByNameKey[nameKey, default: []].append(resource)
        }
        self.resourcesByPath = resourcesByPath
        self.pathsByHash = pathsByHash
        self.resourcesByNameKey = resourcesByNameKey
        self.presenceMap = presenceMap

        var assetsByFingerprint: [AssetFingerprint: RemoteManifestAsset] = [:]
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

        var linksByFingerprint: [AssetFingerprint: [RemoteAssetResourceLink]] = [:]
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
        // Seed committed rows faithfully so post-tombstone orphan resources survive covered snapshots.
        self.committedResourceByPath = materializedState.resources
        rebuildLinkIndexes()
    }

    func containsAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool {
        assetsByFingerprint[fingerprint] != nil
    }

    /// Mirrors `MonthManifestStore.findStrictSubsetAssetFingerprints` so upload paths
    /// can tombstone older partial assets that the incoming bundle supersedes.
    func findStrictSubsetAssetFingerprints(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> [AssetFingerprint] {
        queryStrictSubsets(forResourceKeys: keys, stopAfterFirstMatch: false).fingerprints
    }

    func hasStrictSubsetAssetFingerprint(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> Bool {
        queryStrictSubsets(forResourceKeys: keys, stopAfterFirstMatch: true).hasMatch
    }

    func strictSubsetQueryStatsForTesting(
        forResourceKeys keys: Set<AssetResourceLinkKey>,
        stopAfterFirstMatch: Bool = false
    ) -> StrictSubsetQueryStats {
        queryStrictSubsets(forResourceKeys: keys, stopAfterFirstMatch: stopAfterFirstMatch).stats
    }

    private func queryStrictSubsets(
        forResourceKeys keys: Set<AssetResourceLinkKey>,
        stopAfterFirstMatch: Bool
    ) -> StrictSubsetQueryResult {
        guard !keys.isEmpty else {
            return StrictSubsetQueryResult(
                fingerprints: [],
                hasMatch: false,
                stats: StrictSubsetQueryStats(
                    incomingKeyCount: 0,
                    hashBucketLookups: 0,
                    candidateCount: 0,
                    predicateChecks: 0,
                    stoppedAfterFirstMatch: false
                )
            )
        }

        var candidates: Set<AssetFingerprint> = []
        for key in keys {
            if let bucket = fingerprintsByResourceHash[key.hash] {
                candidates.formUnion(bucket)
            }
        }

        var result: [AssetFingerprint] = []
        var predicateChecks = 0
        var stoppedAfterFirstMatch = false
        for fingerprint in candidates {
            guard let have = linkKeySetByFingerprint[fingerprint] else { continue }
            predicateChecks += 1
            guard AssetResourceLinkSetPredicate.isStrictSubset(have, of: keys) else { continue }
            result.append(fingerprint)
            if stopAfterFirstMatch {
                stoppedAfterFirstMatch = true
                break
            }
        }

        return StrictSubsetQueryResult(
            fingerprints: result,
            hasMatch: !result.isEmpty,
            stats: StrictSubsetQueryStats(
                incomingKeyCount: keys.count,
                hashBucketLookups: keys.count,
                candidateCount: candidates.count,
                predicateChecks: predicateChecks,
                stoppedAfterFirstMatch: stoppedAfterFirstMatch
            )
        )
    }

    private func rebuildLinkIndexes() {
        linkKeySetByFingerprint.removeAll(keepingCapacity: true)
        fingerprintsByResourceHash.removeAll(keepingCapacity: true)
        linkKeySetByFingerprint.reserveCapacity(linksByFingerprint.count)
        for (fingerprint, links) in linksByFingerprint {
            indexAddAsset(fingerprint: fingerprint, links: links)
        }
    }

    private func indexAddAsset(fingerprint: AssetFingerprint, links: [RemoteAssetResourceLink]) {
        linkKeySetByFingerprint[fingerprint] = AssetResourceLinkSetPredicate.keys(fromLinks: links)
        for link in links {
            fingerprintsByResourceHash[link.resourceHash, default: []].insert(fingerprint)
        }
    }

    private func indexRemoveAsset(_ fingerprint: AssetFingerprint) {
        if let oldLinks = linksByFingerprint[fingerprint] {
            for link in oldLinks {
                fingerprintsByResourceHash[link.resourceHash]?.remove(fingerprint)
                if fingerprintsByResourceHash[link.resourceHash]?.isEmpty == true {
                    fingerprintsByResourceHash.removeValue(forKey: link.resourceHash)
                }
            }
        }
        linkKeySetByFingerprint.removeValue(forKey: fingerprint)
    }

    func isAssetIncomplete(_ fingerprint: AssetFingerprint) -> Bool {
        guard let asset = assetsByFingerprint[fingerprint] else { return false }
        let links = linksByFingerprint[fingerprint] ?? []
        // Filter gates actionability only; materialized state stays faithful to the commit log.
        return RemoteAssetIntegrityClassifier.isIncomplete(
            assetFingerprint: asset.assetFingerprint,
            links: links,
            isResourceAvailable: { hash in
                self.anyPresentPath(forHash: hash) != nil
            }
        )
    }

    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource? {
        // Lex-min over all paths would let a missing path shadow a present one and bind metadata to undownloadable bytes.
        guard let chosen = anyPresentPath(forHash: contentHash) else { return nil }
        return resourcesByPath[RemotePhysicalPathKey(chosen)]
    }

    func findByFileName(_ logicalName: String) -> RemoteManifestResource? {
        let leafName = logicalName
            .split(separator: "/", omittingEmptySubsequences: true)
            .last
            .map(String.init) ?? logicalName
        let nameKey = nameCase.foldsCaseForCollisionAvoidance
            ? RemoteFileNaming.collisionKey(for: leafName)
            : leafName
        let candidates = resourcesByNameKey[nameKey] ?? []
        return candidates
            .filter { self.presenceMap.isUsableCandidate($0.physicalRemotePath) }
            .min { $0.physicalRemotePath < $1.physicalRemotePath }
    }

    /// Lex-min of present paths for `hash`; nil if no path's file is on remote.
    private func anyPresentPath(forHash hash: Data) -> String? {
        guard let paths = pathsByHash[hash], !paths.isEmpty else { return nil }
        return paths.lazy
            .map(\.path)
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

    @discardableResult
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource {
        let pathKey = RemotePhysicalPathKey(resource.physicalRemotePath)
        // Drop the old hash mapping before repurposing a path, or lookups can return wrong bytes.
        if let existing = resourcesByPath[pathKey],
           existing.contentHash != resource.contentHash {
            let oldHash = existing.contentHash
            pathsByHash[oldHash]?.remove(RemotePhysicalPathKey(resource.physicalRemotePath))
            if pathsByHash[oldHash]?.isEmpty == true {
                pathsByHash.removeValue(forKey: oldHash)
            }
        }
        if let existing = resourcesByPath[pathKey] {
            removeNameIndexes(for: existing)
        }
        resourcesByPath[pathKey] = resource
        pathsByHash[resource.contentHash, default: []].insert(RemotePhysicalPathKey(resource.physicalRemotePath))
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
        let nameKey = nameCase.foldsCaseForCollisionAvoidance
            ? RemoteFileNaming.collisionKey(for: leaf)
            : leaf
        resourcesByNameKey[nameKey, default: []].append(resource)
    }

    private func removeNameIndexes(for resource: RemoteManifestResource) {
        let leaf = (resource.physicalRemotePath as NSString).lastPathComponent
        let nameKey = nameCase.foldsCaseForCollisionAvoidance
            ? RemoteFileNaming.collisionKey(for: leaf)
            : leaf
        guard var bucket = resourcesByNameKey[nameKey] else { return }
        bucket.removeAll { $0.physicalRemotePath == resource.physicalRemotePath }
        if bucket.isEmpty {
            resourcesByNameKey.removeValue(forKey: nameKey)
        } else {
            resourcesByNameKey[nameKey] = bucket
        }
    }

    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<AssetFingerprint>
    ) throws {
        // Reject links without present resources or flush would emit an asset body missing its resources.
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
            indexRemoveAsset(sub)
            assetsByFingerprint.removeValue(forKey: sub)
            linksByFingerprint.removeValue(forKey: sub)
            pendingV2AssetFingerprints.remove(sub)
            pendingV2TombstoneFingerprints.insert(sub)
        }

        indexRemoveAsset(asset.assetFingerprint)
        assetsByFingerprint[asset.assetFingerprint] = asset
        linksByFingerprint[asset.assetFingerprint] = links
        indexAddAsset(fingerprint: asset.assetFingerprint, links: links)
        pendingV2AssetFingerprints.insert(asset.assetFingerprint)
        pendingV2TombstoneFingerprints.remove(asset.assetFingerprint)
        // Resurrect: mirrors RepoMaterializer's apply-addAsset gate so the snapshot
        // baseline doesn't carry both an asset row and its historical tombstone.
        deletedAssetStamps.removeValue(forKey: asset.assetFingerprint)
    }

    func markRemoteFile(name: String, size: Int64) {
        remoteFilesByName[name] = MonthManifestStore.RemoteFileMetadata(size: size)
        if !existingFileNameSet.contains(name) {
            existingFileNameSet.insert(name)
            collisionKeysCache?.insert(RemoteFileNaming.collisionKey(for: name))
        }
    }


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
    /// via `recordCommit(...)` after the commit log write succeeds. `limit` caps the
    /// total ops (assets + tombstones) returned, draining assets first then tombstones.
    /// A non-nil `limit` enables the U01 hard-cap chunked-flush contract.
    func snapshotPending(limit: Int? = nil) -> (assets: [AssetFingerprint], tombstones: [AssetFingerprint]) {
        let assets = pendingV2AssetFingerprints.sorted(by: { $0.rawValue.lexicographicallyPrecedes($1.rawValue) })
        let tombstones = pendingV2TombstoneFingerprints.sorted(by: { $0.rawValue.lexicographicallyPrecedes($1.rawValue) })
        guard let limit, limit >= 0 else {
            return (assets, tombstones)
        }
        if assets.count >= limit {
            return (Array(assets.prefix(limit)), [])
        }
        let remaining = limit - assets.count
        return (assets, Array(tombstones.prefix(remaining)))
    }

    func asset(forFingerprint fp: AssetFingerprint) -> RemoteManifestAsset? {
        assetsByFingerprint[fp]
    }

    func links(forFingerprint fp: AssetFingerprint) -> [RemoteAssetResourceLink]? {
        linksByFingerprint[fp]
    }

    /// Stamps committed rows so snapshot baselines match replay and stale path overwrites lose LWW.
    func recordCommit(
        assetClocks: [AssetFingerprint: UInt64],
        tombstoneClocks: [AssetFingerprint: UInt64],
        committedResources: [RemotePhysicalPathKey: RemoteManifestResource],
        committedResourceClocks: [RemotePhysicalPathKey: UInt64],
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
        }
        for (path, row) in committedResources {
            guard let clock = committedResourceClocks[path] else { continue }
            let stamp = OpStamp(writerID: writerID, seq: seq, clock: clock)
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
        // Remove only the fingerprints this commit actually stamped — chunked flushes
        // (U01 hard cap) write the remainder in subsequent commit files; `removeAll()`
        // here would silently drop them from the pending set in memory.
        for fp in assetClocks.keys {
            pendingV2AssetFingerprints.remove(fp)
        }
        for fp in tombstoneClocks.keys {
            pendingV2TombstoneFingerprints.remove(fp)
        }
    }

    /// Snapshot state must remain unfiltered so covered ranges equal replayed commits.
    func currentMaterializedState() -> RepoMonthState {
        var state = RepoMonthState.empty
        for (fp, asset) in assetsByFingerprint {
            guard let stamp = asset.stamp else {
                preconditionFailure("currentMaterializedState requires committed asset stamp for \(asset.assetFingerprint)")
            }
            state.assets[fp] = SnapshotAssetRow(
                assetFingerprint: asset.assetFingerprint,
                creationDateMs: asset.creationDateMs,
                backedUpAtMs: asset.backedUpAtMs,
                resourceCount: asset.resourceCount,
                totalFileSizeBytes: asset.totalFileSizeBytes,
                stamp: stamp
            )
        }
        // Use committed rows, not live resources, to keep snapshots equal to fold(commits in covered).
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
        return state
    }
}

import Foundation

/// Path/hash/name availability projection for a single V2 month: the live resource working set
/// keyed by `RemotePhysicalPathKey`, the `pathsByHash` reverse index, the reverse name index, and
/// the `RemoteMonthPresenceMap`. Owns the byte-exact NFC/NFD behavior — find ops resolve over
/// present paths only so metadata never binds to undownloadable bytes.
final class MonthPresenceProjection {
    let year: Int
    let month: Int
    private let nameCase: BackendNameCaseSensitivity

    private var resourcesByPath: [RemotePhysicalPathKey: RemoteManifestResource]
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
            // Reserve every materialized resource's name as occupied, not just physically-listed ones: a
            // referenced resource whose file is missing/unlisted still owns its path, so a different-content
            // upload that reused the name would repurpose that path and fold the month to a dangling-link
            // `.corrupt` on next materialize. Mirrors V1's resourcesByName ∪ remoteFilesByName occupancy.
            existingFileNameSet.insert(logicalName)
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

    /// nil iff no path for `hash` is on remote; otherwise the lex-min present path.
    func anyPresentPath(forHash hash: Data) -> String? {
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

    /// Live resource working set; snapshot emission stays unfiltered so covered ranges equal replayed commits.
    func allResources() -> [RemoteManifestResource] {
        Array(resourcesByPath.values)
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
}

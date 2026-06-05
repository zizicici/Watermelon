import Foundation

/// Coordinator over the focused month authorities — it owns no row maps itself. Splits the former
/// god object into `RepoMonthCommittedState` (durable rows + snapshot conversion),
/// `MonthPresenceProjection` (path/hash/name availability + live resource working set),
/// `PendingCommitBuffer` (in-session pending ops), and `SnapshotProjection` (export). Cross-cutting
/// operations (`upsertAsset`, `recordCommit`, `isAssetIncomplete`) coordinate the authorities here.
final class V2MonthIndexes {
    typealias StrictSubsetQueryStats = RepoMonthCommittedState.StrictSubsetQueryStats

    let year: Int
    let month: Int

    let committed: RepoMonthCommittedState
    let presence: MonthPresenceProjection
    let pending: PendingCommitBuffer

    var hasUncommittedOps: Bool { pending.hasUncommittedOps }
    var pendingOpsCount: Int { pending.pendingOpsCount }
    var hasAnyAsset: Bool { committed.hasAnyAsset }

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
        self.committed = RepoMonthCommittedState(
            year: year,
            month: month,
            materializedState: materializedState
        )
        self.presence = MonthPresenceProjection(
            year: year,
            month: month,
            materializedState: materializedState,
            remoteFilesByName: remoteFilesByName,
            listedSizesByPresenceKey: listedSizesByPresenceKey,
            verifiedMissingHashes: verifiedMissingHashes,
            nameCase: nameCase
        )
        self.pending = PendingCommitBuffer()
    }

    // MARK: - Committed-row queries

    func containsAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool {
        committed.containsAssetFingerprint(fingerprint)
    }

    func containsPendingAssetFingerprint(_ fingerprint: AssetFingerprint) -> Bool {
        pending.containsAssetAdd(fingerprint)
    }

    func findStrictSubsetAssetFingerprints(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> [AssetFingerprint] {
        committed.findStrictSubsetAssetFingerprints(forResourceKeys: keys)
    }

    func hasStrictSubsetAssetFingerprint(
        forResourceKeys keys: Set<AssetResourceLinkKey>
    ) -> Bool {
        committed.hasStrictSubsetAssetFingerprint(forResourceKeys: keys)
    }

    func strictSubsetQueryStatsForTesting(
        forResourceKeys keys: Set<AssetResourceLinkKey>,
        stopAfterFirstMatch: Bool = false
    ) -> StrictSubsetQueryStats {
        committed.strictSubsetQueryStatsForTesting(forResourceKeys: keys, stopAfterFirstMatch: stopAfterFirstMatch)
    }

    func isAssetIncomplete(_ fingerprint: AssetFingerprint) -> Bool {
        committed.isAssetIncomplete(fingerprint) { hash in
            self.presence.anyPresentPath(forHash: hash) != nil
        }
    }

    // MARK: - Presence / path projection

    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource? {
        presence.findResourceByHash(contentHash)
    }

    func findByFileName(_ logicalName: String) -> RemoteManifestResource? {
        presence.findByFileName(logicalName)
    }

    func existingFileNames() -> Set<String> {
        presence.existingFileNames()
    }

    func existingCollisionKeys() -> Set<String> {
        presence.existingCollisionKeys()
    }

    func remoteFileSize(named logicalName: String) -> Int64? {
        presence.remoteFileSize(named: logicalName)
    }

    func physicallyMissingHashesSnapshot() -> Set<Data> {
        presence.physicallyMissingHashesSnapshot()
    }

    @discardableResult
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource {
        try presence.upsertResource(resource)
    }

    func markRemoteFile(name: String, size: Int64) {
        presence.markRemoteFile(name: name, size: size)
    }

    // MARK: - Snapshot export

    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        SnapshotProjection.unsortedSnapshot(committed: committed, presence: presence)
    }

    func currentMaterializedState() -> RepoMonthState {
        committed.currentMaterializedState()
    }

    // MARK: - Cross-cutting mutations

    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<AssetFingerprint>
    ) throws {
        // Reject links without present resources or flush would emit an asset body missing its resources.
        for link in links {
            guard presence.anyPresentPath(forHash: link.resourceHash) != nil else {
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
            committed.removeAsset(sub)
            pending.removeAssetAdd(sub)
            pending.insertTombstone(sub)
        }

        committed.putAsset(asset, links: links)
        pending.insertAssetAdd(asset.assetFingerprint)
        pending.removeTombstone(asset.assetFingerprint)
    }

    /// Stamps committed rows (LWW baseline) then drops only the stamped fingerprints from the pending
    /// buffer — chunked flushes write the remainder in subsequent commit files.
    func recordCommit(
        assetClocks: [AssetFingerprint: UInt64],
        tombstoneClocks: [AssetFingerprint: UInt64],
        committedResources: [RemotePhysicalPathKey: RemoteManifestResource],
        committedResourceClocks: [RemotePhysicalPathKey: UInt64],
        writerID: String,
        seq: UInt64
    ) {
        committed.recordCommit(
            assetClocks: assetClocks,
            tombstoneClocks: tombstoneClocks,
            committedResources: committedResources,
            committedResourceClocks: committedResourceClocks,
            writerID: writerID,
            seq: seq
        )
        pending.removeCommitted(assets: Set(assetClocks.keys), tombstones: Set(tombstoneClocks.keys))
    }
}

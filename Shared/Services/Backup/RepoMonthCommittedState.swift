import Foundation

/// Durable/materialized month row authority: asset rows, asset-resource links (plus the derived
/// link/hash indexes that keep strict-subset queries off the O(N²) path), deleted asset stamps, and
/// the committed resource rows that form the snapshot baseline. Converts to `RepoMonthState` for
/// snapshot emission. Holds no pending-op or presence state.
final class RepoMonthCommittedState {
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

    private var assetsByFingerprint: [AssetFingerprint: RemoteManifestAsset]
    private var linksByFingerprint: [AssetFingerprint: [RemoteAssetResourceLink]]
    /// Derived from linksByFingerprint; future link-state mutators must use the index helpers.
    private var linkKeySetByFingerprint: [AssetFingerprint: Set<AssetResourceLinkKey>] = [:]
    private var fingerprintsByResourceHash: [Data: Set<AssetFingerprint>] = [:]

    /// Mirror of `RepoMonthState.deletedAssetStamps`; survives flushes so snapshot emits deletedKey rows.
    private var deletedAssetStamps: [AssetFingerprint: OpStamp]

    /// Snapshot emission reads committed rows so live same-path overwrites cannot enter the covered baseline.
    private var committedResourceByPath: [RemotePhysicalPathKey: SnapshotResourceRow]

    var hasAnyAsset: Bool { !assetsByFingerprint.isEmpty }

    init(
        year: Int,
        month: Int,
        materializedState: RepoMonthState
    ) {
        self.year = year
        self.month = month

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

    func asset(forFingerprint fp: AssetFingerprint) -> RemoteManifestAsset? {
        assetsByFingerprint[fp]
    }

    func links(forFingerprint fp: AssetFingerprint) -> [RemoteAssetResourceLink]? {
        linksByFingerprint[fp]
    }

    func allAssets() -> [RemoteManifestAsset] {
        Array(assetsByFingerprint.values)
    }

    func allLinks() -> [RemoteAssetResourceLink] {
        linksByFingerprint.values.flatMap { $0 }
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

    /// `isResourceAvailable` is supplied by the presence projection so storage layout cannot skew classification.
    func isAssetIncomplete(
        _ fingerprint: AssetFingerprint,
        isResourceAvailable: (Data) -> Bool
    ) -> Bool {
        guard let asset = assetsByFingerprint[fingerprint] else { return false }
        let links = linksByFingerprint[fingerprint] ?? []
        // Filter gates actionability only; materialized state stays faithful to the commit log.
        return RemoteAssetIntegrityClassifier.isIncomplete(
            assetFingerprint: asset.assetFingerprint,
            links: links,
            isResourceAvailable: isResourceAvailable
        )
    }

    /// Add/replace an asset row + its links, dropping any historical tombstone for the fingerprint.
    /// Resurrect mirrors `RepoMaterializer`'s apply-addAsset gate so the snapshot baseline doesn't
    /// carry both an asset row and its tombstone. Pending-op bookkeeping belongs to the caller.
    func putAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]) {
        indexRemoveAsset(asset.assetFingerprint)
        assetsByFingerprint[asset.assetFingerprint] = asset
        linksByFingerprint[asset.assetFingerprint] = links
        indexAddAsset(fingerprint: asset.assetFingerprint, links: links)
        deletedAssetStamps.removeValue(forKey: asset.assetFingerprint)
    }

    /// Drop an asset row + its links (subset-replacement). The deleted stamp is set later at commit
    /// time via `recordCommit`'s tombstone clocks, so this does not touch `deletedAssetStamps`.
    func removeAsset(_ fingerprint: AssetFingerprint) {
        indexRemoveAsset(fingerprint)
        assetsByFingerprint.removeValue(forKey: fingerprint)
        linksByFingerprint.removeValue(forKey: fingerprint)
    }

    /// Stamps committed rows so snapshot baselines match replay and stale path overwrites lose LWW.
    /// Pending-op removal is the caller's responsibility (chunked flushes stamp only their chunk).
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

import Foundation

/// Fail-closed proof that an accepted snapshot body actually retains the metadata a destructive
/// maintenance pass is about to delete. Covered ranges alone are not authority: a parseable but
/// under-representing baseline (header covers seqs whose rows the body omits) would otherwise let
/// commit GC / snapshot GC delete the last faithful copy. `makeBaseline` only proves structural
/// consistency (no dangling link, ≥1 link per asset) — NOT completeness — so a body can keep an asset
/// fingerprint/stamp yet omit one of its resources/links, or keep the link's hash while mutating the
/// asset row's projected metadata or the backing resource row (path/size); retention is therefore proven
/// down to the per-(role,slot) link, the same-op asset-row metadata, AND each backing resource row the
/// deleted body carries. Stamp comparison reuses `opStampPrecedes`, the materializer's own LWW order.
enum RepoBodyRetention {
    /// `accepted` retains a live asset (`fingerprint`@`stamp`) carrying `links` iff it tombstones the
    /// fingerprint at a non-earlier stamp (asset and all its resources legitimately gone), or holds the
    /// asset at a non-earlier stamp AND carries every (role, slot) link with the same content hash —
    /// plus, when the accepted asset stamp EQUALS the deleted body's stamp, the same projected asset-row
    /// metadata (`creationDateMs`/`backedUpAtMs`/`resourceCount`/`totalFileSizeBytes`) and the same
    /// `logicalName`. A same-stamp accepted row is the SAME op, so any differing same-op field means the
    /// accepted body mutated metadata restore/index consume; a strictly-later add legitimately supersedes
    /// it (same fingerprint ⇒ same role/slot/hash, but projected metadata may change under LWW).
    static func retainsLiveAsset(
        in accepted: RepoMonthState,
        fingerprint: AssetFingerprint,
        stamp: OpStamp,
        creationDateMs: Int64?,
        backedUpAtMs: Int64,
        resourceCount: Int,
        totalFileSizeBytes: Int64,
        links: [(role: Int, slot: Int, hash: Data, logicalName: String)]
    ) -> Bool {
        if let deleted = accepted.deletedAssetStamps[fingerprint], !opStampPrecedes(deleted, stamp) { return true }
        guard let acceptedAsset = accepted.assets[fingerprint], !opStampPrecedes(acceptedAsset.stamp, stamp) else { return false }
        let sameStamp = !opStampPrecedes(stamp, acceptedAsset.stamp)
        if sameStamp {
            if acceptedAsset.creationDateMs != creationDateMs { return false }
            if acceptedAsset.backedUpAtMs != backedUpAtMs { return false }
            if acceptedAsset.resourceCount != resourceCount { return false }
            if acceptedAsset.totalFileSizeBytes != totalFileSizeBytes { return false }
        }
        for link in links {
            let key = AssetResourceKey(assetFingerprint: fingerprint, role: link.role, slot: link.slot)
            guard let acceptedLink = accepted.assetResources[key], acceptedLink.resourceHash == link.hash else {
                return false
            }
            if sameStamp, acceptedLink.logicalName != link.logicalName { return false }
        }
        return true
    }

    /// `accepted` retains a tombstone(`fingerprint`@`stamp`) iff it holds that tombstone at a stamp no
    /// earlier (the deletion survived), or a strictly-later live add (a heal superseded the tombstone).
    static func retainsTombstone(in accepted: RepoMonthState, fingerprint: AssetFingerprint, stamp: OpStamp) -> Bool {
        if let deleted = accepted.deletedAssetStamps[fingerprint], !opStampPrecedes(deleted, stamp) { return true }
        if let live = accepted.assets[fingerprint]?.stamp, opStampPrecedes(stamp, live) { return true }
        return false
    }

    /// `accepted` retains a resource row a deleted body carries at `path` iff it holds a resource row at
    /// the same path with the same content hash AND a non-earlier stamp, and — when that row's stamp
    /// EQUALS the deleted row's stamp (the SAME op) — the same size/type/crypto and the same
    /// creation/backed-up timestamps. A missing path, hash mismatch, or a STRICTLY-OLDER accepted row
    /// (path-keyed resource rows are LWW, so an older row is neither the same op nor a later supersession
    /// of the deleted row) means the faithful resource metadata (the path/size restore consumes and the
    /// `backedUpAtMs` freshness `RepoVerifyMonthService` consumes to gate tombstoning) is not retained;
    /// a strictly-later accepted row legitimately supersedes it under LWW.
    static func retainsResourceRow(
        in accepted: RepoMonthState,
        path: String,
        contentHash: Data,
        fileSize: Int64,
        resourceType: Int,
        crypto: ResourceCryptoMetadata?,
        creationDateMs: Int64?,
        backedUpAtMs: Int64,
        stamp: OpStamp
    ) -> Bool {
        guard let row = accepted.resources[RemotePhysicalPathKey(path)], row.contentHash == contentHash else {
            return false
        }
        guard !opStampPrecedes(row.stamp, stamp) else { return false }
        let sameStamp = !opStampPrecedes(stamp, row.stamp)
        if sameStamp {
            if row.fileSize != fileSize { return false }
            if row.resourceType != resourceType { return false }
            if row.crypto != crypto { return false }
            if row.creationDateMs != creationDateMs { return false }
            if row.backedUpAtMs != backedUpAtMs { return false }
        }
        return true
    }

    /// True iff `accepted` retains every op the commit carries — including each addAsset's full
    /// per-(role,slot) resource link set AND each backing resource row (commit-GC delete barrier).
    static func retainsCommit(_ commit: CommitFile, in accepted: RepoMonthState) -> Bool {
        for op in commit.ops {
            let stamp = OpStamp(writerID: commit.header.writerID, seq: commit.header.seq, clock: op.clock)
            switch op.body {
            case .addAsset(let body):
                let links = body.resources.map { (role: $0.role, slot: $0.slot, hash: $0.contentHash, logicalName: $0.logicalName) }
                if !retainsLiveAsset(
                    in: accepted,
                    fingerprint: body.assetFingerprint,
                    stamp: stamp,
                    creationDateMs: body.creationDateMs,
                    backedUpAtMs: body.backedUpAtMs,
                    resourceCount: body.resources.count,
                    totalFileSizeBytes: body.resources.reduce(Int64(0)) { $0 + $1.fileSize },
                    links: links
                ) {
                    return false
                }
                for resource in body.resources {
                    if !retainsResourceRow(
                        in: accepted,
                        path: resource.physicalRemotePath,
                        contentHash: resource.contentHash,
                        fileSize: resource.fileSize,
                        resourceType: resource.resourceType,
                        crypto: resource.crypto,
                        creationDateMs: body.creationDateMs,
                        backedUpAtMs: body.backedUpAtMs,
                        stamp: stamp
                    ) {
                        return false
                    }
                }
            case .tombstoneAsset(let body):
                if !retainsTombstone(in: accepted, fingerprint: body.assetFingerprint, stamp: stamp) { return false }
            }
        }
        return true
    }

    /// True iff `accepted` retains every row a to-be-deleted snapshot body carries — including each
    /// asset's full link set AND each backing resource row (snapshot-GC delete barrier). An unparseable
    /// asset tombstone key fails closed.
    static func retainsSnapshotBody(_ file: SnapshotFile, in accepted: RepoMonthState) -> Bool {
        var linksByFingerprint: [AssetFingerprint: [(role: Int, slot: Int, hash: Data, logicalName: String)]] = [:]
        for link in file.assetResources {
            linksByFingerprint[link.assetFingerprint, default: []].append((role: link.role, slot: link.slot, hash: link.resourceHash, logicalName: link.logicalName))
        }
        for asset in file.assets {
            let links = linksByFingerprint[asset.assetFingerprint] ?? []
            if !retainsLiveAsset(
                in: accepted,
                fingerprint: asset.assetFingerprint,
                stamp: asset.stamp,
                creationDateMs: asset.creationDateMs,
                backedUpAtMs: asset.backedUpAtMs,
                resourceCount: asset.resourceCount,
                totalFileSizeBytes: asset.totalFileSizeBytes,
                links: links
            ) {
                return false
            }
        }
        for resource in file.resources {
            if !retainsResourceRow(
                in: accepted,
                path: resource.physicalRemotePath,
                contentHash: resource.contentHash,
                fileSize: resource.fileSize,
                resourceType: resource.resourceType,
                crypto: resource.crypto,
                creationDateMs: resource.creationDateMs,
                backedUpAtMs: resource.backedUpAtMs,
                stamp: resource.stamp
            ) {
                return false
            }
        }
        for deletedKey in file.deletedKeys where deletedKey.keyType == .asset {
            guard let fp = try? RepoWireValidator.validateAssetFingerprint(deletedKey.keyValue, field: "keyValue") else {
                return false
            }
            if !retainsTombstone(in: accepted, fingerprint: fp, stamp: deletedKey.stamp) { return false }
        }
        return true
    }
}

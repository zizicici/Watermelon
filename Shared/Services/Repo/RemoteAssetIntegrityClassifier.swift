import Foundation

/// Single source of truth for "what's wrong with this asset". Verify, download
/// eligibility, Home album matching, manifest health all consume this — keeps
/// per-callsite predicates from drifting (which is exactly how Round 11 #8
/// happened: HomeAlbumMatching's narrower predicate let metadata-only assets
/// through to restore).
enum AssetIntegrityState: Sendable, Equatable {
    case healthy
    case partiallyMissing(missingHashes: [Data])
    case metadataOnlyLeft
    case fullyMissing
    case phantom
    case fingerprintMismatch(recomputed: Data)

    /// The asset can be downloaded into Photos and yield a viewable result.
    /// `partiallyMissing` is allowed only when at least one non-metadata role survives;
    /// `metadataOnlyLeft` is unrestorable by definition (just edit history).
    var allowsRestore: Bool {
        switch self {
        case .healthy, .partiallyMissing: return true
        case .metadataOnlyLeft, .fullyMissing, .phantom, .fingerprintMismatch: return false
        }
    }

    /// Verify-driven cleanup may safely tombstone these — no recoverable content remains.
    /// `partiallyMissing` and `fingerprintMismatch` are explicitly EXCLUDED: a partial
    /// loss may be transient (network blip during init listing) and a mismatch may be
    /// tampering rather than data loss; auto-tombstoning either would destroy a
    /// recoverable asset.
    var allowsCleanup: Bool {
        switch self {
        case .phantom, .fullyMissing, .metadataOnlyLeft: return true
        case .healthy, .partiallyMissing, .fingerprintMismatch: return false
        }
    }

    var isHealthy: Bool {
        if case .healthy = self { return true }
        return false
    }
}

/// Tuple shape the classifier reads. Both `RemoteAssetResourceLink` and
/// `SnapshotAssetResourceRow` can project into this trivially — keeping the
/// classifier independent of storage layer types.
struct AssetIntegrityLink: Equatable {
    let role: Int
    let slot: Int
    let resourceHash: Data
}

extension RemoteAssetResourceLink {
    var integrityLink: AssetIntegrityLink {
        AssetIntegrityLink(role: role, slot: slot, resourceHash: resourceHash)
    }
}

extension SnapshotAssetResourceRow {
    var integrityLink: AssetIntegrityLink {
        AssetIntegrityLink(role: role, slot: slot, resourceHash: resourceHash)
    }
}

enum RemoteAssetIntegrityClassifier {
    /// Pure function — input is the asset's fingerprint, its links, and a predicate
    /// for "is this resource hash actually backed by a file we can read". The predicate
    /// is callsite-specific (snapshot cache view, listing-confirmed, etc) so the
    /// classifier doesn't have to know about storage layout.
    static func classify(
        assetFingerprint: Data,
        links: [AssetIntegrityLink],
        isResourceAvailable: (Data) -> Bool
    ) -> AssetIntegrityState {
        if links.isEmpty {
            return .phantom
        }

        var missingHashes: [Data] = []
        var presentRoles: Set<Int> = []
        for link in links {
            if isResourceAvailable(link.resourceHash) {
                presentRoles.insert(link.role)
            } else {
                missingHashes.append(link.resourceHash)
            }
        }

        // Priority order is load-bearing — pick the most actionable diagnosis:
        // 1. fullyMissing wins over fingerprintMismatch: nothing left to act on,
        //    cleanup is safe regardless of fp truth.
        // 2. fingerprintMismatch wins over metadataOnly/partiallyMissing: when
        //    resources DO exist, we can't trust the link set's role/slot mapping,
        //    so restore could yield wrong content. Block until resolved.
        if presentRoles.isEmpty {
            return .fullyMissing
        }

        let recomputed = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: links.map {
                (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash)
            }
        )
        if recomputed != assetFingerprint {
            return .fingerprintMismatch(recomputed: recomputed)
        }

        // Edit-history only (adjustmentData / adjustmentBase*) — without a primary
        // photo/video resource, restore can't reconstruct a viewable asset.
        if presentRoles.subtracting(ResourceTypeCode.metadataOnlyRoles).isEmpty {
            return .metadataOnlyLeft
        }

        if !missingHashes.isEmpty {
            return .partiallyMissing(missingHashes: missingHashes)
        }
        return .healthy
    }

    static func classify(
        assetFingerprint: Data,
        links: [RemoteAssetResourceLink],
        isResourceAvailable: (Data) -> Bool
    ) -> AssetIntegrityState {
        classify(assetFingerprint: assetFingerprint,
                 links: links.map(\.integrityLink),
                 isResourceAvailable: isResourceAvailable)
    }

    static func classify(
        assetFingerprint: Data,
        links: [SnapshotAssetResourceRow],
        isResourceAvailable: (Data) -> Bool
    ) -> AssetIntegrityState {
        classify(assetFingerprint: assetFingerprint,
                 links: links.map(\.integrityLink),
                 isResourceAvailable: isResourceAvailable)
    }
}

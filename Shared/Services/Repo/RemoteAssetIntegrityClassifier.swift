import Foundation

/// Keeps verify, download, Home matching, and manifest health predicates from drifting.
enum AssetIntegrityState: Sendable, Equatable {
    case healthy
    case partiallyMissing(missingHashes: [Data])
    case metadataOnlyLeft
    case fullyMissing
    case phantom
    case fingerprintMismatch(recomputed: Data)

    /// The asset can be downloaded into Photos and pass full-fingerprint verification.
    var allowsRestore: Bool {
        switch self {
        case .healthy: return true
        case .partiallyMissing, .metadataOnlyLeft, .fullyMissing, .phantom, .fingerprintMismatch: return false
        }
    }

    /// Only fully missing assets are safe for automatic tombstone cleanup.
    var allowsCleanup: Bool {
        VerifyMonthReportKind(from: self)?.allowsCleanup ?? false
    }

    var isHealthy: Bool {
        if case .healthy = self { return true }
        return false
    }
}

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
    /// Callers supply file-presence truth so storage layout cannot skew classification.
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

        // Priority order picks the diagnosis that is safest to act on.
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

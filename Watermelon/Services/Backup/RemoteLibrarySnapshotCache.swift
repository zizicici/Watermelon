import Foundation

final class RemoteLibrarySnapshotCache {
    private let lock = NSLock()
    private var snapshot = RemoteLibrarySnapshot(resources: [], assets: [], assetResourceLinks: [])

    func current() -> RemoteLibrarySnapshot {
        lock.lock()
        defer { lock.unlock() }
        return snapshot
    }

    func replace(with snapshot: RemoteLibrarySnapshot) {
        lock.lock()
        self.snapshot = snapshot
        lock.unlock()
    }

    func upsertResource(_ item: RemoteManifestResource) {
        lock.lock()
        defer { lock.unlock() }

        var resources = snapshot.resources
        if let index = resources.firstIndex(where: { $0.id == item.id }) {
            resources[index] = item
        } else {
            resources.append(item)
        }

        resources.sort { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            if lhs.month != rhs.month { return lhs.month < rhs.month }
            let lhsTime = lhs.creationDateNs ?? lhs.backedUpAtNs
            let rhsTime = rhs.creationDateNs ?? rhs.backedUpAtNs
            if lhsTime != rhsTime { return lhsTime < rhsTime }
            return lhs.fileName < rhs.fileName
        }

        snapshot = RemoteLibrarySnapshot(
            resources: resources,
            assets: snapshot.assets,
            assetResourceLinks: snapshot.assetResourceLinks
        )
    }

    func upsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]? = nil) {
        lock.lock()
        defer { lock.unlock() }

        var assets = snapshot.assets
        if let index = assets.firstIndex(where: { $0.id == asset.id }) {
            assets[index] = asset
        } else {
            assets.append(asset)
        }

        assets.sort { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            if lhs.month != rhs.month { return lhs.month < rhs.month }
            let lhsTime = lhs.creationDateNs ?? lhs.backedUpAtNs
            let rhsTime = rhs.creationDateNs ?? rhs.backedUpAtNs
            if lhsTime != rhsTime { return lhsTime < rhsTime }
            return lhs.assetFingerprintHex < rhs.assetFingerprintHex
        }

        var assetResourceLinks = snapshot.assetResourceLinks
        if let links {
            assetResourceLinks.removeAll {
                $0.year == asset.year
                    && $0.month == asset.month
                    && $0.assetFingerprint == asset.assetFingerprint
            }
            assetResourceLinks.append(contentsOf: links)
            assetResourceLinks.sort { lhs, rhs in
                if lhs.year != rhs.year { return lhs.year < rhs.year }
                if lhs.month != rhs.month { return lhs.month < rhs.month }
                if lhs.assetFingerprint != rhs.assetFingerprint {
                    return lhs.assetFingerprint.lexicographicallyPrecedes(rhs.assetFingerprint)
                }
                if lhs.role != rhs.role { return lhs.role < rhs.role }
                if lhs.slot != rhs.slot { return lhs.slot < rhs.slot }
                return lhs.resourceHash.lexicographicallyPrecedes(rhs.resourceHash)
            }
        }

        snapshot = RemoteLibrarySnapshot(
            resources: snapshot.resources,
            assets: assets,
            assetResourceLinks: assetResourceLinks
        )
    }
}

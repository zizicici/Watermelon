import Foundation

final class RemoteLibrarySnapshotCache {
    private struct LinkKey: Hashable {
        let month: LibraryMonthKey
        let assetFingerprint: Data
        let role: Int
        let slot: Int
    }

    private typealias ResourceMap = [String: RemoteManifestResource]
    private typealias AssetMap = [String: RemoteManifestAsset]
    private typealias LinkMap = [LinkKey: RemoteAssetResourceLink]

    private let lock = NSLock()
    private var resourcesByMonth: [LibraryMonthKey: ResourceMap] = [:]
    private var assetsByMonth: [LibraryMonthKey: AssetMap] = [:]
    private var linksByMonth: [LibraryMonthKey: LinkMap] = [:]
    private var linkKeysByAssetID: [String: Set<LinkKey>] = [:]

    private var snapshot = RemoteLibrarySnapshot(resources: [], assets: [], assetResourceLinks: [])
    private var fullSnapshotDirty = false

    private var revision: UInt64 = 0
    private var revisionMonthHistory: [(revision: UInt64, months: Set<LibraryMonthKey>)] = []
    private let maxRevisionHistoryCount = 256

    func current() -> RemoteLibrarySnapshot {
        lock.lock()
        defer { lock.unlock() }
        if fullSnapshotDirty {
            snapshot = rebuildFullSnapshotLocked()
            fullSnapshotDirty = false
        }
        return snapshot
    }

    func state(since baseRevision: UInt64?) -> RemoteLibrarySnapshotState {
        lock.lock()
        defer { lock.unlock() }

        let (isFullSnapshot, changedMonths) = changedMonthsLocked(since: baseRevision)
        let sortedMonths = changedMonths.sorted()

        let monthDeltas = sortedMonths.map { month -> RemoteLibraryMonthDelta in
            RemoteLibraryMonthDelta(
                month: month,
                resources: sortedResourcesLocked(for: month),
                assets: sortedAssetsLocked(for: month),
                assetResourceLinks: sortedLinksLocked(for: month)
            )
        }

        return RemoteLibrarySnapshotState(
            revision: revision,
            isFullSnapshot: isFullSnapshot,
            monthDeltas: monthDeltas
        )
    }

    func replace(with nextSnapshot: RemoteLibrarySnapshot) {
        let (nextResourcesByMonth, nextAssetsByMonth, nextLinksByMonth, nextLinkKeysByAssetID) =
            Self.buildMonthMaps(from: nextSnapshot)

        lock.lock()

        let changedMonths = computeChangedMonthsLocked(
            nextResourcesByMonth: nextResourcesByMonth,
            nextAssetsByMonth: nextAssetsByMonth,
            nextLinksByMonth: nextLinksByMonth
        )

        guard !changedMonths.isEmpty else {
            lock.unlock()
            return
        }

        resourcesByMonth = nextResourcesByMonth
        assetsByMonth = nextAssetsByMonth
        linksByMonth = nextLinksByMonth
        linkKeysByAssetID = nextLinkKeysByAssetID

        self.snapshot = nextSnapshot
        fullSnapshotDirty = false
        bumpRevisionLocked(changedMonths)

        lock.unlock()
    }

    func upsertResource(_ item: RemoteManifestResource) {
        lock.lock()

        let month = LibraryMonthKey(year: item.year, month: item.month)
        var monthResources = resourcesByMonth[month] ?? [:]
        if monthResources[item.id] == item {
            lock.unlock()
            return
        }

        monthResources[item.id] = item
        if monthResources.isEmpty {
            resourcesByMonth[month] = nil
        } else {
            resourcesByMonth[month] = monthResources
        }

        fullSnapshotDirty = true
        bumpRevisionLocked([month])
        lock.unlock()
    }

    func upsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]? = nil) {
        lock.lock()

        let month = LibraryMonthKey(year: asset.year, month: asset.month)
        var hasChanged = false
        var changedMonths: Set<LibraryMonthKey> = [month]

        var monthAssets = assetsByMonth[month] ?? [:]
        if monthAssets[asset.id] != asset {
            monthAssets[asset.id] = asset
            if monthAssets.isEmpty {
                assetsByMonth[month] = nil
            } else {
                assetsByMonth[month] = monthAssets
            }
            hasChanged = true
        }

        if let links {
            let assetID = asset.id
            let oldKeys = linkKeysByAssetID[assetID] ?? []

            var newLinksByKey: LinkMap = [:]
            newLinksByKey.reserveCapacity(links.count)
            for link in links {
                let key = LinkKey(
                    month: LibraryMonthKey(year: link.year, month: link.month),
                    assetFingerprint: link.assetFingerprint,
                    role: link.role,
                    slot: link.slot
                )
                newLinksByKey[key] = link
            }

            var oldLinksByKey: LinkMap = [:]
            oldLinksByKey.reserveCapacity(oldKeys.count)
            for key in oldKeys {
                if let existing = linksByMonth[key.month]?[key] {
                    oldLinksByKey[key] = existing
                }
            }

            if oldLinksByKey != newLinksByKey {
                changedMonths.formUnion(oldKeys.map(\.month))
                changedMonths.formUnion(newLinksByKey.keys.map(\.month))

                for oldKey in oldKeys {
                    if var monthLinks = linksByMonth[oldKey.month] {
                        monthLinks[oldKey] = nil
                        if monthLinks.isEmpty {
                            linksByMonth[oldKey.month] = nil
                        } else {
                            linksByMonth[oldKey.month] = monthLinks
                        }
                    }
                }

                if newLinksByKey.isEmpty {
                    linkKeysByAssetID[assetID] = nil
                } else {
                    linkKeysByAssetID[assetID] = Set(newLinksByKey.keys)
                    for (newKey, value) in newLinksByKey {
                        var monthLinks = linksByMonth[newKey.month] ?? [:]
                        monthLinks[newKey] = value
                        linksByMonth[newKey.month] = monthLinks
                    }
                }

                hasChanged = true
            }
        }

        if hasChanged {
            fullSnapshotDirty = true
            bumpRevisionLocked(changedMonths)
        }

        lock.unlock()
    }

    private func changedMonthsLocked(since baseRevision: UInt64?) -> (Bool, Set<LibraryMonthKey>) {
        guard let baseRevision else {
            return (true, allKnownMonthsLocked())
        }

        if baseRevision == revision {
            return (false, [])
        }

        guard baseRevision < revision else {
            return (true, allKnownMonthsLocked())
        }

        guard let firstHistoryRevision = revisionMonthHistory.first?.revision,
              baseRevision >= firstHistoryRevision - 1 else {
            return (true, allKnownMonthsLocked())
        }

        var changedMonths = Set<LibraryMonthKey>()
        for entry in revisionMonthHistory where entry.revision > baseRevision {
            changedMonths.formUnion(entry.months)
        }
        return (false, changedMonths)
    }

    private func computeChangedMonthsLocked(
        nextResourcesByMonth: [LibraryMonthKey: ResourceMap],
        nextAssetsByMonth: [LibraryMonthKey: AssetMap],
        nextLinksByMonth: [LibraryMonthKey: LinkMap]
    ) -> Set<LibraryMonthKey> {
        let oldMonths = Set(resourcesByMonth.keys)
            .union(assetsByMonth.keys)
            .union(linksByMonth.keys)
        let newMonths = Set(nextResourcesByMonth.keys)
            .union(nextAssetsByMonth.keys)
            .union(nextLinksByMonth.keys)

        var changedMonths = Set<LibraryMonthKey>()
        for month in oldMonths.union(newMonths) {
            if resourcesByMonth[month] != nextResourcesByMonth[month] ||
                assetsByMonth[month] != nextAssetsByMonth[month] ||
                linksByMonth[month] != nextLinksByMonth[month] {
                changedMonths.insert(month)
            }
        }

        return changedMonths
    }

    private func allKnownMonthsLocked() -> Set<LibraryMonthKey> {
        Set(resourcesByMonth.keys)
            .union(assetsByMonth.keys)
            .union(linksByMonth.keys)
    }

    private func bumpRevisionLocked(_ changedMonths: Set<LibraryMonthKey>) {
        guard !changedMonths.isEmpty else { return }

        revision &+= 1
        revisionMonthHistory.append((revision: revision, months: changedMonths))
        if revisionMonthHistory.count > maxRevisionHistoryCount {
            revisionMonthHistory.removeFirst(revisionMonthHistory.count - maxRevisionHistoryCount)
        }
    }

    private func rebuildFullSnapshotLocked() -> RemoteLibrarySnapshot {
        let sortedMonths = allKnownMonthsLocked().sorted()

        var resources: [RemoteManifestResource] = []
        var assets: [RemoteManifestAsset] = []
        var links: [RemoteAssetResourceLink] = []

        resources.reserveCapacity(resourcesByMonth.values.reduce(0) { $0 + $1.count })
        assets.reserveCapacity(assetsByMonth.values.reduce(0) { $0 + $1.count })
        links.reserveCapacity(linksByMonth.values.reduce(0) { $0 + $1.count })

        for month in sortedMonths {
            resources.append(contentsOf: sortedResourcesLocked(for: month))
            assets.append(contentsOf: sortedAssetsLocked(for: month))
            links.append(contentsOf: sortedLinksLocked(for: month))
        }

        return RemoteLibrarySnapshot(resources: resources, assets: assets, assetResourceLinks: links)
    }

    private func sortedResourcesLocked(for month: LibraryMonthKey) -> [RemoteManifestResource] {
        let monthResources = resourcesByMonth[month] ?? [:]
        return monthResources.values.sorted { lhs, rhs in
            if lhs.creationDateNs != rhs.creationDateNs {
                return (lhs.creationDateNs ?? lhs.backedUpAtNs) < (rhs.creationDateNs ?? rhs.backedUpAtNs)
            }
            return lhs.fileName < rhs.fileName
        }
    }

    private func sortedAssetsLocked(for month: LibraryMonthKey) -> [RemoteManifestAsset] {
        let monthAssets = assetsByMonth[month] ?? [:]
        return monthAssets.values.sorted { lhs, rhs in
            if lhs.creationDateNs != rhs.creationDateNs {
                return (lhs.creationDateNs ?? lhs.backedUpAtNs) < (rhs.creationDateNs ?? rhs.backedUpAtNs)
            }
            return lhs.assetFingerprintHex < rhs.assetFingerprintHex
        }
    }

    private func sortedLinksLocked(for month: LibraryMonthKey) -> [RemoteAssetResourceLink] {
        let monthLinks = linksByMonth[month] ?? [:]
        return monthLinks.values.sorted { lhs, rhs in
            if lhs.assetFingerprint != rhs.assetFingerprint {
                return lhs.assetFingerprint.lexicographicallyPrecedes(rhs.assetFingerprint)
            }
            if lhs.role != rhs.role { return lhs.role < rhs.role }
            if lhs.slot != rhs.slot { return lhs.slot < rhs.slot }
            return lhs.resourceHash.lexicographicallyPrecedes(rhs.resourceHash)
        }
    }

    private static func buildMonthMaps(
        from snapshot: RemoteLibrarySnapshot
    ) -> (
        resourcesByMonth: [LibraryMonthKey: ResourceMap],
        assetsByMonth: [LibraryMonthKey: AssetMap],
        linksByMonth: [LibraryMonthKey: LinkMap],
        linkKeysByAssetID: [String: Set<LinkKey>]
    ) {
        var resourcesByMonth: [LibraryMonthKey: ResourceMap] = [:]
        var assetsByMonth: [LibraryMonthKey: AssetMap] = [:]
        var linksByMonth: [LibraryMonthKey: LinkMap] = [:]
        var linkKeysByAssetID: [String: Set<LinkKey>] = [:]

        for resource in snapshot.resources {
            let month = LibraryMonthKey(year: resource.year, month: resource.month)
            resourcesByMonth[month, default: [:]][resource.id] = resource
        }

        for asset in snapshot.assets {
            let month = LibraryMonthKey(year: asset.year, month: asset.month)
            assetsByMonth[month, default: [:]][asset.id] = asset
        }

        for link in snapshot.assetResourceLinks {
            let month = LibraryMonthKey(year: link.year, month: link.month)
            let key = LinkKey(
                month: month,
                assetFingerprint: link.assetFingerprint,
                role: link.role,
                slot: link.slot
            )
            linksByMonth[month, default: [:]][key] = link
            linkKeysByAssetID[link.assetID, default: []].insert(key)
        }

        return (resourcesByMonth, assetsByMonth, linksByMonth, linkKeysByAssetID)
    }
}

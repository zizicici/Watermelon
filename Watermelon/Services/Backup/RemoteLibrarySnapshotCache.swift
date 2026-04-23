import Foundation

final class RemoteLibrarySnapshotCache: @unchecked Sendable {
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

    private var revision: UInt64 = 0
    private var monthLastChangedRevision: [LibraryMonthKey: UInt64] = [:]

    private struct MonthStats {
        var assetCount: Int
        var photoCount: Int
        var videoCount: Int
        var totalSizeBytes: Int64
    }
    private var monthStatsCache: [LibraryMonthKey: MonthStats] = [:]

    private func computeMonthStatsLocked(for month: LibraryMonthKey) -> MonthStats? {
        guard let monthAssets = assetsByMonth[month], !monthAssets.isEmpty else { return nil }
        let totalSize = monthAssets.values.reduce(Int64(0)) { $0 + $1.totalFileSizeBytes }
        let monthLinks = linksByMonth[month] ?? [:]

        // Classify per-asset using the same logic as HomeAlbumMatching.detectMediaKind:
        // livePhoto (pairedVideo + photoLike) and pure photo → photo count;
        // video-like without paired-photo → video count.
        var rolesByAssetID: [String: [Int]] = [:]
        for (_, link) in monthLinks {
            rolesByAssetID[link.assetID, default: []].append(link.role)
        }
        var videoCount = 0
        for assetID in monthAssets.keys {
            let roles = rolesByAssetID[assetID] ?? []
            let hasPairedVideo = roles.contains { ResourceTypeCode.isPairedVideo($0) }
            let hasPhotoLike = roles.contains { ResourceTypeCode.isPhotoLike($0) }
            if hasPairedVideo, hasPhotoLike { continue }
            if roles.contains(where: { ResourceTypeCode.isVideoLike($0) }) {
                videoCount += 1
            }
        }
        let photoCount = monthAssets.count - videoCount

        return MonthStats(assetCount: monthAssets.count, photoCount: photoCount, videoCount: videoCount, totalSizeBytes: totalSize)
    }

    func current() -> RemoteLibrarySnapshot {
        lock.withLock { rebuildFullSnapshotLocked() }
    }

    func state(since baseRevision: UInt64?) -> RemoteLibrarySnapshotState {
        let (isFullSnapshot, responseRevision, monthEntries) = lock.withLock { () -> (Bool, UInt64, [(month: LibraryMonthKey, resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink])]) in
            let (isFullSnapshot, changedMonths) = changedMonthsLocked(since: baseRevision)
            let sortedMonths = changedMonths.sorted()
            let entries = sortedMonths.map { month in
                (
                    month: month,
                    resources: Array((resourcesByMonth[month] ?? [:]).values),
                    assets: Array((assetsByMonth[month] ?? [:]).values),
                    links: Array((linksByMonth[month] ?? [:]).values)
                )
            }
            return (isFullSnapshot, revision, entries)
        }

        let monthDeltas = monthEntries.map { entry in
            RemoteLibraryMonthDelta(
                month: entry.month,
                resources: entry.resources,
                assets: entry.assets,
                assetResourceLinks: entry.links
            )
        }

        return RemoteLibrarySnapshotState(
            revision: responseRevision,
            isFullSnapshot: isFullSnapshot,
            monthDeltas: monthDeltas
        )
    }

    func reset() {
        lock.withLock {
            resourcesByMonth.removeAll()
            assetsByMonth.removeAll()
            linksByMonth.removeAll()
            linkKeysByAssetID.removeAll()
            monthStatsCache.removeAll()
            revision = 0
            monthLastChangedRevision.removeAll()
        }
    }

    @discardableResult
    func replaceMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink]
    ) -> Bool {
        lock.withLock {
            let nextResources = Dictionary(uniqueKeysWithValues: resources.map { ($0.id, $0) })
            let nextAssets = Dictionary(uniqueKeysWithValues: assets.map { ($0.id, $0) })
            let nextLinks = Dictionary(uniqueKeysWithValues: assetResourceLinks.map { link in
                (
                    LinkKey(
                        month: LibraryMonthKey(year: link.year, month: link.month),
                        assetFingerprint: link.assetFingerprint,
                        role: link.role,
                        slot: link.slot
                    ),
                    link
                )
            })

            let previousResources = resourcesByMonth[month] ?? [:]
            let previousAssets = assetsByMonth[month] ?? [:]
            let previousLinks = linksByMonth[month] ?? [:]

            if previousResources == nextResources,
               previousAssets == nextAssets,
               previousLinks == nextLinks {
                return false
            }

            detachLinkKeysLocked(previousLinks)

            resourcesByMonth[month] = nextResources.isEmpty ? nil : nextResources
            assetsByMonth[month] = nextAssets.isEmpty ? nil : nextAssets
            linksByMonth[month] = nextLinks.isEmpty ? nil : nextLinks

            monthStatsCache[month] = computeMonthStatsLocked(for: month)

            attachLinkKeysLocked(nextLinks)

            bumpRevisionLocked([month])
            return true
        }
    }

    @discardableResult
    func removeMonth(_ month: LibraryMonthKey) -> Bool {
        lock.withLock {
            let previousResources = resourcesByMonth.removeValue(forKey: month)
            let previousAssets = assetsByMonth.removeValue(forKey: month)
            let previousLinks = linksByMonth.removeValue(forKey: month)

            guard previousResources != nil || previousAssets != nil || previousLinks != nil else {
                return false
            }

            if let previousLinks {
                detachLinkKeysLocked(previousLinks)
            }
            monthStatsCache[month] = nil
            bumpRevisionLocked([month])
            return true
        }
    }

    func upsertResource(_ item: RemoteManifestResource) {
        lock.withLock {
            let month = LibraryMonthKey(year: item.year, month: item.month)
            var monthResources = resourcesByMonth[month] ?? [:]
            guard monthResources[item.id] != item else { return }

            monthResources[item.id] = item
            resourcesByMonth[month] = monthResources
            bumpRevisionLocked([month])
        }
    }

    func upsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]? = nil) {
        lock.withLock {
            let month = LibraryMonthKey(year: asset.year, month: asset.month)
            var hasChanged = false
            var changedMonths: Set<LibraryMonthKey> = [month]

            var monthAssets = assetsByMonth[month] ?? [:]
            if monthAssets[asset.id] != asset {
                monthAssets[asset.id] = asset
                assetsByMonth[month] = monthAssets
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
                for changedMonth in changedMonths {
                    monthStatsCache[changedMonth] = computeMonthStatsLocked(for: changedMonth)
                }
                bumpRevisionLocked(changedMonths)
            }
        }
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

        let changedMonths = Set(
            monthLastChangedRevision.compactMap { month, lastChangedRevision in
                lastChangedRevision > baseRevision ? month : nil
            }
        )
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
        for month in changedMonths {
            monthLastChangedRevision[month] = revision
        }
    }

    func monthSummaries() -> [(month: LibraryMonthKey, assetCount: Int, photoCount: Int, videoCount: Int, totalSizeBytes: Int64)] {
        lock.withLock {
            monthStatsCache.map { (month, stats) in
                (month: month, assetCount: stats.assetCount, photoCount: stats.photoCount, videoCount: stats.videoCount, totalSizeBytes: stats.totalSizeBytes)
            }
        }
    }

    func monthRawData(for month: LibraryMonthKey) -> RemoteLibraryMonthDelta? {
        lock.withLock {
            let monthAssets = assetsByMonth[month] ?? [:]
            guard !monthAssets.isEmpty else { return nil }
            return RemoteLibraryMonthDelta(
                month: month,
                resources: Array((resourcesByMonth[month] ?? [:]).values),
                assets: Array(monthAssets.values),
                assetResourceLinks: Array((linksByMonth[month] ?? [:]).values)
            )
        }
    }

    private func rebuildFullSnapshotLocked() -> RemoteLibrarySnapshot {
        let sortedMonths = allKnownMonthsLocked().sorted()

        var resources: [RemoteManifestResource] = []
        var assets: [RemoteManifestAsset] = []
        var links: [RemoteAssetResourceLink] = []
        var assetFingerprintSet = Set<Data>()

        resources.reserveCapacity(resourcesByMonth.values.reduce(0) { $0 + $1.count })
        assets.reserveCapacity(assetsByMonth.values.reduce(0) { $0 + $1.count })
        links.reserveCapacity(linksByMonth.values.reduce(0) { $0 + $1.count })
        assetFingerprintSet.reserveCapacity(assetsByMonth.values.reduce(0) { $0 + $1.count })

        for month in sortedMonths {
            resources.append(contentsOf: sortedResourcesLocked(for: month))
            let monthAssets = sortedAssetsLocked(for: month)
            assets.append(contentsOf: monthAssets)
            for asset in monthAssets {
                assetFingerprintSet.insert(asset.assetFingerprint)
            }
            links.append(contentsOf: sortedLinksLocked(for: month))
        }

        return RemoteLibrarySnapshot(
            resources: resources,
            assets: assets,
            assetResourceLinks: links,
            assetFingerprintSet: assetFingerprintSet
        )
    }

    private func sortedResourcesLocked(for month: LibraryMonthKey) -> [RemoteManifestResource] {
        Self.sortedResources(Array((resourcesByMonth[month] ?? [:]).values))
    }

    private func sortedAssetsLocked(for month: LibraryMonthKey) -> [RemoteManifestAsset] {
        Self.sortedAssets(Array((assetsByMonth[month] ?? [:]).values))
    }

    private func sortedLinksLocked(for month: LibraryMonthKey) -> [RemoteAssetResourceLink] {
        Self.sortedLinks(Array((linksByMonth[month] ?? [:]).values))
    }

    private static func sortedResources(_ resources: [RemoteManifestResource]) -> [RemoteManifestResource] {
        resources.sorted { lhs, rhs in
            if lhs.creationDateMs != rhs.creationDateMs {
                return (lhs.creationDateMs ?? lhs.backedUpAtMs) < (rhs.creationDateMs ?? rhs.backedUpAtMs)
            }
            return lhs.fileName < rhs.fileName
        }
    }

    private static func sortedAssets(_ assets: [RemoteManifestAsset]) -> [RemoteManifestAsset] {
        assets.sorted { lhs, rhs in
            if lhs.creationDateMs != rhs.creationDateMs {
                return (lhs.creationDateMs ?? lhs.backedUpAtMs) < (rhs.creationDateMs ?? rhs.backedUpAtMs)
            }
            return lhs.assetFingerprintHex < rhs.assetFingerprintHex
        }
    }

    private static func sortedLinks(_ links: [RemoteAssetResourceLink]) -> [RemoteAssetResourceLink] {
        links.sorted { lhs, rhs in
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

    private func detachLinkKeysLocked(_ links: LinkMap) {
        for (key, link) in links {
            guard var keys = linkKeysByAssetID[link.assetID] else { continue }
            keys.remove(key)
            if keys.isEmpty {
                linkKeysByAssetID[link.assetID] = nil
            } else {
                linkKeysByAssetID[link.assetID] = keys
            }
        }
    }

    private func attachLinkKeysLocked(_ links: LinkMap) {
        for (key, link) in links {
            linkKeysByAssetID[link.assetID, default: []].insert(key)
        }
    }
}

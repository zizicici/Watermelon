import Foundation
import os.log

private let snapshotCacheLog = Logger(subsystem: "com.zizicici.watermelon", category: "RemoteLibrarySnapshotCache")

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

    private var resourceHashesByMonth: [LibraryMonthKey: Set<Data>] = [:]
    private var resourceBytesByMonth: [LibraryMonthKey: Int64] = [:]
    private var lastSyncedAt: Date?
    /// Forces the next state(since:) to full snapshot — post-reset revision can collide
    /// with the engine's old snapshotRevision and silently produce an empty delta.
    private var freshlyReset: Bool = false

    private struct ChangeKind {
        let resources: Bool
        let assets: Bool
        let links: Bool
        var any: Bool { resources || assets || links }
        var anyAssetOrLink: Bool { assets || links }
    }

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
            resourceHashesByMonth.removeAll()
            resourceBytesByMonth.removeAll()
            lastSyncedAt = nil
            revision = 0
            monthLastChangedRevision.removeAll()
            freshlyReset = true
        }
    }

    func markSynced(_ at: Date) {
        lock.withLock {
            lastSyncedAt = at
        }
    }

    func currentLastSyncedAt() -> Date? {
        lock.withLock { lastSyncedAt }
    }

    /// Snapshots per-month dicts under the lock (cheap COW reference copy), then
    /// runs the SHA-256 fingerprint recompute outside the lock so writers aren't
    /// stalled by the (potentially large) incomplete-asset scan.
    func healthDigest() -> RemoteHealthDigest {
        let snapshot: HealthSnapshot = lock.withLock {
            HealthSnapshot(
                assetsByMonth: assetsByMonth,
                linksByMonth: linksByMonth,
                resourcesByMonth: resourcesByMonth,
                resourceHashesByMonth: resourceHashesByMonth,
                resourceBytesByMonth: resourceBytesByMonth,
                lastSyncedAt: lastSyncedAt
            )
        }

        let assetCount = snapshot.assetsByMonth.values.reduce(0) { $0 + $1.count }
        let resourceCount = snapshot.resourcesByMonth.values.reduce(0) { $0 + $1.count }
        let totalBytes = snapshot.resourceBytesByMonth.values.reduce(Int64(0), +)

        var incompleteFlat: [IncompleteAssetEntry] = []
        for month in snapshot.assetsByMonth.keys {
            incompleteFlat.append(contentsOf: Self.computeIncompleteForMonth(month, snapshot: snapshot))
        }
        return RemoteHealthDigest(
            totalAssets: assetCount,
            totalResources: resourceCount,
            totalSizeBytes: totalBytes,
            incompleteAssets: incompleteFlat,
            lastIndexSyncedAt: snapshot.lastSyncedAt
        )
    }

    private struct HealthSnapshot {
        let assetsByMonth: [LibraryMonthKey: AssetMap]
        let linksByMonth: [LibraryMonthKey: LinkMap]
        let resourcesByMonth: [LibraryMonthKey: ResourceMap]
        let resourceHashesByMonth: [LibraryMonthKey: Set<Data>]
        let resourceBytesByMonth: [LibraryMonthKey: Int64]
        let lastSyncedAt: Date?
    }

    /// `precondition` is intentional in Release: violating the ChangeKind/nextX nil-ness
    /// contract would silently desync reverse index and derived caches from main maps.
    @discardableResult
    private func applyMonthFullReplaceLocked(
        _ month: LibraryMonthKey,
        nextResources: ResourceMap?,
        nextAssets: AssetMap?,
        nextLinks: LinkMap?,
        changeKind: ChangeKind,
        bumpRevision: Bool = true
    ) -> Bool {
        precondition(
            (changeKind.resources == (nextResources != nil)) &&
            (changeKind.assets == (nextAssets != nil)) &&
            (changeKind.links == (nextLinks != nil)),
            "ChangeKind must match nil-ness of next* params"
        )
        guard changeKind.any else { return false }

        if changeKind.links, let nextLinks {
            let prevLinks = linksByMonth[month] ?? [:]
            let removedKeys = Set(prevLinks.keys).subtracting(nextLinks.keys)
            let addedKeys = Set(nextLinks.keys).subtracting(prevLinks.keys)
            for key in removedKeys {
                guard let link = prevLinks[key] else { continue }
                if var ks = linkKeysByAssetID[link.assetID] {
                    ks.remove(key)
                    linkKeysByAssetID[link.assetID] = ks.isEmpty ? nil : ks
                }
            }
            for key in addedKeys {
                guard let link = nextLinks[key] else { continue }
                linkKeysByAssetID[link.assetID, default: []].insert(key)
            }
        }

        if changeKind.resources, let nextResources {
            resourcesByMonth[month] = nextResources.isEmpty ? nil : nextResources
        }
        if changeKind.assets, let nextAssets {
            assetsByMonth[month] = nextAssets.isEmpty ? nil : nextAssets
        }
        if changeKind.links, let nextLinks {
            linksByMonth[month] = nextLinks.isEmpty ? nil : nextLinks
        }

        recomputeDerivedForMonthLocked(month, changeKind: changeKind)

        if bumpRevision {
            bumpRevisionLocked([month])
        }
        return true
    }

    /// A key may appear in BOTH `removedLinkKeys` and `addedLinks` (in-place value update);
    /// remove-then-add yields the correct net state because `link.assetID` derives from
    /// month + fingerprint, so both ops target the same `linkKeysByAssetID` entry.
    @discardableResult
    private func applyMonthLinkDeltaLocked(
        _ month: LibraryMonthKey,
        removedLinkKeys: Set<LinkKey>,
        addedLinks: LinkMap,
        assetUpsert: RemoteManifestAsset?,
        bumpRevision: Bool = true
    ) -> Bool {
        let assetsChanged = assetUpsert != nil
        let linksChanged = !removedLinkKeys.isEmpty || !addedLinks.isEmpty
        if !assetsChanged && !linksChanged { return false }

        if linksChanged {
            for key in removedLinkKeys {
                if let link = linksByMonth[month]?[key] {
                    if var ks = linkKeysByAssetID[link.assetID] {
                        ks.remove(key)
                        linkKeysByAssetID[link.assetID] = ks.isEmpty ? nil : ks
                    }
                }
                linksByMonth[month]?.removeValue(forKey: key)
            }
            for (key, link) in addedLinks {
                linksByMonth[month, default: [:]][key] = link
                linkKeysByAssetID[link.assetID, default: []].insert(key)
            }
            if linksByMonth[month]?.isEmpty == true {
                linksByMonth[month] = nil
            }
        }

        if let assetUpsert {
            assetsByMonth[month, default: [:]][assetUpsert.id] = assetUpsert
        }

        let kind = ChangeKind(resources: false, assets: assetsChanged, links: linksChanged)
        recomputeDerivedForMonthLocked(month, changeKind: kind)
        if bumpRevision {
            bumpRevisionLocked([month])
        }
        return true
    }

    /// Stats and resource-hash tables are cheap; incompleteness (SHA-256 recompute
    /// per asset) is deferred to `healthDigest()` so writers aren't blocked.
    private func recomputeDerivedForMonthLocked(_ month: LibraryMonthKey, changeKind: ChangeKind) {
        if changeKind.anyAssetOrLink {
            monthStatsCache[month] = computeMonthStatsLocked(for: month)
        }
        if changeKind.resources {
            let monthResources = resourcesByMonth[month] ?? [:]
            if monthResources.isEmpty {
                resourceHashesByMonth[month] = nil
                resourceBytesByMonth[month] = nil
            } else {
                resourceHashesByMonth[month] = Set(monthResources.values.map(\.contentHash))
                resourceBytesByMonth[month] = monthResources.values.reduce(Int64(0)) { $0 + $1.fileSize }
            }
        }
    }

    /// Iterates `assetsByMonth`, not `linksByMonth`, so phantom assets are surfaced.
    /// Runs against an immutable snapshot, no lock needed.
    private static func computeIncompleteForMonth(
        _ month: LibraryMonthKey,
        snapshot: HealthSnapshot
    ) -> [IncompleteAssetEntry] {
        let monthAssets = snapshot.assetsByMonth[month] ?? [:]
        guard !monthAssets.isEmpty else { return [] }
        let monthLinks = snapshot.linksByMonth[month] ?? [:]
        let resourceHashes = snapshot.resourceHashesByMonth[month] ?? []
        let monthResources = snapshot.resourcesByMonth[month] ?? [:]

        var fileNameByHash: [Data: String] = [:]
        fileNameByHash.reserveCapacity(monthResources.count)
        for resource in monthResources.values {
            fileNameByHash[resource.contentHash] = resource.fileName
        }

        var linksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]
        for (_, link) in monthLinks {
            linksByFingerprint[link.assetFingerprint, default: []].append(link)
        }

        var entries: [IncompleteAssetEntry] = []
        for (_, asset) in monthAssets {
            let fp = asset.assetFingerprint
            let links = linksByFingerprint[fp] ?? []
            guard MonthManifestStore.isAssetIncomplete(
                links: links,
                isResourceAvailable: { resourceHashes.contains($0) },
                assetFingerprint: asset.assetFingerprint
            ) else { continue }

            if links.isEmpty {
                entries.append(IncompleteAssetEntry(
                    id: fp,
                    month: month,
                    creationDate: asset.creationDate,
                    representativeFileName: nil,
                    missingResourceCount: asset.resourceCount,
                    totalResourceCount: asset.resourceCount,
                    missingResourceHashes: [],
                    reason: .manifestOrphan
                ))
            } else {
                let missing = links.filter { !resourceHashes.contains($0.resourceHash) }.map(\.resourceHash)
                let repFileName = links.lazy.compactMap { fileNameByHash[$0.resourceHash] }.first
                entries.append(IncompleteAssetEntry(
                    id: fp,
                    month: month,
                    creationDate: asset.creationDate,
                    representativeFileName: repFileName,
                    missingResourceCount: missing.count,
                    totalResourceCount: links.count,
                    missingResourceHashes: missing,
                    reason: .manifestOrphan
                ))
            }
        }
        return entries
    }

    @discardableResult
    func replaceMonth(
        _ month: LibraryMonthKey,
        resources: [RemoteManifestResource],
        assets: [RemoteManifestAsset],
        assetResourceLinks: [RemoteAssetResourceLink]
    ) -> Bool {
        lock.withLock {
            let nextResources = Self.dedupedByKey(resources, key: \.id, scope: "resource", month: month) { $0.fileName }
            let nextAssets = Self.dedupedByKey(assets, key: \.id, scope: "asset", month: month) { $0.assetFingerprintHex }
            let nextLinks = Self.dedupedByKey(
                assetResourceLinks,
                key: { link in
                    LinkKey(
                        month: LibraryMonthKey(year: link.year, month: link.month),
                        assetFingerprint: link.assetFingerprint,
                        role: link.role,
                        slot: link.slot
                    )
                },
                scope: "link",
                month: month
            ) { "fp=\($0.assetFingerprint.hexString) r=\($0.role) s=\($0.slot)" }

            let previousResources = resourcesByMonth[month] ?? [:]
            let previousAssets = assetsByMonth[month] ?? [:]
            let previousLinks = linksByMonth[month] ?? [:]
            let resourcesChanged = previousResources != nextResources
            let assetsChanged = previousAssets != nextAssets
            let linksChanged = previousLinks != nextLinks
            return applyMonthFullReplaceLocked(
                month,
                nextResources: resourcesChanged ? nextResources : nil,
                nextAssets: assetsChanged ? nextAssets : nil,
                nextLinks: linksChanged ? nextLinks : nil,
                changeKind: ChangeKind(
                    resources: resourcesChanged,
                    assets: assetsChanged,
                    links: linksChanged
                )
            )
        }
    }

    @discardableResult
    func removeMonth(_ month: LibraryMonthKey) -> Bool {
        lock.withLock {
            guard resourcesByMonth[month] != nil
                || assetsByMonth[month] != nil
                || linksByMonth[month] != nil else { return false }
            // Pass all three fields to guarantee full-state cleanup including derived caches,
            // matching the original unconditional `monthStatsCache[month] = nil`.
            return applyMonthFullReplaceLocked(
                month,
                nextResources: [:],
                nextAssets: [:],
                nextLinks: [:],
                changeKind: ChangeKind(resources: true, assets: true, links: true)
            )
        }
    }

    func upsertResource(_ item: RemoteManifestResource) {
        lock.withLock {
            let month = LibraryMonthKey(year: item.year, month: item.month)
            var monthResources = resourcesByMonth[month] ?? [:]
            guard monthResources[item.id] != item else { return }
            monthResources[item.id] = item
            applyMonthFullReplaceLocked(
                month,
                nextResources: monthResources,
                nextAssets: nil,
                nextLinks: nil,
                changeKind: ChangeKind(resources: true, assets: false, links: false)
            )
        }
    }

    func upsertAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]? = nil) {
        lock.withLock {
            let assetMonth = LibraryMonthKey(year: asset.year, month: asset.month)
            var changedMonths: Set<LibraryMonthKey> = []

            let prevAsset = assetsByMonth[assetMonth]?[asset.id]
            let assetChanged = (prevAsset != asset)
            if assetChanged {
                changedMonths.insert(assetMonth)
            }

            var perMonthRemoved: [LibraryMonthKey: Set<LinkKey>] = [:]
            var perMonthAdded: [LibraryMonthKey: LinkMap] = [:]

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
                    for k in oldKeys {
                        perMonthRemoved[k.month, default: []].insert(k)
                    }
                    for (k, v) in newLinksByKey {
                        perMonthAdded[k.month, default: [:]][k] = v
                    }
                    let touchedMonths = Set(perMonthRemoved.keys).union(perMonthAdded.keys)
                    for m in touchedMonths {
                        var removed = perMonthRemoved[m] ?? []
                        var added = perMonthAdded[m] ?? [:]
                        let intersect = removed.intersection(added.keys)
                        for k in intersect where added[k] == linksByMonth[m]?[k] {
                            removed.remove(k)
                            added.removeValue(forKey: k)
                        }
                        perMonthRemoved[m] = removed
                        perMonthAdded[m] = added
                        if !removed.isEmpty || !added.isEmpty {
                            changedMonths.insert(m)
                        }
                    }
                }
            }

            for m in changedMonths {
                let assetUpsertForMonth = (m == assetMonth && assetChanged) ? asset : nil
                applyMonthLinkDeltaLocked(
                    m,
                    removedLinkKeys: perMonthRemoved[m] ?? [],
                    addedLinks: perMonthAdded[m] ?? [:],
                    assetUpsert: assetUpsertForMonth,
                    bumpRevision: false
                )
            }

            if !changedMonths.isEmpty {
                bumpRevisionLocked(changedMonths)
            }
        }
    }

    private func changedMonthsLocked(since baseRevision: UInt64?) -> (Bool, Set<LibraryMonthKey>) {
        if freshlyReset {
            freshlyReset = false
            return (true, allKnownMonthsLocked())
        }

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

    /// Includes resource-only and link-only residue months that `monthSummaries()`
    /// (asset-keyed) skips — verify needs to reach those too.
    func allKnownMonths() -> Set<LibraryMonthKey> {
        lock.withLock { allKnownMonthsLocked() }
    }

    func counts() -> RemoteIndexSyncDigest {
        lock.withLock {
            let resources = resourcesByMonth.values.reduce(0) { $0 + $1.count }
            let assets = assetsByMonth.values.reduce(0) { $0 + $1.count }
            let links = linksByMonth.values.reduce(0) { $0 + $1.count }
            return RemoteIndexSyncDigest(
                resourceCount: resources,
                assetCount: assets,
                linkCount: links
            )
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

    func fileNames(for month: LibraryMonthKey) -> Set<String> {
        lock.withLock {
            Set((resourcesByMonth[month] ?? [:]).values.map(\.fileName))
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

        return RemoteLibrarySnapshot(
            resources: resources,
            assets: assets,
            assetResourceLinks: links
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

    // Callers' SQL PK should make duplicates unreachable; defensive first-seen + assert.
    private static func dedupedByKey<Element, Key: Hashable>(
        _ elements: [Element],
        key: (Element) -> Key,
        scope: String,
        month: LibraryMonthKey,
        describe: (Element) -> String
    ) -> [Key: Element] {
        var result: [Key: Element] = [:]
        result.reserveCapacity(elements.count)
        for element in elements {
            let k = key(element)
            if result[k] != nil {
                let descr = describe(element)
                snapshotCacheLog.error("[RemoteLibrarySnapshotCache] duplicate \(scope, privacy: .public) month=\(month.text, privacy: .public) entry=\(descr, privacy: .public)")
                assertionFailure("Duplicate \(scope) key in replaceMonth — SQL invariant broken")
                continue
            }
            result[k] = element
        }
        return result
    }
}

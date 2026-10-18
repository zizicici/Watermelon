import Foundation
import os.log

private let snapshotCacheLog = Logger(subsystem: "com.zizicici.watermelon", category: "RemoteLibrarySnapshotCache")

final class RemoteLibrarySnapshotCache: @unchecked Sendable {
    let changes = ChangeSignal<Void>()
    let persistedSnapshotInvalidations = ChangeSignal<String>()
    private struct LinkKey: Hashable, Sendable {
        let assetFingerprint: Data
        let role: Int
        let slot: Int
    }

    private typealias ResourceMap = [String: RemoteManifestResource]
    private typealias AssetMap = [Data: RemoteManifestAsset]
    private typealias LinkMap = [LinkKey: RemoteAssetResourceLink]

    private enum Update: Sendable {
        case month(LibraryMonthKey, PreparedMonth, remoteChanged: Bool)
        case reset
        case uncertain

        var affectsRemote: Bool {
            if case .month(_, _, let remoteChanged) = self { return remoteChanged }
            return true
        }
    }

    private static let committedUpdates = ChangeSignal<(UUID, String, Update)>()
    private let identity = UUID()
    private var committedUpdateObserverID: UUID?

    init() {
        committedUpdateObserverID = Self.committedUpdates.addObserver { [weak self] origin, key, update in
            guard let self, origin != self.identity, self.currentProfileKey() == key else { return }
            _ = self.receive(update, profileKey: key)
        }
    }

    deinit {
        if let committedUpdateObserverID { Self.committedUpdates.removeObserver(committedUpdateObserverID) }
    }

    private let lock = NSLock()
    private var resourcesByMonth: [LibraryMonthKey: ResourceMap] = [:]
    private var assetsByMonth: [LibraryMonthKey: AssetMap] = [:]
    private var linksByMonth: [LibraryMonthKey: LinkMap] = [:]

    private var revision: UInt64 = 0
    private var mutationGeneration: UInt64 = 0
    private var monthLastChangedRevision: [LibraryMonthKey: UInt64] = [:]
    private var healthDigestGeneration: UInt64 = 0

    private var resourceHashesByMonth: [LibraryMonthKey: Set<Data>] = [:]
    private var resourceBytesByMonth: [LibraryMonthKey: Int64] = [:]
    private var lastSyncedAt: Date?
    private var cachedHealthDigest: CachedHealthDigest?
    // Profile whose data currently populates this cache; travels with the snapshot so readers can detect a
    // mid-switch mismatch (cache reset to a new profile before the session activates it).
    private var profileKey: String?
    private var fullSnapshotRevision: UInt64 = 0
    private var needsReconciliation = false
    private var refreshPending = false
    private var contextGeneration: UInt64 = 0
    private var sourceGeneration: UInt64 = 0
    private var invalidationGeneration: UInt64 = 0
    private var monthSourceGenerations: [LibraryMonthKey: UInt64] = [:]
    private var manifestDigests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]

    var requiresReconciliation: Bool { lock.withLock { needsReconciliation || refreshPending } }

    struct Scan: Sendable {
        let profileKey: String
        let contextGeneration: UInt64
        let sourceGeneration: UInt64
        let digests: [LibraryMonthKey: RemoteMonthManifestDigest]
        let requiresReconciliation: Bool
        let isNewContext: Bool
    }

    func beginScan(profileKey key: String, allowContextSwitch: Bool) throws -> Scan {
        try withMutation {
            if !allowContextSwitch, let owner = profileKey, owner != key {
                throw NSError(domain: "RemoteIndexSyncService", code: -40)
            }
            let changed = profileKey != key
            if changed {
                resetLocked()
                profileKey = key
            }
            return Scan(profileKey: key, contextGeneration: contextGeneration,
                sourceGeneration: sourceGeneration, digests: manifestDigests,
                requiresReconciliation: needsReconciliation, isNewContext: changed)
        }
    }

    func claimIfUnowned(profileKey key: String) -> Bool {
        withMutation {
            if let owner = profileKey { return owner == key }
            profileKey = key
            contextGeneration &+= 1
            revision &+= 1
            fullSnapshotRevision = revision
            mutationGeneration &+= 1
            return true
        }
    }

    private func accepts(_ scan: Scan, month: LibraryMonthKey) -> Bool {
        profileKey == scan.profileKey && contextGeneration == scan.contextGeneration
            && invalidationGeneration <= scan.sourceGeneration
            && (monthSourceGenerations[month] ?? 0) <= scan.sourceGeneration
    }

    @discardableResult
    func applyScannedMonth(_ delta: RemoteLibraryMonthDelta, scan: Scan) -> Bool {
        let prepared = prepareMonth(delta.month, resources: delta.resources, assets: delta.assets,
            links: delta.assetResourceLinks)
        return withMutation {
            guard accepts(scan, month: delta.month) else { return false }
            return replaceMonthLocked(delta.month, prepared: prepared)
        }
    }

    func finishScan(_ scan: Scan, digests: [LibraryMonthKey: RemoteMonthManifestDigest],
                    monthFilter: Set<LibraryMonthKey>?) throws {
        try withMutation {
            guard profileKey == scan.profileKey, contextGeneration == scan.contextGeneration else {
                throw CancellationError()
            }
            let months = monthFilter ?? allKnownMonthsLocked().union(manifestDigests.keys).union(digests.keys)
            var conflicted = invalidationGeneration > scan.sourceGeneration
            for month in months {
                guard accepts(scan, month: month) else { conflicted = true; continue }
                manifestDigests[month] = digests[month]
                if digests[month] == nil {
                    _ = replaceMonthLocked(month, prepared: .empty)
                }
            }
            if conflicted {
                if !refreshPending { refreshPending = true; mutationGeneration &+= 1 }
            } else {
                if needsReconciliation, monthFilter == nil {
                    needsReconciliation = false
                    revision &+= 1
                    fullSnapshotRevision = revision
                }
                if monthFilter == nil { refreshPending = false }
                lastSyncedAt = Date()
                healthDigestGeneration &+= 1
                mutationGeneration &+= 1
            }
        }
    }

    func snapshotObserver(profileKey key: String) -> MonthManifestStore.SnapshotObserver {
        { [self] event in
            switch event {
            case .confirmed(let delta, let remoteChanged):
                let prepared = prepareMonth(delta.month, resources: delta.resources, assets: delta.assets,
                    links: delta.assetResourceLinks)
                _ = publish(.month(delta.month, prepared, remoteChanged: remoteChanged), profileKey: key)
            case .removed(let month):
                removeConfirmedMonth(month, profileKey: key)
            case .uncertain:
                invalidateIfOwned(by: key)
            }
        }
    }

    func removeConfirmedMonth(_ month: LibraryMonthKey, profileKey key: String) {
        _ = publish(.month(month, .empty, remoteChanged: true), profileKey: key)
    }

    private func publish(_ update: Update, profileKey key: String) -> Bool {
        let accepted = receive(update, profileKey: key)
        if update.affectsRemote { Self.committedUpdates.publish((identity, key, update)) }
        return accepted
    }

    private func receive(_ update: Update, profileKey key: String) -> Bool {
        defer {
            if update.affectsRemote { persistedSnapshotInvalidations.publish(key) }
        }
        return withMutation {
            guard isOwnedLocked(by: key) else { return false }
            switch update {
            case .month(let month, let prepared, let remoteChanged):
                sourceGeneration &+= 1
                monthSourceGenerations[month] = sourceGeneration
                if remoteChanged { manifestDigests[month] = nil }
                _ = replaceMonthLocked(month, prepared: prepared)
            case .reset:
                resetLocked()
                profileKey = key
            case .uncertain:
                sourceGeneration &+= 1
                invalidationGeneration = sourceGeneration
                if !needsReconciliation {
                    needsReconciliation = true
                    revision &+= 1
                    fullSnapshotRevision = revision
                    mutationGeneration &+= 1
                }
            }
            return true
        }
    }

    func scannedSnapshot(for scan: Scan) -> (digests: [LibraryMonthKey: RemoteMonthManifestDigest], months: [RemoteLibraryMonthDelta])? {
        let capture = lock.withLock { () -> ([LibraryMonthKey: RemoteMonthManifestDigest], [LibraryMonthKey: ResourceMap], [LibraryMonthKey: AssetMap], [LibraryMonthKey: LinkMap])? in
            guard profileKey == scan.profileKey, contextGeneration == scan.contextGeneration,
                  sourceGeneration == scan.sourceGeneration, !needsReconciliation, !refreshPending else { return nil }
            return (manifestDigests, resourcesByMonth, assetsByMonth, linksByMonth)
        }
        guard let (digests, resources, assets, links) = capture else { return nil }
        return (digests, digests.keys.sorted().map { month in
            RemoteLibraryMonthDelta(month: month, resources: Array((resources[month] ?? [:]).values),
                assets: Array((assets[month] ?? [:]).values), assetResourceLinks: Array((links[month] ?? [:]).values))
        })
    }

    func isCurrent(_ scan: Scan) -> Bool {
        lock.withLock {
            profileKey == scan.profileKey && contextGeneration == scan.contextGeneration
                && sourceGeneration == scan.sourceGeneration
        }
    }

    private struct CachedHealthDigest {
        let generation: UInt64
        let digest: RemoteHealthDigest
    }

    func snapshotForSeed(profileKey key: String, entryLimit: Int) -> RemoteLibrarySnapshot? {
        let snapshot = lock.withLock { () -> ([LibraryMonthKey: ResourceMap], [LibraryMonthKey: AssetMap], [LibraryMonthKey: LinkMap])? in
            guard profileKey == key, !needsReconciliation, !refreshPending else { return nil }
            let entries = resourcesByMonth.values.reduce(0) { $0 + $1.count }
                + assetsByMonth.values.reduce(0) { $0 + $1.count }
                + linksByMonth.values.reduce(0) { $0 + $1.count }
            guard entries <= entryLimit else { return nil }
            return (resourcesByMonth, assetsByMonth, linksByMonth)
        }
        guard let (resources, assets, links) = snapshot else { return nil }
        return RemoteLibrarySnapshot(resources: resources.values.flatMap { $0.values },
            assets: assets.values.flatMap { $0.values }, assetResourceLinks: links.values.flatMap { $0.values })
    }

    #if DEBUG
    func current() -> RemoteLibrarySnapshot {
        let snapshot = lock.withLock { (resourcesByMonth, assetsByMonth, linksByMonth) }
        let months = Set(snapshot.0.keys).union(snapshot.1.keys).union(snapshot.2.keys).sorted()
        return RemoteLibrarySnapshot(
            resources: months.flatMap { Self.sortedResources(Array((snapshot.0[$0] ?? [:]).values)) },
            assets: months.flatMap { Self.sortedAssets(Array((snapshot.1[$0] ?? [:]).values)) },
            assetResourceLinks: months.flatMap { Self.sortedLinks(Array((snapshot.2[$0] ?? [:]).values)) })
    }
    #endif

    func state(since baseRevision: UInt64?) -> RemoteLibrarySnapshotState {
        let capture = lock.withLock {
            let (isFullSnapshot, changedMonths) = changedMonthsLocked(since: baseRevision)
            return (isFullSnapshot: isFullSnapshot, changedMonths: changedMonths, revision: revision,
                profileKey: profileKey, isAuthoritative: !needsReconciliation,
                resources: resourcesByMonth, assets: assetsByMonth, links: linksByMonth)
        }
        let monthDeltas = capture.changedMonths.sorted().map { month in
            RemoteLibraryMonthDelta(month: month,
                resources: Array((capture.resources[month] ?? [:]).values),
                assets: Array((capture.assets[month] ?? [:]).values),
                assetResourceLinks: Array((capture.links[month] ?? [:]).values))
        }
        return RemoteLibrarySnapshotState(revision: capture.revision, isFullSnapshot: capture.isFullSnapshot,
            monthDeltas: monthDeltas, profileKey: capture.profileKey, isAuthoritative: capture.isAuthoritative)
    }

    #if DEBUG
    func setProfileKey(_ key: String) {
        withMutation {
            guard profileKey != key else { return }
            profileKey = key
            contextGeneration &+= 1
            manifestDigests.removeAll()
            revision &+= 1
            fullSnapshotRevision = revision
            mutationGeneration &+= 1
        }
    }
    #endif

    func currentProfileKey() -> String? {
        lock.withLock { profileKey }
    }

    // A late commit must not change a cache now owned by another profile.
    private func isOwnedLocked(by expectedProfileKey: String?) -> Bool {
        guard let expectedProfileKey else { return true }
        if profileKey == expectedProfileKey { return true }
        snapshotCacheLog.info("[RemoteLibrarySnapshotCache] dropped foreign-owner write (cache no longer belongs to the acting profile)")
        return false
    }

    #if DEBUG
    func reset() {
        withMutation { resetLocked() }
    }
    #endif

    // A confirmed-empty repo still belongs to the connected profile.
    @discardableResult
    func resetIfOwned(by expectedProfileKey: String) -> Bool {
        publish(.reset, profileKey: expectedProfileKey)
    }

    private func resetLocked() {
        contextGeneration &+= 1
        sourceGeneration &+= 1
        invalidationGeneration = sourceGeneration
        monthSourceGenerations.removeAll()
        manifestDigests.removeAll()
        refreshPending = false
        mutationGeneration &+= 1
        resourcesByMonth.removeAll()
        assetsByMonth.removeAll()
        linksByMonth.removeAll()
        resourceHashesByMonth.removeAll()
        resourceBytesByMonth.removeAll()
        lastSyncedAt = nil
        cachedHealthDigest = nil
        healthDigestGeneration &+= 1
        revision &+= 1
        fullSnapshotRevision = revision
        monthLastChangedRevision.removeAll()
        profileKey = nil
        needsReconciliation = false
    }

    func invalidateIfOwned(by expectedProfileKey: String) {
        _ = publish(.uncertain, profileKey: expectedProfileKey)
    }

    #if DEBUG
    func markSynced(_ at: Date, fullyReconciled: Bool = true) {
        withMutation {
            if needsReconciliation, fullyReconciled {
                needsReconciliation = false
                revision &+= 1
                fullSnapshotRevision = revision
                mutationGeneration &+= 1
            }
            guard lastSyncedAt != at else { return }
            lastSyncedAt = at
            healthDigestGeneration &+= 1
            mutationGeneration &+= 1
        }
    }

    func currentLastSyncedAt() -> Date? {
        lock.withLock { lastSyncedAt }
    }
    #endif

    /// Snapshots per-month dicts under the lock (cheap COW reference copy), then
    /// runs the SHA-256 fingerprint recompute outside the lock so writers aren't
    /// stalled by the (potentially large) incomplete-asset scan.
    func healthDigest() -> RemoteHealthDigest {
        let capture: (cached: RemoteHealthDigest?, snapshot: HealthSnapshot?) = lock.withLock {
            if let cachedHealthDigest,
               cachedHealthDigest.generation == healthDigestGeneration {
                return (cachedHealthDigest.digest, nil)
            }
            return (nil, HealthSnapshot(
                generation: healthDigestGeneration,
                assetsByMonth: assetsByMonth,
                linksByMonth: linksByMonth,
                resourcesByMonth: resourcesByMonth,
                resourceHashesByMonth: resourceHashesByMonth,
                resourceBytesByMonth: resourceBytesByMonth,
                lastSyncedAt: lastSyncedAt
            ))
        }
        if let cached = capture.cached { return cached }
        guard let snapshot = capture.snapshot else { preconditionFailure() }

        let assetCount = snapshot.assetsByMonth.values.reduce(0) { $0 + $1.count }
        let resourceCount = snapshot.resourcesByMonth.values.reduce(0) { $0 + $1.count }
        let totalBytes = snapshot.resourceBytesByMonth.values.reduce(Int64(0), +)

        var incompleteFlat: [IncompleteAssetEntry] = []
        for month in snapshot.assetsByMonth.keys {
            incompleteFlat.append(contentsOf: Self.computeIncompleteForMonth(month, snapshot: snapshot))
        }
        let digest = RemoteHealthDigest(
            totalAssets: assetCount,
            totalResources: resourceCount,
            totalSizeBytes: totalBytes,
            incompleteAssets: incompleteFlat,
            lastIndexSyncedAt: snapshot.lastSyncedAt
        )
        lock.withLock {
            if healthDigestGeneration == snapshot.generation {
                cachedHealthDigest = CachedHealthDigest(
                    generation: snapshot.generation,
                    digest: digest
                )
            }
        }
        return digest
    }

    private struct HealthSnapshot {
        let generation: UInt64
        let assetsByMonth: [LibraryMonthKey: AssetMap]
        let linksByMonth: [LibraryMonthKey: LinkMap]
        let resourcesByMonth: [LibraryMonthKey: ResourceMap]
        let resourceHashesByMonth: [LibraryMonthKey: Set<Data>]
        let resourceBytesByMonth: [LibraryMonthKey: Int64]
        let lastSyncedAt: Date?
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

    private struct PreparedMonth: Sendable {
        let resources: ResourceMap
        let assets: AssetMap
        let links: LinkMap
        let hashes: Set<Data>
        let bytes: Int64
        static var empty: Self { Self(resources: [:], assets: [:], links: [:], hashes: [], bytes: 0) }
    }

    private func prepareMonth(_ month: LibraryMonthKey, resources: [RemoteManifestResource],
                              assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) -> PreparedMonth {
        let resourceMap = Self.dedupedByKey(resources, key: \.fileName, scope: "resource", month: month) { $0.fileName }
        let assetMap = Self.dedupedByKey(assets, key: \.assetFingerprint, scope: "asset", month: month) { $0.assetFingerprintHex }
        let linkMap = Self.dedupedByKey(links, key: { link in
            LinkKey(assetFingerprint: link.assetFingerprint, role: link.role, slot: link.slot)
        }, scope: "link", month: month) { "fp=\($0.assetFingerprint.hexString) r=\($0.role) s=\($0.slot)" }
        return PreparedMonth(resources: resourceMap, assets: assetMap, links: linkMap,
            hashes: Set(resourceMap.values.map(\.contentHash)), bytes: resourceMap.values.reduce(0) { $0 + $1.fileSize })
    }

    #if DEBUG
    @discardableResult
    func replaceMonth(_ month: LibraryMonthKey, resources: [RemoteManifestResource], assets: [RemoteManifestAsset],
                      assetResourceLinks: [RemoteAssetResourceLink], onlyIfOwnedBy expectedProfileKey: String? = nil) -> Bool {
        let prepared = prepareMonth(month, resources: resources, assets: assets, links: assetResourceLinks)
        return withMutation {
            guard isOwnedLocked(by: expectedProfileKey) else { return false }
            return replaceMonthLocked(month, prepared: prepared)
        }
    }

    @discardableResult
    func removeMonth(_ month: LibraryMonthKey) -> Bool {
        withMutation { replaceMonthLocked(month, prepared: .empty) }
    }
    #endif

    private func replaceMonthLocked(_ month: LibraryMonthKey, prepared: PreparedMonth) -> Bool {
        let resourcesChanged = (resourcesByMonth[month] ?? [:]) != prepared.resources
        let assetsChanged = (assetsByMonth[month] ?? [:]) != prepared.assets
        let linksChanged = (linksByMonth[month] ?? [:]) != prepared.links
        guard resourcesChanged || assetsChanged || linksChanged else { return false }
        if resourcesChanged {
            resourcesByMonth[month] = prepared.resources.isEmpty ? nil : prepared.resources
            resourceHashesByMonth[month] = prepared.resources.isEmpty ? nil : prepared.hashes
            resourceBytesByMonth[month] = prepared.resources.isEmpty ? nil : prepared.bytes
        }
        if assetsChanged { assetsByMonth[month] = prepared.assets.isEmpty ? nil : prepared.assets }
        if linksChanged { linksByMonth[month] = prepared.links.isEmpty ? nil : prepared.links }
        revision &+= 1
        mutationGeneration &+= 1
        healthDigestGeneration &+= 1
        monthLastChangedRevision[month] = revision
        return true
    }

    private func withMutation<T>(_ body: () throws -> T) rethrows -> T {
        let (result, changed) = try lock.withLock {
            let previous = mutationGeneration
            let result = try body()
            return (result, mutationGeneration != previous)
        }
        if changed {
            changes.publish()
        }
        return result
    }

    private func changedMonthsLocked(since baseRevision: UInt64?) -> (Bool, Set<LibraryMonthKey>) {
        guard let baseRevision, baseRevision >= fullSnapshotRevision else {
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

    private func allKnownMonthsLocked() -> Set<LibraryMonthKey> {
        Set(resourcesByMonth.keys)
            .union(assetsByMonth.keys)
            .union(linksByMonth.keys)
    }

    /// Includes resource-only and link-only residue months that the asset-keyed resolved view
    /// skips — verify needs to reach those too.
    func allKnownMonths() -> Set<LibraryMonthKey> {
        lock.withLock { allKnownMonthsLocked() }
    }

    func currentRevision() -> UInt64 {
        lock.withLock { revision }
    }

    /// Point probe: is this fingerprint recorded in any cached month right now. Lets delete gates re-verify
    /// an absence read from a stale committed presence build against the live cache.
    func containsAssetFingerprint(_ fingerprint: Data) -> (contains: Bool, profileKey: String?) {
        lock.withLock {
            let contains = assetsByMonth.values.contains { $0[fingerprint] != nil }
            return (contains, needsReconciliation ? nil : profileKey)
        }
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
        let snapshot = lock.withLock {
            (resourcesByMonth[month] ?? [:], assetsByMonth[month] ?? [:], linksByMonth[month] ?? [:])
        }
        guard !snapshot.1.isEmpty else { return nil }
        return RemoteLibraryMonthDelta(month: month, resources: Array(snapshot.0.values),
            assets: Array(snapshot.1.values), assetResourceLinks: Array(snapshot.2.values))
    }

    #if DEBUG
    private static func sortedResources(_ resources: [RemoteManifestResource]) -> [RemoteManifestResource] {
        resources.sorted { lhs, rhs in
            let lhsCreationDateMs = LibraryCreationDate.normalized(milliseconds: lhs.creationDateMs).milliseconds
            let rhsCreationDateMs = LibraryCreationDate.normalized(milliseconds: rhs.creationDateMs).milliseconds
            if lhsCreationDateMs != rhsCreationDateMs {
                return lhsCreationDateMs < rhsCreationDateMs
            }
            return lhs.fileName < rhs.fileName
        }
    }

    private static func sortedAssets(_ assets: [RemoteManifestAsset]) -> [RemoteManifestAsset] {
        assets.sorted { lhs, rhs in
            let lhsCreationDateMs = LibraryCreationDate.normalized(milliseconds: lhs.creationDateMs).milliseconds
            let rhsCreationDateMs = LibraryCreationDate.normalized(milliseconds: rhs.creationDateMs).milliseconds
            if lhsCreationDateMs != rhsCreationDateMs {
                return lhsCreationDateMs < rhsCreationDateMs
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
    #endif

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

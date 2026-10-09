import Foundation
import Photos

// One browsable remote asset, projected from the remote manifest snapshot.
struct RemoteBrowserAsset: Hashable, Sendable {
    let fingerprint: Data
    let month: LibraryMonthKey
    let displayMonth: LibraryMonthKey
    let creationDateMs: Int64
    let isVideo: Bool
    let isLivePhoto: Bool
    let photoRemoteRelativePath: String?
    let videoRemoteRelativePath: String?
    // Manifest-recorded content hashes for the display resources (nil for a legacy no-hash manifest) —
    // lets materialization verify downloaded bytes before persisting them under the fingerprint.
    let photoContentHash: Data?
    let videoContentHash: Data?
    // The manifest record is incomplete: only the resolvable subset can be downloaded, producing a new,
    // differently-fingerprinted asset. Shown (marked), not hidden — the user decides at download time.
    let isIncomplete: Bool

    var fingerprintHex: String { fingerprint.hexString }
}

struct RemoteBrowserProjection: Sendable {
    let revision: UInt64
    let ownerProfileKey: String?
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference
    let months: [LibraryMonthKey]
    let assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]]
    let remoteFingerprints: Set<Data>
    let backedUpFingerprints: Set<Data>
    let completeFingerprints: Set<Data>
    let deviceHandles: [Data: String]?

    func attachingDeviceHandles(_ handles: [Data: String]) -> RemoteBrowserProjection {
        RemoteBrowserProjection(
            revision: revision,
            ownerProfileKey: ownerProfileKey,
            monthGroupingTimeZone: monthGroupingTimeZone,
            months: months,
            assetsByMonth: assetsByMonth,
            remoteFingerprints: remoteFingerprints,
            backedUpFingerprints: backedUpFingerprints,
            completeFingerprints: completeFingerprints,
            deviceHandles: handles
        )
    }

    func attachingPresenceFacts(
        remoteFingerprints: Set<Data>,
        backedUpFingerprints: Set<Data>,
        completeFingerprints: Set<Data>
    ) -> RemoteBrowserProjection {
        RemoteBrowserProjection(
            revision: revision,
            ownerProfileKey: ownerProfileKey,
            monthGroupingTimeZone: monthGroupingTimeZone,
            months: months,
            assetsByMonth: assetsByMonth,
            remoteFingerprints: remoteFingerprints,
            backedUpFingerprints: backedUpFingerprints,
            completeFingerprints: completeFingerprints,
            deviceHandles: deviceHandles
        )
    }
}

struct RemoteBrowserProjectionMetrics: Sendable {
    let workerCount: Int
    let resourceCount: Int
    let linkCount: Int
    let resourceMapMs: Double
    let linkGroupMs: Double
    let assetProjectMs: Double
    let sortMs: Double
    let sortPerformedMonths: Int
    let sortSkippedMonths: Int
}

// Projects a remote snapshot into per-month, date-sorted browser assets. Mirrors the resolver drop
// rule (assets without a resolvable link are skipped) and the display-resource priority used elsewhere.
enum RemoteBrowserAssetBuilder {
    static func build(
        from state: RemoteLibrarySnapshotState,
        shouldCancel: () -> Bool = { false }
    ) -> (months: [LibraryMonthKey], assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]]) {
        guard let projection = buildProjection(
            from: state,
            includeBrowserAssets: true,
            collectPresence: false,
            shouldCancel: shouldCancel
        ) else {
            return ([], [:])
        }
        return (projection.months, projection.assetsByMonth)
    }

    static func buildProjection(
        from state: RemoteLibrarySnapshotState,
        includeBrowserAssets: Bool,
        collectPresence: Bool,
        monthGroupingTimeZone: MonthGroupingTimeZonePreference = .frozenCurrent(),
        shouldCancel: () -> Bool = { false },
        onMetrics: ((RemoteBrowserProjectionMetrics) -> Void)? = nil
    ) -> RemoteBrowserProjection? {
        var assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]] = [:]
        assetsByMonth.reserveCapacity(state.monthDeltas.count)
        var remoteFingerprints = Set<Data>()
        var backedUpFingerprints = Set<Data>()
        var completeFingerprints = Set<Data>()
        if collectPresence {
            let assetCount = state.monthDeltas.reduce(0) { $0 + $1.assets.count }
            remoteFingerprints.reserveCapacity(assetCount)
            backedUpFingerprints.reserveCapacity(assetCount)
            completeFingerprints.reserveCapacity(assetCount)
        }
        var resourceCount = 0
        var linkCount = 0
        var resourceMapMs = 0.0
        var linkGroupMs = 0.0
        var assetProjectMs = 0.0
        var sortMs = 0.0
        var sortPerformedMonths = 0
        var sortSkippedMonths = 0
        var displayMonthMemo = MediaBrowserMonthMemo(
            calendar: LibraryMonthKey.monthCalendar(preference: monthGroupingTimeZone)
        )
        for delta in state.monthDeltas {
            guard !shouldCancel() else { return nil }
            let resourceMapStartedAt = CFAbsoluteTimeGetCurrent()
            var resourceByHash: [Data: RemoteManifestResource] = [:]
            resourceByHash.reserveCapacity(delta.resources.count)
            for resource in delta.resources where resourceByHash[resource.contentHash] == nil {
                resourceByHash[resource.contentHash] = resource
            }
            resourceCount += delta.resources.count
            resourceMapMs += (CFAbsoluteTimeGetCurrent() - resourceMapStartedAt) * 1_000
            let linkGroupStartedAt = CFAbsoluteTimeGetCurrent()
            var linksByFingerprint: [Data: [RemoteAssetResourceLink]] = [:]
            linksByFingerprint.reserveCapacity(delta.assets.count)
            for link in delta.assetResourceLinks {
                guard !shouldCancel() else { return nil }
                linksByFingerprint[link.assetFingerprint, default: []].append(link)
            }
            linkCount += delta.assetResourceLinks.count
            linkGroupMs += (CFAbsoluteTimeGetCurrent() - linkGroupStartedAt) * 1_000
            let assetProjectStartedAt = CFAbsoluteTimeGetCurrent()
            var items: [RemoteBrowserAsset] = []
            if includeBrowserAssets { items.reserveCapacity(delta.assets.count) }
            var previousItem: RemoteBrowserAsset?
            var itemsNeedSorting = false
            for asset in delta.assets {
                guard !shouldCancel() else { return nil }
                let allLinks = linksByFingerprint[asset.assetFingerprint] ?? []
                let analysis = analyzeLinks(
                    links: allLinks,
                    resourceByHash: resourceByHash,
                    assetFingerprint: asset.assetFingerprint
                )
                if collectPresence {
                    remoteFingerprints.insert(asset.assetFingerprint)
                    if analysis.hasBackedUpMedia { backedUpFingerprints.insert(asset.assetFingerprint) }
                    if analysis.hasBackedUpMedia && !analysis.isIncomplete {
                        completeFingerprints.insert(asset.assetFingerprint)
                    }
                }
                guard includeBrowserAssets, analysis.hasBackedUpMedia else { continue }
                // Show meaningful records (complete OR partial-but-has-media), flagged when incomplete so the
                // user is asked to confirm at download time. Drop the meaningless ones — a phantom (no resolvable
                // link) or a config-only record (only an adjustment sidecar resolves) has no photo/video to show
                // and isn't a real backup; the future "incomplete resources" entry will own those.
                let creationDate = LibraryCreationDate.normalized(
                    milliseconds: asset.creationDateMs
                )
                let item = makeAsset(
                    asset: asset,
                    analysis: analysis,
                    storageMonth: delta.month,
                    displayMonth: displayMonthMemo.month(for: creationDate.date),
                    creationDateMs: creationDate.milliseconds
                )
                if let previousItem, !precedesForDisplay(previousItem, item) {
                    itemsNeedSorting = true
                }
                previousItem = item
                items.append(item)
            }
            assetProjectMs += (CFAbsoluteTimeGetCurrent() - assetProjectStartedAt) * 1_000
            if includeBrowserAssets {
                let sortStartedAt = CFAbsoluteTimeGetCurrent()
                if itemsNeedSorting {
                    items.sort(by: precedesForDisplay)
                    sortPerformedMonths += 1
                } else {
                    sortSkippedMonths += 1
                }
                if !items.isEmpty { assetsByMonth[delta.month] = items }
                sortMs += (CFAbsoluteTimeGetCurrent() - sortStartedAt) * 1_000
            }
        }
        let monthSortStartedAt = CFAbsoluteTimeGetCurrent()
        let months = assetsByMonth.keys.sorted(by: >)
        sortMs += (CFAbsoluteTimeGetCurrent() - monthSortStartedAt) * 1_000
        onMetrics?(RemoteBrowserProjectionMetrics(
            workerCount: 1,
            resourceCount: resourceCount,
            linkCount: linkCount,
            resourceMapMs: resourceMapMs,
            linkGroupMs: linkGroupMs,
            assetProjectMs: assetProjectMs,
            sortMs: sortMs,
            sortPerformedMonths: sortPerformedMonths,
            sortSkippedMonths: sortSkippedMonths
        ))
        return RemoteBrowserProjection(
            revision: state.revision,
            ownerProfileKey: state.profileKey,
            monthGroupingTimeZone: monthGroupingTimeZone,
            months: months,
            assetsByMonth: assetsByMonth,
            remoteFingerprints: remoteFingerprints,
            backedUpFingerprints: backedUpFingerprints,
            completeFingerprints: completeFingerprints,
            deviceHandles: nil
        )
    }

    static func buildProjectionConcurrently(
        from state: RemoteLibrarySnapshotState,
        includeBrowserAssets: Bool,
        collectPresence: Bool,
        maximumWorkerCount: Int = 4,
        monthGroupingTimeZone: MonthGroupingTimeZonePreference = .frozenCurrent(),
        shouldCancel: @escaping @Sendable () -> Bool = { Task.isCancelled },
        onMetrics: ((RemoteBrowserProjectionMetrics) -> Void)? = nil
    ) async -> RemoteBrowserProjection? {
        let workerCount = min(maximumWorkerCount, state.monthDeltas.count)
        let distinctMonths = Set(state.monthDeltas.map(\.month)).count == state.monthDeltas.count
        guard workerCount > 1, distinctMonths else {
            return buildProjection(
                from: state,
                includeBrowserAssets: includeBrowserAssets,
                collectPresence: collectPresence,
                monthGroupingTimeZone: monthGroupingTimeZone,
                shouldCancel: shouldCancel,
                onMetrics: onMetrics
            )
        }

        let chunks = balancedChunks(state.monthDeltas, count: workerCount)
        let aggregated = await withTaskGroup(
            of: (RemoteBrowserProjection, RemoteBrowserProjectionMetrics)?.self,
            returning: (RemoteBrowserProjection, RemoteBrowserProjectionMetrics)?.self
        ) { group in
            for chunk in chunks {
                group.addTask {
                    guard !Task.isCancelled, !shouldCancel() else { return nil }
                    var metrics: RemoteBrowserProjectionMetrics?
                    let projection = buildProjection(
                        from: RemoteLibrarySnapshotState(
                            revision: state.revision,
                            isFullSnapshot: state.isFullSnapshot,
                            monthDeltas: chunk,
                            profileKey: state.profileKey
                        ),
                        includeBrowserAssets: includeBrowserAssets,
                        collectPresence: collectPresence,
                        monthGroupingTimeZone: monthGroupingTimeZone,
                        shouldCancel: { Task.isCancelled || shouldCancel() },
                        onMetrics: { metrics = $0 }
                    )
                    guard let projection, let metrics else { return nil }
                    return (projection, metrics)
                }
            }

            var assetsByMonth: [LibraryMonthKey: [RemoteBrowserAsset]] = [:]
            assetsByMonth.reserveCapacity(state.monthDeltas.count)
            var remoteFingerprints = Set<Data>()
            var backedUpFingerprints = Set<Data>()
            var completeFingerprints = Set<Data>()
            var resourceCount = 0
            var linkCount = 0
            var resourceMapMs = 0.0
            var linkGroupMs = 0.0
            var assetProjectMs = 0.0
            var sortMs = 0.0
            var sortPerformedMonths = 0
            var sortSkippedMonths = 0
            var valid = true
            for await value in group {
                guard let (projection, metrics) = value else {
                    valid = false
                    group.cancelAll()
                    continue
                }
                for (month, assets) in projection.assetsByMonth {
                    assetsByMonth[month] = assets
                }
                remoteFingerprints.formUnion(projection.remoteFingerprints)
                backedUpFingerprints.formUnion(projection.backedUpFingerprints)
                completeFingerprints.formUnion(projection.completeFingerprints)
                resourceCount += metrics.resourceCount
                linkCount += metrics.linkCount
                resourceMapMs += metrics.resourceMapMs
                linkGroupMs += metrics.linkGroupMs
                assetProjectMs += metrics.assetProjectMs
                sortMs += metrics.sortMs
                sortPerformedMonths += metrics.sortPerformedMonths
                sortSkippedMonths += metrics.sortSkippedMonths
            }
            guard valid, !Task.isCancelled, !shouldCancel() else { return nil }
            let months = assetsByMonth.keys.sorted(by: >)
            return (
                RemoteBrowserProjection(
                    revision: state.revision,
                    ownerProfileKey: state.profileKey,
                    monthGroupingTimeZone: monthGroupingTimeZone,
                    months: months,
                    assetsByMonth: assetsByMonth,
                    remoteFingerprints: remoteFingerprints,
                    backedUpFingerprints: backedUpFingerprints,
                    completeFingerprints: completeFingerprints,
                    deviceHandles: nil
                ),
                RemoteBrowserProjectionMetrics(
                    workerCount: workerCount,
                    resourceCount: resourceCount,
                    linkCount: linkCount,
                    resourceMapMs: resourceMapMs,
                    linkGroupMs: linkGroupMs,
                    assetProjectMs: assetProjectMs,
                    sortMs: sortMs,
                    sortPerformedMonths: sortPerformedMonths,
                    sortSkippedMonths: sortSkippedMonths
                )
            )
        }
        guard let aggregated, !Task.isCancelled, !shouldCancel() else { return nil }
        onMetrics?(aggregated.1)
        return aggregated.0
    }

    private static func balancedChunks(
        _ deltas: [RemoteLibraryMonthDelta],
        count: Int
    ) -> [[RemoteLibraryMonthDelta]] {
        let weighted = deltas.enumerated().sorted { lhs, rhs in
            workWeight(lhs.element) > workWeight(rhs.element)
        }
        var chunks = Array(repeating: [(index: Int, delta: RemoteLibraryMonthDelta)](), count: count)
        var weights = Array(repeating: 0, count: count)
        for entry in weighted {
            let target = weights.indices.min { weights[$0] < weights[$1] } ?? 0
            chunks[target].append((entry.offset, entry.element))
            weights[target] += workWeight(entry.element)
        }
        return chunks.map { chunk in
            chunk.sorted { $0.index < $1.index }.map(\.delta)
        }
    }

    private static func workWeight(_ delta: RemoteLibraryMonthDelta) -> Int {
        max(1, delta.resources.count + delta.assets.count + delta.assetResourceLinks.count)
    }

    private static func precedesForDisplay(
        _ lhs: RemoteBrowserAsset,
        _ rhs: RemoteBrowserAsset
    ) -> Bool {
        if lhs.creationDateMs != rhs.creationDateMs {
            return lhs.creationDateMs > rhs.creationDateMs
        }
        return lhs.fingerprint.lexicographicallyPrecedes(rhs.fingerprint)
    }

    private struct PreferredResource {
        var rank: ResourceRole.DisplaySelectionRank?
        var resource: RemoteManifestResource?

        mutating func consider(
            _ candidate: RemoteManifestResource,
            rank candidateRank: ResourceRole.DisplaySelectionRank
        ) {
            if let rank, candidateRank >= rank { return }
            rank = candidateRank
            resource = candidate
        }
    }

    private struct LinkAnalysis {
        let hasBackedUpMedia: Bool
        let isIncomplete: Bool
        let isLivePhoto: Bool
        let isVideo: Bool
        let photoResource: RemoteManifestResource?
        let videoResource: RemoteManifestResource?
    }

    private static func analyzeLinks(
        links: [RemoteAssetResourceLink],
        resourceByHash: [Data: RemoteManifestResource],
        assetFingerprint: Data
    ) -> LinkAnalysis {
        var allResourcesAvailable = !links.isEmpty
        var hasNonMetadata = false
        var hasPhoto = false
        var hasVideo = false
        var hasPairedVideo = false
        var photo = PreferredResource()
        var video = PreferredResource()

        for link in links {
            if !ResourceRole.isMetadataOnly(link.role) {
                hasNonMetadata = true
            }
            guard let resource = resourceByHash[link.resourceHash] else {
                allResourcesAvailable = false
                continue
            }
            if let rank = ResourceRole.displaySelectionRank(
                role: link.role,
                slot: link.slot,
                side: .photo
            ) {
                hasPhoto = true
                photo.consider(resource, rank: rank)
            }
            if let rank = ResourceRole.displaySelectionRank(
                role: link.role,
                slot: link.slot,
                side: .video
            ) {
                hasVideo = true
                hasPairedVideo = hasPairedVideo || ResourceRole.isPairedVideoSide(link.role)
                video.consider(resource, rank: rank)
            }
        }

        let fingerprintMatches = allResourcesAvailable && BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: links.lazy.map {
                (role: $0.role, slot: $0.slot, contentHash: $0.resourceHash)
            }
        ) == assetFingerprint
        let classification = ResourceRole.classify(
            hasPhotoSide: hasPhoto,
            hasVideoSide: hasVideo,
            hasPairedVideoSide: hasPairedVideo
        )
        return LinkAnalysis(
            hasBackedUpMedia: hasPhoto || hasVideo,
            isIncomplete: MonthManifestStore.isAssetIncomplete(
                hasLinks: !links.isEmpty,
                allResourcesAvailable: allResourcesAvailable,
                fingerprintMatches: fingerprintMatches,
                hasNonMetadata: hasNonMetadata
            ),
            isLivePhoto: classification.isLivePhoto,
            isVideo: classification.isVideo,
            photoResource: photo.resource,
            videoResource: video.resource
        )
    }

    private static func makeAsset(
        asset: RemoteManifestAsset,
        analysis: LinkAnalysis,
        storageMonth: LibraryMonthKey,
        displayMonth: LibraryMonthKey,
        creationDateMs: Int64
    ) -> RemoteBrowserAsset {
        return RemoteBrowserAsset(
            fingerprint: asset.assetFingerprint,
            month: storageMonth,
            displayMonth: displayMonth,
            creationDateMs: creationDateMs,
            isVideo: analysis.isVideo,
            isLivePhoto: analysis.isLivePhoto,
            photoRemoteRelativePath: analysis.photoResource?.remoteRelativePath,
            videoRemoteRelativePath: analysis.videoResource?.remoteRelativePath,
            photoContentHash: recordedHash(analysis.photoResource),
            videoContentHash: recordedHash(analysis.videoResource),
            isIncomplete: analysis.isIncomplete
        )
    }

    private static func recordedHash(_ resource: RemoteManifestResource?) -> Data? {
        guard let hash = resource?.contentHash, !hash.isEmpty else { return nil }
        return hash
    }
}

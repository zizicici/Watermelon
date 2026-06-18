import Foundation

struct HomeRemoteDelta {
    let changedMonths: Set<LibraryMonthKey>
}

/// In-memory mirror of remote month manifests, keyed by `RemoteLibrarySnapshotState.revision`.
///
/// **Concurrency contract**: same as `HomeLocalIndexEngine` — callers must
/// serialize access. `@unchecked Sendable` is granted because
/// `HomeDataProcessingWorker` runs all engine calls on its `processingQueue`.
final class HomeRemoteIndexEngine: @unchecked Sendable {
    private var remoteFingerprintsByMonth: [LibraryMonthKey: Set<Data>] = [:]
    private var summaryByMonth: [LibraryMonthKey: HomeMonthSummary] = [:]

    private(set) var snapshotRevision: UInt64?

    var allMonths: Set<LibraryMonthKey> {
        Set(remoteFingerprintsByMonth.keys)
    }

    func fingerprints(for month: LibraryMonthKey) -> Set<Data> {
        remoteFingerprintsByMonth[month] ?? []
    }

    func summary(for month: LibraryMonthKey) -> HomeMonthSummary? {
        summaryByMonth[month]
    }

    func apply(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) -> HomeRemoteDelta {
        var changedMonths = Set<LibraryMonthKey>()

        guard hasActiveConnection else {
            if !remoteFingerprintsByMonth.isEmpty {
                changedMonths.formUnion(remoteFingerprintsByMonth.keys)
                clearRemoteState()
            }
            snapshotRevision = state.revision
            return HomeRemoteDelta(changedMonths: changedMonths)
        }

        if snapshotRevision == state.revision, !state.isFullSnapshot {
            return HomeRemoteDelta(changedMonths: changedMonths)
        }

        if state.isFullSnapshot {
            changedMonths.formUnion(remoteFingerprintsByMonth.keys)
            clearRemoteState()
        }

        let deltas = state.monthDeltas
        let resolvedMonths = Self.resolveMonths(deltas)
        for (index, monthDelta) in deltas.enumerated() {
            let month = monthDelta.month
            changedMonths.insert(month)

            let resolved = resolvedMonths[index]
            remoteFingerprintsByMonth[month] = resolved.fingerprints.isEmpty ? nil : resolved.fingerprints
            summaryByMonth[month] = resolved.summary
        }

        snapshotRevision = state.revision
        return HomeRemoteDelta(changedMonths: changedMonths)
    }

    private func clearRemoteState() {
        remoteFingerprintsByMonth.removeAll()
        summaryByMonth.removeAll()
    }

    private struct ResolvedMonth {
        let fingerprints: Set<Data>
        let summary: HomeMonthSummary?
    }

    // resolveMonth is pure per-month; fan the CPU-bound resolves across cores for a
    // full/large snapshot, then the caller writes results single-threaded so the engine's
    // serialized-mutation contract holds. Small deltas skip the concurrency overhead.
    private static func resolveMonths(_ deltas: [RemoteLibraryMonthDelta]) -> [ResolvedMonth] {
        guard deltas.count >= 8 else {
            return deltas.map { resolveMonth($0.month, from: $0) }
        }
        return [ResolvedMonth](unsafeUninitializedCapacity: deltas.count) { buffer, count in
            DispatchQueue.concurrentPerform(iterations: deltas.count) { i in
                (buffer.baseAddress! + i).initialize(to: resolveMonth(deltas[i].month, from: deltas[i]))
            }
            count = deltas.count
        }
    }

    /// Applies the same drop rules as `HomeAlbumMatching.buildRemoteItems`: an asset is
    /// included only when at least one of its links points at a resource present in
    /// `delta.resources`. Bytes are summed over those resolvable resources (deduped by
    /// hash) rather than `asset.totalFileSizeBytes`. This matters in the partial-flush
    /// window where assets + links have landed but resources have not.
    private static func resolveMonth(
        _ month: LibraryMonthKey,
        from delta: RemoteLibraryMonthDelta
    ) -> ResolvedMonth {
        guard !delta.assets.isEmpty else {
            return ResolvedMonth(fingerprints: [], summary: nil)
        }

        var resourceSizeByHash: [Data: Int64] = [:]
        resourceSizeByHash.reserveCapacity(delta.resources.count)
        for resource in delta.resources {
            resourceSizeByHash[resource.contentHash] = resource.fileSize
        }

        // Per-asset: collect link roles and the dedup'd set of resolvable resource hashes.
        // Same hash referenced by multiple role/slot pairs still contributes one resource
        // upstream (buildRemoteItems uses seenHashes), so dedup here to match.
        var rolesByAssetID: [String: [Int]] = [:]
        var resolvableHashesByAssetID: [String: Set<Data>] = [:]
        rolesByAssetID.reserveCapacity(delta.assets.count)
        resolvableHashesByAssetID.reserveCapacity(delta.assets.count)
        for link in delta.assetResourceLinks where resourceSizeByHash[link.resourceHash] != nil {
            rolesByAssetID[link.assetID, default: []].append(link.role)
            resolvableHashesByAssetID[link.assetID, default: []].insert(link.resourceHash)
        }

        var fingerprints = Set<Data>()
        fingerprints.reserveCapacity(delta.assets.count)
        var assetCount = 0
        var photoCount = 0
        var videoCount = 0
        var reachableHashes = Set<Data>()
        for asset in delta.assets {
            let roles = rolesByAssetID[asset.id] ?? []
            guard !roles.isEmpty else { continue }
            fingerprints.insert(asset.assetFingerprint)
            assetCount += 1
            if let hashes = resolvableHashesByAssetID[asset.id] {
                reachableHashes.formUnion(hashes)
            }
            let hasPairedVideo = roles.contains { ResourceTypeCode.isPairedVideo($0) }
            let hasPhotoLike = roles.contains { ResourceTypeCode.isPhotoLike($0) }
            let hasVideo = roles.contains { ResourceTypeCode.isVideoLike($0) }
            if hasPairedVideo, hasPhotoLike {
                photoCount += 1  // livePhoto
            } else if hasVideo {
                videoCount += 1
            } else {
                photoCount += 1
            }
        }
        // Hash-deduped: same content shared by multiple assets contributes one resource on disk.
        let totalSize = reachableHashes.reduce(Int64(0)) { $0 + (resourceSizeByHash[$1] ?? 0) }
        let summary: HomeMonthSummary? = assetCount > 0
            ? HomeMonthSummary(
                month: month,
                assetCount: assetCount,
                photoCount: photoCount,
                videoCount: videoCount,
                backedUpCount: nil,
                totalSizeBytes: totalSize
            )
            : nil
        return ResolvedMonth(fingerprints: fingerprints, summary: summary)
    }
}

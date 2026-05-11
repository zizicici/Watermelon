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

        for monthDelta in state.monthDeltas {
            let month = monthDelta.month
            changedMonths.insert(month)

            let resolved = Self.resolveMonth(month, from: monthDelta)
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

    /// Classifier-gated — backed-up count matches what verify/restore filter (partial /
    /// metadata-only / mismatch excluded). Physical-presence overlay is pre-folded into
    /// `resourceSizeByHash`, so the classifier predicate sees both layers.
    private static func resolveMonth(
        _ month: LibraryMonthKey,
        from delta: RemoteLibraryMonthDelta
    ) -> ResolvedMonth {
        guard !delta.assets.isEmpty else {
            return ResolvedMonth(fingerprints: [], summary: nil)
        }

        var resourceSizeByHash: [Data: Int64] = [:]
        resourceSizeByHash.reserveCapacity(delta.resources.count)
        for resource in delta.resources where !delta.physicallyMissingHashes.contains(resource.contentHash) {
            resourceSizeByHash[resource.contentHash] = resource.fileSize
        }

        let linksByAssetID: [String: [RemoteAssetResourceLink]] = Dictionary(grouping: delta.assetResourceLinks, by: \.assetID)

        var fingerprints = Set<Data>()
        fingerprints.reserveCapacity(delta.assets.count)
        var assetCount = 0
        var photoCount = 0
        var videoCount = 0
        var reachableHashes = Set<Data>()
        for asset in delta.assets {
            let links = linksByAssetID[asset.id] ?? []
            let state = RemoteAssetIntegrityClassifier.classify(
                assetFingerprint: asset.assetFingerprint,
                links: links,
                isResourceAvailable: { resourceSizeByHash[$0] != nil }
            )
            guard state.isHealthy else { continue }
            fingerprints.insert(asset.assetFingerprint)
            assetCount += 1
            for link in links { reachableHashes.insert(link.resourceHash) }
            let roles = links.map(\.role)
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

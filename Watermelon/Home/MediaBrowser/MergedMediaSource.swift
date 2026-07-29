import Photos
import UIKit

// Unifies the local library and the remote backup into one timeline, deduplicated by assetFingerprint.
// A photo present on both sides appears once (the remote item already carries a local handle + `.both`
// presence). Local items whose fingerprint is on the remote are dropped as duplicates; the rest show as
// `.localOnly`. Materialization prefers the local handle (no download) when present.
final class MergedMediaSource: MediaBrowserSource, @unchecked Sendable {
    let mode: MediaBrowserMode = .merged

    private let localSource: LocalMediaSource
    private let remoteSource: RemoteMediaSource

    init(localSource: LocalMediaSource, remoteSource: RemoteMediaSource) {
        self.localSource = localSource
        self.remoteSource = remoteSource
    }

    func load() async -> MediaBrowserLoadResult {
        let childrenStartedAt = CFAbsoluteTimeGetCurrent()
        let projection: RemoteBrowserProjection
        switch await remoteSource.prepareProjection() {
        case .ready(let prepared):
            projection = prepared
        case .stale:
            return .stale
        case .cancelled:
            return .cancelled
        }
        async let remote = remoteSource.load(projection: projection)
        async let local = localSource.loadUsingCurrentPresence(
            excludingBackedUpFingerprints: projection.backedUpFingerprints,
            monthGroupingTimeZone: projection.monthGroupingTimeZone
        )
        let (remoteResult, localResult) = await (remote, local)
        guard !Task.isCancelled else { return .cancelled }
        guard projection.monthGroupingTimeZone == .frozenCurrent() else { return .stale }
        guard case .loaded(let remoteContent) = remoteResult,
              case .loaded(let localContent) = localResult else {
            if case .cancelled = remoteResult { return .cancelled }
            if case .cancelled = localResult { return .cancelled }
            return .stale
        }
        let remoteSections = remoteContent.sections
        let localSections = localContent.sections
        let childrenMs = (CFAbsoluteTimeGetCurrent() - childrenStartedAt) * 1_000
        let remoteCount = remoteSections.reduce(0) { $0 + $1.items.count }
        let localCount = localSections.reduce(0) { $0 + $1.items.count }
        let mergeStartedAt = CFAbsoluteTimeGetCurrent()
        let sections = Self.mergePrepared(
            remoteSections: remoteSections,
            localOnlySections: localSections,
            shouldCancel: { Task.isCancelled }
        )
        guard !Task.isCancelled else { return .cancelled }
        let mergeMs = (CFAbsoluteTimeGetCurrent() - mergeStartedAt) * 1_000
        MediaBrowserLoadTrace.emit(
            "mergedBuild",
            startedAt: childrenStartedAt,
            details: "localOnly=\(localCount) remote=\(remoteCount) output=\(sections.reduce(0) { $0 + $1.items.count }) monthCalcs=0 childrenMs=\(String(format: "%.1f", childrenMs)) mergeMs=\(String(format: "%.1f", mergeMs))"
        )
        return .loaded(MediaBrowserContent(sections: sections))
    }

    static func mergePrepared(
        remoteSections: [MediaBrowserSection],
        localOnlySections: [MediaBrowserSection],
        shouldCancel: () -> Bool = { false }
    ) -> [MediaBrowserSection] {
        guard !shouldCancel() else { return [] }
        var result: [MediaBrowserSection] = []
        result.reserveCapacity(remoteSections.count + localOnlySections.count)
        var remoteIndex = 0
        var localIndex = 0
        while remoteIndex < remoteSections.count || localIndex < localOnlySections.count {
            guard !shouldCancel() else { return [] }
            if remoteIndex == remoteSections.count {
                result.append(localOnlySections[localIndex])
                localIndex += 1
                continue
            }
            if localIndex == localOnlySections.count {
                result.append(remoteSections[remoteIndex])
                remoteIndex += 1
                continue
            }

            let remote = remoteSections[remoteIndex]
            let local = localOnlySections[localIndex]
            if remote.month > local.month {
                result.append(remote)
                remoteIndex += 1
            } else if local.month > remote.month {
                result.append(local)
                localIndex += 1
            } else {
                guard let items = mergeDescending(
                    remote.items,
                    local.items,
                    shouldCancel: shouldCancel
                ) else { return [] }
                result.append(MediaBrowserSection(month: remote.month, items: items))
                remoteIndex += 1
                localIndex += 1
            }
        }
        return result
    }

    private static func precedes(_ lhs: MediaBrowserItem, _ rhs: MediaBrowserItem) -> Bool {
        if lhs.creationDateMs != rhs.creationDateMs {
            return lhs.creationDateMs > rhs.creationDateMs
        }
        return lhs.id < rhs.id
    }

    private static func mergeDescending(
        _ lhs: [MediaBrowserItem],
        _ rhs: [MediaBrowserItem],
        shouldCancel: () -> Bool
    ) -> [MediaBrowserItem]? {
        guard !lhs.isEmpty else { return shouldCancel() ? nil : rhs }
        guard !rhs.isEmpty else { return shouldCancel() ? nil : lhs }
        var merged: [MediaBrowserItem] = []
        merged.reserveCapacity(lhs.count + rhs.count)
        var lhsIndex = 0
        var rhsIndex = 0
        while lhsIndex < lhs.count, rhsIndex < rhs.count {
            guard !shouldCancel() else { return nil }
            if precedes(lhs[lhsIndex], rhs[rhsIndex]) {
                merged.append(lhs[lhsIndex])
                lhsIndex += 1
            } else {
                merged.append(rhs[rhsIndex])
                rhsIndex += 1
            }
        }
        if lhsIndex < lhs.count {
            guard !shouldCancel() else { return nil }
            merged.append(contentsOf: lhs[lhsIndex...])
        }
        if rhsIndex < rhs.count {
            guard !shouldCancel() else { return nil }
            merged.append(contentsOf: rhs[rhsIndex...])
        }
        return merged
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        if let r = await route(item).thumbnail(for: item) { return r }
        return canRemoteFallback(item) ? await remoteSource.thumbnail(for: item) : nil
    }

    func photoImage(for item: MediaBrowserItem) async -> UIImage? {
        if let r = await fullSizeRoute(item).photoImage(for: item) { return r }
        return canRemoteFallback(item) ? await remoteSource.photoImage(for: item) : nil
    }

    func video(for item: MediaBrowserItem) async -> MaterializedVideo? {
        if let r = await fullSizeRoute(item).video(for: item) { return r }
        return canRemoteFallback(item) ? await remoteSource.video(for: item) : nil
    }

    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? {
        if let r = await fullSizeRoute(item).livePhoto(for: item, targetSize: targetSize) { return r }
        return canRemoteFallback(item) ? await remoteSource.livePhoto(for: item, targetSize: targetSize) : nil
    }

    // A `.both` item materialized via its local handle can come back nil — a stale handle (the asset was
    // deleted in Photos) or an iCloud-only original not downloaded. Fall back to the remote copy so it still
    // displays instead of going blank.
    private func canRemoteFallback(_ item: MediaBrowserItem) -> Bool {
        item.localIdentifier != nil && item.fingerprint != nil
    }

    func shareItems(for item: MediaBrowserItem) async -> [Any] {
        let items = await fullSizeRoute(item).shareItems(for: item)
        if !items.isEmpty { return items }
        // The local route produced nothing (stale PHAsset handle) — fall back to the remote copy so a `.both`
        // item whose device original was deleted still shares, matching the display materializers.
        return canRemoteFallback(item) ? await remoteSource.shareItems(for: item) : items
    }

    func metadata(for item: MediaBrowserItem) async -> MediaMetadataDocument? {
        let primarySource = fullSizeRoute(item)
        let primaryDocument = await primarySource.metadata(for: item)
        if primarySource === remoteSource || primaryDocument?.isSummaryOnly == false {
            return primaryDocument
        }
        guard canRemoteFallback(item) else {
            return primaryDocument
        }
        let remoteDocument = await remoteSource.metadata(for: item)
        if remoteDocument?.isSummaryOnly == false {
            return remoteDocument
        }
        return primaryDocument ?? remoteDocument
    }

    func shutdown() async {
        await remoteSource.shutdown()
        await localSource.shutdown()
    }

    // Prefer the local handle (no download) when the asset is on device.
    private func route(_ item: MediaBrowserItem) -> MediaBrowserSource {
        item.localIdentifier != nil ? localSource : remoteSource
    }

    // Full-size/share routing for a remote-backed item re-proves the handle at use time: the local source
    // materializes the item's own handle, so a Photos edit after load (handle now stale) must route through
    // the remote source instead — which re-resolves a current twin or serves the backup's bytes.
    private func fullSizeRoute(_ item: MediaBrowserItem) -> MediaBrowserSource {
        guard item.localIdentifier != nil else { return remoteSource }
        guard item.presence != .localOnly, item.fingerprint != nil else { return localSource }
        return remoteSource.currentLocalHandle(for: item) == item.localIdentifier ? localSource : remoteSource
    }
}

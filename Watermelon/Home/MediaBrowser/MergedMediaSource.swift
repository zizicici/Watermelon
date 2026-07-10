import Photos
import UIKit

// Unifies the local library and the remote backup into one timeline, deduplicated by assetFingerprint.
// A photo present on both sides appears once (the remote item already carries a local handle + `.both`
// presence). Local items whose fingerprint is on the remote are dropped as duplicates; the rest show as
// `.localOnly`. Materialization prefers the local handle (no download) when present.
final class MergedMediaSource: MediaBrowserSource {
    let mode: MediaBrowserMode = .merged

    private let localSource: LocalMediaSource
    private let remoteSource: RemoteMediaSource

    init(localSource: LocalMediaSource, remoteSource: RemoteMediaSource) {
        self.localSource = localSource
        self.remoteSource = remoteSource
    }

    func prepare() async {
        await remoteSource.prepare()
        await localSource.prepare()
    }

    func loadSections() async -> [MediaBrowserSection] {
        async let remote = remoteSource.loadSections()
        async let local = localSource.loadSections()
        let remoteItems = (await remote).flatMap { $0.items }
        let localItems = (await local).flatMap { $0.items }
        let calendar = LibraryMonthKey.monthCalendar(preference: .frozenCurrent())
        return Self.merge(remoteItems: remoteItems, localItems: localItems, calendar: calendar)
    }

    // Pure merge (dedup by fingerprint, regroup by month, newest-first). Extracted for testing.
    // Every shown remote record is a real backup — RemoteBrowserAssetBuilder drops the meaningless ones
    // (config-only / phantom) and keeps a partial-but-has-media record, flagged incomplete. So a remote item is
    // authoritative: its local twin dedups away into the (`.both`) remote item, which keeps the incomplete badge.
    // A local photo with no backing remote record shows `.localOnly` and offers Upload.
    static func merge(remoteItems: [MediaBrowserItem], localItems: [MediaBrowserItem], calendar: Calendar) -> [MediaBrowserSection] {
        let backedUp = Set(remoteItems.compactMap { $0.fingerprint })
        // Live local handle by fingerprint — a safety net for a transiently-stale shared presence index. If the
        // remote source built a handle-less item before the index knew this fingerprint is on device (while the
        // local source, reading the repo live, already sees it), the deduped item would show `.remoteOnly` and
        // wrongly offer Download for an on-device asset. Graft the handle so it's `.both` instead.
        let localHandleByFingerprint = Dictionary(
            localItems.compactMap { item in item.fingerprint.flatMap { fp in item.localIdentifier.map { (fp, $0) } } },
            uniquingKeysWith: { first, _ in first }
        )
        let remoteKept = remoteItems.map { item -> MediaBrowserItem in
            guard item.localIdentifier == nil, let fp = item.fingerprint, let localID = localHandleByFingerprint[fp] else { return item }
            var grafted = item
            grafted.localIdentifier = localID
            grafted.presence = .of(onDevice: true, onRemote: true)
            return grafted
        }
        let localOnly = localItems.filter { item in
            guard let fingerprint = item.fingerprint else { return true } // no fingerprint → cannot dedup
            return !backedUp.contains(fingerprint)
        }
        var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
        for item in remoteKept + localOnly {
            let date = Date(timeIntervalSince1970: Double(item.creationDateMs) / 1000)
            byMonth[LibraryMonthKey.from(date: date, calendar: calendar), default: []].append(item)
        }
        return byMonth.keys.sorted(by: >).map { month in
            let items = (byMonth[month] ?? []).sorted { $0.creationDateMs > $1.creationDateMs }
            return MediaBrowserSection(month: month, items: items)
        }
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        if item.presence != .localOnly {
            if let r = await remoteSource.thumbnail(for: item) { return r }
            guard item.thumbnailStorageCodec == RemoteManifestResource.plaintextStorageCodec else { return nil }
            return item.localIdentifier == nil ? nil : await localSource.thumbnail(for: item)
        }
        return await localSource.thumbnail(for: item)
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

    func actions(for item: MediaBrowserItem) -> [MediaBrowserActionKind] {
        switch item.presence {
        case .localOnly: return [.share, .upload, .deleteLocal]
        case .remoteOnly: return [.share, .download, .deleteRemote]
        case .both: return [.share, .deleteLocal, .deleteRemote]
        }
    }

    func shareItems(for item: MediaBrowserItem) async -> [Any] {
        let items = await fullSizeRoute(item).shareItems(for: item)
        if !items.isEmpty { return items }
        // The local route produced nothing (stale PHAsset handle) — fall back to the remote copy so a `.both`
        // item whose device original was deleted still shares, matching the display materializers.
        return canRemoteFallback(item) ? await remoteSource.shareItems(for: item) : items
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

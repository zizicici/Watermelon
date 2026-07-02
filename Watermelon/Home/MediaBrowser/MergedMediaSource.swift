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
    static func merge(remoteItems: [MediaBrowserItem], localItems: [MediaBrowserItem], calendar: Calendar) -> [MediaBrowserSection] {
        // Single source of truth for the local handle: the two loads read the fingerprint→localID map at
        // different instants and can disagree, so a remote twin may arrive with localIdentifier == nil even
        // though the asset is on device. Graft the local handle here (→ prefer the free local materializer).
        var localHandleByFingerprint: [Data: String] = [:]
        for item in localItems {
            guard let fingerprint = item.fingerprint, let localID = item.localIdentifier else { continue }
            if localHandleByFingerprint[fingerprint] == nil { localHandleByFingerprint[fingerprint] = localID }
        }
        let reconciledRemote = remoteItems.map { remote -> MediaBrowserItem in
            guard remote.localIdentifier == nil, let fingerprint = remote.fingerprint,
                  let localID = localHandleByFingerprint[fingerprint] else { return remote }
            var promoted = remote
            promoted.localIdentifier = localID
            promoted.presence = .both
            return promoted
        }
        let remoteFingerprints = Set(remoteItems.compactMap { $0.fingerprint })
        let localOnly = localItems.filter { item in
            guard let fingerprint = item.fingerprint else { return true } // no fingerprint → cannot dedup
            return !remoteFingerprints.contains(fingerprint)
        }
        var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
        for item in reconciledRemote + localOnly {
            let date = Date(timeIntervalSince1970: Double(item.creationDateMs) / 1000)
            byMonth[LibraryMonthKey.from(date: date, calendar: calendar), default: []].append(item)
        }
        return byMonth.keys.sorted(by: >).map { month in
            let items = (byMonth[month] ?? []).sorted { $0.creationDateMs > $1.creationDateMs }
            return MediaBrowserSection(month: month, items: items)
        }
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        await route(item).thumbnail(for: item)
    }

    func photoImage(for item: MediaBrowserItem) async -> UIImage? {
        await route(item).photoImage(for: item)
    }

    func video(for item: MediaBrowserItem) async -> MaterializedVideo? {
        await route(item).video(for: item)
    }

    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? {
        await route(item).livePhoto(for: item, targetSize: targetSize)
    }

    func shutdown() async {
        await remoteSource.shutdown()
        await localSource.shutdown()
    }

    // Prefer the local handle (no download) when the asset is on device.
    private func route(_ item: MediaBrowserItem) -> MediaBrowserSource {
        item.localIdentifier != nil ? localSource : remoteSource
    }
}

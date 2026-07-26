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
        let childrenStartedAt = CFAbsoluteTimeGetCurrent()
        async let remote = remoteSource.loadSections()
        async let local = localSource.loadSections()
        let remoteSections = await remote
        let localSections = await local
        guard !Task.isCancelled else { return [] }
        let childrenMs = (CFAbsoluteTimeGetCurrent() - childrenStartedAt) * 1_000
        let remoteCount = remoteSections.reduce(0) { $0 + $1.items.count }
        let localCount = localSections.reduce(0) { $0 + $1.items.count }
        let calendar = LibraryMonthKey.monthCalendar(preference: .frozenCurrent())
        let mergeStartedAt = CFAbsoluteTimeGetCurrent()
        var monthCalculationCount = 0
        var usedFastPath = false
        let sections = Self.merge(
            remoteSections: remoteSections,
            localSections: localSections,
            calendar: calendar,
            onMonthCalculationCount: { monthCalculationCount = $0 },
            onFastPath: { usedFastPath = $0 },
            shouldCancel: { Task.isCancelled }
        )
        guard !Task.isCancelled else { return [] }
        let mergeMs = (CFAbsoluteTimeGetCurrent() - mergeStartedAt) * 1_000
        MediaBrowserLoadTrace.emit(
            "mergedBuild",
            startedAt: childrenStartedAt,
            details: "local=\(localCount) remote=\(remoteCount) output=\(sections.reduce(0) { $0 + $1.items.count }) monthCalcs=\(monthCalculationCount) mergePath=\(usedFastPath ? "fast" : "fallback") childrenMs=\(String(format: "%.1f", childrenMs)) mergeMs=\(String(format: "%.1f", mergeMs))"
        )
        return sections
    }

    static func merge(
        remoteSections: [MediaBrowserSection],
        localSections: [MediaBrowserSection],
        calendar: Calendar,
        onMonthCalculationCount: ((Int) -> Void)? = nil,
        onFastPath: ((Bool) -> Void)? = nil,
        shouldCancel: () -> Bool = { false }
    ) -> [MediaBrowserSection] {
        guard !shouldCancel() else { return [] }
        var backedUp = Set<Data>()
        backedUp.reserveCapacity(remoteSections.reduce(0) { $0 + $1.items.count })
        var missingLocalHandle = Set<Data>()
        var remoteMonths = Set<LibraryMonthKey>()
        remoteMonths.reserveCapacity(remoteSections.count)
        var monthMemo = MediaBrowserMonthMemo(calendar: calendar)
        var canReuseSections = true
        for section in remoteSections {
            guard !shouldCancel() else { return [] }
            if !remoteMonths.insert(section.month).inserted {
                canReuseSections = false
            }
            if let first = section.items.first {
                let date = Date(timeIntervalSince1970: Double(first.creationDateMs) / 1_000)
                if monthMemo.month(for: date) != section.month {
                    canReuseSections = false
                }
            }
            if section.items.count > 1, let last = section.items.last {
                let date = Date(timeIntervalSince1970: Double(last.creationDateMs) / 1_000)
                if monthMemo.month(for: date) != section.month {
                    canReuseSections = false
                }
            }
            var previousCreationDateMs = Int64.max
            for item in section.items {
                guard !shouldCancel() else { return [] }
                if let fingerprint = item.fingerprint {
                    backedUp.insert(fingerprint)
                    if item.localIdentifier == nil {
                        missingLocalHandle.insert(fingerprint)
                    }
                }
                if item.creationDateMs > previousCreationDateMs {
                    canReuseSections = false
                }
                previousCreationDateMs = item.creationDateMs
            }
        }

        var localHandleByFingerprint: [Data: String] = [:]
        var localOnlyByMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
        localOnlyByMonth.reserveCapacity(localSections.count)
        var localMonths = Set<LibraryMonthKey>()
        localMonths.reserveCapacity(localSections.count)
        for section in localSections {
            guard !shouldCancel() else { return [] }
            if !localMonths.insert(section.month).inserted {
                canReuseSections = false
            }
            if let first = section.items.first {
                let date = Date(timeIntervalSince1970: Double(first.creationDateMs) / 1_000)
                if monthMemo.month(for: date) != section.month {
                    canReuseSections = false
                }
            }
            if section.items.count > 1, let last = section.items.last {
                let date = Date(timeIntervalSince1970: Double(last.creationDateMs) / 1_000)
                if monthMemo.month(for: date) != section.month {
                    canReuseSections = false
                }
            }
            var previousCreationDateMs = Int64.max
            var localOnly: [MediaBrowserItem] = []
            for item in section.items {
                guard !shouldCancel() else { return [] }
                if item.creationDateMs > previousCreationDateMs {
                    canReuseSections = false
                }
                previousCreationDateMs = item.creationDateMs
                guard let fingerprint = item.fingerprint,
                      let localIdentifier = item.localIdentifier else {
                    localOnly.append(item)
                    continue
                }
                if missingLocalHandle.contains(fingerprint),
                   localHandleByFingerprint[fingerprint] == nil {
                    localHandleByFingerprint[fingerprint] = localIdentifier
                }
                if !backedUp.contains(fingerprint) {
                    localOnly.append(item)
                }
            }
            if !localOnly.isEmpty {
                localOnlyByMonth[section.month] = localOnly
            }
        }

        guard canReuseSections else {
            onFastPath?(false)
            return mergeFallback(
                remoteSections: remoteSections,
                localSections: localSections,
                calendar: calendar,
                onMonthCalculationCount: onMonthCalculationCount,
                shouldCancel: shouldCancel
            )
        }

        var sections: [MediaBrowserSection] = []
        sections.reserveCapacity(remoteSections.count + localOnlyByMonth.count)
        for section in remoteSections {
            guard !shouldCancel() else { return [] }
            var remoteItems = section.items
            for index in remoteItems.indices where remoteItems[index].localIdentifier == nil {
                guard !shouldCancel() else { return [] }
                guard let fingerprint = remoteItems[index].fingerprint,
                      let localIdentifier = localHandleByFingerprint[fingerprint] else { continue }
                remoteItems[index].localIdentifier = localIdentifier
                remoteItems[index].presence = .of(onDevice: true, onRemote: true)
            }
            if let localOnly = localOnlyByMonth.removeValue(forKey: section.month) {
                guard let items = mergeDescending(
                    remoteItems,
                    localOnly,
                    shouldCancel: shouldCancel
                ) else { return [] }
                sections.append(
                    MediaBrowserSection(
                        month: section.month,
                        items: items
                    )
                )
            } else {
                sections.append(
                    MediaBrowserSection(month: section.month, items: remoteItems)
                )
            }
        }
        for (month, items) in localOnlyByMonth {
            guard !shouldCancel() else { return [] }
            sections.append(MediaBrowserSection(month: month, items: items))
        }
        guard !shouldCancel() else { return [] }
        sections.sort { $0.month > $1.month }
        onFastPath?(true)
        onMonthCalculationCount?(monthMemo.calculationCount)
        return sections
    }

    private static func mergeFallback(
        remoteSections: [MediaBrowserSection],
        localSections: [MediaBrowserSection],
        calendar: Calendar,
        onMonthCalculationCount: ((Int) -> Void)?,
        shouldCancel: () -> Bool
    ) -> [MediaBrowserSection] {
        var backedUp = Set<Data>()
        for section in remoteSections {
            for item in section.items {
                guard !shouldCancel() else { return [] }
                if let fingerprint = item.fingerprint {
                    backedUp.insert(fingerprint)
                }
            }
        }
        var localHandleByFingerprint: [Data: String] = [:]
        for section in localSections {
            for item in section.items {
                guard !shouldCancel() else { return [] }
                guard let fingerprint = item.fingerprint,
                      let localIdentifier = item.localIdentifier,
                      localHandleByFingerprint[fingerprint] == nil else { continue }
                localHandleByFingerprint[fingerprint] = localIdentifier
            }
        }
        var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
        var monthMemo = MediaBrowserMonthMemo(calendar: calendar)
        for section in remoteSections {
            for item in section.items {
                guard !shouldCancel() else { return [] }
                var projected = item
                if item.localIdentifier == nil,
                   let fingerprint = item.fingerprint,
                   let localIdentifier = localHandleByFingerprint[fingerprint] {
                    projected.localIdentifier = localIdentifier
                    projected.presence = .of(onDevice: true, onRemote: true)
                }
                let date = Date(timeIntervalSince1970: Double(projected.creationDateMs) / 1_000)
                byMonth[monthMemo.month(for: date), default: []].append(projected)
            }
        }
        for section in localSections {
            for item in section.items {
                guard !shouldCancel() else { return [] }
                if let fingerprint = item.fingerprint, backedUp.contains(fingerprint) {
                    continue
                }
                let date = Date(timeIntervalSince1970: Double(item.creationDateMs) / 1_000)
                byMonth[monthMemo.month(for: date), default: []].append(item)
            }
        }
        onMonthCalculationCount?(monthMemo.calculationCount)
        guard !shouldCancel() else { return [] }
        let months = byMonth.keys.sorted(by: >)
        var sections: [MediaBrowserSection] = []
        sections.reserveCapacity(months.count)
        for month in months {
            guard !shouldCancel() else { return [] }
            let items = (byMonth[month] ?? []).sorted {
                $0.creationDateMs > $1.creationDateMs
            }
            guard !shouldCancel() else { return [] }
            sections.append(MediaBrowserSection(
                month: month,
                items: items
            ))
        }
        return sections
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
            if lhs[lhsIndex].creationDateMs >= rhs[rhsIndex].creationDateMs {
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

import Photos
import UIKit

// Browses an on-device library scope (the whole library, or one/more albums). Presence is `.both` when an
// asset's fingerprint is present in the (cached) remote snapshot, else `.localOnly`. Works offline.
final class LocalMediaSource: MediaBrowserSource, @unchecked Sendable {
    let mode: MediaBrowserMode = .local

    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository
    // Single source of truth for local/remote/both — owns the profile-gated remote fingerprint set.
    private let presenceIndex: LibraryPresenceIndex
    // Which on-device assets this source browses.
    private let query: PhotoLibraryQuery

    init(photoLibraryService: PhotoLibraryService, hashIndexRepository: ContentHashIndexRepository, presenceIndex: LibraryPresenceIndex, query: PhotoLibraryQuery = .allAssets) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.presenceIndex = presenceIndex
        self.query = query
    }

    func load() async -> MediaBrowserLoadResult {
        let presenceStartedAt = CFAbsoluteTimeGetCurrent()
        guard await presenceIndex.refresh(notifyOnCommit: false) else {
            return Task.isCancelled ? .cancelled : .stale
        }
        MediaBrowserLoadTrace.emit("localPresence", startedAt: presenceStartedAt)
        return await loadUsingCurrentPresence()
    }

    func loadUsingCurrentPresence(
        excludingBackedUpFingerprints: Set<Data> = [],
        monthGroupingTimeZone: MonthGroupingTimeZonePreference = .frozenCurrent()
    ) async -> MediaBrowserLoadResult {
        let sections = await projectUsingCurrentPresence(
            excludingBackedUpFingerprints: excludingBackedUpFingerprints,
            monthGroupingTimeZone: monthGroupingTimeZone
        )
        guard !Task.isCancelled else { return .cancelled }
        guard monthGroupingTimeZone == .frozenCurrent() else { return .stale }
        return .loaded(MediaBrowserContent(sections: sections))
    }

    private func projectUsingCurrentPresence(
        excludingBackedUpFingerprints: Set<Data>,
        monthGroupingTimeZone: MonthGroupingTimeZonePreference
    ) async -> [MediaBrowserSection] {
        let browserInput = presenceIndex.browserLocalProjectionInput()
        let photoLibraryService = photoLibraryService
        let hashIndexRepository = hashIndexRepository
        let query = query
        let trace = MediaBrowserLoadTrace.context
        return await withCancellableDetachedValue(priority: .userInitiated) {
            let startedAt = CFAbsoluteTimeGetCurrent()
            guard !Task.isCancelled else { return [] }
            var fingerprintByLocalID: [String: LocalAssetFingerprintRecord] = [:]
            var databaseMs = 0.0
            var photoFetchMs = 0.0
            var projectionMs = 0.0
            var assetCount = 0
            let queryName: String
            switch query {
            case .allAssets: queryName = "all"
            case .albums(let identifiers): queryName = "albums(\(identifiers.count))"
            }

            if case .allAssets = query,
               let homeSeed = browserInput.seed,
               homeSeed.monthGroupingTimeZone == monthGroupingTimeZone {
                let projectionStartedAt = CFAbsoluteTimeGetCurrent()
                guard let sections = Self.sections(
                    from: homeSeed,
                    backedUpFingerprints: browserInput.backedUpFingerprints,
                    excludingBackedUpFingerprints: excludingBackedUpFingerprints,
                    shouldCancel: { Task.isCancelled }
                ) else { return [] }
                projectionMs = (CFAbsoluteTimeGetCurrent() - projectionStartedAt) * 1_000
                MediaBrowserLoadTrace.emit(
                    "localBuild",
                    context: trace,
                    startedAt: startedAt,
                    details: "query=\(queryName) source=home assets=\(homeSeed.assets.count) fingerprints=\(homeSeed.localIDByFingerprint.count) sections=\(sections.count) monthCalcs=0 dbMs=0.0 photoFetchMs=0.0 projectMs=\(String(format: "%.1f", projectionMs)) sectionMs=0.0"
                )
                return sections
            }

            let calendar = LibraryMonthKey.monthCalendar(preference: monthGroupingTimeZone)
            var monthMemo = MediaBrowserMonthMemo(calendar: calendar)
            var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
            func append(_ asset: PHAsset) {
                let localID = asset.localIdentifier
                let kind: AlbumMediaKind = PhotoLibraryService.isLivePhoto(asset)
                    ? .livePhoto
                    : (asset.mediaType == .video ? .video : .photo)
                // Home's staleness rule: a row older than the asset's edit no longer fingerprints the current
                // bytes — treat the asset as unfingerprinted so it reads `.localOnly`, offers Upload, and its
                // render never lands in the shared L1 under the pre-edit fingerprint.
                let record = fingerprintByLocalID[localID]
                let fingerprint: Data? = record.flatMap { r in
                    LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: r.updatedAt, assetModificationDate: asset.modificationDate) ? r.fingerprint : nil
                }
                if let fingerprint,
                   excludingBackedUpFingerprints.contains(fingerprint) {
                    return
                }
                // "Backed up" = the remote record has real media (a partial-but-has-media record counts). A local
                // twin of a config-only / phantom record isn't backed up, so it reads `.localOnly` and offers Upload.
                let onRemote = fingerprint.map { browserInput.backedUpFingerprints.contains($0) } ?? false
                let created = LibraryCreationDate.normalized(asset.creationDate)
                let month = monthMemo.month(for: created.date)
                let item = MediaBrowserItem(
                    kind: kind,
                    creationDateMs: created.milliseconds,
                    localIdentifier: localID,
                    fingerprint: fingerprint,
                    isBackedUp: onRemote
                )
                byMonth[month, default: []].append(item)
            }

            switch query {
            case .allAssets:
                let databaseStartedAt = CFAbsoluteTimeGetCurrent()
                fingerprintByLocalID = (try? hashIndexRepository.fetchAssetFingerprintRecords()) ?? [:]
                databaseMs = (CFAbsoluteTimeGetCurrent() - databaseStartedAt) * 1_000
                guard !Task.isCancelled else { return [] }
                let photoFetchStartedAt = CFAbsoluteTimeGetCurrent()
                let result = photoLibraryService.fetchAssetsResult()
                photoFetchMs = (CFAbsoluteTimeGetCurrent() - photoFetchStartedAt) * 1_000
                assetCount = result.count
                let projectionStartedAt = CFAbsoluteTimeGetCurrent()
                var cancelled = false
                result.enumerateObjects { asset, _, stop in
                    guard !Task.isCancelled else {
                        cancelled = true
                        stop.pointee = true
                        return
                    }
                    append(asset)
                }
                guard !cancelled else { return [] }
                projectionMs = (CFAbsoluteTimeGetCurrent() - projectionStartedAt) * 1_000
            case .albums:
                let photoFetchStartedAt = CFAbsoluteTimeGetCurrent()
                let assets = photoLibraryService.fetchAssets(
                    for: query,
                    shouldCancel: { Task.isCancelled }
                )
                photoFetchMs = (CFAbsoluteTimeGetCurrent() - photoFetchStartedAt) * 1_000
                assetCount = assets.count
                guard !Task.isCancelled else { return [] }
                let databaseStartedAt = CFAbsoluteTimeGetCurrent()
                fingerprintByLocalID = (try? hashIndexRepository.fetchAssetFingerprintRecords(
                    assetIDs: Set(assets.map(\.localIdentifier))
                )) ?? [:]
                databaseMs = (CFAbsoluteTimeGetCurrent() - databaseStartedAt) * 1_000
                guard !Task.isCancelled else { return [] }
                let projectionStartedAt = CFAbsoluteTimeGetCurrent()
                for asset in assets {
                    guard !Task.isCancelled else { return [] }
                    append(asset)
                }
                projectionMs = (CFAbsoluteTimeGetCurrent() - projectionStartedAt) * 1_000
            }
            let sectionStartedAt = CFAbsoluteTimeGetCurrent()
            let sections = byMonth.keys.sorted(by: >).map {
                MediaBrowserSection(
                    month: $0,
                    items: Self.sortedIfNeeded(byMonth[$0] ?? [])
                )
            }
            let sectionMs = (CFAbsoluteTimeGetCurrent() - sectionStartedAt) * 1_000
            MediaBrowserLoadTrace.emit(
                "localBuild",
                context: trace,
                startedAt: startedAt,
                details: "query=\(queryName) source=fallback assets=\(assetCount) fingerprints=\(fingerprintByLocalID.count) sections=\(sections.count) monthCalcs=\(monthMemo.calculationCount) dbMs=\(String(format: "%.1f", databaseMs)) photoFetchMs=\(String(format: "%.1f", photoFetchMs)) projectMs=\(String(format: "%.1f", projectionMs)) sectionMs=\(String(format: "%.1f", sectionMs))"
            )
            return sections
        }
    }

    static func sections(
        from seed: HomeBrowserLocalSeed,
        backedUpFingerprints: Set<Data>,
        excludingBackedUpFingerprints: Set<Data> = [],
        shouldCancel: () -> Bool = { false }
    ) -> [MediaBrowserSection]? {
        var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
        byMonth.reserveCapacity(min(seed.assets.count, 256))
        for asset in seed.assets {
            guard !shouldCancel() else { return nil }
            if let fingerprint = asset.fingerprint,
               excludingBackedUpFingerprints.contains(fingerprint) {
                continue
            }
            let onRemote = asset.fingerprint.map { backedUpFingerprints.contains($0) } ?? false
            byMonth[asset.month, default: []].append(MediaBrowserItem(
                kind: asset.kind,
                creationDateMs: asset.creationDateMs,
                localIdentifier: asset.localIdentifier,
                fingerprint: asset.fingerprint,
                isBackedUp: onRemote
            ))
        }
        return byMonth.keys.sorted(by: >).map { month in
            MediaBrowserSection(
                month: month,
                items: Self.sortedIfNeeded(byMonth[month] ?? [])
            )
        }
    }

    private static func sortedIfNeeded(
        _ items: [MediaBrowserItem]
    ) -> [MediaBrowserItem] {
        guard items.count > 1 else { return items }
        for index in 1 ..< items.count where precedes(items[index], items[index - 1]) {
            return items.sorted(by: precedes)
        }
        return items
    }

    private static func precedes(
        _ lhs: MediaBrowserItem,
        _ rhs: MediaBrowserItem
    ) -> Bool {
        if lhs.creationDateMs != rhs.creationDateMs {
            return lhs.creationDateMs > rhs.creationDateMs
        }
        return lhs.id < rhs.id
    }

    func thumbnail(for item: MediaBrowserItem) async -> UIImage? {
        guard let id = item.localIdentifier else { return nil }
        guard let fingerprint = item.fingerprint else {
            return await LocalMediaLoader.thumbnail(localIdentifier: id)
        }
        MediaThumbnailCache.configureIfNeeded()
        if let cached = await MediaThumbnailCache.cached(for: fingerprint) { return cached }
        // A merged tile's handle may come from the presence map (remote record grafted onto a device asset):
        // re-validate row freshness so an edited-after-backup render never persists under the shared
        // fingerprint key. Stale assets render through PhotoKit without entering the content-addressed cache.
        guard presenceIndex.localIdentifierForCurrentBytes(fingerprint) == id else {
            return await LocalMediaLoader.thumbnail(localIdentifier: id)
        }
        guard let image = await PhotoKitImageLoader.thumbnail(localIdentifier: id) else { return nil }
        guard presenceIndex.localIdentifierForCurrentBytes(fingerprint) == id else {
            return await LocalMediaLoader.thumbnail(localIdentifier: id)
        }
        MediaThumbnailCache.store(image, for: fingerprint)
        return image
    }

    func photoImage(for item: MediaBrowserItem) async -> UIImage? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.photoImage(
            localIdentifier: id,
            maxPixel: MediaDisplay.maxPixel,
            allowNetworkAccess: true
        )
    }

    func video(for item: MediaBrowserItem) async -> MaterializedVideo? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.video(localIdentifier: id)
    }

    func livePhoto(for item: MediaBrowserItem, targetSize: CGSize) async -> PHLivePhoto? {
        guard let id = item.localIdentifier else { return nil }
        return await LocalMediaLoader.livePhoto(localIdentifier: id, targetSize: targetSize)
    }

    func metadata(for item: MediaBrowserItem) async -> MediaMetadataDocument? {
        guard let id = item.localIdentifier else { return nil }
        return await MediaMetadataLoader.localDocument(
            localIdentifier: id,
            kind: item.kind
        )
    }
}

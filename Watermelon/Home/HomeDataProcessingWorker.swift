import Foundation
@preconcurrency import Photos
import os.log

private let dataLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeData")

struct HomeDataLoadResult {
    let didReload: Bool
    let changedMonths: Set<LibraryMonthKey>
    let isAuthorized: Bool
}

private struct RemoteOnlyQueryResult: Sendable {
    let remoteItems: [RemoteAlbumItem]
    let localFingerprintSet: Set<Data>
}

final class HomeDataProcessingWorker: @unchecked Sendable {
    private let photoLibraryService: PhotoLibraryService
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let remoteMonthSnapshot: @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
    private let processingQueue = DispatchQueue(
        label: "com.zizicici.watermelon.homeData.processing",
        qos: .userInitiated
    )

    private let localIndex = HomeLocalIndexEngine()
    private let remoteIndex = HomeRemoteIndexEngine()

    // Compared against `expectedScope` on stale-detectable queries so callers never
    // receive asset IDs from a scope different from the one they asked under.
    private var loadedScope: HomeLocalLibraryScope?
    private var hasActiveConnection = false
    private var needsRemoteBootstrap = false

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository,
        remoteMonthSnapshot: @escaping @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
    ) {
        self.photoLibraryService = photoLibraryService
        self.contentHashIndexRepository = contentHashIndexRepository
        self.remoteMonthSnapshot = remoteMonthSnapshot
    }

    func remoteSnapshotRevisionForQuery(hasActiveConnection: Bool) -> UInt64? {
        processingQueue.sync {
            if hasActiveConnection, needsRemoteBootstrap {
                return nil
            }
            return remoteIndex.snapshotRevision
        }
    }

    // Helpers bound to the processing queue: the engines that accept these closures
    // are only invoked while we're already executing on processingQueue.
    //
    // PHFetch fresh mtimes per call: a PHChange that hasn't reached the engine yet
    // would otherwise let a stale DB fingerprint slip through.
    private func fetchFingerprintsForIDs(_ ids: Set<String>) -> [String: LocalAssetFingerprintRecord] {
        guard !ids.isEmpty else { return [:] }
        do {
            let raw = try contentHashIndexRepository.fetchAssetFingerprintRecords(assetIDs: ids)
            guard !raw.isEmpty else { return [:] }
            let phAssets = photoLibraryService.fetchAssets(localIdentifiers: Set(raw.keys))
            var result: [String: LocalAssetFingerprintRecord] = [:]
            result.reserveCapacity(raw.count)
            for asset in phAssets {
                guard let record = raw[asset.localIdentifier] else { continue }
                if let mtime = asset.modificationDate, mtime > record.updatedAt { continue }
                result[asset.localIdentifier] = record
            }
            return result
        } catch {
            dataLog.error("[HomeData] fetchAssetFingerprintRecords(assetIDs:) failed: \(String(describing: error))")
            return [:]
        }
    }

    private func fetchAllFingerprints() -> [String: LocalAssetFingerprintRecord] {
        do {
            return try contentHashIndexRepository.fetchAssetFingerprintRecords()
        } catch {
            dataLog.error("[HomeData] fetchAssetFingerprintRecords() failed: \(String(describing: error))")
            return [:]
        }
    }

    private func remoteFingerprintsForMonth(_ month: LibraryMonthKey) -> Set<Data> {
        remoteIndex.fingerprints(for: month)
    }

    private func refreshBackedUpState(
        connectionFlipped: Bool,
        remoteChangedMonths: Set<LibraryMonthKey>
    ) -> Set<LibraryMonthKey> {
        if connectionFlipped {
            return localIndex.refreshBackedUpState(
                affectedMonths: localIndex.allMonths,
                remoteFingerprintsForMonth: remoteFingerprintsForMonth
            )
        }

        guard !remoteChangedMonths.isEmpty else { return [] }
        return localIndex.refreshBackedUpState(
            affectedMonths: remoteChangedMonths,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
    }

    func loadLocalIndex(forceReload: Bool, scope: HomeLocalLibraryScope) async -> HomeDataLoadResult {
        if !forceReload, processingQueue.sync(execute: { localIndex.hasLoadedIndex && loadedScope == scope }) {
            return HomeDataLoadResult(didReload: false, changedMonths: [], isAuthorized: true)
        }

        let status = photoLibraryService.authorizationStatus()
        let authorized = (status == .authorized || status == .limited)

        guard authorized else {
            let changedMonths = await withCheckedContinuation { continuation in
                processingQueue.async {
                    self.loadedScope = nil
                    continuation.resume(returning: self.localIndex.clearIfNeeded())
                }
            }
            return HomeDataLoadResult(didReload: true, changedMonths: changedMonths, isAuthorized: false)
        }

        let collections: [LibraryAssetCollection] = photoLibraryService
            .fetchResults(query: scope.photoLibraryQuery)
            .map { PhotoKitAssetCollection(fetchResult: $0) }
        let changedMonths = await withCheckedContinuation { continuation in
            processingQueue.async {
                let fingerprintByAsset = self.fetchAllFingerprints()
                let changed = self.localIndex.reload(
                    collections: collections,
                    fingerprintByAsset: fingerprintByAsset,
                    remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                )
                self.loadedScope = scope
                continuation.resume(returning: changed)
            }
        }

        return HomeDataLoadResult(didReload: true, changedMonths: changedMonths, isAuthorized: true)
    }

    func refreshLocalIndex(
        forAssetIDs assetIDs: Set<String>,
        expectedScope: HomeLocalLibraryScope
    ) async -> Set<LibraryMonthKey> {
        guard !assetIDs.isEmpty else { return [] }

        // Album scope can't infer membership for an arbitrary new asset (a downloaded
        // asset isn't necessarily in a user album); All Photos can.
        let shouldFetchMissing: Bool
        switch expectedScope {
        case .allPhotos:
            shouldFetchMissing = true
        case .albums:
            shouldFetchMissing = false
        }

        return await withCheckedContinuation { continuation in
            processingQueue.async {
                let start = CFAbsoluteTimeGetCurrent()
                guard self.loadedScope == expectedScope else {
                    continuation.resume(returning: [])
                    return
                }

                let existingIDs = self.localIndex.knownAssetIDs(in: assetIDs)
                var changedMonths = self.localIndex.refreshExisting(
                    assetIDs: existingIDs,
                    fingerprintsForIDs: self.fetchFingerprintsForIDs,
                    remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                )

                var insertedCount = 0
                if shouldFetchMissing {
                    let missingIDs = assetIDs.subtracting(existingIDs)
                    if !missingIDs.isEmpty {
                        let fetched = self.photoLibraryService.fetchAssets(localIdentifiers: missingIDs)
                        if !fetched.isEmpty {
                            let snapshots = Dictionary(uniqueKeysWithValues: fetched.map { asset in
                                (asset.localIdentifier, LibraryAssetSnapshot(
                                    localIdentifier: asset.localIdentifier,
                                    creationDate: asset.creationDate,
                                    modificationDate: asset.modificationDate,
                                    mediaKind: libraryAssetMediaKind(for: asset)
                                ))
                            })
                            changedMonths.formUnion(self.localIndex.eagerlyInsert(
                                snapshots,
                                fingerprintsForIDs: self.fetchFingerprintsForIDs,
                                remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                            ))
                            insertedCount = snapshots.count
                        }
                    }
                }

                let elapsed = CFAbsoluteTimeGetCurrent() - start
                dataLog.info("[HomeData] refreshLocalIndex: assets=\(existingIDs.count + insertedCount), inserted=\(insertedCount), months=\(changedMonths.count), \(String(format: "%.3f", elapsed))s")
                continuation.resume(returning: changedMonths)
            }
        }
    }

    func syncRemoteSnapshot(
        state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) async -> Set<LibraryMonthKey> {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                let start = CFAbsoluteTimeGetCurrent()
                let connectionFlipped = self.hasActiveConnection != hasActiveConnection
                if connectionFlipped {
                    self.hasActiveConnection = hasActiveConnection
                    if !hasActiveConnection {
                        self.needsRemoteBootstrap = true
                    }
                }

                let localAllMonths = connectionFlipped ? self.localIndex.allMonths : []
                let remoteAllMonths = connectionFlipped ? self.remoteIndex.allMonths : []
                let remoteDelta = self.remoteIndex.apply(state: state, hasActiveConnection: hasActiveConnection)
                var changedMonths = remoteDelta.changedMonths

                let touched = self.refreshBackedUpState(
                    connectionFlipped: connectionFlipped,
                    remoteChangedMonths: remoteDelta.changedMonths
                )
                changedMonths.formUnion(touched)

                if connectionFlipped {
                    changedMonths.formUnion(localAllMonths)
                    changedMonths.formUnion(remoteAllMonths)
                }

                if hasActiveConnection, state.isFullSnapshot {
                    self.needsRemoteBootstrap = false
                }

                let elapsed = CFAbsoluteTimeGetCurrent() - start
                dataLog.info("[HomeData] processingQueue: months=\(changedMonths.count), remoteChanged=\(remoteDelta.changedMonths.count), \(String(format: "%.3f", elapsed))s")
                continuation.resume(returning: changedMonths)
            }
        }
    }

    func applyPhotoLibraryChange(_ changeInstance: PHChange, scope: HomeLocalLibraryScope) async -> Set<LibraryMonthKey> {
        if processingQueue.sync(execute: { loadedScope != scope }) {
            return await loadLocalIndex(forceReload: true, scope: scope).changedMonths
        }

        let provider = PhotoKitChangeProvider(change: changeInstance)
        return await withCheckedContinuation { continuation in
            processingQueue.async {
                let changedMonths = self.localIndex.applyChange(
                    provider,
                    fingerprintsForIDs: self.fetchFingerprintsForIDs,
                    remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                )
                continuation.resume(returning: changedMonths)
            }
        }
    }

    func monthRow(for month: LibraryMonthKey) -> HomeMonthRow {
        processingQueue.sync { monthRowLocked(for: month) }
    }

    func allMonthRows() -> [LibraryMonthKey: HomeMonthRow] {
        processingQueue.sync {
            let allMonths = localIndex.allMonths.union(remoteIndex.allMonths)
            var result: [LibraryMonthKey: HomeMonthRow] = [:]
            result.reserveCapacity(allMonths.count)
            for month in allMonths {
                result[month] = monthRowLocked(for: month)
            }
            return result
        }
    }

    // No hasActiveConnection guard: the remote engine drops its state on disconnect,
    // so remoteIndex.summary(for:) returns nil whenever we're not connected.
    private func monthRowLocked(for month: LibraryMonthKey) -> HomeMonthRow {
        HomeMonthRow(
            month: month,
            local: localIndex.localMonthSummary(for: month),
            remote: remoteIndex.summary(for: month)
        )
    }

    func localAssetIDs(for month: LibraryMonthKey, expectedScope: HomeLocalLibraryScope) -> Set<String> {
        processingQueue.sync {
            guard loadedScope == expectedScope else { return [] }
            return localIndex.localAssetIDs(for: month)
        }
    }

    func remoteOnlyItems(for month: LibraryMonthKey, expectedScope: HomeLocalLibraryScope) async -> [RemoteAlbumItem] {
        // Everything needed — raw remote delta and the local fingerprint set — is captured in
        // a single processingQueue hop so `buildRemoteItems` and the fingerprint diff below
        // see a consistent snapshot. Doing the PHAsset fetch outside the queue (as an earlier
        // version did) opened a window where a concurrent PHChange could delete a local asset
        // between snapshot and fetch, silently promoting its remote twin to remoteOnly and
        // triggering a redundant restore.
        let query: RemoteOnlyQueryResult? = await withCheckedContinuation { cont in
            processingQueue.async {
                guard self.hasActiveConnection,
                      self.loadedScope == expectedScope,
                      let delta = self.remoteMonthSnapshot(month) else {
                    cont.resume(returning: nil)
                    return
                }
                let remoteItems = HomeAlbumMatching.buildRemoteItems(
                    assets: delta.assets,
                    resources: delta.resources,
                    links: delta.assetResourceLinks
                )
                let localIDs = self.localIndex.localAssetIDs(for: month)
                cont.resume(returning: RemoteOnlyQueryResult(
                    remoteItems: remoteItems,
                    localFingerprintSet: Set(self.localIndex.fingerprints(for: localIDs).values)
                ))
            }
        }
        guard let query, !query.remoteItems.isEmpty, !Task.isCancelled else { return [] }

        // Oldest-first so failed downloads retry in a deterministic order across runs.
        return query.remoteItems
            .filter { !query.localFingerprintSet.contains($0.assetFingerprint) }
            .sorted {
                if $0.creationDate != $1.creationDate {
                    return $0.creationDate < $1.creationDate
                }
                return $0.id < $1.id
            }
    }

    func matchedCount(for month: LibraryMonthKey) -> Int {
        // Only fingerprint-matched assets count here. Assets with matching content hashes but
        // no fingerprint (hash preflight without a subsequent upload) show as unbacked until
        // AssetProcessor or writeHashIndex stamps a fingerprint onto them.
        processingQueue.sync {
            guard hasActiveConnection else { return 0 }
            return localIndex.localMonthSummary(for: month)?.backedUpCount ?? 0
        }
    }

    func localMonthsForFileSizeScan() -> [LibraryMonthKey] {
        processingQueue.sync {
            localIndex.localMonthAssetCounts().map(\.month)
        }
    }

    struct FileSizeScanSample {
        let scope: HomeLocalLibraryScope?
        let ids: Set<String>
    }

    /// Snapshot the engine state under processingQueue. The returned `scope` is what
    /// the in-memory write-back gate compares against — if it changes before write-back
    /// (because a `reload(...)` landed mid-scan), the write-back is skipped.
    func sampleFileSizeScan(for month: LibraryMonthKey) async -> FileSizeScanSample {
        await withCheckedContinuation { cont in
            processingQueue.async {
                cont.resume(returning: FileSizeScanSample(
                    scope: self.loadedScope,
                    ids: self.localIndex.localAssetIDs(for: month)
                ))
            }
        }
    }

    /// Write `total` into the engine's in-memory `monthFileSizes[month]` only if
    /// `loadedScope` still matches what was sampled at scan start. Returns whether
    /// the write-back actually landed.
    @discardableResult
    func writeFileSizeIfScopeStable(
        _ total: Int64,
        for month: LibraryMonthKey,
        sampledScope: HomeLocalLibraryScope?
    ) async -> Bool {
        await withCheckedContinuation { cont in
            processingQueue.async {
                let stable = self.loadedScope == sampledScope
                if stable {
                    self.localIndex.setMonthFileSize(total, for: month)
                }
                cont.resume(returning: stable)
            }
        }
    }

    func updateFileSize(
        for month: LibraryMonthKey,
        sizeCache: [String: AssetSizeSnapshot]
    ) async -> [AssetSizeUpdate] {
        let sample = await sampleFileSizeScan(for: month)
        guard !sample.ids.isEmpty else { return [] }

        // `PHAssetResource.assetResources(for:)` returns an autoreleased NSArray per call.
        // Without an explicit pool the objects pile up across every month in the scan —
        // at 100K libraries that's easily tens of MB of ObjC overhead. Drain per month.
        let (total, updates): (Int64, [AssetSizeUpdate]) = autoreleasepool {
            let phAssets = photoLibraryService.fetchAssets(localIdentifiers: sample.ids)
            var assetByID: [String: PHAsset] = [:]
            assetByID.reserveCapacity(phAssets.count)
            for asset in phAssets {
                assetByID[asset.localIdentifier] = asset
            }

            var total: Int64 = 0
            var updates: [AssetSizeUpdate] = []
            for id in sample.ids {
                guard let asset = assetByID[id] else { continue }
                let mtime = asset.modificationDate?.millisecondsSinceEpoch

                if let mtime, let cached = sizeCache[id], cached.modificationDateMs == mtime {
                    total += cached.totalFileSizeBytes
                    continue
                }

                let resources = PHAssetResource.assetResources(for: asset)
                let computedSize = resources.reduce(Int64(0)) { partial, resource in
                    partial + max(PhotoLibraryService.resourceFileSize(resource), 0)
                }
                let safeSize = max(computedSize, 0)
                total += safeSize

                if let mtime {
                    updates.append(AssetSizeUpdate(
                        assetLocalIdentifier: id,
                        totalFileSizeBytes: safeSize,
                        modificationDateMs: mtime
                    ))
                }
            }
            return (total, updates)
        }

        await writeFileSizeIfScopeStable(total, for: month, sampledScope: sample.scope)
        return updates
    }
}

#if DEBUG
extension HomeDataProcessingWorker {
    /// Seed the worker into a "loaded" state without running PhotoKit. Tests use this
    /// to drive the scope-guard logic deterministically (PhotoKit auth would otherwise
    /// gate `loadLocalIndex`).
    func _testSeed(scope: HomeLocalLibraryScope, collections: [LibraryAssetCollection]) {
        processingQueue.sync {
            _ = self.localIndex.reload(
                collections: collections,
                fingerprintByAsset: [:],
                remoteFingerprintsForMonth: { _ in [] }
            )
            self.loadedScope = scope
        }
    }

    /// Flip the recorded `loadedScope` without touching `localIndex`. Models the race
    /// condition between an in-flight scan and a `reload(...)` landing.
    func _testForceLoadedScope(_ scope: HomeLocalLibraryScope?) {
        processingQueue.sync { self.loadedScope = scope }
    }

    func _testMonthFileSize(for month: LibraryMonthKey) -> Int64? {
        processingQueue.sync { localIndex.monthFileSizes[month] }
    }
}
#endif

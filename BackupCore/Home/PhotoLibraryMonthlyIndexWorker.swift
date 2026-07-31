import Foundation
@preconcurrency import Photos

struct PhotoLibraryMonthlyIndexSnapshot: Sendable {
    let sections: [HomeMergedYearSection]
    let totalAssetCount: Int
    let totalPhotoCount: Int
    let totalVideoCount: Int
    let totalSizeBytes: Int64?
    let remoteAssetCount: Int
    let remotePhotoCount: Int
    let remoteVideoCount: Int
    let remoteSizeBytes: Int64?
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference

    static let empty = PhotoLibraryMonthlyIndexSnapshot(
        sections: [],
        totalAssetCount: 0,
        totalPhotoCount: 0,
        totalVideoCount: 0,
        totalSizeBytes: nil,
        remoteAssetCount: 0,
        remotePhotoCount: 0,
        remoteVideoCount: 0,
        remoteSizeBytes: nil,
        monthGroupingTimeZone: .frozenCurrent()
    )
}

struct PhotoLibraryMonthlyIndexLoadResult: Sendable {
    let accessState: PhotoLibraryAccessState
    let loadedScope: HomeLocalLibraryScope
    let snapshot: PhotoLibraryMonthlyIndexSnapshot
    let fingerprintValidationAssetIDs: Set<String>
}

struct PhotoLibraryMonthlyIndexChangeResult: Sendable {
    let snapshot: PhotoLibraryMonthlyIndexSnapshot
    let fingerprintValidationAssetIDs: Set<String>
}

final class PhotoLibraryMonthlyIndexWorker: @unchecked Sendable {
    private struct FileSizeScanSample: Sendable {
        let generation: UInt64
        let scope: HomeLocalLibraryScope?
        let assetIDs: Set<String>
    }

    private struct FileSizeScanResult: Sendable {
        let totalBytes: Int64
        let updates: [AssetSizeUpdate]
    }

    private static let assetSizeWriteBackBatchSize = 200

    private let photoLibraryService: PhotoLibraryService
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let processingQueue = DispatchQueue(
        label: "com.zizicici.watermelon.photoLibrary.monthlyIndex",
        qos: .userInitiated
    )
    private let localIndex = HomeLocalIndexEngine()
    private let remoteIndex = HomeRemoteIndexEngine()
    private var trackedFetchResults: [PHFetchResult<PHAsset>] = []
    private var loadedScope: HomeLocalLibraryScope?
    private var indexGeneration: UInt64 = 0
    private var hasActiveConnection = false

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository
    ) {
        self.photoLibraryService = photoLibraryService
        self.contentHashIndexRepository = contentHashIndexRepository
    }

    func load(
        scope: HomeLocalLibraryScope = .allPhotos
    ) async -> PhotoLibraryMonthlyIndexLoadResult {
        let accessState = photoLibraryService.currentAccessState()
        guard accessState.canReadLibrary else {
            await clear()
            return PhotoLibraryMonthlyIndexLoadResult(
                accessState: accessState,
                loadedScope: scope,
                snapshot: currentSnapshot(),
                fingerprintValidationAssetIDs: []
            )
        }

        let loaded = await withCheckedContinuation { continuation in
            processingQueue.async {
                let resolvedScope: HomeLocalLibraryScope
                switch scope {
                case .allPhotos:
                    resolvedScope = .allPhotos
                case .albums(let identifiers):
                    let existing = self.photoLibraryService
                        .existingUserAlbumIdentifiers(in: identifiers)
                    resolvedScope = scope.reconciled(
                        existingAlbumIdentifiers: existing
                    )
                }
                let results = self.photoLibraryService.fetchResults(
                    query: resolvedScope.photoLibraryQuery
                )
                let snapshotsPerCollection = results.map(snapshots(of:))
                let fingerprints = (
                    try? self.contentHashIndexRepository.fetchAssetFingerprintRecords()
                ) ?? [:]
                let evaluation = LocalAssetFingerprintFreshness.evaluate(
                    snapshots: snapshotsPerCollection.joined(),
                    records: fingerprints
                )
                _ = self.localIndex.reload(
                    payload: LibraryInitialPayload(
                        collections: snapshotsPerCollection
                    ),
                    fingerprintByAsset: evaluation.freshRecords,
                    remoteFingerprintsForMonth: self.remoteFingerprints
                )
                self.trackedFetchResults = results
                self.loadedScope = resolvedScope
                self.indexGeneration &+= 1
                continuation.resume(
                    returning: (
                        scope: resolvedScope,
                        snapshot: self.makeSnapshotLocked(),
                        validationAssetIDs:
                            evaluation.validationAssetIDs
                    )
                )
            }
        }
        return PhotoLibraryMonthlyIndexLoadResult(
            accessState: accessState,
            loadedScope: loaded.scope,
            snapshot: loaded.snapshot,
            fingerprintValidationAssetIDs:
                loaded.validationAssetIDs
        )
    }

    func currentSnapshot() -> PhotoLibraryMonthlyIndexSnapshot {
        processingQueue.sync {
            makeSnapshotLocked()
        }
    }

    func localAssetIDs(
        for month: LibraryMonthKey,
        expectedScope: HomeLocalLibraryScope = .allPhotos
    ) -> Set<String> {
        processingQueue.sync {
            guard loadedScope == expectedScope else { return [] }
            return localIndex.localAssetIDs(for: month)
        }
    }

    func reconciledScope(
        for scope: HomeLocalLibraryScope
    ) async -> HomeLocalLibraryScope {
        guard case .albums(let identifiers) = scope else {
            return scope
        }
        return await withCheckedContinuation { continuation in
            processingQueue.async {
                let existing = self.photoLibraryService
                    .existingUserAlbumIdentifiers(in: identifiers)
                continuation.resume(
                    returning: scope.reconciled(
                        existingAlbumIdentifiers: existing
                    )
                )
            }
        }
    }

    func applyRemoteSnapshot(
        _ state: RemoteLibrarySnapshotState,
        hasActiveConnection: Bool
    ) async -> PhotoLibraryMonthlyIndexSnapshot {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                let connectionFlipped =
                    self.hasActiveConnection != hasActiveConnection
                self.hasActiveConnection = hasActiveConnection
                let delta = self.remoteIndex.apply(
                    state: state,
                    hasActiveConnection: hasActiveConnection
                )
                let affectedLocalMonths = connectionFlipped
                    ? self.localIndex.allMonths
                    : delta.changedMonths
                _ = self.localIndex.refreshBackedUpState(
                    affectedMonths: affectedLocalMonths,
                    remoteFingerprintsForMonth: self.remoteFingerprints
                )
                continuation.resume(returning: self.makeSnapshotLocked())
            }
        }
    }

    func clearRemoteSnapshot() async -> PhotoLibraryMonthlyIndexSnapshot {
        await applyRemoteSnapshot(
            RemoteLibrarySnapshotState(
                revision: 0,
                isFullSnapshot: true,
                monthDeltas: [],
                profileKey: nil
            ),
            hasActiveConnection: false
        )
    }

    func scanFileSizes(
        onUpdate: @escaping @Sendable (PhotoLibraryMonthlyIndexSnapshot) -> Void
    ) async {
        let sizeCache = await Task.detached(
            priority: .utility
        ) { [contentHashIndexRepository] in
            (try? contentHashIndexRepository.fetchAssetSizes()) ?? [:]
        }.value
        guard !Task.isCancelled else { return }

        let seed = processingQueue.sync {
            (
                generation: indexGeneration,
                scope: loadedScope,
                months: localIndex.allMonths.sorted(by: >)
            )
        }
        guard seed.scope != nil else { return }

        var mutableSizeCache = sizeCache
        var writeBackBuffer: [AssetSizeUpdate] = []

        for month in seed.months {
            guard !Task.isCancelled else { break }
            let sample = processingQueue.sync {
                FileSizeScanSample(
                    generation: indexGeneration,
                    scope: loadedScope,
                    assetIDs: localIndex.localAssetIDs(for: month)
                )
            }
            guard sample.generation == seed.generation,
                  sample.scope == seed.scope else {
                break
            }
            guard !sample.assetIDs.isEmpty else { continue }

            let currentSizeCache = mutableSizeCache
            let result = await Task.detached(
                priority: .utility
            ) { [photoLibraryService] in
                Self.computeFileSizes(
                    assetIDs: sample.assetIDs,
                    sizeCache: currentSizeCache,
                    photoLibraryService: photoLibraryService
                )
            }.value
            guard !Task.isCancelled else { break }

            let snapshot: PhotoLibraryMonthlyIndexSnapshot? = processingQueue.sync {
                let isStable = indexGeneration == sample.generation
                    && loadedScope == sample.scope
                    && localIndex.localAssetIDs(for: month) == sample.assetIDs
                guard isStable else { return nil }
                localIndex.setMonthFileSize(result.totalBytes, for: month)
                return makeSnapshotLocked()
            }
            guard let snapshot else { break }

            for update in result.updates {
                mutableSizeCache[update.assetLocalIdentifier] = AssetSizeSnapshot(
                    totalFileSizeBytes: update.totalFileSizeBytes,
                    modificationDateMs: update.modificationDateMs
                )
            }
            writeBackBuffer.append(contentsOf: result.updates)
            if writeBackBuffer.count >= Self.assetSizeWriteBackBatchSize {
                await flushAssetSizeWriteBack(&writeBackBuffer)
            }
            onUpdate(snapshot)
            await Task.yield()
        }

        await flushAssetSizeWriteBack(&writeBackBuffer)
    }

    func handlePhotoLibraryChange(
        _ change: PHChange,
        completion: @escaping @Sendable (
            PhotoLibraryMonthlyIndexChangeResult
        ) -> Void
    ) {
        processingQueue.async {
            var collectionChanges: [LibraryChangePayload.CollectionChange] = []
            collectionChanges.reserveCapacity(self.trackedFetchResults.count)
            var candidateSnapshotsByID:
                [String: LibraryAssetSnapshot] = [:]

            for index in self.trackedFetchResults.indices {
                let fetchResult = self.trackedFetchResults[index]
                guard let details = change.changeDetails(for: fetchResult) else {
                    continue
                }
                let nextFetchResult = details.fetchResultAfterChanges
                let entry: LibraryChangePayload.CollectionChange

                if details.hasIncrementalChanges {
                    let removedIDs = details.removedIndexes.map { indexes in
                        indexes.map {
                            fetchResult.object(at: $0).localIdentifier
                        }
                    } ?? []
                    let inserted = details.insertedIndexes.map { indexes in
                        indexes.map {
                            snapshot(nextFetchResult.object(at: $0))
                        }
                    } ?? []
                    let changed = details.changedIndexes.map { indexes in
                        indexes.map {
                            snapshot(nextFetchResult.object(at: $0))
                        }
                    } ?? []
                    var moved: [LibraryAssetSnapshot] = []
                    if details.hasMoves {
                        details.enumerateMoves { _, toIndex in
                            moved.append(
                                snapshot(nextFetchResult.object(at: toIndex))
                            )
                        }
                    }
                    for snapshot in inserted + changed + moved {
                        candidateSnapshotsByID[
                            snapshot.localIdentifier
                        ] = snapshot
                    }
                    entry = .incremental(
                        collectionIndex: index,
                        removed: removedIDs,
                        inserted: inserted,
                        changed: changed,
                        moved: moved
                    )
                } else {
                    let nextSnapshots = snapshots(of: nextFetchResult)
                    for snapshot in nextSnapshots {
                        candidateSnapshotsByID[
                            snapshot.localIdentifier
                        ] = snapshot
                    }
                    entry = .nonIncremental(
                        collectionIndex: index,
                        nextSnapshots: nextSnapshots
                    )
                }

                collectionChanges.append(entry)
                self.trackedFetchResults[index] = nextFetchResult
            }

            guard !collectionChanges.isEmpty else { return }
            self.indexGeneration &+= 1
            _ = self.localIndex.applyChange(
                LibraryChangePayload(collectionChanges: collectionChanges),
                fingerprintsForIDs: self.fetchFingerprints,
                remoteFingerprintsForMonth: self.remoteFingerprints
            )
            let candidateIDs = Set(candidateSnapshotsByID.keys)
            let records = (
                try? self.contentHashIndexRepository
                    .fetchAssetFingerprintRecords(
                        assetIDs: candidateIDs
                    )
            ) ?? [:]
            let evaluation = LocalAssetFingerprintFreshness.evaluate(
                snapshots: candidateSnapshotsByID.values,
                records: records
            )
            completion(
                PhotoLibraryMonthlyIndexChangeResult(
                    snapshot: self.makeSnapshotLocked(),
                    fingerprintValidationAssetIDs:
                        evaluation.validationAssetIDs
                )
            )
        }
    }

    func refreshLocalFingerprints(
        for assetIDs: Set<String>
    ) async -> PhotoLibraryMonthlyIndexSnapshot? {
        guard !assetIDs.isEmpty else { return nil }
        return await withCheckedContinuation { continuation in
            processingQueue.async {
                guard self.loadedScope != nil else {
                    continuation.resume(returning: nil)
                    return
                }
                _ = self.localIndex.refreshExisting(
                    assetIDs: assetIDs,
                    fingerprintsForIDs: self.fetchFingerprints,
                    remoteFingerprintsForMonth: self.remoteFingerprints
                )
                continuation.resume(
                    returning: self.makeSnapshotLocked()
                )
            }
        }
    }

    private func clear() async {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                self.trackedFetchResults.removeAll()
                self.loadedScope = nil
                self.indexGeneration &+= 1
                _ = self.localIndex.clearIfNeeded()
                continuation.resume()
            }
        }
    }

    private func fetchFingerprints(
        assetIDs: Set<String>
    ) -> [String: LocalAssetFingerprintRecord] {
        let records = (
            try? contentHashIndexRepository.fetchAssetFingerprintRecords(
                assetIDs: assetIDs
            )
        ) ?? [:]
        guard !records.isEmpty else { return [:] }
        let assets = photoLibraryService.fetchAssets(
            localIdentifiers: Set(records.keys)
        )
        return LocalAssetFingerprintFreshness.evaluate(
            snapshots: assets.map(snapshot),
            records: records
        ).freshRecords
    }

    private func remoteFingerprints(
        for month: LibraryMonthKey
    ) -> Set<Data> {
        remoteIndex.fingerprints(for: month)
    }

    private nonisolated static func computeFileSizes(
        assetIDs: Set<String>,
        sizeCache: [String: AssetSizeSnapshot],
        photoLibraryService: PhotoLibraryService
    ) -> FileSizeScanResult {
        autoreleasepool {
            let assets = photoLibraryService.fetchAssets(
                localIdentifiers: assetIDs
            )
            var assetByID: [String: PHAsset] = [:]
            assetByID.reserveCapacity(assets.count)
            for asset in assets {
                assetByID[asset.localIdentifier] = asset
            }

            var totalBytes: Int64 = 0
            var updates: [AssetSizeUpdate] = []
            for assetID in assetIDs {
                guard let asset = assetByID[assetID] else { continue }
                let modificationDateMs = asset.modificationDate?.millisecondsSinceEpoch
                if let modificationDateMs,
                   let cached = sizeCache[assetID],
                   cached.modificationDateMs == modificationDateMs {
                    totalBytes += cached.totalFileSizeBytes
                    continue
                }

                let bytes = PHAssetResource.assetResources(for: asset)
                    .reduce(Int64(0)) {
                        $0 + max(
                            PhotoLibraryService.resourceFileSize($1),
                            0
                        )
                    }
                totalBytes += bytes
                if let modificationDateMs {
                    updates.append(
                        AssetSizeUpdate(
                            assetLocalIdentifier: assetID,
                            totalFileSizeBytes: bytes,
                            modificationDateMs: modificationDateMs
                        )
                    )
                }
            }
            return FileSizeScanResult(
                totalBytes: totalBytes,
                updates: updates
            )
        }
    }

    private func flushAssetSizeWriteBack(
        _ buffer: inout [AssetSizeUpdate]
    ) async {
        guard !buffer.isEmpty else { return }
        let entries = buffer
        buffer.removeAll(keepingCapacity: true)
        await Task.detached(
            priority: .background
        ) { [contentHashIndexRepository] in
            try? contentHashIndexRepository.upsertAssetSizes(entries)
        }.value
    }

    private func makeSnapshotLocked() -> PhotoLibraryMonthlyIndexSnapshot {
        let allMonths = localIndex.allMonths.union(remoteIndex.allMonths)
        let rows = allMonths
            .map {
                HomeMonthRow(
                    month: $0,
                    local: localIndex.localMonthSummary(for: $0),
                    remote: remoteIndex.summary(for: $0)
                )
            }
            .sorted { $0.month > $1.month }
        let rowsByYear = Dictionary(grouping: rows, by: { $0.month.year })
        let sections = rowsByYear
            .map { year, yearRows in
                HomeMergedYearSection(
                    year: year,
                    rows: yearRows
                        .sorted { $0.month > $1.month }
                )
            }
            .sorted { $0.year > $1.year }
        let localSummaries = rows.compactMap(\.local)
        let remoteSummaries = rows.compactMap(\.remote)
        let totalSizeValues = localSummaries.compactMap(\.totalSizeBytes)
        let totalSize = localSummaries.isEmpty
            || totalSizeValues.count != localSummaries.count
            ? nil
            : totalSizeValues.reduce(0, +)
        let remoteSizeValues = remoteSummaries.compactMap(\.totalSizeBytes)
        let remoteSize = remoteSummaries.isEmpty
            || remoteSizeValues.count != remoteSummaries.count
            ? nil
            : remoteSizeValues.reduce(0, +)
        return PhotoLibraryMonthlyIndexSnapshot(
            sections: sections,
            totalAssetCount: localSummaries.reduce(0) {
                $0 + $1.assetCount
            },
            totalPhotoCount: localSummaries.reduce(0) {
                $0 + $1.photoCount
            },
            totalVideoCount: localSummaries.reduce(0) {
                $0 + $1.videoCount
            },
            totalSizeBytes: totalSize,
            remoteAssetCount: remoteSummaries.reduce(0) {
                $0 + $1.assetCount
            },
            remotePhotoCount: remoteSummaries.reduce(0) {
                $0 + $1.photoCount
            },
            remoteVideoCount: remoteSummaries.reduce(0) {
                $0 + $1.videoCount
            },
            remoteSizeBytes: remoteSize,
            monthGroupingTimeZone: localIndex.monthGroupingTimeZone
        )
    }
}

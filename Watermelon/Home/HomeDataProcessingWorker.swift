import Foundation
@preconcurrency import Photos
import os.log

private let dataLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeData")

struct HomeDataLoadResult {
    var readFailed = false
    let changedMonths: Set<LibraryMonthKey>
    let isAuthorized: Bool
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference?
    let fingerprintValidationAssetIDs: Set<String>
}

enum HomeLocalIndexRefreshResult {
    case requiresReload
    case failed(changedMonths: Set<LibraryMonthKey>)
    case reloaded(HomeDataLoadResult)
    case incremental(changedMonths: Set<LibraryMonthKey>, membershipChangedMonths: Set<LibraryMonthKey>)

    var changedMonths: Set<LibraryMonthKey> {
        switch self {
        case .requiresReload: []
        case .failed(let months): months
        case .reloaded(let result): result.changedMonths
        case .incremental(let months, _): months
        }
    }
}

struct HomeDataChangeResult {
    var requiresReload = false
    var readFailed = false
    let changedMonths: Set<LibraryMonthKey>
    let fingerprintValidationAssetIDs: Set<String>
}

struct HomeBrowserLocalSeed: Sendable {
    let localIDByFingerprint: [Data: String]
    let assets: [HomeBrowserLocalAsset]
    let monthGroupingTimeZone: MonthGroupingTimeZonePreference
}

struct HomeBrowserLocalAsset: Sendable {
    let localIdentifier: String
    let month: LibraryMonthKey
    let kind: AlbumMediaKind
    let creationDateMs: Int64
    let fingerprint: Data?
}

private struct RemoteOnlyQueryResult: Sendable {
    let remoteItems: [RemoteAlbumItem]
    let localFingerprintSet: Set<Data>
}

final class HomeDataProcessingWorker: @unchecked Sendable {
    struct LocalLibraryInput {
        let collections: [PhotoLibraryFetchCollection]
        let payload: LibraryInitialPayload
    }

    private let photoLibraryService: PhotoLibraryService
    private let fetchLocalLibrary: @Sendable (PhotoLibraryQuery) -> LocalLibraryInput?
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let remoteMonthSnapshot: @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?
    private let processingQueue = DispatchQueue(
        label: "com.zizicici.watermelon.homeData.processing",
        qos: .userInitiated
    )

    private let localIndex = HomeLocalIndexEngine()
    private let remoteIndex = HomeRemoteIndexEngine()
    private var trackedFetchResults: [PhotoLibraryFetchCollection] = []

    private var isAppActive = true
    private var needsLocalIndexReload = false
    private var localIndexGeneration: UInt64 = 0
    private var localIndexRevision: UInt64 = 0
    private var loadedScope: HomeLocalLibraryScope?
    private var hasActiveConnection = false
    private var remoteProfileKey: String?
    private var needsRemoteBootstrap = false

    init(
        photoLibraryService: PhotoLibraryService,
        contentHashIndexRepository: ContentHashIndexRepository,
        remoteMonthSnapshot: @escaping @Sendable (LibraryMonthKey) -> RemoteLibraryMonthDelta?,
        fetchLocalLibrary: (@Sendable (PhotoLibraryQuery) -> LocalLibraryInput?)? = nil
    ) {
        self.photoLibraryService = photoLibraryService
        self.contentHashIndexRepository = contentHashIndexRepository
        self.remoteMonthSnapshot = remoteMonthSnapshot
        self.fetchLocalLibrary = fetchLocalLibrary ?? { query in
            let status = photoLibraryService.authorizationStatus()
            guard status == .authorized || status == .limited else { return nil }
            let collections = photoLibraryService.fetchCollections(query: query)
            return LocalLibraryInput(collections: collections,
                payload: LibraryInitialPayload(collections: collections.map { snapshots(of: $0.assets) }))
        }
    }

    func remoteSnapshotRevisionForQuery(hasActiveConnection: Bool) -> UInt64? {
        processingQueue.sync {
            if hasActiveConnection, needsRemoteBootstrap {
                return nil
            }
            return remoteIndex.snapshotRevision
        }
    }

    func allAccessibleAssetIDs() async -> Set<String> {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                continuation.resume(returning: self.photoLibraryService.collectAssetIDs(query: .library(.all)))
            }
        }
    }

    func browserLocalSeed(expectedScope: HomeLocalLibraryScope) async -> HomeBrowserLocalSeed? {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                guard expectedScope == .device(.all),
                      self.loadedScope == expectedScope,
                      self.localIndex.hasLoadedIndex,
                      !self.needsLocalIndexReload,
                      self.localIndexRevision == self.contentHashIndexRepository.changes.currentRevision else {
                    continuation.resume(returning: nil)
                    return
                }
                continuation.resume(returning: self.localIndex.currentBrowserLocalSeed())
            }
        }
    }

    @concurrent func browserFileSizes(for identifiers: Set<String>) async -> [String: Int64] {
        guard !identifiers.isEmpty, !Task.isCancelled else { return [:] }
        let repository = contentHashIndexRepository
        let cache = await withCancellableDetachedValue(priority: .userInitiated) {
            (try? repository.fetchAssetSizes()) ?? [:]
        }
        let identifiers = Array(identifiers)
        var sizes: [String: Int64] = [:]
        sizes.reserveCapacity(identifiers.count)
        for start in stride(from: 0, to: identifiers.count, by: 200) {
            guard !Task.isCancelled else { return [:] }
            let batch = Set(identifiers[start ..< min(start + 200, identifiers.count)])
            let assets: [PHAsset] = await withCheckedContinuation { continuation in
                processingQueue.async {
                    continuation.resume(returning: self.photoLibraryService.fetchAssets(localIdentifiers: batch))
                }
            }
            let batchSizes: [String: Int64] = autoreleasepool {
                var result: [String: Int64] = [:]
                for asset in assets {
                    guard !Task.isCancelled else { return [:] }
                    if let cached = cache[asset.localIdentifier],
                       let mtime = asset.modificationDate?.millisecondsSinceEpoch,
                       cached.modificationDateMs == mtime {
                        result[asset.localIdentifier] = cached.totalFileSizeBytes
                        continue
                    }
                    let resources = PHAssetResource.assetResources(for: asset)
                    let size = resources.reduce(Int64(0)) {
                        $0 + max(0, PhotoLibraryService.resourceFileSize($1))
                    }
                    if size > 0 { result[asset.localIdentifier] = size }
                }
                return result
            }
            sizes.merge(batchSizes) { _, new in new }
        }
        return sizes
    }

    private func currentFingerprints(
        _ records: [String: LocalAssetFingerprintRecord], assets: [PHAsset]
    ) -> [String: LocalAssetFingerprintRecord] {
        var result: [String: LocalAssetFingerprintRecord] = [:]
        result.reserveCapacity(records.count)
        for asset in assets {
            guard let record = records[asset.localIdentifier] else { continue }
            if let mtime = asset.modificationDate, mtime > record.updatedAt { continue }
            result[asset.localIdentifier] = record
        }
        return result
    }

    private func localIndexReadFailed(_ error: Error) -> Set<LibraryMonthKey> {
        needsLocalIndexReload = true
        localIndexGeneration &+= 1
        dataLog.error("[HomeData] local index read failed: \(String(describing: error))")
        return localIndex.allMonths
    }

    static func fingerprintValidationAssetIDs(
        snapshots: some Sequence<LibraryAssetSnapshot>,
        records: [String: LocalAssetFingerprintRecord],
        invalidatedAssetIDs: Set<String> = []
    ) -> Set<String> {
        var result = Set<String>()
        for snapshot in snapshots {
            if invalidatedAssetIDs.contains(snapshot.localIdentifier) {
                result.insert(snapshot.localIdentifier)
                continue
            }
            guard let modificationDate = snapshot.modificationDate,
                  let record = records[snapshot.localIdentifier],
                  modificationDate > record.updatedAt else { continue }
            result.insert(snapshot.localIdentifier)
        }
        return result
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

    func reloadLocalIndex(scope: HomeLocalLibraryScope) async -> HomeDataLoadResult {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                continuation.resume(returning: self.reloadLocalIndexLocked(scope: scope))
            }
        }
    }

    private func reloadLocalIndexLocked(scope: HomeLocalLibraryScope) -> HomeDataLoadResult {
        dispatchPrecondition(condition: .onQueue(processingQueue))
        localIndexGeneration &+= 1
        let startedAt = CFAbsoluteTimeGetCurrent()
        let localRevision = contentHashIndexRepository.changes.currentRevision
        guard let input = fetchLocalLibrary(scope.photoLibraryQuery) else {
            trackedFetchResults.removeAll()
            loadedScope = nil
            needsLocalIndexReload = false
            return HomeDataLoadResult(changedMonths: localIndex.clearIfNeeded(),
                isAuthorized: false, monthGroupingTimeZone: nil, fingerprintValidationAssetIDs: [])
        }
        let fingerprintByAsset: [String: LocalAssetFingerprintRecord]
        let invalidatedAssetIDs: Set<String>
        do {
            fingerprintByAsset = try contentHashIndexRepository.fetchAssetFingerprintRecords()
            invalidatedAssetIDs = try contentHashIndexRepository.fetchInvalidatedFingerprintAssetIDs()
        } catch {
            var changedMonths = Set<LibraryMonthKey>()
            if loadedScope != scope {
                trackedFetchResults.removeAll()
                changedMonths = localIndex.clearIfNeeded()
            }
            loadedScope = scope
            changedMonths.formUnion(localIndexReadFailed(error))
            return HomeDataLoadResult(readFailed: true, changedMonths: changedMonths,
                isAuthorized: true, monthGroupingTimeZone: nil, fingerprintValidationAssetIDs: [])
        }
        trackedFetchResults = input.collections
        let changedMonths = localIndex.reload(
            payload: input.payload,
            fingerprintByAsset: fingerprintByAsset,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth
        )
        localIndexRevision = localRevision
        loadedScope = scope
        needsLocalIndexReload = false
        let validationAssetIDs = Self.fingerprintValidationAssetIDs(
            snapshots: input.payload.collections.joined(),
            records: fingerprintByAsset,
            invalidatedAssetIDs: invalidatedAssetIDs
        )
        dataLog.info("[HomeData] reloadLocalIndex: months=\(changedMonths.count), \(String(format: "%.3f", CFAbsoluteTimeGetCurrent() - startedAt))s")
        return HomeDataLoadResult(changedMonths: changedMonths, isAuthorized: true,
            monthGroupingTimeZone: localIndex.monthGroupingTimeZone, fingerprintValidationAssetIDs: validationAssetIDs)
    }

    func refreshLocalIndex(
        forAssetIDs assetIDs: Set<String>,
        expectedScope: HomeLocalLibraryScope,
        allowFullReload: Bool = true
    ) async -> HomeLocalIndexRefreshResult {
        return await withCheckedContinuation { continuation in
            processingQueue.async {
                let start = CFAbsoluteTimeGetCurrent()
                guard self.loadedScope == expectedScope else {
                    continuation.resume(returning: .incremental(changedMonths: [], membershipChangedMonths: []))
                    return
                }

                if self.needsLocalIndexReload {
                    let result: HomeLocalIndexRefreshResult = allowFullReload
                        ? .reloaded(self.reloadLocalIndexLocked(scope: expectedScope))
                        : .requiresReload
                    continuation.resume(returning: result)
                    return
                }
                let delta = self.contentHashIndexRepository.changes.delta(since: self.localIndexRevision)
                let changedIDs: Set<String>
                switch delta.change {
                case .touched(let ids): changedIDs = assetIDs.union(ids)
                case .bulkInvalidation:
                    continuation.resume(returning: allowFullReload
                        ? .reloaded(self.reloadLocalIndexLocked(scope: expectedScope)) : .requiresReload)
                    return
                }
                let records: [String: LocalAssetFingerprintRecord]
                do {
                    records = try self.contentHashIndexRepository.fetchAssetFingerprintRecords(assetIDs: changedIDs)
                } catch {
                    continuation.resume(returning: .failed(changedMonths: self.localIndexReadFailed(error)))
                    return
                }
                let fetchedAssets = self.photoLibraryService.fetchAssets(localIdentifiers: changedIDs)
                let fingerprints = self.currentFingerprints(records, assets: fetchedAssets)
                let accessibleIDs = Set(fetchedAssets.map(\.localIdentifier))
                let existingIDs = self.localIndex.knownAssetIDs(in: changedIDs)
                var changedMonths = self.localIndex.removeAssets(
                    existingIDs.subtracting(accessibleIDs), remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                )
                var membershipChangedMonths = changedMonths
                changedMonths.formUnion(self.localIndex.refreshExisting(
                    assetIDs: existingIDs.intersection(accessibleIDs),
                    fingerprintsForIDs: { _ in fingerprints },
                    remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                ))

                var insertedCount = 0
                // New assets have no inferred membership in a selected user album.
                if let mediaFilter = expectedScope.deviceMediaFilter {
                    let missingIDs = accessibleIDs.subtracting(existingIDs)
                    if !missingIDs.isEmpty {
                        let fetched = fetchedAssets.filter {
                            missingIDs.contains($0.localIdentifier) && mediaFilter.includes(libraryAssetMediaKind(for: $0))
                        }
                        if !fetched.isEmpty {
                            let snapshots = Dictionary(uniqueKeysWithValues: fetched.map { asset in
                                (asset.localIdentifier, LibraryAssetSnapshot(
                                    localIdentifier: asset.localIdentifier,
                                    creationDate: asset.creationDate,
                                    modificationDate: asset.modificationDate,
                                    mediaKind: libraryAssetMediaKind(for: asset)
                                ))
                            })
                            let insertedMonths = self.localIndex.eagerlyInsert(
                                snapshots,
                                fingerprintsForIDs: { _ in fingerprints },
                                remoteFingerprintsForMonth: self.remoteFingerprintsForMonth
                            )
                            changedMonths.formUnion(insertedMonths)
                            membershipChangedMonths.formUnion(insertedMonths)
                            insertedCount = snapshots.count
                        }
                    }
                }

                self.localIndexRevision = delta.revision
                let elapsed = CFAbsoluteTimeGetCurrent() - start
                dataLog.info("[HomeData] refreshLocalIndex: assets=\(existingIDs.count + insertedCount), inserted=\(insertedCount), months=\(changedMonths.count), \(String(format: "%.3f", elapsed))s")
                continuation.resume(returning: .incremental(changedMonths: changedMonths,
                    membershipChangedMonths: membershipChangedMonths))
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
                let nextProfileKey = hasActiveConnection ? state.profileKey : nil
                let connectionFlipped = self.hasActiveConnection != hasActiveConnection || self.remoteProfileKey != nextProfileKey
                self.remoteProfileKey = nextProfileKey
                if connectionFlipped {
                    self.hasActiveConnection = hasActiveConnection
                }

                // A cancelled connect can consume disconnected cache before ever flipping from connected.
                if !hasActiveConnection {
                    self.needsRemoteBootstrap = true
                }

                let localAllMonths = connectionFlipped ? self.localIndex.allMonths : []
                let remoteAllMonths = connectionFlipped ? self.remoteIndex.allMonths : []
                let applyStart = CFAbsoluteTimeGetCurrent()
                let remoteDelta = self.remoteIndex.apply(state: state, hasActiveConnection: hasActiveConnection)
                let applyElapsed = CFAbsoluteTimeGetCurrent() - applyStart
                var changedMonths = remoteDelta.changedMonths

                let matchStart = CFAbsoluteTimeGetCurrent()
                let touched = self.refreshBackedUpState(
                    connectionFlipped: connectionFlipped,
                    remoteChangedMonths: remoteDelta.changedMonths
                )
                let matchElapsed = CFAbsoluteTimeGetCurrent() - matchStart
                changedMonths.formUnion(touched)

                if connectionFlipped {
                    changedMonths.formUnion(localAllMonths)
                    changedMonths.formUnion(remoteAllMonths)
                }

                if hasActiveConnection, state.isFullSnapshot {
                    self.needsRemoteBootstrap = false
                }

                let elapsed = CFAbsoluteTimeGetCurrent() - start
                dataLog.info("[HomeData] processingQueue: months=\(changedMonths.count), remoteChanged=\(remoteDelta.changedMonths.count), apply=\(String(format: "%.3f", applyElapsed))s, match=\(String(format: "%.3f", matchElapsed))s, total=\(String(format: "%.3f", elapsed))s")
                continuation.resume(returning: changedMonths)
            }
        }
    }

    func setAppActive(_ active: Bool) {
        processingQueue.async { self.isAppActive = active }
    }

    func handlePhotoLibraryChange(
        _ change: PHChange,
        completion: @escaping (HomeDataChangeResult) -> Void
    ) {
        processingQueue.async {
            guard self.isAppActive, !self.needsLocalIndexReload else {
                self.needsLocalIndexReload = true
                DispatchQueue.main.async {
                    completion(HomeDataChangeResult(requiresReload: true, changedMonths: [], fingerprintValidationAssetIDs: []))
                }
                return
            }
            var collectionChanges: [LibraryChangePayload.CollectionChange] = []
            collectionChanges.reserveCapacity(self.trackedFetchResults.count)
            var nextFetchResults = self.trackedFetchResults
            var candidateSnapshotsByID: [String: LibraryAssetSnapshot] = [:]
            var burstMembershipChanged = false

            for index in self.trackedFetchResults.indices {
                let collection = self.trackedFetchResults[index]
                let fetchResult = collection.assets
                guard let details = change.changeDetails(for: fetchResult) else { continue }
                let nextFetchResult = details.fetchResultAfterChanges
                if case .album = collection.source {
                    if !details.hasIncrementalChanges {
                        burstMembershipChanged = true
                    } else {
                        let removedBurst = details.removedObjects.contains { $0.burstIdentifier != nil }
                        let insertedBurst = details.insertedObjects.contains { $0.burstIdentifier != nil }
                        let changedBurst = details.changedObjects.contains { $0.burstIdentifier != nil }
                        burstMembershipChanged = burstMembershipChanged || removedBurst || insertedBurst || changedBurst
                    }
                }

                let entry: LibraryChangePayload.CollectionChange
                if details.hasIncrementalChanges {
                    let removedIDs = details.removedIndexes.map { idxs in
                        idxs.map { fetchResult.object(at: $0).localIdentifier }
                    } ?? []
                    let inserted = details.insertedIndexes.map { idxs in
                        idxs.map { snapshot(nextFetchResult.object(at: $0)) }
                    } ?? []
                    let changed = details.changedIndexes.map { idxs in
                        idxs.map { snapshot(nextFetchResult.object(at: $0)) }
                    } ?? []
                    var moved: [LibraryAssetSnapshot] = []
                    if details.hasMoves {
                        details.enumerateMoves { _, toIndex in
                            moved.append(snapshot(nextFetchResult.object(at: toIndex)))
                        }
                    }
                    for snapshot in inserted + changed + moved {
                        candidateSnapshotsByID[snapshot.localIdentifier] = snapshot
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
                        candidateSnapshotsByID[snapshot.localIdentifier] = snapshot
                    }
                    entry = .nonIncremental(
                        collectionIndex: index,
                        nextSnapshots: nextSnapshots
                    )
                }

                collectionChanges.append(entry)
                nextFetchResults[index].assets = nextFetchResult
            }

            guard !collectionChanges.isEmpty else { return }
            var collectionUpdates: [LibraryCollectionUpdate]?
            if burstMembershipChanged, let scope = self.loadedScope, case .albums(let identifiers) = scope.photoLibraryQuery {
                let bursts = self.photoLibraryService.fetchBurstCollections(inAlbumIdentifiers: identifiers)
                let indicesBySource = Dictionary(uniqueKeysWithValues: nextFetchResults.enumerated().map { ($0.element.source, $0.offset) })
                var nextCollections: [PhotoLibraryFetchCollection] = []
                var updates: [LibraryCollectionUpdate] = []
                for (index, collection) in nextFetchResults.enumerated() {
                    if case .burst = collection.source { continue }
                    nextCollections.append(collection)
                    updates.append(.existing(index))
                }
                for burst in bursts {
                    if let index = indicesBySource[burst.source] {
                        nextCollections.append(nextFetchResults[index])
                        updates.append(.existing(index))
                    } else {
                        let added = snapshots(of: burst.assets)
                        nextCollections.append(burst)
                        updates.append(.inserted(added))
                        for snapshot in added { candidateSnapshotsByID[snapshot.localIdentifier] = snapshot }
                    }
                }
                collectionUpdates = updates
                nextFetchResults = nextCollections
            }
            let result = self.applyPhotoChangeLocked(
                LibraryChangePayload(collectionChanges: collectionChanges),
                nextFetchResults: nextFetchResults, collectionUpdates: collectionUpdates,
                candidateSnapshotsByID: candidateSnapshotsByID)
            guard result.readFailed || !result.changedMonths.isEmpty || !result.fingerprintValidationAssetIDs.isEmpty else { return }
            DispatchQueue.main.async {
                completion(result)
            }
        }
    }

    private func applyPhotoChangeLocked(
        _ payload: LibraryChangePayload,
        nextFetchResults: [PhotoLibraryFetchCollection],
        collectionUpdates: [LibraryCollectionUpdate]?,
        candidateSnapshotsByID: [String: LibraryAssetSnapshot]
    ) -> HomeDataChangeResult {
        let records: [String: LocalAssetFingerprintRecord]
        let invalidatedAssetIDs: Set<String>
        do {
            records = try contentHashIndexRepository.fetchAssetFingerprintRecords(assetIDs: Set(candidateSnapshotsByID.keys))
            invalidatedAssetIDs = try contentHashIndexRepository.fetchInvalidatedFingerprintAssetIDs()
        } catch {
            return HomeDataChangeResult(readFailed: true, changedMonths: localIndexReadFailed(error),
                fingerprintValidationAssetIDs: [])
        }
        let fingerprints = currentFingerprints(records,
            assets: photoLibraryService.fetchAssets(localIdentifiers: Set(records.keys)))
        var changedMonths = localIndex.applyChange(payload,
            fingerprintsForIDs: { _ in fingerprints }, remoteFingerprintsForMonth: remoteFingerprintsForMonth)
        if let collectionUpdates {
            changedMonths.formUnion(localIndex.updateCollections(collectionUpdates,
                fingerprintsForIDs: { _ in fingerprints }, remoteFingerprintsForMonth: remoteFingerprintsForMonth))
        }
        trackedFetchResults = nextFetchResults
        return HomeDataChangeResult(changedMonths: changedMonths,
            fingerprintValidationAssetIDs: Self.fingerprintValidationAssetIDs(
                snapshots: candidateSnapshotsByID.values, records: records, invalidatedAssetIDs: invalidatedAssetIDs))
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

    func remoteOnlyItems(for month: LibraryMonthKey, expectedScope: HomeLocalLibraryScope) async throws -> [RemoteAlbumItem] {
        // Everything needed — raw remote delta and the local fingerprint set — is captured in
        // a single processingQueue hop so `buildRemoteItems` and the fingerprint diff below
        // see a consistent snapshot. Doing the PHAsset fetch outside the queue (as an earlier
        // version did) opened a window where a concurrent PHChange could delete a local asset
        // between snapshot and fetch, silently promoting its remote twin to remoteOnly and
        // triggering a redundant restore.
        let query: RemoteOnlyQueryResult? = try await withCheckedThrowingContinuation { cont in
            processingQueue.async {
                guard !self.needsLocalIndexReload else {
                    cont.resume(throwing: HomeLocalIndexReadError())
                    return
                }
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
                let fingerprints = self.localIndex.fingerprints(for: localIDs)
                let matched = Set(fingerprints.values)
                cont.resume(returning: RemoteOnlyQueryResult(
                    remoteItems: remoteItems,
                    localFingerprintSet: matched
                ))
            }
        }
        guard let query, !query.remoteItems.isEmpty, !Task.isCancelled else { return [] }

        // Dedup against visible fingerprints only. An asset hidden after backup leaves the default fetch and is
        // indistinguishable from a deleted one (iOS 16+ locks the Hidden album from third-party fetches by
        // default, even with `includeHiddenAssets`), so its remote twin may re-import; suppressing it instead
        // would risk skipping a genuine restore of a deleted asset.
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

    func localMonthsForFileSizeScan() async -> [LibraryMonthKey] {
        await withCheckedContinuation { cont in
            processingQueue.async {
                cont.resume(returning: self.needsLocalIndexReload ? [] : self.localIndex.localMonthAssetCounts().map(\.month))
            }
        }
    }

    struct FileSizeScanSample {
        let scope: HomeLocalLibraryScope?
        let generation: UInt64
        let ids: Set<String>
    }

    /// Snapshot the engine state under processingQueue for the write-back gate.
    func sampleFileSizeScan(for month: LibraryMonthKey) async -> FileSizeScanSample {
        await withCheckedContinuation { cont in
            processingQueue.async {
                cont.resume(returning: FileSizeScanSample(
                    scope: self.loadedScope,
                    generation: self.localIndexGeneration,
                    ids: self.needsLocalIndexReload ? [] : self.localIndex.localAssetIDs(for: month)
                ))
            }
        }
    }

    /// Write `total` only if local month membership still matches the scan sample.
    @discardableResult
    func writeFileSizeIfIndexStable(
        _ total: Int64,
        for month: LibraryMonthKey,
        sampledScope: HomeLocalLibraryScope?,
        sampledGeneration: UInt64,
        sampledAssetIDs: Set<String>
    ) async -> Bool {
        await withCheckedContinuation { cont in
            processingQueue.async {
                let stable = !self.needsLocalIndexReload && self.loadedScope == sampledScope
                    && self.localIndexGeneration == sampledGeneration
                    && self.localIndex.localAssetIDs(for: month) == sampledAssetIDs
                if stable {
                    self.localIndex.setMonthFileSize(total, for: month)
                }
                cont.resume(returning: stable)
            }
        }
    }

    // @concurrent: under NonisolatedNonsendingByDefault this would otherwise run on the
    // caller's actor — the @MainActor scan loop — putting the PHAssetResource walk on main.
    @concurrent func updateFileSize(
        for month: LibraryMonthKey,
        sizeCache: [String: AssetSizeSnapshot]
    ) async -> [AssetSizeUpdate] {
        let sample = await sampleFileSizeScan(for: month)
        guard !sample.ids.isEmpty else { return [] }

        // `PHAssetResource.assetResources(for:)` returns an autoreleased NSArray per call.
        // Without an explicit pool the objects pile up across every month in the scan —
        // at 100K libraries that's easily tens of MB of ObjC overhead. Drain per month.
        let phAssets: [PHAsset] = await withCheckedContinuation { continuation in
            processingQueue.async {
                continuation.resume(returning: self.photoLibraryService.fetchAssets(localIdentifiers: sample.ids))
            }
        }
        let (total, updates): (Int64, [AssetSizeUpdate]) = autoreleasepool {
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

        await writeFileSizeIfIndexStable(
            total,
            for: month,
            sampledScope: sample.scope,
            sampledGeneration: sample.generation,
            sampledAssetIDs: sample.ids
        )
        return updates
    }
}

#if DEBUG
extension HomeDataProcessingWorker {
    func _testIndexState(assetIDs: Set<String>) -> (revision: UInt64, fingerprints: [String: Data]) {
        processingQueue.sync { (localIndexRevision, localIndex.fingerprints(for: assetIDs)) }
    }

    func _testApplyPhotoChange(_ payload: LibraryChangePayload, snapshots: [LibraryAssetSnapshot]) async -> HomeDataChangeResult {
        await withCheckedContinuation { continuation in
            processingQueue.async {
                continuation.resume(returning: self.applyPhotoChangeLocked(payload,
                    nextFetchResults: self.trackedFetchResults, collectionUpdates: nil,
                    candidateSnapshotsByID: Dictionary(uniqueKeysWithValues: snapshots.map { ($0.localIdentifier, $0) })))
            }
        }
    }

    /// Seed the worker into a "loaded" state without running PhotoKit. Tests use this
    /// to drive the scope-guard logic deterministically (PhotoKit auth would otherwise
    /// gate `reloadLocalIndex`).
    func _testSeed(
        scope: HomeLocalLibraryScope,
        payload: LibraryInitialPayload,
        fingerprints: [String: LocalAssetFingerprintRecord] = [:]
    ) {
        processingQueue.sync {
            _ = self.localIndex.reload(
                payload: payload,
                fingerprintByAsset: fingerprints,
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

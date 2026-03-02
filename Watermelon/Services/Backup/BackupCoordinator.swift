import Foundation
import Photos

struct BackupRunState: Sendable {
    var total: Int = 0
    var succeeded: Int = 0
    var failed: Int = 0
    var skipped: Int = 0
    var paused: Bool = false

    var processed: Int {
        succeeded + failed + skipped
    }
}

protocol BackupCoordinatorProtocol: Sendable {
    func runBackup(request: BackupRunRequest, eventStream: BackupEventStream) async throws -> BackupExecutionResult

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String,
        eventStream: BackupEventStream?
    ) async throws -> RemoteLibrarySnapshot

    func currentRemoteSnapshot() -> RemoteLibrarySnapshot
    func currentRemoteSnapshotState(since revision: UInt64?) -> RemoteLibrarySnapshotState
}

extension BackupCoordinatorProtocol {
    func runBackup(request: BackupRunRequest) async throws -> BackupExecutionResult {
        let eventStream = BackupEventStream()
        defer {
            eventStream.finish()
        }
        return try await runBackup(request: request, eventStream: eventStream)
    }

    func runBackup(
        profile: ServerProfileRecord,
        password: String,
        onlyAssetLocalIdentifiers: Set<String>? = nil
    ) async throws -> BackupExecutionResult {
        try await runBackup(request: BackupRunRequest(
            profile: profile,
            password: password,
            onlyAssetLocalIdentifiers: onlyAssetLocalIdentifiers
        ))
    }

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String
    ) async throws -> RemoteLibrarySnapshot {
        try await reloadRemoteIndex(profile: profile, password: password, eventStream: nil)
    }
}

final class BackupCoordinator: BackupCoordinatorProtocol, Sendable {
    private let photoLibraryService: PhotoLibraryServiceProtocol
    private let storageClientFactory: StorageClientFactoryProtocol
    private let hashIndexRepository: ContentHashIndexRepositoryProtocol
    private let remoteIndexService: RemoteIndexSyncService
    private let assetProcessor: AssetProcessor

    init(
        photoLibraryService: PhotoLibraryServiceProtocol,
        storageClientFactory: StorageClientFactoryProtocol,
        hashIndexRepository: ContentHashIndexRepositoryProtocol,
        remoteIndexService: RemoteIndexSyncService? = nil,
        assetProcessor: AssetProcessor? = nil
    ) {
        self.photoLibraryService = photoLibraryService
        self.storageClientFactory = storageClientFactory
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService ?? RemoteIndexSyncService()
        self.assetProcessor = assetProcessor ?? AssetProcessor(
            photoLibraryService: photoLibraryService,
            hashIndexRepository: hashIndexRepository,
            remoteIndexService: self.remoteIndexService
        )
    }

    func runBackup(request: BackupRunRequest, eventStream: BackupEventStream) async throws -> BackupExecutionResult {
        let profile = request.profile
        let password = request.password
        let onlyAssetLocalIdentifiers = request.onlyAssetLocalIdentifiers

        try await ensurePhotoAuthorization()

        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()

        do {
            try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
            var snapshotSeedLookup: MonthSeedLookup?

            do {
                let snapshot = try await remoteIndexService.syncIndex(
                    client: client,
                    profile: profile,
                    eventStream: eventStream
                )
                snapshotSeedLookup = makeMonthSeedLookup(from: snapshot)
                eventStream.emit(.log(
                    "Remote index synced. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s)."
                ))
            } catch {
                if profile.isExternalStorageUnavailableError(error) {
                    throw error
                }
                eventStream.emit(.log("Remote index scan warning: \(error.localizedDescription)"))
            }

            let retryMode = onlyAssetLocalIdentifiers != nil
            let assetsResult: PHFetchResult<PHAsset>? = retryMode
                ? nil
                : photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)

            var retryAssets: [PHAsset] = []
            if let retryTargets = onlyAssetLocalIdentifiers {
                let fetched = PHAsset.fetchAssets(withLocalIdentifiers: Array(retryTargets), options: nil)
                retryAssets.reserveCapacity(fetched.count)
                for index in 0 ..< fetched.count {
                    retryAssets.append(fetched.object(at: index))
                }
                retryAssets.sort { lhs, rhs in
                    (lhs.creationDate ?? .distantPast) < (rhs.creationDate ?? .distantPast)
                }
            }

            var state = BackupRunState(total: retryMode ? retryAssets.count : (assetsResult?.count ?? 0))

            if retryMode {
                let requested = onlyAssetLocalIdentifiers?.count ?? 0
                let missing = max(requested - retryAssets.count, 0)
                eventStream.emit(.log(
                    "Retry mode: requested \(requested), resolved \(retryAssets.count), missing \(missing)."
                ))
            } else {
                eventStream.emit(.log("Start backup by asset (oldest month first)."))
            }

            eventStream.emit(.started(totalAssets: state.total))

            if state.total == 0 {
                let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
                eventStream.emit(.finished(result))
                await disconnectClient(client)
                return result
            }

            let localHashCacheByAssetID: [String: LocalAssetHashCache]
            do {
                if let onlyAssetLocalIdentifiers {
                    localHashCacheByAssetID = try hashIndexRepository.fetchAssetHashCaches(assetIDs: onlyAssetLocalIdentifiers)
                } else {
                    localHashCacheByAssetID = try hashIndexRepository.fetchAssetHashCaches()
                }
            } catch {
                localHashCacheByAssetID = [:]
                eventStream.emit(.log("Local hash cache load warning: \(error.localizedDescription)"))
            }

            var activeMonth: MonthKey?
            var activeStore: MonthManifestStore?
            var stageTimingWindow = StageTimingWindow()

            let loopCount = retryMode ? retryAssets.count : (assetsResult?.count ?? 0)
            for loopIndex in 0 ..< loopCount {
                if Task.isCancelled {
                    state.paused = true
                    break
                }

                let asset: PHAsset
                if retryMode {
                    asset = retryAssets[loopIndex]
                } else if let assetsResult {
                    asset = assetsResult.object(at: loopIndex)
                } else {
                    continue
                }
                let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
                    from: PHAssetResource.assetResources(for: asset)
                )
                if selectedResources.isEmpty {
                    state.total = max(state.total - 1, 0)
                    continue
                }

                let currentPosition = state.processed + 1
                let monthKey = AssetProcessor.monthKey(for: asset.creationDate)

                if activeMonth != monthKey {
                    if let activeStore {
                        do {
                            _ = try await activeStore.flushToRemote()
                            eventStream.emit(.monthChanged(MonthChangeEvent(
                                year: activeMonth?.year ?? 0,
                                month: activeMonth?.month ?? 0,
                                action: .flushed
                            )))
                        } catch {
                            eventStream.emit(.monthChanged(MonthChangeEvent(
                                year: activeMonth?.year ?? 0,
                                month: activeMonth?.month ?? 0,
                                action: .flushFailed(error.localizedDescription)
                            )))
                        }
                    }

                    activeMonth = monthKey
                    activeStore = try await MonthManifestStore.loadOrCreate(
                        client: client,
                        basePath: profile.basePath,
                        year: monthKey.year,
                        month: monthKey.month,
                        seed: snapshotSeedLookup?.seed(for: monthKey)
                    )
                    eventStream.emit(.monthChanged(MonthChangeEvent(
                        year: monthKey.year,
                        month: monthKey.month,
                        action: .started
                    )))
                }

                guard let monthStore = activeStore else {
                    continue
                }

                do {
                    let context = AssetProcessContext(
                        asset: asset,
                        selectedResources: selectedResources,
                        cachedLocalHash: localHashCacheByAssetID[asset.localIdentifier],
                        monthStore: monthStore,
                        profile: profile,
                        assetPosition: currentPosition,
                        totalAssets: state.total
                    )

                    let result = try await assetProcessor.process(
                        context: context,
                        client: client,
                        eventStream: eventStream,
                        cancellationController: nil
                    )

                    switch result.status {
                    case .success:
                        state.succeeded += 1
                    case .failed:
                        state.failed += 1
                    case .skipped:
                        state.skipped += 1
                    }

                    emitProgress(
                        eventStream: eventStream,
                        state: state,
                        result: result,
                        position: currentPosition,
                        asset: asset
                    )
                    stageTimingWindow.record(result)
                    if let stageTimingSummary = stageTimingWindow.takeSummaryIfNeeded(
                        processed: state.processed,
                        total: state.total
                    ) {
                        eventStream.emit(.log(stageTimingSummary))
                    }
                } catch {
                    if error is CancellationError {
                        state.paused = true
                        break
                    }
                    if profile.isExternalStorageUnavailableError(error) {
                        eventStream.emit(.log("External storage unavailable. Stop backup immediately."))
                        throw error
                    }

                    state.failed += 1

                    let displayName = BackupAssetResourcePlanner.assetDisplayName(
                        asset: asset,
                        selectedResources: selectedResources
                    )
                    let errorMessage = profile.userFacingStorageErrorMessage(error)
                    eventStream.emit(.log("Failed asset: \(displayName) - \(errorMessage)"))
                    emitFailureProgress(
                        eventStream: eventStream,
                        state: state,
                        asset: asset,
                        displayName: displayName,
                        errorMessage: errorMessage,
                        position: currentPosition
                    )
                    stageTimingWindow.record(nil)
                    if let stageTimingSummary = stageTimingWindow.takeSummaryIfNeeded(
                        processed: state.processed,
                        total: state.total
                    ) {
                        eventStream.emit(.log(stageTimingSummary))
                    }
                }
            }

            if let finalStageTimingSummary = stageTimingWindow.takeSummaryIfNeeded(
                processed: state.processed,
                total: state.total,
                force: true
            ) {
                eventStream.emit(.log(finalStageTimingSummary))
            }

            if let activeStore {
                let shouldForceFlushOnTermination = state.paused && activeStore.dirty
                do {
                    _ = try await activeStore.flushToRemote(ignoreCancellation: shouldForceFlushOnTermination)
                    eventStream.emit(.monthChanged(MonthChangeEvent(
                        year: activeMonth?.year ?? 0,
                        month: activeMonth?.month ?? 0,
                        action: .flushed
                    )))
                    if shouldForceFlushOnTermination {
                        eventStream.emit(.log("Cancellation requested. Current month manifest flushed before exit."))
                    }
                } catch {
                    eventStream.emit(.monthChanged(MonthChangeEvent(
                        year: activeMonth?.year ?? 0,
                        month: activeMonth?.month ?? 0,
                        action: .flushFailed(error.localizedDescription)
                    )))
                }
            }

            let result = BackupExecutionResult(
                total: state.total,
                succeeded: state.succeeded,
                failed: state.failed,
                skipped: state.skipped,
                paused: state.paused
            )
            eventStream.emit(.finished(result))
            await disconnectClient(client)
            return result
        } catch {
            await disconnectClient(client)
            throw error
        }
    }

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String,
        eventStream: BackupEventStream? = nil
    ) async throws -> RemoteLibrarySnapshot {
        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        do {
            try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
            let snapshot = try await remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                eventStream: eventStream
            )
            eventStream?.emit(.log(
                "Remote index reloaded. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s)."
            ))
            await disconnectClient(client)
            return snapshot
        } catch {
            await disconnectClient(client)
            throw error
        }
    }

    func currentRemoteSnapshot() -> RemoteLibrarySnapshot {
        remoteIndexService.currentSnapshot()
    }

    func currentRemoteSnapshotState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        remoteIndexService.currentState(since: revision)
    }

    private func ensurePhotoAuthorization() async throws {
        let status = photoLibraryService.authorizationStatus()
        if status != .authorized && status != .limited {
            let requested = await photoLibraryService.requestAuthorization()
            guard requested == .authorized || requested == .limited else {
                throw BackupError.photoPermissionDenied
            }
        }
    }

    private func makeStorageClient(
        profile: ServerProfileRecord,
        password: String
    ) throws -> any RemoteStorageClientProtocol {
        try storageClientFactory.makeClient(profile: profile, password: password)
    }

    private func emitProgress(
        eventStream: BackupEventStream,
        state: BackupRunState,
        result: AssetProcessResult,
        position: Int,
        asset: PHAsset
    ) {
        let message = Self.message(for: result, position: position, total: state.total)
        let event = BackupItemEvent(
            assetLocalIdentifier: asset.localIdentifier,
            assetFingerprint: result.assetFingerprint,
            displayName: result.displayName,
            resourceDate: asset.creationDate,
            status: result.status,
            reason: result.reason,
            resourceSummary: result.resourceSummary,
            updatedAt: Date()
        )
        let progress = BackupProgress(
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            total: state.total,
            message: message,
            itemEvent: event,
            transferState: nil
        )
        eventStream.emit(.progress(progress))
    }

    private func emitFailureProgress(
        eventStream: BackupEventStream,
        state: BackupRunState,
        asset: PHAsset,
        displayName: String,
        errorMessage: String,
        position: Int
    ) {
        let message = "[\(position)/\(state.total)] Failed asset \(displayName)"
        let event = BackupItemEvent(
            assetLocalIdentifier: asset.localIdentifier,
            assetFingerprint: nil,
            displayName: displayName,
            resourceDate: asset.creationDate,
            status: .failed,
            reason: errorMessage,
            resourceSummary: "资源处理失败",
            updatedAt: Date()
        )
        let progress = BackupProgress(
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            total: state.total,
            message: message,
            itemEvent: event,
            transferState: nil
        )
        eventStream.emit(.progress(progress))
    }

    private static func message(for result: AssetProcessResult, position: Int, total: Int) -> String {
        let prefix = "[\(position)/\(max(total, 1))]"
        switch result.status {
        case .success:
            return "\(prefix) Asset done \(result.displayName)"
        case .failed:
            return "\(prefix) Asset failed \(result.displayName)"
        case .skipped:
            if let reason = result.reason {
                return "\(prefix) Asset skipped \(result.displayName) (\(reason))"
            }
            return "\(prefix) Asset skipped \(result.displayName)"
        }
    }

    private func disconnectClient(_ client: any RemoteStorageClientProtocol) async {
        if Task.isCancelled {
            let cleanupTask = Task.detached(priority: .utility) {
                await client.disconnect()
            }
            _ = await cleanupTask.value
            return
        }
        await client.disconnect()
    }

    private func makeMonthSeedLookup(from snapshot: RemoteLibrarySnapshot) -> MonthSeedLookup? {
        let lookup = MonthSeedLookup(snapshot: snapshot)
        return lookup.isEmpty ? nil : lookup
    }
}

private struct MonthSeedLookup {
    private let snapshot: RemoteLibrarySnapshot
    private let resourceRangesByMonth: [MonthKey: [Range<Int>]]
    private let assetRangesByMonth: [MonthKey: [Range<Int>]]
    private let linkRangesByMonth: [MonthKey: [Range<Int>]]

    var isEmpty: Bool {
        resourceRangesByMonth.isEmpty && assetRangesByMonth.isEmpty && linkRangesByMonth.isEmpty
    }

    init(snapshot: RemoteLibrarySnapshot) {
        self.snapshot = snapshot
        resourceRangesByMonth = Self.makeRangesByMonth(from: snapshot.resources) { resource in
            MonthKey(year: resource.year, month: resource.month)
        }
        assetRangesByMonth = Self.makeRangesByMonth(from: snapshot.assets) { asset in
            MonthKey(year: asset.year, month: asset.month)
        }
        linkRangesByMonth = Self.makeRangesByMonth(from: snapshot.assetResourceLinks) { link in
            MonthKey(year: link.year, month: link.month)
        }
    }

    func seed(for month: MonthKey) -> MonthManifestStore.Seed? {
        let resources = Self.materialize(from: snapshot.resources, ranges: resourceRangesByMonth[month])
        let assets = Self.materialize(from: snapshot.assets, ranges: assetRangesByMonth[month])
        let links = Self.materialize(from: snapshot.assetResourceLinks, ranges: linkRangesByMonth[month])
        guard !resources.isEmpty || !assets.isEmpty || !links.isEmpty else {
            return nil
        }
        return MonthManifestStore.Seed(
            resources: resources,
            assets: assets,
            assetResourceLinks: links
        )
    }

    private static func materialize<T>(
        from source: [T],
        ranges: [Range<Int>]?
    ) -> [T] {
        guard let ranges, !ranges.isEmpty else { return [] }
        if ranges.count == 1, let range = ranges.first {
            return Array(source[range])
        }

        var result: [T] = []
        let totalCount = ranges.reduce(into: 0) { partial, range in
            partial += range.count
        }
        result.reserveCapacity(totalCount)
        for range in ranges {
            result.append(contentsOf: source[range])
        }
        return result
    }

    private static func makeRangesByMonth<T>(
        from items: [T],
        month: (T) -> MonthKey
    ) -> [MonthKey: [Range<Int>]] {
        var result: [MonthKey: [Range<Int>]] = [:]
        result.reserveCapacity(32)
        guard !items.isEmpty else { return result }

        var start = 0
        var currentMonth = month(items[0])

        for index in 1 ..< items.count {
            let nextMonth = month(items[index])
            if nextMonth != currentMonth {
                result[currentMonth, default: []].append(start ..< index)
                start = index
                currentMonth = nextMonth
            }
        }
        result[currentMonth, default: []].append(start ..< items.count)
        return result
    }
}

private struct StageTimingWindow {
    private static let batchSize = 200

    private var processedCount = 0
    private var timedCount = 0
    private var exportHashSeconds: TimeInterval = 0
    private var collisionCheckSeconds: TimeInterval = 0
    private var uploadBodySeconds: TimeInterval = 0
    private var setModificationDateSeconds: TimeInterval = 0
    private var databaseSeconds: TimeInterval = 0
    private var totalFileSizeBytes: Int64 = 0
    private var uploadedFileSizeBytes: Int64 = 0

    mutating func record(_ result: AssetProcessResult?) {
        processedCount += 1
        guard let result else { return }
        timedCount += 1
        exportHashSeconds += result.timing.exportHashSeconds
        collisionCheckSeconds += result.timing.collisionCheckSeconds
        uploadBodySeconds += result.timing.uploadBodySeconds
        setModificationDateSeconds += result.timing.setModificationDateSeconds
        databaseSeconds += result.timing.databaseSeconds
        totalFileSizeBytes += max(result.totalFileSizeBytes, 0)
        uploadedFileSizeBytes += max(result.uploadedFileSizeBytes, 0)
    }

    mutating func takeSummaryIfNeeded(
        processed: Int,
        total: Int,
        force: Bool = false
    ) -> String? {
        guard processedCount > 0 else { return nil }
        guard force || processedCount >= Self.batchSize else { return nil }

        let perAssetDivisor = max(Double(timedCount), 1)
        let uploadRateBytesPerSecond = uploadBodySeconds > 0 ? Double(uploadedFileSizeBytes) / uploadBodySeconds : 0
        let prefix = force && processedCount < Self.batchSize
            ? "阶段耗时(最后\(processedCount)项"
            : "阶段耗时(最近\(processedCount)项"
        let summary = String(
            format: "%@, 进度%lld/%lld): size total=%@ uploaded=%@ bodyRate=%@/s, export/hash %.2fs (avg %.1fms), collision %.2fs (avg %.1fms), uploadBody %.2fs (avg %.1fms), setMtime %.2fs (avg %.1fms), db %.2fs (avg %.1fms), timedAssets=%lld",
            prefix,
            Int64(processed),
            Int64(max(total, 1)),
            Self.formatBytes(totalFileSizeBytes),
            Self.formatBytes(uploadedFileSizeBytes),
            Self.formatBytes(Int64(uploadRateBytesPerSecond.rounded())),
            exportHashSeconds,
            exportHashSeconds * 1_000 / perAssetDivisor,
            collisionCheckSeconds,
            collisionCheckSeconds * 1_000 / perAssetDivisor,
            uploadBodySeconds,
            uploadBodySeconds * 1_000 / perAssetDivisor,
            setModificationDateSeconds,
            setModificationDateSeconds * 1_000 / perAssetDivisor,
            databaseSeconds,
            databaseSeconds * 1_000 / perAssetDivisor,
            Int64(timedCount)
        )
        reset()
        return summary
    }

    private mutating func reset() {
        processedCount = 0
        timedCount = 0
        exportHashSeconds = 0
        collisionCheckSeconds = 0
        uploadBodySeconds = 0
        setModificationDateSeconds = 0
        databaseSeconds = 0
        totalFileSizeBytes = 0
        uploadedFileSizeBytes = 0
    }

    private static func formatBytes(_ value: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter.string(fromByteCount: max(0, value))
    }
}

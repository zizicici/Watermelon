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

enum ManifestFlushFailurePolicy: Sendable {
    case failRun
    case logAndContinue
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
        onlyAssetLocalIdentifiers: Set<String>? = nil,
        workerCountOverride: Int? = nil
    ) async throws -> BackupExecutionResult {
        try await runBackup(request: BackupRunRequest(
            profile: profile,
            password: password,
            onlyAssetLocalIdentifiers: onlyAssetLocalIdentifiers,
            workerCountOverride: workerCountOverride
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
    private let manifestFlushFailurePolicy: ManifestFlushFailurePolicy

    init(
        photoLibraryService: PhotoLibraryServiceProtocol,
        storageClientFactory: StorageClientFactoryProtocol,
        hashIndexRepository: ContentHashIndexRepositoryProtocol,
        remoteIndexService: RemoteIndexSyncService? = nil,
        assetProcessor: AssetProcessor? = nil,
        manifestFlushFailurePolicy: ManifestFlushFailurePolicy = .failRun
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
        self.manifestFlushFailurePolicy = manifestFlushFailurePolicy
    }

    func runBackup(request: BackupRunRequest, eventStream: BackupEventStream) async throws -> BackupExecutionResult {
        let profile = request.profile
        let password = request.password
        let onlyAssetLocalIdentifiers = request.onlyAssetLocalIdentifiers

        try await ensurePhotoAuthorization()

        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        var initialClientManagedByPool = false

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

            let allAssets: [PHAsset]
            if retryMode {
                allAssets = retryAssets
            } else if let assetsResult {
                var fetchedAssets: [PHAsset] = []
                fetchedAssets.reserveCapacity(assetsResult.count)
                for index in 0 ..< assetsResult.count {
                    fetchedAssets.append(assetsResult.object(at: index))
                }
                allAssets = fetchedAssets
            } else {
                allAssets = []
            }

            state.total = allAssets.count
            if state.total == 0 {
                let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
                eventStream.emit(.finished(result))
                await disconnectClient(client)
                return result
            }

            let monthPlans = Self.buildMonthPlans(
                assets: allAssets,
                localHashCacheByAssetID: localHashCacheByAssetID
            )
            let workerCount = Self.resolveWorkerCount(
                profile: profile,
                monthCount: monthPlans.count,
                override: request.workerCountOverride
            )
            let connectionPoolSize = Self.resolveConnectionPoolSize(
                profile: profile,
                workerCount: workerCount
            )
            let monthQueue = MonthWorkQueue(months: monthPlans)
            let workerCountSource = request.workerCountOverride == nil ? "protocol-default" : "user-override"
            eventStream.emit(.log(
                "Parallel month scheduler: month(s)=\(monthPlans.count), worker(s)=\(workerCount), connectionPool=\(connectionPoolSize), strategy=dynamic-pull, source=\(workerCountSource), storage=\(profile.resolvedStorageType.rawValue)."
            ))

            let aggregator = ParallelBackupProgressAggregator(total: state.total)
            let clientPool = StorageClientPool(
                maxConnections: connectionPoolSize,
                makeClient: { [self, profile, password] in
                    try makeStorageClient(profile: profile, password: password)
                }
            )
            await clientPool.seedConnectedClient(client)
            initialClientManagedByPool = true

            do {
                try await withThrowingTaskGroup(of: WorkerRunState.self) { group in
                    for workerID in 0 ..< workerCount {
                        group.addTask { [self] in
                            try await runParallelMonthWorker(
                                workerID: workerID,
                                monthQueue: monthQueue,
                                profile: profile,
                                snapshotSeedLookup: snapshotSeedLookup,
                                localHashCacheByAssetID: localHashCacheByAssetID,
                                eventStream: eventStream,
                                aggregator: aggregator,
                                clientPool: clientPool
                            )
                        }
                    }

                    for try await workerState in group where workerState.paused {
                        await aggregator.markPaused()
                    }
                }
            } catch {
                await clientPool.shutdown()
                if profile.isExternalStorageUnavailableError(error) {
                    eventStream.emit(.log("External storage unavailable. Stop backup immediately."))
                    throw error
                }
                throw error
            }

            await clientPool.shutdown()

            if let finalStageTimingSummary = await aggregator.finalTimingSummary() {
                eventStream.emit(.log(finalStageTimingSummary))
            }
            state = await aggregator.snapshot()

            let result = BackupExecutionResult(
                total: state.total,
                succeeded: state.succeeded,
                failed: state.failed,
                skipped: state.skipped,
                paused: state.paused
            )
            eventStream.emit(.finished(result))
            return result
        } catch {
            if !initialClientManagedByPool {
                await disconnectClient(client)
            }
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

    private static func buildMonthPlans(
        assets: [PHAsset],
        localHashCacheByAssetID: [String: LocalAssetHashCache]
    ) -> [MonthWorkItem] {
        var assetsByMonth: [MonthKey: [PHAsset]] = [:]
        assetsByMonth.reserveCapacity(32)

        var estimatedBytesByMonth: [MonthKey: Int64] = [:]
        estimatedBytesByMonth.reserveCapacity(32)

        for asset in assets {
            let monthKey = AssetProcessor.monthKey(for: asset.creationDate)
            assetsByMonth[monthKey, default: []].append(asset)

            if let cache = localHashCacheByAssetID[asset.localIdentifier] {
                estimatedBytesByMonth[monthKey, default: 0] += max(cache.totalFileSizeBytes, 0)
            }
        }

        var plans: [MonthWorkItem] = []
        plans.reserveCapacity(assetsByMonth.count)
        for (month, monthAssets) in assetsByMonth {
            plans.append(MonthWorkItem(
                month: month,
                assets: monthAssets,
                estimatedBytes: estimatedBytesByMonth[month] ?? 0
            ))
        }

        plans.sort { lhs, rhs in
            if lhs.estimatedBytes != rhs.estimatedBytes {
                return lhs.estimatedBytes > rhs.estimatedBytes
            }
            if lhs.assets.count != rhs.assets.count {
                return lhs.assets.count > rhs.assets.count
            }
            return lhs.month < rhs.month
        }

        return plans
    }

    private static func resolveWorkerCount(
        profile: ServerProfileRecord,
        monthCount: Int,
        override: Int?
    ) -> Int {
        let lowerBound = 1
        let upperBound = 4
        let protocolDefault: Int
        switch profile.resolvedStorageType {
        case .smb:
            protocolDefault = 2
        case .webdav:
            protocolDefault = 2
        case .externalVolume:
            protocolDefault = 3
        }

        let requested = override ?? protocolDefault
        let clampedByPolicy = max(lowerBound, min(upperBound, requested))
        let clampedByWorkload = max(lowerBound, min(clampedByPolicy, max(monthCount, 1)))
        return clampedByWorkload
    }

    private static func resolveConnectionPoolSize(
        profile: ServerProfileRecord,
        workerCount: Int
    ) -> Int {
        switch profile.resolvedStorageType {
        case .smb, .webdav:
            // Avoid opening too many network sessions when users force a high worker count.
            return max(1, min(workerCount, 2))
        case .externalVolume:
            return max(1, workerCount)
        }
    }

    private func runParallelMonthWorker(
        workerID: Int,
        monthQueue: MonthWorkQueue,
        profile: ServerProfileRecord,
        snapshotSeedLookup: MonthSeedLookup?,
        localHashCacheByAssetID: [String: LocalAssetHashCache],
        eventStream: BackupEventStream,
        aggregator: ParallelBackupProgressAggregator,
        clientPool: StorageClientPool
    ) async throws -> WorkerRunState {
        var workerState = WorkerRunState()
        let client = try await clientPool.acquire()
        var clientReusable = true
        do {
            while let monthPlan = await monthQueue.next() {
                if Task.isCancelled {
                    workerState.paused = true
                    break
                }

                let monthKey = monthPlan.month
                let monthStore: MonthManifestStore
                do {
                    monthStore = try await MonthManifestStore.loadOrCreate(
                        client: client,
                        basePath: profile.basePath,
                        year: monthKey.year,
                        month: monthKey.month,
                        seed: snapshotSeedLookup?.seed(for: monthKey)
                    )
                } catch {
                    if error is CancellationError {
                        workerState.paused = true
                        break
                    }
                    throw error
                }

                eventStream.emit(.log(
                    "Worker\(workerID + 1) claimed month \(monthKey.text), assets=\(monthPlan.assets.count), est=\(StageTimingWindow.formatBytes(monthPlan.estimatedBytes))."
                ))
                eventStream.emit(.monthChanged(MonthChangeEvent(
                    year: monthKey.year,
                    month: monthKey.month,
                    action: .started
                )))

                var monthFatalError: Error?

                for asset in monthPlan.assets {
                    if Task.isCancelled {
                        workerState.paused = true
                        break
                    }

                    let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
                        from: PHAssetResource.assetResources(for: asset)
                    )
                    if selectedResources.isEmpty {
                        await aggregator.reduceTotalForEmptyAsset()
                        continue
                    }

                    let dispatch = await aggregator.allocateDispatchSlot()

                    do {
                        let context = AssetProcessContext(
                            workerID: workerID + 1,
                            asset: asset,
                            selectedResources: selectedResources,
                            cachedLocalHash: localHashCacheByAssetID[asset.localIdentifier],
                            monthStore: monthStore,
                            profile: profile,
                            assetPosition: dispatch.position,
                            totalAssets: dispatch.total
                        )

                        let result = try await assetProcessor.process(
                            context: context,
                            client: client,
                            eventStream: eventStream,
                            cancellationController: nil
                        )
                        let progressState = await aggregator.record(result: result)

                        emitProgress(
                            eventStream: eventStream,
                            state: progressState.state,
                            result: result,
                            position: progressState.position,
                            asset: asset
                        )
                        if let timingSummary = progressState.timingSummary {
                            eventStream.emit(.log(timingSummary))
                        }
                    } catch {
                        if error is CancellationError {
                            workerState.paused = true
                            break
                        }
                        if profile.isExternalStorageUnavailableError(error) {
                            clientReusable = false
                            monthFatalError = error
                            break
                        }

                        let displayName = BackupAssetResourcePlanner.assetDisplayName(
                            asset: asset,
                            selectedResources: selectedResources
                        )
                        let errorMessage = profile.userFacingStorageErrorMessage(error)
                        eventStream.emit(.log("Failed asset: \(displayName) - \(errorMessage)"))

                        let progressState = await aggregator.recordFailure()
                        emitFailureProgress(
                            eventStream: eventStream,
                            state: progressState.state,
                            asset: asset,
                            displayName: displayName,
                            errorMessage: errorMessage,
                            position: progressState.position
                        )
                        if let timingSummary = progressState.timingSummary {
                            eventStream.emit(.log(timingSummary))
                        }
                    }
                }

                let shouldForceFlush = workerState.paused && monthStore.dirty
                do {
                    _ = try await monthStore.flushToRemote(ignoreCancellation: shouldForceFlush)
                    eventStream.emit(.monthChanged(MonthChangeEvent(
                        year: monthKey.year,
                        month: monthKey.month,
                        action: .flushed
                    )))
                    if shouldForceFlush {
                        eventStream.emit(.log(
                            "Worker\(workerID + 1): cancellation requested. Month \(monthKey.text) manifest flushed before exit."
                        ))
                    }
                } catch {
                    eventStream.emit(.monthChanged(MonthChangeEvent(
                        year: monthKey.year,
                        month: monthKey.month,
                        action: .flushFailed(error.localizedDescription)
                    )))
                    if manifestFlushFailurePolicy == .failRun {
                        throw error
                    }
                }

                if let monthFatalError {
                    throw monthFatalError
                }

                if workerState.paused {
                    break
                }
            }

            await clientPool.release(client, reusable: clientReusable)
            return workerState
        } catch {
            if profile.isExternalStorageUnavailableError(error) {
                clientReusable = false
            }
            await clientPool.release(client, reusable: clientReusable)
            throw error
        }
    }

    private func makeMonthSeedLookup(from snapshot: RemoteLibrarySnapshot) -> MonthSeedLookup? {
        let lookup = MonthSeedLookup(snapshot: snapshot)
        return lookup.isEmpty ? nil : lookup
    }
}

private actor StorageClientPool {
    private let maxConnections: Int
    private let makeClient: @Sendable () throws -> any RemoteStorageClientProtocol
    private var createdConnections = 0
    private var idleClients: [any RemoteStorageClientProtocol] = []
    private var waiters: [CheckedContinuation<any RemoteStorageClientProtocol, Error>] = []

    init(
        maxConnections: Int,
        makeClient: @escaping @Sendable () throws -> any RemoteStorageClientProtocol
    ) {
        self.maxConnections = max(1, maxConnections)
        self.makeClient = makeClient
    }

    func seedConnectedClient(_ client: any RemoteStorageClientProtocol) {
        guard createdConnections < maxConnections else { return }
        createdConnections += 1
        if !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            waiter.resume(returning: client)
        } else {
            idleClients.append(client)
        }
    }

    func acquire() async throws -> any RemoteStorageClientProtocol {
        if let client = idleClients.popLast() {
            return client
        }
        if createdConnections < maxConnections {
            createdConnections += 1
            do {
                let client = try makeClient()
                try await client.connect()
                return client
            } catch {
                createdConnections = max(createdConnections - 1, 0)
                throw error
            }
        }
        return try await withCheckedThrowingContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func release(_ client: any RemoteStorageClientProtocol, reusable: Bool) async {
        if reusable {
            if !waiters.isEmpty {
                let waiter = waiters.removeFirst()
                waiter.resume(returning: client)
            } else {
                idleClients.append(client)
            }
            return
        }

        createdConnections = max(createdConnections - 1, 0)
        await client.disconnect()
        guard !waiters.isEmpty else { return }
        let waiter = waiters.removeFirst()
        do {
            let replacement = try makeClient()
            try await replacement.connect()
            createdConnections += 1
            waiter.resume(returning: replacement)
        } catch {
            waiter.resume(throwing: error)
        }
    }

    func shutdown() async {
        let clients = idleClients
        idleClients.removeAll()
        let pendingWaiters = waiters
        waiters.removeAll()
        createdConnections = 0

        for waiter in pendingWaiters {
            waiter.resume(throwing: CancellationError())
        }
        for client in clients {
            await client.disconnect()
        }
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

private struct MonthWorkItem: @unchecked Sendable {
    let month: MonthKey
    let assets: [PHAsset]
    let estimatedBytes: Int64
}

private struct WorkerRunState: Sendable {
    var paused: Bool = false
}

private struct AggregatedProgressState: Sendable {
    let state: BackupRunState
    let position: Int
    let timingSummary: String?
}

private struct DispatchSlot: Sendable {
    let position: Int
    let total: Int
}

private actor MonthWorkQueue {
    private let months: [MonthWorkItem]
    private var nextIndex: Int = 0

    init(months: [MonthWorkItem]) {
        self.months = months
    }

    func next() -> MonthWorkItem? {
        guard nextIndex < months.count else { return nil }
        let month = months[nextIndex]
        nextIndex += 1
        return month
    }
}

private actor ParallelBackupProgressAggregator {
    private var state: BackupRunState
    private var stageTimingWindow = StageTimingWindow()
    private var scheduledCount = 0

    init(total: Int) {
        state = BackupRunState(total: total)
    }

    func allocateDispatchSlot() -> DispatchSlot {
        scheduledCount += 1
        return DispatchSlot(position: max(scheduledCount, 1), total: max(state.total, 1))
    }

    func reduceTotalForEmptyAsset() {
        state.total = max(state.total - 1, 0)
    }

    func record(result: AssetProcessResult) -> AggregatedProgressState {
        switch result.status {
        case .success:
            state.succeeded += 1
        case .failed:
            state.failed += 1
        case .skipped:
            state.skipped += 1
        }

        stageTimingWindow.record(result)
        let summary = stageTimingWindow.takeSummaryIfNeeded(
            processed: state.processed,
            total: state.total
        )
        return AggregatedProgressState(
            state: state,
            position: max(state.processed, 1),
            timingSummary: summary
        )
    }

    func recordFailure() -> AggregatedProgressState {
        state.failed += 1
        stageTimingWindow.record(nil)
        let summary = stageTimingWindow.takeSummaryIfNeeded(
            processed: state.processed,
            total: state.total
        )
        return AggregatedProgressState(
            state: state,
            position: max(state.processed, 1),
            timingSummary: summary
        )
    }

    func markPaused() {
        state.paused = true
    }

    func finalTimingSummary() -> String? {
        stageTimingWindow.takeSummaryIfNeeded(
            processed: state.processed,
            total: state.total,
            force: true
        )
    }

    func snapshot() -> BackupRunState {
        state
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
    private var firstRecordAt: CFAbsoluteTime?
    private var lastRecordAt: CFAbsoluteTime?
    private var firstUploadRecordAt: CFAbsoluteTime?
    private var lastUploadRecordAt: CFAbsoluteTime?

    mutating func record(_ result: AssetProcessResult?) {
        let now = CFAbsoluteTimeGetCurrent()
        if firstRecordAt == nil {
            firstRecordAt = now
        }
        lastRecordAt = now

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

        if result.uploadedFileSizeBytes > 0 {
            if firstUploadRecordAt == nil {
                firstUploadRecordAt = now
            }
            lastUploadRecordAt = now
        }
    }

    mutating func takeSummaryIfNeeded(
        processed: Int,
        total: Int,
        force: Bool = false
    ) -> String? {
        guard processedCount > 0 else { return nil }
        guard force || processedCount >= Self.batchSize else { return nil }

        let perAssetDivisor = max(Double(timedCount), 1)
        let uploadWallSeconds: TimeInterval = {
            guard let start = firstUploadRecordAt ?? firstRecordAt else { return 0 }
            guard let end = lastUploadRecordAt ?? lastRecordAt else { return 0 }
            return max(end - start, 0)
        }()
        let wallRateBytesPerSecond: Double = {
            guard uploadedFileSizeBytes > 0 else { return 0 }
            guard uploadWallSeconds > 0 else { return 0 }
            return Double(uploadedFileSizeBytes) / uploadWallSeconds
        }()
        let summedBodyRateBytesPerSecond: Double = {
            guard uploadedFileSizeBytes > 0 else { return 0 }
            guard uploadBodySeconds > 0 else { return 0 }
            return Double(uploadedFileSizeBytes) / uploadBodySeconds
        }()
        let prefix = force && processedCount < Self.batchSize
            ? "阶段耗时(最后\(processedCount)项"
            : "阶段耗时(最近\(processedCount)项"
        let summary = String(
            format: "%@, 进度%lld/%lld): size total=%@ uploaded=%@ rate=%@/s bodyRate=%@/s (wall %.2fs), export/hash %.2fs (avg %.1fms), collision %.2fs (avg %.1fms), uploadBody %.2fs (avg %.1fms), setMtime %.2fs (avg %.1fms), db %.2fs (avg %.1fms), timedAssets=%lld",
            prefix,
            Int64(processed),
            Int64(max(total, 1)),
            Self.formatBytes(totalFileSizeBytes),
            Self.formatBytes(uploadedFileSizeBytes),
            Self.formatBytes(Int64(wallRateBytesPerSecond.rounded())),
            Self.formatBytes(Int64(summedBodyRateBytesPerSecond.rounded())),
            uploadWallSeconds,
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
        firstRecordAt = nil
        lastRecordAt = nil
        firstUploadRecordAt = nil
        lastUploadRecordAt = nil
    }

    static func formatBytes(_ value: Int64) -> String {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useKB, .useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter.string(fromByteCount: max(0, value))
    }
}

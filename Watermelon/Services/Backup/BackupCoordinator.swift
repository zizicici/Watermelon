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
            var snapshotSeedByMonth: [MonthKey: MonthManifestStore.Seed] = [:]

            do {
                let snapshot = try await remoteIndexService.syncIndex(
                    client: client,
                    profile: profile,
                    eventStream: eventStream
                )
                snapshotSeedByMonth = buildMonthSeedMap(from: snapshot)
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
                        seed: snapshotSeedByMonth.removeValue(forKey: monthKey)
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
                }
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

    private func buildMonthSeedMap(from snapshot: RemoteLibrarySnapshot) -> [MonthKey: MonthManifestStore.Seed] {
        var resourcesByMonth: [MonthKey: [RemoteManifestResource]] = [:]
        resourcesByMonth.reserveCapacity(snapshot.resources.count)
        for resource in snapshot.resources {
            let month = MonthKey(year: resource.year, month: resource.month)
            resourcesByMonth[month, default: []].append(resource)
        }

        var assetsByMonth: [MonthKey: [RemoteManifestAsset]] = [:]
        assetsByMonth.reserveCapacity(snapshot.assets.count)
        for asset in snapshot.assets {
            let month = MonthKey(year: asset.year, month: asset.month)
            assetsByMonth[month, default: []].append(asset)
        }

        var linksByMonth: [MonthKey: [RemoteAssetResourceLink]] = [:]
        linksByMonth.reserveCapacity(snapshot.assetResourceLinks.count)
        for link in snapshot.assetResourceLinks {
            let month = MonthKey(year: link.year, month: link.month)
            linksByMonth[month, default: []].append(link)
        }

        let allMonths = Set(resourcesByMonth.keys)
            .union(assetsByMonth.keys)
            .union(linksByMonth.keys)

        var result: [MonthKey: MonthManifestStore.Seed] = [:]
        result.reserveCapacity(allMonths.count)
        for month in allMonths {
            result[month] = MonthManifestStore.Seed(
                resources: resourcesByMonth[month] ?? [],
                assets: assetsByMonth[month] ?? [],
                assetResourceLinks: linksByMonth[month] ?? []
            )
        }
        return result
    }
}

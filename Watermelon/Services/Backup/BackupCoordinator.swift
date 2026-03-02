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
    func runBackup(
        profile: ServerProfileRecord,
        password: String,
        onlyAssetLocalIdentifiers: Set<String>?
    ) async throws -> BackupExecutionResult

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String
    ) async throws -> RemoteLibrarySnapshot

    func currentRemoteSnapshot() -> RemoteLibrarySnapshot
    func currentRemoteSnapshotState(since revision: UInt64?) -> RemoteLibrarySnapshotState
    var eventStream: AsyncStream<BackupEvent> { get }
}

final class BackupCoordinator: BackupCoordinatorProtocol, Sendable {
    private let photoLibraryService: PhotoLibraryServiceProtocol
    private let storageClientFactory: StorageClientFactoryProtocol
    private let hashIndexRepository: ContentHashIndexRepositoryProtocol
    private let remoteIndexService: RemoteIndexSyncService
    private let assetProcessor: AssetProcessor
    private let eventStreamActor = BackupEventStream()

    var eventStream: AsyncStream<BackupEvent> {
        eventStreamActor.stream
    }

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

    deinit {
        eventStreamActor.finish()
    }

    func runBackup(
        profile: ServerProfileRecord,
        password: String,
        onlyAssetLocalIdentifiers: Set<String>? = nil
    ) async throws -> BackupExecutionResult {
        try await ensurePhotoAuthorization()

        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        defer {
            Task { await client.disconnect() }
        }

        try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))

        do {
            let snapshot = try await remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                eventStream: eventStreamActor
            )
            await eventStreamActor.emit(.log(
                "Remote index synced. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s)."
            ))
        } catch {
            if profile.isExternalStorageUnavailableError(error) {
                throw error
            }
            await eventStreamActor.emit(.log("Remote index scan warning: \(error.localizedDescription)"))
        }

        let assetsResult = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        let retryMode = onlyAssetLocalIdentifiers != nil

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

        var state = BackupRunState(total: retryMode ? retryAssets.count : assetsResult.count)

        if retryMode {
            let requested = onlyAssetLocalIdentifiers?.count ?? 0
            let missing = max(requested - retryAssets.count, 0)
            await eventStreamActor.emit(.log(
                "Retry mode: requested \(requested), resolved \(retryAssets.count), missing \(missing)."
            ))
        } else {
            await eventStreamActor.emit(.log("Start backup by asset (oldest month first)."))
        }

        await eventStreamActor.emit(.started(totalAssets: state.total))

        if state.total == 0 {
            let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
            await eventStreamActor.emit(.finished(result))
            return result
        }

        let localHashCacheByAssetID: [String: LocalAssetHashCache]
        do {
            localHashCacheByAssetID = try hashIndexRepository.fetchAssetHashCaches()
        } catch {
            localHashCacheByAssetID = [:]
            await eventStreamActor.emit(.log("Local hash cache load warning: \(error.localizedDescription)"))
        }

        var activeMonth: MonthKey?
        var activeStore: MonthManifestStore?

        let loopCount = retryMode ? retryAssets.count : assetsResult.count
        for loopIndex in 0 ..< loopCount {
            if Task.isCancelled {
                state.paused = true
                break
            }

            let asset = retryMode ? retryAssets[loopIndex] : assetsResult.object(at: loopIndex)
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
                        await eventStreamActor.emit(.monthChanged(MonthChangeEvent(
                            year: activeMonth?.year ?? 0,
                            month: activeMonth?.month ?? 0,
                            action: .flushed
                        )))
                    } catch {
                        await eventStreamActor.emit(.monthChanged(MonthChangeEvent(
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
                    month: monthKey.month
                )
                await eventStreamActor.emit(.monthChanged(MonthChangeEvent(
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
                    eventStream: eventStreamActor,
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

                await emitProgress(state: state, result: result, position: currentPosition, asset: asset)
            } catch {
                if error is CancellationError {
                    state.paused = true
                    break
                }
                if profile.isExternalStorageUnavailableError(error) {
                    await eventStreamActor.emit(.log("External storage unavailable. Stop backup immediately."))
                    throw error
                }

                state.failed += 1

                let displayName = BackupAssetResourcePlanner.assetDisplayName(
                    asset: asset,
                    selectedResources: selectedResources
                )
                let errorMessage = profile.userFacingStorageErrorMessage(error)
                await eventStreamActor.emit(.log("Failed asset: \(displayName) - \(errorMessage)"))
                await emitFailureProgress(
                    state: state,
                    asset: asset,
                    displayName: displayName,
                    errorMessage: errorMessage,
                    position: currentPosition
                )
            }
        }

        if let activeStore {
            do {
                _ = try await activeStore.flushToRemote()
                await eventStreamActor.emit(.monthChanged(MonthChangeEvent(
                    year: activeMonth?.year ?? 0,
                    month: activeMonth?.month ?? 0,
                    action: .flushed
                )))
            } catch {
                await eventStreamActor.emit(.monthChanged(MonthChangeEvent(
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
        await eventStreamActor.emit(.finished(result))
        return result
    }

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String
    ) async throws -> RemoteLibrarySnapshot {
        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        defer {
            Task { await client.disconnect() }
        }

        try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        let snapshot = try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            eventStream: eventStreamActor
        )
        await eventStreamActor.emit(.log(
            "Remote index reloaded. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s)."
        ))
        return snapshot
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
        state: BackupRunState,
        result: AssetProcessResult,
        position: Int,
        asset: PHAsset
    ) async {
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
        await eventStreamActor.emit(.progress(progress))
        await eventStreamActor.emit(.assetCompleted(AssetCompletionEvent(
            assetLocalIdentifier: asset.localIdentifier,
            assetFingerprint: result.assetFingerprint,
            displayName: result.displayName,
            status: result.status,
            reason: result.reason,
            resourceSummary: result.resourceSummary,
            position: position,
            total: state.total
        )))
    }

    private func emitFailureProgress(
        state: BackupRunState,
        asset: PHAsset,
        displayName: String,
        errorMessage: String,
        position: Int
    ) async {
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
        await eventStreamActor.emit(.progress(progress))
        await eventStreamActor.emit(.assetCompleted(AssetCompletionEvent(
            assetLocalIdentifier: asset.localIdentifier,
            assetFingerprint: nil,
            displayName: displayName,
            status: .failed,
            reason: errorMessage,
            resourceSummary: "资源处理失败",
            position: position,
            total: state.total
        )))
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
}

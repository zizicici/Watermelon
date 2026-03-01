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

struct BackupRunContext: Sendable {
    let cancellationController: BackupCancellationController
    let eventSink: BackupEventStream
}

protocol BackupCoordinatorProtocol: Sendable {
    func runBackup(
        profile: ServerProfileRecord,
        password: String,
        onlyAssetLocalIdentifiers: Set<String>?,
        context: BackupRunContext
    ) async throws -> BackupExecutionResult

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String
    ) async throws -> RemoteLibrarySnapshot

    func currentRemoteSnapshot() -> RemoteLibrarySnapshot
    func currentRemoteSnapshotState(since revision: UInt64?) -> RemoteLibrarySnapshotState
}

final class BackupCoordinator: BackupCoordinatorProtocol, @unchecked Sendable {
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

    func runBackup(
        profile: ServerProfileRecord,
        password: String,
        onlyAssetLocalIdentifiers: Set<String>? = nil,
        context: BackupRunContext
    ) async throws -> BackupExecutionResult {
        let cancellationController = context.cancellationController
        let eventSink = context.eventSink

        try await ensurePhotoAuthorization()
        try cancellationController.throwIfCancelled()

        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        defer {
            Task.detached(priority: .utility) { [client] in
                await client.disconnect()
            }
        }

        try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        await emitStorageCapacityHint(client: client, eventSink: eventSink)

        do {
            let snapshot = try await remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                eventStream: eventSink,
                cancellationController: cancellationController
            )
            eventSink.emit(.log(
                "Remote index synced. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s)."
            ))
        } catch {
            if error is CancellationError {
                throw CancellationError()
            }
            if profile.isExternalStorageUnavailableError(error) {
                throw error
            }
            eventSink.emit(.log("Remote index scan warning: \(error.localizedDescription)"))
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
            eventSink.emit(.log(
                "Retry mode: requested \(requested), resolved \(retryAssets.count), missing \(missing)."
            ))
        } else {
            eventSink.emit(.log("Start backup by asset (oldest month first)."))
        }

        eventSink.emit(.started(totalAssets: state.total))

        if state.total == 0 {
            let result = BackupExecutionResult(total: 0, succeeded: 0, failed: 0, skipped: 0, paused: false)
            eventSink.emit(.finished(result))
            return result
        }

        let localHashCacheByAssetID: [String: LocalAssetHashCache]
        do {
            localHashCacheByAssetID = try hashIndexRepository.fetchAssetHashCaches()
        } catch {
            localHashCacheByAssetID = [:]
            eventSink.emit(.log("Local hash cache load warning: \(error.localizedDescription)"))
        }

        var activeMonth: MonthKey?
        var activeStore: MonthManifestStore?

        let loopCount = retryMode ? retryAssets.count : assetsResult.count
        for loopIndex in 0 ..< loopCount {
            if Task.isCancelled || cancellationController.isCancelled {
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

            do {
                try cancellationController.throwIfCancelled()
                try Task.checkCancellation()

                if activeMonth != monthKey {
                    if let activeStore {
                        await flushMonthManifest(
                            activeStore,
                            monthKey: activeMonth,
                            ignoreCancellation: false,
                            eventSink: eventSink
                        )
                    }

                    try cancellationController.throwIfCancelled()
                    try Task.checkCancellation()

                    activeMonth = monthKey
                    activeStore = try await MonthManifestStore.loadOrCreate(
                        client: client,
                        basePath: profile.basePath,
                        year: monthKey.year,
                        month: monthKey.month
                    )
                    eventSink.emit(.monthChanged(MonthChangeEvent(
                        year: monthKey.year,
                        month: monthKey.month,
                        action: .started
                    )))
                }

                guard let monthStore = activeStore else {
                    continue
                }

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
                    eventStream: eventSink,
                    cancellationController: cancellationController
                )

                switch result.status {
                case .success:
                    state.succeeded += 1
                case .failed:
                    state.failed += 1
                case .skipped:
                    state.skipped += 1
                }

                emitProgress(state: state, result: result, position: currentPosition, asset: asset, eventSink: eventSink)
            } catch {
                if error is CancellationError {
                    state.paused = true
                    break
                }
                if profile.isExternalStorageUnavailableError(error) {
                    eventSink.emit(.log("External storage unavailable. Stop backup immediately."))
                    throw error
                }

                state.failed += 1

                let displayName = BackupAssetResourcePlanner.assetDisplayName(
                    asset: asset,
                    selectedResources: selectedResources
                )
                let errorMessage = profile.userFacingStorageErrorMessage(error)
                eventSink.emit(.log("Failed asset: \(displayName) - \(errorMessage)"))
                emitFailureProgress(
                    state: state,
                    asset: asset,
                    displayName: displayName,
                    errorMessage: errorMessage,
                    position: currentPosition,
                    eventSink: eventSink
                )
            }
        }

        if let activeStore {
            await flushMonthManifest(
                activeStore,
                monthKey: activeMonth,
                ignoreCancellation: state.paused,
                eventSink: eventSink
            )
        }

        let result = BackupExecutionResult(
            total: state.total,
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            paused: state.paused
        )
        eventSink.emit(.finished(result))
        return result
    }

    private func flushMonthManifest(
        _ store: MonthManifestStore,
        monthKey: MonthKey?,
        ignoreCancellation: Bool,
        eventSink: BackupEventStream
    ) async {
        do {
            _ = try await store.flushToRemote(ignoreCancellation: ignoreCancellation)
            eventSink.emit(.monthChanged(MonthChangeEvent(
                year: monthKey?.year ?? 0,
                month: monthKey?.month ?? 0,
                action: .flushed
            )))
        } catch {
            eventSink.emit(.monthChanged(MonthChangeEvent(
                year: monthKey?.year ?? 0,
                month: monthKey?.month ?? 0,
                action: .flushFailed(error.localizedDescription)
            )))
        }
    }

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String
    ) async throws -> RemoteLibrarySnapshot {
        let client = try makeStorageClient(profile: profile, password: password)
        try await client.connect()
        defer {
            Task.detached(priority: .utility) { [client] in
                await client.disconnect()
            }
        }

        try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        return try await remoteIndexService.syncIndex(
            client: client,
            profile: profile,
            eventStream: nil,
            cancellationController: nil
        )
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

    private func emitStorageCapacityHint(
        client: any RemoteStorageClientProtocol,
        eventSink: BackupEventStream
    ) async {
        do {
            guard let capacity = try await client.storageCapacity() else { return }
            let availableText = capacity.availableBytes.map(Self.formatByteCount) ?? "未知"
            let totalText = capacity.totalBytes.map(Self.formatByteCount) ?? "未知"
            eventSink.emit(.log("远端容量：可用 \(availableText) / 总量 \(totalText)"))

            if let available = capacity.availableBytes, available < Self.lowCapacityWarningThresholdBytes {
                eventSink.emit(.log("警告：远端可用空间不足（低于 \(Self.formatByteCount(Self.lowCapacityWarningThresholdBytes))），备份中断风险较高。"))
            }
        } catch {
            eventSink.emit(.log("远端容量读取失败：\(error.localizedDescription)"))
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
        asset: PHAsset,
        eventSink: BackupEventStream
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
        eventSink.emit(.progress(progress))
    }

    private func emitFailureProgress(
        state: BackupRunState,
        asset: PHAsset,
        displayName: String,
        errorMessage: String,
        position: Int,
        eventSink: BackupEventStream
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
        eventSink.emit(.progress(progress))
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

    private static func formatByteCount(_ bytes: Int64) -> String {
        byteCountFormatter.string(fromByteCount: max(bytes, 0))
    }

    private static let lowCapacityWarningThresholdBytes: Int64 = 1_000_000_000

    private static let byteCountFormatter: ByteCountFormatter = {
        let formatter = ByteCountFormatter()
        formatter.allowedUnits = [.useMB, .useGB, .useTB]
        formatter.countStyle = .file
        formatter.includesUnit = true
        formatter.isAdaptive = true
        return formatter
    }()
}

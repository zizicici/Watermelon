import CryptoKit
import Foundation
import Photos

struct BackupExecutionResult {
    let total: Int
    let succeeded: Int
    let failed: Int
    let skipped: Int
    let paused: Bool
}

final class BackupExecutor {
    private struct RunState {
        var total: Int = 0
        var succeeded: Int = 0
        var failed: Int = 0
        var skipped: Int = 0
        var paused: Bool = false

        var processed: Int {
            succeeded + failed + skipped
        }
    }

    private struct MonthKey: Hashable, Comparable {
        let year: Int
        let month: Int

        var text: String {
            String(format: "%04d-%02d", year, month)
        }

        static func < (lhs: MonthKey, rhs: MonthKey) -> Bool {
            if lhs.year == rhs.year {
                return lhs.month < rhs.month
            }
            return lhs.year < rhs.year
        }
    }

    private struct PreparedResource {
        let local: LocalPhotoResource
        let tempFileURL: URL
        let contentHash: Data
        let fileSize: Int64
        let shotDate: Date?
    }

    private struct ResourceUploadResult {
        let status: BackupItemStatus
        let reason: String?
    }

    private struct AssetProcessingResult {
        let status: BackupItemStatus
        let reason: String?
        let displayName: String
        let resourceSummary: String
        let assetFingerprint: Data?
    }

    private static let monthCalendar = Calendar(identifier: .gregorian)
    private static let smallFileThresholdBytes: Int64 = 5 * 1024 * 1024

    private let photoLibraryService: PhotoLibraryService
    private let storageClientFactory: StorageClientFactoryProtocol
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let remoteManifestIndexScanner: RemoteManifestIndexScanner
    private let remoteSnapshotCache = RemoteLibrarySnapshotCache()
    private var activeRemoteProfileKey: String?
    private var remoteManifestDigests: [LibraryMonthKey: RemoteMonthManifestDigest] = [:]

    init(
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryService,
        storageClientFactory: StorageClientFactoryProtocol = StorageClientFactory()
    ) {
        self.photoLibraryService = photoLibraryService
        self.storageClientFactory = storageClientFactory
        contentHashIndexRepository = ContentHashIndexRepository(databaseManager: databaseManager)
        remoteManifestIndexScanner = RemoteManifestIndexScanner()
    }

    func runBackup(
        profile: ServerProfileRecord,
        password: String,
        appVersion _: String,
        onlyAssetLocalIdentifiers: Set<String>? = nil,
        onProgress: @escaping @MainActor (BackupProgress) -> Void,
        onLog: @escaping @MainActor (String) -> Void
    ) async throws -> BackupExecutionResult {
        try await ensurePhotoAuthorization()

        let smbClient = try makeStorageClient(profile: profile, password: password)
        try await smbClient.connect()
        defer {
            Task { await smbClient.disconnect() }
        }

        try await smbClient.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))

        do {
            let snapshot = try await syncRemoteIndexIncrementally(
                client: smbClient,
                profile: profile,
                onLog: onLog
            )
            await onLog("Remote index synced. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s).")
        } catch {
            if profile.isExternalStorageUnavailableError(error) {
                throw error
            }
            await onLog("Remote index scan warning: \(error.localizedDescription)")
        }

        let assetsResult = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        let retryTargets = onlyAssetLocalIdentifiers
        let retryMode = retryTargets != nil

        var retryAssets: [PHAsset] = []
        if let retryTargets {
            let fetched = PHAsset.fetchAssets(withLocalIdentifiers: Array(retryTargets), options: nil)
            retryAssets.reserveCapacity(fetched.count)
            for index in 0 ..< fetched.count {
                retryAssets.append(fetched.object(at: index))
            }
            retryAssets.sort { lhs, rhs in
                (lhs.creationDate ?? .distantPast) < (rhs.creationDate ?? .distantPast)
            }
        }

        var state = RunState(total: retryMode ? retryAssets.count : assetsResult.count)

        if retryMode {
            let requested = retryTargets?.count ?? 0
            let missing = max(requested - retryAssets.count, 0)
            await onLog("Retry mode: requested \(requested), resolved \(retryAssets.count), missing \(missing).")
        } else {
            await onLog("Start backup by asset (oldest month first).")
        }

        if state.total == 0 {
            await onProgress(progressSnapshot(state: state, message: "No asset to process"))
            return BackupExecutionResult(
                total: 0,
                succeeded: 0,
                failed: 0,
                skipped: 0,
                paused: false
            )
        }

        let localHashCacheByAssetID: [String: LocalAssetHashCache]
        do {
            localHashCacheByAssetID = try contentHashIndexRepository.fetchAssetHashCaches()
        } catch {
            localHashCacheByAssetID = [:]
            await onLog("Local hash cache load warning: \(error.localizedDescription)")
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
            let selectedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(from:
                PHAssetResource.assetResources(for: asset)
            )
            if selectedResources.isEmpty {
                state.total = max(state.total - 1, 0)
                continue
            }

            let currentPosition = state.processed + 1

            let monthKey = Self.monthKey(for: asset.creationDate)
            if activeMonth != monthKey {
                if let activeStore {
                    do {
                        _ = try await activeStore.flushToRemote()
                        await onLog("Month \(activeMonth?.text ?? "unknown") manifest flushed.")
                    } catch {
                        await onLog("Month \(activeMonth?.text ?? "unknown") manifest flush failed: \(error.localizedDescription)")
                    }
                }

                activeMonth = monthKey
                activeStore = try await MonthManifestStore.loadOrCreate(
                    client: smbClient,
                    basePath: profile.basePath,
                    year: monthKey.year,
                    month: monthKey.month
                )
                await onLog("Processing month \(monthKey.text).")
            }

            guard let monthStore = activeStore else {
                continue
            }

            do {
                let result = try await processAsset(
                    asset: asset,
                    selectedResources: selectedResources,
                    cachedLocalHash: localHashCacheByAssetID[asset.localIdentifier],
                    monthStore: monthStore,
                    profile: profile,
                    smbClient: smbClient,
                    assetPosition: currentPosition,
                    totalAssets: state.total,
                    onTransferProgress: { [state] transferState in
                        onProgress(
                            self.progressSnapshot(
                                state: state,
                                message: Self.transferMessage(for: transferState),
                                transferState: transferState
                            )
                        )
                    },
                    onLog: onLog
                )

                switch result.status {
                case .success:
                    state.succeeded += 1
                case .failed:
                    state.failed += 1
                case .skipped:
                    state.skipped += 1
                }

                await onProgress(
                    progressSnapshot(
                        state: state,
                        message: message(for: result, position: currentPosition, total: state.total),
                        itemEvent: Self.event(
                            assetLocalIdentifier: asset.localIdentifier,
                            assetFingerprint: result.assetFingerprint,
                            displayName: result.displayName,
                            resourceDate: asset.creationDate,
                            status: result.status,
                            reason: result.reason,
                            resourceSummary: result.resourceSummary
                        )
                    )
                )
            } catch {
                if error is CancellationError {
                    state.paused = true
                    break
                }
                if profile.isExternalStorageUnavailableError(error) {
                    await onLog("External storage unavailable. Stop backup immediately.")
                    throw error
                }

                state.failed += 1

                let displayName = BackupAssetResourcePlanner.assetDisplayName(asset: asset, selectedResources: selectedResources)
                let errorMessage = profile.userFacingStorageErrorMessage(error)
                await onLog("Failed asset: \(displayName) - \(errorMessage)")
                await onProgress(
                    progressSnapshot(
                        state: state,
                        message: "[\(currentPosition)/\(state.total)] Failed asset \(displayName)",
                        itemEvent: Self.event(
                            assetLocalIdentifier: asset.localIdentifier,
                            assetFingerprint: nil,
                            displayName: displayName,
                            resourceDate: asset.creationDate,
                            status: .failed,
                            reason: errorMessage,
                            resourceSummary: "资源处理失败"
                        )
                    )
                )
            }
        }

        if let activeStore {
            do {
                _ = try await activeStore.flushToRemote()
                await onLog("Month \(activeMonth?.text ?? "unknown") manifest flushed.")
            } catch {
                await onLog("Month \(activeMonth?.text ?? "unknown") manifest flush failed: \(error.localizedDescription)")
            }
        }

        let finalMessage = state.paused
            ? "Backup paused"
            : "Backup finished"
        await onProgress(progressSnapshot(state: state, message: finalMessage))

        return BackupExecutionResult(
            total: state.total,
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            paused: state.paused
        )
    }

    func reloadRemoteIndex(
        profile: ServerProfileRecord,
        password: String,
        onLog: (@MainActor (String) -> Void)? = nil
    ) async throws -> RemoteLibrarySnapshot {
        let smbClient = try makeStorageClient(profile: profile, password: password)
        try await smbClient.connect()
        defer {
            Task { await smbClient.disconnect() }
        }

        try await smbClient.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        let snapshot = try await syncRemoteIndexIncrementally(
            client: smbClient,
            profile: profile,
            onLog: onLog
        )
        if let onLog {
            await onLog("Remote index reloaded. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s).")
        }
        return snapshot
    }

    func currentRemoteSnapshot() -> RemoteLibrarySnapshot {
        remoteSnapshotCache.current()
    }

    func currentRemoteSnapshotState(since revision: UInt64?) -> RemoteLibrarySnapshotState {
        remoteSnapshotCache.state(since: revision)
    }

    private func upsertCachedRemoteSnapshotResource(_ item: RemoteManifestResource) {
        remoteSnapshotCache.upsertResource(item)
    }

    private func upsertCachedRemoteSnapshotAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]? = nil) {
        remoteSnapshotCache.upsertAsset(asset, links: links)
    }

    private func syncRemoteIndexIncrementally(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        onLog: (@MainActor (String) -> Void)? = nil
    ) async throws -> RemoteLibrarySnapshot {
        ensureRemoteContext(for: profile)

        let remoteDigests = try await remoteManifestIndexScanner.scanManifestDigests(
            client: client,
            basePath: profile.basePath
        )
        let previousDigests = remoteManifestDigests

        let previousMonths = Set(previousDigests.keys)
        let remoteMonths = Set(remoteDigests.keys)

        var changedMonths = Set<LibraryMonthKey>()
        if previousDigests.isEmpty {
            changedMonths = remoteMonths
        } else {
            for (month, digest) in remoteDigests where previousDigests[month] != digest {
                changedMonths.insert(month)
            }
        }

        let removedMonths = previousMonths.subtracting(remoteMonths)

        if changedMonths.isEmpty, removedMonths.isEmpty {
            if let onLog {
                await onLog("Remote index unchanged. Month digests matched (\(remoteMonths.count) month(s)).")
            }
            return remoteSnapshotCache.current()
        }

        var monthDeltas: [LibraryMonthKey: RemoteLibraryMonthDelta] = [:]
        monthDeltas.reserveCapacity(changedMonths.count)

        for month in changedMonths.sorted() {
            guard let store = try await MonthManifestStore.loadManifestOnlyIfExists(
                client: client,
                basePath: profile.basePath,
                year: month.year,
                month: month.month
            ) else {
                throw NSError(
                    domain: "BackupExecutor",
                    code: -21,
                    userInfo: [NSLocalizedDescriptionKey: "Month manifest is missing for \(month.text)."]
                )
            }

            let monthAssets = store.allAssets()
            let monthLinks = monthAssets.flatMap { store.links(forAssetFingerprint: $0.assetFingerprint) }
            monthDeltas[month] = RemoteLibraryMonthDelta(
                month: month,
                resources: store.allItems(),
                assets: monthAssets,
                assetResourceLinks: monthLinks
            )
        }

        var appliedChangedMonths = 0
        var appliedRemovedMonths = 0

        for month in changedMonths.sorted() {
            guard let monthDelta = monthDeltas[month] else { continue }
            if remoteSnapshotCache.replaceMonth(
                month,
                resources: monthDelta.resources,
                assets: monthDelta.assets,
                assetResourceLinks: monthDelta.assetResourceLinks
            ) {
                appliedChangedMonths += 1
            }
        }

        for month in removedMonths.sorted() {
            if remoteSnapshotCache.removeMonth(month) {
                appliedRemovedMonths += 1
            }
        }

        remoteManifestDigests = remoteDigests

        if let onLog {
            await onLog(
                "Remote incremental sync: scanned=\(remoteMonths.count) changed=\(appliedChangedMonths) removed=\(appliedRemovedMonths)."
            )
        }

        return remoteSnapshotCache.current()
    }

    private func ensureRemoteContext(for profile: ServerProfileRecord) {
        let profileKey = Self.remoteProfileKey(profile)
        guard activeRemoteProfileKey != profileKey else { return }

        activeRemoteProfileKey = profileKey
        remoteManifestDigests.removeAll()
        remoteSnapshotCache.reset()
    }

    private func processAsset(
        asset: PHAsset,
        selectedResources: [BackupSelectedResource],
        cachedLocalHash: LocalAssetHashCache?,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        smbClient: RemoteStorageClientProtocol,
        assetPosition: Int,
        totalAssets: Int,
        onTransferProgress: @escaping @MainActor (BackupTransferState) async -> Void,
        onLog: @escaping @MainActor (String) -> Void
    ) async throws -> AssetProcessingResult {
        var preparedResources: [PreparedResource] = []
        preparedResources.reserveCapacity(selectedResources.count)

        defer {
            for prepared in preparedResources {
                try? FileManager.default.removeItem(at: prepared.tempFileURL)
            }
        }

        let displayName = BackupAssetResourcePlanner.assetDisplayName(asset: asset, selectedResources: selectedResources)

        if let cachedResult = try processAssetWithLocalCache(
            asset: asset,
            selectedResources: selectedResources,
            cachedLocalHash: cachedLocalHash,
            monthStore: monthStore,
            displayName: displayName
        ) {
            return cachedResult
        }

        for (resourcePosition, selected) in selectedResources.enumerated() {
            try Task.checkCancellation()

            let local = makeLocalResource(asset: asset, selected: selected)
            await onTransferProgress(
                Self.makeTransferState(
                    assetLocalIdentifier: asset.localIdentifier,
                    assetDisplayName: displayName,
                    resourceDate: local.asset.creationDate ?? local.resourceModificationDate,
                    assetPosition: assetPosition,
                    totalAssets: totalAssets,
                    resourceDisplayName: local.originalFilename,
                    resourcePosition: resourcePosition + 1,
                    totalResources: selectedResources.count,
                    resourceFraction: 0,
                    stageDescription: "准备资源"
                )
            )

            let tempFileURL = try await photoLibraryService.exportResourceToTempFile(local.resource)
            var shouldRemoveTempFile = true
            defer {
                if shouldRemoveTempFile {
                    try? FileManager.default.removeItem(at: tempFileURL)
                }
            }
            let localHash = try Self.contentHash(of: tempFileURL)
            let localFileSize = max(
                local.fileSize,
                (try? FileManager.default.attributesOfItem(atPath: tempFileURL.path)[.size] as? NSNumber)?.int64Value ?? 0
            )
            let shotDate = local.asset.creationDate ?? local.resourceModificationDate
            if let shotDate {
                try? FileManager.default.setAttributes([.modificationDate: shotDate], ofItemAtPath: tempFileURL.path)
            }

            try contentHashIndexRepository.upsertAssetResource(
                assetLocalIdentifier: local.assetLocalIdentifier,
                role: local.resourceRole,
                slot: local.resourceSlot,
                contentHash: localHash
            )

            preparedResources.append(
                PreparedResource(
                    local: local,
                    tempFileURL: tempFileURL,
                    contentHash: localHash,
                    fileSize: localFileSize,
                    shotDate: shotDate
                )
            )
            shouldRemoveTempFile = false
        }

        let assetFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: preparedResources.map {
                (role: $0.local.resourceRole, slot: $0.local.resourceSlot, contentHash: $0.contentHash)
            }
        )

        if monthStore.containsAssetFingerprint(assetFingerprint) {
            try contentHashIndexRepository.upsertAssetFingerprint(
                assetLocalIdentifier: asset.localIdentifier,
                assetFingerprint: assetFingerprint,
                resourceCount: preparedResources.count
            )
            return AssetProcessingResult(
                status: .skipped,
                reason: "asset_exists",
                displayName: displayName,
                resourceSummary: "资源\(preparedResources.count) 已存在",
                assetFingerprint: assetFingerprint
            )
        }

        var uploadResults: [ResourceUploadResult] = []
        uploadResults.reserveCapacity(preparedResources.count)
        var links: [RemoteAssetResourceLink] = []
        links.reserveCapacity(preparedResources.count)

        for (resourceOffset, prepared) in preparedResources.enumerated() {
            try Task.checkCancellation()

            let resourcePosition = resourceOffset + 1
            let totalResources = max(preparedResources.count, 1)
            await onTransferProgress(
                Self.makeTransferState(
                    assetLocalIdentifier: prepared.local.assetLocalIdentifier,
                    assetDisplayName: displayName,
                    resourceDate: prepared.shotDate,
                    assetPosition: assetPosition,
                    totalAssets: totalAssets,
                    resourceDisplayName: prepared.local.originalFilename,
                    resourcePosition: resourcePosition,
                    totalResources: totalResources,
                    resourceFraction: 0,
                    stageDescription: "上传资源"
                )
            )

            var lastReportedPercent = -1
            let uploadResult = try await processPreparedResource(
                prepared: prepared,
                monthStore: monthStore,
                profile: profile,
                smbClient: smbClient,
                onUploadProgress: { fraction in
                    let clamped = min(max(fraction, 0), 1)
                    let percent = clamped >= 1 ? 100 : Int(floor(clamped * 100))
                    guard percent != lastReportedPercent else { return }
                    lastReportedPercent = percent
                    Task { @MainActor in
                        await onTransferProgress(
                            Self.makeTransferState(
                                assetLocalIdentifier: prepared.local.assetLocalIdentifier,
                                assetDisplayName: displayName,
                                resourceDate: prepared.shotDate,
                                assetPosition: assetPosition,
                                totalAssets: totalAssets,
                                resourceDisplayName: prepared.local.originalFilename,
                                resourcePosition: resourcePosition,
                                totalResources: totalResources,
                                resourceFraction: Float(clamped),
                                stageDescription: "上传资源"
                            )
                        )
                    }
                }
            )
            uploadResults.append(uploadResult)

            await onTransferProgress(
                Self.makeTransferState(
                    assetLocalIdentifier: prepared.local.assetLocalIdentifier,
                    assetDisplayName: displayName,
                    resourceDate: prepared.shotDate,
                    assetPosition: assetPosition,
                    totalAssets: totalAssets,
                    resourceDisplayName: prepared.local.originalFilename,
                    resourcePosition: resourcePosition,
                    totalResources: totalResources,
                    resourceFraction: 1,
                    stageDescription: "上传完成"
                )
            )

            if uploadResult.status != .failed {
                links.append(
                    RemoteAssetResourceLink(
                        year: monthStore.year,
                        month: monthStore.month,
                        assetFingerprint: assetFingerprint,
                        resourceHash: prepared.contentHash,
                        role: prepared.local.resourceRole,
                        slot: prepared.local.resourceSlot
                    )
                )
            }
        }

        let failedResults = uploadResults.filter { $0.status == .failed }
        let skippedResults = uploadResults.filter { $0.status == .skipped }
        let successResults = uploadResults.filter { $0.status == .success }

        if !failedResults.isEmpty {
            let firstError = failedResults.first?.reason ?? "resource_failed"
            await onLog("Asset failed (partial): \(displayName). success=\(successResults.count), skipped=\(skippedResults.count), failed=\(failedResults.count)")
            return AssetProcessingResult(
                status: .failed,
                reason: firstError,
                displayName: displayName,
                resourceSummary: "资源\(preparedResources.count) 上传\(successResults.count) 跳过\(skippedResults.count) 失败\(failedResults.count)",
                assetFingerprint: assetFingerprint
            )
        }

        let manifestAsset = RemoteManifestAsset(
            year: monthStore.year,
            month: monthStore.month,
            assetFingerprint: assetFingerprint,
            creationDateNs: Self.nanosecondsSinceEpoch(asset.creationDate),
            backedUpAtNs: Self.nanosecondsSinceEpoch(Date()) ?? 0,
            resourceCount: links.count
        )

        try monthStore.upsertAsset(manifestAsset, links: links)
        upsertCachedRemoteSnapshotAsset(manifestAsset, links: links)

        try contentHashIndexRepository.upsertAssetFingerprint(
            assetLocalIdentifier: asset.localIdentifier,
            assetFingerprint: assetFingerprint,
            resourceCount: preparedResources.count
        )

        if successResults.isEmpty {
            return AssetProcessingResult(
                status: .skipped,
                reason: "resources_reused",
                displayName: displayName,
                resourceSummary: "资源\(preparedResources.count) 上传0 跳过\(skippedResults.count) 失败0",
                assetFingerprint: assetFingerprint
            )
        }

        return AssetProcessingResult(
            status: .success,
            reason: nil,
            displayName: displayName,
            resourceSummary: "资源\(preparedResources.count) 上传\(successResults.count) 跳过\(skippedResults.count) 失败0",
            assetFingerprint: assetFingerprint
        )
    }

    private func processAssetWithLocalCache(
        asset: PHAsset,
        selectedResources: [BackupSelectedResource],
        cachedLocalHash: LocalAssetHashCache?,
        monthStore: MonthManifestStore,
        displayName: String
    ) throws -> AssetProcessingResult? {
        guard let cachedLocalHash else { return nil }
        guard cachedLocalHash.resourceCount == selectedResources.count else { return nil }

        if let modificationDate = asset.modificationDate, modificationDate > cachedLocalHash.updatedAt {
            return nil
        }

        guard let roleSlotHashes = roleSlotHashes(from: selectedResources, cachedLocalHash: cachedLocalHash),
              roleSlotHashes.count == selectedResources.count else {
            return nil
        }

        let cachedFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: roleSlotHashes
        )
        guard cachedFingerprint == cachedLocalHash.assetFingerprint else {
            return nil
        }

        if monthStore.containsAssetFingerprint(cachedFingerprint) {
            try contentHashIndexRepository.upsertAssetFingerprint(
                assetLocalIdentifier: asset.localIdentifier,
                assetFingerprint: cachedFingerprint,
                resourceCount: selectedResources.count
            )
            return AssetProcessingResult(
                status: .skipped,
                reason: "asset_exists_cached",
                displayName: displayName,
                resourceSummary: "资源\(selectedResources.count) 已存在（缓存命中）",
                assetFingerprint: cachedFingerprint
            )
        }

        let links = roleSlotHashes.map { item in
            RemoteAssetResourceLink(
                year: monthStore.year,
                month: monthStore.month,
                assetFingerprint: cachedFingerprint,
                resourceHash: item.contentHash,
                role: item.role,
                slot: item.slot
            )
        }

        for link in links where monthStore.findResourceByHash(link.resourceHash) == nil {
            return nil
        }

        let manifestAsset = RemoteManifestAsset(
            year: monthStore.year,
            month: monthStore.month,
            assetFingerprint: cachedFingerprint,
            creationDateNs: Self.nanosecondsSinceEpoch(asset.creationDate),
            backedUpAtNs: Self.nanosecondsSinceEpoch(Date()) ?? 0,
            resourceCount: links.count
        )
        try monthStore.upsertAsset(manifestAsset, links: links)
        upsertCachedRemoteSnapshotAsset(manifestAsset, links: links)

        try contentHashIndexRepository.upsertAssetFingerprint(
            assetLocalIdentifier: asset.localIdentifier,
            assetFingerprint: cachedFingerprint,
            resourceCount: selectedResources.count
        )

        return AssetProcessingResult(
            status: .skipped,
            reason: "resources_reused_cached",
            displayName: displayName,
            resourceSummary: "资源\(selectedResources.count) 上传0 跳过\(selectedResources.count) 失败0（缓存命中）",
            assetFingerprint: cachedFingerprint
        )
    }

    private func roleSlotHashes(
        from selectedResources: [BackupSelectedResource],
        cachedLocalHash: LocalAssetHashCache
    ) -> [(role: Int, slot: Int, contentHash: Data)]? {
        var result: [(role: Int, slot: Int, contentHash: Data)] = []
        result.reserveCapacity(selectedResources.count)

        for selected in selectedResources {
            let key = AssetResourceRoleSlot(role: selected.role, slot: selected.slot)
            guard let contentHash = cachedLocalHash.hashesByRoleSlot[key] else {
                return nil
            }
            result.append((role: selected.role, slot: selected.slot, contentHash: contentHash))
        }

        return result
    }

    private func processPreparedResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        smbClient: RemoteStorageClientProtocol,
        onUploadProgress: ((Double) -> Void)? = nil
    ) async throws -> ResourceUploadResult {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize

        if monthStore.findResourceByHash(localHash) != nil {
            return ResourceUploadResult(
                status: .skipped,
                reason: "hash_exists"
            )
        }

        var targetFileName = RemotePathBuilder.sanitizeFilename(local.originalFilename)
        var skipReason: String?
        var attemptedFileNames: Set<String> = [targetFileName]

        if monthStore.existingFileNames().contains(targetFileName) {
            let existingManifestResource = monthStore.findByFileName(targetFileName)
            if localFileSize < Self.smallFileThresholdBytes {
                let remoteHash = try await downloadAndHashRemoteFile(
                    profile: profile,
                    smbClient: smbClient,
                    monthStore: monthStore,
                    fileName: targetFileName
                )
                if remoteHash == localHash {
                    skipReason = "name_same_hash"
                }
            } else {
                // Keep the historical large-file heuristic only for files not present in manifest.
                if existingManifestResource == nil {
                    let remoteSize = monthStore.remoteEntry(named: targetFileName)?.size
                    if remoteSize == localFileSize {
                        skipReason = "name_same_size"
                    }
                }
            }

            if skipReason == nil {
                targetFileName = RemoteNameCollisionResolver.resolveNextAvailableName(
                    baseName: targetFileName,
                    occupiedNames: monthStore.existingFileNames()
                )
                attemptedFileNames.insert(targetFileName)
            }
        }

        if let skipReason {
            let manifestItem = RemoteManifestResource(
                year: monthStore.year,
                month: monthStore.month,
                fileName: targetFileName,
                contentHash: localHash,
                fileSize: localFileSize,
                resourceType: local.resourceTypeCode,
                creationDateNs: Self.nanosecondsSinceEpoch(local.asset.creationDate),
                backedUpAtNs: Self.nanosecondsSinceEpoch(Date()) ?? 0
            )

            if monthStore.findResourceByHash(localHash) == nil {
                let inserted = try monthStore.upsertResource(manifestItem)
                upsertCachedRemoteSnapshotResource(inserted)
            }

            monthStore.markRemoteFile(name: targetFileName, size: localFileSize, creationDate: local.asset.creationDate)

            return ResourceUploadResult(
                status: .skipped,
                reason: skipReason
            )
        }

        let remoteRelativePath = monthStore.monthRelativePath + "/" + targetFileName
        var remoteAbsolutePath = RemotePathBuilder.absolutePath(basePath: profile.basePath, remoteRelativePath: remoteRelativePath)

        let maxRetry = 3
        var lastError: Error?

        for attempt in 0 ..< maxRetry {
            do {
                try Task.checkCancellation()
                try await smbClient.upload(
                    localURL: prepared.tempFileURL,
                    remotePath: remoteAbsolutePath,
                    respectTaskCancellation: true,
                    onProgress: onUploadProgress
                )
                if let shotDate = prepared.shotDate {
                    try? await smbClient.setModificationDate(shotDate, forPath: remoteAbsolutePath)
                }

                let backedUpAtNs = Self.nanosecondsSinceEpoch(Date()) ?? 0
                let manifestItem = RemoteManifestResource(
                    year: monthStore.year,
                    month: monthStore.month,
                    fileName: targetFileName,
                    contentHash: localHash,
                    fileSize: localFileSize,
                    resourceType: local.resourceTypeCode,
                    creationDateNs: Self.nanosecondsSinceEpoch(local.asset.creationDate),
                    backedUpAtNs: backedUpAtNs
                )
                let inserted = try monthStore.upsertResource(manifestItem)
                monthStore.markRemoteFile(name: targetFileName, size: localFileSize, creationDate: local.asset.creationDate)
                upsertCachedRemoteSnapshotResource(inserted)

                return ResourceUploadResult(
                    status: .success,
                    reason: nil
                )
            } catch {
                if error is CancellationError {
                    throw error
                }
                if profile.isExternalStorageUnavailableError(error) {
                    throw error
                }
                lastError = error

                let message = error.localizedDescription
                if message.contains("STATUS_OBJECT_NAME_COLLISION") {
                    var occupiedNames = monthStore.existingFileNames()
                    occupiedNames.formUnion(attemptedFileNames)
                    occupiedNames.insert(targetFileName)
                    targetFileName = RemoteNameCollisionResolver.resolveNextAvailableName(
                        baseName: targetFileName,
                        occupiedNames: occupiedNames
                    )
                    attemptedFileNames.insert(targetFileName)
                    let retryRelativePath = monthStore.monthRelativePath + "/" + targetFileName
                    remoteAbsolutePath = RemotePathBuilder.absolutePath(basePath: profile.basePath, remoteRelativePath: retryRelativePath)
                    continue
                }

                if attempt < maxRetry - 1 {
                    let sleepNanos = UInt64(pow(2.0, Double(attempt))) * 500_000_000
                    try? await Task.sleep(nanoseconds: sleepNanos)
                    continue
                }
            }
        }

        return ResourceUploadResult(
            status: .failed,
            reason: lastError?.localizedDescription ?? "Unknown upload failure"
        )
    }

    private func downloadAndHashRemoteFile(
        profile: ServerProfileRecord,
        smbClient: RemoteStorageClientProtocol,
        monthStore: MonthManifestStore,
        fileName: String
    ) async throws -> Data {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("remote_compare_\(UUID().uuidString)_\(fileName)")
        try? FileManager.default.removeItem(at: tempURL)
        defer {
            try? FileManager.default.removeItem(at: tempURL)
        }

        let remotePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: monthStore.monthRelativePath + "/" + fileName
        )
        try await smbClient.download(remotePath: remotePath, localURL: tempURL)
        return try Self.contentHash(of: tempURL)
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

    private func makeStorageClient(profile: ServerProfileRecord, password: String) throws -> any RemoteStorageClientProtocol {
        try storageClientFactory.makeClient(profile: profile, password: password)
    }

    private func makeLocalResource(asset: PHAsset, selected: BackupSelectedResource) -> LocalPhotoResource {
        LocalPhotoResource(
            asset: asset,
            resource: selected.resource,
            assetLocalIdentifier: asset.localIdentifier,
            resourceLocalIdentifier: "\(asset.localIdentifier)::\(selected.role)::\(selected.slot)",
            resourceRole: selected.role,
            resourceSlot: selected.slot,
            resourceType: PhotoLibraryService.resourceTypeName(selected.resource.type),
            resourceTypeCode: PhotoLibraryService.resourceTypeCode(selected.resource.type),
            uti: selected.resource.uniformTypeIdentifier,
            originalFilename: selected.resource.originalFilename,
            fileSize: PhotoLibraryService.resourceFileSize(selected.resource),
            resourceModificationDate: asset.modificationDate
        )
    }

    private func progressSnapshot(
        state: RunState,
        message: String,
        itemEvent: BackupItemEvent? = nil,
        transferState: BackupTransferState? = nil
    ) -> BackupProgress {
        BackupProgress(
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            total: state.total,
            message: message,
            itemEvent: itemEvent,
            transferState: transferState
        )
    }

    private static func makeTransferState(
        assetLocalIdentifier: String,
        assetDisplayName: String,
        resourceDate: Date?,
        assetPosition: Int,
        totalAssets: Int,
        resourceDisplayName: String,
        resourcePosition: Int,
        totalResources: Int,
        resourceFraction: Float,
        stageDescription: String
    ) -> BackupTransferState {
        BackupTransferState(
            assetLocalIdentifier: assetLocalIdentifier,
            assetDisplayName: assetDisplayName,
            resourceDate: resourceDate,
            assetPosition: max(1, assetPosition),
            totalAssets: max(totalAssets, 1),
            resourceDisplayName: resourceDisplayName,
            resourcePosition: max(1, resourcePosition),
            totalResources: max(totalResources, 1),
            resourceFraction: min(max(resourceFraction, 0), 1),
            stageDescription: stageDescription
        )
    }

    private static func transferMessage(for state: BackupTransferState) -> String {
        let clamped = state.clampedResourceFraction
        let resourcePercent = clamped >= 1 ? 100 : Int(floor(Double(clamped) * 100))
        return "[\(state.assetPosition)/\(max(state.totalAssets, 1))] \(state.assetDisplayName) · [\(state.resourcePosition)/\(state.totalResources)] \(state.resourceDisplayName) \(resourcePercent)%"
    }

    private func message(for result: AssetProcessingResult, position: Int, total: Int) -> String {
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

    private static func event(
        assetLocalIdentifier: String,
        assetFingerprint: Data?,
        displayName: String,
        resourceDate: Date?,
        status: BackupItemStatus,
        reason: String? = nil,
        resourceSummary: String? = nil
    ) -> BackupItemEvent {
        BackupItemEvent(
            assetLocalIdentifier: assetLocalIdentifier,
            assetFingerprint: assetFingerprint,
            displayName: displayName,
            resourceDate: resourceDate,
            status: status,
            reason: reason,
            resourceSummary: resourceSummary,
            updatedAt: Date()
        )
    }

    private static func monthKey(for date: Date?) -> MonthKey {
        let date = date ?? Date(timeIntervalSince1970: 0)
        let comps = monthCalendar.dateComponents([.year, .month], from: date)
        return MonthKey(
            year: comps.year ?? 1970,
            month: comps.month ?? 1
        )
    }

    private static func remoteProfileKey(_ profile: ServerProfileRecord) -> String {
        profile.storageProfile.identityKey
    }

    private static func nanosecondsSinceEpoch(_ date: Date?) -> Int64? {
        guard let date else { return nil }
        return Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())
    }

    private static func contentHash(of fileURL: URL) throws -> Data {
        guard let stream = InputStream(url: fileURL) else {
            throw NSError(
                domain: "BackupExecutor",
                code: -2,
                userInfo: [NSLocalizedDescriptionKey: "Cannot open file stream for hashing."]
            )
        }

        var hasher = SHA256()
        let bufferSize = 64 * 1024
        var buffer = [UInt8](repeating: 0, count: bufferSize)
        stream.open()
        defer { stream.close() }

        while stream.hasBytesAvailable {
            let read = stream.read(&buffer, maxLength: bufferSize)
            if read < 0 {
                throw stream.streamError ?? NSError(
                    domain: "BackupExecutor",
                    code: -3,
                    userInfo: [NSLocalizedDescriptionKey: "Failed to read file data for hashing."]
                )
            }
            if read == 0 {
                break
            }
            hasher.update(data: Data(buffer[0 ..< read]))
        }

        let digest = hasher.finalize()
        return Data(digest)
    }
}

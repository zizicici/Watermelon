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

    private struct SelectedResource {
        let resourceIndex: Int
        let resource: PHAssetResource
        let role: Int
        let slot: Int
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
        let displayFileName: String
        let resource: RemoteManifestResource?
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
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let remoteLibraryScanner: RemoteLibraryScanner

    private let snapshotLock = NSLock()
    private var cachedRemoteSnapshot = RemoteLibrarySnapshot(resources: [], assets: [], assetResourceLinks: [])

    init(
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryService,
        manifestSyncService _: ManifestSyncService
    ) {
        self.photoLibraryService = photoLibraryService
        contentHashIndexRepository = ContentHashIndexRepository(databaseManager: databaseManager)
        remoteLibraryScanner = RemoteLibraryScanner()
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

        let smbClient = try makeSMBClient(profile: profile, password: password)
        try await smbClient.connect()
        defer {
            Task { await smbClient.disconnect() }
        }

        try await smbClient.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))

        do {
            let snapshot = try await remoteLibraryScanner.scanYearMonthTree(
                client: smbClient,
                basePath: profile.basePath
            )
            updateCachedRemoteSnapshot(snapshot)
            await onLog("Remote index synced. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s).")
        } catch {
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

        var activeMonth: MonthKey?
        var activeStore: MonthManifestStore?

        let loopCount = retryMode ? retryAssets.count : assetsResult.count
        for loopIndex in 0 ..< loopCount {
            if Task.isCancelled {
                state.paused = true
                break
            }

            let asset = retryMode ? retryAssets[loopIndex] : assetsResult.object(at: loopIndex)
            let selectedResources = Self.orderedAssetResourcesWithRoleSlot(
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
                    _ = try? await activeStore.flushToRemote()
                    await onLog("Month \(activeMonth?.text ?? "unknown") manifest flushed.")
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
                    monthStore: monthStore,
                    profile: profile,
                    smbClient: smbClient,
                    position: currentPosition - 1,
                    totalAssets: state.total,
                    onProgressMessage: { [state] message in
                        onProgress(self.progressSnapshot(state: state, message: message))
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

                state.failed += 1

                let displayName = Self.assetDisplayName(asset: asset, selectedResources: selectedResources)
                await onLog("Failed asset: \(displayName) - \(error.localizedDescription)")
                await onProgress(
                    progressSnapshot(
                        state: state,
                        message: "[\(currentPosition)/\(state.total)] Failed asset \(displayName)",
                        itemEvent: Self.event(
                            assetLocalIdentifier: asset.localIdentifier,
                            assetFingerprint: nil,
                            displayName: displayName,
                            status: .failed,
                            reason: error.localizedDescription,
                            resourceSummary: "资源处理失败"
                        )
                    )
                )
            }
        }

        if let activeStore {
            _ = try? await activeStore.flushToRemote()
            await onLog("Month \(activeMonth?.text ?? "unknown") manifest flushed.")
        }

        do {
            let snapshot = try await remoteLibraryScanner.scanYearMonthTree(
                client: smbClient,
                basePath: profile.basePath
            )
            updateCachedRemoteSnapshot(snapshot)
        } catch {
            await onLog("Remote rescan warning: \(error.localizedDescription)")
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
        let smbClient = try makeSMBClient(profile: profile, password: password)
        try await smbClient.connect()
        defer {
            Task { await smbClient.disconnect() }
        }

        try await smbClient.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        let snapshot = try await remoteLibraryScanner.scanYearMonthTree(client: smbClient, basePath: profile.basePath)
        updateCachedRemoteSnapshot(snapshot)
        if let onLog {
            await onLog("Remote index reloaded. \(snapshot.totalResourceCount) resource(s), \(snapshot.totalCount) asset(s).")
        }
        return snapshot
    }

    func currentRemoteSnapshot() -> RemoteLibrarySnapshot {
        snapshotLock.lock()
        defer { snapshotLock.unlock() }
        return cachedRemoteSnapshot
    }

    private func updateCachedRemoteSnapshot(_ snapshot: RemoteLibrarySnapshot) {
        snapshotLock.lock()
        cachedRemoteSnapshot = snapshot
        snapshotLock.unlock()
    }

    private func upsertCachedRemoteSnapshotResource(_ item: RemoteManifestResource) {
        snapshotLock.lock()
        defer { snapshotLock.unlock() }

        var resources = cachedRemoteSnapshot.resources
        if let index = resources.firstIndex(where: { $0.id == item.id }) {
            resources[index] = item
        } else {
            resources.append(item)
        }

        resources.sort { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            if lhs.month != rhs.month { return lhs.month < rhs.month }
            let lhsTime = lhs.creationDateNs ?? lhs.backedUpAtNs
            let rhsTime = rhs.creationDateNs ?? rhs.backedUpAtNs
            if lhsTime != rhsTime { return lhsTime < rhsTime }
            return lhs.fileName < rhs.fileName
        }

        cachedRemoteSnapshot = RemoteLibrarySnapshot(
            resources: resources,
            assets: cachedRemoteSnapshot.assets,
            assetResourceLinks: cachedRemoteSnapshot.assetResourceLinks
        )
    }

    private func upsertCachedRemoteSnapshotAsset(_ asset: RemoteManifestAsset, links: [RemoteAssetResourceLink]? = nil) {
        snapshotLock.lock()
        defer { snapshotLock.unlock() }

        var assets = cachedRemoteSnapshot.assets
        if let index = assets.firstIndex(where: { $0.id == asset.id }) {
            assets[index] = asset
        } else {
            assets.append(asset)
        }

        assets.sort { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            if lhs.month != rhs.month { return lhs.month < rhs.month }
            let lhsTime = lhs.creationDateNs ?? lhs.backedUpAtNs
            let rhsTime = rhs.creationDateNs ?? rhs.backedUpAtNs
            if lhsTime != rhsTime { return lhsTime < rhsTime }
            return lhs.assetFingerprintHex < rhs.assetFingerprintHex
        }

        var assetResourceLinks = cachedRemoteSnapshot.assetResourceLinks
        if let links {
            assetResourceLinks.removeAll {
                $0.year == asset.year
                    && $0.month == asset.month
                    && $0.assetFingerprint == asset.assetFingerprint
            }
            assetResourceLinks.append(contentsOf: links)
            assetResourceLinks.sort { lhs, rhs in
                if lhs.year != rhs.year { return lhs.year < rhs.year }
                if lhs.month != rhs.month { return lhs.month < rhs.month }
                if lhs.assetFingerprint != rhs.assetFingerprint {
                    return lhs.assetFingerprint.lexicographicallyPrecedes(rhs.assetFingerprint)
                }
                if lhs.role != rhs.role { return lhs.role < rhs.role }
                if lhs.slot != rhs.slot { return lhs.slot < rhs.slot }
                return lhs.resourceHash.lexicographicallyPrecedes(rhs.resourceHash)
            }
        }

        cachedRemoteSnapshot = RemoteLibrarySnapshot(
            resources: cachedRemoteSnapshot.resources,
            assets: assets,
            assetResourceLinks: assetResourceLinks
        )
    }

    private func processAsset(
        asset: PHAsset,
        selectedResources: [SelectedResource],
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        smbClient: SMBClientProtocol,
        position: Int,
        totalAssets: Int,
        onProgressMessage: @escaping @MainActor (String) async -> Void,
        onLog: @escaping @MainActor (String) -> Void
    ) async throws -> AssetProcessingResult {
        var preparedResources: [PreparedResource] = []
        preparedResources.reserveCapacity(selectedResources.count)

        defer {
            for prepared in preparedResources {
                try? FileManager.default.removeItem(at: prepared.tempFileURL)
            }
        }

        let displayName = Self.assetDisplayName(asset: asset, selectedResources: selectedResources)

        for (resourcePosition, selected) in selectedResources.enumerated() {
            try Task.checkCancellation()

            let local = makeLocalResource(asset: asset, selected: selected)
            let stageText = "[\(position + 1)/\(max(totalAssets, 1))] \(displayName) · [\(resourcePosition + 1)/\(selectedResources.count)] \(local.originalFilename)"
            await onProgressMessage(stageText)

            let tempFileURL = try await photoLibraryService.exportResourceToTempFile(local.resource)
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
        }

        let assetFingerprint = Self.assetFingerprint(from: preparedResources)

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

        for prepared in preparedResources {
            try Task.checkCancellation()

            let uploadResult = try await processPreparedResource(
                prepared: prepared,
                monthStore: monthStore,
                profile: profile,
                smbClient: smbClient
            )
            uploadResults.append(uploadResult)

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

    private func processPreparedResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        smbClient: SMBClientProtocol
    ) async throws -> ResourceUploadResult {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize

        if let existing = monthStore.findResourceByHash(localHash) {
            return ResourceUploadResult(
                status: .skipped,
                reason: "hash_exists",
                displayFileName: existing.fileName,
                resource: existing
            )
        }

        var targetFileName = RemotePathBuilder.sanitizeFilename(local.originalFilename)
        var skipReason: String?

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
                reason: skipReason,
                displayFileName: targetFileName,
                resource: monthStore.findResourceByHash(localHash)
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
                    respectTaskCancellation: true
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
                    reason: nil,
                    displayFileName: targetFileName,
                    resource: inserted
                )
            } catch {
                if error is CancellationError {
                    throw error
                }
                lastError = error

                let message = error.localizedDescription
                if message.contains("STATUS_OBJECT_NAME_COLLISION") {
                    targetFileName = RemoteNameCollisionResolver.resolveNextAvailableName(
                        baseName: targetFileName,
                        occupiedNames: monthStore.existingFileNames()
                    )
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
            reason: lastError?.localizedDescription ?? "Unknown upload failure",
            displayFileName: targetFileName,
            resource: nil
        )
    }

    private func downloadAndHashRemoteFile(
        profile: ServerProfileRecord,
        smbClient: SMBClientProtocol,
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

    private func makeSMBClient(profile: ServerProfileRecord, password: String) throws -> AMSMB2Client {
        try AMSMB2Client(config: SMBServerConfig(
            host: profile.host,
            port: profile.port,
            shareName: profile.shareName,
            basePath: profile.basePath,
            username: profile.username,
            password: password,
            domain: profile.domain
        ))
    }

    private func makeLocalResource(asset: PHAsset, selected: SelectedResource) -> LocalPhotoResource {
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
        itemEvent: BackupItemEvent? = nil
    ) -> BackupProgress {
        BackupProgress(
            succeeded: state.succeeded,
            failed: state.failed,
            skipped: state.skipped,
            total: state.total,
            message: message,
            itemEvent: itemEvent
        )
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
        status: BackupItemStatus,
        reason: String? = nil,
        resourceSummary: String? = nil
    ) -> BackupItemEvent {
        BackupItemEvent(
            assetLocalIdentifier: assetLocalIdentifier,
            assetFingerprint: assetFingerprint,
            displayName: displayName,
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

    private static func nanosecondsSinceEpoch(_ date: Date?) -> Int64? {
        guard let date else { return nil }
        return Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())
    }

    private static func orderedAssetResourcesWithRoleSlot(_ resources: [PHAssetResource]) -> [SelectedResource] {
        let sorted = Array(resources.enumerated()).sorted { lhs, rhs in
            let lhsRole = PhotoLibraryService.resourceTypeCode(lhs.1.type)
            let rhsRole = PhotoLibraryService.resourceTypeCode(rhs.1.type)
            if lhsRole != rhsRole { return lhsRole < rhsRole }

            let lhsName = lhs.1.originalFilename.lowercased()
            let rhsName = rhs.1.originalFilename.lowercased()
            if lhsName != rhsName { return lhsName < rhsName }
            return lhs.0 < rhs.0
        }

        var roleCounters: [Int: Int] = [:]
        var result: [SelectedResource] = []
        result.reserveCapacity(sorted.count)

        for (resourceIndex, resource) in sorted {
            let role = PhotoLibraryService.resourceTypeCode(resource.type)
            let slot = roleCounters[role, default: 0]
            roleCounters[role] = slot + 1

            result.append(
                SelectedResource(
                    resourceIndex: resourceIndex,
                    resource: resource,
                    role: role,
                    slot: slot
                )
            )
        }

        return result
    }

    private static func assetFingerprint(from preparedResources: [PreparedResource]) -> Data {
        let tokens = preparedResources
            .map { prepared in
                let hashHex = prepared.contentHash.map { String(format: "%02x", $0) }.joined()
                return "\(prepared.local.resourceRole)|\(prepared.local.resourceSlot)|\(hashHex)"
            }
            .sorted()
            .joined(separator: "\n")

        let digest = SHA256.hash(data: Data(tokens.utf8))
        return Data(digest)
    }

    private static func assetDisplayName(asset: PHAsset, selectedResources: [SelectedResource]) -> String {
        if let first = selectedResources.first {
            return first.resource.originalFilename
        }

        let timestamp = nanosecondsSinceEpoch(asset.creationDate) ?? 0
        return "asset_\(timestamp)"
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

import CryptoKit
import Foundation
import Photos

final class AssetProcessor: Sendable {
    static let smallFileThresholdBytes: Int64 = 5 * 1024 * 1024
    static let hashBufferSize = 64 * 1024
    static let transferProgressMinimumStep = 0.01
    static let transferProgressMinimumInterval: TimeInterval = 0.12

    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository
    let remoteIndexService: RemoteIndexSyncService

    init(
        photoLibraryService: PhotoLibraryService,
        hashIndexRepository: ContentHashIndexRepository,
        remoteIndexService: RemoteIndexSyncService
    ) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService
    }

    static func monthKey(for date: Date?) -> LibraryMonthKey {
        LibraryMonthKey.from(date: date)
    }

    func process(
        context: AssetProcessContext,
        client: RemoteStorageClientProtocol,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> AssetProcessResult {
        var preparedResources: [PreparedResource] = []
        preparedResources.reserveCapacity(context.selectedResources.count)
        var timing = AssetProcessTiming()
        let emitTransferState = true

        defer {
            for prepared in preparedResources {
                try? FileManager.default.removeItem(at: prepared.tempFileURL)
            }
        }

        let displayName = BackupAssetResourcePlanner.assetDisplayName(
            asset: context.asset,
            selectedResources: context.selectedResources
        )

        if let cachedResult = try processWithLocalCache(
            context: context,
            displayName: displayName,
            cancellationController: cancellationController
        ) {
            return cachedResult
        }

        let preferredAssetNameStem = Self.preferredAssetNameStem(
            asset: context.asset,
            selectedResources: context.selectedResources
        )

        for (resourcePosition, selected) in context.selectedResources.enumerated() {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()

            let local = makeLocalResource(
                asset: context.asset,
                selected: selected,
                preferredAssetNameStem: preferredAssetNameStem
            )
            if emitTransferState {
                eventStream.emit(.transferState(
                    Self.makeTransferState(
                        workerID: context.workerID,
                        assetLocalIdentifier: context.asset.localIdentifier,
                        assetDisplayName: displayName,
                        resourceDate: local.asset.creationDate ?? local.resourceModificationDate,
                        assetPosition: context.assetPosition,
                        totalAssets: context.totalAssets,
                        resourceDisplayName: local.originalFilename,
                        resourcePosition: resourcePosition + 1,
                        totalResources: context.selectedResources.count,
                        resourceFraction: 0,
                        stageDescription: "准备资源"
                    )
                ))
            }

            let exportHashStart = CFAbsoluteTimeGetCurrent()
            let exportedResource: ExportedResourceFile
            do {
                exportedResource = try await photoLibraryService.exportResourceToTempFileAndDigest(
                    local.resource,
                    cancellationController: cancellationController,
                    allowNetworkAccess: context.iCloudPhotoBackupMode.allowsNetworkAccess
                )
                timing.exportHashSeconds += Self.elapsedSeconds(since: exportHashStart)
            } catch {
                timing.exportHashSeconds += Self.elapsedSeconds(since: exportHashStart)
                if !context.iCloudPhotoBackupMode.allowsNetworkAccess,
                   PhotoLibraryService.isNetworkAccessRequiredError(error) {
                    eventStream.emitLog(
                        "跳过 iCloud 资源：\(displayName)。资源未下载到本机，且\"允许访问 iCloud 原件\"未开启。",
                        level: .warning
                    )
                    return Self.makeICloudDisabledSkipResult(
                        context: context,
                        displayName: displayName,
                        timing: timing
                    )
                }
                throw error
            }
            let tempFileURL = exportedResource.fileURL
            var shouldRemoveTempFile = true
            defer {
                if shouldRemoveTempFile {
                    try? FileManager.default.removeItem(at: tempFileURL)
                }
            }
            let localHash = exportedResource.contentHash
            let localFileSize = max(
                local.fileSize,
                exportedResource.fileSize
            )
            let shotDate = local.asset.creationDate ?? local.resourceModificationDate
            if let shotDate {
                try? FileManager.default.setAttributes([.modificationDate: shotDate], ofItemAtPath: tempFileURL.path)
            }

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

        var uploadResults: [ResourceUploadResult] = []
        uploadResults.reserveCapacity(preparedResources.count)
        var links: [RemoteAssetResourceLink] = []
        links.reserveCapacity(preparedResources.count)

        for (resourcePosition, prepared) in preparedResources.enumerated() {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()

            let uploadResult = try await uploadResource(
                prepared: prepared,
                monthStore: context.monthStore,
                profile: context.profile,
                client: client,
                workerID: context.workerID,
                resourcePosition: resourcePosition + 1,
                totalResources: preparedResources.count,
                assetPosition: context.assetPosition,
                totalAssets: context.totalAssets,
                displayName: displayName,
                eventStream: eventStream,
                emitTransferState: emitTransferState,
                assetTiming: &timing,
                cancellationController: cancellationController
            )
            uploadResults.append(uploadResult)

            if emitTransferState {
                eventStream.emit(.transferState(
                    Self.makeTransferState(
                        workerID: context.workerID,
                        assetLocalIdentifier: prepared.local.assetLocalIdentifier,
                        assetDisplayName: displayName,
                        resourceDate: prepared.shotDate,
                        assetPosition: context.assetPosition,
                        totalAssets: context.totalAssets,
                        resourceDisplayName: prepared.local.originalFilename,
                        resourcePosition: resourcePosition + 1,
                        totalResources: preparedResources.count,
                        resourceFraction: 1,
                        stageDescription: "上传完成"
                    )
                ))
            }

            if uploadResult.status != .failed {
                links.append(
                    RemoteAssetResourceLink(
                        year: context.monthStore.year,
                        month: context.monthStore.month,
                        assetFingerprint: assetFingerprint,
                        resourceHash: prepared.contentHash,
                        role: prepared.local.resourceRole,
                        slot: prepared.local.resourceSlot
                    )
                )
            }
        }

        var failedCount = 0, skippedCount = 0, successCount = 0
        var firstFailedReason: String?
        for result in uploadResults {
            switch result.status {
            case .failed:
                failedCount += 1
                if firstFailedReason == nil { firstFailedReason = result.reason }
            case .skipped: skippedCount += 1
            case .success: successCount += 1
            }
        }
        let totalFileSizeBytes = preparedResources.reduce(Int64(0)) { partial, prepared in
            partial + max(prepared.fileSize, 0)
        }
        let uploadedFileSizeBytes = zip(preparedResources, uploadResults).reduce(Int64(0)) { partial, pair in
            pair.1.status == .success ? (partial + max(pair.0.fileSize, 0)) : partial
        }

        if failedCount > 0 {
            let firstError = firstFailedReason ?? "resource_failed"
            print("[BackupUpload] asset FAILED: asset=\(displayName), success=\(successCount), skipped=\(skippedCount), failed=\(failedCount), reason=\(firstError)")
            eventStream.emitLog(
                "Asset failed (partial): \(displayName). success=\(successCount), skipped=\(skippedCount), failed=\(failedCount)",
                level: .error
            )
            return AssetProcessResult(
                status: .failed,
                reason: firstError,
                displayName: displayName,
                resourceSummary: "资源\(preparedResources.count) 上传\(successCount) 跳过\(skippedCount) 失败\(failedCount)",
                assetFingerprint: assetFingerprint,
                timing: timing,
                totalFileSizeBytes: totalFileSizeBytes,
                uploadedFileSizeBytes: uploadedFileSizeBytes
            )
        }

        let manifestAsset = RemoteManifestAsset(
            year: context.monthStore.year,
            month: context.monthStore.month,
            assetFingerprint: assetFingerprint,
            creationDateNs: context.asset.creationDate?.nanosecondsSinceEpoch,
            backedUpAtNs: Date().nanosecondsSinceEpoch,
            resourceCount: links.count,
            totalFileSizeBytes: totalFileSizeBytes
        )

        let manifestWriteStart = CFAbsoluteTimeGetCurrent()
        try context.monthStore.upsertAsset(manifestAsset, links: links)
        timing.databaseSeconds += Self.elapsedSeconds(since: manifestWriteStart)
        remoteIndexService.upsertCachedAsset(manifestAsset, links: links)

        let snapshotWriteStart = CFAbsoluteTimeGetCurrent()
        try hashIndexRepository.upsertAssetHashSnapshot(
            assetLocalIdentifier: context.asset.localIdentifier,
            assetFingerprint: assetFingerprint,
            resources: preparedResources.map {
                LocalAssetResourceHashRecord(
                    role: $0.local.resourceRole,
                    slot: $0.local.resourceSlot,
                    contentHash: $0.contentHash,
                    fileSize: $0.fileSize
                )
            },
            totalFileSizeBytes: totalFileSizeBytes
        )
        timing.databaseSeconds += Self.elapsedSeconds(since: snapshotWriteStart)

        if successCount == 0 {
            return AssetProcessResult(
                status: .skipped,
                reason: "resources_reused",
                displayName: displayName,
                resourceSummary: "资源\(preparedResources.count) 上传0 跳过\(skippedCount) 失败0",
                assetFingerprint: assetFingerprint,
                timing: timing,
                totalFileSizeBytes: totalFileSizeBytes,
                uploadedFileSizeBytes: uploadedFileSizeBytes
            )
        }

        return AssetProcessResult(
            status: .success,
            reason: nil,
            displayName: displayName,
            resourceSummary: "资源\(preparedResources.count) 上传\(successCount) 跳过\(skippedCount) 失败0",
            assetFingerprint: assetFingerprint,
            timing: timing,
            totalFileSizeBytes: totalFileSizeBytes,
            uploadedFileSizeBytes: uploadedFileSizeBytes
        )
    }

    private func processWithLocalCache(
        context: AssetProcessContext,
        displayName: String,
        cancellationController: BackupCancellationController?
    ) throws -> AssetProcessResult? {
        var timing = AssetProcessTiming()
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()
        guard let cachedLocalHash = context.cachedLocalHash else { return nil }
        guard cachedLocalHash.resourceCount == context.selectedResources.count else { return nil }

        if let modificationDate = context.asset.modificationDate, modificationDate > cachedLocalHash.updatedAt {
            return nil
        }

        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()
        guard let roleSlotHashes = roleSlotHashes(
            from: context.selectedResources,
            cachedLocalHash: cachedLocalHash
        ), roleSlotHashes.count == context.selectedResources.count else {
            return nil
        }

        let cachedFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: roleSlotHashes
        )
        guard cachedFingerprint == cachedLocalHash.assetFingerprint else {
            return nil
        }

        if context.monthStore.containsAssetFingerprint(cachedFingerprint) {
            let totalFileSizeBytes = Self.totalSizeBytes(of: context.selectedResources)
            let dbStart = CFAbsoluteTimeGetCurrent()
            try hashIndexRepository.upsertAssetFingerprint(
                assetLocalIdentifier: context.asset.localIdentifier,
                assetFingerprint: cachedFingerprint,
                resourceCount: context.selectedResources.count,
                totalFileSizeBytes: totalFileSizeBytes
            )
            timing.databaseSeconds += Self.elapsedSeconds(since: dbStart)
            return AssetProcessResult(
                status: .skipped,
                reason: "asset_exists_cached",
                displayName: displayName,
                resourceSummary: "资源\(context.selectedResources.count) 已存在（缓存命中）",
                assetFingerprint: cachedFingerprint,
                timing: timing,
                totalFileSizeBytes: totalFileSizeBytes,
                uploadedFileSizeBytes: 0
            )
        }

        let links = roleSlotHashes.map { item in
            RemoteAssetResourceLink(
                year: context.monthStore.year,
                month: context.monthStore.month,
                assetFingerprint: cachedFingerprint,
                resourceHash: item.contentHash,
                role: item.role,
                slot: item.slot
            )
        }

        for link in links where context.monthStore.findResourceByHash(link.resourceHash) == nil {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            return nil
        }

        let totalFileSizeBytes = links.reduce(Int64(0)) { partial, link in
            partial + max(context.monthStore.findResourceByHash(link.resourceHash)?.fileSize ?? 0, 0)
        }

        let manifestAsset = RemoteManifestAsset(
            year: context.monthStore.year,
            month: context.monthStore.month,
            assetFingerprint: cachedFingerprint,
            creationDateNs: context.asset.creationDate?.nanosecondsSinceEpoch,
            backedUpAtNs: Date().nanosecondsSinceEpoch,
            resourceCount: links.count,
            totalFileSizeBytes: totalFileSizeBytes
        )
        let manifestWriteStart = CFAbsoluteTimeGetCurrent()
        try context.monthStore.upsertAsset(manifestAsset, links: links)
        timing.databaseSeconds += Self.elapsedSeconds(since: manifestWriteStart)
        remoteIndexService.upsertCachedAsset(manifestAsset, links: links)

        let dbStart = CFAbsoluteTimeGetCurrent()
        try hashIndexRepository.upsertAssetFingerprint(
            assetLocalIdentifier: context.asset.localIdentifier,
            assetFingerprint: cachedFingerprint,
            resourceCount: context.selectedResources.count,
            totalFileSizeBytes: totalFileSizeBytes
        )
        timing.databaseSeconds += Self.elapsedSeconds(since: dbStart)

        return AssetProcessResult(
            status: .skipped,
            reason: "resources_reused_cached",
            displayName: displayName,
            resourceSummary: "资源\(context.selectedResources.count) 上传0 跳过\(context.selectedResources.count) 失败0（缓存命中）",
            assetFingerprint: cachedFingerprint,
            timing: timing,
            totalFileSizeBytes: totalFileSizeBytes,
            uploadedFileSizeBytes: 0
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

    private static func makeICloudDisabledSkipResult(
        context: AssetProcessContext,
        displayName: String,
        timing: AssetProcessTiming
    ) -> AssetProcessResult {
        let totalFileSizeBytes = totalSizeBytes(of: context.selectedResources)
        return AssetProcessResult(
            status: .skipped,
            reason: "icloud_photo_backup_disabled",
            displayName: displayName,
            resourceSummary: "资源\(context.selectedResources.count) 未上传（包含仅存于 iCloud 的资源）",
            assetFingerprint: nil,
            timing: timing,
            totalFileSizeBytes: totalFileSizeBytes,
            uploadedFileSizeBytes: 0
        )
    }
}

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
    private let thumbnailRenderer: ThumbnailRenderer?

    init(
        photoLibraryService: PhotoLibraryService,
        hashIndexRepository: ContentHashIndexRepository,
        remoteIndexService: RemoteIndexSyncService,
        thumbnailRenderer: ThumbnailRenderer? = nil
    ) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService
        self.thumbnailRenderer = thumbnailRenderer
    }

    static func monthKey(for date: Date?, calendar: Calendar) -> LibraryMonthKey {
        LibraryMonthKey.from(date: date, calendar: calendar)
    }

    func process(
        context: AssetProcessContext,
        client: RemoteStorageClientProtocol,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> AssetProcessResult {
        // Re-fetch — stale PHAsset (deleted/edited mid-batch) surfaces as PHPhotosErrorDomain -1 deep in requestData.
        var context = context
        let refetchResult = PHAsset.fetchAssets(
            withLocalIdentifiers: [context.asset.localIdentifier],
            options: nil
        )
        guard refetchResult.count > 0 else {
            return AssetProcessResult(
                status: .skipped,
                reason: "asset_gone",
                displayName: BackupAssetResourcePlanner.assetDisplayName(
                    asset: context.asset,
                    selectedResources: context.selectedResources
                ),
                assetFingerprint: nil,
                timing: AssetProcessTiming(),
                totalFileSizeBytes: 0,
                uploadedFileSizeBytes: 0
            )
        }
        let refetchedAsset = refetchResult.object(at: 0)
        let refetchedResources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
            from: PHAssetResource.assetResources(for: refetchedAsset)
        )
        guard !refetchedResources.isEmpty else {
            return AssetProcessResult(
                status: .skipped,
                reason: "asset_no_resources",
                displayName: BackupAssetResourcePlanner.assetDisplayName(
                    asset: refetchedAsset,
                    selectedResources: []
                ),
                assetFingerprint: nil,
                timing: AssetProcessTiming(),
                totalFileSizeBytes: 0,
                uploadedFileSizeBytes: 0
            )
        }
        context = context.withRefreshedAsset(refetchedAsset, selectedResources: refetchedResources)

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
                        kind: .upload,
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
                        resourceBytesTransferred: nil,
                        resourceTotalBytes: nil,
                        stageDescription: String(localized: "backup.transfer.prepareResource")
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
                        String.localizedStringWithFormat(
                            String(localized: "backup.log.skipICloudResource"),
                            displayName
                        ),
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
                cancellationController: cancellationController,
                writeMode: context.writeMode
            )
            uploadResults.append(uploadResult)

            if emitTransferState {
                eventStream.emit(.transferState(
                    Self.makeTransferState(
                        kind: .upload,
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
                        resourceBytesTransferred: prepared.fileSize,
                        resourceTotalBytes: prepared.fileSize,
                        countsTowardTransferSpeed: uploadResult.status == .success,
                        stageDescription: String(localized: "backup.transfer.uploadCompleted")
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
                String.localizedStringWithFormat(
                    String(localized: "backup.log.assetPartialFailure"),
                    displayName,
                    successCount,
                    skippedCount,
                    failedCount
                ),
                level: .error
            )
            return AssetProcessResult(
                status: .failed,
                reason: firstError,
                displayName: displayName,
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
            creationDateMs: context.asset.creationDate?.millisecondsSinceEpoch,
            backedUpAtMs: Date().millisecondsSinceEpoch,
            resourceCount: links.count,
            totalFileSizeBytes: totalFileSizeBytes
        )

        let manifestWriteStart = CFAbsoluteTimeGetCurrent()
        try context.monthStore.upsertAsset(manifestAsset, links: links)
        timing.databaseSeconds += Self.elapsedSeconds(since: manifestWriteStart)
        remoteIndexService.upsertCachedAsset(manifestAsset, links: links, expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(context.profile))

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
            totalFileSizeBytes: totalFileSizeBytes,
            modificationDateMs: context.asset.modificationDate?.millisecondsSinceEpoch
        )
        timing.databaseSeconds += Self.elapsedSeconds(since: snapshotWriteStart)

        if successCount == 0 {
            return AssetProcessResult(
                status: .skipped,
                reason: "resources_reused",
                displayName: displayName,
                assetFingerprint: assetFingerprint,
                timing: timing,
                totalFileSizeBytes: totalFileSizeBytes,
                uploadedFileSizeBytes: uploadedFileSizeBytes
            )
        }

        // Best-effort thumbnail sidecar — gated per-profile, never affects the asset's success.
        // Inline (synchronous) so it is guaranteed for every genuinely-uploaded asset, foreground and
        // background alike. The throughput cost is accepted: enabling the flag is opt-in to it.
        if context.profile.generateRemoteThumbnails, let thumbnailRenderer {
            await uploadThumbnailBestEffort(
                renderer: thumbnailRenderer,
                asset: context.asset,
                assetFingerprint: assetFingerprint,
                profile: context.profile,
                client: client,
                allowNetworkAccess: context.iCloudPhotoBackupMode.allowsNetworkAccess,
                cancellationController: cancellationController
            )
        }

        return AssetProcessResult(
            status: .success,
            reason: nil,
            displayName: displayName,
            assetFingerprint: assetFingerprint,
            timing: timing,
            totalFileSizeBytes: totalFileSizeBytes,
            uploadedFileSizeBytes: uploadedFileSizeBytes
        )
    }

    // Generates and uploads the content-addressed thumbnail sidecar inline. Fully isolated: returns
    // Void, swallows every error (including LiteRepoError.isUploadFailFast, which must never bubble or
    // the executor would stop the whole month), and treats cancellation as "skip" — the asset's
    // resources are already uploaded and recorded, so it must still report success.
    private func uploadThumbnailBestEffort(
        renderer: ThumbnailRenderer,
        asset: PHAsset,
        assetFingerprint: Data,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        allowNetworkAccess: Bool,
        cancellationController: BackupCancellationController?
    ) async {
        do {
            if cancellationController?.isCancelled == true || Task.isCancelled { return }

            guard let data = await renderer.renderThumbnailJPEG(
                for: asset,
                allowNetworkAccess: allowNetworkAccess
            ) else { return }

            if cancellationController?.isCancelled == true || Task.isCancelled { return }

            let fingerprintHex = assetFingerprint.hexString
            let tempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("thumb_\(fingerprintHex)_\(UUID().uuidString).jpg")
            try data.write(to: tempURL)
            defer { try? FileManager.default.removeItem(at: tempURL) }

            // New-upload path: the sidecar almost never pre-exists, so skip the exists() probe and just
            // .replace (content-addressed + idempotent). createDirectory is idempotent (no-op on S3).
            let shardDir = RemoteThumbnailPaths.shardDirectoryAbsolutePath(
                basePath: profile.basePath,
                fingerprintHex: fingerprintHex
            )
            try? await client.createDirectory(path: shardDir)

            let thumbPath = RemoteThumbnailPaths.absolutePath(
                basePath: profile.basePath,
                fingerprintHex: fingerprintHex
            )
            try await Self.uploadSidecarReplacing(localURL: tempURL, thumbPath: thumbPath, client: client)
        } catch {
            // Best-effort: never surface thumbnail failures to the backup result.
        }
    }

    // Detached + cancellation-blind, mirroring RemoteThumbnailService.writeSidecar: a stop cancelling the
    // run mid-transfer would leave a torn partial at the canonical path (WebDAV excludes bare cancels from
    // cleanup; SMB cleanup fails on a dead session), and no writer overwrites an existing sidecar. The
    // small upload runs to completion instead. `internal` only so the shield is pinnable by tests.
    static func uploadSidecarReplacing(localURL: URL, thumbPath: String, client: RemoteStorageClientProtocol) async throws {
        let transfer = Task.detached {
            try await client.upload(localURL: localURL, remotePath: thumbPath, mode: .replace, respectTaskCancellation: false, onProgress: nil)
        }
        try await transfer.value
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

        // Incomplete asset falls through to the full upload path so missing resources heal.
        if context.monthStore.containsAssetFingerprint(cachedFingerprint),
           !context.monthStore.isAssetIncomplete(cachedFingerprint) {
            let totalFileSizeBytes = Self.totalSizeBytes(of: context.selectedResources)
            let dbStart = CFAbsoluteTimeGetCurrent()
            try hashIndexRepository.upsertAssetFingerprint(
                assetLocalIdentifier: context.asset.localIdentifier,
                assetFingerprint: cachedFingerprint,
                resourceCount: context.selectedResources.count,
                totalFileSizeBytes: totalFileSizeBytes,
                modificationDateMs: context.asset.modificationDate?.millisecondsSinceEpoch
            )
            timing.databaseSeconds += Self.elapsedSeconds(since: dbStart)
            return AssetProcessResult(
                status: .skipped,
                reason: "asset_exists_cached",
                displayName: displayName,
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
            creationDateMs: context.asset.creationDate?.millisecondsSinceEpoch,
            backedUpAtMs: Date().millisecondsSinceEpoch,
            resourceCount: links.count,
            totalFileSizeBytes: totalFileSizeBytes
        )
        let manifestWriteStart = CFAbsoluteTimeGetCurrent()
        try context.monthStore.upsertAsset(manifestAsset, links: links)
        timing.databaseSeconds += Self.elapsedSeconds(since: manifestWriteStart)
        remoteIndexService.upsertCachedAsset(manifestAsset, links: links, expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(context.profile))

        let dbStart = CFAbsoluteTimeGetCurrent()
        try hashIndexRepository.upsertAssetFingerprint(
            assetLocalIdentifier: context.asset.localIdentifier,
            assetFingerprint: cachedFingerprint,
            resourceCount: context.selectedResources.count,
            totalFileSizeBytes: totalFileSizeBytes,
            modificationDateMs: context.asset.modificationDate?.millisecondsSinceEpoch
        )
        timing.databaseSeconds += Self.elapsedSeconds(since: dbStart)

        return AssetProcessResult(
            status: .skipped,
            reason: "resources_reused_cached",
            displayName: displayName,
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
            assetFingerprint: nil,
            timing: timing,
            totalFileSizeBytes: totalFileSizeBytes,
            uploadedFileSizeBytes: 0
        )
    }
}

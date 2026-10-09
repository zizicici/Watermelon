import CryptoKit
import Foundation
import Photos

final class AssetProcessor: Sendable {
    static let assetDateUpdatedReason = "asset_date_updated"
    static let assetResourcesUpdatedReason = "asset_resources_updated"
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

        if let cachedResult = try await processWithLocalCache(
            context: context,
            displayName: displayName,
            client: client,
            eventStream: eventStream,
            cancellationController: cancellationController
        ) {
            return cachedResult
        }

        for (resourcePosition, selected) in context.selectedResources.enumerated() {
            do {
                preparedResources.append(try await prepareResource(
                    selected, position: resourcePosition + 1, context: context,
                    displayName: displayName, eventStream: eventStream,
                    timing: &timing, cancellationController: cancellationController
                ))
            } catch {
                if let result = networkExportResult(error: error, context: context,
                    displayName: displayName, timing: timing, eventStream: eventStream) {
                    return result
                }
                throw error
            }
        }

        let assetFingerprint = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: preparedResources.map {
            (role: $0.local.resourceRole, slot: $0.local.resourceSlot, contentHash: $0.contentHash)
        })
        if context.monthStore.containsAssetFingerprint(assetFingerprint), !context.monthStore.isAssetIncomplete(assetFingerprint) {
            try assertAssetUnchanged(context.asset)
            try hashIndexRepository.upsertAssetHashSnapshot(assetLocalIdentifier: context.asset.localIdentifier,
                assetFingerprint: assetFingerprint, resources: preparedResources.map {
                    .init(role: $0.local.resourceRole, slot: $0.local.resourceSlot, contentHash: $0.contentHash, fileSize: $0.fileSize)
                }, totalFileSizeBytes: preparedResources.reduce(0) { $0 + $1.fileSize }, modificationDateMs: context.asset.modificationDate?.millisecondsSinceEpoch)
            if let result = try await processMatchedAsset(
                fingerprint: assetFingerprint,
                localResources: preparedResources.map { ($0.local.resourceRole, $0.local.resourceSlot, $0.contentHash) },
                preparedResources: preparedResources,
                totalFileSizeBytes: preparedResources.reduce(0) { $0 + $1.fileSize },
                context: context, displayName: displayName, skipReason: "asset_content_exists",
                client: client, eventStream: eventStream, timing: &timing,
                cancellationController: cancellationController
            ) {
                return result
            }
        }

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
                emitUploadCompletion(prepared: prepared, result: uploadResult, position: resourcePosition + 1,
                    totalResources: preparedResources.count, context: context, displayName: displayName, eventStream: eventStream)
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
            creationDateMs: LibraryCreationDate.optionalMilliseconds(context.asset.creationDate),
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
                allowNetworkAccess: context.allowsNetworkExport,
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

    private func emitUploadCompletion(
        prepared: PreparedResource,
        result: ResourceUploadResult,
        position: Int,
        totalResources: Int,
        context: AssetProcessContext,
        displayName: String,
        eventStream: BackupEventStream
    ) {
        eventStream.emit(.transferState(Self.makeTransferState(
            kind: .upload, workerID: context.workerID, assetLocalIdentifier: prepared.local.assetLocalIdentifier,
            assetDisplayName: displayName, resourceDate: prepared.shotDate,
            assetPosition: context.assetPosition, totalAssets: context.totalAssets,
            resourceDisplayName: prepared.local.originalFilename, resourcePosition: position,
            totalResources: totalResources, resourceFraction: 1,
            resourceBytesTransferred: prepared.fileSize, resourceTotalBytes: prepared.fileSize,
            countsTowardTransferSpeed: result.status == .success,
            stageDescription: String(localized: "backup.transfer.uploadCompleted")
        )))
    }

    private func prepareResource(
        _ selected: BackupSelectedResource,
        position: Int,
        context: AssetProcessContext,
        displayName: String,
        eventStream: BackupEventStream,
        timing: inout AssetProcessTiming,
        cancellationController: BackupCancellationController?
    ) async throws -> PreparedResource {
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()
        let local = makeLocalResource(asset: context.asset, selected: selected,
            preferredAssetNameStem: Self.preferredAssetNameStem(asset: context.asset, selectedResources: context.selectedResources))
        let shotDate = local.asset.creationDate ?? local.resourceModificationDate
        let reportProgress: @Sendable (Double) -> Void = { fraction in
            eventStream.emit(.transferState(Self.makeTransferState(
                kind: .upload, workerID: context.workerID, assetLocalIdentifier: local.assetLocalIdentifier,
                assetDisplayName: displayName, resourceDate: shotDate,
                assetPosition: context.assetPosition, totalAssets: context.totalAssets,
                resourceDisplayName: local.originalFilename, resourcePosition: position,
                totalResources: context.selectedResources.count, resourceFraction: Float(fraction),
                resourceBytesTransferred: nil, resourceTotalBytes: nil, countsTowardTransferSpeed: false,
                stageDescription: String(localized: "backup.transfer.prepareResource")
            )))
        }
        reportProgress(0)
        let start = CFAbsoluteTimeGetCurrent()
        defer { timing.exportHashSeconds += Self.elapsedSeconds(since: start) }
        let exported = try await photoLibraryService.exportResourceToTempFileAndDigest(
            local.resource, cancellationController: cancellationController,
            allowNetworkAccess: context.allowsNetworkExport, onProgress: reportProgress
        )
        if let shotDate {
            try? FileManager.default.setAttributes([.modificationDate: shotDate], ofItemAtPath: exported.fileURL.path)
        }
        return PreparedResource(local: local, tempFileURL: exported.fileURL,
            contentHash: exported.contentHash, fileSize: exported.fileSize, shotDate: shotDate)
    }

    private func networkExportResult(
        error: Error,
        context: AssetProcessContext,
        displayName: String,
        timing: AssetProcessTiming,
        eventStream: BackupEventStream
    ) -> AssetProcessResult? {
        guard !context.allowsNetworkExport, PhotoLibraryService.isNetworkAccessRequiredError(error) else { return nil }
        if context.defersNetworkResources {
            return Self.makeICloudDeferredResult(context: context, displayName: displayName, timing: timing)
        }
        eventStream.emitLog(String.localizedStringWithFormat(
            String(localized: "backup.log.skipICloudResource"), displayName), level: .warning)
        return Self.makeICloudDisabledSkipResult(context: context, displayName: displayName, timing: timing)
    }

    private func assertAssetUnchanged(_ asset: PHAsset) throws {
        guard let current = PHAsset.fetchAssets(withLocalIdentifiers: [asset.localIdentifier], options: nil).firstObject,
              current.modificationDate == asset.modificationDate else {
            try hashIndexRepository.deleteIndexEntries(assetIDs: [asset.localIdentifier])
            throw NSError(domain: PHPhotosErrorDomain, code: PHPhotosError.operationInterrupted.rawValue)
        }
    }

    private func processMatchedAsset(
        fingerprint: Data,
        localResources: [(role: Int, slot: Int, contentHash: Data)],
        preparedResources: [PreparedResource],
        totalFileSizeBytes: Int64,
        context: AssetProcessContext,
        displayName: String,
        skipReason: String,
        client: RemoteStorageClientProtocol,
        eventStream: BackupEventStream,
        timing: inout AssetProcessTiming,
        cancellationController: BackupCancellationController?
    ) async throws -> AssetProcessResult? {
        guard let remoteAsset = context.monthStore.assetsByFingerprint[fingerprint] else { return nil }
        guard let links = BackupAssetResourcePlanner.updatedAdjustmentLinks(
            localResources: localResources, remoteAsset: remoteAsset,
            remoteLinks: context.monthStore.links(forAssetFingerprint: fingerprint)
        ) else {
            let start = CFAbsoluteTimeGetCurrent()
            let dateUpdated = try updateBackedUpAssetDate(fingerprint, context: context)
            timing.databaseSeconds += Self.elapsedSeconds(since: start)
            return AssetProcessResult(status: dateUpdated ? .success : .skipped,
                reason: dateUpdated ? Self.assetDateUpdatedReason : skipReason,
                displayName: displayName, assetFingerprint: fingerprint, timing: timing,
                totalFileSizeBytes: totalFileSizeBytes, uploadedFileSizeBytes: 0)
        }

        var exports: [PreparedResource] = []
        var uploads: [(position: Int, resource: PreparedResource)] = []
        defer { for resource in exports { try? FileManager.default.removeItem(at: resource.tempFileURL) } }
        for link in links where context.monthStore.findResourceByHash(link.resourceHash) == nil {
            guard link.role == ResourceTypeCode.adjustmentData,
                  let position = context.selectedResources.firstIndex(where: { $0.role == link.role && $0.slot == link.slot }) else { return nil }
            let prepared: PreparedResource
            if let existing = preparedResources.first(where: { $0.local.resourceRole == link.role && $0.local.resourceSlot == link.slot }) {
                prepared = existing
            } else {
                do {
                    prepared = try await prepareResource(context.selectedResources[position], position: position + 1,
                        context: context, displayName: displayName, eventStream: eventStream,
                        timing: &timing, cancellationController: cancellationController)
                    exports.append(prepared)
                } catch {
                    if let result = networkExportResult(error: error, context: context,
                        displayName: displayName, timing: timing, eventStream: eventStream) { return result }
                    throw error
                }
            }
            guard prepared.contentHash == link.resourceHash else {
                try hashIndexRepository.deleteIndexEntries(assetIDs: [context.asset.localIdentifier])
                return nil
            }
            uploads.append((position + 1, prepared))
        }
        try assertAssetUnchanged(context.asset)

        var uploadedBytes: Int64 = 0
        for (position, prepared) in uploads {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            let result = try await uploadResource(prepared: prepared, monthStore: context.monthStore,
                profile: context.profile, client: client, workerID: context.workerID,
                resourcePosition: position, totalResources: context.selectedResources.count,
                assetPosition: context.assetPosition, totalAssets: context.totalAssets, displayName: displayName,
                eventStream: eventStream, emitTransferState: true, assetTiming: &timing,
                cancellationController: cancellationController, writeMode: context.writeMode)
            if result.status == .success { uploadedBytes += max(prepared.fileSize, 0) }
            if result.status != .failed {
                emitUploadCompletion(prepared: prepared, result: result, position: position,
                    totalResources: context.selectedResources.count, context: context, displayName: displayName, eventStream: eventStream)
            }
            if result.status == .failed {
                return AssetProcessResult(status: .failed, reason: result.reason,
                    displayName: displayName, assetFingerprint: fingerprint, timing: timing,
                    totalFileSizeBytes: totalFileSizeBytes, uploadedFileSizeBytes: uploadedBytes)
            }
        }

        try await RepoWriteGuard.assertOrdinaryWriteAllowed(context.writeMode)
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()
        try assertAssetUnchanged(context.asset)
        let updated = RemoteManifestAsset(year: remoteAsset.year, month: remoteAsset.month,
            assetFingerprint: fingerprint, creationDateMs: LibraryCreationDate.optionalMilliseconds(context.asset.creationDate),
            backedUpAtMs: Date().millisecondsSinceEpoch, resourceCount: links.count,
            totalFileSizeBytes: links.reduce(0) { $0 + max(context.monthStore.findResourceByHash($1.resourceHash)?.fileSize ?? 0, 0) })
        let start = CFAbsoluteTimeGetCurrent()
        try context.monthStore.upsertAsset(updated, links: links)
        remoteIndexService.upsertCachedAsset(updated, links: links, expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(context.profile))
        timing.databaseSeconds += Self.elapsedSeconds(since: start)
        return AssetProcessResult(status: .success, reason: Self.assetResourcesUpdatedReason,
            displayName: displayName, assetFingerprint: fingerprint, timing: timing,
            totalFileSizeBytes: totalFileSizeBytes, uploadedFileSizeBytes: uploadedBytes)
    }

    private func processWithLocalCache(
        context: AssetProcessContext,
        displayName: String,
        client: RemoteStorageClientProtocol,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> AssetProcessResult? {
        var timing = AssetProcessTiming()
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()
        guard let cachedLocalHash = context.cachedLocalHash else { return nil }
        guard let roleSlotHashes = Self.cachedRoleSlotHashes(
            asset: context.asset, selectedResources: context.selectedResources, cachedLocalHash: cachedLocalHash
        ) else { return nil }
        let cachedFingerprint = cachedLocalHash.assetFingerprint

        if context.monthStore.containsAssetFingerprint(cachedFingerprint),
           !context.monthStore.isAssetIncomplete(cachedFingerprint) {
            return try await processMatchedAsset(
                fingerprint: cachedFingerprint, localResources: roleSlotHashes, preparedResources: [],
                totalFileSizeBytes: cachedLocalHash.totalFileSizeBytes,
                context: context, displayName: displayName, skipReason: "asset_exists_cached",
                client: client, eventStream: eventStream, timing: &timing,
                cancellationController: cancellationController
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
            creationDateMs: LibraryCreationDate.optionalMilliseconds(context.asset.creationDate),
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

    private func updateBackedUpAssetDate(_ fingerprint: Data, context: AssetProcessContext) throws -> Bool {
        guard let updated = try context.monthStore.updateAssetCreationDate(context.asset.creationDate, for: fingerprint) else { return false }
        remoteIndexService.upsertCachedAsset(updated, expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(context.profile))
        return true
    }

    static func cachedRoleSlotHashes(
        asset: PHAsset,
        selectedResources: [BackupSelectedResource],
        cachedLocalHash: LocalAssetHashCache
    ) -> [(role: Int, slot: Int, contentHash: Data)]? {
        guard cachedLocalHash.resourceCount == selectedResources.count,
              cachedLocalHash.hashesByRoleSlot.count == selectedResources.count else { return nil }
        if let modificationDate = asset.modificationDate, modificationDate > cachedLocalHash.updatedAt { return nil }
        var result: [(role: Int, slot: Int, contentHash: Data)] = []
        result.reserveCapacity(selectedResources.count)

        for selected in selectedResources {
            let key = AssetResourceRoleSlot(role: selected.role, slot: selected.slot)
            guard let contentHash = cachedLocalHash.hashesByRoleSlot[key] else {
                return nil
            }
            result.append((role: selected.role, slot: selected.slot, contentHash: contentHash))
        }

        guard cachedLocalHash.assetFingerprint == BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: result) else { return nil }
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

    // Deferred assets must not be counted or marked resume-complete in the local pass.
    static let iCloudDeferredReason = "icloud_deferred_to_icloud_pass"

    private static func makeICloudDeferredResult(
        context: AssetProcessContext,
        displayName: String,
        timing: AssetProcessTiming
    ) -> AssetProcessResult {
        AssetProcessResult(
            status: .skipped,
            reason: iCloudDeferredReason,
            displayName: displayName,
            assetFingerprint: nil,
            timing: timing,
            totalFileSizeBytes: totalSizeBytes(of: context.selectedResources),
            uploadedFileSizeBytes: 0
        )
    }
}

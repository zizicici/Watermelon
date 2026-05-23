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
    let optimisticWriter: OptimisticAssetWriter

    init(
        photoLibraryService: PhotoLibraryService,
        hashIndexRepository: ContentHashIndexRepository,
        remoteIndexService: RemoteIndexSyncService
    ) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService
        self.optimisticWriter = remoteIndexService.makeOptimisticAssetWriter()
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
                        stageDescription: String(localized: "backup.transfer.uploadCompleted")
                    )
                ))
            }

            if uploadResult.status != .failed {
                // logicalName is what restore surfaces as `originalFilename`; must reflect the user's name, not the collision-renamed remote path.
                let originalLogicalName = prepared.local.preferredRemoteFileName
                links.append(
                    RemoteAssetResourceLink(
                        year: context.monthStore.year,
                        month: context.monthStore.month,
                        assetFingerprint: assetFingerprint,
                        resourceHash: prepared.contentHash,
                        role: prepared.local.resourceRole,
                        slot: prepared.local.resourceSlot,
                        logicalName: originalLogicalName
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
        let resourceKeys = AssetResourceLinkSetPredicate.keys(fromLinks: links)
        let subsetFingerprints = Set(
            context.monthStore.findStrictSubsetAssetFingerprints(forResourceKeys: resourceKeys)
        )
        try context.monthStore.upsertAsset(
            manifestAsset,
            links: links,
            replacingSubsetFingerprints: subsetFingerprints
        )
        timing.databaseSeconds += Self.elapsedSeconds(since: manifestWriteStart)

        let resourceSignature = BackupAssetResourcePlanner.resourceSignature(
            orderedResources: context.selectedResources
        )
        try await finalizeRowWritingAsset(
            monthStore: context.monthStore,
            manifestAsset: manifestAsset,
            links: links,
            timing: &timing,
            tombstonedSubsetFingerprints: subsetFingerprints
        ) { [hashIndexRepository] in
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
                modificationDateMs: context.asset.modificationDate?.millisecondsSinceEpoch,
                selectionVersion: BackupAssetResourcePlanner.currentSelectionVersion,
                resourceSignature: resourceSignature
            )
        }

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

    private func processWithLocalCache(
        context: AssetProcessContext,
        displayName: String,
        cancellationController: BackupCancellationController?
    ) async throws -> AssetProcessResult? {
        var timing = AssetProcessTiming()
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()
        guard let cachedLocalHash = context.cachedLocalHash else { return nil }
        guard cachedLocalHash.resourceCount == context.selectedResources.count else { return nil }
        guard LocalHashIndexTrust.cacheFieldsPassCheapChecks(
            cachedLocalHash.trustFields,
            modificationDate: context.asset.modificationDate
        ) else { return nil }

        let currentSignature = BackupAssetResourcePlanner.resourceSignature(
            orderedResources: context.selectedResources
        )
        guard LocalHashIndexTrust.signatureMatches(cachedLocalHash.trustFields, currentSignature: currentSignature) else { return nil }

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
        // Subset survivors (older partial assets in the manifest whose links are a strict
        // subset of this asset's) also force the fall-through so the resources-reused
        // upsert tombstones them — without this, pre-fix backups never self-heal.
        let cachedResourceKeys = Set(
            roleSlotHashes.map {
                AssetResourceLinkKey(role: $0.role, slot: $0.slot, hash: $0.contentHash)
            }
        )
        if context.monthStore.containsAssetFingerprint(cachedFingerprint),
           !context.monthStore.isAssetIncomplete(cachedFingerprint),
           !context.monthStore.hasStrictSubsetAssetFingerprint(
               forResourceKeys: cachedResourceKeys
           ) {
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

        // logicalName must reflect the user's name; reusing the existing resource's name would surface another asset's collision-renamed filename on restore.
        let preferredAssetNameStem = Self.preferredAssetNameStem(
            asset: context.asset,
            selectedResources: context.selectedResources
        )
        let logicalNamesByRoleSlot: [AssetResourceRoleSlot: String] = Dictionary(
            uniqueKeysWithValues: context.selectedResources.map { selected in
                let key = AssetResourceRoleSlot(role: selected.role, slot: selected.slot)
                let name = Self.preferredRemoteFileName(
                    preferredAssetNameStem: preferredAssetNameStem,
                    selected: selected
                )
                return (key, name)
            }
        )
        let links = roleSlotHashes.map { item in
            let key = AssetResourceRoleSlot(role: item.role, slot: item.slot)
            let logical = logicalNamesByRoleSlot[key] ?? ""
            return RemoteAssetResourceLink(
                year: context.monthStore.year,
                month: context.monthStore.month,
                assetFingerprint: cachedFingerprint,
                resourceHash: item.contentHash,
                role: item.role,
                slot: item.slot,
                logicalName: logical
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
        let resourceKeys = AssetResourceLinkSetPredicate.keys(fromLinks: links)
        let subsetFingerprints = Set(
            context.monthStore.findStrictSubsetAssetFingerprints(forResourceKeys: resourceKeys)
        )
        try context.monthStore.upsertAsset(
            manifestAsset,
            links: links,
            replacingSubsetFingerprints: subsetFingerprints
        )
        timing.databaseSeconds += Self.elapsedSeconds(since: manifestWriteStart)

        try await finalizeRowWritingAsset(
            monthStore: context.monthStore,
            manifestAsset: manifestAsset,
            links: links,
            timing: &timing,
            tombstonedSubsetFingerprints: subsetFingerprints
        ) { [hashIndexRepository] in
            try hashIndexRepository.upsertAssetFingerprint(
                assetLocalIdentifier: context.asset.localIdentifier,
                assetFingerprint: cachedFingerprint,
                resourceCount: context.selectedResources.count,
                totalFileSizeBytes: totalFileSizeBytes,
                modificationDateMs: context.asset.modificationDate?.millisecondsSinceEpoch
            )
        }

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

    func finalizeRowWritingAsset(
        monthStore: any BackupMonthStore,
        manifestAsset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        timing: inout AssetProcessTiming,
        tombstonedSubsetFingerprints: Set<Data> = [],
        hashIndexWrite: () throws -> Void
    ) async throws {
        let commitStart = CFAbsoluteTimeGetCurrent()
        let delta = try await monthStore.commitPendingAssetToRemote(ignoreCancellation: false)
        timing.databaseSeconds += Self.elapsedSeconds(since: commitStart)
        publishCommittedSweepIfNeeded(
            monthStore: monthStore,
            manifestAsset: manifestAsset,
            delta: delta,
            tombstonedSubsetFingerprints: tombstonedSubsetFingerprints
        )
        optimisticWriter.appendAsset(manifestAsset, links: links)

        let dbStart = CFAbsoluteTimeGetCurrent()
        try hashIndexWrite()
        timing.databaseSeconds += Self.elapsedSeconds(since: dbStart)
    }

    private func publishCommittedSweepIfNeeded(
        monthStore: any BackupMonthStore,
        manifestAsset: RemoteManifestAsset,
        delta: MonthManifestStore.FlushDelta,
        tombstonedSubsetFingerprints: Set<Data>
    ) {
        // V1 commits eagerly inside upsertAsset, so `delta` always reports no tombstones —
        // the `tombstonedSubsetFingerprints` argument is the only signal that subset rows
        // were just removed and the cache needs eviction.
        guard !delta.committedV2TombstoneFingerprints.isEmpty ||
            !tombstonedSubsetFingerprints.subtracting([manifestAsset.assetFingerprint]).isEmpty ||
            delta.committedV2AssetFingerprints.subtracting([manifestAsset.assetFingerprint]).isEmpty == false else {
            return
        }
        let snapshot = monthStore.unsortedSnapshot()
        remoteIndexService.replaceCachedMonth(
            LibraryMonthKey(year: monthStore.year, month: monthStore.month),
            resources: snapshot.resources,
            assets: snapshot.assets,
            links: snapshot.links,
            physicallyMissingHashes: monthStore.physicallyMissingHashesAreAuthoritative
                ? monthStore.physicallyMissingHashesSnapshot()
                : nil
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

import CryptoKit
import Foundation
import Photos

struct AssetProcessContext {
    let asset: PHAsset
    let selectedResources: [BackupSelectedResource]
    let cachedLocalHash: LocalAssetHashCache?
    let monthStore: MonthManifestStore
    let profile: ServerProfileRecord
    let assetPosition: Int
    let totalAssets: Int
}

struct AssetProcessResult: Sendable {
    let status: BackupItemStatus
    let reason: String?
    let displayName: String
    let resourceSummary: String
    let assetFingerprint: Data?
}

struct PreparedResource: Sendable {
    let local: LocalPhotoResource
    let tempFileURL: URL
    let contentHash: Data
    let fileSize: Int64
    let shotDate: Date?
}

struct ResourceUploadResult: Sendable {
    let status: BackupItemStatus
    let reason: String?
}

private struct UploadPreparation {
    var targetFileName: String
    var remoteAbsolutePath: String
    var attemptedFileNames: Set<String>
    let skipReason: String?
}

private struct UploadRetryOutcome {
    let fileName: String?
    let lastError: Error?
}

final class AssetProcessor: Sendable {
    private static let monthCalendar = Calendar(identifier: .gregorian)
    private static let smallFileThresholdBytes: Int64 = 5 * 1024 * 1024
    private static let hashBufferSize = 64 * 1024
    private static let transferProgressMinimumStep = 0.01
    private static let transferProgressMinimumInterval: TimeInterval = 0.12

    private let photoLibraryService: PhotoLibraryServiceProtocol
    private let hashIndexRepository: ContentHashIndexRepositoryProtocol
    private let remoteIndexService: RemoteIndexSyncService

    init(
        photoLibraryService: PhotoLibraryServiceProtocol,
        hashIndexRepository: ContentHashIndexRepositoryProtocol,
        remoteIndexService: RemoteIndexSyncService
    ) {
        self.photoLibraryService = photoLibraryService
        self.hashIndexRepository = hashIndexRepository
        self.remoteIndexService = remoteIndexService
    }

    static func monthKey(for date: Date?) -> MonthKey {
        let date = date ?? Date(timeIntervalSince1970: 0)
        let comps = monthCalendar.dateComponents([.year, .month], from: date)
        return MonthKey(
            year: comps.year ?? 1970,
            month: comps.month ?? 1
        )
    }

    func process(
        context: AssetProcessContext,
        client: RemoteStorageClientProtocol,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> AssetProcessResult {
        var preparedResources: [PreparedResource] = []
        preparedResources.reserveCapacity(context.selectedResources.count)

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

        for (resourcePosition, selected) in context.selectedResources.enumerated() {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()

            let local = makeLocalResource(asset: context.asset, selected: selected)
            eventStream.emit(.transferState(
                Self.makeTransferState(
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

            let tempFileURL = try await photoLibraryService.exportResourceToTempFile(
                local.resource,
                cancellationController: cancellationController
            )
            var shouldRemoveTempFile = true
            defer {
                if shouldRemoveTempFile {
                    try? FileManager.default.removeItem(at: tempFileURL)
                }
            }
            let localHash = try Self.contentHash(of: tempFileURL, cancellationController: cancellationController)
            let localFileSize = max(
                local.fileSize,
                (try? FileManager.default.attributesOfItem(atPath: tempFileURL.path)[.size] as? NSNumber)?.int64Value ?? 0
            )
            let shotDate = local.asset.creationDate ?? local.resourceModificationDate
            if let shotDate {
                try? FileManager.default.setAttributes([.modificationDate: shotDate], ofItemAtPath: tempFileURL.path)
            }

            try hashIndexRepository.upsertAssetResource(
                assetLocalIdentifier: local.assetLocalIdentifier,
                role: local.resourceRole,
                slot: local.resourceSlot,
                contentHash: localHash,
                fileSize: localFileSize
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
                resourcePosition: resourcePosition + 1,
                totalResources: preparedResources.count,
                assetPosition: context.assetPosition,
                totalAssets: context.totalAssets,
                displayName: displayName,
                eventStream: eventStream,
                cancellationController: cancellationController
            )
            uploadResults.append(uploadResult)

            eventStream.emit(.transferState(
                Self.makeTransferState(
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

        let failedResults = uploadResults.filter { $0.status == .failed }
        let skippedResults = uploadResults.filter { $0.status == .skipped }
        let successResults = uploadResults.filter { $0.status == .success }
        let totalFileSizeBytes = preparedResources.reduce(Int64(0)) { partial, prepared in
            partial + max(prepared.fileSize, 0)
        }

        if !failedResults.isEmpty {
            let firstError = failedResults.first?.reason ?? "resource_failed"
            eventStream.emit(.log(
                "Asset failed (partial): \(displayName). success=\(successResults.count), skipped=\(skippedResults.count), failed=\(failedResults.count)"
            ))
            return AssetProcessResult(
                status: .failed,
                reason: firstError,
                displayName: displayName,
                resourceSummary: "资源\(preparedResources.count) 上传\(successResults.count) 跳过\(skippedResults.count) 失败\(failedResults.count)",
                assetFingerprint: assetFingerprint
            )
        }

        let manifestAsset = RemoteManifestAsset(
            year: context.monthStore.year,
            month: context.monthStore.month,
            assetFingerprint: assetFingerprint,
            creationDateNs: Self.nanosecondsSinceEpoch(context.asset.creationDate),
            backedUpAtNs: Self.nanosecondsSinceEpoch(Date()) ?? 0,
            resourceCount: links.count,
            totalFileSizeBytes: totalFileSizeBytes
        )

        try context.monthStore.upsertAsset(manifestAsset, links: links)
        remoteIndexService.upsertCachedAsset(manifestAsset, links: links)

        try hashIndexRepository.upsertAssetFingerprint(
            assetLocalIdentifier: context.asset.localIdentifier,
            assetFingerprint: assetFingerprint,
            resourceCount: preparedResources.count,
            totalFileSizeBytes: totalFileSizeBytes
        )

        if successResults.isEmpty {
            return AssetProcessResult(
                status: .skipped,
                reason: "resources_reused",
                displayName: displayName,
                resourceSummary: "资源\(preparedResources.count) 上传0 跳过\(skippedResults.count) 失败0",
                assetFingerprint: assetFingerprint
            )
        }

        return AssetProcessResult(
            status: .success,
            reason: nil,
            displayName: displayName,
            resourceSummary: "资源\(preparedResources.count) 上传\(successResults.count) 跳过\(skippedResults.count) 失败0",
            assetFingerprint: assetFingerprint
        )
    }

    private func processWithLocalCache(
        context: AssetProcessContext,
        displayName: String,
        cancellationController: BackupCancellationController?
    ) throws -> AssetProcessResult? {
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
            try hashIndexRepository.upsertAssetFingerprint(
                assetLocalIdentifier: context.asset.localIdentifier,
                assetFingerprint: cachedFingerprint,
                resourceCount: context.selectedResources.count,
                totalFileSizeBytes: totalFileSizeBytes
            )
            return AssetProcessResult(
                status: .skipped,
                reason: "asset_exists_cached",
                displayName: displayName,
                resourceSummary: "资源\(context.selectedResources.count) 已存在（缓存命中）",
                assetFingerprint: cachedFingerprint
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
            creationDateNs: Self.nanosecondsSinceEpoch(context.asset.creationDate),
            backedUpAtNs: Self.nanosecondsSinceEpoch(Date()) ?? 0,
            resourceCount: links.count,
            totalFileSizeBytes: totalFileSizeBytes
        )
        try context.monthStore.upsertAsset(manifestAsset, links: links)
        remoteIndexService.upsertCachedAsset(manifestAsset, links: links)

        try hashIndexRepository.upsertAssetFingerprint(
            assetLocalIdentifier: context.asset.localIdentifier,
            assetFingerprint: cachedFingerprint,
            resourceCount: context.selectedResources.count,
            totalFileSizeBytes: totalFileSizeBytes
        )

        return AssetProcessResult(
            status: .skipped,
            reason: "resources_reused_cached",
            displayName: displayName,
            resourceSummary: "资源\(context.selectedResources.count) 上传0 跳过\(context.selectedResources.count) 失败0（缓存命中）",
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

    private func uploadResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        resourcePosition: Int,
        totalResources: Int,
        assetPosition: Int,
        totalAssets: Int,
        displayName: String,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> ResourceUploadResult {
        let localHash = prepared.contentHash

        if monthStore.findResourceByHash(localHash) != nil {
            return ResourceUploadResult(status: .skipped, reason: "hash_exists")
        }

        var preparation = try await prepareUpload(
            prepared: prepared,
            monthStore: monthStore,
            profile: profile,
            client: client,
            cancellationController: cancellationController
        )

        if let skipReason = preparation.skipReason {
            try recordSkippedResource(
                prepared: prepared,
                monthStore: monthStore,
                targetFileName: preparation.targetFileName
            )
            return ResourceUploadResult(status: .skipped, reason: skipReason)
        }

        let retryOutcome = try await performUploadWithRetry(
            prepared: prepared,
            monthStore: monthStore,
            profile: profile,
            client: client,
            uploadPreparation: &preparation,
            resourcePosition: resourcePosition,
            totalResources: totalResources,
            assetPosition: assetPosition,
            totalAssets: totalAssets,
            displayName: displayName,
            eventStream: eventStream,
            cancellationController: cancellationController
        )

        guard let uploadedFileName = retryOutcome.fileName else {
            return ResourceUploadResult(
                status: .failed,
                reason: retryOutcome.lastError?.localizedDescription ?? "Unknown upload failure"
            )
        }

        try recordUploadedResource(
            prepared: prepared,
            monthStore: monthStore,
            targetFileName: uploadedFileName
        )
        return ResourceUploadResult(status: .success, reason: nil)
    }

    private func prepareUpload(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        cancellationController: BackupCancellationController?
    ) async throws -> UploadPreparation {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize

        var targetFileName = RemotePathBuilder.sanitizeFilename(local.originalFilename)
        var skipReason: String?
        var attemptedFileNames: Set<String> = [targetFileName]

        if monthStore.existingFileNames().contains(targetFileName) {
            let existingManifestResource = monthStore.findByFileName(targetFileName)
            let knownRemoteSize = existingManifestResource?.fileSize ?? monthStore.remoteEntry(named: targetFileName)?.size
            if localFileSize < Self.smallFileThresholdBytes {
                if let knownRemoteSize, knownRemoteSize != localFileSize {
                    // Different size means definitely not the same file; avoid remote download+hash.
                } else {
                    try cancellationController?.throwIfCancelled()
                    try Task.checkCancellation()
                    let remoteHash = try await downloadAndHashRemoteFile(
                        profile: profile,
                        client: client,
                        monthStore: monthStore,
                        fileName: targetFileName,
                        cancellationController: cancellationController
                    )
                    if remoteHash == localHash {
                        skipReason = "name_same_hash"
                    }
                }
            } else if existingManifestResource == nil {
                if knownRemoteSize == localFileSize {
                    skipReason = "name_same_size"
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

        let remoteRelativePath = monthStore.monthRelativePath + "/" + targetFileName
        let remoteAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: remoteRelativePath
        )

        return UploadPreparation(
            targetFileName: targetFileName,
            remoteAbsolutePath: remoteAbsolutePath,
            attemptedFileNames: attemptedFileNames,
            skipReason: skipReason
        )
    }

    private func recordSkippedResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        targetFileName: String
    ) throws {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize
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
            remoteIndexService.upsertCachedResource(inserted)
        }

        monthStore.markRemoteFile(name: targetFileName, size: localFileSize, creationDate: local.asset.creationDate)
    }

    private func performUploadWithRetry(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        uploadPreparation: inout UploadPreparation,
        resourcePosition: Int,
        totalResources: Int,
        assetPosition: Int,
        totalAssets: Int,
        displayName: String,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> UploadRetryOutcome {
        let local = prepared.local
        let maxRetry = 3
        var lastError: Error?
        let progressEmitLock = NSLock()
        var lastProgressFraction = -1.0
        var lastProgressEmitAt = Date.distantPast

        for attempt in 0 ..< maxRetry {
            do {
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
                try await client.upload(
                    localURL: prepared.tempFileURL,
                    remotePath: uploadPreparation.remoteAbsolutePath,
                    respectTaskCancellation: true,
                    onProgress: { fraction in
                        let clamped = max(0, min(1, fraction))
                        let now = Date()
                        let shouldEmit: Bool
                        progressEmitLock.lock()
                        if clamped >= 1 {
                            shouldEmit = true
                            lastProgressFraction = clamped
                            lastProgressEmitAt = now
                        } else {
                            let advancedEnough = (clamped - lastProgressFraction) >= Self.transferProgressMinimumStep
                            let waitedEnough = now.timeIntervalSince(lastProgressEmitAt) >= Self.transferProgressMinimumInterval
                            shouldEmit = advancedEnough || waitedEnough
                            if shouldEmit {
                                lastProgressFraction = clamped
                                lastProgressEmitAt = now
                            }
                        }
                        progressEmitLock.unlock()

                        guard shouldEmit else { return }
                        eventStream.emit(.transferState(
                            Self.makeTransferState(
                                assetLocalIdentifier: local.assetLocalIdentifier,
                                assetDisplayName: displayName,
                                resourceDate: prepared.shotDate,
                                assetPosition: assetPosition,
                                totalAssets: totalAssets,
                                resourceDisplayName: local.originalFilename,
                                resourcePosition: resourcePosition,
                                totalResources: totalResources,
                                resourceFraction: Float(clamped),
                                stageDescription: "上传资源"
                            )
                        ))
                    }
                )
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
                if let shotDate = prepared.shotDate {
                    try? await client.setModificationDate(shotDate, forPath: uploadPreparation.remoteAbsolutePath)
                }
                return UploadRetryOutcome(fileName: uploadPreparation.targetFileName, lastError: nil)
            } catch {
                if error is CancellationError {
                    throw error
                }
                if cancellationController?.isCancelled == true {
                    throw CancellationError()
                }
                if profile.isExternalStorageUnavailableError(error) {
                    throw error
                }
                lastError = error

                let message = error.localizedDescription
                if message.contains("STATUS_OBJECT_NAME_COLLISION") {
                    var occupiedNames = monthStore.existingFileNames()
                    occupiedNames.formUnion(uploadPreparation.attemptedFileNames)
                    occupiedNames.insert(uploadPreparation.targetFileName)
                    uploadPreparation.targetFileName = RemoteNameCollisionResolver.resolveNextAvailableName(
                        baseName: uploadPreparation.targetFileName,
                        occupiedNames: occupiedNames
                    )
                    uploadPreparation.attemptedFileNames.insert(uploadPreparation.targetFileName)
                    let retryRelativePath = monthStore.monthRelativePath + "/" + uploadPreparation.targetFileName
                    uploadPreparation.remoteAbsolutePath = RemotePathBuilder.absolutePath(
                        basePath: profile.basePath,
                        remoteRelativePath: retryRelativePath
                    )
                    continue
                }

                if attempt < maxRetry - 1 {
                    let sleepNanos = UInt64(pow(2.0, Double(attempt))) * 500_000_000
                    do {
                        try await Task.sleep(nanoseconds: sleepNanos)
                    } catch {
                        throw CancellationError()
                    }
                    continue
                }
            }
        }

        return UploadRetryOutcome(fileName: nil, lastError: lastError)
    }

    private func recordUploadedResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        targetFileName: String
    ) throws {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize
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
        remoteIndexService.upsertCachedResource(inserted)
    }

    private func downloadAndHashRemoteFile(
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        monthStore: MonthManifestStore,
        fileName: String,
        cancellationController: BackupCancellationController?
    ) async throws -> Data {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("remote_compare_\(UUID().uuidString)_\(fileName)")
        try? FileManager.default.removeItem(at: tempURL)
        defer {
            try? FileManager.default.removeItem(at: tempURL)
        }

        try cancellationController?.throwIfCancelled()
        let remotePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: monthStore.monthRelativePath + "/" + fileName
        )
        try await client.download(remotePath: remotePath, localURL: tempURL)
        return try Self.contentHash(of: tempURL, cancellationController: cancellationController)
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

    private static func contentHash(
        of fileURL: URL,
        cancellationController: BackupCancellationController? = nil
    ) throws -> Data {
        let fileHandle = try FileHandle(forReadingFrom: fileURL)
        defer {
            try? fileHandle.close()
        }

        var hasher = SHA256()
        while true {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            let chunk = try fileHandle.read(upToCount: hashBufferSize) ?? Data()
            if chunk.isEmpty {
                break
            }
            hasher.update(data: chunk)
        }

        return Data(hasher.finalize())
    }

    private static func nanosecondsSinceEpoch(_ date: Date?) -> Int64? {
        guard let date else { return nil }
        return Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())
    }

    private static func totalSizeBytes(of selectedResources: [BackupSelectedResource]) -> Int64 {
        selectedResources.reduce(Int64(0)) { partial, selected in
            partial + max(PhotoLibraryService.resourceFileSize(selected.resource), 0)
        }
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
}

struct MonthKey: Hashable, Comparable, Sendable {
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

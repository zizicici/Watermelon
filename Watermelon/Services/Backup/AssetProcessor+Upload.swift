import CryptoKit
import Foundation
import Photos

struct UploadPreparation {
    let baseFileName: String
    var targetFileName: String
    var remoteAbsolutePath: String
    var attemptedFileNames: Set<String>
    let skipReason: String?
}

struct UploadRetryOutcome {
    let fileName: String?
    let lastError: Error?
}

extension AssetProcessor {
    // A Lite lease/ownership loss is terminal for a data upload: retrying, backing off, or renaming on a
    // collision cannot recover a lease that is no longer confidently held, so it must propagate at once.
    static func isLeaseFailFast(_ error: Error) -> Bool {
        guard let liteError = error as? LiteRepoError else { return false }
        return liteError.isUploadFailFast
    }

    // A transient fault on a network backend (not an ejected external volume) that reconnect + backoff can recover.
    static func isRecoverableNetworkFault(_ error: Error, profile: ServerProfileRecord) -> Bool {
        !profile.isBrowserLinkProfile &&
            profile.resolvedStorageType != .externalVolume &&
            RemoteFaultLite.classify(error) == .retryable
    }

    func uploadResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        workerID: Int,
        resourcePosition: Int,
        totalResources: Int,
        assetPosition: Int,
        totalAssets: Int,
        displayName: String,
        eventStream: BackupEventStream,
        emitTransferState: Bool,
        assetTiming: inout AssetProcessTiming,
        cancellationController: BackupCancellationController?,
        writeMode: RepoWriteMode
    ) async throws -> ResourceUploadResult {
        let localHash = prepared.contentHash

        if monthStore.findResourceByHash(localHash) != nil {
            return ResourceUploadResult(status: .skipped, reason: "hash_exists")
        }

        let collisionStart = CFAbsoluteTimeGetCurrent()
        var preparation: UploadPreparation
        do {
            preparation = try await prepareUpload(
                prepared: prepared,
                monthStore: monthStore,
                profile: profile,
                client: client,
                displayName: displayName,
                eventStream: eventStream,
                cancellationController: cancellationController
            )
        } catch {
            if !(error is CancellationError) {
                print("[BackupUpload] prepare FAILED: asset=\(displayName), resource=\(prepared.local.originalFilename), reason=\(error.localizedDescription)")
            }
            throw error
        }
        let collisionSeconds = Self.elapsedSeconds(since: collisionStart)
        assetTiming.collisionCheckSeconds += collisionSeconds
        Self.traceOneDriveUploadStage(
            profile: profile,
            stage: "prepare",
            displayName: displayName,
            resourceName: prepared.local.originalFilename,
            remotePath: preparation.remoteAbsolutePath,
            size: prepared.fileSize,
            duration: collisionSeconds,
            extra: "skip=\(preparation.skipReason ?? "-")"
        )

        if let skipReason = preparation.skipReason {
            let dbStart = CFAbsoluteTimeGetCurrent()
            try recordSkippedResource(
                prepared: prepared,
                monthStore: monthStore,
                profile: profile,
                targetFileName: preparation.targetFileName
            )
            let dbSeconds = Self.elapsedSeconds(since: dbStart)
            assetTiming.databaseSeconds += dbSeconds
            Self.traceOneDriveUploadStage(
                profile: profile,
                stage: "manifest.recordSkippedResource",
                displayName: displayName,
                resourceName: prepared.local.originalFilename,
                remotePath: preparation.remoteAbsolutePath,
                size: prepared.fileSize,
                duration: dbSeconds,
                extra: "reason=\(skipReason)"
            )
            return ResourceUploadResult(status: .skipped, reason: skipReason)
        }

        // No standalone pre-upload gate: performUploadWithRetry's per-attempt gate (attempt 1 + retries)
        // is the "before writing bytes" proof; a separate one here would be a redundant duplicate.
        let retryOutcome = try await performUploadWithRetry(
            prepared: prepared,
            monthStore: monthStore,
            profile: profile,
            client: client,
            uploadPreparation: &preparation,
            workerID: workerID,
            resourcePosition: resourcePosition,
            totalResources: totalResources,
            assetPosition: assetPosition,
            totalAssets: totalAssets,
            displayName: displayName,
            eventStream: eventStream,
            emitTransferState: emitTransferState,
            assetTiming: &assetTiming,
            cancellationController: cancellationController,
            writeMode: writeMode
        )

        guard let uploadedFileName = retryOutcome.fileName else {
            let reason = retryOutcome.lastError?.localizedDescription ?? "Unknown upload failure"
            print("[BackupUpload] upload FAILED: asset=\(displayName), resource=\(prepared.local.originalFilename), reason=\(reason)")
            return ResourceUploadResult(
                status: .failed,
                reason: reason
            )
        }

        // Completion fence: a long upload can outlive lease confidence before we record it in the manifest.
        try await RepoWriteGuard.assertDataWriteAllowed(writeMode)

        let dbStart = CFAbsoluteTimeGetCurrent()
        try recordUploadedResource(
            prepared: prepared,
            monthStore: monthStore,
            profile: profile,
            targetFileName: uploadedFileName
        )
        let dbSeconds = Self.elapsedSeconds(since: dbStart)
        assetTiming.databaseSeconds += dbSeconds
        Self.traceOneDriveUploadStage(
            profile: profile,
            stage: "manifest.recordUploadedResource",
            displayName: displayName,
            resourceName: prepared.local.originalFilename,
            remotePath: preparation.remoteAbsolutePath,
            size: prepared.fileSize,
            duration: dbSeconds
        )
        return ResourceUploadResult(status: .success, reason: nil)
    }

    func prepareUpload(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        displayName: String,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> UploadPreparation {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize

        let baseFileName = profile.storageProfile.remoteFileNamePolicy.sanitize(
            local.preferredRemoteFileName
        )
        var targetFileName = baseFileName
        var skipReason: String?
        var attemptedFileNames: Set<String> = [targetFileName]

        // Read the maintained fold; re-folding every name per resource is O(N^2) per month.
        let existingCollisionKeys = monthStore.existingCollisionKeys()
        if existingCollisionKeys.contains(RemoteFileNaming.collisionKey(for: targetFileName)) {
            let existingManifestResource = monthStore.findByFileName(targetFileName)
            let knownRemoteSize = existingManifestResource?.fileSize ?? monthStore.remoteFileSize(named: targetFileName)
            let shouldDownloadRemoteForNameCollision =
                (client as? OneDriveUploadCollisionPolicyClient)?.shouldDownloadRemoteFileForNameCollision ?? true
            if shouldDownloadRemoteForNameCollision, localFileSize < Self.smallFileThresholdBytes {
                if let knownRemoteSize, knownRemoteSize != localFileSize {
                    // Different size means definitely not the same file; avoid remote download+hash.
                } else {
                    try cancellationController?.throwIfCancelled()
                    try Task.checkCancellation()
                    let remoteHash = try await downloadAndHashRemoteFileForConflictCheck(
                        profile: profile,
                        client: client,
                        monthStore: monthStore,
                        fileName: targetFileName,
                        displayName: displayName,
                        eventStream: eventStream,
                        cancellationController: cancellationController
                    )
                    if let remoteHash, remoteHash == localHash {
                        skipReason = "name_same_hash"
                    }
                }
            }
            if skipReason == nil {
                targetFileName = RemoteFileNaming.resolveNextAvailableName(
                    baseName: baseFileName,
                    collisionKeys: existingCollisionKeys,
                    maximumLength: profile.storageProfile.remoteFileNamePolicy.maximumComponentLength
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
            baseFileName: baseFileName,
            targetFileName: targetFileName,
            remoteAbsolutePath: remoteAbsolutePath,
            attemptedFileNames: attemptedFileNames,
            skipReason: skipReason
        )
    }

    func recordSkippedResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
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
            creationDateMs: LibraryCreationDate.optionalMilliseconds(prepared.shotDate),
            backedUpAtMs: Date().millisecondsSinceEpoch
        )

        if monthStore.findResourceByHash(localHash) == nil {
            let inserted = try monthStore.upsertResource(manifestItem)
            remoteIndexService.upsertCachedResource(inserted, expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(profile))
        }

        monthStore.markRemoteFile(name: targetFileName, size: localFileSize)
    }

    func performUploadWithRetry(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        uploadPreparation: inout UploadPreparation,
        workerID: Int,
        resourcePosition: Int,
        totalResources: Int,
        assetPosition: Int,
        totalAssets: Int,
        displayName: String,
        eventStream: BackupEventStream,
        emitTransferState: Bool,
        assetTiming: inout AssetProcessTiming,
        cancellationController: BackupCancellationController?,
        writeMode: RepoWriteMode
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
                // Before writing remote data bytes (attempt 1 + retries): the lease must still be held.
                try await RepoWriteGuard.assertDataWriteAllowed(writeMode)
                let uploadBodyStart = CFAbsoluteTimeGetCurrent()
                let onProgress: ((Double) -> Void)?
                if emitTransferState {
                    onProgress = { fraction in
                        let clamped = max(0, min(1, fraction))
                        let now = Date()
                        let shouldEmit: Bool = progressEmitLock.withLock {
                            if clamped >= 1 {
                                lastProgressFraction = clamped
                                lastProgressEmitAt = now
                                return true
                            } else {
                                let advancedEnough = (clamped - lastProgressFraction) >= Self.transferProgressMinimumStep
                                let waitedEnough = now.timeIntervalSince(lastProgressEmitAt) >= Self.transferProgressMinimumInterval
                                if advancedEnough || waitedEnough {
                                    lastProgressFraction = clamped
                                    lastProgressEmitAt = now
                                    return true
                                }
                                return false
                            }
                        }

                        guard shouldEmit else { return }
                        eventStream.emit(.transferState(
                            Self.makeTransferState(
                                kind: .upload,
                                workerID: workerID,
                                assetLocalIdentifier: local.assetLocalIdentifier,
                                assetDisplayName: displayName,
                                resourceDate: prepared.shotDate,
                                assetPosition: assetPosition,
                                totalAssets: totalAssets,
                                resourceDisplayName: local.originalFilename,
                                resourcePosition: resourcePosition,
                                totalResources: totalResources,
                                resourceFraction: Float(clamped),
                                resourceBytesTransferred: Int64((Double(prepared.fileSize) * clamped).rounded()),
                                resourceTotalBytes: prepared.fileSize,
                                stageDescription: String(localized: "backup.transfer.uploadResource")
                            )
                        ))
                    }
                } else {
                    onProgress = nil
                }
                do {
                    let uploadMode: RemoteUploadMode =
                        client is OneDriveUploadCollisionPolicyClient ? .createIfAbsent : .replace
                    try await client.upload(
                        localURL: prepared.tempFileURL,
                        remotePath: uploadPreparation.remoteAbsolutePath,
                        mode: uploadMode,
                        respectTaskCancellation: true,
                        onProgress: onProgress
                    )
                } catch {
                    let uploadSeconds = Self.elapsedSeconds(since: uploadBodyStart)
                    assetTiming.uploadBodySeconds += uploadSeconds
                    Self.traceOneDriveUploadStage(
                        profile: profile,
                        stage: "uploadBody.failed",
                        displayName: displayName,
                        resourceName: local.originalFilename,
                        remotePath: uploadPreparation.remoteAbsolutePath,
                        size: prepared.fileSize,
                        duration: uploadSeconds,
                        extra: "attempt=\(attempt + 1) error=\((error as NSError).localizedDescription)"
                    )
                    if profile.resolvedStorageType == .onedrive {
                        eventStream.emitLog(
                            "[OneDriveUpload] failed asset=\(displayName) resource=\(local.originalFilename) size=\(prepared.fileSize) attempt=\(attempt + 1) path=\(uploadPreparation.remoteAbsolutePath) error=\((error as NSError).localizedDescription)",
                            level: .debug
                        )
                    }
                    throw error
                }
                let uploadSeconds = Self.elapsedSeconds(since: uploadBodyStart)
                assetTiming.uploadBodySeconds += uploadSeconds
                Self.traceOneDriveUploadStage(
                    profile: profile,
                    stage: "uploadBody",
                    displayName: displayName,
                    resourceName: local.originalFilename,
                    remotePath: uploadPreparation.remoteAbsolutePath,
                    size: prepared.fileSize,
                    duration: uploadSeconds,
                    extra: "attempt=\(attempt + 1)"
                )
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
                if let shotDate = prepared.shotDate, client.shouldSetModificationDate() {
                    let setDateStart = CFAbsoluteTimeGetCurrent()
                    do {
                        try await client.setModificationDate(shotDate, forPath: uploadPreparation.remoteAbsolutePath)
                    } catch {
                        // keep upload success even if metadata write failed
                    }
                    let setDateSeconds = Self.elapsedSeconds(since: setDateStart)
                    assetTiming.setModificationDateSeconds += setDateSeconds
                    Self.traceOneDriveUploadStage(
                        profile: profile,
                        stage: "setModificationDate",
                        displayName: displayName,
                        resourceName: local.originalFilename,
                        remotePath: uploadPreparation.remoteAbsolutePath,
                        size: prepared.fileSize,
                        duration: setDateSeconds
                    )
                }
                return UploadRetryOutcome(fileName: uploadPreparation.targetFileName, lastError: nil)
            } catch {
                if error is CancellationError {
                    throw error
                }
                if cancellationController?.isCancelled == true {
                    throw CancellationError()
                }
                if Self.isLeaseFailFast(error) {
                    throw error   // lease/ownership loss is not recoverable by retry/sleep/rename
                }
                // Ejected/non-recoverable unavailable fail-fasts; recoverable network faults fall through to retry.
                if profile.isConnectionUnavailableError(error), !Self.isRecoverableNetworkFault(error, profile: profile) {
                    throw error
                }
                lastError = error
                let shouldLimitUploadRetries = client.shouldLimitUploadRetries(for: error)
                let retryLimit = shouldLimitUploadRetries ? min(maxRetry, 2) : maxRetry

                if SMBErrorClassifier.isNameCollision(error) {
                    var collisionKeys = monthStore.existingCollisionKeys()
                    for name in uploadPreparation.attemptedFileNames {
                        collisionKeys.insert(RemoteFileNaming.collisionKey(for: name))
                    }
                    collisionKeys.insert(RemoteFileNaming.collisionKey(for: uploadPreparation.targetFileName))
                    let previousFileName = uploadPreparation.targetFileName
                    uploadPreparation.targetFileName = RemoteFileNaming.resolveNextAvailableName(
                        baseName: uploadPreparation.baseFileName,
                        collisionKeys: collisionKeys,
                        maximumLength: profile.storageProfile.remoteFileNamePolicy.maximumComponentLength
                    )
                    uploadPreparation.attemptedFileNames.insert(uploadPreparation.targetFileName)
                    let retryRelativePath = monthStore.monthRelativePath + "/" + uploadPreparation.targetFileName
                    uploadPreparation.remoteAbsolutePath = RemotePathBuilder.absolutePath(
                        basePath: profile.basePath,
                        remoteRelativePath: retryRelativePath
                    )
                    print("[BackupUpload] collision RETRY: asset=\(displayName), resource=\(local.originalFilename), previous=\(previousFileName), next=\(uploadPreparation.targetFileName)")
                    continue
                }

                guard attempt < retryLimit - 1 else {
                    break
                }
                if shouldLimitUploadRetries {
                    let reason = profile.userFacingStorageErrorMessage(error)
                    print("[BackupUpload] upload watchdog RETRY: asset=\(displayName), resource=\(local.originalFilename), nextAttempt=\(attempt + 2)/\(retryLimit), finalAttempt=true, reason=\(reason)")
                    eventStream.emitLog(
                        String.localizedStringWithFormat(
                            String(localized: "backup.log.uploadStalledRetry"),
                            displayName,
                            local.originalFilename,
                            attempt + 2,
                            retryLimit,
                            reason
                        ),
                        level: .warning
                    )
                }
                let sleepNanos = UInt64(pow(2.0, Double(attempt))) * 500_000_000
                do {
                    try await Task.sleep(nanoseconds: sleepNanos)
                } catch {
                    throw CancellationError()
                }
            }
        }

        // A surviving recoverable network fault propagates so the worker can reconnect and retry the asset.
        if let lastError, Self.isRecoverableNetworkFault(lastError, profile: profile) {
            throw lastError
        }
        return UploadRetryOutcome(fileName: nil, lastError: lastError)
    }

    func recordUploadedResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        targetFileName: String
    ) throws {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize
        let backedUpAtMs = Date().millisecondsSinceEpoch
        let manifestItem = RemoteManifestResource(
            year: monthStore.year,
            month: monthStore.month,
            fileName: targetFileName,
            contentHash: localHash,
            fileSize: localFileSize,
            resourceType: local.resourceTypeCode,
            creationDateMs: LibraryCreationDate.optionalMilliseconds(prepared.shotDate),
            backedUpAtMs: backedUpAtMs
        )
        let inserted = try monthStore.upsertResource(manifestItem)
        monthStore.markRemoteFile(name: targetFileName, size: localFileSize)
        remoteIndexService.upsertCachedResource(inserted, expectedProfileKey: RemoteIndexSyncService.remoteProfileKey(profile))
    }

    func downloadAndHashRemoteFile(
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

    func downloadAndHashRemoteFileForConflictCheck(
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        monthStore: MonthManifestStore,
        fileName: String,
        displayName: String,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> Data? {
        do {
            return try await downloadAndHashRemoteFile(
                profile: profile,
                client: client,
                monthStore: monthStore,
                fileName: fileName,
                cancellationController: cancellationController
            )
        } catch {
            if error is CancellationError {
                throw error
            }
            if cancellationController?.isCancelled == true {
                throw CancellationError()
            }
            // Propagate recoverable network faults (incl. WebDAV transient statuses, which are not
            // isConnectionUnavailableError) so the worker reconnects and re-checks, rather than renaming a
            // file a transient outage merely hid — which would duplicate an identical resource.
            if profile.isConnectionUnavailableError(error) || Self.isRecoverableNetworkFault(error, profile: profile) {
                throw error
            }

            let reason = profile.userFacingStorageErrorMessage(error)
            print("[BackupUpload] remote hash download FALLBACK_RENAME: asset=\(displayName), file=\(fileName), reason=\(reason)")
            eventStream.emitLog(
                String.localizedStringWithFormat(
                    String(localized: "backup.log.remoteHashDownloadFallbackRename"),
                    displayName,
                    fileName,
                    reason
                ),
                level: .warning
            )
            return nil
        }
    }

    static func traceOneDriveUploadStage(
        profile: ServerProfileRecord,
        stage: String,
        displayName: String,
        resourceName: String,
        remotePath: String,
        size: Int64,
        duration: TimeInterval,
        extra: String = ""
    ) {
        guard profile.resolvedStorageType == .onedrive else { return }
        let suffix = extra.isEmpty ? "" : " \(extra)"
        #if DEBUG
        print("[BackupTrace] storage=onedrive stage=\(stage) asset=\(displayName) resource=\(resourceName) size=\(size) durationMs=\(String(format: "%.1f", duration * 1_000)) remotePath=\(remotePath)\(suffix)")
        #endif
    }

    static func makeTransferState(
        kind: BackupTransferKind,
        workerID: Int,
        assetLocalIdentifier: String,
        assetDisplayName: String,
        resourceDate: Date?,
        assetPosition: Int,
        totalAssets: Int,
        resourceDisplayName: String,
        resourcePosition: Int,
        totalResources: Int,
        resourceFraction: Float,
        resourceBytesTransferred: Int64?,
        resourceTotalBytes: Int64?,
        countsTowardTransferSpeed: Bool = true,
        stageDescription: String
    ) -> BackupTransferState {
        BackupTransferState(
            kind: kind,
            workerID: max(1, workerID),
            assetLocalIdentifier: assetLocalIdentifier,
            assetDisplayName: assetDisplayName,
            resourceDate: resourceDate,
            assetPosition: max(1, assetPosition),
            totalAssets: max(totalAssets, 1),
            resourceDisplayName: resourceDisplayName,
            resourcePosition: max(1, resourcePosition),
            totalResources: max(totalResources, 1),
            resourceFraction: min(max(resourceFraction, 0), 1),
            resourceBytesTransferred: resourceBytesTransferred.map { max(0, $0) },
            resourceTotalBytes: resourceTotalBytes.map { max(0, $0) },
            countsTowardTransferSpeed: countsTowardTransferSpeed,
            stageDescription: stageDescription
        )
    }
}

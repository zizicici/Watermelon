import CryptoKit
import Foundation
import Photos

struct UploadPreparation {
    let baseFileName: String
    var targetFileName: String
    var remoteAbsolutePath: String
    var attemptedFileNames: Set<String>
    let skipReason: String?
    let forceWriterIDSuffix: Bool
}

struct UploadRetryOutcome {
    let fileName: String?
    let lastError: Error?
}

extension AssetProcessor {
    func uploadResource(
        prepared: PreparedResource,
        monthStore: any BackupMonthStore,
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
        cancellationController: BackupCancellationController?
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
        assetTiming.collisionCheckSeconds += Self.elapsedSeconds(since: collisionStart)

        if let skipReason = preparation.skipReason {
            let dbStart = CFAbsoluteTimeGetCurrent()
            try recordSkippedResource(
                prepared: prepared,
                monthStore: monthStore,
                targetFileName: preparation.targetFileName
            )
            assetTiming.databaseSeconds += Self.elapsedSeconds(since: dbStart)
            return ResourceUploadResult(status: .skipped, reason: skipReason)
        }

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
            cancellationController: cancellationController
        )

        guard let uploadedFileName = retryOutcome.fileName else {
            let reason = retryOutcome.lastError?.localizedDescription ?? "Unknown upload failure"
            print("[BackupUpload] upload FAILED: asset=\(displayName), resource=\(prepared.local.originalFilename), reason=\(reason)")
            return ResourceUploadResult(
                status: .failed,
                reason: reason
            )
        }

        let dbStart = CFAbsoluteTimeGetCurrent()
        try recordUploadedResource(
            prepared: prepared,
            monthStore: monthStore,
            targetFileName: uploadedFileName
        )
        assetTiming.databaseSeconds += Self.elapsedSeconds(since: dbStart)
        return ResourceUploadResult(status: .success, reason: nil)
    }

    func prepareUpload(
        prepared: PreparedResource,
        monthStore: any BackupMonthStore,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        displayName: String,
        eventStream: BackupEventStream,
        cancellationController: BackupCancellationController?
    ) async throws -> UploadPreparation {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize

        let baseFileName = local.preferredRemoteFileName
        var targetFileName = baseFileName
        var skipReason: String?
        var attemptedFileNames: Set<String> = [targetFileName]

        let existingCollisionKeys = monthStore.existingCollisionKeys()

        // .overwritePossible backends (SMB / S3-multipart) need writerID-suffix to keep peer uploads distinct.
        let baseRemotePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: monthStore.monthRelativePath + "/" + baseFileName
        )
        let baseGuarantee = client.atomicCreateGuarantee(forFileSize: localFileSize, remotePath: baseRemotePath)
        let forceWriterIDSuffix = baseGuarantee == .overwritePossible && monthStore.v2Services?.writerID != nil
        let baseCollides = existingCollisionKeys.contains(RemoteFileNaming.collisionKey(for: baseFileName))

        // Same-name + same-hash = orphan reuse; runs before suffix-rename so crash retries
        // don't bloat remote with ~writerID duplicates. Big collisions skip the dedup.
        let precheckMaxBytes: Int64 = 64 * 1024 * 1024
        // Force-suffix backends: also probe ~wid6 name so crash-retry reuses the bytes.
        var probeCandidate: String? = baseCollides ? baseFileName : nil
        if probeCandidate == nil, forceWriterIDSuffix, let writerID = monthStore.v2Services?.writerID {
            let candidate = RemoteFileNaming.writerIDSuffixedName(baseName: baseFileName, writerID: writerID)
            if existingCollisionKeys.contains(RemoteFileNaming.collisionKey(for: candidate)) {
                probeCandidate = candidate
            }
        }
        if let candidate = probeCandidate, localFileSize <= precheckMaxBytes {
            let existingManifestResource = monthStore.findByFileName(candidate)
            let knownRemoteSize = existingManifestResource?.fileSize ?? monthStore.remoteFileSize(named: candidate)
            let sizesMatchOrUnknown = knownRemoteSize == nil || knownRemoteSize == localFileSize
            if sizesMatchOrUnknown {
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
                let remoteHash = try await downloadAndHashRemoteFileForConflictCheck(
                    profile: profile,
                    client: client,
                    monthStore: monthStore,
                    fileName: candidate,
                    displayName: displayName,
                    eventStream: eventStream,
                    cancellationController: cancellationController
                )
                if let remoteHash, remoteHash == localHash {
                    skipReason = "name_same_hash"
                    targetFileName = candidate
                }
            }
        }

        if skipReason == nil && (forceWriterIDSuffix || baseCollides) {
            targetFileName = RemoteFileNaming.resolveNextAvailableName(
                baseName: baseFileName,
                collisionKeys: existingCollisionKeys,
                writerID: monthStore.v2Services?.writerID,
                forceWriterIDSuffix: forceWriterIDSuffix
            )
            attemptedFileNames.insert(targetFileName)
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
            skipReason: skipReason,
            forceWriterIDSuffix: forceWriterIDSuffix
        )
    }

    func recordSkippedResource(
        prepared: PreparedResource,
        monthStore: any BackupMonthStore,
        targetFileName: String
    ) throws {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize
        let manifestItem = RemoteManifestResource(
            year: monthStore.year,
            month: monthStore.month,
            physicalRemotePath: monthStore.monthRelativePath + "/" + targetFileName,
            contentHash: localHash,
            fileSize: localFileSize,
            resourceType: local.resourceTypeCode,
            creationDateMs: local.asset.creationDate?.millisecondsSinceEpoch,
            backedUpAtMs: Date().millisecondsSinceEpoch
        )

        let inserted = try monthStore.upsertResource(manifestItem)
        optimisticWriter.appendResource(inserted)

        monthStore.markRemoteFile(name: targetFileName, size: localFileSize)
    }

    func performUploadWithRetry(
        prepared: PreparedResource,
        monthStore: any BackupMonthStore,
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
                                stageDescription: String(localized: "backup.transfer.uploadResource")
                            )
                        ))
                    }
                } else {
                    onProgress = nil
                }
                let createResult: AtomicCreateResult
                do {
                    createResult = try await client.atomicCreate(
                        localURL: prepared.tempFileURL,
                        remotePath: uploadPreparation.remoteAbsolutePath,
                        respectTaskCancellation: true,
                        onProgress: onProgress
                    )
                    onProgress?(1.0)
                } catch {
                    assetTiming.uploadBodySeconds += Self.elapsedSeconds(since: uploadBodyStart)
                    throw error
                }
                if case .alreadyExists = createResult {
                    var occupiedNames = monthStore.existingFileNames()
                    occupiedNames.formUnion(uploadPreparation.attemptedFileNames)
                    occupiedNames.insert(uploadPreparation.targetFileName)
                    let previousFileName = uploadPreparation.targetFileName
                    uploadPreparation.targetFileName = RemoteFileNaming.resolveNextAvailableName(
                        baseName: uploadPreparation.baseFileName,
                        occupiedNames: occupiedNames,
                        writerID: monthStore.v2Services?.writerID,
                        forceWriterIDSuffix: uploadPreparation.forceWriterIDSuffix
                    )
                    uploadPreparation.attemptedFileNames.insert(uploadPreparation.targetFileName)
                    let retryRelativePath = monthStore.monthRelativePath + "/" + uploadPreparation.targetFileName
                    uploadPreparation.remoteAbsolutePath = RemotePathBuilder.absolutePath(
                        basePath: profile.basePath,
                        remoteRelativePath: retryRelativePath
                    )
                    assetTiming.uploadBodySeconds += Self.elapsedSeconds(since: uploadBodyStart)
                    print("[BackupUpload] atomicCreate already-exists RETRY: asset=\(displayName), resource=\(local.originalFilename), previous=\(previousFileName), next=\(uploadPreparation.targetFileName)")
                    continue
                }
                if case .bestEffortRetry = createResult {
                    // `~<wid6>` paths aren't run-unique — same-writer concurrent runs can collide; always verify on bestEffortRetry.
                    let raceDetected = await Self.detectRemoteContentRace(
                        client: client,
                        remotePath: uploadPreparation.remoteAbsolutePath,
                        expectedSize: prepared.fileSize,
                        expectedHash: prepared.contentHash,
                        cancellationController: cancellationController
                    )
                    if raceDetected {
                        var occupiedNames = monthStore.existingFileNames()
                        occupiedNames.formUnion(uploadPreparation.attemptedFileNames)
                        occupiedNames.insert(uploadPreparation.targetFileName)
                        let previousFileName = uploadPreparation.targetFileName
                        uploadPreparation.targetFileName = RemoteFileNaming.resolveNextAvailableName(
                            baseName: uploadPreparation.baseFileName,
                            occupiedNames: occupiedNames,
                            writerID: monthStore.v2Services?.writerID,
                            forceWriterIDSuffix: uploadPreparation.forceWriterIDSuffix
                        )
                        uploadPreparation.attemptedFileNames.insert(uploadPreparation.targetFileName)
                        let retryRelativePath = monthStore.monthRelativePath + "/" + uploadPreparation.targetFileName
                        uploadPreparation.remoteAbsolutePath = RemotePathBuilder.absolutePath(
                            basePath: profile.basePath,
                            remoteRelativePath: retryRelativePath
                        )
                        assetTiming.uploadBodySeconds += Self.elapsedSeconds(since: uploadBodyStart)
                        print("[BackupUpload] bestEffort race RETRY: asset=\(displayName), resource=\(local.originalFilename), previous=\(previousFileName), next=\(uploadPreparation.targetFileName)")
                        continue
                    }
                }
                assetTiming.uploadBodySeconds += Self.elapsedSeconds(since: uploadBodyStart)
                try cancellationController?.throwIfCancelled()
                try Task.checkCancellation()
                if let shotDate = prepared.shotDate {
                    let setDateStart = CFAbsoluteTimeGetCurrent()
                    if client.shouldSetModificationDate() {
                        do {
                            try await client.setModificationDate(shotDate, forPath: uploadPreparation.remoteAbsolutePath)
                        } catch {
                            // keep upload success even if metadata write failed
                        }
                    }
                    assetTiming.setModificationDateSeconds += Self.elapsedSeconds(since: setDateStart)
                }
                return UploadRetryOutcome(fileName: uploadPreparation.targetFileName, lastError: nil)
            } catch {
                if error is CancellationError {
                    throw error
                }
                if cancellationController?.isCancelled == true {
                    throw CancellationError()
                }
                if profile.isConnectionUnavailableError(error) {
                    throw error
                }
                lastError = error
                let shouldLimitUploadRetries = client.shouldLimitUploadRetries(for: error)
                let retryLimit = shouldLimitUploadRetries ? min(maxRetry, 2) : maxRetry

                if SMBErrorClassifier.isNameCollision(error) {
                    var occupiedNames = monthStore.existingFileNames()
                    occupiedNames.formUnion(uploadPreparation.attemptedFileNames)
                    occupiedNames.insert(uploadPreparation.targetFileName)
                    let previousFileName = uploadPreparation.targetFileName
                    uploadPreparation.targetFileName = RemoteFileNaming.resolveNextAvailableName(
                        baseName: uploadPreparation.baseFileName,
                        occupiedNames: occupiedNames,
                        writerID: monthStore.v2Services?.writerID,
                        forceWriterIDSuffix: uploadPreparation.forceWriterIDSuffix
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

        // All retries used by collision-rename → lastError stays nil, surface a real reason.
        if lastError == nil {
            lastError = NSError(
                domain: "AssetProcessor.Upload",
                code: -120,
                userInfo: [NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                    String(localized: "backup.upload.error.collisionExhausted"),
                    maxRetry
                )]
            )
        }
        return UploadRetryOutcome(fileName: nil, lastError: lastError)
    }

    func recordUploadedResource(
        prepared: PreparedResource,
        monthStore: any BackupMonthStore,
        targetFileName: String
    ) throws {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize
        let backedUpAtMs = Date().millisecondsSinceEpoch
        let manifestItem = RemoteManifestResource(
            year: monthStore.year,
            month: monthStore.month,
            physicalRemotePath: monthStore.monthRelativePath + "/" + targetFileName,
            contentHash: localHash,
            fileSize: localFileSize,
            resourceType: local.resourceTypeCode,
            creationDateMs: local.asset.creationDate?.millisecondsSinceEpoch,
            backedUpAtMs: backedUpAtMs
        )
        let inserted = try monthStore.upsertResource(manifestItem)
        monthStore.markRemoteFile(name: targetFileName, size: localFileSize)
        optimisticWriter.appendResource(inserted)
    }

    func downloadAndHashRemoteFile(
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        monthStore: any BackupMonthStore,
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
        monthStore: any BackupMonthStore,
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
            if profile.isConnectionUnavailableError(error) {
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

    /// Fail-closed: unverifiable remote (size match, metadata error, missing) reports race rather than binding our hash to peer bytes.
    static func detectRemoteContentRace(
        client: RemoteStorageClientProtocol,
        remotePath: String,
        expectedSize: Int64,
        expectedHash: Data,
        cancellationController: BackupCancellationController?
    ) async -> Bool {
        let entry: RemoteStorageEntry?
        do {
            entry = try await client.metadata(path: remotePath)
        } catch {
            return true
        }
        guard let entry, !entry.isDirectory else { return true }
        if entry.size != expectedSize { return true }
        // Same size — could still be different content. Hash to confirm.
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("upload-verify-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        do {
            try await client.download(remotePath: remotePath, localURL: tempURL)
            let actualHash = try Self.contentHash(of: tempURL, cancellationController: cancellationController)
            return actualHash != expectedHash
        } catch {
            return true
        }
    }

    static func makeTransferState(
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
        stageDescription: String
    ) -> BackupTransferState {
        BackupTransferState(
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
            stageDescription: stageDescription
        )
    }
}

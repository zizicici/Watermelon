import CryptoKit
import Foundation
import Photos

struct UploadPreparation {
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
        cancellationController: BackupCancellationController?
    ) async throws -> ResourceUploadResult {
        let localHash = prepared.contentHash

        if monthStore.findResourceByHash(localHash) != nil {
            return ResourceUploadResult(status: .skipped, reason: "hash_exists")
        }

        let collisionStart = CFAbsoluteTimeGetCurrent()
        var preparation = try await prepareUpload(
            prepared: prepared,
            monthStore: monthStore,
            profile: profile,
            client: client,
            cancellationController: cancellationController
        )
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
            return ResourceUploadResult(
                status: .failed,
                reason: retryOutcome.lastError?.localizedDescription ?? "Unknown upload failure"
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
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        client: RemoteStorageClientProtocol,
        cancellationController: BackupCancellationController?
    ) async throws -> UploadPreparation {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize

        var targetFileName = local.preferredRemoteFileName
        var skipReason: String?
        var attemptedFileNames: Set<String> = [targetFileName]

        if monthStore.existingFileNames().contains(targetFileName) {
            let existingManifestResource = monthStore.findByFileName(targetFileName)
            let knownRemoteSize = existingManifestResource?.fileSize ?? monthStore.remoteFileSize(named: targetFileName)
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
            }
            if skipReason == nil {
                targetFileName = Self.resolveNextAvailableName(
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

    func recordSkippedResource(
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
            creationDateNs: local.asset.creationDate?.nanosecondsSinceEpoch,
            backedUpAtNs: Date().nanosecondsSinceEpoch
        )

        if monthStore.findResourceByHash(localHash) == nil {
            let inserted = try monthStore.upsertResource(manifestItem)
            remoteIndexService.upsertCachedResource(inserted)
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
                                stageDescription: "上传资源"
                            )
                        ))
                    }
                } else {
                    onProgress = nil
                }
                do {
                    try await client.upload(
                        localURL: prepared.tempFileURL,
                        remotePath: uploadPreparation.remoteAbsolutePath,
                        respectTaskCancellation: true,
                        onProgress: onProgress
                    )
                } catch {
                    assetTiming.uploadBodySeconds += Self.elapsedSeconds(since: uploadBodyStart)
                    throw error
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
                if profile.isExternalStorageUnavailableError(error) {
                    throw error
                }
                lastError = error

                let message = error.localizedDescription
                if message.contains("STATUS_OBJECT_NAME_COLLISION") {
                    var occupiedNames = monthStore.existingFileNames()
                    occupiedNames.formUnion(uploadPreparation.attemptedFileNames)
                    occupiedNames.insert(uploadPreparation.targetFileName)
                    uploadPreparation.targetFileName = Self.resolveNextAvailableName(
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

    private static func resolveNextAvailableName(baseName: String, occupiedNames: Set<String>) -> String {
        guard occupiedNames.contains(baseName) else {
            return baseName
        }

        let nsName = baseName as NSString
        let ext = nsName.pathExtension
        let stem = nsName.deletingPathExtension

        let parsed = splitStemAndSuffix(stem)
        let root = parsed.root
        var suffix = max(parsed.suffix ?? 0, 0)

        while true {
            suffix += 1
            let candidateStem = "\(root)_\(suffix)"
            let candidate: String
            if ext.isEmpty {
                candidate = candidateStem
            } else {
                candidate = "\(candidateStem).\(ext)"
            }
            if !occupiedNames.contains(candidate) {
                return candidate
            }
        }
    }

    private static func splitStemAndSuffix(_ stem: String) -> (root: String, suffix: Int?) {
        guard let underscore = stem.lastIndex(of: "_") else {
            return (stem, nil)
        }
        let suffixStart = stem.index(after: underscore)
        guard suffixStart < stem.endIndex else {
            return (stem, nil)
        }
        let suffixPart = String(stem[suffixStart...])
        guard let suffixValue = Int(suffixPart) else {
            return (stem, nil)
        }
        let root = String(stem[..<underscore])
        guard !root.isEmpty else {
            return (stem, nil)
        }
        return (root, suffixValue)
    }

    func recordUploadedResource(
        prepared: PreparedResource,
        monthStore: MonthManifestStore,
        targetFileName: String
    ) throws {
        let local = prepared.local
        let localHash = prepared.contentHash
        let localFileSize = prepared.fileSize
        let backedUpAtNs = Date().nanosecondsSinceEpoch
        let manifestItem = RemoteManifestResource(
            year: monthStore.year,
            month: monthStore.month,
            fileName: targetFileName,
            contentHash: localHash,
            fileSize: localFileSize,
            resourceType: local.resourceTypeCode,
            creationDateNs: local.asset.creationDate?.nanosecondsSinceEpoch,
            backedUpAtNs: backedUpAtNs
        )
        let inserted = try monthStore.upsertResource(manifestItem)
        monthStore.markRemoteFile(name: targetFileName, size: localFileSize)
        remoteIndexService.upsertCachedResource(inserted)
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

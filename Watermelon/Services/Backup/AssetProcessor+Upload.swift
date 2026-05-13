import CryptoKit
import Foundation
import Photos
import os.log

private let uploadLog = Logger(subsystem: "com.zizicici.watermelon", category: "BackupUpload")

struct UploadPreparation {
    let baseFileName: String
    var targetFileName: String
    var remoteAbsolutePath: String
    var attemptedFileNames: Set<String>
    let skipReason: String?
    let forceWriterIDSuffix: Bool
    let retryBaseFileName: String
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
        var retryBaseFileName = baseFileName

        let writerID = monthStore.v2Services?.writerID
        let runID = monthStore.v2Services?.runID
        let forceWriterIDSuffix = client.dataPathOverwriteRisk == .perKey && writerID != nil
        var collisionKeys = monthStore.existingCollisionKeys()
        let baseCollides = collisionKeys.contains(RemoteFileNaming.collisionKey(for: baseFileName))

        var claimCandidates: [(name: String, knownSize: Int64?)] = []
        var claimCandidateKeys = Set<String>()
        func appendClaimCandidate(_ name: String, knownSize: Int64?) {
            let key = RemoteFileNaming.collisionKey(for: name)
            guard !claimCandidateKeys.contains(key) else { return }
            claimCandidateKeys.insert(key)
            claimCandidates.append((name, knownSize))
        }
        if baseCollides {
            let manifestSize = monthStore.findByFileName(baseFileName)?.fileSize
                ?? monthStore.remoteFileSize(named: baseFileName)
            appendClaimCandidate(baseFileName, knownSize: manifestSize)
        }
        if forceWriterIDSuffix, let writerID {
            let candidates: [String]
            if let runID {
                let runCandidate = RemoteFileNaming.writerIDRunIDSuffixedName(baseName: baseFileName, writerID: writerID, runID: runID)
                candidates = [
                    runCandidate,
                    RemoteFileNaming.writerIDSuffixedName(baseName: baseFileName, writerID: writerID)
                ]
            } else {
                candidates = [RemoteFileNaming.writerIDSuffixedName(baseName: baseFileName, writerID: writerID)]
            }
            for candidate in candidates {
                let key = RemoteFileNaming.collisionKey(for: candidate)
                if collisionKeys.contains(key) {
                    let manifestSize = monthStore.findByFileName(candidate)?.fileSize
                        ?? monthStore.remoteFileSize(named: candidate)
                    appendClaimCandidate(candidate, knownSize: manifestSize)
                    continue
                }
                let probePath = RemotePathBuilder.absolutePath(
                    basePath: profile.basePath,
                    remoteRelativePath: monthStore.monthRelativePath + "/" + candidate
                )
                let probe = try await probeRemoteExistence(
                    client: client,
                    profile: profile,
                    path: probePath,
                    cancellationController: cancellationController
                )
                let remoteSize: Int64?
                switch probe {
                case .absent: continue
                case .present(let size): remoteSize = size
                }
                // Block from target selection — different content at this path must not be overwritten.
                collisionKeys.insert(key)
                attemptedFileNames.insert(candidate)
                appendClaimCandidate(candidate, knownSize: remoteSize)
            }
            let writerMarker = "~\(RepoLayout.writerIDShort(writerID))"
            let baseExt = (baseFileName as NSString).pathExtension
            let baseStem = (baseFileName as NSString).deletingPathExtension
            var addedFallbackCandidates = 0
            for existingName in monthStore.existingFileNames().sorted() {
                guard addedFallbackCandidates < 32 else { break }
                let candidateExt = (existingName as NSString).pathExtension
                if !baseExt.isEmpty, candidateExt.caseInsensitiveCompare(baseExt) != .orderedSame {
                    continue
                }
                let candidateStem = (existingName as NSString).deletingPathExtension
                guard let markerRange = candidateStem.range(of: writerMarker) else { continue }
                let prefix = String(candidateStem[..<markerRange.lowerBound])
                guard !prefix.isEmpty,
                      baseStem.hasPrefix(prefix) || prefix.hasPrefix(baseStem) else { continue }
                let manifestSize = monthStore.findByFileName(existingName)?.fileSize
                    ?? monthStore.remoteFileSize(named: existingName)
                appendClaimCandidate(existingName, knownSize: manifestSize)
                addedFallbackCandidates += 1
            }
        }

        for candidate in claimCandidates {
            let sizesMatchOrUnknown = candidate.knownSize == nil || candidate.knownSize == localFileSize
            guard sizesMatchOrUnknown else { continue }
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            let remoteHash = try await downloadAndHashRemoteFileForConflictCheck(
                profile: profile,
                client: client,
                monthStore: monthStore,
                fileName: candidate.name,
                displayName: displayName,
                eventStream: eventStream,
                cancellationController: cancellationController
            )
            if let remoteHash, remoteHash == localHash {
                skipReason = "name_same_hash"
                targetFileName = candidate.name
                break
            }
        }

        if skipReason == nil && (forceWriterIDSuffix || baseCollides) {
            let baseAbsolutePath = RemotePathBuilder.absolutePath(
                basePath: profile.basePath,
                remoteRelativePath: monthStore.monthRelativePath + "/" + baseFileName
            )
            let sizeGuarantee = client.atomicCreateGuarantee(forFileSize: localFileSize, remotePath: baseAbsolutePath)
            if forceWriterIDSuffix, sizeGuarantee == .overwritePossible,
               let writerID, let runID {
                let runCandidate = RemoteFileNaming.writerIDRunIDSuffixedName(baseName: baseFileName, writerID: writerID, runID: runID)
                retryBaseFileName = runCandidate
                if collisionKeys.contains(RemoteFileNaming.collisionKey(for: runCandidate)) {
                    targetFileName = try RemoteFileNaming.resolveNextAvailableNameOrThrow(
                        baseName: runCandidate,
                        collisionKeys: collisionKeys
                    )
                } else {
                    targetFileName = runCandidate
                }
            } else {
                targetFileName = try RemoteFileNaming.resolveNextAvailableNameOrThrow(
                    baseName: baseFileName,
                    collisionKeys: collisionKeys,
                    writerID: writerID,
                    forceWriterIDSuffix: forceWriterIDSuffix
                )
            }
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
            forceWriterIDSuffix: forceWriterIDSuffix,
            retryBaseFileName: retryBaseFileName
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
                    let differs = try await Self.detectRemoteContentRaceOrFallbackToRename(
                        client: client,
                        profile: profile,
                        remotePath: uploadPreparation.remoteAbsolutePath,
                        expectedSize: prepared.fileSize,
                        expectedHash: prepared.contentHash,
                        cancellationController: cancellationController
                    )
                    if differs {
                        let previousFileName = uploadPreparation.targetFileName
                        try advanceUploadPreparationToNextName(
                            &uploadPreparation,
                            monthStore: monthStore,
                            profile: profile
                        )
                        assetTiming.uploadBodySeconds += Self.elapsedSeconds(since: uploadBodyStart)
                        print("[BackupUpload] atomicCreate already-exists RETRY: asset=\(displayName), resource=\(local.originalFilename), previous=\(previousFileName), next=\(uploadPreparation.targetFileName)")
                        continue
                    }
                    print("[BackupUpload] atomicCreate already-exists MATCHED: asset=\(displayName), resource=\(local.originalFilename), file=\(uploadPreparation.targetFileName)")
                }
                if case .bestEffortRetry = createResult {
                    // Ambiguous create must prove bytes before manifest binding.
                    let raceDetected = try await Self.detectRemoteContentRaceOrFallbackToRename(
                        client: client,
                        profile: profile,
                        remotePath: uploadPreparation.remoteAbsolutePath,
                        expectedSize: prepared.fileSize,
                        expectedHash: prepared.contentHash,
                        cancellationController: cancellationController
                    )
                    if raceDetected {
                        let previousFileName = uploadPreparation.targetFileName
                        try advanceUploadPreparationToNextName(
                            &uploadPreparation,
                            monthStore: monthStore,
                            profile: profile
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
                if error is RemoteFileNaming.ResolutionError {
                    throw error
                }
                if profile.isConnectionUnavailableError(error) {
                    throw error
                }
                lastError = error
                let shouldLimitUploadRetries = client.shouldLimitUploadRetries(for: error)
                let retryLimit = shouldLimitUploadRetries ? min(maxRetry, 2) : maxRetry

                if SMBErrorClassifier.isNameCollision(error) {
                    let previousFileName = uploadPreparation.targetFileName
                    try advanceUploadPreparationToNextName(
                        &uploadPreparation,
                        monthStore: monthStore,
                        profile: profile
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

        // Collision-rename burns retries without throwing; lastError stays nil and would surface as "Unknown".
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

    func advanceUploadPreparationToNextName(
        _ uploadPreparation: inout UploadPreparation,
        monthStore: any BackupMonthStore,
        profile: ServerProfileRecord
    ) throws {
        var occupiedNames = monthStore.existingFileNames()
        occupiedNames.formUnion(uploadPreparation.attemptedFileNames)
        occupiedNames.insert(uploadPreparation.targetFileName)
        if uploadPreparation.retryBaseFileName != uploadPreparation.baseFileName {
            uploadPreparation.targetFileName = try RemoteFileNaming.resolveNextAvailableNameOrThrow(
                baseName: uploadPreparation.retryBaseFileName,
                occupiedNames: occupiedNames
            )
        } else {
            uploadPreparation.targetFileName = try RemoteFileNaming.resolveNextAvailableNameOrThrow(
                baseName: uploadPreparation.baseFileName,
                occupiedNames: occupiedNames,
                writerID: monthStore.v2Services?.writerID,
                forceWriterIDSuffix: uploadPreparation.forceWriterIDSuffix
            )
        }
        uploadPreparation.attemptedFileNames.insert(uploadPreparation.targetFileName)
        let retryRelativePath = monthStore.monthRelativePath + "/" + uploadPreparation.targetFileName
        uploadPreparation.remoteAbsolutePath = RemotePathBuilder.absolutePath(
            basePath: profile.basePath,
            remoteRelativePath: retryRelativePath
        )
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

    enum RemoteProbeResult {
        case absent
        case present(size: Int64?)
    }

    /// Unknown probe state reports `.present(size: nil)` so resolveNextAvailableName won't pick the path.
    func probeRemoteExistence(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        path: String,
        cancellationController: BackupCancellationController?
    ) async throws -> RemoteProbeResult {
        do {
            if let meta = try await client.metadata(path: path), !meta.isDirectory {
                return .present(size: meta.size)
            }
            return .absent
        } catch {
            if error is CancellationError { throw error }
            if cancellationController?.isCancelled == true { throw CancellationError() }
            if profile.isConnectionUnavailableError(error) { throw error }
            if isStorageNotFoundError(error) { return .absent }
            if Self.metadataProbeCanRetryAtCreateBoundary(error) { return .absent }
            uploadLog.warning("metadata probe ambiguous for \(path, privacy: .public): \(String(describing: error), privacy: .public)")
            return .present(size: nil)
        }
    }

    private static func metadataProbeCanRetryAtCreateBoundary(_ error: Error) -> Bool {
        for nsError in nsErrorChain(error) {
            if nsError.domain == WebDAVClient.errorDomain {
                switch nsError.code {
                case 408, 429, 500:
                    return true
                default:
                    break
                }
            }
            if nsError.domain == S3ErrorClassifier.errorDomain {
                if let status = nsError.userInfo[S3ErrorClassifier.userInfoStatusCodeKey] as? Int,
                   status == 408 || status == 429 || status == 500 {
                    return true
                }
                if let serverCode = nsError.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String,
                   serverCode == "InternalError" {
                    return true
                }
            }
        }
        return false
    }

    private static func nsErrorChain(_ error: Error) -> [NSError] {
        var pending: [Error] = [error]
        var seen: Set<String> = []
        var result: [NSError] = []
        while let next = pending.popLast() {
            if let storage = next as? RemoteStorageClientError,
               case .underlying(let inner) = storage {
                pending.append(inner)
                continue
            }
            let nsError = next as NSError
            let key = "\(nsError.domain)#\(nsError.code)"
            guard seen.insert(key).inserted else { continue }
            result.append(nsError)
            if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(underlying)
            }
        }
        return result
    }

    private static func detectRemoteContentRaceOrFallbackToRename(
        client: RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        remotePath: String,
        expectedSize: Int64,
        expectedHash: Data,
        cancellationController: BackupCancellationController?
    ) async throws -> Bool {
        do {
            return try await Self.detectRemoteContentRace(
                client: client,
                remotePath: remotePath,
                expectedSize: expectedSize,
                expectedHash: expectedHash,
                cancellationController: cancellationController
            )
        } catch {
            if error is CancellationError || Task.isCancelled || cancellationController?.isCancelled == true {
                throw CancellationError()
            }
            if profile.isConnectionUnavailableError(error) {
                throw error
            }
            return true
        }
    }

    static func detectRemoteContentRace(
        client: RemoteStorageClientProtocol,
        remotePath: String,
        expectedSize: Int64,
        expectedHash: Data,
        cancellationController: BackupCancellationController?
    ) async throws -> Bool {
        try cancellationController?.throwIfCancelled()
        try Task.checkCancellation()
        let entry: RemoteStorageEntry?
        do {
            entry = try await client.metadata(path: remotePath)
        } catch {
            if error is CancellationError || Task.isCancelled || cancellationController?.isCancelled == true {
                throw CancellationError()
            }
            throw error
        }
        guard let entry else { return true }
        guard !entry.isDirectory else { return true }
        if entry.size != expectedSize { return true }
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("upload-verify-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        do {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            try await client.download(remotePath: remotePath, localURL: tempURL)
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            let actualHash = try Self.contentHash(of: tempURL, cancellationController: cancellationController)
            return actualHash != expectedHash
        } catch {
            if error is CancellationError || Task.isCancelled || cancellationController?.isCancelled == true {
                throw CancellationError()
            }
            throw error
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

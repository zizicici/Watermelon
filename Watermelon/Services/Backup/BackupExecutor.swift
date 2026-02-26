import CryptoKit
import Foundation
import Photos

struct BackupExecutionResult {
    let total: Int
    let completed: Int
    let failed: Int
    let skipped: Int
    let paused: Bool
}

final class BackupExecutor {
    private struct RunState {
        var total: Int = 0
        var completed: Int = 0
        var failed: Int = 0
        var skipped: Int = 0
        var paused: Bool = false

        var succeeded: Int {
            max(completed - skipped, 0)
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

    private struct ResourceProcessingResult {
        let status: BackupItemStatus
        let reason: String?
        let displayFileName: String
    }

    private static let monthCalendar = Calendar(identifier: .gregorian)
    private static let smallFileThresholdBytes: Int64 = 5 * 1024 * 1024

    private let photoLibraryService: PhotoLibraryService
    private let contentHashIndexRepository: ContentHashIndexRepository
    private let remoteLibraryScanner: RemoteLibraryScanner

    private let snapshotLock = NSLock()
    private var cachedRemoteSnapshot = RemoteLibrarySnapshot(resources: [])

    init(
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryService,
        manifestSyncService _: ManifestSyncService
    ) {
        self.photoLibraryService = photoLibraryService
        self.contentHashIndexRepository = ContentHashIndexRepository(databaseManager: databaseManager)
        self.remoteLibraryScanner = RemoteLibraryScanner()
    }

    func runBackup(
        profile: ServerProfileRecord,
        password: String,
        appVersion _: String,
        onlyResourceIdentifiers: Set<String>? = nil,
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
            await onLog("Remote index synced. \(snapshot.totalCount) item(s).")
        } catch {
            await onLog("Remote index scan warning: \(error.localizedDescription)")
        }

        let assetsResult = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        let retryMode = onlyResourceIdentifiers != nil
        var retryTargets = onlyResourceIdentifiers ?? Set<String>()

        if retryMode {
            await onLog("Retry mode: \(retryTargets.count) resource(s).")
        } else {
            await onLog("Start backup (oldest month first).")
        }

        var state = RunState()
        var activeMonth: MonthKey?
        var activeStore: MonthManifestStore?

        outerLoop: for index in 0 ..< assetsResult.count {
            if retryMode, retryTargets.isEmpty {
                break
            }

            if Task.isCancelled {
                state.paused = true
                break
            }

            let asset = assetsResult.object(at: index)
            let monthKey = Self.monthKey(for: asset.creationDate)
            let resources = PHAssetResource.assetResources(for: asset)

            if resources.isEmpty {
                continue
            }

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

            for (resourceIndex, resource) in resources.enumerated() {
                if retryMode, retryTargets.isEmpty {
                    break outerLoop
                }

                if Task.isCancelled {
                    state.paused = true
                    break outerLoop
                }

                let local = makeLocalResource(asset: asset, resource: resource, resourceIndex: resourceIndex)
                if retryMode, !retryTargets.contains(local.resourceLocalIdentifier) {
                    continue
                }

                do {
                    let processResult = try await processResource(
                        local: local,
                        monthStore: monthStore,
                        profile: profile,
                        smbClient: smbClient
                    )

                    state.total += 1
                    state.completed += 1
                    switch processResult.status {
                    case .success:
                        break
                    case .failed:
                        state.failed += 1
                    case .skipped:
                        state.skipped += 1
                    }

                    await onProgress(
                        progressSnapshot(
                            state: state,
                            message: message(for: processResult, resourceName: local.originalFilename),
                            itemEvent: Self.event(
                                for: local,
                                status: processResult.status,
                                reason: processResult.reason,
                                displayFileName: processResult.displayFileName
                            )
                        )
                    )
                } catch {
                    if error is CancellationError {
                        state.paused = true
                        break outerLoop
                    }

                    state.total += 1
                    state.completed += 1
                    state.failed += 1

                    await onLog("Failed: \(local.originalFilename) - \(error.localizedDescription)")
                    await onProgress(
                        progressSnapshot(
                            state: state,
                            message: "Failed \(local.originalFilename)",
                            itemEvent: Self.event(
                                for: local,
                                status: .failed,
                                reason: error.localizedDescription,
                                displayFileName: local.originalFilename
                            )
                        )
                    )
                }

                if retryMode {
                    retryTargets.remove(local.resourceLocalIdentifier)
                }
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
            completed: state.completed,
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
            await onLog("Remote index reloaded. \(snapshot.totalCount) item(s).")
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

    private func processResource(
        local: LocalPhotoResource,
        monthStore: MonthManifestStore,
        profile: ServerProfileRecord,
        smbClient: SMBClientProtocol
    ) async throws -> ResourceProcessingResult {
        var exportedTempURL: URL?
        defer {
            if let exportedTempURL {
                try? FileManager.default.removeItem(at: exportedTempURL)
            }
        }

        let tempFileURL = try await photoLibraryService.exportResourceToTempFile(local.resource)
        exportedTempURL = tempFileURL

        let localHash = try Self.contentHash(of: tempFileURL)
        let localFileSize = max(
            local.fileSize,
            (try? FileManager.default.attributesOfItem(atPath: tempFileURL.path)[.size] as? NSNumber)?.int64Value ?? 0
        )

        if monthStore.containsHash(localHash) {
            try contentHashIndexRepository.upsert(
                assetLocalIdentifier: local.assetLocalIdentifier,
                resourceLocalIdentifier: local.resourceLocalIdentifier,
                contentHash: localHash
            )
            return ResourceProcessingResult(status: .skipped, reason: "hash_exists", displayFileName: local.originalFilename)
        }

        var targetFileName = RemotePathBuilder.sanitizeFilename(local.originalFilename)
        var skipReason: String?

        if monthStore.existingFileNames().contains(targetFileName) {
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
                let remoteSize = monthStore.remoteEntry(named: targetFileName)?.size
                    ?? monthStore.findByFileName(targetFileName)?.fileSize
                if remoteSize == localFileSize {
                    skipReason = "name_same_size"
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
            try monthStore.upsertItem(manifestItem)

            try contentHashIndexRepository.upsert(
                assetLocalIdentifier: local.assetLocalIdentifier,
                resourceLocalIdentifier: local.resourceLocalIdentifier,
                contentHash: localHash
            )

            return ResourceProcessingResult(status: .skipped, reason: skipReason, displayFileName: targetFileName)
        }

        let remoteRelativePath = monthStore.monthRelativePath + "/" + targetFileName
        let remoteAbsolutePath = RemotePathBuilder.absolutePath(basePath: profile.basePath, remoteRelativePath: remoteRelativePath)

        let maxRetry = 3
        var lastError: Error?

        for attempt in 0 ..< maxRetry {
            do {
                try Task.checkCancellation()
                try await smbClient.upload(
                    localURL: tempFileURL,
                    remotePath: remoteAbsolutePath,
                    respectTaskCancellation: true
                )

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
                try monthStore.upsertItem(manifestItem)
                monthStore.markRemoteFile(name: targetFileName, size: localFileSize, creationDate: local.asset.creationDate)

                try contentHashIndexRepository.upsert(
                    assetLocalIdentifier: local.assetLocalIdentifier,
                    resourceLocalIdentifier: local.resourceLocalIdentifier,
                    contentHash: localHash
                )

                return ResourceProcessingResult(status: .success, reason: nil, displayFileName: targetFileName)
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
                    continue
                }

                if attempt < maxRetry - 1 {
                    let sleepNanos = UInt64(pow(2.0, Double(attempt))) * 500_000_000
                    try? await Task.sleep(nanoseconds: sleepNanos)
                    continue
                }
            }
        }

        throw lastError ?? NSError(
            domain: "BackupExecutor",
            code: -1,
            userInfo: [NSLocalizedDescriptionKey: "Unknown upload failure"]
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

    private func makeLocalResource(asset: PHAsset, resource: PHAssetResource, resourceIndex: Int) -> LocalPhotoResource {
        LocalPhotoResource(
            asset: asset,
            resource: resource,
            assetLocalIdentifier: asset.localIdentifier,
            resourceLocalIdentifier: "\(asset.localIdentifier)::\(resourceIndex)::\(resource.type.rawValue)",
            resourceType: PhotoLibraryService.resourceTypeName(resource.type),
            resourceTypeCode: PhotoLibraryService.resourceTypeCode(resource.type),
            uti: resource.uniformTypeIdentifier,
            originalFilename: resource.originalFilename,
            fileSize: PhotoLibraryService.resourceFileSize(resource),
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

    private func message(for result: ResourceProcessingResult, resourceName: String) -> String {
        switch result.status {
        case .success:
            return "Uploaded \(resourceName)"
        case .failed:
            return "Failed \(resourceName)"
        case .skipped:
            if let reason = result.reason {
                return "Skipped \(resourceName) (\(reason))"
            }
            return "Skipped \(resourceName)"
        }
    }

    private static func event(
        for local: LocalPhotoResource,
        status: BackupItemStatus,
        reason: String? = nil,
        displayFileName: String? = nil
    ) -> BackupItemEvent {
        BackupItemEvent(
            assetLocalIdentifier: local.assetLocalIdentifier,
            resourceLocalIdentifier: local.resourceLocalIdentifier,
            originalFilename: displayFileName ?? local.originalFilename,
            status: status,
            reason: reason,
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

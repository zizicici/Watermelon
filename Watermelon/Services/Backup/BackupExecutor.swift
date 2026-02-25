import CryptoKit
import Foundation
import GRDB
import Photos

struct BackupExecutionResult {
    let total: Int
    let completed: Int
    let failed: Int
    let skipped: Int
    let paused: Bool
}

final class BackupExecutor {
    private static let maxPlannedItemsPerMonth = 300
    private static let manifestPushInterval = 10
    private static let monthCalendar = Calendar(identifier: .gregorian)

    private let databaseManager: DatabaseManager
    private let photoLibraryService: PhotoLibraryService
    private let manifestSyncService: ManifestSyncService

    init(
        databaseManager: DatabaseManager,
        photoLibraryService: PhotoLibraryService,
        manifestSyncService: ManifestSyncService
    ) {
        self.databaseManager = databaseManager
        self.photoLibraryService = photoLibraryService
        self.manifestSyncService = manifestSyncService
    }

    func runBackup(
        profile: ServerProfileRecord,
        password: String,
        appVersion: String,
        onProgress: @escaping @MainActor (BackupProgress) -> Void,
        onLog: @escaping @MainActor (String) -> Void
    ) async throws -> BackupExecutionResult {
        let status = photoLibraryService.authorizationStatus()
        if status != .authorized && status != .limited {
            let requested = await photoLibraryService.requestAuthorization()
            guard requested == .authorized || requested == .limited else {
                throw BackupError.photoPermissionDenied
            }
        }

        let smbClient = try AMSMB2Client(config: SMBServerConfig(
            host: profile.host,
            port: profile.port,
            shareName: profile.shareName,
            basePath: profile.basePath,
            username: profile.username,
            password: password,
            domain: profile.domain
        ))

        try await smbClient.connect()
        defer {
            Task {
                await smbClient.disconnect()
            }
        }

        try await smbClient.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        let manifestRefreshResult = try await manifestSyncService.refreshFromRemote(
            client: smbClient,
            basePath: profile.basePath,
            clearLocalWhenMissing: true
        )
        switch manifestRefreshResult {
        case .pulled:
            await onLog("Remote manifest synced.")
        case .remoteMissingClearedLocal:
            await onLog("Remote manifest missing and remote base is empty. Local index cache has been reset.")
        case .remoteMissingKeptLocal:
            await onLog("Remote manifest missing. Will recreate manifest from local index when backup finishes.")
        }

        let existingState = try databaseManager.read { db -> ([String: String], [String: (hash: String, sourceSignature: String)], Set<String>) in
            var hashToPath: [String: String] = [:]
            var cachedHashByResourceKey: [String: (hash: String, sourceSignature: String)] = [:]
            var occupiedRemotePaths = Set<String>()

            let rows = try Row.fetchAll(
                db,
                sql: """
                SELECT assetLocalIdentifier, resourceLocalIdentifier, fingerprint, sourceSignature, remoteRelativePath
                FROM resources
                """
            )
            for row in rows {
                let assetID: String = row["assetLocalIdentifier"]
                let resourceID: String = row["resourceLocalIdentifier"]
                let fingerprint: String = row["fingerprint"]
                let sourceSignature: String? = row["sourceSignature"]
                let storedRemotePath: String = row["remoteRelativePath"]
                let relativeRemotePath = RemotePathBuilder.storedPathToRelative(
                    basePath: profile.basePath,
                    storedPath: storedRemotePath
                )

                if !fingerprint.isEmpty {
                    if hashToPath[fingerprint] == nil {
                        hashToPath[fingerprint] = relativeRemotePath
                    }
                    occupiedRemotePaths.insert(relativeRemotePath)
                }

                if let sourceSignature, !sourceSignature.isEmpty, !fingerprint.isEmpty {
                    cachedHashByResourceKey[Self.resourceCacheKey(assetID: assetID, resourceID: resourceID)] = (fingerprint, sourceSignature)
                }
            }

            return (hashToPath, cachedHashByResourceKey, occupiedRemotePaths)
        }

        let assetsResult = photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        var plannedCount = 0
        var totalResourceCount = 0


        var job = BackupJobRecord(
            id: nil,
            serverProfileID: profile.id ?? 0,
            status: .running,
            totalCount: 0,
            completedCount: 0,
            startedAt: Date(),
            finishedAt: nil,
            lastError: nil
        )

        let jobID: Int64 = try databaseManager.write { db in
            try job.insert(db)
            return job.id ?? 0
        }

        await onLog("Start scanning (oldest month first), per-month cap: \(Self.maxPlannedItemsPerMonth).")

        var completed = 0
        var failed = 0
        var skipped = 0
        var completedSinceLastManifestPush = 0
        var preparedDirectories = Set<String>()
        var paused = false
        var plannedCountByMonth: [String: Int] = [:]
        var monthLimitNoticeSent = Set<String>()

        var knownHashToRemotePath = existingState.0
        var cachedHashByResourceKey = existingState.1
        var executionOccupiedPaths = existingState.2

        outerLoop: for index in 0..<assetsResult.count {
            let asset = assetsResult.object(at: index)
            let resources = PHAssetResource.assetResources(for: asset)

            for (resourceIndex, resource) in resources.enumerated() {
                totalResourceCount += 1
                if totalResourceCount % 40 == 0 {
                    await Task.yield()
                }

                do {
                    try Task.checkCancellation()
                } catch {
                    paused = true
                    break outerLoop
                }

                let local = makeLocalResource(asset: asset, resource: resource, resourceIndex: resourceIndex)
                let monthKey = Self.monthKey(for: asset.creationDate)
                if plannedCountByMonth[monthKey, default: 0] >= Self.maxPlannedItemsPerMonth {
                    if monthLimitNoticeSent.insert(monthKey).inserted {
                        await onLog("Month \(monthKey) reached cap \(Self.maxPlannedItemsPerMonth), remaining items defer to next run.")
                    }
                    continue
                }

                let resourceKey = Self.resourceCacheKey(
                    assetID: local.assetLocalIdentifier,
                    resourceID: local.resourceLocalIdentifier
                )
                let sourceSignature = Self.makeSourceSignature(for: local)

                if let cached = cachedHashByResourceKey[resourceKey],
                   cached.sourceSignature == sourceSignature,
                   knownHashToRemotePath[cached.hash] != nil {
                    skipped += 1
                    continue
                }

                plannedCount += 1
                plannedCountByMonth[monthKey, default: 0] += 1

                var exportedTempURL: URL?
                defer {
                    if let exportedTempURL {
                        try? FileManager.default.removeItem(at: exportedTempURL)
                    }
                }

                var contentHash: String
                if let cached = cachedHashByResourceKey[resourceKey],
                   cached.sourceSignature == sourceSignature {
                    contentHash = cached.hash
                } else {
                    do {
                        try Task.checkCancellation()
                        let tempFileURL = try await photoLibraryService.exportResourceToTempFile(local.resource)
                        exportedTempURL = tempFileURL
                        contentHash = try Self.contentHashHex(of: tempFileURL)
                        cachedHashByResourceKey[resourceKey] = (contentHash, sourceSignature)
                    } catch {
                        if error is CancellationError {
                            paused = true
                            break outerLoop
                        }
                        failed += 1
                        await onLog("Failed: \(local.originalFilename) - \(error.localizedDescription)")
                        try? databaseManager.write { db in
                            let jobItem = BackupJobItemRecord(
                                id: nil,
                                jobID: jobID,
                                assetLocalIdentifier: local.assetLocalIdentifier,
                                resourceLocalIdentifier: local.resourceLocalIdentifier,
                                fingerprint: "",
                                status: .failed,
                                retryCount: 0,
                                errorMessage: error.localizedDescription,
                                updatedAt: Date()
                            )
                            try Self.upsertJobItem(jobItem, db: db)
                        }
                        continue
                    }
                }

                if let existingRemoteRelativePath = knownHashToRemotePath[contentHash] {
                    do {
                        try databaseManager.write { db in
                            let localAsset = local.asset
                            let assetRecord = BackupAssetRecord(
                                id: nil,
                                localIdentifier: localAsset.localIdentifier,
                                mediaType: PhotoLibraryService.mediaTypeName(for: localAsset),
                                creationDate: localAsset.creationDate,
                                modificationDate: localAsset.modificationDate,
                                locationJSON: PhotoLibraryService.locationJSON(for: localAsset),
                                pixelWidth: localAsset.pixelWidth,
                                pixelHeight: localAsset.pixelHeight,
                                duration: localAsset.duration,
                                isLivePhoto: PhotoLibraryService.isLivePhoto(localAsset),
                                lastSeenAt: Date()
                            )

                            try Self.upsertAsset(assetRecord, db: db)

                            let resourceRecord = BackupResourceRecord(
                                id: nil,
                                assetLocalIdentifier: local.assetLocalIdentifier,
                                resourceLocalIdentifier: local.resourceLocalIdentifier,
                                resourceType: local.resourceType,
                                uti: local.uti,
                                originalFilename: local.originalFilename,
                                fileSize: local.fileSize,
                                fingerprint: contentHash,
                                sourceSignature: sourceSignature,
                                remoteRelativePath: existingRemoteRelativePath,
                                backedUpAt: Date(),
                                checksum: contentHash
                            )
                            try Self.upsertResource(resourceRecord, db: db)

                            let jobItem = BackupJobItemRecord(
                                id: nil,
                                jobID: jobID,
                                assetLocalIdentifier: local.assetLocalIdentifier,
                                resourceLocalIdentifier: local.resourceLocalIdentifier,
                                fingerprint: contentHash,
                                status: .skipped,
                                retryCount: 0,
                                errorMessage: nil,
                                updatedAt: Date()
                            )
                            try Self.upsertJobItem(jobItem, db: db)
                        }

                        completed += 1
                        skipped += 1
                        completedSinceLastManifestPush += 1
                        await onProgress(BackupProgress(
                            completed: completed,
                            total: plannedCount,
                            message: "Skipped duplicate \(local.originalFilename)"
                        ))

                        if completedSinceLastManifestPush >= Self.manifestPushInterval {
                            do {
                                try await manifestSyncService.pushLocalManifest(client: smbClient, basePath: profile.basePath, appVersion: appVersion)
                                completedSinceLastManifestPush = 0
                                await onLog("Remote manifest synced after \(completed) updates.")
                            } catch {
                                await onLog("Manifest sync warning: \(error.localizedDescription)")
                            }
                        }
                    } catch {
                        failed += 1
                        await onLog("Failed: \(local.originalFilename) - \(error.localizedDescription)")
                    }

                    try databaseManager.write { db in
                        try db.execute(
                            sql: "UPDATE backup_jobs SET totalCount = ?, completedCount = ?, lastError = ? WHERE id = ?",
                            arguments: [plannedCount, completed, nil, jobID]
                        )
                    }
                    continue
                }

                let remoteRelativePath = makeUniqueRemotePath(
                    localResource: local,
                    occupiedPaths: &executionOccupiedPaths
                )

                let maxRetry = 3
                var success = false
                var lastError: Error?

                for attempt in 0..<maxRetry {
                    do {
                        try Task.checkCancellation()
                        if exportedTempURL == nil {
                            exportedTempURL = try await photoLibraryService.exportResourceToTempFile(local.resource)
                        }
                        guard let tempFileURL = exportedTempURL else {
                            throw NSError(domain: "BackupExecutor", code: -1, userInfo: [NSLocalizedDescriptionKey: "Failed to prepare local temp file."])
                        }

                        try Task.checkCancellation()
                        let remoteAbsolutePath = RemotePathBuilder.absolutePath(
                            basePath: profile.basePath,
                            remoteRelativePath: remoteRelativePath
                        )
                        let directory = RemotePathBuilder.directory(of: remoteAbsolutePath)
                        if !preparedDirectories.contains(directory) {
                            try await smbClient.createDirectory(path: directory)
                            preparedDirectories.insert(directory)
                        }

                        try Task.checkCancellation()
                        try await smbClient.upload(
                            localURL: tempFileURL,
                            remotePath: remoteAbsolutePath,
                            respectTaskCancellation: true
                        )

                        try Task.checkCancellation()
                        try databaseManager.write { db in
                            let localAsset = local.asset
                            let assetRecord = BackupAssetRecord(
                                id: nil,
                                localIdentifier: localAsset.localIdentifier,
                                mediaType: PhotoLibraryService.mediaTypeName(for: localAsset),
                                creationDate: localAsset.creationDate,
                                modificationDate: localAsset.modificationDate,
                                locationJSON: PhotoLibraryService.locationJSON(for: localAsset),
                                pixelWidth: localAsset.pixelWidth,
                                pixelHeight: localAsset.pixelHeight,
                                duration: localAsset.duration,
                                isLivePhoto: PhotoLibraryService.isLivePhoto(localAsset),
                                lastSeenAt: Date()
                            )

                            try Self.upsertAsset(assetRecord, db: db)

                            let resolvedFileSize = max(
                                local.fileSize,
                                (try? FileManager.default.attributesOfItem(atPath: tempFileURL.path)[.size] as? NSNumber)?.int64Value ?? 0
                            )
                            let resourceRecord = BackupResourceRecord(
                                id: nil,
                                assetLocalIdentifier: local.assetLocalIdentifier,
                                resourceLocalIdentifier: local.resourceLocalIdentifier,
                                resourceType: local.resourceType,
                                uti: local.uti,
                                originalFilename: local.originalFilename,
                                fileSize: resolvedFileSize,
                                fingerprint: contentHash,
                                sourceSignature: sourceSignature,
                                remoteRelativePath: remoteRelativePath,
                                backedUpAt: Date(),
                                checksum: contentHash
                            )

                            try Self.upsertResource(resourceRecord, db: db)

                            let jobItem = BackupJobItemRecord(
                                id: nil,
                                jobID: jobID,
                                assetLocalIdentifier: local.assetLocalIdentifier,
                                resourceLocalIdentifier: local.resourceLocalIdentifier,
                                fingerprint: contentHash,
                                status: .success,
                                retryCount: attempt,
                                errorMessage: nil,
                                updatedAt: Date()
                            )
                            try Self.upsertJobItem(jobItem, db: db)
                        }

                        knownHashToRemotePath[contentHash] = remoteRelativePath

                        completed += 1
                        completedSinceLastManifestPush += 1
                        await onProgress(BackupProgress(
                            completed: completed,
                            total: plannedCount,
                            message: "Uploaded \(local.originalFilename)"
                        ))

                        if completedSinceLastManifestPush >= Self.manifestPushInterval {
                            do {
                                try await manifestSyncService.pushLocalManifest(client: smbClient, basePath: profile.basePath, appVersion: appVersion)
                                completedSinceLastManifestPush = 0
                                await onLog("Remote manifest synced after \(completed) uploads.")
                            } catch {
                                await onLog("Manifest sync warning: \(error.localizedDescription)")
                            }
                        }
                        success = true
                        break
                    } catch {
                        if error is CancellationError {
                            paused = true
                            break
                        }
                        lastError = error
                        if attempt < maxRetry - 1 {
                            let sleepNanos = UInt64(pow(2.0, Double(attempt))) * 1_000_000_000
                            try? await Task.sleep(nanoseconds: sleepNanos)
                        }
                    }
                }

                if paused {
                    break outerLoop
                }

                if !success {
                    failed += 1
                    let message = lastError?.localizedDescription ?? "Unknown error"
                    await onLog("Failed: \(local.originalFilename) - \(message)")

                    try databaseManager.write { db in
                        let jobItem = BackupJobItemRecord(
                            id: nil,
                            jobID: jobID,
                            assetLocalIdentifier: local.assetLocalIdentifier,
                            resourceLocalIdentifier: local.resourceLocalIdentifier,
                            fingerprint: contentHash,
                            status: .failed,
                            retryCount: 3,
                            errorMessage: message,
                            updatedAt: Date()
                        )
                        try Self.upsertJobItem(jobItem, db: db)
                    }
                }

                try databaseManager.write { db in
                    try db.execute(
                        sql: "UPDATE backup_jobs SET totalCount = ?, completedCount = ?, lastError = ? WHERE id = ?",
                        arguments: [plannedCount, completed, lastError?.localizedDescription, jobID]
                    )
                }
            }
        }

        let finalStatus: BackupJobStatus
        if paused {
            finalStatus = .paused
        } else if failed > 0 {
            finalStatus = .failed
        } else {
            finalStatus = .done
        }

        try databaseManager.write { db in
            try db.execute(
                sql: "UPDATE backup_jobs SET status = ?, totalCount = ?, completedCount = ?, finishedAt = ?, lastError = ? WHERE id = ?",
                arguments: [finalStatus.rawValue, plannedCount, completed, Date(), failed > 0 ? "Some files failed." : nil, jobID]
            )
        }

        if completed > 0 || paused || manifestRefreshResult != .pulled || completedSinceLastManifestPush > 0 {
            do {
                try await manifestSyncService.pushLocalManifest(client: smbClient, basePath: profile.basePath, appVersion: appVersion)
                await onLog("Remote manifest synced at backup end.")
            } catch {
                await onLog("Manifest sync warning (final): \(error.localizedDescription)")
            }
        }

        let finalMessage: String
        switch finalStatus {
        case .done:
            finalMessage = "Backup complete"
        case .paused:
            finalMessage = "Backup paused"
        case .failed:
            finalMessage = "Backup finished with errors"
        default:
            finalMessage = "Backup stopped"
        }

        await onProgress(BackupProgress(completed: completed, total: plannedCount, message: finalMessage))

        return BackupExecutionResult(
            total: plannedCount,
            completed: completed,
            failed: failed,
            skipped: skipped,
            paused: paused
        )
    }

    func syncManifest(profile: ServerProfileRecord, password: String, appVersion: String) async throws {
        let smbClient = try AMSMB2Client(config: SMBServerConfig(
            host: profile.host,
            port: profile.port,
            shareName: profile.shareName,
            basePath: profile.basePath,
            username: profile.username,
            password: password,
            domain: profile.domain
        ))

        try await smbClient.connect()
        defer {
            Task {
                await smbClient.disconnect()
            }
        }

        try await smbClient.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        try await manifestSyncService.pushLocalManifest(client: smbClient, basePath: profile.basePath, appVersion: appVersion)
    }

    private func makeLocalResource(asset: PHAsset, resource: PHAssetResource, resourceIndex: Int) -> LocalPhotoResource {
        LocalPhotoResource(
            asset: asset,
            resource: resource,
            assetLocalIdentifier: asset.localIdentifier,
            resourceLocalIdentifier: "\(asset.localIdentifier)::\(resourceIndex)::\(resource.type.rawValue)",
            resourceType: PhotoLibraryService.resourceTypeName(resource.type),
            uti: resource.uniformTypeIdentifier,
            originalFilename: resource.originalFilename,
            fileSize: PhotoLibraryService.resourceFileSize(resource),
            resourceModificationDate: asset.modificationDate
        )
    }

    private func makeUniqueRemotePath(
        localResource: LocalPhotoResource,
        occupiedPaths: inout Set<String>
    ) -> String {
        var duplicateIndex = 0
        var remotePath = RemotePathBuilder.buildRelativePath(
            originalFilename: localResource.originalFilename,
            creationDate: localResource.asset.creationDate,
            duplicateIndex: duplicateIndex
        )

        while occupiedPaths.contains(remotePath) {
            duplicateIndex += 1
            remotePath = RemotePathBuilder.buildRelativePath(
                originalFilename: localResource.originalFilename,
                creationDate: localResource.asset.creationDate,
                duplicateIndex: duplicateIndex
            )
        }

        occupiedPaths.insert(remotePath)
        return remotePath
    }

    private static func resourceCacheKey(assetID: String, resourceID: String) -> String {
        "\(assetID)|\(resourceID)"
    }

    private static func makeSourceSignature(for localResource: LocalPhotoResource) -> String {
        let modificationTimeNs = Self.nanosecondsSinceEpoch(localResource.resourceModificationDate) ?? -1
        return [
            localResource.assetLocalIdentifier,
            localResource.resourceLocalIdentifier,
            localResource.resourceType,
            localResource.originalFilename,
            String(localResource.fileSize),
            localResource.uti ?? "",
            String(modificationTimeNs)
        ].joined(separator: "|")
    }

    private static func contentHashHex(of fileURL: URL) throws -> String {
        guard let stream = InputStream(url: fileURL) else {
            throw NSError(domain: "BackupExecutor", code: -2, userInfo: [NSLocalizedDescriptionKey: "Cannot open file stream for hashing."])
        }

        var hasher = SHA256()
        let bufferSize = 64 * 1024
        var buffer = [UInt8](repeating: 0, count: bufferSize)
        stream.open()
        defer { stream.close() }

        while stream.hasBytesAvailable {
            let read = stream.read(&buffer, maxLength: bufferSize)
            if read < 0 {
                throw stream.streamError ?? NSError(domain: "BackupExecutor", code: -3, userInfo: [NSLocalizedDescriptionKey: "Failed to read file data for hashing."])
            }
            if read == 0 {
                break
            }
            hasher.update(data: Data(buffer[0..<read]))
        }

        return hasher.finalize().map { String(format: "%02x", $0) }.joined()
    }

    private static func upsertAsset(_ asset: BackupAssetRecord, db: Database) throws {
        try db.execute(
            sql: """
            INSERT INTO assets (
                localIdentifier, mediaType, creationDate, modificationDate, locationJSON,
                pixelWidth, pixelHeight, duration, isLivePhoto, lastSeenAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(localIdentifier) DO UPDATE SET
                mediaType = excluded.mediaType,
                creationDate = excluded.creationDate,
                modificationDate = excluded.modificationDate,
                locationJSON = excluded.locationJSON,
                pixelWidth = excluded.pixelWidth,
                pixelHeight = excluded.pixelHeight,
                duration = excluded.duration,
                isLivePhoto = excluded.isLivePhoto,
                lastSeenAt = excluded.lastSeenAt
            """,
            arguments: [
                asset.localIdentifier,
                asset.mediaType,
                asset.creationDate,
                asset.modificationDate,
                asset.locationJSON,
                asset.pixelWidth,
                asset.pixelHeight,
                asset.duration,
                asset.isLivePhoto,
                asset.lastSeenAt
            ]
        )
    }

    private static func upsertResource(_ resource: BackupResourceRecord, db: Database) throws {
        try db.execute(
            sql: """
            INSERT INTO resources (
                assetLocalIdentifier, resourceLocalIdentifier, resourceType, uti, originalFilename,
                fileSize, fingerprint, sourceSignature, remoteRelativePath, backedUpAt, checksum
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(assetLocalIdentifier, resourceLocalIdentifier) DO UPDATE SET
                resourceType = excluded.resourceType,
                uti = excluded.uti,
                originalFilename = excluded.originalFilename,
                fileSize = excluded.fileSize,
                fingerprint = excluded.fingerprint,
                sourceSignature = excluded.sourceSignature,
                remoteRelativePath = excluded.remoteRelativePath,
                backedUpAt = excluded.backedUpAt,
                checksum = excluded.checksum
            """,
            arguments: [
                resource.assetLocalIdentifier,
                resource.resourceLocalIdentifier,
                resource.resourceType,
                resource.uti,
                resource.originalFilename,
                resource.fileSize,
                resource.fingerprint,
                resource.sourceSignature,
                resource.remoteRelativePath,
                resource.backedUpAt,
                resource.checksum
            ]
        )
    }

    private static func upsertJobItem(_ item: BackupJobItemRecord, db: Database) throws {
        try db.execute(
            sql: """
            INSERT INTO job_items (
                jobID, assetLocalIdentifier, resourceLocalIdentifier, fingerprint,
                status, retryCount, errorMessage, updatedAt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(jobID, resourceLocalIdentifier) DO UPDATE SET
                status = excluded.status,
                fingerprint = excluded.fingerprint,
                retryCount = excluded.retryCount,
                errorMessage = excluded.errorMessage,
                updatedAt = excluded.updatedAt
            """,
            arguments: [
                item.jobID,
                item.assetLocalIdentifier,
                item.resourceLocalIdentifier,
                item.fingerprint,
                item.status.rawValue,
                item.retryCount,
                item.errorMessage,
                item.updatedAt
            ]
        )
    }

    private static func monthKey(for date: Date?) -> String {
        guard let date else { return "unknown" }
        let comps = monthCalendar.dateComponents([.year, .month], from: date)
        let year = comps.year ?? 0
        let month = comps.month ?? 0
        return String(format: "%04d-%02d", year, month)
    }

    private static func nanosecondsSinceEpoch(_ date: Date?) -> Int64? {
        guard let date else { return nil }
        return Int64((date.timeIntervalSince1970 * 1_000_000_000).rounded())
    }
}

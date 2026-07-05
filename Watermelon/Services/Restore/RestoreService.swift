import Foundation
import Photos

enum RestoreIntegrityError: Error, LocalizedError {
    case contentHashMismatch(fileName: String, expectedHashHex: String, actualHashHex: String)

    var errorDescription: String? {
        switch self {
        case let .contentHashMismatch(fileName, expectedHashHex, actualHashHex):
            return "Downloaded \(fileName) failed integrity check (expected \(expectedHashHex.prefix(8)), got \(actualHashHex.prefix(8)))"
        }
    }
}

final class RestoreService {
    private let storageClientFactory: StorageClientFactory

    init(
        databaseManager _: DatabaseManager,
        storageClientFactory: StorageClientFactory = StorageClientFactory()
    ) {
        self.storageClientFactory = storageClientFactory
    }

    struct RestoreItemDescriptor: Sendable {
        let instances: [RemoteAssetResourceInstance]
        let identity: Data
    }

    struct RestoredAsset {
        let localIdentifier: String
        let importedInstances: [RemoteAssetResourceInstance]
    }

    struct RestoredItem: Sendable {
        let identity: Data
        let asset: RestoredAsset
    }

    func restoreItems(
        items: [RestoreItemDescriptor],
        profile: ServerProfileRecord,
        password: String,
        onTransferState: (@Sendable (BackupTransferState) async -> Void)? = nil,
        onItemCompleted: @Sendable (Int, Int, RestoredItem?) async throws -> Void
    ) async throws -> [RestoredItem] {
        guard !items.isEmpty else { return [] }

        // Ride out a transient connect blip within the recovery window instead of failing restore on one wobble.
        let storageClient: any RemoteStorageClientProtocol
        switch await NetworkRecovery.connectRidingOut(
            deadline: Date().addingTimeInterval(NetworkRecoveryPolicy.foregroundWindow),
            makeClient: { [storageClientFactory] in try storageClientFactory.makeClient(profile: profile, password: password) }
        ) {
        case .succeeded(let client):
            storageClient = client
        case .failed(let error), .exhausted(let error), .stopped(let error):
            throw error
        case .cancelled:
            throw CancellationError()
        }
        // Boxed so a mid-restore reconnect can hot-swap the client for all subsequent downloads.
        let clientBox = RestoreClientBox(storageClient)
        defer {
            Task { [clientBox] in await clientBox.client.disconnect() }
        }

        var results: [RestoredItem] = []
        for (index, item) in items.enumerated() {
            try Task.checkCancellation()
            let creationDate = item.instances
                .compactMap(\.creationDateMs)
                .min()
                .map { Date(millisecondsSinceEpoch: $0) }
            let group = RestoreGroup(creationDate: creationDate, instances: item.instances)
            var restoredItem: RestoredItem?
            if let asset = try await restoreGroup(
                group,
                itemIdentity: item.identity,
                itemPosition: index + 1,
                totalItems: items.count,
                profile: profile,
                password: password,
                clientBox: clientBox,
                onTransferState: onTransferState
            ) {
                let restored = RestoredItem(identity: item.identity, asset: asset)
                results.append(restored)
                restoredItem = restored
            }
            try await onItemCompleted(index + 1, items.count, restoredItem)
        }
        return results
    }

    private func restoreGroup(
        _ group: RestoreGroup,
        itemIdentity: Data,
        itemPosition: Int,
        totalItems: Int,
        profile: ServerProfileRecord,
        password: String,
        clientBox: RestoreClientBox,
        onTransferState: (@Sendable (BackupTransferState) async -> Void)?
    ) async throws -> RestoredAsset? {
        let resourceDesc = group.instances.map { instance in
            let mapped = instance.resourceType
            let typeStr = mapped.map { String($0.rawValue) } ?? "skip"
            return "\(instance.fileName) (role=\(instance.role), slot=\(instance.slot), type=\(typeStr), size=\(instance.fileSize), hash=\(instance.contentHashHex.prefix(8)))"
        }.joined(separator: ", ")
        print("[RestoreService] restoreGroup: \(group.instances.count) instance(s), creationDate=\(group.creationDate?.description ?? "nil") — [\(resourceDesc)]")

        var downloadedURLsByHash: [Data: URL] = [:]
        downloadedURLsByHash.reserveCapacity(group.instances.count)
        var downloaded: [(RemoteAssetResourceInstance, URL)] = []
        downloaded.reserveCapacity(group.instances.count)

        for (resourceIndex, instance) in group.instances.enumerated() {
            try Task.checkCancellation()
            // Content-addressed reuse only with a real hash. A legacy no-hash manifest leaves resourceHash empty;
            // deduping on the empty key would collapse every resource of a multi-resource asset onto the first
            // downloaded file. (verifyDownloadedResource likewise skips the empty-hash case.)
            if !instance.resourceHash.isEmpty, let cachedURL = downloadedURLsByHash[instance.resourceHash] {
                downloaded.append((instance, cachedURL))
                continue
            }

            let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(
                "restore_\(UUID().uuidString)_\(instance.fileName)"
            )
            try? FileManager.default.removeItem(at: tempURL)

            let remotePath = RemotePathBuilder.absolutePath(
                basePath: profile.basePath,
                remoteRelativePath: instance.remoteRelativePath
            )
            try await downloadWithRecovery(
                clientBox: clientBox,
                remotePath: remotePath,
                localURL: tempURL,
                instanceName: instance.fileName,
                itemIdentity: itemIdentity,
                itemDisplayName: group.instances.first?.fileName ?? String(itemIdentity.hexString.prefix(12)),
                itemCreationDate: group.creationDate,
                itemPosition: itemPosition,
                totalItems: totalItems,
                resourcePosition: resourceIndex + 1,
                totalResources: group.instances.count,
                resource: instance,
                profile: profile,
                password: password,
                onTransferState: onTransferState
            )

            let fileSize = (try? FileManager.default.attributesOfItem(atPath: tempURL.path)[.size] as? Int64) ?? -1
            let fileExists = FileManager.default.fileExists(atPath: tempURL.path)
            print("[RestoreService]   downloaded: \(instance.fileName) → \(tempURL.lastPathComponent), exists=\(fileExists), localSize=\(fileSize), expectedSize=\(instance.fileSize)")
            do {
                try Self.verifyDownloadedResource(at: tempURL, instance: instance)
            } catch {
                try? FileManager.default.removeItem(at: tempURL)
                print("[RestoreService]   integrity FAILED: \(instance.fileName), \(error.localizedDescription)")
                throw error
            }
            if !instance.resourceHash.isEmpty { downloadedURLsByHash[instance.resourceHash] = tempURL }
            downloaded.append((instance, tempURL))
        }

        let acceptedDownloaded = Self.acceptedDownloadedResources(from: downloaded)
        let uniqueDownloadedURLs = Set(downloaded.map(\.1))
        do {
            try Task.checkCancellation()
            let localID = try await saveToPhotoLibrary(downloaded: acceptedDownloaded, creationDate: group.creationDate)
            print("[RestoreService]   saveToPhotoLibrary succeeded, localID=\(localID ?? "nil")")

            for url in uniqueDownloadedURLs {
                try? FileManager.default.removeItem(at: url)
            }

            guard let localID else { return nil }
            return RestoredAsset(
                localIdentifier: localID,
                importedInstances: acceptedDownloaded.map(\.0)
            )
        } catch {
            print("[RestoreService]   saveToPhotoLibrary FAILED: \(error)")
            for url in uniqueDownloadedURLs {
                try? FileManager.default.removeItem(at: url)
            }
            throw error
        }
    }

    // Download one resource with the same ride-out as upload: a transient network fault reconnects + retries
    // within a window rather than failing the whole restore; a terminal fault or ejected external volume fails
    // fast; the window elapsing surfaces the last fault.
    private func downloadWithRecovery(
        clientBox: RestoreClientBox,
        remotePath: String,
        localURL: URL,
        instanceName: String,
        itemIdentity: Data,
        itemDisplayName: String,
        itemCreationDate: Date?,
        itemPosition: Int,
        totalItems: Int,
        resourcePosition: Int,
        totalResources: Int,
        resource: RemoteAssetResourceInstance,
        profile: ServerProfileRecord,
        password: String,
        onTransferState: (@Sendable (BackupTransferState) async -> Void)?
    ) async throws {
        let deadline = Date().addingTimeInterval(NetworkRecoveryPolicy.foregroundWindow)
        let transferRelay = onTransferState.map { RestoreTransferProgressRelay(onTransferState: $0) }
        let result: NetworkRecoveryResult<Void> = await NetworkRecovery.run(
            deadline: deadline,
            isRetryable: { AssetProcessor.isRecoverableNetworkFault($0, profile: profile) }
        ) {
            do {
                try await clientBox.client.download(
                    remotePath: remotePath,
                    localURL: localURL,
                    onProgress: makeTransferProgressHandler(
                        itemIdentity: itemIdentity,
                        itemDisplayName: itemDisplayName,
                        itemCreationDate: itemCreationDate,
                        itemPosition: itemPosition,
                        totalItems: totalItems,
                        resourcePosition: resourcePosition,
                        totalResources: totalResources,
                        resource: resource,
                        transferRelay: transferRelay
                    )
                )
                return .succeeded(())
            } catch {
                // Reconnect only for a recoverable fault, before the driver backs off and retries; a terminal
                // fault / ejected volume falls through to fail fast (isRetryable is false for them).
                if AssetProcessor.isRecoverableNetworkFault(error, profile: profile) {
                    try? FileManager.default.removeItem(at: localURL)   // discard any partial file
                    await clientBox.client.disconnectSafely()
                    if let fresh = try? storageClientFactory.makeClient(profile: profile, password: password) {
                        do {
                            // Cap the reconnect at the cumulative download window so it can't overrun by a full connectTimeout.
                            try await NetworkRecovery.boundedConnect(
                                fresh, deadline: min(deadline, Date().addingTimeInterval(NetworkRecoveryPolicy.connectTimeout))
                            )
                            clientBox.client = fresh
                        } catch let reconnectError {
                            await fresh.disconnectSafely()
                            // A terminal reconnect fault (auth/config) is the real cause — surface it instead of
                            // masking it behind the original network error and retrying until the window elapses.
                            if !AssetProcessor.isRecoverableNetworkFault(reconnectError, profile: profile) {
                                return .failed(reconnectError)
                            }
                            // else keep retrying; next pass reconnects again
                        }
                    }
                }
                return .failed(error)
            }
        }
        if let transferRelay {
            await transferRelay.finish()
        }
        switch result {
        case .succeeded, .stopped:   // no shouldStop predicate, so .stopped never occurs
            return
        case .cancelled:
            throw CancellationError()
        case .failed(let error), .exhausted(let error):
            print("[RestoreService]   download FAILED: \(instanceName), remotePath=\(remotePath), reason=\(error.localizedDescription)")
            throw error
        }
    }

    private func makeTransferProgressHandler(
        itemIdentity: Data,
        itemDisplayName: String,
        itemCreationDate: Date?,
        itemPosition: Int,
        totalItems: Int,
        resourcePosition: Int,
        totalResources: Int,
        resource: RemoteAssetResourceInstance,
        transferRelay: RestoreTransferProgressRelay?
    ) -> ((Double) -> Void)? {
        guard let transferRelay else { return nil }
        let totalBytes = resource.fileSize > 0 ? resource.fileSize : nil
        let fractionLock = NSLock()
        var lastEmittedFraction = 0.0
        return { fraction in
            let clamped = min(max(fraction, 0), 1)
            let shouldEmit = fractionLock.withLock {
                if clamped < lastEmittedFraction, clamped < 1 {
                    return false
                }
                lastEmittedFraction = max(lastEmittedFraction, clamped)
                return true
            }
            guard shouldEmit else { return }
            let transferred = totalBytes.map { Int64((Double($0) * clamped).rounded()) }
            let state = BackupTransferState(
                kind: .download,
                workerID: 1,
                assetLocalIdentifier: itemIdentity.hexString,
                assetDisplayName: itemDisplayName,
                resourceDate: itemCreationDate,
                assetPosition: max(1, itemPosition),
                totalAssets: max(1, totalItems),
                resourceDisplayName: resource.fileName,
                resourcePosition: max(1, resourcePosition),
                totalResources: max(1, totalResources),
                resourceFraction: Float(clamped),
                resourceBytesTransferred: transferred,
                resourceTotalBytes: totalBytes,
                countsTowardTransferSpeed: true,
                stageDescription: String(localized: "backup.transfer.downloadResource")
            )
            transferRelay.emit(state)
        }
    }

    private final class RestoreTransferProgressRelay: @unchecked Sendable {
        private let continuation: AsyncStream<BackupTransferState>.Continuation
        private let deliveryTask: Task<Void, Never>

        init(onTransferState: @escaping @Sendable (BackupTransferState) async -> Void) {
            var capturedContinuation: AsyncStream<BackupTransferState>.Continuation?
            let stream = AsyncStream<BackupTransferState>(bufferingPolicy: .bufferingNewest(1)) { continuation in
                capturedContinuation = continuation
            }
            guard let capturedContinuation else {
                preconditionFailure("Restore transfer progress continuation was not initialized.")
            }
            self.continuation = capturedContinuation
            self.deliveryTask = Task {
                for await state in stream {
                    await onTransferState(state)
                }
            }
        }

        func emit(_ state: BackupTransferState) {
            continuation.yield(state)
        }

        func finish() async {
            continuation.finish()
            await deliveryTask.value
        }
    }

    private final class RestoreClientBox {
        var client: any RemoteStorageClientProtocol
        init(_ client: any RemoteStorageClientProtocol) { self.client = client }
    }

    private struct RestoreGroup {
        let creationDate: Date?
        let instances: [RemoteAssetResourceInstance]
    }

    // Manifest resourceHash is SHA-256 of the exact stored bytes; a completed-but-wrong/corrupt download must
    // fail here rather than be imported and recorded in the local hash index as matching the remote.
    static func verifyDownloadedResource(at fileURL: URL, instance: RemoteAssetResourceInstance) throws {
        guard !instance.resourceHash.isEmpty else { return }
        let actualHash = try AssetProcessor.contentHash(of: fileURL)
        guard actualHash == instance.resourceHash else {
            throw RestoreIntegrityError.contentHashMismatch(
                fileName: instance.fileName,
                expectedHashHex: instance.contentHashHex,
                actualHashHex: actualHash.hexString
            )
        }
    }

    static func acceptedDownloadedResources(
        from downloaded: [(RemoteAssetResourceInstance, URL)]
    ) -> [(RemoteAssetResourceInstance, URL)] {
        // Normalize the subset into a PHAssetCreationRequest-valid set first (promotes a missing primary; complete
        // records pass through unchanged), then map each planned instance back to its downloaded file. Key by
        // remoteRelativePath — the physical file identity, preserved across promotion and unique per resource even
        // for a legacy no-hash manifest (resourceHash is empty there, so keying on it would collapse a
        // multi-resource asset onto one file). A promoted instance keeps its path, so its file is still found.
        let planned = RestoreImportPlan.normalize(downloaded.map(\.0))
        let urlByPath = Dictionary(downloaded.map { ($0.0.remoteRelativePath, $0.1) }, uniquingKeysWith: { first, _ in first })

        var accepted: [(RemoteAssetResourceInstance, URL)] = []
        accepted.reserveCapacity(planned.count)
        var addedResourceTypes = Set<PHAssetResourceType>()

        for instance in planned {
            guard let type = instance.resourceType, let url = urlByPath[instance.remoteRelativePath] else { continue }
            if !addedResourceTypes.insert(type).inserted {
                print("[RestoreService]   duplicate resource type skipped: role=\(instance.role), slot=\(instance.slot), file=\(instance.fileName)")
                continue
            }
            accepted.append((instance, url))
        }

        return accepted
    }

    private func saveToPhotoLibrary(downloaded: [(RemoteAssetResourceInstance, URL)], creationDate: Date?) async throws -> String? {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<String?, Error>) in
            var placeholderID: String?
            PHPhotoLibrary.shared().performChanges {
                let request = PHAssetCreationRequest.forAsset()
                request.creationDate = creationDate
                placeholderID = request.placeholderForCreatedAsset?.localIdentifier

                for (instance, url) in downloaded {
                    guard let type = instance.resourceType else { continue }
                    let options = PHAssetResourceCreationOptions()
                    options.originalFilename = instance.fileName
                    request.addResource(with: type, fileURL: url, options: options)
                }
            } completionHandler: { success, error in
                if let error {
                    continuation.resume(throwing: error)
                } else if success {
                    continuation.resume(returning: placeholderID)
                } else {
                    continuation.resume(throwing: NSError(
                        domain: "RestoreService",
                        code: -1,
                        userInfo: [NSLocalizedDescriptionKey: String(localized: "restore.error.unknownFailure")]
                    ))
                }
            }
        }
    }
}

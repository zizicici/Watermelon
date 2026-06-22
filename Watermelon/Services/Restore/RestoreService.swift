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
            if let asset = try await restoreGroup(group, profile: profile, password: password, clientBox: clientBox) {
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
        profile: ServerProfileRecord,
        password: String,
        clientBox: RestoreClientBox
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

        for instance in group.instances {
            try Task.checkCancellation()
            if let cachedURL = downloadedURLsByHash[instance.resourceHash] {
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
                profile: profile,
                password: password
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
            downloadedURLsByHash[instance.resourceHash] = tempURL
            downloaded.append((instance, tempURL))
        }

        let acceptedDownloaded = acceptedDownloadedResources(from: downloaded)
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
        profile: ServerProfileRecord,
        password: String
    ) async throws {
        let deadline = Date().addingTimeInterval(NetworkRecoveryPolicy.foregroundWindow)
        let result: NetworkRecoveryResult<Void> = await NetworkRecovery.run(
            deadline: deadline,
            isRetryable: { AssetProcessor.isRecoverableNetworkFault($0, profile: profile) }
        ) {
            do {
                try await clientBox.client.download(remotePath: remotePath, localURL: localURL)
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

    private func acceptedDownloadedResources(
        from downloaded: [(RemoteAssetResourceInstance, URL)]
    ) -> [(RemoteAssetResourceInstance, URL)] {
        var accepted: [(RemoteAssetResourceInstance, URL)] = []
        accepted.reserveCapacity(downloaded.count)
        var addedResourceTypes = Set<PHAssetResourceType>()

        for entry in downloaded {
            let instance = entry.0
            guard let type = instance.resourceType else { continue }
            if !addedResourceTypes.insert(type).inserted {
                print("[RestoreService]   duplicate resource type skipped: role=\(instance.role), slot=\(instance.slot), file=\(instance.fileName)")
                continue
            }
            accepted.append(entry)
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

import Foundation
import Photos

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

        let storageClient = try storageClientFactory.makeClient(profile: profile, password: password)

        try await storageClient.connect()
        defer {
            Task { await storageClient.disconnect() }
        }

        var results: [RestoredItem] = []
        for (index, item) in items.enumerated() {
            try Task.checkCancellation()
            let creationDate = item.instances
                .compactMap(\.creationDateNs)
                .min()
                .map { Date(nanosecondsSinceEpoch: $0) }
            let group = RestoreGroup(creationDate: creationDate, instances: item.instances)
            var restoredItem: RestoredItem?
            if let asset = try await restoreGroup(group, profile: profile, storageClient: storageClient) {
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
        storageClient: RemoteStorageClientProtocol
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
            do {
                try await storageClient.download(remotePath: remotePath, localURL: tempURL)
            } catch {
                print("[RestoreService]   download FAILED: \(instance.fileName), remotePath=\(remotePath), reason=\(error.localizedDescription)")
                throw error
            }

            let fileSize = (try? FileManager.default.attributesOfItem(atPath: tempURL.path)[.size] as? Int64) ?? -1
            let fileExists = FileManager.default.fileExists(atPath: tempURL.path)
            print("[RestoreService]   downloaded: \(instance.fileName) → \(tempURL.lastPathComponent), exists=\(fileExists), localSize=\(fileSize), expectedSize=\(instance.fileSize)")
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

    private struct RestoreGroup {
        let creationDate: Date?
        let instances: [RemoteAssetResourceInstance]
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
                        userInfo: [NSLocalizedDescriptionKey: "Unknown restore failure."]
                    ))
                }
            }
        }
    }
}

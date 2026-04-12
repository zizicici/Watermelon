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

    struct RestoredAsset {
        let localIdentifier: String
        let resources: [RemoteManifestResource]
    }

    struct IndexedRestoredAsset {
        let itemIndex: Int
        let asset: RestoredAsset
    }

    /// Restore pre-grouped items with a single connection. Each inner array is one asset's resources.
    /// Returns results with `itemIndex` matching the input `items` array index.
    func restoreItems(
        items: [[RemoteManifestResource]],
        profile: ServerProfileRecord,
        password: String,
        onItemCompleted: @Sendable (Int, Int, IndexedRestoredAsset?) async throws -> Void
    ) async throws -> [IndexedRestoredAsset] {
        guard !items.isEmpty else { return [] }

        let storageClient = try storageClientFactory.makeClient(profile: profile, password: password)

        try await storageClient.connect()
        defer {
            Task { await storageClient.disconnect() }
        }

        var results: [IndexedRestoredAsset] = []
        for (index, resources) in items.enumerated() {
            try Task.checkCancellation()
            let creationDate = resources
                .compactMap(\.creationDateNs)
                .min()
                .map { Date(nanosecondsSinceEpoch: $0) }
            let group = RestoreGroup(creationDate: creationDate, resources: resources)
            var restoredAsset: IndexedRestoredAsset?
            if let asset = try await restoreGroup(group, profile: profile, storageClient: storageClient) {
                let indexed = IndexedRestoredAsset(itemIndex: index, asset: asset)
                results.append(indexed)
                restoredAsset = indexed
            }
            try await onItemCompleted(index + 1, items.count, restoredAsset)
        }
        return results
    }

    private func restoreGroup(
        _ group: RestoreGroup,
        profile: ServerProfileRecord,
        storageClient: RemoteStorageClientProtocol
    ) async throws -> RestoredAsset? {
        let resourceDesc = group.resources.map { r in
            let mapped = Self.mapResourceType(code: r.resourceType)
            let typeStr = mapped.map { String($0.rawValue) } ?? "skip"
            return "\(r.fileName) (type=\(r.resourceType)→\(typeStr), size=\(r.fileSize), hash=\(r.contentHashHex.prefix(8)))"
        }.joined(separator: ", ")
        print("[RestoreService] restoreGroup: \(group.resources.count) resource(s), creationDate=\(group.creationDate?.description ?? "nil") — [\(resourceDesc)]")

        var downloaded: [(RemoteManifestResource, URL)] = []
        for resource in group.resources {
            try Task.checkCancellation()
            let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(
                "restore_\(UUID().uuidString)_\(resource.fileName)"
            )
            try? FileManager.default.removeItem(at: tempURL)

            let remotePath = RemotePathBuilder.absolutePath(
                basePath: profile.basePath,
                remoteRelativePath: resource.remoteRelativePath
            )
            try await storageClient.download(remotePath: remotePath, localURL: tempURL)

            let fileSize = (try? FileManager.default.attributesOfItem(atPath: tempURL.path)[.size] as? Int64) ?? -1
            let fileExists = FileManager.default.fileExists(atPath: tempURL.path)
            print("[RestoreService]   downloaded: \(resource.fileName) → \(tempURL.lastPathComponent), exists=\(fileExists), localSize=\(fileSize), expectedSize=\(resource.fileSize)")
            downloaded.append((resource, tempURL))
        }

        do {
            try Task.checkCancellation()
            let localID = try await saveToPhotoLibrary(downloaded: downloaded, creationDate: group.creationDate)
            print("[RestoreService]   saveToPhotoLibrary succeeded, localID=\(localID ?? "nil")")

            for (_, url) in downloaded {
                try? FileManager.default.removeItem(at: url)
            }

            guard let localID else { return nil }
            return RestoredAsset(localIdentifier: localID, resources: group.resources)
        } catch {
            print("[RestoreService]   saveToPhotoLibrary FAILED: \(error)")
            for (_, url) in downloaded {
                try? FileManager.default.removeItem(at: url)
            }
            throw error
        }
    }

    private struct RestoreGroup {
        let creationDate: Date?
        let resources: [RemoteManifestResource]
    }

    private static func groupResourcesForRestore(_ resources: [RemoteManifestResource]) -> [RestoreGroup] {
        let grouped = Dictionary(grouping: resources) { resource -> String in
            let base = (resource.fileName as NSString).deletingPathExtension.lowercased()
            return "\(resource.creationDateNs ?? -1)|\(base)|\(resource.monthKey)"
        }

        var result: [RestoreGroup] = []
        result.reserveCapacity(grouped.count)

        for group in grouped.values {
            let hasPhotoLike = group.contains { ResourceTypeCode.isPhotoLike($0.resourceType) }
            let hasPairedVideo = group.contains { $0.resourceType == ResourceTypeCode.pairedVideo }

            if hasPhotoLike, hasPairedVideo {
                let creationDate = group
                    .compactMap(\.creationDateNs)
                    .min()
                    .map { Date(nanosecondsSinceEpoch: $0) }
                result.append(RestoreGroup(creationDate: creationDate, resources: group))
                continue
            }

            for resource in group {
                let creationDate = resource.creationDateNs
                    .map { Date(nanosecondsSinceEpoch: $0) }
                result.append(RestoreGroup(creationDate: creationDate, resources: [resource]))
            }
        }

        return result.sorted { lhs, rhs in
            (lhs.creationDate ?? .distantPast) < (rhs.creationDate ?? .distantPast)
        }
    }

    private func saveToPhotoLibrary(downloaded: [(RemoteManifestResource, URL)], creationDate: Date?) async throws -> String? {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<String?, Error>) in
            var placeholderID: String?
            PHPhotoLibrary.shared().performChanges {
                let request = PHAssetCreationRequest.forAsset()
                request.creationDate = creationDate
                placeholderID = request.placeholderForCreatedAsset?.localIdentifier

                var addedTypes = Set<PHAssetResourceType>()
                for (resource, url) in downloaded {
                    guard let type = Self.mapResourceType(code: resource.resourceType) else { continue }
                    guard addedTypes.insert(type).inserted else { continue }
                    let options = PHAssetResourceCreationOptions()
                    options.originalFilename = resource.fileName
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

    private static func mapResourceType(code: Int) -> PHAssetResourceType? {
        guard code > 0 else { return nil }
        return PHAssetResourceType(rawValue: code)
    }

}

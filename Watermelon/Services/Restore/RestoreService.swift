import Foundation
import Photos

final class RestoreService {
    private let storageClientFactory: StorageClientFactoryProtocol

    init(
        databaseManager _: DatabaseManager,
        storageClientFactory: StorageClientFactoryProtocol = StorageClientFactory()
    ) {
        self.storageClientFactory = storageClientFactory
    }

    func restore(
        resources: [RemoteManifestResource],
        profile: ServerProfileRecord,
        password: String,
        onLog: @escaping @MainActor (String) -> Void
    ) async throws {
        guard !resources.isEmpty else {
            throw BackupError.restoreNoSelection
        }

        let storageClient = try storageClientFactory.makeClient(profile: profile, password: password)

        try await storageClient.connect()
        defer {
            Task { await storageClient.disconnect() }
        }

        let restoreGroups = Self.groupResourcesForRestore(resources)
        for group in restoreGroups {
            var downloaded: [(RemoteManifestResource, URL)] = []
            for resource in group.resources {
                let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(
                    "restore_\(UUID().uuidString)_\(resource.fileName)"
                )
                try? FileManager.default.removeItem(at: tempURL)

                let remotePath = RemotePathBuilder.absolutePath(
                    basePath: profile.basePath,
                    remoteRelativePath: resource.remoteRelativePath
                )
                try await storageClient.download(remotePath: remotePath, localURL: tempURL)
                downloaded.append((resource, tempURL))
            }

            try await saveToPhotoLibrary(downloaded: downloaded, creationDate: group.creationDate)

            for (_, url) in downloaded {
                try? FileManager.default.removeItem(at: url)
            }

            await onLog("Restored group with \(group.resources.count) resource(s).")
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

    private func saveToPhotoLibrary(downloaded: [(RemoteManifestResource, URL)], creationDate: Date?) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            PHPhotoLibrary.shared().performChanges {
                let request = PHAssetCreationRequest.forAsset()
                request.creationDate = creationDate

                for (resource, url) in downloaded {
                    let type = Self.mapResourceType(code: resource.resourceType)
                    let options = PHAssetResourceCreationOptions()
                    options.originalFilename = resource.fileName
                    request.addResource(with: type, fileURL: url, options: options)
                }
            } completionHandler: { success, error in
                if let error {
                    continuation.resume(throwing: error)
                } else if success {
                    continuation.resume(returning: ())
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

    private static func mapResourceType(code: Int) -> PHAssetResourceType {
        switch code {
        case ResourceTypeCode.pairedVideo:
            return .pairedVideo
        case ResourceTypeCode.video, ResourceTypeCode.fullSizeVideo:
            return .video
        case ResourceTypeCode.audio:
            return .audio
        case ResourceTypeCode.alternatePhoto:
            return .alternatePhoto
        case ResourceTypeCode.adjustmentData:
            return .adjustmentData
        case ResourceTypeCode.photoProxy:
            return .photoProxy
        default:
            return .photo
        }
    }

}

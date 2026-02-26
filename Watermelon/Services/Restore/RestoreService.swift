import Foundation
import Photos

final class RestoreService {
    init(databaseManager _: DatabaseManager) {}

    func restore(
        resources: [RemoteManifestResource],
        profile: ServerProfileRecord,
        password: String,
        onLog: @escaping @MainActor (String) -> Void
    ) async throws {
        guard !resources.isEmpty else {
            throw BackupError.restoreNoSelection
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
            Task { await smbClient.disconnect() }
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
                try await smbClient.download(remotePath: remotePath, localURL: tempURL)
                downloaded.append((resource, tempURL))
            }

            try await saveToPhotoLibrary(downloaded: downloaded, creationDate: group.creationDate)

            for (_, url) in downloaded {
                try? FileManager.default.removeItem(at: url)
            }

            await onLog("Restored group with \(group.resources.count) resource(s).")
        }
    }

    // Legacy adapter for older screens that still provide BackupResourceRecord.
    func restore(
        resources: [BackupResourceRecord],
        profile: ServerProfileRecord,
        password: String,
        onLog: @escaping @MainActor (String) -> Void
    ) async throws {
        guard !resources.isEmpty else {
            throw BackupError.restoreNoSelection
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
            Task { await smbClient.disconnect() }
        }

        for resource in resources {
            let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent("restore_\(UUID().uuidString)_\(resource.originalFilename)")
            try? FileManager.default.removeItem(at: tempURL)

            let remotePath = RemotePathBuilder.absolutePath(
                basePath: profile.basePath,
                remoteRelativePath: resource.remoteRelativePath
            )
            try await smbClient.download(remotePath: remotePath, localURL: tempURL)

            try await saveToPhotoLibrary(downloadedLegacy: [(resource, tempURL)], creationDate: resource.backedUpAt)
            try? FileManager.default.removeItem(at: tempURL)
            await onLog("Restored resource \(resource.originalFilename).")
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
                    .map { Date(timeIntervalSince1970: Double($0) / 1_000_000_000) }
                result.append(RestoreGroup(creationDate: creationDate, resources: group))
                continue
            }

            for resource in group {
                let creationDate = resource.creationDateNs
                    .map { Date(timeIntervalSince1970: Double($0) / 1_000_000_000) }
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

    private func saveToPhotoLibrary(downloadedLegacy: [(BackupResourceRecord, URL)], creationDate: Date?) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            PHPhotoLibrary.shared().performChanges {
                let request = PHAssetCreationRequest.forAsset()
                request.creationDate = creationDate

                for (resource, url) in downloadedLegacy {
                    let type = Self.mapResourceType(name: resource.resourceType)
                    let options = PHAssetResourceCreationOptions()
                    options.originalFilename = resource.originalFilename
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

    private static func mapResourceType(name: String) -> PHAssetResourceType {
        switch name {
        case "pairedVideo":
            return .pairedVideo
        case "video", "fullSizeVideo":
            return .video
        case "audio":
            return .audio
        case "alternatePhoto":
            return .alternatePhoto
        case "adjustmentData":
            return .adjustmentData
        case "photoProxy":
            return .photoProxy
        default:
            return .photo
        }
    }
}

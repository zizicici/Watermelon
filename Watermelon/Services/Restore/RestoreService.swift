import Foundation
import GRDB
import Photos

final class RestoreService {
    private let databaseManager: DatabaseManager

    init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
    }

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
            Task {
                await smbClient.disconnect()
            }
        }

        let grouped = Dictionary(grouping: resources, by: { $0.assetLocalIdentifier })
        for (assetIdentifier, groupResources) in grouped {
            var downloaded: [(BackupResourceRecord, URL)] = []
            for resource in groupResources {
                let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent("restore_\(UUID().uuidString)_\(resource.originalFilename)")
                try? FileManager.default.removeItem(at: tempURL)
                let remotePath = RemotePathBuilder.absolutePath(
                    basePath: profile.basePath,
                    remoteRelativePath: resource.remoteRelativePath
                )
                try await smbClient.download(remotePath: remotePath, localURL: tempURL)
                downloaded.append((resource, tempURL))
            }

            let creationDate = try databaseManager.read { db in
                try BackupAssetRecord
                    .filter(Column("localIdentifier") == assetIdentifier)
                    .fetchOne(db)?.creationDate
            }

            try await saveToPhotoLibrary(downloaded: downloaded, creationDate: creationDate)

            for (_, url) in downloaded {
                try? FileManager.default.removeItem(at: url)
            }

            await onLog("Restored asset \(assetIdentifier) with \(groupResources.count) resource(s).")
        }
    }

    private func saveToPhotoLibrary(downloaded: [(BackupResourceRecord, URL)], creationDate: Date?) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            PHPhotoLibrary.shared().performChanges {
                let request = PHAssetCreationRequest.forAsset()
                request.creationDate = creationDate

                for (resource, url) in downloaded {
                    let type = Self.mapResourceType(resource.resourceType)
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
                    continuation.resume(throwing: NSError(domain: "RestoreService", code: -1, userInfo: [NSLocalizedDescriptionKey: "Unknown restore failure."]))
                }
            }
        }
    }

    private static func mapResourceType(_ value: String) -> PHAssetResourceType {
        switch value {
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

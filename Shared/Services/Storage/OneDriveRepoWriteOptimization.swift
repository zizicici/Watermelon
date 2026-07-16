import Foundation

struct OneDriveKnownFile: Sendable, Equatable {
    let path: String
    let itemID: String
    let eTag: String?
    let size: Int64?
}

struct OneDriveManifestPublishOutcome: Sendable {
    let backedUpPriorFinal: Bool
    let finalFile: OneDriveKnownFile
    let backupFile: OneDriveKnownFile?
}

protocol OneDriveUploadCollisionPolicyClient: AnyObject {
    var shouldDownloadRemoteFileForNameCollision: Bool { get }
}

protocol OneDriveManifestItemIDClient: AnyObject {
    func publishUploadedManifest(
        tempPath: String,
        finalPath: String,
        backupPath: String,
        ignoreCancellation: Bool,
        assertOwnership: @escaping @Sendable () async throws -> Void
    ) async throws -> OneDriveManifestPublishOutcome

    func downloadKnownFileForReadBackVerification(_ file: OneDriveKnownFile, localURL: URL) async throws
    func deleteKnownPresentFile(_ file: OneDriveKnownFile) async throws
    func deleteKnownPresentFile(path: String) async throws
}

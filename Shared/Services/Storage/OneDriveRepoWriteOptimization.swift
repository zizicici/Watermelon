import Foundation

struct OneDriveKnownFile: Sendable {
    let itemID: String
}

struct OneDriveManifestPublishOutcome: Sendable {
    let finalFile: OneDriveKnownFile
    let backupFile: OneDriveKnownFile?
}

protocol RemoteUploadCollisionPolicyClient: AnyObject {
    var shouldDownloadRemoteFileForNameCollision: Bool { get }
}

protocol RemoteUploadOutcomeVerificationClient: AnyObject, Sendable {
    func remoteFileMatches(localURL: URL, remotePath: String) async throws -> Bool
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

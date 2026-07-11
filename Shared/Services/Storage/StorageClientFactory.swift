import Foundation

final class StorageClientFactory: @unchecked Sendable {
    private let databaseManager: DatabaseManager?

    init(databaseManager: DatabaseManager? = nil) {
        self.databaseManager = databaseManager
    }

    func makeClient(profile: ServerProfileRecord, password: String) throws -> any RemoteStorageClientProtocol {
        let storageType = profile.resolvedStorageType
        switch storageType {
        case .smb:
            return try AMSMB2Client(config: SMBServerConfig(
                host: profile.host,
                port: profile.port,
                shareName: profile.shareName,
                basePath: profile.basePath,
                username: profile.username,
                password: password,
                domain: profile.domain
            ))
        case .webdav:
            guard let endpointURL = profile.webDAVEndpointURL else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return WebDAVClient(config: WebDAVClient.Config(
                endpointURL: endpointURL,
                username: profile.username,
                password: password
            ))
        case .externalVolume:
            guard let params = profile.externalVolumeParams else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            let onBookmarkRefreshed: ((LocalVolumeClient.BookmarkRefreshPayload) -> Void)?
            if let profileID = profile.id, databaseManager != nil {
                onBookmarkRefreshed = { [profile, weak databaseManager] payload in
                    guard let databaseManager else { return }
                    let refreshedParams = ExternalVolumeConnectionParams(
                        rootBookmarkData: payload.bookmarkData,
                        displayPath: payload.displayPath
                    )
                    guard let encoded = try? ServerProfileRecord.encodedConnectionParams(refreshedParams) else { return }
                    _ = try? databaseManager.refreshExternalVolumeConnectionParams(
                        profileID: profileID,
                        expectedConnectionParams: profile.connectionParams,
                        refreshedConnectionParams: encoded
                    )
                }
            } else {
                onBookmarkRefreshed = nil
            }

            return LocalVolumeClient(config: LocalVolumeClient.Config(
                rootBookmarkData: params.rootBookmarkData,
                displayPath: params.displayPath,
                onBookmarkRefreshed: onBookmarkRefreshed
            ))
        case .s3:
            guard let params = profile.s3Params, !profile.host.isEmpty, !profile.shareName.isEmpty else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return S3Client(config: S3Client.Config(
                endpointHost: profile.host,
                endpointPort: profile.port,
                scheme: params.scheme,
                region: S3Client.resolveRegion(userInput: params.region, host: profile.host),
                bucket: profile.shareName,
                basePath: profile.basePath,
                usePathStyle: params.usePathStyle,
                accessKeyID: profile.username,
                secretAccessKey: password,
                sessionToken: nil
            ))
        case .sftp:
            guard let params = profile.sftpParams,
                  !profile.host.isEmpty,
                  !params.hostKeyFingerprintSHA256.isEmpty else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            let credential: SFTPCredentialBlob
            do {
                credential = try SFTPCredentialBlob.decode(from: password)
            } catch {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return SFTPClient(config: SFTPClient.Config(
                host: profile.host,
                port: profile.port,
                username: profile.username,
                credential: credential,
                expectedHostKeyFingerprintSHA256: params.hostKeyFingerprintSHA256
            ))
        }
    }
}

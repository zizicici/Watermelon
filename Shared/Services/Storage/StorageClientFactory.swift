import Foundation

final class StorageClientFactory: @unchecked Sendable {
    private let databaseManager: DatabaseManager?

    init(databaseManager: DatabaseManager? = nil) {
        self.databaseManager = databaseManager
    }

    static func canonicalConnection(for profile: ServerProfileRecord) throws -> CanonicalProfileConnection {
        guard let connection = profile.canonicalConnection else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return connection
    }

    func makeClient(profile: ServerProfileRecord, password: String) throws -> any RemoteStorageClientProtocol {
        let storageType = profile.resolvedStorageType
        switch storageType {
        case .smb:
            guard case .smb(let connection) = try Self.canonicalConnection(for: profile) else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return try AMSMB2Client(config: SMBServerConfig(
                host: connection.host.socketHost,
                port: connection.port.value,
                shareName: connection.shareName,
                basePath: connection.basePath,
                username: connection.username,
                password: password,
                domain: connection.domain
            ))
        case .webdav:
            guard case .webDAV(let connection) = try Self.canonicalConnection(for: profile),
                  let endpointURL = connection.endpointURL else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return WebDAVClient(config: WebDAVClient.Config(
                endpointURL: endpointURL,
                username: connection.username,
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
            guard case .s3(let connection) = try Self.canonicalConnection(for: profile) else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return S3Client(config: S3Client.Config(
                endpointHost: connection.endpoint.host.socketHost,
                endpointPort: connection.endpoint.port.value,
                scheme: connection.endpoint.scheme.rawValue,
                region: connection.resolvedRegion,
                bucket: connection.bucket,
                basePath: connection.basePrefix,
                usePathStyle: connection.usePathStyle,
                accessKeyID: connection.accessKeyID,
                secretAccessKey: password,
                sessionToken: nil
            ))
        case .sftp:
            guard case .sftp(let connection) = try Self.canonicalConnection(for: profile),
                  !connection.hostKeyFingerprintSHA256.isEmpty else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            let credential: SFTPCredentialBlob
            do {
                credential = try SFTPCredentialBlob.decode(from: password)
            } catch {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return SFTPClient(config: SFTPClient.Config(
                host: connection.host.socketHost,
                port: connection.port.value,
                username: connection.username,
                credential: credential,
                expectedHostKeyFingerprintSHA256: connection.hostKeyFingerprintSHA256
            ))
        }
    }
}

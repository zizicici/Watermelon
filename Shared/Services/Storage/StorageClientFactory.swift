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
            if profile.id != nil, databaseManager != nil {
                onBookmarkRefreshed = { [profile, weak databaseManager] payload in
                    guard let databaseManager else { return }
                    var updated = profile
                    let refreshedParams = ExternalVolumeConnectionParams(
                        rootBookmarkData: payload.bookmarkData,
                        displayPath: payload.displayPath
                    )
                    guard let encoded = try? ServerProfileRecord.encodedConnectionParams(refreshedParams) else { return }
                    updated.connectionParams = encoded
                    try? databaseManager.saveServerProfile(&updated)
                }
            } else {
                onBookmarkRefreshed = nil
            }

            return LocalVolumeClient(config: LocalVolumeClient.Config(
                rootBookmarkData: params.rootBookmarkData,
                onBookmarkRefreshed: onBookmarkRefreshed
            ))
        }
    }
}

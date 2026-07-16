import Foundation

final class StorageClientFactory: @unchecked Sendable {
    struct BrowserLinkRegistrationToken: Hashable, Sendable {
        fileprivate let sessionID: String
        fileprivate let nonce: UUID
    }

    private struct BrowserLinkRegistration {
        let token: BrowserLinkRegistrationToken
        let client: any RemoteStorageClientProtocol
    }

    private let databaseManager: DatabaseManager?
    private let oneDriveTokenProvider: (any OneDriveAccessTokenProviding)?
    private let oneDriveSharedState: OneDriveSharedState
    private let browserLinkLock = NSLock()
    private var browserLinkClients: [String: BrowserLinkRegistration] = [:]

    init(
        databaseManager: DatabaseManager? = nil,
        oneDriveTokenProvider: (any OneDriveAccessTokenProviding)? = nil,
        oneDriveSharedState: OneDriveSharedState = OneDriveSharedState()
    ) {
        self.databaseManager = databaseManager
        self.oneDriveTokenProvider = oneDriveTokenProvider
        self.oneDriveSharedState = oneDriveSharedState
    }

    static func canonicalConnection(for profile: ServerProfileRecord) throws -> CanonicalProfileConnection {
        guard let connection = profile.canonicalConnection else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return connection
    }

    func makeClient(
        profile: ServerProfileRecord,
        credentialPayload: String
    ) throws -> any RemoteStorageClientProtocol {
        if let sessionID = profile.browserLinkSessionID {
            guard let client = browserLinkLock.withLock({ browserLinkClients[sessionID]?.client }) else {
                throw RemoteStorageClientError.notConnected
            }
            return client
        }
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
                password: credentialPayload,
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
                password: credentialPayload
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
                secretAccessKey: credentialPayload,
                sessionToken: nil
            ))
        case .sftp:
            guard case .sftp(let connection) = try Self.canonicalConnection(for: profile),
                  !connection.hostKeyFingerprintSHA256.isEmpty else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            let credential: SFTPCredentialBlob
            do {
                credential = try SFTPCredentialBlob.decode(from: credentialPayload)
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
        case .onedrive:
            guard case .oneDrive(let connection) = try Self.canonicalConnection(for: profile),
                  connection.accountType == .personal,
                  let oneDriveTokenProvider else {
                throw RemoteStorageClientError.unsupportedStorageType(storageType.rawValue)
            }
            let credential: OneDriveCredentialBlob
            do {
                credential = try OneDriveCredentialBlob.decode(from: credentialPayload)
            } catch {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return OneDriveClient(
                config: OneDriveClient.Config(connection: connection),
                credential: credential,
                tokenProvider: oneDriveTokenProvider,
                sharedState: oneDriveSharedState
            )
        }
    }

    @discardableResult
    func registerBrowserLink(
        sessionID: String,
        client: any RemoteStorageClientProtocol
    ) -> BrowserLinkRegistrationToken {
        let token = BrowserLinkRegistrationToken(sessionID: sessionID, nonce: UUID())
        browserLinkLock.withLock {
            browserLinkClients[sessionID] = BrowserLinkRegistration(token: token, client: client)
        }
        return token
    }

    func unregisterBrowserLink(token: BrowserLinkRegistrationToken) {
        browserLinkLock.withLock {
            guard browserLinkClients[token.sessionID]?.token == token else { return }
            browserLinkClients.removeValue(forKey: token.sessionID)
        }
    }
}

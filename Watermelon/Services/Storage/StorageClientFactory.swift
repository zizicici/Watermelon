import Foundation

protocol StorageClientFactoryProtocol {
    func makeClient(profile: ServerProfileRecord, password: String) throws -> any RemoteStorageClientProtocol
}

final class StorageClientFactory: StorageClientFactoryProtocol {
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
        case .externalVolume:
            guard let params = profile.externalVolumeParams else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            return LocalVolumeClient(config: LocalVolumeClient.Config(rootBookmarkData: params.rootBookmarkData))
        }
    }
}

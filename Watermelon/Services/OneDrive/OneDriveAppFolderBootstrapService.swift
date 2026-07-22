import Foundation

nonisolated struct OneDriveAppFolderBootstrapResult: Sendable {
    let connectionParams: OneDriveConnectionParams
}

nonisolated final class OneDriveAppFolderBootstrapService: @unchecked Sendable {
    private let tokenProvider: any OneDriveAccessTokenProviding
    private let sharedState: OneDriveSharedState
    private let sessionConfiguration: URLSessionConfiguration?

    init(
        tokenProvider: any OneDriveAccessTokenProviding,
        sharedState: OneDriveSharedState = OneDriveSharedState(),
        sessionConfiguration: URLSessionConfiguration? = nil
    ) {
        self.tokenProvider = tokenProvider
        self.sharedState = sharedState
        self.sessionConfiguration = sessionConfiguration?.copy() as? URLSessionConfiguration
    }

    func bootstrap(credential: OneDriveCredentialBlob) async throws -> OneDriveAppFolderBootstrapResult {
        let baseURL = OneDriveCloudEnvironment.global.graphBaseURL
        guard var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        components.percentEncodedPath += "/me/drive/special/approot"
        components.queryItems = [URLQueryItem(name: "$select", value: "id,name,folder,parentReference")]
        guard let url = components.url else { throw RemoteStorageClientError.invalidConfiguration }

        let transport = OneDriveGraphTransport(
            credential: credential,
            tokenProvider: tokenProvider,
            sharedState: sharedState,
            graphBaseURL: baseURL,
            sessionConfiguration: sessionConfiguration?.copy() as? URLSessionConfiguration
        )
        let (data, _) = try await transport.performGraph(
            method: "GET",
            url: url,
            expected: [200]
        )
        let item = try OneDriveJSON.decode(OneDriveDriveItem.self, from: data)
        guard item.folder != nil,
              let driveID = item.parentReference?.driveId,
              !driveID.isEmpty else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return OneDriveAppFolderBootstrapResult(
            connectionParams: OneDriveConnectionParams(
                driveID: driveID,
                rootItemID: item.id,
                displayRootPath: "OneDrive/Apps/\(item.name)"
            )
        )
    }
}

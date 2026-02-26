import Foundation

enum RemoteManifestRefreshResult {
    case pulled
    case remoteMissingClearedLocal
    case remoteMissingKeptLocal
}

final class ManifestSyncService {
    static let manifestFileName = MonthManifestStore.manifestFileName

    private let contentHashIndexRepository: ContentHashIndexRepository
    private let remoteLibraryScanner: RemoteLibraryScanner

    init(
        databaseManager: DatabaseManager,
        remoteLibraryScanner: RemoteLibraryScanner = RemoteLibraryScanner()
    ) {
        self.contentHashIndexRepository = ContentHashIndexRepository(databaseManager: databaseManager)
        self.remoteLibraryScanner = remoteLibraryScanner
    }

    func refreshFromRemote(
        client: SMBClientProtocol,
        basePath: String,
        clearLocalWhenMissing: Bool
    ) async throws -> RemoteManifestRefreshResult {
        let snapshot = try await remoteLibraryScanner.scanYearMonthTree(client: client, basePath: basePath)
        if snapshot.totalCount == 0 {
            if clearLocalWhenMissing {
                try contentHashIndexRepository.clearAll()
                return .remoteMissingClearedLocal
            }
            return .remoteMissingKeptLocal
        }
        return .pulled
    }

    func scanRemoteLibrary(
        client: SMBClientProtocol,
        basePath: String
    ) async throws -> RemoteLibrarySnapshot {
        try await remoteLibraryScanner.scanYearMonthTree(client: client, basePath: basePath)
    }
}

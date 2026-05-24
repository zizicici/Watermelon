import Foundation

enum RepoJSONLDownload {
    static func download(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        to localURL: URL,
        notFoundError: @autoclosure () -> Error
    ) async throws {
        do {
            try await client.download(remotePath: remotePath, localURL: localURL)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            if RemoteStorageErrorClassifier.isNotFound(error) { throw notFoundError() }
            throw error
        }
    }
}

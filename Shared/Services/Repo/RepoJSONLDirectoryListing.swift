import Foundation

enum RepoJSONLDirectoryListing {
    /// Metadata probe distinguishes confirmed-absent directories (return [])
    /// from transport/format errors (rethrow). Only applied for not-found list
    /// errors — other list failures always propagate.
    static func listFilenames(
        client: any RemoteStorageClientProtocol,
        directory: String
    ) async throws -> [String] {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: directory)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            if isStorageNotFoundError(error) {
                do {
                    let metadata = try await client.metadata(path: directory)
                    if metadata == nil { return [] }
                } catch {
                    if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                }
            }
            throw error
        }
        return entries.compactMap { entry in
            guard !entry.isDirectory, entry.name.hasSuffix(".jsonl") else { return nil }
            return entry.name
        }
    }
}

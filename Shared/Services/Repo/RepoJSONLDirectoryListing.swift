import Foundation

enum RepoJSONLDirectoryListing {
    /// Backend not-found codes vary; metadata probe distinguishes absent (return [])
    /// from transient (rethrow). Cancellation from either call surfaces as CancellationError.
    static func listFilenames(
        client: any RemoteStorageClientProtocol,
        directory: String
    ) async throws -> [String] {
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: directory)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            do {
                let metadata = try await client.metadata(path: directory)
                if metadata == nil { return [] }
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            }
            throw error
        }
        return entries.compactMap { entry in
            guard !entry.isDirectory, entry.name.hasSuffix(".jsonl") else { return nil }
            return entry.name
        }
    }
}

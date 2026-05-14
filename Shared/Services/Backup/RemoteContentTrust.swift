import CryptoKit
import Foundation

enum RemoteContentTrust {
    static func verifyHash(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        expectedSize: Int64,
        expectedHash: Data
    ) async throws -> Bool {
        do {
            guard let metadata = try await client.metadata(path: remotePath), !metadata.isDirectory else {
                return false
            }
            guard metadata.size == expectedSize else {
                return false
            }
        } catch {
            if isStorageNotFoundError(error) { return false }
            throw error
        }
        try Task.checkCancellation()
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("remote-content-verify-\(UUID().uuidString).bin")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: remotePath, localURL: temp)
        try Task.checkCancellation()
        return try hashDownloadedFile(localURL: temp, expectedSize: expectedSize, expectedHash: expectedHash, remotePath: remotePath)
    }

    private static func hashDownloadedFile(
        localURL: URL,
        expectedSize: Int64,
        expectedHash: Data,
        remotePath: String
    ) throws -> Bool {
        try Task.checkCancellation()
        let attributes = try? FileManager.default.attributesOfItem(atPath: localURL.path)
        let downloadedSize = (attributes?[.size] as? NSNumber)?.int64Value ?? -1
        guard downloadedSize == expectedSize else {
            throw NSError(
                domain: "RemoteContentTrust",
                code: 100,
                userInfo: [
                    NSLocalizedDescriptionKey:
                        "content verify download truncated for \(remotePath): expected \(expectedSize) bytes, got \(downloadedSize)"
                ]
            )
        }
        try Task.checkCancellation()
        let handle = try FileHandle(forReadingFrom: localURL)
        defer { try? handle.close() }
        var hasher = SHA256()
        let chunkSize = 64 * 1024
        while true {
            try Task.checkCancellation()
            let chunk = try handle.read(upToCount: chunkSize) ?? Data()
            if chunk.isEmpty { break }
            hasher.update(data: chunk)
        }
        return Data(hasher.finalize()) == expectedHash
    }
}

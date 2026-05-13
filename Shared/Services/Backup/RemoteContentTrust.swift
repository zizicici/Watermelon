import CryptoKit
import Foundation

enum RemoteContentTrust {
    static let defaultSmallFileLimitBytes: Int64 = 5 * 1024 * 1024

    static func verifyHash(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        expectedSize: Int64,
        expectedHash: Data
    ) async throws -> Bool {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("remote-content-verify-\(UUID().uuidString).bin")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: remotePath, localURL: temp)
        let attrs = try FileManager.default.attributesOfItem(atPath: temp.path)
        let downloadedSize = (attrs[.size] as? NSNumber)?.int64Value ?? -1
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
        let handle = try FileHandle(forReadingFrom: temp)
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

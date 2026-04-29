import CryptoKit
import Foundation

enum FileHasher {
    private static let bufferSize = 64 * 1024

    static func sha256(of fileURL: URL) throws -> (hash: Data, size: Int64) {
        let handle = try FileHandle(forReadingFrom: fileURL)
        defer { try? handle.close() }

        var hasher = SHA256()
        var totalBytes: Int64 = 0
        while true {
            try Task.checkCancellation()
            let shouldContinue: Bool = try autoreleasepool {
                let chunk = try handle.read(upToCount: bufferSize) ?? Data()
                guard !chunk.isEmpty else { return false }
                hasher.update(data: chunk)
                totalBytes += Int64(chunk.count)
                return true
            }
            if !shouldContinue { break }
        }
        return (Data(hasher.finalize()), totalBytes)
    }
}

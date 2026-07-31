import CryptoKit
import Foundation

nonisolated enum FileDigestService {
    private static let bufferSize = 64 * 1024

    static func sha256(
        of fileURL: URL,
        cancellationController: BackupCancellationController? = nil
    ) throws -> Data {
        try sha256AndSize(
            of: fileURL,
            cancellationController: cancellationController
        ).hash
    }

    static func sha256AndSize(
        of fileURL: URL,
        cancellationController: BackupCancellationController? = nil
    ) throws -> (hash: Data, size: Int64) {
        let fileHandle = try FileHandle(forReadingFrom: fileURL)
        defer {
            try? fileHandle.close()
        }

        var hasher = SHA256()
        var totalBytes: Int64 = 0
        while true {
            try cancellationController?.throwIfCancelled()
            try Task.checkCancellation()
            let shouldContinue: Bool = try autoreleasepool {
                let chunk = try fileHandle.read(upToCount: bufferSize) ?? Data()
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

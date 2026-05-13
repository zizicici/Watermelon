import Foundation
import CryptoKit

/// Metadata writes (commits, snapshots, repo.json, version.json) on backends
/// without exclusive create are the durable truth of the repo and cannot
/// tolerate a peer's bytes silently winning the destination path.
enum MetadataCreateGate {
    enum Error: Swift.Error {
        case staleAfterStagedMove(remotePath: String)
    }

    /// `.exclusive` → direct create. `.overwritePossible` → UUID staging path,
    /// verify, move, post-verify; mismatch surfaces as `.alreadyExists` so the
    /// caller re-allocates.
    static func createWithStagingFallback(
        client: any RemoteStorageClientProtocol,
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool
    ) async throws -> AtomicCreateResult {
        let size = (try? FileManager.default.attributesOfItem(atPath: localURL.path)[.size] as? Int64) ?? 0
        let guarantee = client.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
        switch guarantee {
        case .exclusive:
            let result = try await client.atomicCreate(
                localURL: localURL,
                remotePath: remotePath,
                respectTaskCancellation: respectTaskCancellation
            )
            // S3 single-part PUT phantom: server wrote our bytes but client timed out;
            // retry hits If-None-Match → 412 → .alreadyExists, but it's our own bytes.
            // Caller paths are writer-unique so a peer can't be the source. Verify SHA
            // and upgrade to .created when it matches.
            if case .alreadyExists = result {
                do {
                    if try await verifyMatchesLocal(client: client, remotePath: remotePath, localURL: localURL) {
                        return .created
                    }
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    // Verify inconclusive — fall through to surface .alreadyExists as-is.
                }
            }
            return result
        case .overwritePossible:
            let stagingPath = "\(remotePath).staging-\(UUID().uuidString)"
            let stagingResult = try await client.atomicCreate(
                localURL: localURL,
                remotePath: stagingPath,
                respectTaskCancellation: respectTaskCancellation
            )
            switch stagingResult {
            case .created, .bestEffortRetry:
                break
            case .alreadyExists:
                throw NSError(domain: "MetadataCreateGate", code: -2, userInfo: [
                    NSLocalizedDescriptionKey:
                        "staging path \(stagingPath) already exists — UUID collision indicates a programming error"
                ])
            }
            do {
                let finalization = try await client.moveIfAbsent(from: stagingPath, to: remotePath)
                if case .alreadyExists = finalization {
                    do {
                        if try await verifyMatchesLocal(client: client, remotePath: remotePath, localURL: localURL) {
                            try? await client.delete(path: stagingPath)
                            return .created
                        }
                    } catch is CancellationError {
                        try? await client.delete(path: stagingPath)
                        throw CancellationError()
                    } catch {
                        // Verify inconclusive — fall through to `.alreadyExists`.
                    }
                    try? await client.delete(path: stagingPath)
                    return .alreadyExists
                }
            } catch {
                try? await client.delete(path: stagingPath)
                throw error
            }
            // A peer can overwrite during the move window; ambiguous retries must re-allocate.
            for attempt in 0..<3 {
                do {
                    if try await verifyMatchesLocal(client: client, remotePath: remotePath, localURL: localURL) {
                        return .created
                    } else {
                        return .alreadyExists
                    }
                } catch is CancellationError {
                    throw CancellationError()
                } catch {
                    if attempt + 1 < 3 {
                        try await Task.sleep(for: .milliseconds(200 * (1 << attempt)))
                    }
                }
            }
            return .bestEffortRetry
        }
    }

    private static func verifyMatchesLocal(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        localURL: URL
    ) async throws -> Bool {
        let verifyURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("metadata-verify-\(UUID().uuidString).bin")
        defer { try? FileManager.default.removeItem(at: verifyURL) }
        try await client.download(remotePath: remotePath, localURL: verifyURL)
        let remoteAttrs = try FileManager.default.attributesOfItem(atPath: verifyURL.path)
        let localAttrs = try FileManager.default.attributesOfItem(atPath: localURL.path)
        guard let remoteSize = remoteAttrs[.size] as? Int64,
              let localSize = localAttrs[.size] as? Int64,
              remoteSize == localSize else {
            return false
        }
        return try streamingSHA256(of: verifyURL) == streamingSHA256(of: localURL)
    }

    private static func streamingSHA256(of url: URL) throws -> SHA256.Digest {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var hasher = SHA256()
        let chunkSize = 64 * 1024
        while true {
            try Task.checkCancellation()
            let chunk = try handle.read(upToCount: chunkSize) ?? Data()
            if chunk.isEmpty { break }
            hasher.update(data: chunk)
        }
        return hasher.finalize()
    }
}

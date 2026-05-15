import Foundation
import CryptoKit

/// Metadata writes (commits, snapshots, repo.json, version.json) on backends
/// without exclusive create are the durable truth of the repo and cannot
/// tolerate a peer's bytes silently winning the destination path.
enum MetadataCreateGate {
    enum FinalizationPolicy: Equatable {
        case allowBestEffort
        case requireExclusiveMove
    }

    enum Error: LocalizedError {
        case stagingVerificationFailed(remotePath: String, underlying: Swift.Error?)
        case finalVerificationFailed(remotePath: String, underlying: Swift.Error?)
        case nonExclusiveFinalization(remotePath: String)

        var errorDescription: String? {
            switch self {
            case .stagingVerificationFailed(let remotePath, let underlying):
                if let underlying {
                    return "staged metadata bytes could not be verified at \(remotePath): \(underlying.localizedDescription)"
                }
                return "staged metadata bytes diverged at \(remotePath)"
            case .finalVerificationFailed(let remotePath, let underlying):
                if let underlying {
                    return "metadata bytes could not be verified after move at \(remotePath): \(underlying.localizedDescription)"
                }
                return "metadata bytes diverged post-move at \(remotePath)"
            case .nonExclusiveFinalization(let remotePath):
                return "metadata finalization at \(remotePath) requires exclusive move support"
            }
        }
    }

    /// `.exclusive` → direct create. `.overwritePossible` → UUID staging path,
    /// verify, move, post-verify.
    static func createWithStagingFallback(
        client: any RemoteStorageClientProtocol,
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        finalizationPolicy: FinalizationPolicy = .allowBestEffort
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
                    if try await verifyMatchesLocalWithRetries(client: client, remotePath: remotePath, localURL: localURL) {
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
                guard try await verifyMatchesLocalWithRetries(
                    client: client,
                    remotePath: stagingPath,
                    localURL: localURL
                ) else {
                    try? await client.delete(path: stagingPath)
                    throw Error.stagingVerificationFailed(remotePath: stagingPath, underlying: nil)
                }
            } catch is CancellationError {
                try? await client.delete(path: stagingPath)
                throw CancellationError()
            } catch let error as Error {
                throw error
            } catch {
                try? await client.delete(path: stagingPath)
                throw Error.stagingVerificationFailed(remotePath: stagingPath, underlying: error)
            }
            do {
                // Probe before the call. Backends with `.exclusive` guarantee skip the probe (the flag is the answer);
                // anything else asks the backend at runtime so vendor-specific failure modes don't leak back here as error classification.
                let supportsExclusiveMove: Bool
                if client.moveIfAbsentGuarantee == .exclusive {
                    supportsExclusiveMove = true
                } else {
                    supportsExclusiveMove = try await client.supportsExclusiveMoveIfAbsent(forDestinationPath: remotePath)
                }
                let finalization: AtomicCreateResult
                if supportsExclusiveMove {
                    finalization = try await client.moveIfAbsent(from: stagingPath, to: remotePath)
                } else {
                    switch finalizationPolicy {
                    case .requireExclusiveMove:
                        try? await client.delete(path: stagingPath)
                        throw Error.nonExclusiveFinalization(remotePath: remotePath)
                    case .allowBestEffort:
                        finalization = try await bestEffortCopyIfAbsent(
                            client: client,
                            stagingPath: stagingPath,
                            remotePath: remotePath
                        )
                    }
                }
                if case .alreadyExists = finalization {
                    do {
                        if try await verifyMatchesLocalWithRetries(client: client, remotePath: remotePath, localURL: localURL) {
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
                if case .bestEffortRetry = finalization,
                   finalizationPolicy == .requireExclusiveMove,
                   !supportsExclusiveMove {
                    try? await client.delete(path: stagingPath)
                    throw Error.nonExclusiveFinalization(remotePath: remotePath)
                }
            } catch {
                try? await client.delete(path: stagingPath)
                throw error
            }
            try? await client.delete(path: stagingPath)
            do {
                guard try await verifyMatchesLocalWithRetries(
                    client: client,
                    remotePath: remotePath,
                    localURL: localURL
                ) else {
                    throw Error.finalVerificationFailed(remotePath: remotePath, underlying: nil)
                }
                return .created
            } catch is CancellationError {
                throw CancellationError()
            } catch let error as Error {
                throw error
            } catch {
                throw Error.finalVerificationFailed(remotePath: remotePath, underlying: error)
            }
        }
    }

    private static func bestEffortCopyIfAbsent(
        client: any RemoteStorageClientProtocol,
        stagingPath: String,
        remotePath: String
    ) async throws -> AtomicCreateResult {
        if try await client.metadata(path: remotePath) != nil {
            return .alreadyExists
        }
        try await client.copy(from: stagingPath, to: remotePath)
        do {
            try await client.delete(path: stagingPath)
            return .bestEffortRetry
        } catch {
            return .bestEffortRetry
        }
    }

    static func verifyMatchesLocalWithRetries(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        localURL: URL
    ) async throws -> Bool {
        var lastError: Swift.Error?
        let deadline = Date().addingTimeInterval(max(client.livenessConsistencyGraceSeconds, 1))
        var attempt = 0
        while true {
            do {
                if try await verifyMatchesLocal(client: client, remotePath: remotePath, localURL: localURL) {
                    return true
                }
                lastError = nil
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                lastError = error
            }
            // Same-size stale reads after our own write also need the grace window;
            // exiting on the first byte mismatch defeats the eventual-consistency budget.
            guard Date() < deadline else {
                if let lastError { throw lastError }
                return false
            }
            try await Task.sleep(for: .milliseconds(200 * (1 << min(attempt, 3))))
            attempt += 1
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

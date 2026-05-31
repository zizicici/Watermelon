import Foundation
import CryptoKit

// Typed result of a metadata-write verifier. Never thrown — every result (including
// cancellation and transport failures) is encoded so callers can clean up locally
// then map to their own error vocabulary.
enum MetadataWriteVerifyOutcome: Sendable {
    // Remote bytes confirmed to match local per the verifier's policy.
    case matched
    // Remote bytes deterministically differ from local per the verifier's policy.
    case deterministicMismatch
    // Transport error classified .transient by RemoteWriteClassifier.classifyVerifyFailure.
    case transientFailure(underlying: any Error)
    // Transport error classified .permanent by RemoteWriteClassifier.classifyVerifyFailure.
    case permanentFailure(underlying: any Error)
    // CancellationError observed inside the verifier — caller cleans up then throws CancellationError().
    case cancelled
}

protocol MetadataWriteVerifier: Sendable {
    // Inspects the remote object at remotePath against localURL per this verifier's policy.
    // NEVER throws — every result is encoded in MetadataWriteVerifyOutcome.
    func verify(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        localURL: URL
    ) async -> MetadataWriteVerifyOutcome
}

enum MetadataWriteVerifiers {
    static let byteEquality: any MetadataWriteVerifier = ByteEqualityVerifier()

    static func commitAware(expectedSha: String, expectedRowCount: Int) -> any MetadataWriteVerifier {
        CommitAwareVerifier(expectedSha: expectedSha, expectedRowCount: expectedRowCount)
    }
}

enum MetadataCreateOrchestrator {
    enum AfterCreateAttempt: Sendable {
        case createdWithoutVerification
        case verifyAttempted(result: AtomicCreateResult, verify: MetadataWriteVerifyOutcome)
    }

    // EXCLUSIVE-CREATE shape ONLY: directly atomicCreate on the FINAL remotePath,
    // then verify if needed. SAFE ONLY for callers that already know the backend
    // treats this remotePath as exclusive (atomicCreateGuarantee == .exclusive),
    // OR for callers that have separately ensured peer-safe finalization.
    // MUST NOT be used as the entry point for overwrite-prone backends needing
    // staging. For staged callers, go through
    // MetadataCreateGate.createWithStagingFallback(..., finalizationPolicy: .requireExclusiveMove)
    // and consume the verifier directly on the post-Gate .bestEffortRetry result.
    static func atomicCreateThenVerify(
        client: any RemoteStorageClientProtocol,
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        verifier: any MetadataWriteVerifier
    ) async throws -> AfterCreateAttempt {
        // URLSession-backed clients (S3) surface task cancellation as
        // NSURLErrorCancelled, not literal CancellationError. Normalize at the
        // boundary so direct callers see CancellationError instead of a wrapped
        // finalization failure.
        let result: AtomicCreateResult
        do {
            result = try await client.atomicCreate(
                localURL: localURL,
                remotePath: remotePath,
                respectTaskCancellation: respectTaskCancellation
            )
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
        switch result {
        case .created:
            return .createdWithoutVerification
        case .alreadyExists, .bestEffortRetry:
            // S3 single-part PUT phantom: server wrote our bytes but client timed out;
            // retry hits If-None-Match → 412 → .alreadyExists, but it's our own bytes.
            // Caller payloads embed writer-distinguishing fields (created_by_writer /
            // created_at_ms / run_id) so a peer can never SHA-collide with our local
            // bytes — matching SHA proves the remote bytes are ours.
            let outcome = await verifier.verify(client: client, remotePath: remotePath, localURL: localURL)
            return .verifyAttempted(result: result, verify: outcome)
        }
    }
}

private struct ByteEqualityVerifier: MetadataWriteVerifier {
    func verify(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        localURL: URL
    ) async -> MetadataWriteVerifyOutcome {
        var lastError: (any Error)?
        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: 1)
        var attempt = 0
        while true {
            do {
                if try await Self.verifyMatchesLocal(client: client, remotePath: remotePath, localURL: localURL) {
                    return .matched
                }
                // Same-size stale reads after our own write also need the grace window;
                // exiting on the first byte mismatch defeats the eventual-consistency budget.
                lastError = nil
            } catch is CancellationError {
                return .cancelled
            } catch {
                if RemoteWriteClassifier.isCancellation(error) {
                    return .cancelled
                }
                lastError = error
            }
            guard Date() < deadline else {
                if let lastError {
                    switch RemoteWriteClassifier.classifyVerifyFailure(lastError) {
                    case .cancelled:
                        return .cancelled
                    case .transient:
                        return .transientFailure(underlying: lastError)
                    case .permanent:
                        return .permanentFailure(underlying: lastError)
                    }
                }
                return .deterministicMismatch
            }
            do {
                try await Task.sleep(for: .milliseconds(200 * (1 << min(attempt, 3))))
            } catch {
                return .cancelled
            }
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
}

private struct CommitAwareVerifier: MetadataWriteVerifier {
    let expectedSha: String
    let expectedRowCount: Int

    func verify(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        localURL: URL
    ) async -> MetadataWriteVerifyOutcome {
        var lastError: (any Error)?
        // A just-written commit can 404 inside the backend read-after-write grace window;
        // retry the structured readback until the deadline (like ByteEqualityVerifier) so
        // visibility lag isn't misclassified as a permanent ioFailure that fails the flush.
        let deadline = client.metadataReadAfterWriteDeadline(floorSeconds: 1)
        var attempt = 0
        while true {
            do {
                if try await downloadAndCompare(client: client, remotePath: remotePath) {
                    return .matched
                }
                // Content-addressed mismatch is deterministic (full object or 404, never a
                // stale-but-parseable body), so reseq promptly instead of spending the budget.
                return .deterministicMismatch
            } catch is CancellationError {
                return .cancelled
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { return .cancelled }
                lastError = error
            }
            guard Date() < deadline else {
                if let lastError {
                    switch RemoteWriteClassifier.classifyVerifyFailure(lastError) {
                    case .cancelled:
                        return .cancelled
                    case .transient:
                        return .transientFailure(underlying: lastError)
                    case .permanent:
                        return .permanentFailure(underlying: lastError)
                    }
                }
                return .deterministicMismatch
            }
            do {
                try await Task.sleep(for: .milliseconds(200 * (1 << min(attempt, 3))))
            } catch {
                return .cancelled
            }
            attempt += 1
        }
    }

    // Returns true on full match, false on a deterministic parse/sha/rowCount mismatch.
    // Transport failures throw so the caller can retry within the grace window.
    private func downloadAndCompare(
        client: any RemoteStorageClientProtocol,
        remotePath: String
    ) async throws -> Bool {
        let verifyURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("commit-verify-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: verifyURL) }
        try await client.download(remotePath: remotePath, localURL: verifyURL)
        let parsed: CommitFile
        do {
            parsed = try CommitLogReader.parse(localURL: verifyURL)
        } catch {
            return false
        }
        if parsed.sha256Hex.lowercased() != expectedSha.lowercased() || parsed.rowCount != expectedRowCount {
            return false
        }
        return true
    }
}

private func streamingSHA256(of url: URL) throws -> SHA256.Digest {
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

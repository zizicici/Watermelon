import Foundation

// Durable repo metadata must not let peer bytes silently win destination paths.
enum MetadataCreateGate {
    enum FinalizationPolicy: Equatable {
        case allowBestEffort
        case requireExclusiveMove
    }

    nonisolated enum MetadataWriteVerification: Sendable, Equatable {
        case verifiedLocalBytes
        case unverified
    }

    struct CreateOutcome: Sendable {
        let result: AtomicCreateResult
        let verification: MetadataWriteVerification
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

    static func createWithStagingFallback(
        client: any RemoteStorageClientProtocol,
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        finalizationPolicy: FinalizationPolicy = .allowBestEffort
    ) async throws -> AtomicCreateResult {
        try await createWithStagingFallbackOutcome(
            client: client,
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            finalizationPolicy: finalizationPolicy
        ).result
    }

    static func createWithStagingFallbackOutcome(
        client: any RemoteStorageClientProtocol,
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        finalizationPolicy: FinalizationPolicy = .allowBestEffort
    ) async throws -> CreateOutcome {
        if !respectTaskCancellation {
            return try await Task { @Sendable () throws -> CreateOutcome in
                try await Self._stagingFallbackOutcomeBody(
                    client: client, localURL: localURL, remotePath: remotePath,
                    respectTaskCancellation: false, finalizationPolicy: finalizationPolicy
                )
            }.value
        }
        return try await _stagingFallbackOutcomeBody(
            client: client, localURL: localURL, remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation, finalizationPolicy: finalizationPolicy
        )
    }

    private static func _stagingFallbackOutcomeBody(
        client: any RemoteStorageClientProtocol,
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        finalizationPolicy: FinalizationPolicy = .allowBestEffort
    ) async throws -> CreateOutcome {
        let size = (try? FileManager.default.attributesOfItem(atPath: localURL.path)[.size] as? Int64) ?? 0
        let guarantee = client.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
        switch guarantee {
        case .exclusive:
            let attempt = try await MetadataCreateOrchestrator.atomicCreateThenVerify(
                client: client,
                localURL: localURL,
                remotePath: remotePath,
                respectTaskCancellation: respectTaskCancellation,
                verifier: MetadataWriteVerifiers.byteEquality
            )
            return try Self.mapExclusiveCreateAttempt(attempt, remotePath: remotePath)
        case .overwritePossible:
            let stagingPath = "\(remotePath).staging-\(UUID().uuidString)"
            let stagingResult: AtomicCreateResult
            do {
                stagingResult = try await client.atomicCreate(
                    localURL: localURL,
                    remotePath: stagingPath,
                    respectTaskCancellation: respectTaskCancellation
                )
            } catch {
                try? await client.delete(path: stagingPath)
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                throw error
            }
            switch stagingResult {
            case .created, .bestEffortRetry:
                break
            case .alreadyExists:
                throw NSError(domain: "MetadataCreateGate", code: -2, userInfo: [
                    NSLocalizedDescriptionKey:
                        "staging path \(stagingPath) already exists — UUID collision indicates a programming error"
                ])
            }
            let stagingVerifyOutcome = await MetadataWriteVerifiers.byteEquality.verify(
                client: client, remotePath: stagingPath, localURL: localURL
            )
            switch Self.mapStagingVerify(stagingVerifyOutcome, stagingPath: stagingPath) {
            case .continueToFinalization:
                break
            case .cleanupThenThrowStagingVerificationFailed(let underlying):
                try? await client.delete(path: stagingPath)
                throw Error.stagingVerificationFailed(remotePath: stagingPath, underlying: underlying)
            case .cleanupThenThrowCancellation:
                try? await client.delete(path: stagingPath)
                throw CancellationError()
            }
            do {
                let supportsExclusiveMove = try await client.resolvedSupportsExclusiveMoveIfAbsent(forDestinationPath: remotePath)
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
                    let postMoveOutcome = await MetadataWriteVerifiers.byteEquality.verify(
                        client: client, remotePath: remotePath, localURL: localURL
                    )
                    let mapped = Self.mapPostMoveAlreadyExistsVerify(postMoveOutcome, remotePath: remotePath)
                    try? await client.delete(path: stagingPath)
                    switch mapped {
                    case .returnCreatedVerifiedLocalBytes:
                        return CreateOutcome(result: .created, verification: .verifiedLocalBytes)
                    case .returnAlreadyExistsUnverified:
                        return CreateOutcome(result: .alreadyExists, verification: .unverified)
                    case .throwFinalVerificationFailed(let underlying):
                        throw Error.finalVerificationFailed(remotePath: remotePath, underlying: underlying)
                    case .throwCancellation:
                        throw CancellationError()
                    }
                }
            } catch {
                try? await client.delete(path: stagingPath)
                // URLSession-shaped cancellation can leak from moveIfAbsent / copy /
                // metadata on URLSession-backed clients; normalize so direct callers
                // get CancellationError instead of a wrapped finalization failure.
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                throw error
            }
            try? await client.delete(path: stagingPath)
            let finalVerifyOutcome = await MetadataWriteVerifiers.byteEquality.verify(
                client: client, remotePath: remotePath, localURL: localURL
            )
            return try Self.mapFinalPostMoveVerify(finalVerifyOutcome, remotePath: remotePath)
        }
    }

    // MARK: - Verify-outcome mapping helpers (internal for test coverage)

    internal enum StagingVerifyAction: Sendable {
        case continueToFinalization
        case cleanupThenThrowStagingVerificationFailed(underlying: (any Swift.Error)?)
        case cleanupThenThrowCancellation
    }

    internal enum PostMoveAlreadyExistsAction: Sendable {
        case returnCreatedVerifiedLocalBytes
        case returnAlreadyExistsUnverified
        case throwFinalVerificationFailed(underlying: (any Swift.Error)?)
        case throwCancellation
    }

    internal static func mapExclusiveCreateAttempt(
        _ attempt: MetadataCreateOrchestrator.AfterCreateAttempt,
        remotePath: String
    ) throws -> CreateOutcome {
        switch attempt {
        case .createdWithoutVerification:
            return CreateOutcome(result: .created, verification: .unverified)
        case .verifyAttempted(_, .matched):
            return CreateOutcome(result: .created, verification: .verifiedLocalBytes)
        case .verifyAttempted(let result, .deterministicMismatch),
             .verifyAttempted(let result, .transientFailure):
            // Preserve the original atomicCreate result so callers like
            // MigrationMarkerStore that distinguish .alreadyExists (peer collision —
            // retry with a different marker) from .bestEffortRetry (our upload landed
            // but couldn't be verified — fall through to caller's verify) keep their
            // existing decision paths.
            return CreateOutcome(result: result, verification: .unverified)
        case .verifyAttempted(_, .permanentFailure(let underlying)):
            throw Error.finalVerificationFailed(remotePath: remotePath, underlying: underlying)
        case .verifyAttempted(_, .cancelled):
            throw CancellationError()
        }
    }

    internal static func mapStagingVerify(
        _ outcome: MetadataWriteVerifyOutcome,
        stagingPath: String
    ) -> StagingVerifyAction {
        switch outcome {
        case .matched:
            return .continueToFinalization
        case .deterministicMismatch:
            return .cleanupThenThrowStagingVerificationFailed(underlying: nil)
        case .transientFailure(let underlying), .permanentFailure(let underlying):
            return .cleanupThenThrowStagingVerificationFailed(underlying: underlying)
        case .cancelled:
            return .cleanupThenThrowCancellation
        }
    }

    internal static func mapPostMoveAlreadyExistsVerify(
        _ outcome: MetadataWriteVerifyOutcome,
        remotePath: String
    ) -> PostMoveAlreadyExistsAction {
        switch outcome {
        case .matched:
            return .returnCreatedVerifiedLocalBytes
        case .deterministicMismatch, .transientFailure:
            return .returnAlreadyExistsUnverified
        case .permanentFailure(let underlying):
            return .throwFinalVerificationFailed(underlying: underlying)
        case .cancelled:
            return .throwCancellation
        }
    }

    internal static func mapFinalPostMoveVerify(
        _ outcome: MetadataWriteVerifyOutcome,
        remotePath: String
    ) throws -> CreateOutcome {
        switch outcome {
        case .matched:
            return CreateOutcome(result: .created, verification: .verifiedLocalBytes)
        case .deterministicMismatch:
            throw Error.finalVerificationFailed(remotePath: remotePath, underlying: nil)
        case .transientFailure:
            return CreateOutcome(result: .bestEffortRetry, verification: .unverified)
        case .permanentFailure(let underlying):
            throw Error.finalVerificationFailed(remotePath: remotePath, underlying: underlying)
        case .cancelled:
            throw CancellationError()
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

    // Thin shim for existing direct callers; delegates to MetadataWriteVerifiers.byteEquality.
    static func verifyMatchesLocalWithRetries(
        client: any RemoteStorageClientProtocol,
        remotePath: String,
        localURL: URL
    ) async throws -> Bool {
        switch await MetadataWriteVerifiers.byteEquality.verify(
            client: client, remotePath: remotePath, localURL: localURL
        ) {
        case .matched:
            return true
        case .deterministicMismatch:
            return false
        case .transientFailure(let underlying), .permanentFailure(let underlying):
            throw underlying
        case .cancelled:
            throw CancellationError()
        }
    }
}

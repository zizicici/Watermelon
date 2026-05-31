import Foundation

// Stages overwrite-prone backends so exists+upload cannot replace a peer commit.
actor CommitLogWriter {
    enum WriteError: Error {
        case alreadyExists
        case ioFailure(Error)
        case encodingFailed(Error)
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    @discardableResult
    func write(
        header: CommitHeader,
        ops: [CommitOp],
        month: LibraryMonthKey,
        respectTaskCancellation: Bool
    ) async throws -> CommitFile {
        var lines: [String] = []
        var integrity = IntegrityAccumulator()

        let headerLine = try CommitOpMapper.encodeHeaderLine(header)
        lines.append(headerLine)
        integrity.absorbLine(headerLine)

        for op in ops {
            let line = try CommitOpMapper.encodeOpLine(op)
            lines.append(line)
            integrity.absorbLine(line)
        }

        let sha = integrity.finalize()
        let endLine = try CommitOpMapper.encodeEndLine(sha256Hex: sha, rowCount: integrity.rowCount)
        lines.append(endLine)

        let body = lines.joined(separator: "\n") + "\n"
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("commit-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        guard let data = body.data(using: .utf8) else {
            throw WriteError.encodingFailed(NSError(domain: "CommitLogWriter", code: 1, userInfo: [NSLocalizedDescriptionKey: "utf8 encoding failed"]))
        }
        do {
            try data.write(to: tempURL, options: .atomic)
        } catch {
            throw WriteError.ioFailure(error)
        }

        let remotePath = RepoLayout.commitFilePath(
            base: basePath,
            month: month,
            writerID: header.writerID,
            seq: header.seq
        )
        let size = Int64(data.count)
        let guarantee = client.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
        switch guarantee {
        case .exclusive:
            if !respectTaskCancellation {
                // Keep create and readback verification in the same uncancelled task.
                try await Task { @Sendable () throws -> Void in
                    try await Self.performExclusiveCreateAndVerify(
                        client: client, localURL: tempURL, remotePath: remotePath,
                        expectedSha: sha, expectedRowCount: integrity.rowCount,
                        respectTaskCancellation: false
                    )
                }.value
            } else {
                try await Self.performExclusiveCreateAndVerify(
                    client: client, localURL: tempURL, remotePath: remotePath,
                    expectedSha: sha, expectedRowCount: integrity.rowCount,
                    respectTaskCancellation: true
                )
            }
        case .overwritePossible:
            if !respectTaskCancellation {
                try await Task { @Sendable () throws -> Void in
                    try await Self.performStagedCreateAndVerify(
                        client: client, localURL: tempURL, remotePath: remotePath,
                        expectedSha: sha, expectedRowCount: integrity.rowCount,
                        respectTaskCancellation: false
                    )
                }.value
            } else {
                try await Self.performStagedCreateAndVerify(
                    client: client, localURL: tempURL, remotePath: remotePath,
                    expectedSha: sha, expectedRowCount: integrity.rowCount,
                    respectTaskCancellation: true
                )
            }
        }

        return CommitFile(header: header, ops: ops, sha256Hex: sha, rowCount: integrity.rowCount)
    }

    private static func performExclusiveCreateAndVerify(
        client: any RemoteStorageClientProtocol,
        localURL: URL,
        remotePath: String,
        expectedSha: String,
        expectedRowCount: Int,
        respectTaskCancellation: Bool
    ) async throws {
        let attempt: MetadataCreateOrchestrator.AfterCreateAttempt
        do {
            attempt = try await MetadataCreateOrchestrator.atomicCreateThenVerify(
                client: client,
                localURL: localURL,
                remotePath: remotePath,
                respectTaskCancellation: respectTaskCancellation,
                verifier: MetadataWriteVerifiers.commitAware(
                    expectedSha: expectedSha, expectedRowCount: expectedRowCount
                )
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw WriteError.ioFailure(error)
        }
        try Self.mapExclusiveCreateAttempt(attempt)
    }

    private static func performStagedCreateAndVerify(
        client: any RemoteStorageClientProtocol,
        localURL: URL,
        remotePath: String,
        expectedSha: String,
        expectedRowCount: Int,
        respectTaskCancellation: Bool
    ) async throws {
        let result: AtomicCreateResult
        do {
            result = try await MetadataCreateGate.createAuthoritative(
                client: client,
                localURL: localURL,
                remotePath: remotePath,
                respectTaskCancellation: respectTaskCancellation
            )
        } catch let error as MetadataCreateGate.Error {
            if RemoteWriteClassifier.isMetadataGateCancellation(error) { throw CancellationError() }
            throw WriteError.ioFailure(error)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw WriteError.ioFailure(error)
        }
        switch result {
        case .created:
            return
        case .alreadyExists:
            throw WriteError.alreadyExists
        case .bestEffortRetry:
            let outcome = await MetadataWriteVerifiers.commitAware(
                expectedSha: expectedSha, expectedRowCount: expectedRowCount
            ).verify(client: client, remotePath: remotePath, localURL: localURL)
            try Self.mapStagedBestEffortRetryVerify(outcome)
        }
    }

    // MARK: - Verify-outcome mapping helpers (internal for test coverage)

    internal static func mapExclusiveCreateAttempt(
        _ attempt: MetadataCreateOrchestrator.AfterCreateAttempt
    ) throws {
        switch attempt {
        case .createdWithoutVerification:
            return
        case .verifyAttempted(_, .matched):
            return
        case .verifyAttempted(_, .deterministicMismatch),
             .verifyAttempted(_, .transientFailure):
            throw WriteError.alreadyExists
        case .verifyAttempted(_, .permanentFailure(let underlying)):
            throw WriteError.ioFailure(underlying)
        case .verifyAttempted(_, .cancelled):
            throw CancellationError()
        }
    }

    internal static func mapStagedBestEffortRetryVerify(
        _ outcome: MetadataWriteVerifyOutcome
    ) throws {
        switch outcome {
        case .matched:
            return
        case .deterministicMismatch, .transientFailure:
            throw WriteError.alreadyExists
        case .permanentFailure(let underlying):
            throw WriteError.ioFailure(underlying)
        case .cancelled:
            throw CancellationError()
        }
    }

}

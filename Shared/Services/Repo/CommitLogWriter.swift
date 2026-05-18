import Foundation

/// Stages overwrite-prone backends so exists+upload cannot replace a peer commit.
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
            let result: AtomicCreateResult
            do {
                result = try await client.atomicCreate(
                    localURL: tempURL,
                    remotePath: remotePath,
                    respectTaskCancellation: respectTaskCancellation
                )
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                throw WriteError.ioFailure(error)
            }
            switch result {
            case .created:
                break
            case .alreadyExists:
                // Verify alreadyExists because S3 may have stored our bytes before losing the response.
                try await verifyAfterAlreadyExists(
                    remotePath: remotePath,
                    expectedSha: sha,
                    expectedRowCount: integrity.rowCount
                )
            case .bestEffortRetry:
                // Verify best-effort retries so peer collisions still surface.
                try await verifyAfterAlreadyExists(
                    remotePath: remotePath,
                    expectedSha: sha,
                    expectedRowCount: integrity.rowCount
                )
            }
        case .overwritePossible:
            // Stage SMB-style writes so exists+upload cannot overwrite a peer commit.
            let result: AtomicCreateResult
            do {
                result = try await MetadataCreateGate.createWithStagingFallback(
                    client: client,
                    localURL: tempURL,
                    remotePath: remotePath,
                    respectTaskCancellation: respectTaskCancellation,
                    finalizationPolicy: .requireExclusiveMove
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
                break
            case .alreadyExists:
                throw WriteError.alreadyExists
            case .bestEffortRetry:
                // Re-verify transient readback failures so peer collisions still surface.
                try await verifyAfterAlreadyExists(
                    remotePath: remotePath,
                    expectedSha: sha,
                    expectedRowCount: integrity.rowCount
                )
            }
        }

        return CommitFile(header: header, ops: ops, sha256Hex: sha, rowCount: integrity.rowCount)
    }

    /// Only transient verify failures become alreadyExists retries; permanent causes must surface.
    private func verifyAfterAlreadyExists(
        remotePath: String,
        expectedSha: String,
        expectedRowCount: Int
    ) async throws {
        do {
            try await verifyCommitOnRemote(
                remotePath: remotePath,
                expectedSha: expectedSha,
                expectedRowCount: expectedRowCount
            )
        } catch WriteError.ioFailure(let underlying) {
            switch RemoteWriteClassifier.classifyVerifyFailure(underlying) {
            case .cancelled:
                throw CancellationError()
            case .transient:
                throw WriteError.alreadyExists
            case .permanent:
                throw WriteError.ioFailure(underlying)
            }
        }
    }

    private func verifyCommitOnRemote(
        remotePath: String,
        expectedSha: String,
        expectedRowCount: Int
    ) async throws {
        let verifyURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("commit-verify-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: verifyURL) }
        do {
            try await client.download(remotePath: remotePath, localURL: verifyURL)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                throw CancellationError()
            }
            throw WriteError.ioFailure(error)
        }
        let parsed: CommitFile
        do {
            parsed = try CommitLogReader.parse(localURL: verifyURL)
        } catch {
            throw WriteError.alreadyExists
        }
        if parsed.sha256Hex.lowercased() != expectedSha.lowercased() || parsed.rowCount != expectedRowCount {
            throw WriteError.alreadyExists
        }
    }

}

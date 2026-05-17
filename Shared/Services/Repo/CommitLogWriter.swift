import Foundation

/// Writes to `(writerID, seq)`-unique paths; only same-writer concurrent runs
/// can collide. On `.exclusive` backends we publish directly. On
/// `.overwritePossible` backends we stage + `moveIfAbsent` via the gate so a
/// peer commit at the same `(writerID, seq)` path can't be silently overwritten
/// by an `exists + upload` TOCTOU; gate-detected collisions surface as
/// `.alreadyExists` so the caller re-allocates seq.
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
            let result = try await client.atomicCreate(
                localURL: tempURL,
                remotePath: remotePath,
                respectTaskCancellation: respectTaskCancellation
            )
            switch result {
            case .created:
                break
            case .alreadyExists:
                throw WriteError.alreadyExists
            case .bestEffortRetry:
                // Defensive — `.exclusive` shouldn't return bestEffortRetry, but if it does
                // (transport timeout after write), SHA-verify so a peer collision still surfaces.
                try await verifyCommitOnRemote(
                    remotePath: remotePath,
                    expectedSha: sha,
                    expectedRowCount: integrity.rowCount
                )
            }
        case .overwritePossible:
            // SMB exists+upload would TOCTOU-overwrite a peer's commit and self-SHA-verify clean.
            // Stage + moveIfAbsent so a same-(writer,seq) collision fails closed → .alreadyExists.
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
                throw WriteError.ioFailure(error)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                throw WriteError.ioFailure(error)
            }
            switch result {
            case .created:
                break
            case .alreadyExists:
                throw WriteError.alreadyExists
            case .bestEffortRetry:
                // `.requireExclusiveMove` policy never returns bestEffortRetry from staging-fallback;
                // belt-and-braces re-verify so a future gate path change still fails closed.
                try await verifyCommitOnRemote(
                    remotePath: remotePath,
                    expectedSha: sha,
                    expectedRowCount: integrity.rowCount
                )
            }
        }

        return CommitFile(header: header, ops: ops, sha256Hex: sha, rowCount: integrity.rowCount)
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

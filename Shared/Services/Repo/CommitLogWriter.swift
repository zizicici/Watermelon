import Foundation

/// Writes to `(writerID, seq)`-unique paths; only same-writer concurrent runs
/// can collide. Bypasses `MetadataCreateGate.createWithStagingFallback` — the
/// gate's staging→move would overwrite a peer's commit on the rare collision;
/// instead, on `.bestEffortRetry` we SHA-verify and surface peer bytes as
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
            // Same-writer concurrent runs can both allocate the same seq —
            // SHA verify so a mismatch surfaces as alreadyExists for re-allocation.
            try await verifyCommitOnRemote(
                remotePath: remotePath,
                expectedSha: sha,
                expectedRowCount: integrity.rowCount
            )
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

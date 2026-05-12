import Foundation

actor CommitLogReader {
    enum ReadError: Error {
        case missingHeader
        case missingEnd
        case integrityMismatch(IntegrityResult)
        case decodeFailure(Error)
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    func listCommitFilenames() async throws -> [String] {
        let dir = RepoLayout.commitsDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            // Not-found error codes vary by backend; metadata probe is backend-agnostic.
            // If the probe also fails, propagate original (don't silently return empty).
            do {
                let metadata = try await client.metadata(path: dir)
                if metadata == nil { return [] }
            } catch {
                // metadata also failed → propagate original list error
            }
            throw error
        }
        return entries.compactMap { entry in
            guard !entry.isDirectory, entry.name.hasSuffix(".jsonl") else { return nil }
            return entry.name
        }
    }

    func read(filename: String) async throws -> CommitFile {
        let remotePath = RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory, RepoLayout.commitsDirectory, filename])
        return try await read(remotePath: remotePath)
    }

    func read(remotePath: String) async throws -> CommitFile {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("commit-fetch-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: remotePath, localURL: temp)
        return try Self.parse(localURL: temp)
    }

    static func parse(localURL: URL) throws -> CommitFile {
        let data = try Data(contentsOf: localURL)
        guard let raw = String(data: data, encoding: .utf8) else {
            throw ReadError.decodeFailure(CommitWireError.malformed("utf8"))
        }
        return try parse(text: raw)
    }

    static func parse(text: String) throws -> CommitFile {
        // Mid-stream blank lines are corruption; only writer's trailing \n is benign.
        // Strip per-line \r — some WebDAV gateways inject CRLF; bare \r corrupts SHA.
        var lines = text.split(separator: "\n", omittingEmptySubsequences: false).map { sub -> String in
            var s = String(sub)
            if s.hasSuffix("\r") { s.removeLast() }
            return s
        }
        while let last = lines.last, last.isEmpty { lines.removeLast() }
        guard !lines.isEmpty else {
            throw ReadError.missingHeader
        }
        if lines.contains(where: { $0.isEmpty }) {
            throw ReadError.decodeFailure(CommitWireError.malformed("blank line"))
        }
        let endRaw = lines.removeLast()

        var integrity = IntegrityAccumulator()
        var header: CommitHeader?
        var ops: [CommitOp] = []

        for line in lines {
            integrity.absorbLine(line)
            let row: CommitWireRow
            do { row = try CommitOpMapper.decodeLine(line) }
            catch { throw ReadError.decodeFailure(error) }
            switch row {
            case .header(let h):
                guard header == nil else {
                    throw ReadError.decodeFailure(CommitWireError.malformed("duplicate header"))
                }
                header = h
            case .op(let o):
                guard header != nil else {
                    throw ReadError.decodeFailure(CommitWireError.malformed("op before header"))
                }
                ops.append(o)
            case .end:
                throw ReadError.decodeFailure(CommitWireError.malformed("end before tail"))
            }
        }

        guard let header else {
            throw ReadError.missingHeader
        }
        let endRow: CommitWireRow
        do { endRow = try CommitOpMapper.decodeLine(endRaw) }
        catch { throw ReadError.decodeFailure(error) }
        guard case .end(let sha, let rowCount) = endRow else {
            throw ReadError.missingEnd
        }

        let result = verifyIntegrity(
            expectedSha256: sha,
            expectedRowCount: rowCount,
            actualSha256: integrity.finalize(),
            actualRowCount: integrity.rowCount
        )
        if result != .ok {
            throw ReadError.integrityMismatch(result)
        }

        return CommitFile(header: header, ops: ops, sha256Hex: sha, rowCount: rowCount)
    }
}

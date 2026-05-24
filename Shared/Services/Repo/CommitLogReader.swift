import Foundation

actor CommitLogReader {
    enum ReadError: Error {
        case missingHeader
        case missingEnd
        case integrityMismatch(IntegrityResult)
        case decodeFailure(Error)
        case notFound(filename: String)
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    func listCommitFilenames() async throws -> [String] {
        try await RepoJSONLDirectoryListing.listFilenames(
            client: client,
            directory: RepoLayout.commitsDirectoryPath(base: basePath)
        )
    }

    func read(filename: String) async throws -> CommitFile {
        let remotePath = RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory, RepoLayout.commitsDirectory, filename])
        return try await read(remotePath: remotePath, filename: filename)
    }

    func read(remotePath: String) async throws -> CommitFile {
        try await read(remotePath: remotePath, filename: (remotePath as NSString).lastPathComponent)
    }

    private func read(remotePath: String, filename: String) async throws -> CommitFile {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("commit-fetch-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await RepoJSONLDownload.download(
            client: client,
            remotePath: remotePath,
            to: temp,
            notFoundError: ReadError.notFound(filename: filename)
        )
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
        var lines = text.split(separator: "\n", omittingEmptySubsequences: false).map { sub -> String in
            let line = String(sub)
            let end = line.lastIndex(where: { $0 != "\r" }).map { line.index(after: $0) } ?? line.startIndex
            return String(line[..<end])
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

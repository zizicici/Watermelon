import Foundation

actor SnapshotReader {
    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    func listSnapshotFilenames() async throws -> [String] {
        try await RepoJSONLDirectoryListing.listFilenames(
            client: client,
            directory: RepoLayout.snapshotsDirectoryPath(base: basePath)
        )
    }

    func read(filename: String) async throws -> SnapshotFile {
        let path = RepoLayout.normalize(joining: [
            basePath, RepoLayout.watermelonDirectory, RepoLayout.snapshotsDirectory, filename
        ])
        return try await read(remotePath: path, filename: filename)
    }

    func read(remotePath: String) async throws -> SnapshotFile {
        try await read(remotePath: remotePath, filename: (remotePath as NSString).lastPathComponent)
    }

    private func read(remotePath: String, filename: String) async throws -> SnapshotFile {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("snapshot-fetch-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await RepoJSONLDownload.download(
            client: client,
            remotePath: remotePath,
            to: temp,
            notFoundError: RepoJSONLReadError.notFound(filename: filename)
        )
        let data = try Data(contentsOf: temp)
        guard let raw = String(data: data, encoding: .utf8) else {
            throw RepoJSONLReadError.decodeFailure(SnapshotWireError.malformed("utf8"))
        }
        return try Self.parse(text: raw)
    }

    static func parse(text: String) throws -> SnapshotFile {
        var lines = text.split(separator: "\n", omittingEmptySubsequences: false).map { sub -> String in
            let line = String(sub)
            let end = line.lastIndex(where: { $0 != "\r" }).map { line.index(after: $0) } ?? line.startIndex
            return String(line[..<end])
        }
        while let last = lines.last, last.isEmpty { lines.removeLast() }
        guard !lines.isEmpty else { throw RepoJSONLReadError.missingHeader }
        if lines.contains(where: { $0.isEmpty }) {
            throw RepoJSONLReadError.decodeFailure(SnapshotWireError.malformed("blank line"))
        }
        let endRaw = lines.removeLast()

        var integrity = IntegrityAccumulator()
        var header: SnapshotHeader?
        var assets: [SnapshotAssetRow] = []
        var resources: [SnapshotResourceRow] = []
        var assetResources: [SnapshotAssetResourceRow] = []
        var deletedKeys: [SnapshotDeletedKeyRow] = []

        for line in lines {
            integrity.absorbLine(line)
            let row: SnapshotRow
            do { row = try SnapshotRowMapper.decodeLine(line) }
            catch { throw RepoJSONLReadError.decodeFailure(error) }
            switch row {
            case .header(let h):
                guard header == nil else {
                    throw RepoJSONLReadError.decodeFailure(SnapshotWireError.malformed("duplicate header"))
                }
                header = h
            case .asset(let a): assets.append(a)
            case .resource(let r): resources.append(r)
            case .assetResource(let r): assetResources.append(r)
            case .deletedKey(let k): deletedKeys.append(k)
            case .end: throw RepoJSONLReadError.decodeFailure(SnapshotWireError.malformed("end before tail"))
            }
        }

        guard let header else { throw RepoJSONLReadError.missingHeader }
        let endRow: SnapshotRow
        do { endRow = try SnapshotRowMapper.decodeLine(endRaw) }
        catch { throw RepoJSONLReadError.decodeFailure(error) }
        guard case .end(let sha, let rowCount) = endRow else {
            throw RepoJSONLReadError.missingEnd
        }

        try integrity.verifyOrThrowJSONLMismatch(expectedSha256: sha, expectedRowCount: rowCount)

        return SnapshotFile(
            header: header,
            assets: assets,
            resources: resources,
            assetResources: assetResources,
            deletedKeys: deletedKeys,
            sha256Hex: sha,
            rowCount: rowCount
        )
    }
}

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

    enum AuthenticatedReadResult: Sendable {
        case full(SnapshotFile)
        /// Body unreadable. `authenticatedCoverage` is non-nil only when the filename digest authenticated
        /// the recovered header's covered; otherwise coverage is unknown and callers fail closed.
        case corruptBody(authenticatedCoverage: CoveredRanges?)
    }

    /// One download. A full valid body returns `.full` (unchanged trust). A body-corrupt download
    /// (`integrityMismatch` / `missingHeader` / `missingEnd` / `decodeFailure`) attempts authenticated
    /// header-only recovery from the bytes already downloaded — never reading body rows. `.notFound`
    /// still throws so the materializer's read-after-write race handling is preserved.
    func readAuthenticated(
        parsed: RepoLayout.ParsedSnapshotFilename,
        filename: String,
        expectedRepoID: String
    ) async throws -> AuthenticatedReadResult {
        let path = RepoLayout.normalize(joining: [
            basePath, RepoLayout.watermelonDirectory, RepoLayout.snapshotsDirectory, filename
        ])
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("snapshot-fetch-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await RepoJSONLDownload.download(
            client: client,
            remotePath: path,
            to: temp,
            notFoundError: RepoJSONLReadError.notFound(filename: filename)
        )
        let data = try Data(contentsOf: temp)
        guard let raw = String(data: data, encoding: .utf8) else {
            // Unreadable bytes — no header to authenticate.
            return .corruptBody(authenticatedCoverage: nil)
        }
        do {
            return .full(try Self.parse(text: raw))
        } catch let error as RepoJSONLReadError {
            switch error {
            case .notFound:
                throw error
            case .integrityMismatch, .missingHeader, .missingEnd, .decodeFailure:
                return .corruptBody(
                    authenticatedCoverage: Self.recoverAuthenticatedCoverage(
                        rawText: raw, parsed: parsed, expectedRepoID: expectedRepoID
                    )
                )
            }
        }
    }

    /// Authenticated header-only recovery: parse only the first header line, accept its covered solely
    /// when the filename digest exists and matches the digest recomputed from the recovered header's
    /// attestation, repoID, scope/month, writerID, and the filename's lamport/runIDPrefix. Any mismatch,
    /// missing attestation, or unparseable header fails closed to nil (coverage unknown).
    static func recoverAuthenticatedCoverage(
        rawText: String,
        parsed: RepoLayout.ParsedSnapshotFilename,
        expectedRepoID: String
    ) -> CoveredRanges? {
        guard let digest = parsed.digest else { return nil }
        guard let firstLine = firstNonEmptyLine(rawText) else { return nil }
        let row: SnapshotRow
        do { row = try SnapshotRowMapper.decodeLine(firstLine) } catch { return nil }
        guard case .header(let header) = row else { return nil }
        guard let attestation = header.coverageAttestation,
              attestation.version == SnapshotCoverageAttestation.currentVersion else { return nil }
        guard header.repoID == expectedRepoID,
              CommitHeader.parseMonthScope(header.scope) == parsed.month,
              header.writerID == parsed.writerID else { return nil }
        let recomputed = SnapshotCoverageDigest.digest(
            version: attestation.version,
            repoID: header.repoID,
            month: parsed.month,
            writerID: header.writerID,
            filenameLamport: parsed.lamport,
            filenameRunIDPrefix: parsed.runIDPrefix,
            covered: header.covered
        )
        guard recomputed == digest else { return nil }
        return header.covered
    }

    private static func firstNonEmptyLine(_ text: String) -> String? {
        for sub in text.split(separator: "\n", omittingEmptySubsequences: false) {
            let line = String(sub)
            let end = line.lastIndex(where: { $0 != "\r" }).map { line.index(after: $0) } ?? line.startIndex
            let trimmed = String(line[..<end])
            if trimmed.isEmpty { continue }
            return trimmed
        }
        return nil
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

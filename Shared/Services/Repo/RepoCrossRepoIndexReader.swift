import Foundation

actor RepoCrossRepoIndexReader {
    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    func listIndexFilenames() async throws -> [String] {
        try await RepoJSONLDirectoryListing.listFilenames(
            client: client,
            directory: RepoLayout.indexDirectoryPath(base: basePath)
        )
    }

    func read(filename: String) async throws -> RepoCrossRepoIndexFile {
        let path = RepoLayout.normalize(joining: [
            basePath, RepoLayout.watermelonDirectory, RepoLayout.indexDirectory, filename
        ])
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("crossrepo-index-fetch-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await RepoJSONLDownload.download(
            client: client,
            remotePath: path,
            to: temp,
            notFoundError: RepoJSONLReadError.notFound(filename: filename)
        )
        let data = try Data(contentsOf: temp)
        guard let raw = String(data: data, encoding: .utf8) else {
            throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("utf8"))
        }
        return try Self.parse(text: raw)
    }

    static func parse(text: String) throws -> RepoCrossRepoIndexFile {
        var lines = text.split(separator: "\n", omittingEmptySubsequences: false).map { sub -> String in
            let line = String(sub)
            let end = line.lastIndex(where: { $0 != "\r" }).map { line.index(after: $0) } ?? line.startIndex
            return String(line[..<end])
        }
        while let last = lines.last, last.isEmpty { lines.removeLast() }
        guard !lines.isEmpty else { throw RepoJSONLReadError.missingHeader }
        if lines.contains(where: { $0.isEmpty }) {
            throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("blank line"))
        }
        let endRaw = lines.removeLast()

        var integrity = IntegrityAccumulator()
        var header: RepoCrossRepoIndexHeader?
        var tail: RepoCrossRepoIndexTail?
        var monthSections: [RepoCrossRepoIndexMonthSection] = []
        var seenMonths: Set<LibraryMonthKey> = []
        var currentMonth: LibraryMonthKey?
        var currentAssets: [SnapshotAssetRow] = []
        var currentResources: [SnapshotResourceRow] = []
        var currentAssetResources: [SnapshotAssetResourceRow] = []
        var currentDeletedKeys: [SnapshotDeletedKeyRow] = []

        for line in lines {
            integrity.absorbLine(line)
            let row: RepoCrossRepoIndexRow
            do { row = try RepoCrossRepoIndexRowMapper.decodeLine(line) }
            catch { throw RepoJSONLReadError.decodeFailure(error) }
            switch row {
            case .header(let h):
                guard header == nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("duplicate header"))
                }
                header = h
            case .monthBegin(let m):
                guard header != nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("monthBegin before header"))
                }
                guard currentMonth == nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("monthBegin within open month section"))
                }
                guard seenMonths.insert(m).inserted else {
                    throw RepoJSONLReadError.decodeFailure(
                        RepoCrossRepoIndexWireError.malformed("duplicate monthBegin for \(m.text)")
                    )
                }
                currentMonth = m
                currentAssets = []
                currentResources = []
                currentAssetResources = []
                currentDeletedKeys = []
            case .asset(let a):
                guard currentMonth != nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("asset row outside month section"))
                }
                currentAssets.append(a)
            case .resource(let r):
                guard currentMonth != nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("resource row outside month section"))
                }
                currentResources.append(r)
            case .assetResource(let r):
                guard currentMonth != nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("asset_resource row outside month section"))
                }
                currentAssetResources.append(r)
            case .deletedKey(let k):
                guard currentMonth != nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("deleted_key row outside month section"))
                }
                currentDeletedKeys.append(k)
            case .monthEnd(let m):
                guard let openMonth = currentMonth else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("monthEnd without monthBegin"))
                }
                guard openMonth == m else {
                    throw RepoJSONLReadError.decodeFailure(
                        RepoCrossRepoIndexWireError.malformed("monthEnd \(m.text) does not match open monthBegin \(openMonth.text)")
                    )
                }
                monthSections.append(RepoCrossRepoIndexMonthSection(
                    month: openMonth,
                    assets: currentAssets,
                    resources: currentResources,
                    assetResources: currentAssetResources,
                    deletedKeys: currentDeletedKeys
                ))
                currentMonth = nil
            case .tail(let t):
                guard currentMonth == nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("tail inside open month section"))
                }
                guard tail == nil else {
                    throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("duplicate tail"))
                }
                tail = t
            case .end:
                throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("end before tail"))
            }
        }

        guard let header else { throw RepoJSONLReadError.missingHeader }
        guard currentMonth == nil else {
            throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("unterminated month section at end of body"))
        }
        guard let tail else {
            throw RepoJSONLReadError.decodeFailure(RepoCrossRepoIndexWireError.malformed("missing tail row"))
        }

        let endRow: RepoCrossRepoIndexRow
        do { endRow = try RepoCrossRepoIndexRowMapper.decodeLine(endRaw) }
        catch { throw RepoJSONLReadError.decodeFailure(error) }
        guard case .end(let sha, let rowCount) = endRow else {
            throw RepoJSONLReadError.missingEnd
        }

        try integrity.verifyOrThrowJSONLMismatch(expectedSha256: sha, expectedRowCount: rowCount)

        // Header's `coveredByMonth` must enumerate exactly the same months that the body
        // emits sections for. A header that claims to cover a month but omits its section
        // (or vice versa) is a fail-closed case: the materializer would otherwise serve a
        // header-only coverage claim that suppresses commit replay without an authoritative
        // body to derive state from.
        let headerMonths = Set(header.coveredByMonth.keys)
        let sectionMonths = Set(monthSections.map(\.month))
        guard headerMonths == sectionMonths else {
            throw RepoJSONLReadError.decodeFailure(
                RepoCrossRepoIndexWireError.malformed(
                    "header.coveredByMonth.keys=\(headerMonths.map(\.text).sorted()) does not match body section months=\(sectionMonths.map(\.text).sorted())"
                )
            )
        }

        return RepoCrossRepoIndexFile(
            header: header,
            monthSections: monthSections,
            tail: tail,
            sha256Hex: sha,
            rowCount: rowCount
        )
    }
}

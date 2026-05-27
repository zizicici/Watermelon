import Foundation
import os

private let crossRepoIndexWriterLog = Logger(subsystem: "com.zizicici.watermelon", category: "CrossRepoIndexWriter")

actor RepoCrossRepoIndexWriter {
    enum WriteError: Error {
        case alreadyExists
        case ioFailure(any Error)
        case encodingFailed(any Error)
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    @discardableResult
    func write(
        materialized: RepoMaterializer.MaterializeOutput,
        expectedRepoID: String,
        writerID: String,
        runID: String,
        lamport: UInt64,
        respectTaskCancellation: Bool
    ) async throws -> RepoCrossRepoIndexFile {
        // Defense-in-depth: ensure the index directory exists. RepoBootstrap.ensureSubdirectories
        // creates it on every open path, but creating again is idempotent and protects any caller
        // that bypasses bootstrap.
        do {
            try await client.createDirectory(path: RepoLayout.indexDirectoryPath(base: basePath))
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw WriteError.ioFailure(error)
        }

        let runIDPrefix = RepoLayout.runIDPrefix(runID)
        let header = RepoCrossRepoIndexHeader(
            schemaVersion: RepoCrossRepoIndexSchema.currentVersion,
            repoID: expectedRepoID,
            writerID: writerID,
            lamport: lamport,
            runIDPrefix: runIDPrefix,
            observedClock: materialized.state.observedClock,
            coveredByMonth: materialized.coveredByMonth
        )

        let monthSections = Self.buildMonthSections(from: materialized)

        let baselinesAtIndexTime: [LibraryMonthKey: RepoCrossRepoIndexAcceptedSnapshotInfo] =
            Dictionary(uniqueKeysWithValues: materialized.acceptedSnapshotBaselinesByMonth.map { month, info in
                (month, RepoCrossRepoIndexAcceptedSnapshotInfo(
                    filename: info.filename,
                    lamport: info.lamport,
                    writerID: info.writerID,
                    runIDPrefix: info.runIDPrefix,
                    covered: info.covered
                ))
            })

        let tail = RepoCrossRepoIndexTail(
            observedSeqByWriter: materialized.observedSeqByWriter,
            acceptedSnapshotBaselinesByMonthAtIndexTime: baselinesAtIndexTime,
            corruptedSnapshotMonthsAtIndexTime: materialized.corruptedSnapshotMonths
        )

        var lines: [String] = []
        var integrity = IntegrityAccumulator()

        do {
            let headerLine = try RepoCrossRepoIndexRowMapper.encodeHeaderLine(header)
            lines.append(headerLine)
            integrity.absorbLine(headerLine)

            for section in monthSections {
                let beginLine = try RepoCrossRepoIndexRowMapper.encodeMonthBeginLine(section.month)
                lines.append(beginLine)
                integrity.absorbLine(beginLine)

                for asset in section.assets {
                    let line = try RepoCrossRepoIndexRowMapper.encodeAssetLine(asset)
                    lines.append(line)
                    integrity.absorbLine(line)
                }
                for resource in section.resources {
                    let line = try RepoCrossRepoIndexRowMapper.encodeResourceLine(resource)
                    lines.append(line)
                    integrity.absorbLine(line)
                }
                for ar in section.assetResources {
                    let line = try RepoCrossRepoIndexRowMapper.encodeAssetResourceLine(ar)
                    lines.append(line)
                    integrity.absorbLine(line)
                }
                for d in section.deletedKeys {
                    let line = try RepoCrossRepoIndexRowMapper.encodeDeletedKeyLine(d)
                    lines.append(line)
                    integrity.absorbLine(line)
                }

                let endLine = try RepoCrossRepoIndexRowMapper.encodeMonthEndLine(section.month)
                lines.append(endLine)
                integrity.absorbLine(endLine)
            }

            let tailLine = try RepoCrossRepoIndexRowMapper.encodeTailLine(tail)
            lines.append(tailLine)
            integrity.absorbLine(tailLine)
        } catch {
            throw WriteError.encodingFailed(error)
        }

        let sha = integrity.finalize()
        let rowCount = integrity.rowCount
        let endLine: String
        do {
            endLine = try RepoCrossRepoIndexRowMapper.encodeEndLine(sha256Hex: sha, rowCount: rowCount)
        } catch {
            throw WriteError.encodingFailed(error)
        }
        lines.append(endLine)

        let body = lines.joined(separator: "\n") + "\n"
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("crossrepo-index-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        guard let data = body.data(using: .utf8) else {
            throw WriteError.encodingFailed(NSError(
                domain: "RepoCrossRepoIndexWriter",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "utf8 encoding failed"]
            ))
        }
        do {
            try data.write(to: tempURL, options: .atomic)
        } catch {
            throw WriteError.ioFailure(error)
        }

        let finalPath = RepoLayout.crossRepoIndexFilePath(
            base: basePath,
            lamport: lamport,
            writerID: writerID,
            runID: runID
        )

        let result: AtomicCreateResult
        do {
            result = try await MetadataCreateGate.createWithStagingFallback(
                client: client,
                localURL: tempURL,
                remotePath: finalPath,
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
            let outcome = await MetadataWriteVerifiers.crossRepoIndexAware(
                expectedSha: sha,
                expectedRowCount: rowCount
            ).verify(client: client, remotePath: finalPath, localURL: tempURL)
            switch outcome {
            case .matched:
                break
            case .deterministicMismatch, .transientFailure:
                throw WriteError.alreadyExists
            case .permanentFailure(let underlying):
                throw WriteError.ioFailure(underlying)
            case .cancelled:
                throw CancellationError()
            }
        }

        crossRepoIndexWriterLog.info("cross-repo index published lamport=\(lamport, privacy: .public) writer=\(writerID, privacy: .public) months=\(monthSections.count, privacy: .public)")
        return RepoCrossRepoIndexFile(
            header: header,
            monthSections: monthSections,
            tail: tail,
            sha256Hex: sha,
            rowCount: rowCount
        )
    }

    private static func buildMonthSections(from materialized: RepoMaterializer.MaterializeOutput) -> [RepoCrossRepoIndexMonthSection] {
        // Emit a section for every month the materialize observed (covered ranges OR populated state).
        // Empty months in coveredByMonth still need a section so the reader's covered claim matches the body.
        var months: Set<LibraryMonthKey> = Set(materialized.state.months.keys)
        for month in materialized.coveredByMonth.keys { months.insert(month) }
        let sortedMonths = months.sorted { lhs, rhs in
            if lhs.year != rhs.year { return lhs.year < rhs.year }
            return lhs.month < rhs.month
        }
        var sections: [RepoCrossRepoIndexMonthSection] = []
        sections.reserveCapacity(sortedMonths.count)
        for month in sortedMonths {
            let monthState = materialized.state.months[month] ?? .empty
            let assets = monthState.assets.values.sorted { $0.assetFingerprint.rawValue.lexicographicallyPrecedes($1.assetFingerprint.rawValue) }
            let resources = monthState.resources.values.sorted { $0.physicalRemotePath < $1.physicalRemotePath }
            let assetResources = monthState.assetResources.values.sorted { lhs, rhs in
                if lhs.assetFingerprint != rhs.assetFingerprint {
                    return lhs.assetFingerprint.rawValue.lexicographicallyPrecedes(rhs.assetFingerprint.rawValue)
                }
                if lhs.role != rhs.role { return lhs.role < rhs.role }
                return lhs.slot < rhs.slot
            }
            let deletedKeys = monthState.deletedAssetStamps
                .sorted { $0.key.rawValue.lexicographicallyPrecedes($1.key.rawValue) }
                .map { (fp, stamp) in
                    SnapshotDeletedKeyRow(
                        keyType: .asset,
                        keyValue: fp.rawValue.hexString,
                        stamp: stamp
                    )
                }
            sections.append(RepoCrossRepoIndexMonthSection(
                month: month,
                assets: assets,
                resources: resources,
                assetResources: assetResources,
                deletedKeys: deletedKeys
            ))
        }
        return sections
    }
}

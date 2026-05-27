import Foundation
import os

private let snapshotWriterLog = Logger(subsystem: "com.zizicici.watermelon", category: "SnapshotWriter")

actor SnapshotWriter {
    enum WriteError: Error {
        case verificationFailed(IntegrityResult)
        case ioFailure(Error)
        case finalizationFailed(Error)
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    @discardableResult
    func write(
        header: SnapshotHeader,
        assets: [SnapshotAssetRow],
        resources: [SnapshotResourceRow],
        assetResources: [SnapshotAssetResourceRow],
        deletedKeys: [SnapshotDeletedKeyRow],
        month: LibraryMonthKey,
        lamport: UInt64,
        runID: String,
        respectTaskCancellation: Bool
    ) async throws -> SnapshotFile {
        // header.covered is the caller's contract — it must match the materialized state.
        // LIST-based enrichment here would cover corrupt / unreplayed seqs.
        let sortedAssets = assets.sorted { $0.assetFingerprint.rawValue.lexicographicallyPrecedes($1.assetFingerprint.rawValue) }
        let sortedResources = resources.sorted { $0.physicalRemotePath < $1.physicalRemotePath }
        let sortedAssetResources = assetResources.sorted { lhs, rhs in
            if lhs.assetFingerprint != rhs.assetFingerprint {
                return lhs.assetFingerprint.rawValue.lexicographicallyPrecedes(rhs.assetFingerprint.rawValue)
            }
            if lhs.role != rhs.role { return lhs.role < rhs.role }
            return lhs.slot < rhs.slot
        }
        let sortedDeleted = deletedKeys.sorted { lhs, rhs in
            if lhs.keyType != rhs.keyType {
                return lhs.keyType.rawValue < rhs.keyType.rawValue
            }
            return lhs.keyValue < rhs.keyValue
        }

        var integrity = IntegrityAccumulator()
        var lines: [String] = []
        let headerLine = try SnapshotRowMapper.encodeHeaderLine(header)
        lines.append(headerLine)
        integrity.absorbLine(headerLine)

        for row in sortedAssets {
            let line = try SnapshotRowMapper.encodeAssetLine(row)
            lines.append(line)
            integrity.absorbLine(line)
        }
        for row in sortedResources {
            let line = try SnapshotRowMapper.encodeResourceLine(row)
            lines.append(line)
            integrity.absorbLine(line)
        }
        for row in sortedAssetResources {
            let line = try SnapshotRowMapper.encodeAssetResourceLine(row)
            lines.append(line)
            integrity.absorbLine(line)
        }
        for row in sortedDeleted {
            let line = try SnapshotRowMapper.encodeDeletedKeyLine(row)
            lines.append(line)
            integrity.absorbLine(line)
        }
        let sha = integrity.finalize()
        let endLine = try SnapshotRowMapper.encodeEndLine(sha256Hex: sha, rowCount: integrity.rowCount)
        lines.append(endLine)

        let body = lines.joined(separator: "\n") + "\n"
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("snapshot-\(UUID().uuidString).jsonl")
        defer { try? FileManager.default.removeItem(at: tempURL) }
        guard let data = body.data(using: .utf8) else {
            throw WriteError.ioFailure(NSError(domain: "SnapshotWriter", code: 2, userInfo: [NSLocalizedDescriptionKey: "utf8 encoding failed"]))
        }
        do {
            try data.write(to: tempURL, options: .atomic)
        } catch {
            throw WriteError.ioFailure(error)
        }

        let finalPath = RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: lamport,
            writerID: header.writerID,
            runID: runID
        )

        do {
            let result = try await MetadataCreateGate.createWithStagingFallback(
                client: client,
                localURL: tempURL,
                remotePath: finalPath,
                respectTaskCancellation: respectTaskCancellation
            )
            switch result {
            case .created:
                break
            case .alreadyExists:
                throw WriteError.finalizationFailed(NSError(domain: "SnapshotWriter", code: 1, userInfo: [
                    NSLocalizedDescriptionKey: "snapshot path already occupied at \(finalPath)"
                ]))
            case .bestEffortRetry:
                // Snapshot is derived from commit log (durable truth) — accepting an
                // unverified write is bounded: next materialize falls back to commit
                // replay if this file turns out unreadable. Log so ops can spot a
                // backend that's chronically failing post-move verify.
                snapshotWriterLog.warning("snapshot at \(finalPath, privacy: .public) wrote with unverified bytes; relying on commit log to rebuild on next read")
            }
        } catch is CancellationError {
            // Gate normalizes URL-shape cancellation to CancellationError; wrapping
            // it as finalizationFailed loses the user-stop signal for callers like
            // V1MigrationService that don't peel SnapshotWriter.WriteError.
            throw CancellationError()
        } catch let error as WriteError {
            throw error
        } catch {
            throw WriteError.finalizationFailed(error)
        }

        return SnapshotFile(
            header: header,
            assets: sortedAssets,
            resources: sortedResources,
            assetResources: sortedAssetResources,
            deletedKeys: sortedDeleted,
            sha256Hex: sha,
            rowCount: integrity.rowCount
        )
    }
}

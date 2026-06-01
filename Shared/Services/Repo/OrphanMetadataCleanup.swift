import Foundation
import os.log

private let cleanupLog = Logger(subsystem: "com.zizicici.watermelon", category: "OrphanMetadataCleanup")

enum OrphanMetadataCleanup {
    struct SweepDirectory: Sendable {
        let path: String
        let parseWriter: @Sendable (String) -> String?
    }

    static func standardSweepDirectories(basePath: String) -> [SweepDirectory] {
        [
            SweepDirectory(
                path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory]),
                parseWriter: { _ in nil }
            ),
            SweepDirectory(
                path: RepoLayout.commitsDirectoryPath(base: basePath),
                parseWriter: { RepoLayout.parseCommitFilename($0)?.writerID }
            ),
            SweepDirectory(
                path: RepoLayout.snapshotsDirectoryPath(base: basePath),
                parseWriter: { RepoLayout.parseSnapshotFilename($0)?.writerID }
            ),
            SweepDirectory(
                path: RepoLayout.identityDirectoryPath(base: basePath),
                parseWriter: { RepoLayout.parseWriterIDJSONFilename($0) }
            ),
            SweepDirectory(
                path: RepoLayout.migrationsDirectoryPath(base: basePath),
                parseWriter: { RepoLayout.parseMigrationMarkerFilename($0)?.writerID }
            )
        ]
    }

    static func sweepOwnStagings(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String,
        ageThresholdSeconds: TimeInterval = 3600,
        now: Date = Date()
    ) async throws -> Int {
        try Task.checkCancellation()
        let directories = standardSweepDirectories(basePath: basePath)
        var deleted = 0
        for dir in directories {
            let entries: [RemoteStorageEntry]
            do {
                entries = try await client.list(path: dir.path)
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                continue
            }
            for entry in entries {
                try Task.checkCancellation()
                guard !entry.isDirectory else { continue }
                guard let range = entry.name.range(of: ".staging-") else { continue }
                let originalName = String(entry.name[..<range.lowerBound])
                // Only clean staging attributed to this writer; skip non-self and unattributable
                guard let parsedWriter = dir.parseWriter(originalName), parsedWriter == writerID else {
                    continue
                }
                guard let mtime = entry.modificationDate else { continue }
                if now.timeIntervalSince(mtime) < ageThresholdSeconds { continue }
                let path = RepoLayout.normalize(joining: [dir.path, entry.name])
                do {
                    try await client.delete(path: path)
                    deleted += 1
                } catch {
                    if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                    cleanupLog.warning("own-staging orphan delete failed: \(path, privacy: .public) \(String(describing: error), privacy: .public)")
                }
            }
        }
        return deleted
    }
}

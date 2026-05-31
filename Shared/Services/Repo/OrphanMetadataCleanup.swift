import Foundation
import os.log

private let cleanupLog = Logger(subsystem: "com.zizicici.watermelon", category: "OrphanMetadataCleanup")

enum OrphanMetadataCleanup {
    struct SweepDirectory: Sendable {
        let path: String
        let parseWriter: @Sendable (String) -> String?
    }

    static func sweepSnapshots(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        activeWriters: Set<String>,
        ageThresholdSeconds: TimeInterval = 3600,
        now: Date = Date()
    ) async -> Int {
        await sweep(
            client: client,
            directories: [SweepDirectory(
                path: RepoLayout.snapshotsDirectoryPath(base: basePath),
                parseWriter: { RepoLayout.parseSnapshotFilename($0)?.writerID }
            )],
            activeWriters: activeWriters,
            ageThresholdSeconds: ageThresholdSeconds,
            now: now
        )
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

    static func sweep(
        client: any RemoteStorageClientProtocol,
        directories: [SweepDirectory],
        activeWriters: Set<String>,
        ageThresholdSeconds: TimeInterval,
        now: Date
    ) async -> Int {
        var deleted = 0
        for dir in directories {
            if Task.isCancelled { return deleted }
            let entries: [RemoteStorageEntry]
            do {
                entries = try await client.list(path: dir.path)
            } catch {
                cleanupLog.warning("sweep list failed: \(dir.path, privacy: .public) \(String(describing: error), privacy: .public)")
                continue
            }
            var stagingsSeen = 0
            var stagingsWithoutMtime = 0
            for entry in entries {
                if Task.isCancelled { return deleted }
                guard !entry.isDirectory else { continue }
                guard let range = entry.name.range(of: ".staging-") else { continue }
                let originalName = String(entry.name[..<range.lowerBound])
                // Only clean staging attributed to known writers; skip non-active and unattributable
                guard let writerID = dir.parseWriter(originalName), activeWriters.contains(writerID) else {
                    continue
                }
                stagingsSeen += 1
                guard let mtime = entry.modificationDate else {
                    stagingsWithoutMtime += 1
                    continue
                }
                if now.timeIntervalSince(mtime) < ageThresholdSeconds { continue }
                let path = RepoLayout.normalize(joining: [dir.path, entry.name])
                do {
                    try await client.delete(path: path)
                    deleted += 1
                } catch {
                    cleanupLog.warning("orphan delete failed: \(path, privacy: .public) \(String(describing: error), privacy: .public)")
                }
            }
            if stagingsSeen > 0 && stagingsWithoutMtime == stagingsSeen {
                cleanupLog.warning("staging files in \(dir.path, privacy: .public) all lack mtime; sweep disabled until backend exposes modificationDate")
            }
        }
        return deleted
    }
}

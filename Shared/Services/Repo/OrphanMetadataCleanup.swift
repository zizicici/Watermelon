import Foundation
import os.log

private let cleanupLog = Logger(subsystem: "com.zizicici.watermelon", category: "OrphanMetadataCleanup")

/// Cleans `.staging-<uuid>` orphans from aborted metadata writes. Assumes the
/// staging window is shorter than `ageThresholdSeconds` — revisit if any metadata
/// payload grows much larger than KB-scale JSONL.
enum OrphanMetadataCleanup {
    /// Directory + writer-extractor pairing. The extractor takes the "original"
    /// filename (everything before `.staging-<uuid>...`) and returns the writerID
    /// if recognizable. Used by the sweep gate to skip files belonging to active
    /// writers; nil falls through to mtime-only protection.
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

    /// Standard set of directories whose staging orphans we sweep on bootstrap.
    /// Liveness produces `.staging-<uuid>.tmp` from `tick`'s rename-on-fallback
    /// path; without coverage here those orphans accumulate forever.
    static func standardSweepDirectories(basePath: String) -> [SweepDirectory] {
        [
            SweepDirectory(
                path: RepoLayout.snapshotsDirectoryPath(base: basePath),
                parseWriter: { RepoLayout.parseSnapshotFilename($0)?.writerID }
            ),
            SweepDirectory(
                path: RepoLayout.livenessDirectoryPath(base: basePath),
                // Liveness file naming is `<writerID>.json`; the "original" before
                // `.staging-...` is that same name.
                parseWriter: { RepoLayout.parseLivenessFilename($0) }
            )
        ]
    }

    /// Sweep each directory with its own writer parser — without per-directory
    /// parsers, liveness files (different name shape than snapshots) lose the
    /// per-writer activeWriters gate and rely on mtime alone, which is weaker.
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
                // Persistent permission / transport errors used to be silently swallowed,
                // leaving orphans accumulating across runs. Log so ops can spot it.
                cleanupLog.warning("sweep list failed: \(dir.path, privacy: .public) \(String(describing: error), privacy: .public)")
                continue
            }
            var stagingsSeen = 0
            var stagingsWithoutMtime = 0
            for entry in entries {
                if Task.isCancelled { return deleted }
                guard !entry.isDirectory else { continue }
                guard let range = entry.name.range(of: ".staging-") else { continue }
                stagingsSeen += 1
                let originalName = String(entry.name[..<range.lowerBound])
                if let writerID = dir.parseWriter(originalName), activeWriters.contains(writerID) {
                    continue
                }
                // Fail-closed: nil mtime can't distinguish orphan from peer mid-write.
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

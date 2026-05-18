import Foundation
import os.log

private let cleanupLog = Logger(subsystem: "com.zizicici.watermelon", category: "OrphanMetadataCleanup")

/// Age gate assumes metadata staging writes stay KB-scale and short-lived.
enum OrphanMetadataCleanup {
    /// Writer extraction keeps active peers protected beyond mtime-only gating.
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
                path: RepoLayout.livenessDirectoryPath(base: basePath),
                // Liveness file naming is `<writerID>.json`; the "original" before
                // `.staging-...` is that same name.
                parseWriter: { RepoLayout.parseLivenessFilename($0) }
            ),
            SweepDirectory(
                path: RepoLayout.identityDirectoryPath(base: basePath),
                parseWriter: { RepoLayout.parseLivenessFilename($0) }
            ),
            SweepDirectory(
                path: RepoLayout.migrationsDirectoryPath(base: basePath),
                parseWriter: { RepoLayout.parseMigrationMarkerFilename($0)?.writerID }
            )
        ]
    }

    /// Sweeps our writerID before liveness starts because the general sweep must treat us as active.
    static func sweepOwnLivenessStagings(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String,
        ageThresholdSeconds: TimeInterval = 3600,
        now: Date = Date()
    ) async -> Int {
        let dir = RepoLayout.livenessDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            cleanupLog.warning("self-liveness sweep list failed: \(dir, privacy: .public) \(String(describing: error), privacy: .public)")
            return 0
        }
        var deleted = 0
        var stagingsSeen = 0
        var stagingsWithoutMtime = 0
        for entry in entries {
            if Task.isCancelled { return deleted }
            guard !entry.isDirectory else { continue }
            guard let range = entry.name.range(of: ".staging-") else { continue }
            let originalName = String(entry.name[..<range.lowerBound])
            guard RepoLayout.parseLivenessFilename(originalName) == writerID else { continue }
            stagingsSeen += 1
            guard let mtime = entry.modificationDate else {
                stagingsWithoutMtime += 1
                continue
            }
            if now.timeIntervalSince(mtime) < ageThresholdSeconds { continue }
            let path = RepoLayout.normalize(joining: [dir, entry.name])
            do {
                try await client.delete(path: path)
                deleted += 1
            } catch {
                cleanupLog.warning("self-liveness orphan delete failed: \(path, privacy: .public) \(String(describing: error), privacy: .public)")
            }
        }
        if stagingsSeen > 0 && stagingsWithoutMtime == stagingsSeen {
            cleanupLog.warning("own-liveness staging files in \(dir, privacy: .public) all lack mtime; sweep disabled until backend exposes modificationDate")
        }
        return deleted
    }

    /// Per-directory parsers keep each staging filename shape under the active-writer gate.
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

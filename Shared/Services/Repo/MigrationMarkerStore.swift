import Foundation
import os.log

private let migrationMarkerStoreLog = Logger(
    subsystem: "com.zizicici.watermelon",
    category: "MigrationMarkerStore"
)

/// Single owner of `.watermelon/migrations/*.json`. Raw/path APIs never download or
/// parse bytes; tolerant parse APIs swallow every failure mode (V1MigrationService
/// `requireValid: false` policy); `parseEntries` rethrows cancellation and non-not-found
/// IO and only skips parse failures with a warning (RemoteFormatCompatibility policy).
struct MigrationMarkerStore: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    // MARK: - Raw / path layer (no download, no parse)

    /// Fail-closed: list errors throw so a network blip can't be misread as "no migration".
    func migrationsDirectoryEntries() async throws -> [RemoteStorageEntry] {
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        do {
            return try await client.list(path: dir)
        } catch {
            if isStorageNotFoundError(error) { return [] }
            throw error
        }
    }

    func existsAny() async throws -> Bool {
        try await migrationsDirectoryEntries().contains { !$0.isDirectory && $0.name.hasSuffix(".json") }
    }

    /// Canonical path is seeded unconditionally so short non-UUID writerIDs
    /// (test fixtures: `"w"`, `"peer"`, `"test-writer"`) still resolve to the
    /// well-known location even though `parseMigrationMarkerFilename` rejects them.
    func pathsFor(writerID: String) async throws -> [String] {
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        var paths: Set<String> = [RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)]
        do {
            let entries = try await client.list(path: dir)
            for entry in entries where !entry.isDirectory {
                guard RepoLayout.parseMigrationMarkerFilename(entry.name)?.writerID == writerID else { continue }
                paths.insert(RemotePathBuilder.absolutePath(basePath: dir, remoteRelativePath: entry.name))
            }
        } catch {
            if !isStorageNotFoundError(error) { throw error }
        }
        return Array(paths)
    }

    /// Any metadata hit counts as presence — mirrors current `ownsMigrationMarker`,
    /// which does not filter on `isDirectory` so a half-written directory at the
    /// canonical path still blocks phase3 cleanup.
    func existsFor(writerID: String) async throws -> Bool {
        for path in try await pathsFor(writerID: writerID) {
            if try await metadataIfPresent(path: path) != nil {
                return true
            }
        }
        return false
    }

    func deleteAll(writerID: String) async throws {
        for path in try await pathsFor(writerID: writerID) {
            guard try await metadataIfPresent(path: path) != nil else { continue }
            try await client.delete(path: path)
        }
    }

    // MARK: - Tolerant parsed access (V1MigrationService policy)

    /// Pre-v:2 markers (no `phase` field) report `.phase1`; any unparseable marker
    /// still counts as `sawMarker` so phase1 idempotence holds.
    func currentPhase(writerID: String) async throws -> MigrationMarkerPhase? {
        let paths = try await pathsFor(writerID: writerID)
        var bestPhase: MigrationMarkerPhase?
        var sawMarker = false
        for path in paths {
            guard let meta = try await metadataIfPresent(path: path), !meta.isDirectory else { continue }
            sawMarker = true
            guard let info = await tolerantMarkerInfo(path: path, writerID: writerID) else {
                bestPhase = Self.maxPhase(bestPhase, .phase1)
                continue
            }
            bestPhase = Self.maxPhase(bestPhase, info.phase)
        }
        if let bestPhase { return bestPhase }
        return sawMarker ? .phase1 : nil
    }

    func startedAt(writerID: String) async throws -> Int64? {
        for path in try await pathsFor(writerID: writerID) {
            if let info = await tolerantMarkerInfo(path: path, writerID: writerID),
               let startedAtMs = info.startedAtMs {
                return startedAtMs
            }
        }
        return nil
    }

    // MARK: - Inspection parsed access (RemoteFormatCompatibility policy)

    /// `CancellationError` and non-not-found IO errors propagate so a transport
    /// glitch can't be misread as "no markers"; parse failures log + skip.
    func parseEntries(_ rawEntries: [RemoteStorageEntry]) async throws -> [ParsedMigrationMarker] {
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        var results: [ParsedMigrationMarker] = []
        for entry in rawEntries where !entry.isDirectory && entry.name.hasSuffix(".json") {
            let path = RemotePathBuilder.absolutePath(basePath: dir, remoteRelativePath: entry.name)
            let temp = FileManager.default.temporaryDirectory
                .appendingPathComponent("migration-marker-detect-\(UUID().uuidString).json")
            defer { try? FileManager.default.removeItem(at: temp) }
            let data: Data
            do {
                try await client.download(remotePath: path, localURL: temp)
                data = try Data(contentsOf: temp)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if !isStorageNotFoundError(error) { throw error }
                continue
            }
            do {
                let parsed = try MigrationMarker.parse(filename: entry.name, bytes: data)
                results.append(parsed)
            } catch {
                migrationMarkerStoreLog.warning(
                    "skipping migration marker at \(path, privacy: .public): \(String(describing: error), privacy: .public)"
                )
                continue
            }
        }
        return results
    }

    // MARK: - Writes

    func writePhase(writerID: String, phase: MigrationMarkerPhase, runID: String) async throws {
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        let nowMs = Int64(Date().timeIntervalSince1970 * 1000)
        let startedAtMs = try await startedAt(writerID: writerID) ?? nowMs
        let marker = ParsedMigrationMarker(
            writerID: writerID,
            phase: phase,
            runID: runID,
            startedAtMs: startedAtMs,
            lastStepMs: nowMs
        )
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("migration-marker-\(UUID().uuidString).json")
        try MigrationMarker.encode(marker).write(to: temp, options: .atomic)
        defer { try? FileManager.default.removeItem(at: temp) }

        if phase == .phase1 {
            let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
            if try await metadataIfPresent(path: canonical) == nil {
                let outcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
                    client: client,
                    localURL: temp,
                    remotePath: canonical,
                    respectTaskCancellation: false
                )
                if case .alreadyExists = outcome.result {
                    try await writeUnique(writerID: writerID, phase: phase, localURL: temp)
                    return
                }
                if !outcome.verifiedAgainstLocalContent {
                    try await verify(remotePath: canonical, localURL: temp)
                }
                return
            }
        }
        try await writeUnique(writerID: writerID, phase: phase, localURL: temp)
    }

    // MARK: - Internals

    private func writeUnique(writerID: String, phase: MigrationMarkerPhase, localURL: URL) async throws {
        for _ in 0..<4 {
            let markerID = UUID().uuidString.lowercased()
            let path = RepoLayout.migrationPhaseMarkerPath(
                base: basePath,
                writerID: writerID,
                phase: phase.rawValue,
                markerID: markerID
            )
            let outcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
                client: client,
                localURL: localURL,
                remotePath: path,
                respectTaskCancellation: false
            )
            if case .alreadyExists = outcome.result {
                continue
            }
            if !outcome.verifiedAgainstLocalContent {
                try await verify(remotePath: path, localURL: localURL)
            }
            return
        }
        throw NSError(domain: "V1MigrationService", code: -43, userInfo: [
            NSLocalizedDescriptionKey: "could not allocate unique migration marker for \(writerID)"
        ])
    }

    /// Share the metadata-write readback contract so eventually-consistent backends
    /// get the same `readAfterWriteGraceSeconds` budget here as commits/snapshots —
    /// a fixed loop reports stale reads as fatal marker failures even when the
    /// write itself landed.
    private func verify(remotePath: String, localURL: URL) async throws {
        do {
            if try await MetadataCreateGate.verifyMatchesLocalWithRetries(
                client: client,
                remotePath: remotePath,
                localURL: localURL
            ) {
                return
            }
            throw NSError(
                domain: "V1MigrationService",
                code: -41,
                userInfo: [NSLocalizedDescriptionKey: "migration marker bytes did not verify at \(remotePath)"]
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            if let nsError = error as NSError?, nsError.domain == "V1MigrationService", nsError.code == -41 {
                throw error
            }
            throw NSError(
                domain: "V1MigrationService",
                code: -41,
                userInfo: [
                    NSLocalizedDescriptionKey: "migration marker bytes did not verify at \(remotePath)",
                    NSUnderlyingErrorKey: error
                ]
            )
        }
    }

    /// V1 `requireValid: false` policy — every failure (transport, decode, parser,
    /// writerID mismatch, even `CancellationError`) collapses to `nil`.
    private func tolerantMarkerInfo(path: String, writerID: String) async -> ParsedMigrationMarker? {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("migration-marker-existing-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await client.download(remotePath: path, localURL: temp)
        } catch {
            return nil
        }
        let data: Data
        do {
            data = try Data(contentsOf: temp)
        } catch {
            return nil
        }
        let filename = (path as NSString).lastPathComponent
        do {
            let parsed = try MigrationMarker.parse(filename: filename, bytes: data)
            if parsed.writerID != writerID { return nil }
            return parsed
        } catch {
            return nil
        }
    }

    private func metadataIfPresent(path: String) async throws -> RemoteStorageEntry? {
        do {
            return try await client.metadata(path: path)
        } catch {
            if isStorageNotFoundError(error) { return nil }
            throw error
        }
    }

    private static func maxPhase(_ lhs: MigrationMarkerPhase?, _ rhs: MigrationMarkerPhase) -> MigrationMarkerPhase {
        guard let lhs else { return rhs }
        return lhs.rawValue >= rhs.rawValue ? lhs : rhs
    }
}

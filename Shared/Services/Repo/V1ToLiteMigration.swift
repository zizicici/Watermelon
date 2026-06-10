import Foundation
import GRDB

// Foreground V1→Lite migration. Relocates each legacy V1 month manifest
// (YYYY/MM/.watermelon_manifest.sqlite) into the Lite months directory
// (.watermelon/months/YYYY-MM.sqlite), then commits version.json as the single commit point. Resource
// bytes under YYYY/MM are never touched; old V1 manifests are left in place so an interrupted run still
// routes as .v1Migrate and resumes idempotently. Copying is publish-by-rename: bytes land on a temp
// path, get size/quick_check/byte validated, and only then move to the final month file.
struct V1ToLiteMigration: Sendable {
    // The copy could not be validated as a sound SQLite manifest; fail the whole run closed rather than
    // commit a version.json that claims a month is present when it is not.
    enum Failure: Error, Equatable {
        case monthManifestUnreadable(month: String)
    }

    let client: any RemoteStorageClientProtocol
    let basePath: String
    // Re-asserts the foreground write lease against the backend. Consulted before every month publish
    // and before the version.json commit; a false result fails the migration closed. nil ⇒ no gating.
    let assertOwnership: (@Sendable () async -> Bool)?

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        assertOwnership: (@Sendable () async -> Bool)? = nil
    ) {
        self.client = client
        self.basePath = basePath
        self.assertOwnership = assertOwnership
    }

    func run(createdAt: String, createdBy: String) async throws {
        try Task.checkCancellation()   // before enumeration
        for source in try await enumerateV1Months() {
            try await migrateMonth(source)
        }
        try await assertOwnedOrThrow()   // before the single commit point
        try Task.checkCancellation()   // before the version commit
        do {
            try await VersionManifestWriter(client: client, basePath: basePath)
                .commit(createdAt: createdAt, createdBy: createdBy)
        } catch {
            if Self.isCancellation(error) { throw error }   // cancellation must surface, never versionCommitFailed
            throw LiteRepoError.versionCommitFailed
        }
    }

    // MARK: - Per-month copy

    private struct V1Month {
        let month: LibraryMonthKey
        let manifestPath: String
        let size: Int64
    }

    private func migrateMonth(_ source: V1Month) async throws {
        try Task.checkCancellation()   // before each month
        let finalPath = RepoLayoutLite.monthPath(basePath: basePath, month: source.month)
        let finalMetadata = try await finalManifestMetadata(at: finalPath)
        let finalPresent = finalMetadata?.isDirectory == false

        // Idempotent rerun, cheap path: the migration copy is a verbatim byte copy of the source manifest,
        // so a non-directory final whose size matches the source size is proof of a prior publish — skip the
        // full download/quick_check.
        if finalPresent, source.size > 0, finalMetadata?.size == source.size {
            return
        }
        // Size diverged (or unknown): fall back to a full download/validate before deciding to re-copy.
        if finalPresent, try await downloadValidatedSqlite(at: finalPath) != nil {
            return
        }

        let monthsDirectory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        try await client.createDirectory(path: monthsDirectory)

        let sourceURL = Self.scratchURL()
        defer { Self.removeScratch(sourceURL) }
        do {
            try await client.download(remotePath: source.manifestPath, localURL: sourceURL)
        } catch {
            if Self.isCancellation(error) { throw error }   // cancellation must surface, never monthManifestUnreadable
            throw Failure.monthManifestUnreadable(month: source.month.text)
        }
        let sourceData = (try? Data(contentsOf: sourceURL)) ?? Data()

        // Avoid a dot-prefixed `.sqlite` temp name: some NAS AV/extension filters reject those.
        let tempPath = monthsDirectory + "/migrate_\(UUID().uuidString).tmp"
        do {
            try await client.upload(
                localURL: sourceURL,
                remotePath: tempPath,
                respectTaskCancellation: false,
                onProgress: nil
            )
            try Task.checkCancellation()   // between the non-cancellable upload and publish
            // Validate the copy (not the source) before it becomes authoritative: size matches, the
            // bytes survived the round-trip, and SQLite integrity passes.
            guard let validated = try await downloadValidatedSqlite(at: tempPath),
                  validated.size == Int64(sourceData.count),
                  validated.data == sourceData else {
                throw Failure.monthManifestUnreadable(month: source.month.text)
            }
            try await assertOwnedOrThrow()   // before publish
            if finalPresent {
                // Repairing an invalid existing final: drop it first so the rename lands on all backends.
                try? await client.delete(path: finalPath)
            }
            try await client.move(from: tempPath, to: finalPath)
        } catch {
            if (try? await client.exists(path: tempPath)) == true {
                try? await client.delete(path: tempPath)
            }
            throw error
        }
    }

    // MARK: - Validation

    private struct ValidatedSqlite {
        let data: Data
        let size: Int64
    }

    // Downloads a remote sqlite and returns its bytes only if non-empty and `PRAGMA quick_check` passes.
    // A genuine download failure (absent / transport fault) reads as "no valid sqlite here" (nil), but a
    // cancellation must surface so an interrupted validation is never misread as an invalid manifest.
    private func downloadValidatedSqlite(at remotePath: String) async throws -> ValidatedSqlite? {
        let localURL = Self.scratchURL()
        defer { Self.removeScratch(localURL) }
        do {
            try await client.download(remotePath: remotePath, localURL: localURL)
        } catch {
            if Self.isCancellation(error) { throw error }
            return nil
        }
        guard let data = try? Data(contentsOf: localURL), !data.isEmpty else { return nil }
        guard Self.quickCheckPasses(at: localURL) else { return nil }
        return ValidatedSqlite(data: data, size: Int64(data.count))
    }

    private static func isCancellation(_ error: Error) -> Bool {
        RemoteFaultLite.classify(error) == .cancelled
    }

    private static func quickCheckPasses(at url: URL) -> Bool {
        guard let queue = try? DatabaseQueue(path: url.path) else { return false }
        defer { try? queue.close() }
        let results = (try? queue.read { try String.fetchAll($0, sql: "PRAGMA quick_check") }) ?? []
        return results == ["ok"]
    }

    // MARK: - Ownership

    private func assertOwnedOrThrow() async throws {
        if let assertOwnership, await assertOwnership() == false {
            throw LiteRepoError.ownershipLost
        }
    }

    // MARK: - Remote probing

    // Final Lite month metadata, distinguishing genuine absence (`.notFound` ⇒ nil) from a probe that
    // could not be completed. A non-not-found metadata fault must surface, never read as absence:
    // silently dropping a candidate month would migrate a short list and then commit an incomplete
    // `.current`, hiding that month from the Lite path even though its V1 manifest still exists.
    private func finalManifestMetadata(at path: String) async throws -> RemoteStorageEntry? {
        do {
            return try await client.metadata(path: path)
        } catch {
            if RemoteFaultLite.classify(error) == .notFound { return nil }
            throw error
        }
    }

    // MARK: - V1 enumeration

    // Deterministic V1 scan via the shared scanner. A non-notFound fault surfaces so an interrupted scan
    // never reads as "no months to migrate"; the cancellation hook lets a long scan stop between probes.
    private func enumerateV1Months() async throws -> [V1Month] {
        try await V1ManifestScanner(client: client, basePath: basePath)
            .scan(checkCancellation: { try Task.checkCancellation() })
            .map { V1Month(month: $0.month, manifestPath: $0.manifestPath, size: $0.size) }
    }

    private static func scratchURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("v1lite_\(UUID().uuidString).sqlite")
    }

    private static func removeScratch(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }
}

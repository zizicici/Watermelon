import Foundation
import GRDB

// Foreground V1→Lite migration. Relocates each legacy V1 month manifest
// (YYYY/MM/.watermelon_manifest.sqlite) into the Lite months directory
// (.watermelon/months/YYYY-MM.sqlite), then commits version.json as the single commit point. Resource
// bytes under YYYY/MM are never touched; old V1 manifests are left in place so an interrupted run still
// routes as .v1Migrate and resumes idempotently. Copying is publish-by-rename: bytes land on a temp
// path, get schema/byte validated, and only then move to the final month file.
struct V1ToLiteMigration: Sendable {
    // The copy could not be validated as a sound SQLite manifest; fail the whole run closed rather than
    // commit a version.json that claims a month is present when it is not.
    enum Failure: Error, Equatable {
        case monthManifestUnreadable(month: String)
        case existingLiteManifestConflict(month: String)
        case sourceChangedDuringMigration
    }

    let client: any RemoteStorageClientProtocol
    let basePath: String
    // Re-asserts the foreground write lease against the backend. Consulted before every month publish
    // and before the version.json commit; a false result fails the migration closed. nil ⇒ no gating.
    let assertOwnership: MonthManifestOwnershipAssertion?

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        assertOwnership: MonthManifestOwnershipAssertion? = nil
    ) {
        self.client = client
        self.basePath = basePath
        self.assertOwnership = assertOwnership
    }

    func run(createdAt: String, createdBy: String) async throws {
        try Task.checkCancellation()   // before enumeration
        let sources = try await enumerateV1Months()
        for source in sources {
            try await migrateMonth(source)
        }
        try await validateV1SourcesStillMigrated(sources)
        try await assertOwnedOrThrow()   // before the single commit point
        try Task.checkCancellation()   // before the version commit
        do {
            try await VersionManifestWriter(
                client: client,
                basePath: basePath,
                assertOwnership: assertOwnership
            )
                .commit(createdAt: createdAt, createdBy: createdBy)
        } catch {
            if Self.isCancellation(error) { throw error }   // cancellation must surface, never versionCommitFailed
            if let liteError = error as? LiteRepoError,
               liteError == .ownershipLost || liteError == .leaseConfidenceLost {
                throw error
            }
            throw LiteRepoError.versionCommitFailed
        }
    }

    private func validateV1SourcesStillMigrated(_ sources: [V1Month]) async throws {
        try Task.checkCancellation()
        let expectedPaths = Set(sources.map(\.manifestPath))
        let currentSources = try await enumerateV1Months()
        guard Set(currentSources.map(\.manifestPath)) == expectedPaths else {
            throw Failure.sourceChangedDuringMigration
        }
        for source in currentSources {
            try Task.checkCancellation()
            let sourceURL = Self.scratchURL()
            defer { Self.removeScratch(sourceURL) }
            do {
                try await client.download(remotePath: source.manifestPath, localURL: sourceURL)
            } catch {
                if Self.isCancellation(error) { throw error }
                throw Failure.sourceChangedDuringMigration
            }
            guard let sourceData = try? Data(contentsOf: sourceURL),
                  !sourceData.isEmpty,
                  isLoadableMonthManifestFile(at: sourceURL, month: source.month) else {
                throw Failure.sourceChangedDuringMigration
            }
            let finalPath = RepoLayoutLite.monthPath(basePath: basePath, month: source.month)
            guard let final = try await downloadValidatedManifest(at: finalPath, month: source.month),
                  final.data == sourceData else {
                throw Failure.sourceChangedDuringMigration
            }
        }
    }

    // MARK: - Per-month copy

    private struct V1Month {
        let month: LibraryMonthKey
        let manifestPath: String
    }

    private func migrateMonth(_ source: V1Month) async throws {
        try Task.checkCancellation()   // before each month
        let finalPath = RepoLayoutLite.monthPath(basePath: basePath, month: source.month)
        let sourceURL = Self.scratchURL()
        defer { Self.removeScratch(sourceURL) }
        do {
            try await client.download(remotePath: source.manifestPath, localURL: sourceURL)
        } catch {
            if Self.isCancellation(error) { throw error }   // cancellation must surface, never monthManifestUnreadable
            throw Failure.monthManifestUnreadable(month: source.month.text)
        }
        let sourceData = (try? Data(contentsOf: sourceURL)) ?? Data()
        guard !sourceData.isEmpty, isLoadableMonthManifestFile(at: sourceURL, month: source.month) else {
            throw Failure.monthManifestUnreadable(month: source.month.text)
        }

        let finalMetadata = try await finalManifestMetadata(at: finalPath)
        var repairExistingFinal = finalMetadata?.isDirectory == true
        if finalMetadata?.isDirectory == false {
            switch try await remoteManifestState(at: finalPath, month: source.month) {
            case .valid(let validatedFinal):
                guard validatedFinal.data == sourceData else {
                    throw Failure.existingLiteManifestConflict(month: source.month.text)
                }
                return
            case .invalid:
                repairExistingFinal = true
            case .missing:
                repairExistingFinal = false
            }
        }

        let monthsDirectory = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        try await client.createDirectory(path: monthsDirectory)

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
            // bytes survived the round-trip, and the manifest schema loads.
            guard let validated = try await downloadValidatedManifest(at: tempPath, month: source.month),
                  validated.size == Int64(sourceData.count),
                  validated.data == sourceData else {
                throw Failure.monthManifestUnreadable(month: source.month.text)
            }
            try await assertOwnedOrThrow()   // before publish
            if repairExistingFinal {
                // Repairing an invalid existing final: drop it first so the rename lands on all backends.
                try await assertOwnedOrThrow()
                try? await client.delete(path: finalPath)
            }
            try await assertOwnedOrThrow()
            try await client.move(from: tempPath, to: finalPath)
            guard let final = try await downloadValidatedManifest(at: finalPath, month: source.month),
                  final.size == Int64(sourceData.count),
                  final.data == sourceData else {
                throw Failure.monthManifestUnreadable(month: source.month.text)
            }
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

    private enum RemoteManifestState {
        case missing
        case invalid
        case valid(ValidatedSqlite)
    }

    // Downloads a remote sqlite and returns its bytes only if non-empty and the month manifest schema loads.
    // A missing/invalid sqlite reads as nil; transport faults surface so a blinking backend is never
    // mistaken for a corrupt final that can be overwritten.
    private func downloadValidatedManifest(at remotePath: String, month: LibraryMonthKey) async throws -> ValidatedSqlite? {
        switch try await remoteManifestState(at: remotePath, month: month) {
        case .valid(let manifest):
            return manifest
        case .missing, .invalid:
            return nil
        }
    }

    private func remoteManifestState(at remotePath: String, month: LibraryMonthKey) async throws -> RemoteManifestState {
        let localURL = Self.scratchURL()
        defer { Self.removeScratch(localURL) }
        do {
            try await client.download(remotePath: remotePath, localURL: localURL)
        } catch {
            if Self.isCancellation(error) { throw error }
            if RemoteFaultLite.classify(error) == .notFound { return .missing }
            throw error
        }
        guard let data = try? Data(contentsOf: localURL), !data.isEmpty else { return .invalid }
        guard isLoadableMonthManifestFile(at: localURL, month: month) else { return .invalid }
        return .valid(ValidatedSqlite(data: data, size: Int64(data.count)))
    }

    private func isLoadableMonthManifestFile(at localURL: URL, month: LibraryMonthKey) -> Bool {
        let validationURL = Self.scratchURL()
        var validationQueue: DatabaseQueue?
        defer {
            MonthManifestStore.closeAndRemoveLocalManifest(at: validationURL, queue: validationQueue)
        }
        do {
            try FileManager.default.copyItem(at: localURL, to: validationURL)
            let prepared = try MonthManifestStore.prepareLocalManifest(
                localURL: validationURL,
                origin: .downloadedFromRemote
            )
            validationQueue = prepared.queue
            let store = MonthManifestStore(
                client: client,
                basePath: basePath,
                year: month.year,
                month: month.month,
                localManifestURL: validationURL,
                dbQueue: prepared.queue,
                remoteFilesByName: [:],
                dirty: prepared.requiresRemoteSync,
                layout: .lite
            )
            try store.reloadCache()
            return true
        } catch {
            return false
        }
    }

    private static func isCancellation(_ error: Error) -> Bool {
        RemoteFaultLite.classify(error) == .cancelled
    }

    // MARK: - Ownership

    private func assertOwnedOrThrow() async throws {
        try await assertOwnership?()
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
            .map { V1Month(month: $0.month, manifestPath: $0.manifestPath) }
    }

    private static func scratchURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("v1lite_\(UUID().uuidString).sqlite")
    }

    private static func removeScratch(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }
}

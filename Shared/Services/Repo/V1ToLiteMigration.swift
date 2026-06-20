import CryptoKit
import Foundation

// Foreground V1→Lite migration. Relocates each legacy V1 month manifest
// (YYYY/MM/.watermelon_manifest.sqlite) into the Lite months directory
// (.watermelon/months/YYYY-MM.sqlite), then commits version.json as the single commit point. Resource
// bytes under YYYY/MM are never touched during the copy/commit phase. Copying is publish-by-rename:
// bytes land on a temp path, move to the final month file, and get schema/byte validated before
// version.json commits.
struct V1ToLiteMigrationProgress: Equatable, Sendable {
    let phase: RepoUpgradePhase
    let current: Int
    let total: Int
}

struct V1ToLiteMigrationSource: Equatable, Sendable {
    let month: LibraryMonthKey
    let manifestPath: String
    let sha256Hex: String
}

struct V1ToLiteMigrationResult: Equatable, Sendable {
    let migratedSources: [V1ToLiteMigrationSource]
}

struct V1ToLiteMigration: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String
    // Re-asserts the foreground write lease against the backend. Consulted before every month publish
    // and before the version.json commit; a false result fails the migration closed. nil ⇒ no gating.
    let assertOwnership: MonthManifestOwnershipAssertion?
    let onProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        assertOwnership: MonthManifestOwnershipAssertion? = nil,
        onProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) {
        self.client = client
        self.basePath = basePath
        self.assertOwnership = assertOwnership
        self.onProgress = onProgress
    }

    @discardableResult
    func run(createdAt: String, createdBy: String) async throws -> V1ToLiteMigrationResult {
        try Task.checkCancellation()   // before enumeration
        let sources = try await enumerateV1Months()
        await onProgress?(V1ToLiteMigrationProgress(phase: .copying, current: 0, total: sources.count))
        for (index, source) in sources.enumerated() {
            try await migrateMonth(source)
            await onProgress?(V1ToLiteMigrationProgress(phase: .copying, current: index + 1, total: sources.count))
        }
        let migratedSources = try await validateV1SourcesStillMigrated(sources)
        try await assertOwnedOrThrow()   // before the single commit point
        try Task.checkCancellation()   // before the version commit
        await onProgress?(V1ToLiteMigrationProgress(phase: .finalizing, current: 0, total: 0))
        try await writeLegacyV1PruneMarker(migratedSources)
        try await assertOwnedOrThrow()
        try Task.checkCancellation()
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
               liteError.preservesOriginalDuringVersionCommit {
                throw error
            }
            throw LiteRepoError.versionCommitFailed
        }
        return V1ToLiteMigrationResult(migratedSources: migratedSources)
    }

    private func validateV1SourcesStillMigrated(_ sources: [V1Month]) async throws -> [V1ToLiteMigrationSource] {
        try Task.checkCancellation()
        let expectedPaths = Set(sources.map(\.manifestPath))
        let currentSources = try await enumerateV1Months()
        guard Set(currentSources.map(\.manifestPath)) == expectedPaths else {
            throw LiteRepoError.v1SourceChangedDuringMigration
        }
        var migratedSources: [V1ToLiteMigrationSource] = []
        await onProgress?(V1ToLiteMigrationProgress(phase: .validating, current: 0, total: currentSources.count))
        for (index, source) in currentSources.enumerated() {
            try Task.checkCancellation()
            let sourceURL = Self.scratchURL()
            defer { Self.removeScratch(sourceURL) }
            do {
                try await client.download(remotePath: source.manifestPath, localURL: sourceURL)
            } catch {
                if Self.isCancellation(error) { throw error }
                throw LiteRepoError.v1SourceChangedDuringMigration
            }
            guard let sourceData = try? Data(contentsOf: sourceURL),
                  !sourceData.isEmpty,
                  isLoadableMonthManifestFile(at: sourceURL, month: source.month) else {
                throw LiteRepoError.v1SourceChangedDuringMigration
            }
            let finalPath = RepoLayoutLite.monthPath(basePath: basePath, month: source.month)
            guard let final = try await downloadValidatedManifest(at: finalPath, month: source.month),
                  final.data == sourceData else {
                throw LiteRepoError.v1SourceChangedDuringMigration
            }
            migratedSources.append(
                V1ToLiteMigrationSource(
                    month: source.month,
                    manifestPath: source.manifestPath,
                    sha256Hex: Self.sha256Hex(sourceData)
                )
            )
            await onProgress?(V1ToLiteMigrationProgress(phase: .validating, current: index + 1, total: currentSources.count))
        }
        return migratedSources
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
            if Self.isCancellation(error) { throw error }   // cancellation must surface, never be localized
            throw LiteRepoError.v1MonthManifestUnreadable(month: source.month.text)
        }
        let sourceData = (try? Data(contentsOf: sourceURL)) ?? Data()
        guard !sourceData.isEmpty, isLoadableMonthManifestFile(at: sourceURL, month: source.month) else {
            throw LiteRepoError.v1MonthManifestUnreadable(month: source.month.text)
        }

        let finalMetadata = try await finalManifestMetadata(at: finalPath)
        if finalMetadata?.isDirectory == true {
            throw LiteRepoError.existingLiteManifestConflict(month: source.month.text)
        }
        var repairExistingFinal = false
        if finalMetadata?.isDirectory == false {
            switch try await remoteManifestState(at: finalPath, month: source.month) {
            case .valid(let validatedFinal):
                guard validatedFinal.data == sourceData else {
                    throw LiteRepoError.existingLiteManifestConflict(month: source.month.text)
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
        let tempPath = RepoLayoutLite.migrationPublishTempPath(basePath: basePath)
        do {
            try await client.upload(
                localURL: sourceURL,
                remotePath: tempPath,
                respectTaskCancellation: false,
                onProgress: nil
            )
            try Task.checkCancellation()   // between the non-cancellable upload and publish
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
                throw LiteRepoError.v1MonthManifestUnreadable(month: source.month.text)
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
        MonthManifestStore.validateMonthManifestFile(
            at: localURL,
            year: month.year,
            month: month.month,
            client: client,
            basePath: basePath,
            layout: .lite
        ) == .valid
    }

    private static func isCancellation(_ error: Error) -> Bool {
        RemoteFaultLite.classify(error) == .cancelled
    }

    // MARK: - Ownership

    private func assertOwnedOrThrow() async throws {
        try await assertOwnership?()
    }

    private func writeLegacyV1PruneMarker(_ sources: [V1ToLiteMigrationSource]) async throws {
        guard !sources.isEmpty else { return }
        let markerURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("legacy-v1-prune-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: markerURL) }
        do {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.sortedKeys]
            let markerData = try encoder.encode(LegacyV1PruneMarker(migratedSources: sources))
            try markerData.write(to: markerURL)
            try await assertOwnedOrThrow()
            try await client.upload(
                localURL: markerURL,
                remotePath: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath),
                respectTaskCancellation: false,
                onProgress: nil
            )
        } catch {
            if Self.isCancellation(error) { throw error }
            if let liteError = error as? LiteRepoError,
               liteError.preservesOriginalDuringVersionCommit {
                throw error
            }
            throw LiteRepoError.versionCommitFailed
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
        // Migration is the write/commit plane: a directory-valued candidate manifest must fail closed before
        // version.json commits, never be silently dropped from the migrated set.
        try await V1ManifestScanner(client: client, basePath: basePath)
            .scan(failOnDirectoryCandidate: true, checkCancellation: { try Task.checkCancellation() })
            .map { V1Month(month: $0.month, manifestPath: $0.manifestPath) }
    }

    private static func sha256Hex(_ data: Data) -> String {
        Data(SHA256.hash(data: data)).hexString
    }

    private static func scratchURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("v1lite_\(UUID().uuidString).sqlite")
    }

    private static func removeScratch(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }
}

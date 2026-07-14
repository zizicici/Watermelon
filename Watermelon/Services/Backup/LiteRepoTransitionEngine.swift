import CryptoKit
import Foundation

enum LiteRepoTransitionEngine {
    struct WritePlan<Session: RepoWriteSession>: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: Session
        let monthsListing: LiteMonthsListingSnapshot
    }

    enum PreparationOutcome<Session: RepoWriteSession> {
        case proceed(WritePlan<Session>)
        case skip
    }

    enum ReloadDisposition {
        case ready
        case requiresWrite(RepoFormatDecision)
    }

    private enum OwnedCleanupMode {
        case foreground
        case background
    }

    private enum OwnedAction {
        case useCurrent(OwnedCleanupMode)
        case commitVersion(OwnedCleanupMode)
        case migrate(runCleanup: Bool)
        case skipAfterReleaseAndUnwind
        case fail(LiteRepoError)
    }

    static func reloadDisposition(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> ReloadDisposition {
        let decision = try await classifyForRead(client: client, basePath: basePath)
        switch decision {
        case .current, .fresh:
            return .ready
        case .v1Migrate, .malformedVersion:
            return .requiresWrite(decision)
        case .damaged:
            throw LiteRepoError.repoDamaged
        case .unsupported(let minAppVersion):
            throw LiteRepoError.repoUnsupported(minAppVersion: minAppVersion)
        }
    }

    static func resolveReadLayout(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> MonthManifestStore.ManifestLayout {
        switch try await classify(client: client, basePath: basePath) {
        case .current, .fresh:
            return .lite
        case .v1Migrate:
            throw LiteRepoError.repoMaintenanceUnavailable
        case .damaged, .malformedVersion:
            throw LiteRepoError.repoDamaged
        case .unsupported(let minAppVersion):
            throw LiteRepoError.repoUnsupported(minAppVersion: minAppVersion)
        }
    }

    // `.skip` is produced only for background declines; foreground/maintenance declines throw.
    static func prepareWrite<Coordinator: RepoWriteCoordinator>(
        mode: RepoWritePreparationMode,
        client: any RemoteStorageClientProtocol,
        coordinator: Coordinator,
        basePath: String,
        writerID: String?,
        now: Date,
        initialDecision: RepoFormatDecision?,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?
    ) async throws -> PreparationOutcome<Coordinator.Session> {
        // Background turns every fail-closed condition into a safe skip; the other modes surface it.
        func decline(_ error: LiteRepoError) throws -> PreparationOutcome<Coordinator.Session> {
            if mode == .background {
                if Self.isCancellationFault(error) { throw CancellationError() }
                return .skip
            }
            throw error
        }

        // 1) Use the caller's initial decision or classify before acquiring write authority.
        let decision: RepoFormatDecision
        if let initialDecision {
            decision = initialDecision
        } else {
            do {
                decision = try await classify(client: client, basePath: basePath)
            } catch {
                if mode == .background {
                    if Self.isCancellationFault(error) { throw CancellationError() }
                    return .skip
                }
                throw error   // probe fault
            }
        }

        // 2) Decide whether this initial state is eligible to acquire write authority.
        switch decision {
        case .current:
            break                                    // every write mode may take ownership of a committed repo
        case .fresh:
            switch mode {
            case .foreground, .background: break     // initialize-eligible (commit under the lock)
            case .maintenance: throw LiteRepoError.repoMaintenanceUnavailable   // verify never initializes
            }
        case .v1Migrate:
            break                                    // migrate under the lock
        case .malformedVersion:
            break                                    // recover current version scratch under the lock
        case .damaged:
            return try decline(.repoDamaged)
        case .unsupported(let minAppVersion):
            return try decline(.repoUnsupported(minAppVersion: minAppVersion))
        }

        let monthsListing = LiteMonthsListingSnapshot()
        let authority: AcquiredRepoWriteAuthority<Coordinator.Session>
        do {
            switch try await coordinator.acquire(
                basePath: basePath,
                writerID: writerID,
                mode: mode,
                now: now
            ) {
            case .acquired(let acquired):
                authority = acquired
            case .declined(let error):
                return try decline(error)
            }
        } catch {
            if mode == .background, Self.isCancellationFault(error) {
                throw CancellationError()
            }
            if mode == .background { return .skip }
            throw error
        }

        do {
            return try await applySessionPolicy(
                mode: mode,
                client: client,
                basePath: basePath,
                writerID: authority.authorID,
                authority: authority,
                decision: decision,
                now: now,
                monthsListing: monthsListing,
                onMigrationProgress: onMigrationProgress
            )
        } catch {
            await attemptMarkerUnwind(client: client, basePath: basePath)
            throw error
        }
    }

    private static func applySessionPolicy<Session: RepoWriteSession>(
        mode: RepoWritePreparationMode,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        authority: AcquiredRepoWriteAuthority<Session>,
        decision: RepoFormatDecision,
        now: Date,
        monthsListing: LiteMonthsListingSnapshot,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?
    ) async throws -> PreparationOutcome<Session> {
        let probe: RepoFormatProbe
        do {
            probe = try await classifyDetailed(client: client, basePath: basePath)
        } catch {
            await authority.session.release()
            if mode == .background, Self.isCancellationFault(error) {
                throw CancellationError()
            }
            throw error
        }

        switch ownedAction(mode: mode, initialDecision: decision, ownedDecision: probe.decision) {
        case .useCurrent(let cleanupMode):
            await seedMonthsListing(
                monthsListing,
                basePath: basePath,
                entries: probe.monthsDirectoryEntries
            )
            let session = authority.session
            await session.begin()
            return .proceed(
                await runCleanup(
                    session: session,
                    cleanupMode: cleanupMode,
                    client: client,
                    basePath: basePath,
                    writerID: writerID,
                    now: now,
                    cleansCoordinationArtifacts: authority.cleansCoordinationArtifacts,
                    monthsListing: monthsListing,
                    repoDirectoryEntries: probe.repoDirectoryEntries
                )
            )

        case .commitVersion(let cleanupMode):
            let session = authority.session
            await session.begin()
            try await commitVersionWithOwnership(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: { try await session.assertControlWriteAllowed(now: Date()) },
                releaseOnFailure: { await session.release() }
            )
            await monthsListing.invalidate(basePath: basePath)
            return .proceed(
                await runCleanup(
                    session: session,
                    cleanupMode: cleanupMode,
                    client: client,
                    basePath: basePath,
                    writerID: writerID,
                    now: now,
                    cleansCoordinationArtifacts: authority.cleansCoordinationArtifacts,
                    monthsListing: monthsListing,
                    repoDirectoryEntries: nil
                )
            )

        case .migrate(let runCleanup):
            let session = authority.session
            await onMigrationProgress?(V1ToLiteMigrationProgress(phase: .copying, current: 0, total: 0))
            return .proceed(try await migrateV1WithSession(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                session: session,
                runCleanup: runCleanup,
                cleansCoordinationArtifacts: authority.cleansCoordinationArtifacts,
                monthsListing: monthsListing,
                onMigrationProgress: onMigrationProgress
            ))

        case .skipAfterReleaseAndUnwind:
            await authority.session.release()
            await attemptMarkerUnwind(client: client, basePath: basePath)
            return .skip

        case .fail(let error):
            await authority.session.release()
            throw error
        }
    }

    // MARK: - Helpers

    private static func ownedAction(
        mode: RepoWritePreparationMode,
        initialDecision: RepoFormatDecision,
        ownedDecision: RepoFormatDecision
    ) -> OwnedAction {
        switch mode {
        case .foreground:
            switch ownedDecision {
            case .current:
                return .useCurrent(.foreground)
            case .fresh where initialDecision == .fresh:
                return .commitVersion(.foreground)
            case .fresh:
                return .fail(.repoDamaged)
            case .v1Migrate:
                return .migrate(runCleanup: true)
            case .malformedVersion:
                return .commitVersion(.foreground)
            case .damaged:
                return .fail(.repoDamaged)
            case .unsupported(let minAppVersion):
                return .fail(.repoUnsupported(minAppVersion: minAppVersion))
            }

        case .background:
            switch ownedDecision {
            case .current:
                return .useCurrent(.background)
            case .v1Migrate:
                return .migrate(runCleanup: false)
            case .fresh where initialDecision == .fresh:
                return .commitVersion(.background)
            case .malformedVersion:
                return .commitVersion(.background)
            case .fresh, .damaged, .unsupported(_):
                return .skipAfterReleaseAndUnwind
            }

        case .maintenance:
            switch (initialDecision, ownedDecision) {
            case (_, .current):
                return .useCurrent(.foreground)
            case (_, .v1Migrate):
                return .migrate(runCleanup: true)
            case (.malformedVersion, .malformedVersion):
                return .commitVersion(.foreground)
            case (_, .unsupported(let minAppVersion)):
                // A committed-but-future/foreign format is unsupported, not damaged: preserve the upgrade
                // signal the foreground/initial/read routes already emit, never collapse it to repoDamaged.
                return .fail(.repoUnsupported(minAppVersion: minAppVersion))
            case (_, .fresh), (_, .malformedVersion), (_, .damaged):
                return .fail(.repoDamaged)
            }
        }
    }

    private static func runCleanup<Session: RepoWriteSession>(
        session: Session,
        cleanupMode: OwnedCleanupMode,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        cleansCoordinationArtifacts: Bool,
        monthsListing: LiteMonthsListingSnapshot,
        repoDirectoryEntries: [RemoteStorageEntry]?
    ) async -> WritePlan<Session> {
        switch cleanupMode {
        case .foreground:
            await runForegroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: RepoWriteGuard.controlWriteAssertion(session),
                cleansCoordinationArtifacts: cleansCoordinationArtifacts,
                monthsListing: monthsListing,
                repoDirectoryEntries: repoDirectoryEntries
            )
        case .background:
            await runBackgroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: RepoWriteGuard.controlWriteAssertion(session),
                cleansCoordinationArtifacts: cleansCoordinationArtifacts,
                monthsListing: monthsListing,
                repoDirectoryEntries: repoDirectoryEntries
            )
        }
        return WritePlan(layout: .lite, session: session, monthsListing: monthsListing)
    }

    private static func seedMonthsListing(
        _ monthsListing: LiteMonthsListingSnapshot,
        basePath: String,
        entries: [RemoteStorageEntry]?
    ) async {
        guard let entries else { return }
        await monthsListing.seed(basePath: basePath, entries: entries)
    }

    private static func migrateV1WithSession<Session: RepoWriteSession>(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        session: Session,
        runCleanup: Bool,
        cleansCoordinationArtifacts: Bool,
        monthsListing: LiteMonthsListingSnapshot,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?
    ) async throws -> WritePlan<Session> {
        await session.begin()
        do {
            let migrationResult = try await V1ToLiteMigration(
                client: client,
                basePath: basePath,
                assertOwnership: { try await session.assertControlWriteAllowed(now: Date()) },
                onProgress: onMigrationProgress
            ).run(createdAt: isoTimestamp(now), createdBy: writerID ?? "")
            let prunedAll = await pruneCommittedV1Manifests(
                client: client,
                basePath: basePath,
                sources: migrationResult.migratedSources,
                session: session,
                onProgress: onMigrationProgress
            )
            if prunedAll {
                await deleteLegacyV1PruneMarker(client: client, basePath: basePath, session: session)
            }
            await monthsListing.invalidate(basePath: basePath)
        } catch {
            await session.release()
            throw error
        }
        if runCleanup {
            await onMigrationProgress?(V1ToLiteMigrationProgress(phase: .cleaning, current: 0, total: 0))
            await runForegroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: RepoWriteGuard.controlWriteAssertion(session),
                cleansCoordinationArtifacts: cleansCoordinationArtifacts,
                monthsListing: monthsListing,
                repoDirectoryEntries: nil,
                pruneLegacyV1Manifests: false
            )
        }
        return WritePlan(layout: .lite, session: session, monthsListing: monthsListing)
    }

    private static func pruneCommittedV1Manifests<Session: RepoWriteSession>(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        sources: [V1ToLiteMigrationSource],
        session: Session,
        onProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async -> Bool {
        var prunedAll = true
        await onProgress?(V1ToLiteMigrationProgress(phase: .cleaning, current: 0, total: sources.count))
        for (index, source) in sources.enumerated() {
            let pruned = await pruneCommittedV1Manifest(
                client: client,
                basePath: basePath,
                source: source,
                session: session
            )
            prunedAll = prunedAll && pruned
            await onProgress?(V1ToLiteMigrationProgress(phase: .cleaning, current: index + 1, total: sources.count))
        }
        return prunedAll
    }

    private static func pruneCommittedV1Manifest<Session: RepoWriteSession>(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        source: V1ToLiteMigrationSource,
        session: Session
    ) async -> Bool {
        guard await isRemoteFile(client: client, path: source.manifestPath),
              source.manifestPath == MonthManifestStore.ManifestLayout.v1.manifestAbsolutePath(
                  basePath: basePath,
                  year: source.month.year,
                  month: source.month.month
              ),
              let sourceData = await downloadValidatedManifestBytes(
                  client: client,
                  basePath: basePath,
                  path: source.manifestPath,
                  month: source.month,
                  layout: .v1
              ),
              sha256Hex(sourceData) == source.sha256Hex else {
            return false
        }
        let litePath = RepoLayoutLite.monthPath(basePath: basePath, month: source.month)
        guard await isRemoteFile(client: client, path: litePath),
              let liteData = await downloadValidatedManifestBytes(
                  client: client,
                  basePath: basePath,
                  path: litePath,
                  month: source.month,
                  layout: .lite
              ),
              liteData == sourceData,
              await isRemoteFile(client: client, path: source.manifestPath) else {
            return false
        }
        do {
            try await session.assertControlWriteAllowed(now: Date())
            guard await isCurrentVersionManifest(client: client, basePath: basePath) else { return false }
            try await session.assertControlWriteAllowed(now: Date())
            guard await remoteManifestMatchesHash(
                client: client,
                basePath: basePath,
                path: source.manifestPath,
                month: source.month,
                layout: .v1,
                sha256Hex: source.sha256Hex
            ) else { return false }
            try await session.assertControlWriteAllowed(now: Date())
            try await client.delete(path: source.manifestPath)
            return true
        } catch {
            return false
        }
    }

    private static func deleteLegacyV1PruneMarker<Session: RepoWriteSession>(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        session: Session
    ) async {
        do {
            try await session.assertControlWriteAllowed(now: Date())
            try await client.delete(path: RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath))
        } catch {
            return
        }
    }

    private static func isRemoteFile(client: any RemoteStorageClientProtocol, path: String) async -> Bool {
        do {
            guard let entry = try await client.metadata(path: path) else { return false }
            return !entry.isDirectory
        } catch {
            return false
        }
    }

    private static func isCurrentVersionManifest(client: any RemoteStorageClientProtocol, basePath: String) async -> Bool {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-prune-version-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: localURL) }
        do {
            try await client.download(remotePath: RepoLayoutLite.versionPath(basePath: basePath), localURL: localURL)
            let data = try Data(contentsOf: localURL)
            return VersionManifestLite.compatibility(for: data) == .readableWritable
        } catch {
            return false
        }
    }

    private static func downloadValidatedManifestBytes(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        path: String,
        month: LibraryMonthKey,
        layout: MonthManifestStore.ManifestLayout
    ) async -> Data? {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1-prune-\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: localURL) }
        do {
            try await client.download(remotePath: path, localURL: localURL)
            let data = try Data(contentsOf: localURL)
            guard !data.isEmpty,
                  MonthManifestStore.validateMonthManifestFile(
                      at: localURL,
                      year: month.year,
                      month: month.month,
                      client: client,
                      basePath: basePath,
                      layout: layout
                  ) == .valid else { return nil }
            return data
        } catch {
            return nil
        }
    }

    private static func remoteManifestMatchesHash(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        path: String,
        month: LibraryMonthKey,
        layout: MonthManifestStore.ManifestLayout,
        sha256Hex expectedHash: String
    ) async -> Bool {
        guard let data = await downloadValidatedManifestBytes(
            client: client,
            basePath: basePath,
            path: path,
            month: month,
            layout: layout
        ) else {
            return false
        }
        return sha256Hex(data) == expectedHash
    }

    private static func sha256Hex(_ data: Data) -> String {
        Data(SHA256.hash(data: data)).hexString
    }

    // Foreground metadata cleanup is best-effort and never deletes data bytes.
    private static func runForegroundCleanup(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        assertOwnership: MonthManifestOwnershipAssertion?,
        cleansCoordinationArtifacts: Bool,
        monthsListing: LiteMonthsListingSnapshot?,
        repoDirectoryEntries: [RemoteStorageEntry]?,
        pruneLegacyV1Manifests: Bool = true
    ) async {
        await OrphanCleanupLite(
            client: client,
            basePath: basePath,
            currentWriterID: writerID,
            assertOwnership: assertOwnership,
            cleansCoordinationArtifacts: cleansCoordinationArtifacts,
            monthsListing: monthsListing,
            repoDirectoryEntries: repoDirectoryEntries,
            pruneLegacyV1Manifests: pruneLegacyV1Manifests
        )
            .run(mode: .foreground, now: now)
    }

    // Background skips version scratch while retaining month-scratch repair.
    private static func runBackgroundCleanup(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        assertOwnership: MonthManifestOwnershipAssertion?,
        cleansCoordinationArtifacts: Bool,
        monthsListing: LiteMonthsListingSnapshot?,
        repoDirectoryEntries: [RemoteStorageEntry]?,
        pruneLegacyV1Manifests: Bool = true
    ) async {
        await OrphanCleanupLite(
            client: client,
            basePath: basePath,
            currentWriterID: writerID,
            assertOwnership: assertOwnership,
            cleansCoordinationArtifacts: cleansCoordinationArtifacts,
            monthsListing: monthsListing,
            repoDirectoryEntries: repoDirectoryEntries,
            pruneLegacyV1Manifests: pruneLegacyV1Manifests
        )
            .run(mode: .background, now: now)
    }

    // Commits version.json only while write authority is proven.
    private static func commitVersionWithOwnership(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        assertOwnership: @escaping MonthManifestOwnershipAssertion,
        releaseOnFailure: @escaping @Sendable () async -> Void
    ) async throws {
        do {
            try await VersionManifestWriter(
                client: client,
                basePath: basePath,
                assertOwnership: assertOwnership
            )
                .commit(createdAt: isoTimestamp(now), createdBy: writerID ?? "")
        } catch {
            await releaseOnFailure()
            // Cancellation must surface as cancellation, never be relabeled as a repo commit failure.
            if RemoteFaultLite.classify(error) == .cancelled { throw error }
            if let liteError = error as? LiteRepoError,
               liteError.preservesOriginalDuringVersionCommit {
                throw error
            }
            throw LiteRepoError.versionCommitFailed
        }
    }

    // Leaves uncommitted marker directories in place; recursive directory delete can erase a new writer.
    private static func attemptMarkerUnwind(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async {
        // A committed (or probe-faulting) version.json must never be removed.
        do {
            if try await client.exists(path: RepoLayoutLite.versionPath(basePath: basePath)) { return }
        } catch {
            return   // cannot prove version absence → fail closed, delete nothing
        }
    }

    static func classify(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RepoFormatDecision {
        do {
            return try await RepoFormatRouter(client: client, basePath: basePath).classify()
        } catch let RepoFormatRouterError.probeFault(category, detail) {
            throw LiteRepoError.probeFault(category, detail: detail)
        }
    }

    // Read/connect-only fast classify (version.json first); write paths keep the full `classify`.
    static func classifyForRead(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RepoFormatDecision {
        do {
            return try await RepoFormatRouter(client: client, basePath: basePath).classifyForRead()
        } catch let RepoFormatRouterError.probeFault(category, detail) {
            throw LiteRepoError.probeFault(category, detail: detail)
        }
    }

    private static func classifyDetailed(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RepoFormatProbe {
        do {
            return try await RepoFormatRouter(client: client, basePath: basePath).classifyDetailed()
        } catch let RepoFormatRouterError.probeFault(category, detail) {
            throw LiteRepoError.probeFault(category, detail: detail)
        }
    }

    private static func isCancellationFault(_ error: Error) -> Bool {
        RemoteFaultLite.classify(error) == .cancelled
    }

    private static func isoTimestamp(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.string(from: date)
    }
}

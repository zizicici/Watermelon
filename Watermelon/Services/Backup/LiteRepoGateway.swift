import CryptoKit
import Foundation

// Maps RepoFormatRouter decisions onto Lite read/write plans and fails closed before data mutation.
enum LiteRepoGateway {
    struct WritePlan: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: RepoLeaseSession
        let monthsListing: LiteMonthsListingSnapshot
    }

    struct MaintenancePlan: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: RepoLeaseSession?
        let monthsListing: LiteMonthsListingSnapshot?
    }

    enum BackgroundOutcome {
        case proceed(WritePlan)
        case skip                       // declined safely: no Lite write performed
    }

    private enum WritePreparationOutcome {
        case proceed(WritePlan)
        case skip
    }

    // The write entry points share one classify/acquire/reclassify/apply pipeline.
    private enum WriteMode {
        case foreground
        case background
        case maintenance
    }

    private enum UnderLockCleanupMode {
        case foreground
        case background
    }

    private enum UnderLockAction {
        case useCurrent(UnderLockCleanupMode)
        case commitVersion(UnderLockCleanupMode)
        case migrate(runCleanup: Bool)
        case skipAfterReleaseAndUnwind
        case fail(LiteRepoError)
    }

    // MARK: - Public entry points

    static func prepareForegroundWrite(
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool = false,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        initialDecision: RepoFormatDecision? = nil,
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> WritePlan {
        let outcome = try await prepareWrite(
            mode: .foreground, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: initialDecision,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved,
            leaseDiagnosticLogger: leaseDiagnosticLogger,
            onMigrationProgress: onMigrationProgress
        )
        return try requireWritePlan(outcome)
    }

    // Background pre-lock declines skip; failures after mutation begins surface to the profile run.
    static func prepareBackgroundWrite(
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool = false,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> BackgroundOutcome {
        switch try await prepareWrite(
            mode: .background, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: nil,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved,
            leaseDiagnosticLogger: leaseDiagnosticLogger,
            onMigrationProgress: onMigrationProgress
        ) {
        case .proceed(let plan):
            return .proceed(plan)
        case .skip:
            return .skip
        }
    }

    // Verify owns reconcile/flush and may migrate/repair, but never initializes a fresh repo.
    static func prepareMaintenance(
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool = false,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        let outcome = try await prepareWrite(
            mode: .maintenance, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: nil,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved,
            leaseDiagnosticLogger: leaseDiagnosticLogger,
            onMigrationProgress: onMigrationProgress
        )
        let plan = try requireWritePlan(outcome)
        return MaintenancePlan(layout: plan.layout, session: plan.session, monthsListing: plan.monthsListing)
    }

    // Reload/connect is read-only when the repo is already current/fresh, but upgrades V1 or recovers
    // current version scratch through the same foreground write path as backup.
    static func prepareReload(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        makeLockClient: @escaping @Sendable () async throws -> LiteLockClientHandle,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        let decision = try await classifyForRead(client: client, basePath: basePath)
        switch decision {
        case .current, .fresh:
            return MaintenancePlan(layout: .lite, session: nil, monthsListing: LiteMonthsListingSnapshot())
        case .v1Migrate, .malformedVersion:
            var lock = try await makeLockClient()
            do {
                let plan = try await prepareForegroundWrite(
                    client: client,
                    lockClient: lock.client,
                    ownsLockClient: lock.ownsClient,
                    basePath: basePath,
                    writerID: writerID,
                    now: now,
                    initialDecision: decision,
                    reconnectLockClient: makeLockClient,
                    onForeignWriterObserved: onForeignWriterObserved,
                    leaseDiagnosticLogger: leaseDiagnosticLogger,
                    onMigrationProgress: onMigrationProgress
                )
                lock.transferToSession()
                return MaintenancePlan(layout: plan.layout, session: plan.session, monthsListing: plan.monthsListing)
            } catch {
                await lock.disconnectIfOwned()
                throw error
            }
        case .damaged:
            throw LiteRepoError.repoDamaged
        case .unsupported(let minAppVersion):
            throw LiteRepoError.repoUnsupported(minAppVersion: minAppVersion)
        }
    }

    // Pure-read path: layout only, never a lock. V1 is not a readable steady-state layout in this client;
    // callers that can write must migrate first.
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
            // Pure read fails closed when version recovery would need the write path.
            throw LiteRepoError.repoDamaged
        case .unsupported(let minAppVersion):
            throw LiteRepoError.repoUnsupported(minAppVersion: minAppVersion)
        }
    }

    // MARK: - Shared write-preparation state machine

    // `.skip` is produced only for background declines; foreground/maintenance declines throw.
    private static func prepareWrite(
        mode: WriteMode,
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool,
        basePath: String,
        writerID: String?,
        now: Date,
        initialDecision: RepoFormatDecision?,
        reconnectLockClient: ConnectedLockClientProvider?,
        onForeignWriterObserved: (@Sendable () async -> Void)?,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger?,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?
    ) async throws -> WritePreparationOutcome {
        // Background turns every fail-closed condition into a safe skip; the other modes surface it.
        func decline(_ error: LiteRepoError) throws -> WritePreparationOutcome {
            if mode == .background {
                if Self.isCancellationFault(error) { throw CancellationError() }
                return .skip
            }
            throw error
        }

        // 1) Use the caller's pre-lock decision or classify before acquiring.
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

        // 2) Pre-lock mode policy: is this initial state eligible to acquire a lock at all?
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

        // 3) Acquire the write lock in the mode-selected lock mode.
        let monthsListing = LiteMonthsListingSnapshot()
        guard let writerID,
              let lock = WriteLockService(
                  basePath: basePath, writerID: writerID, client: lockClient,
                  onForeignWriterObserved: onForeignWriterObserved
              ) else {
            return try decline(.writerIdentityUnavailable)
        }
        do {
            try await client.createDirectory(path: RemotePathBuilder.normalizePath(basePath))
        } catch {
            if mode == .background, Self.isCancellationFault(error) {
                throw CancellationError()
            }
            if mode == .background {
                return .skip
            }
            throw error
        }
        let lockMode: WriteLockService.Mode = mode == .background ? .background : .foreground
        switch await lock.acquire(mode: lockMode, now: now) {
        case .acquired:
            break
        case .blocked, .skipped:
            return try decline(.lockConflict)
        case .blockedByOwnLock(let block), .skippedByOwnLock(let block):
            return try decline(.ownLockConflict(block))
        case .faulted(let category):
            if mode == .background, category == .cancelled {
                throw CancellationError()
            }
            return try decline(.lockFault(category))
        }

        // A failure after acquire may leave an empty uncommitted `.watermelon`; unwind it best-effort.
        do {
            return try await applyUnderLockPolicy(
                mode: mode, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
                basePath: basePath, writerID: writerID,
                lock: lock, decision: decision, now: now,
                reconnectLockClient: reconnectLockClient,
                monthsListing: monthsListing,
                leaseDiagnosticLogger: leaseDiagnosticLogger,
                onMigrationProgress: onMigrationProgress
            )
        } catch {
            await attemptMarkerUnwind(client: client, basePath: basePath)
            throw error
        }
    }

    // Re-classifies under the lock and applies the mode's commit/migrate/proceed/reject policy.
    private static func applyUnderLockPolicy(
        mode: WriteMode,
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool,
        basePath: String,
        writerID: String,
        lock: WriteLockService,
        decision: RepoFormatDecision,
        now: Date,
        reconnectLockClient: ConnectedLockClientProvider?,
        monthsListing: LiteMonthsListingSnapshot,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger?,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?
    ) async throws -> WritePreparationOutcome {
        // 4) Re-classify under the lock; the under-lock decision is authoritative.
        let underLockProbe: RepoFormatProbe
        do {
            underLockProbe = try await classifyDetailed(client: client, basePath: basePath)
        } catch {
            await releaseShieldingCancellation(lock)
            if mode == .background, Self.isCancellationFault(error) {
                throw CancellationError()
            }
            throw error   // probe fault under the lock
        }
        let underLock = underLockProbe.decision

        let action = underLockAction(mode: mode, initialDecision: decision, underLock: underLock)
        switch action {
        case .useCurrent(let cleanupMode):
            await seedMonthsListing(
                monthsListing,
                basePath: basePath,
                entries: underLockProbe.monthsDirectoryEntries
            )
            let session = makeWriteSession(
                lock: lock,
                lockClient: lockClient,
                ownsLockClient: ownsLockClient,
                reconnectLockClient: reconnectLockClient,
                leaseDiagnosticLogger: leaseDiagnosticLogger
            )
            return .proceed(
                await startSessionAndRunCleanup(
                    session: session,
                    cleanupMode: cleanupMode,
                    client: client,
                    basePath: basePath,
                    writerID: writerID,
                    now: now,
                    monthsListing: monthsListing,
                    repoDirectoryEntries: underLockProbe.repoDirectoryEntries
                )
            )

        case .commitVersion(let cleanupMode):
            let session = try await commitVersionWithSessionUnderLock(
                client: client,
                basePath: basePath,
                writerID: writerID,
                lock: lock,
                lockClient: lockClient,
                ownsLockClient: ownsLockClient,
                reconnectLockClient: reconnectLockClient,
                leaseDiagnosticLogger: leaseDiagnosticLogger,
                now: now
            )
            await monthsListing.invalidate(basePath: basePath)
            return .proceed(
                await startSessionAndRunCleanup(
                    session: session,
                    cleanupMode: cleanupMode,
                    client: client,
                    basePath: basePath,
                    writerID: writerID,
                    now: now,
                    monthsListing: monthsListing,
                    repoDirectoryEntries: nil
                )
            )

        case .migrate(let runCleanup):
            await onMigrationProgress?(V1ToLiteMigrationProgress(phase: .copying, current: 0, total: 0))
            let plan = try await migrateV1UnderLock(
                client: client,
                basePath: basePath,
                writerID: writerID,
                lock: lock,
                lockClient: lockClient,
                ownsLockClient: ownsLockClient,
                now: now,
                reconnectLockClient: reconnectLockClient,
                leaseDiagnosticLogger: leaseDiagnosticLogger,
                runCleanup: runCleanup,
                monthsListing: monthsListing,
                onMigrationProgress: onMigrationProgress
            )
            return .proceed(plan)

        case .skipAfterReleaseAndUnwind:
            await releaseShieldingCancellation(lock)
            await attemptMarkerUnwind(client: client, basePath: basePath)
            return .skip

        case .fail(let error):
            await releaseShieldingCancellation(lock)
            throw error
        }
    }

    // MARK: - Helpers

    private static func underLockAction(
        mode: WriteMode,
        initialDecision: RepoFormatDecision,
        underLock: RepoFormatDecision
    ) -> UnderLockAction {
        switch mode {
        case .foreground:
            switch underLock {
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
            switch underLock {
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
            switch (initialDecision, underLock) {
            case (_, .current):
                return .useCurrent(.foreground)
            case (_, .v1Migrate):
                return .migrate(runCleanup: true)
            case (.malformedVersion, .malformedVersion):
                return .commitVersion(.foreground)
            case (_, .unsupported(let minAppVersion)):
                // A committed-but-future/foreign format is unsupported, not damaged: preserve the upgrade
                // signal the foreground/pre-lock/read routes already emit, never collapse it to repoDamaged.
                return .fail(.repoUnsupported(minAppVersion: minAppVersion))
            case (_, .fresh), (_, .malformedVersion), (_, .damaged):
                return .fail(.repoDamaged)
            }
        }
    }

    private static func startSessionAndRunCleanup(
        session: RepoLeaseSession,
        cleanupMode: UnderLockCleanupMode,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String,
        now: Date,
        monthsListing: LiteMonthsListingSnapshot,
        repoDirectoryEntries: [RemoteStorageEntry]?
    ) async -> WritePlan {
        await session.startRefresh()
        switch cleanupMode {
        case .foreground:
            await runForegroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: RepoLeaseGuard.leaseProvenAssertion(session),
                monthsListing: monthsListing,
                repoDirectoryEntries: repoDirectoryEntries
            )
        case .background:
            await runBackgroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: RepoLeaseGuard.leaseProvenAssertion(session),
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

    private static func makeWriteSession(
        lock: WriteLockService,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool,
        reconnectLockClient: ConnectedLockClientProvider?,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger?
    ) -> RepoLeaseSession {
        RepoLeaseSession(
            lock: lock,
            ownedLockClient: ownsLockClient ? lockClient : nil,
            reconnectLockClient: reconnectLockClient,
            diagnosticLogger: leaseDiagnosticLogger
        )
    }

    private static func commitVersionWithSessionUnderLock(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String,
        lock: WriteLockService,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool,
        reconnectLockClient: ConnectedLockClientProvider?,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger?,
        now: Date
    ) async throws -> RepoLeaseSession {
        let session = makeWriteSession(
            lock: lock,
            lockClient: lockClient,
            ownsLockClient: ownsLockClient,
            reconnectLockClient: reconnectLockClient,
            leaseDiagnosticLogger: leaseDiagnosticLogger
        )
        // Start the refresh task BEFORE the commit (mirrors migrateV1UnderLock). The read-only proof gating
        // the commit never renews the lease, so on a slow fresh-init / malformed-version recovery the refresh
        // task — the sole lock writer — keeps the own lock fresh and prevents a spurious expiry/fail-closed.
        // Idempotent with the later startSessionAndRunCleanup; released on failure below.
        await session.startRefresh()
        try await commitVersionUnderLock(
            client: client,
            basePath: basePath,
            writerID: writerID,
            now: now,
            assertOwnership: { try await session.assertLeaseProvenForWrite() },
            releaseOnFailure: { await session.stopAndRelease() }
        )
        return session
    }

    private static func migrateV1UnderLock(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String,
        lock: WriteLockService,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool,
        now: Date,
        reconnectLockClient: ConnectedLockClientProvider?,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger?,
        runCleanup: Bool,
        monthsListing: LiteMonthsListingSnapshot,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)?
    ) async throws -> WritePlan {
        let session = makeWriteSession(
            lock: lock,
            lockClient: lockClient,
            ownsLockClient: ownsLockClient,
            reconnectLockClient: reconnectLockClient,
            leaseDiagnosticLogger: leaseDiagnosticLogger
        )
        await session.startRefresh()
        do {
            let migrationResult = try await V1ToLiteMigration(
                client: client,
                basePath: basePath,
                assertOwnership: { try await session.assertLeaseProvenForWrite() },
                onProgress: onMigrationProgress
            ).run(createdAt: isoTimestamp(now), createdBy: writerID)
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
            await session.stopAndRelease()
            throw error
        }
        if runCleanup {
            await onMigrationProgress?(V1ToLiteMigrationProgress(phase: .cleaning, current: 0, total: 0))
            await runForegroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: RepoLeaseGuard.leaseProvenAssertion(session),
                monthsListing: monthsListing,
                repoDirectoryEntries: nil,
                pruneLegacyV1Manifests: false
            )
        }
        return WritePlan(layout: .lite, session: session, monthsListing: monthsListing)
    }

    private static func pruneCommittedV1Manifests(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        sources: [V1ToLiteMigrationSource],
        session: RepoLeaseSession,
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

    private static func pruneCommittedV1Manifest(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        source: V1ToLiteMigrationSource,
        session: RepoLeaseSession
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
            try await session.assertLeaseProvenForWrite()
            guard await isCurrentVersionManifest(client: client, basePath: basePath) else { return false }
            try await session.assertLeaseProvenForWrite()
            guard await remoteManifestMatchesHash(
                client: client,
                basePath: basePath,
                path: source.manifestPath,
                month: source.month,
                layout: .v1,
                sha256Hex: source.sha256Hex
            ) else { return false }
            try await session.assertLeaseProvenForWrite()
            try await client.delete(path: source.manifestPath)
            return true
        } catch {
            return false
        }
    }

    private static func deleteLegacyV1PruneMarker(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        session: RepoLeaseSession
    ) async {
        do {
            try await session.assertLeaseProvenForWrite()
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
        monthsListing: LiteMonthsListingSnapshot?,
        repoDirectoryEntries: [RemoteStorageEntry]?,
        pruneLegacyV1Manifests: Bool = true
    ) async {
        await OrphanCleanupLite(
            client: client,
            basePath: basePath,
            currentWriterID: writerID,
            assertOwnership: assertOwnership,
            monthsListing: monthsListing,
            repoDirectoryEntries: repoDirectoryEntries,
            pruneLegacyV1Manifests: pruneLegacyV1Manifests
        )
            .run(mode: .foreground, now: now)
    }

    // Release must finish even under cancellation — a cancelled delete would leak the remote lock and block every device until expiry.
    private static func releaseShieldingCancellation(_ lock: WriteLockService) async {
        await Task { await lock.release() }.value
    }

    // Background skips version scratch but still repairs month scratch and clears expired/invalid locks.
    private static func runBackgroundCleanup(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        assertOwnership: MonthManifestOwnershipAssertion?,
        monthsListing: LiteMonthsListingSnapshot?,
        repoDirectoryEntries: [RemoteStorageEntry]?,
        pruneLegacyV1Manifests: Bool = true
    ) async {
        await OrphanCleanupLite(
            client: client,
            basePath: basePath,
            currentWriterID: writerID,
            assertOwnership: assertOwnership,
            monthsListing: monthsListing,
            repoDirectoryEntries: repoDirectoryEntries,
            pruneLegacyV1Manifests: pruneLegacyV1Manifests
        )
            .run(mode: .background, now: now)
    }

    // Commits version.json under an already-held lock.
    private static func commitVersionUnderLock(
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

    private static func classify(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RepoFormatDecision {
        do {
            return try await RepoFormatRouter(client: client, basePath: basePath).classify()
        } catch let RepoFormatRouterError.probeFault(category) {
            throw LiteRepoError.probeFault(category)
        }
    }

    // Read/connect-only fast classify (version.json first); write paths keep the full `classify`.
    private static func classifyForRead(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RepoFormatDecision {
        do {
            return try await RepoFormatRouter(client: client, basePath: basePath).classifyForRead()
        } catch let RepoFormatRouterError.probeFault(category) {
            throw LiteRepoError.probeFault(category)
        }
    }

    private static func classifyDetailed(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> RepoFormatProbe {
        do {
            return try await RepoFormatRouter(client: client, basePath: basePath).classifyDetailed()
        } catch let RepoFormatRouterError.probeFault(category) {
            throw LiteRepoError.probeFault(category)
        }
    }

    private static func requireWritePlan(_ outcome: WritePreparationOutcome) throws -> WritePlan {
        switch outcome {
        case .proceed(let plan):
            return plan
        case .skip:
            throw LiteRepoError.repoDamaged
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

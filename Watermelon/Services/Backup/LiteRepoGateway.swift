import Foundation

// Maps RepoFormatRouter decisions onto Lite read/write plans and fails closed before data mutation.
enum LiteRepoGateway {
    struct WritePlan: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: LiteWriteSession
        let monthsListing: LiteMonthsListingSnapshot
    }

    struct MaintenancePlan: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: LiteWriteSession?
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
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> WritePlan {
        let outcome = try await prepareWrite(
            mode: .foreground, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: initialDecision,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved
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
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> BackgroundOutcome {
        switch try await prepareWrite(
            mode: .background, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: nil,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved
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
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        let outcome = try await prepareWrite(
            mode: .maintenance, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: nil,
            reconnectLockClient: reconnectLockClient,
            onForeignWriterObserved: onForeignWriterObserved
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
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        let decision = try await classify(client: client, basePath: basePath)
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
                    onForeignWriterObserved: onForeignWriterObserved
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
        onForeignWriterObserved: (@Sendable () async -> Void)?
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
        case .blockedByOwnLock, .skippedByOwnLock:
            return try decline(.ownLockConflict)
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
                monthsListing: monthsListing
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
        monthsListing: LiteMonthsListingSnapshot
    ) async throws -> WritePreparationOutcome {
        // 4) Re-classify under the lock; the under-lock decision is authoritative.
        let underLock: RepoFormatDecision
        do {
            underLock = try await classify(client: client, basePath: basePath)
        } catch {
            await releaseShieldingCancellation(lock)
            if mode == .background, Self.isCancellationFault(error) {
                throw CancellationError()
            }
            throw error   // probe fault under the lock
        }

        let action = underLockAction(mode: mode, initialDecision: decision, underLock: underLock)
        switch action {
        case .useCurrent(let cleanupMode):
            let session = makeWriteSession(
                lock: lock,
                lockClient: lockClient,
                ownsLockClient: ownsLockClient,
                reconnectLockClient: reconnectLockClient
            )
            return .proceed(
                await startSessionAndRunCleanup(
                    session: session,
                    cleanupMode: cleanupMode,
                    client: client,
                    basePath: basePath,
                    writerID: writerID,
                    now: now,
                    monthsListing: monthsListing
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
                    monthsListing: monthsListing
                )
            )

        case .migrate(let runCleanup):
            let plan = try await migrateV1UnderLock(
                client: client,
                basePath: basePath,
                writerID: writerID,
                lock: lock,
                lockClient: lockClient,
                ownsLockClient: ownsLockClient,
                now: now,
                reconnectLockClient: reconnectLockClient,
                runCleanup: runCleanup,
                monthsListing: monthsListing
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
            case (_, .fresh), (_, .malformedVersion), (_, .damaged), (_, .unsupported(_)):
                return .fail(.repoDamaged)
            }
        }
    }

    private static func startSessionAndRunCleanup(
        session: LiteWriteSession,
        cleanupMode: UnderLockCleanupMode,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String,
        now: Date,
        monthsListing: LiteMonthsListingSnapshot
    ) async -> WritePlan {
        await session.startRefresh()
        switch cleanupMode {
        case .foreground:
            await runForegroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: LiteWriteGuard.ownershipAssertion(session),
                assertLeaseConfidence: { try await session.assertLeaseConfidence() },
                monthsListing: monthsListing
            )
        case .background:
            await runBackgroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: LiteWriteGuard.ownershipAssertion(session),
                assertLeaseConfidence: { try await session.assertLeaseConfidence() },
                monthsListing: monthsListing
            )
        }
        return WritePlan(layout: .lite, session: session, monthsListing: monthsListing)
    }

    private static func makeWriteSession(
        lock: WriteLockService,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool,
        reconnectLockClient: ConnectedLockClientProvider?
    ) -> LiteWriteSession {
        LiteWriteSession(
            lock: lock,
            ownedLockClient: ownsLockClient ? lockClient : nil,
            reconnectLockClient: reconnectLockClient
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
        now: Date
    ) async throws -> LiteWriteSession {
        let session = makeWriteSession(
            lock: lock,
            lockClient: lockClient,
            ownsLockClient: ownsLockClient,
            reconnectLockClient: reconnectLockClient
        )
        try await commitVersionUnderLock(
            client: client,
            basePath: basePath,
            writerID: writerID,
            now: now,
            assertOwnership: { try await session.assertStillOwnedForWrite() },
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
        runCleanup: Bool,
        monthsListing: LiteMonthsListingSnapshot
    ) async throws -> WritePlan {
        let session = makeWriteSession(
            lock: lock,
            lockClient: lockClient,
            ownsLockClient: ownsLockClient,
            reconnectLockClient: reconnectLockClient
        )
        await session.startRefresh()
        do {
            try await V1ToLiteMigration(
                client: client,
                basePath: basePath,
                assertOwnership: { try await session.assertStillOwnedForWrite() }
            ).run(createdAt: isoTimestamp(now), createdBy: writerID)
            await monthsListing.invalidate(basePath: basePath)
        } catch {
            await session.stopAndRelease()
            throw error
        }
        if runCleanup {
            await runForegroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: LiteWriteGuard.ownershipAssertion(session),
                assertLeaseConfidence: { try await session.assertLeaseConfidence() },
                monthsListing: monthsListing
            )
        }
        return WritePlan(layout: .lite, session: session, monthsListing: monthsListing)
    }

    // Foreground metadata cleanup is best-effort and never deletes data bytes.
    private static func runForegroundCleanup(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        assertOwnership: MonthManifestOwnershipAssertion?,
        assertLeaseConfidence: MonthManifestOwnershipAssertion?,
        monthsListing: LiteMonthsListingSnapshot?
    ) async {
        await OrphanCleanupLite(
            client: client,
            basePath: basePath,
            currentWriterID: writerID,
            assertOwnership: assertOwnership,
            assertLeaseConfidence: assertLeaseConfidence,
            monthsListing: monthsListing
        )
            .run(mode: .foreground, now: now)
    }

    // Release must finish even under cancellation — a cancelled delete would leak the remote lock and block every device until expiry.
    private static func releaseShieldingCancellation(_ lock: WriteLockService) async {
        await Task { await lock.release() }.value
    }

    // Background runs only month-scratch repair (no version scratch / lock cleanup) so a recoverable `.bak` is restored before a fresh manifest can be minted over it.
    private static func runBackgroundCleanup(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        assertOwnership: MonthManifestOwnershipAssertion?,
        assertLeaseConfidence: MonthManifestOwnershipAssertion?,
        monthsListing: LiteMonthsListingSnapshot?
    ) async {
        await OrphanCleanupLite(
            client: client,
            basePath: basePath,
            currentWriterID: writerID,
            assertOwnership: assertOwnership,
            assertLeaseConfidence: assertLeaseConfidence,
            monthsListing: monthsListing
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

    // Removes only a conclusively empty, uncommitted `.watermelon` marker.
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

        let repoDir = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: repoDir)
        } catch {
            return   // already gone, or a fault we won't act on
        }
        // Any child other than a `locks` directory is real/unknown content (version, months, dev marker,
        // leftover temp): never delete.
        let locksName = RepoLayoutLite.locksDirectoryName
        for entry in entries where !(entry.isDirectory && entry.name == locksName) {
            return
        }
        if entries.contains(where: { $0.isDirectory && $0.name == locksName }) {
            let locksDir = RepoLayoutLite.locksDirectoryPath(basePath: basePath)
            do {
                guard try await client.list(path: locksDir).isEmpty else { return }   // a live/holdover lock
            } catch {
                return
            }
            try? await client.delete(path: locksDir)
        }
        try? await client.delete(path: repoDir)
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

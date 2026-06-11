import Foundation

// Maps RepoFormatRouter decisions onto Lite read/write plans and fails closed before data mutation.
enum LiteRepoGateway {
    struct WritePlan {
        let layout: MonthManifestStore.ManifestLayout
        let session: LiteWriteSession
    }

    struct MaintenancePlan {
        let layout: MonthManifestStore.ManifestLayout
        let session: LiteWriteSession?
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

    // MARK: - Public entry points

    static func prepareForegroundWrite(
        client: any RemoteStorageClientProtocol,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool = false,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        initialDecision: RepoFormatDecision? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> WritePlan {
        let outcome = try await prepareWrite(
            mode: .foreground, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: initialDecision,
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
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> BackgroundOutcome {
        switch try await prepareWrite(
            mode: .background, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: nil,
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
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        let outcome = try await prepareWrite(
            mode: .maintenance, client: client, lockClient: lockClient, ownsLockClient: ownsLockClient,
            basePath: basePath, writerID: writerID, now: now,
            initialDecision: nil,
            onForeignWriterObserved: onForeignWriterObserved
        )
        let plan = try requireWritePlan(outcome)
        return MaintenancePlan(layout: plan.layout, session: plan.session)
    }

    // Reload/connect is read-only when the repo is already current/fresh, but upgrades V1 or repairs a
    // malformed marker through the same foreground write path as backup.
    static func prepareReload(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        makeLockClient: @Sendable () async throws -> LiteLockClientHandle,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        let decision = try await classify(client: client, basePath: basePath)
        switch decision {
        case .current, .fresh:
            return MaintenancePlan(layout: .lite, session: nil)
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
                    onForeignWriterObserved: onForeignWriterObserved
                )
                lock.transferToSession()
                return MaintenancePlan(layout: plan.layout, session: plan.session)
            } catch {
                await lock.disconnectIfOwned()
                throw error
            }
        case .damaged:
            throw LiteRepoError.repoDamaged
        case .unsupported:
            throw LiteRepoError.repoUnsupported
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
            // Pure read fails closed for a malformed version marker: repair is a write-path concern.
            throw LiteRepoError.repoDamaged
        case .unsupported:
            throw LiteRepoError.repoUnsupported
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
            break                                    // repair under the lock
        case .damaged:
            return try decline(.repoDamaged)
        case .unsupported:
            return try decline(.repoUnsupported)
        }

        // 3) Acquire the write lock in the mode-selected lock mode.
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
                lock: lock, decision: decision, now: now
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
        now: Date
    ) async throws -> WritePreparationOutcome {
        // 4) Re-classify under the lock; the under-lock decision is authoritative.
        let underLock: RepoFormatDecision
        do {
            underLock = try await classify(client: client, basePath: basePath)
        } catch {
            await lock.release()
            if mode == .background, Self.isCancellationFault(error) {
                throw CancellationError()
            }
            throw error   // probe fault under the lock
        }

        // 5) Apply the mode policy, then start the lease.
        switch mode {
        case .foreground:
            switch underLock {
            case .v1Migrate:
                let plan = try await migrateV1UnderLock(
                    client: client,
                    basePath: basePath,
                    writerID: writerID,
                    lock: lock,
                    lockClient: lockClient,
                    ownsLockClient: ownsLockClient,
                    now: now,
                    runCleanup: true
                )
                return .proceed(plan)
            case .current:
                break   // committed (initially or by another writer) — no version commit needed
            case .fresh:
                // `.current`/`.v1` seen outside the lock but `.fresh` inside means the repo was emptied:
                // fail closed rather than initialize over a just-deleted repo.
                guard decision == .fresh else {
                    await lock.release()
                    throw LiteRepoError.repoDamaged
                }
                try await commitVersionUnderLock(
                    client: client, basePath: basePath, writerID: writerID, lock: lock, now: now
                )
            case .malformedVersion:
                // Owned repair of an existing (malformed) version marker, re-probed under the lock.
                try await commitVersionUnderLock(
                    client: client, basePath: basePath, writerID: writerID, lock: lock, now: now
                )
            case .damaged:
                await lock.release()
                throw LiteRepoError.repoDamaged
            case .unsupported:
                await lock.release()
                throw LiteRepoError.repoUnsupported
            }
            let session = LiteWriteSession(lock: lock, ownedLockClient: ownsLockClient ? lockClient : nil)
            await session.startRefresh()
            await runForegroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: LiteWriteGuard.ownershipAssertion(session)
            )
            return .proceed(WritePlan(layout: .lite, session: session))

        case .background:
            switch underLock {
            case .current:
                break
            case .v1Migrate:
                let plan = try await migrateV1UnderLock(
                    client: client,
                    basePath: basePath,
                    writerID: writerID,
                    lock: lock,
                    lockClient: lockClient,
                    ownsLockClient: ownsLockClient,
                    now: now,
                    runCleanup: false
                )
                return .proceed(plan)
            case .fresh where decision == .fresh:
                try await commitVersionUnderLock(
                    client: client, basePath: basePath, writerID: writerID, lock: lock, now: now
                )
            case .malformedVersion:
                try await commitVersionUnderLock(
                    client: client, basePath: basePath, writerID: writerID, lock: lock, now: now
                )
            default:
                await lock.release()
                await attemptMarkerUnwind(client: client, basePath: basePath)
                return .skip
            }
            let session = LiteWriteSession(lock: lock, ownedLockClient: ownsLockClient ? lockClient : nil)
            await session.startRefresh()
            return .proceed(WritePlan(layout: .lite, session: session))   // background runs no cleanup

        case .maintenance:
            switch (decision, underLock) {
            case (_, .current):
                break   // still committed, or repaired to current by another writer between the two reads
            case (_, .v1Migrate):
                let plan = try await migrateV1UnderLock(
                    client: client,
                    basePath: basePath,
                    writerID: writerID,
                    lock: lock,
                    lockClient: lockClient,
                    ownsLockClient: ownsLockClient,
                    now: now,
                    runCleanup: true
                )
                return .proceed(plan)
            case (.malformedVersion, .malformedVersion):
                try await commitVersionUnderLock(
                    client: client, basePath: basePath, writerID: writerID, lock: lock, now: now
                )
            default:
                // A `.current` that drifted off-current, or a malformed marker that no longer reads
                // malformed under the lock: fail closed rather than guess.
                await lock.release()
                throw LiteRepoError.repoDamaged
            }
            let session = LiteWriteSession(lock: lock, ownedLockClient: ownsLockClient ? lockClient : nil)
            await session.startRefresh()
            await runForegroundCleanup(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                assertOwnership: LiteWriteGuard.ownershipAssertion(session)
            )
            return .proceed(WritePlan(layout: .lite, session: session))
        }
    }

    // MARK: - Helpers

    private static func migrateV1UnderLock(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String,
        lock: WriteLockService,
        lockClient: any RemoteStorageClientProtocol,
        ownsLockClient: Bool,
        now: Date,
        runCleanup: Bool
    ) async throws -> WritePlan {
        let session = LiteWriteSession(lock: lock, ownedLockClient: ownsLockClient ? lockClient : nil)
        await session.startRefresh()
        do {
            try await V1ToLiteMigration(
                client: client,
                basePath: basePath,
                assertOwnership: { try await session.assertStillOwnedForWrite() }
            ).run(createdAt: isoTimestamp(now), createdBy: writerID)
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
                assertOwnership: LiteWriteGuard.ownershipAssertion(session)
            )
        }
        return WritePlan(layout: .lite, session: session)
    }

    // Foreground metadata cleanup is best-effort and never deletes data bytes.
    private static func runForegroundCleanup(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        assertOwnership: MonthManifestOwnershipAssertion?
    ) async {
        await OrphanCleanupLite(
            client: client,
            basePath: basePath,
            currentWriterID: writerID,
            assertOwnership: assertOwnership
        )
            .run(mode: .foreground, now: now)
    }

    // Commits version.json under an already-held lock.
    private static func commitVersionUnderLock(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        lock: WriteLockService,
        now: Date
    ) async throws {
        let assertOwnership: MonthManifestOwnershipAssertion = {
            switch await lock.assertStillOwned(mode: .foreground, now: Date()) {
            case .stillOwned:
                return
            case .lost:
                throw LiteRepoError.ownershipLost
            case .faulted:
                throw LiteRepoError.leaseConfidenceLost
            }
        }
        do {
            try await VersionManifestWriter(
                client: client,
                basePath: basePath,
                assertOwnership: assertOwnership
            )
                .commit(createdAt: isoTimestamp(now), createdBy: writerID ?? "")
        } catch {
            await lock.release()
            // Cancellation must surface as cancellation, never be relabeled as a repo commit failure.
            if RemoteFaultLite.classify(error) == .cancelled { throw error }
            if let liteError = error as? LiteRepoError,
               liteError == .ownershipLost || liteError == .leaseConfidenceLost {
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
        if RemoteFaultLite.classify(error) == .cancelled { return true }
        guard let lite = error as? LiteRepoError else { return false }
        switch lite {
        case .probeFault(.cancelled), .lockFault(.cancelled):
            return true
        default:
            return false
        }
    }

    private static func isoTimestamp(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.string(from: date)
    }
}

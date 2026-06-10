import Foundation

// Routing seam for the dormant Repo V2 (Lite) cutover. Classifies the remote with RepoFormatRouter and
// maps the decision onto a write/read plan, failing closed before any data mutation. Kept free of
// PHAsset / pipeline state so the full decision matrix is unit-testable against a fake remote.
enum LiteRepoGateway {
    struct ForegroundPlan {
        let layout: MonthManifestStore.ManifestLayout
        let session: LiteWriteSession
    }

    struct MaintenancePlan {
        let layout: MonthManifestStore.ManifestLayout
        let session: LiteWriteSession?   // nil when the remote is a lock-free V1 tree
    }

    enum BackgroundOutcome {
        case proceed(ForegroundPlan)
        case skip                       // declined safely: no Lite write performed
    }

    // Foreground write path. `.fresh`/`.current` → acquire lock (and, for `.fresh`, commit version.json);
    // `.v1Migrate` → acquire lock then migrate the legacy tree into Lite; everything else throws before
    // mutating data. All routes that proceed start the refresh loop and continue on `.lite`.
    static func prepareForegroundWrite(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> ForegroundPlan {
        let decision = try await classify(client: client, basePath: basePath)
        switch decision {
        case .fresh, .current, .v1Migrate, .malformedVersion:
            break
        case .damaged:
            throw LiteRepoError.repoDamaged
        case .unsupported:
            throw LiteRepoError.repoUnsupported
        }

        let lock = try await acquireForegroundLock(
            client: client, basePath: basePath, writerID: writerID, now: now,
            onForeignWriterObserved: onForeignWriterObserved
        )

        // Re-classify under the foreground lock to catch TOCTOU state changes (e.g. V1 data
        // appeared after the initial probe, or another Lite writer committed version.json).
        let underLockDecision: RepoFormatDecision
        do {
            underLockDecision = try await classify(client: client, basePath: basePath)
        } catch {
            await lock.release()
            throw error
        }

        switch underLockDecision {
        case .v1Migrate:
            let session = LiteWriteSession(lock: lock)
            await session.startRefresh()
            do {
                try await migrateUnderLock(
                    client: client, basePath: basePath, writerID: writerID, session: session, now: now
                )
            } catch {
                await session.stopAndRelease()
                throw error
            }
            await runForegroundCleanup(client: client, basePath: basePath, writerID: writerID, now: now)
            return ForegroundPlan(layout: .lite, session: session)

        case .current:
            break   // committed (initially or by another writer) — no version commit needed

        case .fresh:
            // Only safe if initial probe was also .fresh; .current -> .fresh means repo was deleted.
            guard decision == .fresh else {
                await lock.release()
                throw LiteRepoError.repoDamaged
            }
            try await commitVersionUnderLock(
                client: client, basePath: basePath, writerID: writerID, lock: lock, now: now
            )

        case .malformedVersion:
            // Owned repair of an existing (malformed) version marker, re-probed under the lock. Not fresh
            // initialization: we overwrite the broken marker with canonical bytes and continue as current.
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

        let session = LiteWriteSession(lock: lock)
        await session.startRefresh()
        await runForegroundCleanup(client: client, basePath: basePath, writerID: writerID, now: now)
        return ForegroundPlan(layout: .lite, session: session)
    }

    // Foreground V1→Lite migration under an acquired lock. Re-reads the route first (TOCTOU): a
    // concurrent writer may have committed (`.current`), emptied (`.fresh`), or broken the repo since the
    // initial probe, so the authoritative decision must come from under the lock.
    private static func migrateUnderLock(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        session: LiteWriteSession,
        now: Date
    ) async throws {
        switch try await classify(client: client, basePath: basePath) {
        case .v1Migrate:
            try await V1ToLiteMigration(
                client: client,
                basePath: basePath,
                assertOwnership: { await session.assertStillOwned() }
            ).run(createdAt: Self.isoTimestamp(now), createdBy: writerID ?? "")
        case .current:
            break   // already migrated/committed by another writer
        case .fresh:
            // V1 data seen outside the lock but gone inside: contradictory probe. The safe action
            // is to fail closed rather than commit an empty Lite repo and delete V1 manifests.
            throw LiteRepoError.repoDamaged
        case .damaged, .malformedVersion:
            // Migration does not repair version markers; a contradictory under-lock state fails closed.
            throw LiteRepoError.repoDamaged
        case .unsupported:
            throw LiteRepoError.repoUnsupported
        }
    }

    // Maintenance (verify) path. A committed/fresh Lite repo takes a foreground lock so reconcile/flush
    // are owned; an existing V1 tree verifies lock-free as V1. Never commits version.json — verify must
    // not initialize a repo. Fails closed on damaged/unsupported/fault.
    static func prepareMaintenance(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        let decision = try await classify(client: client, basePath: basePath)
        switch decision {
        case .current, .fresh:
            let lock = try await acquireForegroundLock(
                client: client, basePath: basePath, writerID: writerID, now: now,
                onForeignWriterObserved: onForeignWriterObserved
            )
            let session = LiteWriteSession(lock: lock)
            await session.startRefresh()
            // Cleanup only after the repo is committed/current. Verify never commits version.json, so a
            // `.fresh` route has no committed Lite repo to maintain and must be left untouched.
            if decision == .current {
                await runForegroundCleanup(client: client, basePath: basePath, writerID: writerID, now: now)
            }
            return MaintenancePlan(layout: .lite, session: session)
        case .malformedVersion:
            // Repairs an existing Lite marker (not fresh init), so maintenance may rewrite version.json —
            // but only under the lock and after re-probing. Maintenance `.fresh` still never commits.
            let lock = try await acquireForegroundLock(
                client: client, basePath: basePath, writerID: writerID, now: now,
                onForeignWriterObserved: onForeignWriterObserved
            )
            let underLock: RepoFormatDecision
            do {
                underLock = try await classify(client: client, basePath: basePath)
            } catch {
                await lock.release()
                throw error
            }
            switch underLock {
            case .current:
                break   // already repaired by another writer
            case .malformedVersion:
                try await commitVersionUnderLock(
                    client: client, basePath: basePath, writerID: writerID, lock: lock, now: now
                )
            case .fresh, .v1Migrate, .damaged, .unsupported:
                await lock.release()
                throw LiteRepoError.repoDamaged
            }
            let session = LiteWriteSession(lock: lock)
            await session.startRefresh()
            await runForegroundCleanup(client: client, basePath: basePath, writerID: writerID, now: now)
            return MaintenancePlan(layout: .lite, session: session)
        case .v1Migrate:
            return MaintenancePlan(layout: .v1, session: nil)
        case .damaged:
            throw LiteRepoError.repoDamaged
        case .unsupported:
            throw LiteRepoError.repoUnsupported
        }
    }

    private static func acquireForegroundLock(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> WriteLockService {
        guard let writerID,
              let lock = WriteLockService(
                  basePath: basePath, writerID: writerID, client: client,
                  onForeignWriterObserved: onForeignWriterObserved
              ) else {
            throw LiteRepoError.writerIdentityUnavailable
        }
        switch await lock.acquire(mode: .foreground, now: now) {
        case .acquired:
            return lock
        case .blocked, .skipped:
            throw LiteRepoError.lockConflict
        case .faulted(let category):
            throw LiteRepoError.lockFault(category)
        }
    }

    // Whitelisted metadata cleanup on a Lite-owned foreground path; never touches data bytes and never
    // throws, so it cannot change the caller's outcome. The writer ID is threaded so cleanup never
    // deletes the current writer's own active lock.
    private static func runForegroundCleanup(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date
    ) async {
        await OrphanCleanupLite(client: client, basePath: basePath, currentWriterID: writerID)
            .run(mode: .foreground, now: now)
    }

    // Pure-read path: layout only, never a lock. `.fresh`/`.current` read Lite; an existing V1 tree is
    // still read as V1 (read-only); damaged/unsupported/fault fail closed so we never show wrong data.
    static func resolveReadLayout(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> MonthManifestStore.ManifestLayout {
        switch try await classify(client: client, basePath: basePath) {
        case .current, .fresh:
            return .lite
        case .v1Migrate:
            return .v1
        case .damaged, .malformedVersion:
            // Pure read fails closed for a malformed version marker: repair is a write-path concern.
            throw LiteRepoError.repoDamaged
        case .unsupported:
            throw LiteRepoError.repoUnsupported
        }
    }

    // Background write path. Never throws to abort the whole session — any non-`.acquired` Lite-writable
    // outcome (conflict, fault, v1Migrate, damaged, unsupported) becomes `.skip` so the runner moves on
    // without ever writing Lite data unowned.
    static func prepareBackgroundWrite(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async -> BackgroundOutcome {
        let decision: RepoFormatDecision
        do {
            decision = try await classify(client: client, basePath: basePath)
        } catch {
            return .skip
        }
        switch decision {
        case .fresh, .current:
            break
        case .v1Migrate, .damaged, .unsupported, .malformedVersion:
            // Background skips malformed-version repair: it stays conservative and never repairs markers.
            return .skip
        }

        guard let writerID,
              let lock = WriteLockService(
                  basePath: basePath, writerID: writerID, client: client,
                  onForeignWriterObserved: onForeignWriterObserved
              ) else {
            return .skip
        }

        switch await lock.acquire(mode: .background, now: now) {
        case .acquired:
            break
        case .blocked, .skipped, .faulted:
            return .skip
        }

        // Re-classify under lock to catch TOCTOU state changes (e.g. V1 data appeared after
        // the initial probe classified as .fresh). Background must not initialize Lite over V1.
        let underLockDecision: RepoFormatDecision
        do {
            underLockDecision = try await classify(client: client, basePath: basePath)
        } catch {
            await lock.release()
            return .skip
        }
        switch underLockDecision {
        case .current:
            break
        case .fresh where decision == .fresh:
            break
        default:
            await lock.release()
            return .skip
        }

        if underLockDecision == .fresh {
            do {
                try await VersionManifestWriter(client: client, basePath: basePath)
                    .commit(createdAt: Self.isoTimestamp(now), createdBy: writerID)
            } catch {
                await lock.release()
                return .skip
            }
        }

        let session = LiteWriteSession(lock: lock)
        await session.startRefresh()
        return .proceed(ForegroundPlan(layout: .lite, session: session))
    }

    // MARK: - Helpers

    // Commits version.json under an already-held lock, releasing the lock and surfacing
    // versionCommitFailed if the write/read-back fails. Shared by the fresh-init and malformed-repair routes.
    private static func commitVersionUnderLock(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        lock: WriteLockService,
        now: Date
    ) async throws {
        do {
            try await VersionManifestWriter(client: client, basePath: basePath)
                .commit(createdAt: isoTimestamp(now), createdBy: writerID ?? "")
        } catch {
            await lock.release()
            throw LiteRepoError.versionCommitFailed
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

    private static func isoTimestamp(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.string(from: date)
    }
}

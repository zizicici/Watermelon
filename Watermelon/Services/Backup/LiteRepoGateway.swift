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

    // The three write-preparation entry points differ only in policy: which initial states may proceed,
    // whether a decline throws or skips, the lock mode acquired, and which routes commit/migrate/repair.
    // They share one classify → acquire → reclassify → apply → cleanup state machine (`prepareWrite`).
    private enum WriteMode {
        case foreground
        case background
        case maintenance
    }

    // MARK: - Public entry points

    // Foreground write path. `.fresh`/`.current` → acquire lock (and, for `.fresh`, commit version.json);
    // `.v1Migrate` → acquire lock then migrate the legacy tree into Lite; `.malformedVersion` → repair the
    // marker under the lock; everything else throws before mutating data. All routes that proceed start the
    // refresh loop and continue on `.lite`.
    static func prepareForegroundWrite(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> ForegroundPlan {
        switch try await prepareWrite(
            mode: .foreground, client: client, basePath: basePath, writerID: writerID, now: now,
            onForeignWriterObserved: onForeignWriterObserved
        ) {
        case .proceed(let plan):
            return plan
        case .skip:
            throw LiteRepoError.repoDamaged   // unreachable: foreground declines by throwing, never skip
        }
    }

    // Background write path. Never throws to abort the whole session — any Lite-unwritable outcome
    // (probe fault, conflict, lock fault, v1Migrate, damaged, unsupported, malformed, under-lock TOCTOU
    // mismatch, missing writer ID) becomes `.skip` so the runner moves on without ever writing Lite data
    // unowned. Stays conservative: never migrates, repairs markers, or takes over a foreign lock.
    static func prepareBackgroundWrite(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async -> BackgroundOutcome {
        (try? await prepareWrite(
            mode: .background, client: client, basePath: basePath, writerID: writerID, now: now,
            onForeignWriterObserved: onForeignWriterObserved
        )) ?? .skip
    }

    // Maintenance (verify) path. A committed `.current` repo takes a foreground lock so reconcile/flush are
    // owned; a `.malformedVersion` marker is repaired under the lock. Verify never initializes a repo, so a
    // `.fresh` (no committed repo) or `.v1Migrate` (un-migrated legacy tree) is rejected without taking a
    // lock or writing anything. Fails closed on damaged/unsupported/fault.
    static func prepareMaintenance(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        switch try await prepareWrite(
            mode: .maintenance, client: client, basePath: basePath, writerID: writerID, now: now,
            onForeignWriterObserved: onForeignWriterObserved
        ) {
        case .proceed(let plan):
            return MaintenancePlan(layout: plan.layout, session: plan.session)
        case .skip:
            throw LiteRepoError.repoDamaged   // unreachable: maintenance declines by throwing, never skip
        }
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

    // MARK: - Shared write-preparation state machine

    // One classify → acquire → reclassify → apply pipeline for all three write modes. `.proceed` carries a
    // started Lite write lease; `.skip` is only ever produced in `.background` mode (foreground/maintenance
    // declines throw). The mode policy is applied at four points: the pre-lock classify gate, whether a
    // decline throws or skips, the lock mode acquired, and the under-lock decision handler.
    private static func prepareWrite(
        mode: WriteMode,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        now: Date,
        onForeignWriterObserved: (@Sendable () async -> Void)?
    ) async throws -> BackgroundOutcome {
        // Background turns every fail-closed condition into a safe skip; the other modes surface it.
        func decline(_ error: LiteRepoError) throws -> BackgroundOutcome {
            if mode == .background { return .skip }
            throw error
        }

        // 1) Classify before any lock.
        let decision: RepoFormatDecision
        do {
            decision = try await classify(client: client, basePath: basePath)
        } catch {
            if mode == .background { return .skip }
            throw error   // probe fault
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
            switch mode {
            case .foreground: break                  // migrate under the lock
            case .background: return .skip           // background never migrates
            case .maintenance: throw LiteRepoError.repoMaintenanceUnavailable   // no lock-free V1 maintenance write
            }
        case .malformedVersion:
            switch mode {
            case .foreground, .maintenance: break    // owned repair under the lock
            case .background: return .skip           // background never repairs markers
            }
        case .damaged:
            return try decline(.repoDamaged)
        case .unsupported:
            return try decline(.repoUnsupported)
        }

        // 3) Acquire the write lock in the mode-selected lock mode.
        guard let writerID,
              let lock = WriteLockService(
                  basePath: basePath, writerID: writerID, client: client,
                  onForeignWriterObserved: onForeignWriterObserved
              ) else {
            return try decline(.writerIdentityUnavailable)
        }
        let lockMode: WriteLockService.Mode = mode == .background ? .background : .foreground
        switch await lock.acquire(mode: lockMode, now: now) {
        case .acquired:
            break
        case .blocked, .skipped:
            return try decline(.lockConflict)
        case .faulted(let category):
            return try decline(.lockFault(category))
        }

        // 4-5) Apply the under-lock policy. A failure after acquire can leave a half-created `.watermelon`
        //      marker (locks dir / own lock) with no committed version.json; best-effort unwind an empty,
        //      uncommitted marker so a released old client does not later reject a bare `.watermelon`.
        //      Mitigation, not elimination — a fault mid-unwind leaves the marker for retry self-heal.
        do {
            return try await applyUnderLockPolicy(
                mode: mode, client: client, basePath: basePath, writerID: writerID,
                lock: lock, decision: decision, now: now
            )
        } catch {
            await attemptMarkerUnwind(client: client, basePath: basePath)
            throw error
        }
    }

    // Re-classifies under the lock and applies the mode's commit/migrate/proceed/reject policy. Extracted so
    // prepareWrite can wrap it with prepare-failure marker unwind. `writerID` is non-optional (acquire above).
    private static func applyUnderLockPolicy(
        mode: WriteMode,
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String,
        lock: WriteLockService,
        decision: RepoFormatDecision,
        now: Date
    ) async throws -> BackgroundOutcome {
        // 4) Re-classify under the lock to catch TOCTOU state changes (V1 appeared after the probe, another
        //    writer committed version.json, a marker broke). The under-lock decision is authoritative.
        let underLock: RepoFormatDecision
        do {
            underLock = try await classify(client: client, basePath: basePath)
        } catch {
            await lock.release()
            if mode == .background { return .skip }
            throw error   // probe fault under the lock
        }

        // 5) Apply the under-lock policy: commit / migrate / proceed / reject, then start the lease and run
        //    cleanup for the modes that own it.
        switch mode {
        case .foreground:
            switch underLock {
            case .v1Migrate:
                // Migration re-asserts ownership per month, so the live session must exist before its writes.
                // It consumes the under-lock decision directly — no third classify.
                let session = LiteWriteSession(lock: lock)
                await session.startRefresh()
                do {
                    try await V1ToLiteMigration(
                        client: client,
                        basePath: basePath,
                        assertOwnership: { await session.assertStillOwned() }
                    ).run(createdAt: isoTimestamp(now), createdBy: writerID)
                } catch {
                    await session.stopAndRelease()
                    throw error
                }
                await runForegroundCleanup(client: client, basePath: basePath, writerID: writerID, now: now)
                return .proceed(ForegroundPlan(layout: .lite, session: session))
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
            let session = LiteWriteSession(lock: lock)
            await session.startRefresh()
            await runForegroundCleanup(client: client, basePath: basePath, writerID: writerID, now: now)
            return .proceed(ForegroundPlan(layout: .lite, session: session))

        case .background:
            switch underLock {
            case .current:
                break
            case .fresh where decision == .fresh:
                // Background must not initialize Lite over V1: only a fresh-both route commits version.json.
                do {
                    try await commitVersionUnderLock(
                        client: client, basePath: basePath, writerID: writerID, lock: lock, now: now
                    )
                } catch {
                    // commitVersionUnderLock already released the lock; unwind the empty uncommitted marker.
                    await attemptMarkerUnwind(client: client, basePath: basePath)
                    return .skip
                }
            default:
                await lock.release()
                return .skip   // any other under-lock state (v1Migrate, malformed, damaged, fresh-over-V1) skips
            }
            let session = LiteWriteSession(lock: lock)
            await session.startRefresh()
            return .proceed(ForegroundPlan(layout: .lite, session: session))   // background runs no cleanup

        case .maintenance:
            switch (decision, underLock) {
            case (_, .current):
                break   // still committed, or repaired to current by another writer between the two reads
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
            let session = LiteWriteSession(lock: lock)
            await session.startRefresh()
            await runForegroundCleanup(client: client, basePath: basePath, writerID: writerID, now: now)
            return .proceed(ForegroundPlan(layout: .lite, session: session))
        }
    }

    // MARK: - Helpers

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
            // Cancellation must surface as cancellation, never be relabeled as a repo commit failure.
            if RemoteFaultLite.classify(error) == .cancelled { throw error }
            throw LiteRepoError.versionCommitFailed
        }
    }

    // Best-effort removal of a half-created `.watermelon` marker left by a prepare that failed before
    // committing version.json. Deletes the marker only when it is conclusively uncommitted (no version.json)
    // and empty apart from an empty `locks` directory. A committed version, a month sqlite, a dev marker,
    // data, an unknown child, or any probe fault aborts without deleting. Never throws — a cleanup failure
    // must not mask the original prepare error. Mitigates, but does not close, the marker-before-version
    // window: a fault mid-unwind leaves the marker for retry self-heal.
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

    private static func isoTimestamp(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.string(from: date)
    }
}

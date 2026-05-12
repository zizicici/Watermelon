import Foundation

enum BackupV2RuntimeBuildError: Error {
    case profileMissingID
    case unsupportedRemoteFormat(minAppVersion: String?)
    case requiresForegroundMigration
    /// Local repoID disagrees with remote `repo.json` — caller must resolve before backup
    /// (drop local repo_state + retry, or re-point the profile).
    case repoIdentityMismatch(local: String, remote: String)
    /// Local says migrated but remote inspection is V1 — V2 marker disappeared
    /// (cloud-sync delay, manual delete, peer wipe). Stale local seq/clock would
    /// race against whatever other devices wrote in V2 since.
    case repoFormatRegression(repoID: String)
}

enum BackupV2RuntimeBuilder {
    static func build(
        client: any RemoteStorageClientProtocol,
        metadataClient: any RemoteStorageClientProtocol,
        ownsMetadataClient: Bool = true,
        runMaintenanceTasks: Bool = true,
        profile: ServerProfileRecord,
        databaseManager: DatabaseManager,
        format: RemoteFormatCompatibilityService = RemoteFormatCompatibilityService(),
        allowMigration: Bool,
        onMigrationStart: (() async -> Void)? = nil,
        onMigrationComplete: ((Int) async -> Void)? = nil,
        onBootstrap: (() async -> Void)? = nil
    ) async throws -> BackupV2RuntimeServices {
        guard let profileID = profile.id else {
            throw BackupV2RuntimeBuildError.profileMissingID
        }
        // Both foreground (BackupRunPreparation) and background paths land here — ensure
        // base path exists before inspect tries to list it on a fresh profile / empty remote.
        try await client.createDirectory(path: RemotePathBuilder.normalizePath(profile.basePath))
        let inspection = try await format.inspectRemoteFormat(client: client, profile: profile)

        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profileID)
        let runID = RepoIdentity.newRunID()
        let bootstrap = RepoBootstrap(client: client, basePath: profile.basePath)

        let resolvedRepoID: String
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
        case .v2:
            // ensureRepoJSON heals half-bootstrap (version present, repo absent) — without
            // it each session would generate a fresh local UUID that never reaches remote.
            let localRepoID = try await identity.findRepoStateByProfile(profileID: profileID)?.repoID
            let suggested = localRepoID ?? UUID().uuidString.lowercased()
            resolvedRepoID = try await bootstrap.ensureRepoJSON(repoID: suggested, writerID: writerID)
            if let localRepoID, resolvedRepoID != localRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: localRepoID, remote: resolvedRepoID)
            }
            // WebDAV/SMB/SFTP don't auto-create parents on PUT.
            try await bootstrap.ensureSubdirectories()
        case .fresh:
            do {
                resolvedRepoID = try await bootstrap.initializeFreshRepo(writerID: writerID)
            } catch let error as RepoBootstrap.VersionConflict {
                switch error {
                case .higherFormatVersion(_, _, let minApp),
                     .mismatchedFormatVersion(_, _, let minApp):
                    throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minApp)
                case .unreadable:
                    throw error
                }
            }
            await onBootstrap?()
        case .v1:
            guard allowMigration else {
                throw BackupV2RuntimeBuildError.requiresForegroundMigration
            }
            // Canonical repoID before phase 1 — otherwise a peer racing on repo.json
            // leaves us writing phase-1 commits stamped with our local (foreign-to-remote) id.
            let storedRepoID = try await identity.findRepoStateByProfile(profileID: profileID)?.repoID
            let suggestedRepoID = storedRepoID ?? UUID().uuidString.lowercased()
            resolvedRepoID = try await bootstrap.ensureRepoJSON(repoID: suggestedRepoID, writerID: writerID)
            // Mismatch only when DB had a binding; freshly-generated UUID has nothing to mismatch.
            if let storedRepoID, resolvedRepoID != storedRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: storedRepoID, remote: resolvedRepoID)
            }
            let state = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: resolvedRepoID, writerID: writerID)
            let migration = V1MigrationService(
                client: client,
                basePath: profile.basePath,
                database: databaseManager,
                identity: identity,
                bootstrap: bootstrap
            )
            let needsMigration: Bool
            if state.migrationCompleted != 1 {
                needsMigration = true
            } else if try await migration.ownsMigrationMarker(writerID: writerID) {
                // Our phase3 was incomplete; phase1 is idempotent at fingerprint level so re-running is safe.
                needsMigration = true
            } else {
                // V1 manifests after our completed migration: peer mid-migration, older
                // V1-only client writing into the V2 repo, or stale marker. All three are
                // resolved by re-running phase1+2+3 idempotently — phase1 folds new V1 into
                // V2 commits, phase3 deletes only what we scanned. No marker is no longer
                // a regression signal: V1 visibility itself triggers re-migrate.
                let lingering = try await migration.scanV1Months()
                needsMigration = !lingering.isEmpty
            }

            if needsMigration {
                await onMigrationStart?()
                let processed = try await migration.runPhase1(profileID: profileID, repoID: resolvedRepoID, writerID: writerID, runID: runID)
                do {
                    try await migration.runPhase2(profileID: profileID, repoID: resolvedRepoID, writerID: writerID)
                } catch let error as RepoBootstrap.VersionConflict {
                    if case .higherFormatVersion(_, _, let minApp) = error {
                        throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minApp)
                    }
                    throw error
                }
                try await migration.runPhase3(writerID: writerID)
                await onMigrationComplete?(processed)
            }
        }

        let state = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: resolvedRepoID, writerID: writerID)
        let initialSeq = UInt64(bitPattern: state.lastSeq)
        let initialClock = UInt64(bitPattern: state.lastClock)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: resolvedRepoID, initial: initialSeq)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: resolvedRepoID, initial: initialClock)
        // Writers share a dedicated connection so they don't contend with worker uploads.
        let commitWriter = CommitLogWriter(client: metadataClient, basePath: profile.basePath)
        let snapshotWriter = SnapshotWriter(client: metadataClient, basePath: profile.basePath)

        // Always materialize — even after our own phase1, peer commits may interleave with
        // higher clocks; the earlier "skip after migration" optimization let those go
        // unobserved and our subsequent writes could land below peer ops, breaking LWW.
        let materializer = RepoMaterializer(client: client, basePath: profile.basePath)
        let output = try await materializer.materialize(expectedRepoID: resolvedRepoID)
        let remoteSeqMax = output.observedSeqByWriter.values.max() ?? 0
        if remoteSeqMax > initialSeq {
            try await allocator.observeRemoteMax(remoteSeqMax)
        }
        if output.state.observedClock > initialClock {
            try await lamport.observe(output.state.observedClock)
        }
        let initialMaterialize: RepoMaterializer.MaterializeOutput? = output

        let liveness = LivenessTracker(
            client: metadataClient,
            basePath: profile.basePath,
            writerID: writerID,
            isLocalVolume: profile.resolvedStorageType == .externalVolume
        )
        // verify-cleanup borrows caller's client — skip maintenance to avoid concurrent ops.
        var sweepTask: Task<Void, Never>? = nil
        if runMaintenanceTasks {
            await liveness.start()
            if let otherActive = try? await liveness.listOtherActiveWriters() {
                var activeWriters = Set(otherActive)
                activeWriters.insert(writerID)
                sweepTask = Task(priority: .utility) { [activeWriters] in
                    _ = await OrphanMetadataCleanup.sweep(
                        client: metadataClient,
                        directories: OrphanMetadataCleanup.standardSweepDirectories(basePath: profile.basePath),
                        activeWriters: activeWriters,
                        ageThresholdSeconds: 3600,
                        now: Date()
                    )
                }
            }
        }

        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: resolvedRepoID,
            runID: runID,
            basePath: profile.basePath,
            database: databaseManager,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: commitWriter,
            snapshotWriter: snapshotWriter,
            liveness: liveness,
            metadataClient: metadataClient,
            ownsMetadataClient: ownsMetadataClient,
            initialMaterializeOutput: InitialMaterializeOutputBox(initialMaterialize),
            sweepTask: sweepTask
        )
    }

}

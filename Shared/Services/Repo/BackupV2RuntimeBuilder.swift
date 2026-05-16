import Foundation

enum BackupV2RuntimeBuildError: Error {
    case profileMissingID
    case unsupportedRemoteFormat(minAppVersion: String?)
    case requiresForegroundMigration
    case repoIdentityMismatch(local: String, remote: String)
    case repoFormatRegression(repoID: String)
    case damagedV2Repo
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
        // Inspect lists basePath; ensure it exists for fresh-profile bootstraps.
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
            let sources = try await RepoIdentitySources.collect(
                profileID: profileID,
                identity: identity,
                client: client,
                basePath: profile.basePath,
                format: format
            )
            resolvedRepoID = try await sources.publish(bootstrap: bootstrap, writerID: writerID)
            // WebDAV/SMB/SFTP don't auto-create parents on PUT.
            try await bootstrap.ensureSubdirectories()
        case .v2WithPendingMigrationCleanup(_, let ownerWriterID):
            try await bootstrap.ensureSubdirectories()
            // Heavy commit/snapshot scan stays on data `client`; publication on the
            // metadata bootstrap keeps marker writes off the data connection.
            let sources = try await RepoIdentitySources.collect(
                profileID: profileID,
                identity: identity,
                client: client,
                basePath: profile.basePath,
                format: format
            )
            let cleanupBootstrap = RepoBootstrap(client: metadataClient, basePath: profile.basePath)
            let cleanup = V1MigrationService(
                client: metadataClient,
                basePath: profile.basePath,
                database: databaseManager,
                identity: identity,
                bootstrap: cleanupBootstrap
            )
            do {
                let resolved = try await sources.publish(bootstrap: cleanupBootstrap, writerID: writerID)
                try await cleanup.runCleanupOnly(
                    ownerWriterID: ownerWriterID,
                    writerID: writerID,
                    runID: runID
                )
                resolvedRepoID = resolved
            } catch let error as RepoBootstrap.VersionConflict {
                switch error {
                case .higherFormatVersion(_, _, let minApp),
                     .mismatchedFormatVersion(_, _, let minApp):
                    throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minApp)
                case .unreadable:
                    throw error
                }
            }
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
        case .v1, .v2WithV1Manifests:
            guard allowMigration else {
                throw BackupV2RuntimeBuildError.requiresForegroundMigration
            }
            let sources = try await RepoIdentitySources.collect(
                profileID: profileID,
                identity: identity,
                client: client,
                basePath: profile.basePath,
                format: format
            )
            let migration = V1MigrationService(
                client: client,
                basePath: profile.basePath,
                database: databaseManager,
                identity: identity,
                bootstrap: bootstrap
            )
            do {
                let resolved = try await sources.publish(bootstrap: bootstrap, writerID: writerID)
                _ = try await migration.runFullMigration(
                    profileID: profileID,
                    repoID: resolved,
                    writerID: writerID,
                    runID: runID,
                    onMigrationStart: onMigrationStart,
                    onMigrationComplete: onMigrationComplete
                )
                resolvedRepoID = resolved
            } catch let error as RepoBootstrap.VersionConflict {
                switch error {
                case .higherFormatVersion(_, _, let minApp),
                     .mismatchedFormatVersion(_, _, let minApp):
                    throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minApp)
                case .unreadable:
                    throw error
                }
            }
        }

        let state = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: resolvedRepoID, writerID: writerID)
        let initialSeq = UInt64(bitPattern: state.lastSeq)
        let initialClock = UInt64(bitPattern: state.lastClock)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: resolvedRepoID, initial: initialSeq)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: resolvedRepoID, initial: initialClock)
        // Dedicated metadata connection so commits/snapshots/liveness don't contend with worker uploads.
        let commitWriter = CommitLogWriter(client: metadataClient, basePath: profile.basePath)
        let snapshotWriter = SnapshotWriter(client: metadataClient, basePath: profile.basePath)

        // Observe peer clocks/seq before allocating so our writes can't land below them.
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
        var sweepTask: Task<Void, Never>? = nil
        if runMaintenanceTasks {
            await liveness.start()
            if metadataClient.supportsLivenessSafeRenewal {
                // Sweep only on a determinate peer view — `isComplete == false` means at
                // least one peer is `.unknown` and we'd risk deleting an active peer's
                // staging files. Next bootstrap retries.
                do {
                    let view = try await liveness.snapshotPeerStatuses()
                    if view.isComplete {
                        var protectedWriters = view.sweepProtectionSet
                        protectedWriters.insert(writerID)
                        sweepTask = Task(priority: .utility) { [protectedWriters] in
                            _ = await OrphanMetadataCleanup.sweep(
                                client: metadataClient,
                                directories: OrphanMetadataCleanup.standardSweepDirectories(basePath: profile.basePath),
                                activeWriters: protectedWriters,
                                ageThresholdSeconds: 3600,
                                now: Date()
                            )
                        }
                    }
                } catch {
                    // List-level failure → no view at all; skip sweep (same as the prior `try?` behavior).
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

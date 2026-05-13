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
            let localRepoID = try await identity.findRepoStateByProfile(profileID: profileID)?.repoID
            if localRepoID == nil {
                // No local cache + no remote identity + V2 data present = minting a new UUID would orphan existing commits.
                let remoteIdentity = try await bootstrap.loadRepoID()
                if remoteIdentity == nil,
                   try await format.hasAnyV2CommitOrSnapshotData(client: client, basePath: profile.basePath) {
                    throw BackupV2RuntimeBuildError.damagedV2Repo
                }
            }
            let suggested = localRepoID ?? UUID().uuidString.lowercased()
            resolvedRepoID = try await bootstrap.ensureRepoJSON(repoID: suggested, writerID: writerID)
            if let localRepoID, resolvedRepoID != localRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: localRepoID, remote: resolvedRepoID)
            }
            // WebDAV/SMB/SFTP don't auto-create parents on PUT.
            try await bootstrap.ensureSubdirectories()
        case .v2WithPendingMigrationCleanup(_, let ownerWriterID):
            let localRepoID = try await identity.findRepoStateByProfile(profileID: profileID)?.repoID
            if localRepoID == nil {
                let remoteIdentity = try await bootstrap.loadRepoID()
                if remoteIdentity == nil,
                   try await format.hasAnyV2CommitOrSnapshotData(client: client, basePath: profile.basePath) {
                    throw BackupV2RuntimeBuildError.damagedV2Repo
                }
            }
            let suggested = localRepoID ?? UUID().uuidString.lowercased()
            resolvedRepoID = try await bootstrap.ensureRepoJSON(repoID: suggested, writerID: writerID)
            if let localRepoID, resolvedRepoID != localRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: localRepoID, remote: resolvedRepoID)
            }
            try await bootstrap.ensureSubdirectories()
            let cleanup = V1MigrationService(
                client: client,
                basePath: profile.basePath,
                database: databaseManager,
                identity: identity,
                bootstrap: bootstrap
            )
            try await cleanup.runPhase3Cleanup(writerID: ownerWriterID, runID: runID)
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
            // Canonical repoID must land before phase 1 so commits are stamped with the remote-canonical id.
            let storedRepoID = try await identity.findRepoStateByProfile(profileID: profileID)?.repoID
            let suggestedRepoID = storedRepoID ?? UUID().uuidString.lowercased()
            resolvedRepoID = try await bootstrap.ensureRepoJSON(repoID: suggestedRepoID, writerID: writerID)
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
                needsMigration = true
            } else {
                let lingering = try await migration.scanV1Months()
                needsMigration = !lingering.isEmpty
            }

            if needsMigration {
                await onMigrationStart?()
                let processed = try await migration.runPhase1(profileID: profileID, repoID: resolvedRepoID, writerID: writerID, runID: runID)
                do {
                    try await migration.runPhase2(profileID: profileID, repoID: resolvedRepoID, writerID: writerID, runID: runID)
                } catch let error as RepoBootstrap.VersionConflict {
                    if case .higherFormatVersion(_, _, let minApp) = error {
                        throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minApp)
                    }
                    throw error
                }
                try await migration.runPhase3(writerID: writerID, runID: runID)
                await onMigrationComplete?(processed)
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
        let ourRemoteMax = output.observedSeqByWriter[writerID] ?? 0
        if ourRemoteMax > initialSeq {
            try await allocator.observeRemoteMax(ourRemoteMax)
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

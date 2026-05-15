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

        func checkedExistingV2DataRepoID(storedRepoID: String?) async throws -> String? {
            let dataRepoIDs = try await Self.existingRepoIDsInV2Data(client: client, basePath: profile.basePath)
            guard !dataRepoIDs.isEmpty else {
                if try await format.hasAnyV2CommitOrSnapshotData(client: client, basePath: profile.basePath) {
                    throw BackupV2RuntimeBuildError.damagedV2Repo
                }
                return nil
            }
            guard dataRepoIDs.count == 1, let dataRepoID = dataRepoIDs.first else {
                throw BackupV2RuntimeBuildError.damagedV2Repo
            }
            if let storedRepoID, storedRepoID != dataRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: storedRepoID, remote: dataRepoID)
            }
            return dataRepoID
        }

        func resolveExistingV2RepoID() async throws -> String {
            let storedRepoID = try await identity.findRepoStateByProfile(profileID: profileID)?.repoID
            let remoteRepoID = try await bootstrap.loadRepoID()
            if let storedRepoID, let remoteRepoID, storedRepoID != remoteRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: storedRepoID, remote: remoteRepoID)
            }
            let dataRepoID = try await checkedExistingV2DataRepoID(storedRepoID: storedRepoID)
            if let remoteRepoID, let dataRepoID, remoteRepoID != dataRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: remoteRepoID, remote: dataRepoID)
            }

            let suggested = remoteRepoID ?? dataRepoID ?? storedRepoID ?? UUID().uuidString.lowercased()
            let resolvedRepoID = try await bootstrap.ensureRepoJSON(repoID: suggested, writerID: writerID)
            if let storedRepoID, resolvedRepoID != storedRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: storedRepoID, remote: resolvedRepoID)
            }
            if let remoteRepoID, resolvedRepoID != remoteRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: remoteRepoID, remote: resolvedRepoID)
            }
            if let dataRepoID, resolvedRepoID != dataRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: dataRepoID, remote: resolvedRepoID)
            }
            return resolvedRepoID
        }

        func buildMigrationSources() async throws -> V1MigrationService.RepoIdentitySources {
            let stored = try await identity.findRepoStateByProfile(profileID: profileID)?.repoID
            let remote = try await bootstrap.loadRepoID()
            if let stored, let remote, stored != remote {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: stored, remote: remote)
            }
            let data = try await checkedExistingV2DataRepoID(storedRepoID: stored)
            if let remote, let data, remote != data {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: data, remote: remote)
            }
            let suggested = remote ?? data ?? stored ?? UUID().uuidString.lowercased()
            return V1MigrationService.RepoIdentitySources(stored: stored, remote: remote, data: data, suggested: suggested)
        }

        let resolvedRepoID: String
        var wroteV2DataBeforeFinalRepoCheck = false
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
        case .v2:
            resolvedRepoID = try await resolveExistingV2RepoID()
            // WebDAV/SMB/SFTP don't auto-create parents on PUT.
            try await bootstrap.ensureSubdirectories()
        case .v2WithPendingMigrationCleanup:
            try await bootstrap.ensureSubdirectories()
            let cleanupBootstrap = RepoBootstrap(client: metadataClient, basePath: profile.basePath)
            let cleanup = V1MigrationService(
                client: metadataClient,
                basePath: profile.basePath,
                database: databaseManager,
                identity: identity,
                bootstrap: cleanupBootstrap
            )
            let sources = try await buildMigrationSources()
            do {
                let outcome = try await cleanup.run(
                    profileID: profileID,
                    inspection: inspection,
                    writerID: writerID,
                    runID: runID,
                    sources: sources
                )
                resolvedRepoID = outcome.resolvedRepoID
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
            let migration = V1MigrationService(
                client: client,
                basePath: profile.basePath,
                database: databaseManager,
                identity: identity,
                bootstrap: bootstrap
            )
            let sources = try await buildMigrationSources()
            do {
                let outcome = try await migration.run(
                    profileID: profileID,
                    inspection: inspection,
                    writerID: writerID,
                    runID: runID,
                    sources: sources,
                    onMigrationStart: onMigrationStart,
                    onMigrationComplete: onMigrationComplete
                )
                resolvedRepoID = outcome.resolvedRepoID
                wroteV2DataBeforeFinalRepoCheck = outcome.v2DataWritten
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

        let finalizedRepoID = try await bootstrap.ensureIdentityFinalization(repoID: resolvedRepoID, writerID: writerID)
        if finalizedRepoID != resolvedRepoID {
            throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: resolvedRepoID, remote: finalizedRepoID)
        }
        if wroteV2DataBeforeFinalRepoCheck {
            if let currentRepoID = try await bootstrap.loadRepoID(), currentRepoID != resolvedRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: resolvedRepoID, remote: currentRepoID)
            }
        } else {
            let confirmedRepoID = try await bootstrap.ensureRepoJSON(repoID: resolvedRepoID, writerID: writerID)
            if confirmedRepoID != resolvedRepoID {
                throw BackupV2RuntimeBuildError.repoIdentityMismatch(local: resolvedRepoID, remote: confirmedRepoID)
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

    private static func existingRepoIDsInV2Data(
        client: any RemoteStorageClientProtocol,
        basePath: String
    ) async throws -> Set<String> {
        let effectiveClient = wrapIfSerial(client)
        let commitReader = CommitLogReader(client: effectiveClient, basePath: basePath)
        let snapshotReader = SnapshotReader(client: effectiveClient, basePath: basePath)
        async let commitFilenames = commitReader.listCommitFilenames()
        async let snapshotFilenames = snapshotReader.listSnapshotFilenames()
        var repoIDs: Set<String> = []
        for filename in try await commitFilenames {
            guard RepoLayout.parseCommitFilename(filename) != nil else { continue }
            do {
                let file = try await commitReader.read(filename: filename)
                repoIDs.insert(file.header.repoID)
            } catch is CancellationError {
                throw CancellationError()
            } catch is CommitLogReader.ReadError {
                throw BackupV2RuntimeBuildError.damagedV2Repo
            } catch {
                if isStorageNotFoundError(error) { continue }
                throw error
            }
        }
        for filename in try await snapshotFilenames {
            guard RepoLayout.parseSnapshotFilename(filename) != nil else { continue }
            do {
                let file = try await snapshotReader.read(filename: filename)
                if file.header.repoID.isEmpty { continue }
                repoIDs.insert(file.header.repoID)
            } catch is CancellationError {
                throw CancellationError()
            } catch is SnapshotReader.ReadError {
                throw BackupV2RuntimeBuildError.damagedV2Repo
            } catch {
                if isStorageNotFoundError(error) { continue }
                throw error
            }
        }
        return repoIDs
    }

}

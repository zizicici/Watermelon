import Foundation

struct OpenedBackupV2Repo: Sendable {
    let profileID: Int64
    let writerID: String
    let repoID: String
    let runID: String
    let basePath: String
    let identity: RepoIdentity
    let seqAllocator: SeqAllocator
    let lamport: PersistedLamportClock
    let commitWriter: CommitLogWriter
    let snapshotWriter: SnapshotWriter
    let initialMaterializeOutput: RepoMaterializer.MaterializeOutput?
    let isLocalVolume: Bool
}

struct BackupV2RepoOpenService: @unchecked Sendable {
    let client: any RemoteStorageClientProtocol
    let metadataClient: any RemoteStorageClientProtocol
    let profile: ServerProfileRecord
    let databaseManager: DatabaseManager
    let format: RemoteFormatCompatibilityService
    let allowMigration: Bool
    let onMigrationStart: (() async -> Void)?
    let onMigrationComplete: ((Int) async -> Void)?
    let onBootstrap: (() async -> Void)?

    init(
        client: any RemoteStorageClientProtocol,
        metadataClient: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        databaseManager: DatabaseManager,
        format: RemoteFormatCompatibilityService,
        allowMigration: Bool,
        onMigrationStart: (() async -> Void)? = nil,
        onMigrationComplete: ((Int) async -> Void)? = nil,
        onBootstrap: (() async -> Void)? = nil
    ) {
        self.client = client
        self.metadataClient = metadataClient
        self.profile = profile
        self.databaseManager = databaseManager
        self.format = format
        self.allowMigration = allowMigration
        self.onMigrationStart = onMigrationStart
        self.onMigrationComplete = onMigrationComplete
        self.onBootstrap = onBootstrap
    }

    func open() async throws -> OpenedBackupV2Repo {
        guard let profileID = profile.id else {
            throw BackupV2RuntimeBuildError.profileMissingID
        }
        let basePath = RemotePathBuilder.normalizePath(profile.basePath)
        do {
            try await client.createDirectory(path: basePath)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }

        let inspection: RemoteFormatInspection
        do {
            inspection = try await format.inspectRemoteFormat(client: client, profile: profile)
        } catch BackupCompatibilityError.damagedV2Repo {
            throw BackupV2RuntimeBuildError.damagedV2Repo
        }

        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profileID)
        let runID = RepoIdentity.newRunID()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)

        let resolvedRepoID: String
        switch inspection {
        case .unsupported(let minAppVersion):
            throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
        case .v2:
            resolvedRepoID = try await resolveAndPublishIdentityForShapedRepo(
                profileID: profileID,
                writerID: writerID,
                identity: identity,
                dataClient: client,
                basePath: basePath,
                format: format,
                publishBootstrap: bootstrap
            )
            try await bootstrap.ensureSubdirectories()
        case .v2WithPendingMigrationCleanup(_, let ownerWriterID):
            try await bootstrap.ensureSubdirectories()
            let cleanupBootstrap = RepoBootstrap(client: metadataClient, basePath: basePath)
            let cleanup = V1MigrationService(
                client: metadataClient,
                basePath: basePath,
                database: databaseManager,
                identity: identity,
                bootstrap: cleanupBootstrap
            )
            resolvedRepoID = try await resolveAndPublishIdentityForShapedRepo(
                profileID: profileID,
                writerID: writerID,
                identity: identity,
                dataClient: client,
                basePath: basePath,
                format: format,
                publishBootstrap: cleanupBootstrap
            )
            try await withShapedRepoBootstrapErrorMapping {
                try await cleanup.runCleanupOnly(
                    ownerWriterID: ownerWriterID,
                    writerID: writerID,
                    runID: runID
                )
            }
        case .fresh:
            if let existing = try await identity.findRepoStateByProfile(profileID: profileID) {
                throw BackupV2RuntimeBuildError.repoFormatRegression(repoID: existing.repoID)
            }
            do {
                do {
                    resolvedRepoID = try await bootstrap.initializeFreshRepo(writerID: writerID)
                } catch let error as RepoBootstrap.VersionConflict {
                    switch error {
                    case .higherFormatVersion(_, _, let minApp),
                         .mismatchedFormatVersion(_, _, let minApp):
                        throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minApp)
                    case .unreadable(let underlying):
                        if let underlying, RemoteWriteClassifier.isCancellation(underlying) {
                            throw CancellationError()
                        }
                        throw BackupV2RuntimeBuildError.damagedV2Repo
                    }
                }
            } catch let error as RepoBootstrap.BootstrapError {
                switch error {
                case .ioFailure(let underlying):
                    if RemoteWriteClassifier.isCancellation(underlying) {
                        throw CancellationError()
                    }
                    throw BackupV2RuntimeBuildError.damagedV2Repo
                case .futureFormatVersion(let minAppVersion):
                    throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
                }
            }
            await onBootstrap?()
        case .v1, .v2WithV1Manifests:
            guard allowMigration else {
                throw BackupV2RuntimeBuildError.requiresForegroundMigration
            }
            let migration = V1MigrationService(
                client: client,
                basePath: basePath,
                database: databaseManager,
                identity: identity,
                bootstrap: bootstrap
            )
            resolvedRepoID = try await resolveAndPublishIdentityForShapedRepo(
                profileID: profileID,
                writerID: writerID,
                identity: identity,
                dataClient: client,
                basePath: basePath,
                format: format,
                publishBootstrap: bootstrap
            )
            _ = try await withShapedRepoBootstrapErrorMapping {
                try await migration.runFullMigration(
                    profileID: profileID,
                    repoID: resolvedRepoID,
                    writerID: writerID,
                    runID: runID,
                    onMigrationStart: onMigrationStart,
                    onMigrationComplete: onMigrationComplete
                )
            }
        }

        let state = try await identity.lazyEnsureRepoState(
            profileID: profileID,
            repoID: resolvedRepoID,
            writerID: writerID
        )
        let counters = RepoStateAuthority.counters(from: state)
        let allocator = SeqAllocator(
            database: databaseManager,
            profileID: profileID,
            repoID: resolvedRepoID,
            initial: counters.lastSeq
        )
        let lamport = PersistedLamportClock(
            database: databaseManager,
            profileID: profileID,
            repoID: resolvedRepoID,
            initial: counters.lastClock
        )
        let commitWriter = CommitLogWriter(client: metadataClient, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: metadataClient, basePath: basePath)

        let materializer = RepoMaterializer(client: client, basePath: basePath)
        let output = try await materializer.materialize(expectedRepoID: resolvedRepoID)
        try await RepoStateAuthority.observeSameWriterSeq(
            writerID: writerID,
            observedSeqByWriter: output.observedSeqByWriter,
            allocator: allocator
        )
        try await lamport.observe(output.state.observedClock)
        try await lamport.repairPoisonedDBIfNeeded()

        return OpenedBackupV2Repo(
            profileID: profileID,
            writerID: writerID,
            repoID: resolvedRepoID,
            runID: runID,
            basePath: basePath,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: commitWriter,
            snapshotWriter: snapshotWriter,
            initialMaterializeOutput: output,
            isLocalVolume: profile.resolvedStorageType == .externalVolume
        )
    }

    private func resolveAndPublishIdentityForShapedRepo(
        profileID: Int64,
        writerID: String,
        identity: RepoIdentity,
        dataClient: any RemoteStorageClientProtocol,
        basePath: String,
        format: RemoteFormatCompatibilityService,
        publishBootstrap: RepoBootstrap
    ) async throws -> String {
        try await withShapedRepoBootstrapErrorMapping {
            let authority = RepoIdentityAuthority(context: RepoIdentityAuthorityContext(
                profileID: profileID,
                writerID: writerID,
                basePath: basePath,
                dataClient: dataClient,
                identity: identity,
                format: format
            ))
            let resolution = try await authority.resolve()
            return try await authority.publish(resolution, using: publishBootstrap)
        }
    }

    private func withShapedRepoBootstrapErrorMapping<T>(
        _ operation: () async throws -> T
    ) async throws -> T {
        do {
            return try await operation()
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as BackupV2RuntimeBuildError {
            throw error
        } catch let error as RepoBootstrap.BootstrapError {
            switch error {
            case .ioFailure(let underlying):
                if RemoteWriteClassifier.isCancellation(underlying) {
                    throw CancellationError()
                }
                throw BackupV2RuntimeBuildError.damagedV2Repo
            case .futureFormatVersion(let minAppVersion):
                throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
            }
        } catch let error as RepoBootstrap.VersionConflict {
            switch error {
            case .higherFormatVersion(_, _, let minApp),
                 .mismatchedFormatVersion(_, _, let minApp):
                throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minApp)
            case .unreadable(let underlying):
                if let underlying, RemoteWriteClassifier.isCancellation(underlying) {
                    throw CancellationError()
                }
                throw BackupV2RuntimeBuildError.damagedV2Repo
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                throw CancellationError()
            }
            throw error
        }
    }
}

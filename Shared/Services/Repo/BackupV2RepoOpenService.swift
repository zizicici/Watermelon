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
    /// Inspection result safe to forward to `RemoteIndexSyncService.syncIndex(preInspection:)`
    /// when non-nil. Non-nil ONLY when the open action was `.openExistingV2` — that path writes
    /// `repo.json` (identity) and creates subdirectories but does NOT touch `version.json`,
    /// migration markers, or V1 manifests, so pre-open and post-open inspections are observably
    /// identical. On `.openWithCleanupV2` / `.bootstrapFresh` / `.migrateFromV1` the open
    /// mutated format-marker state, so this is `nil` and sync MUST re-inspect to observe the
    /// post-mutation shape.
    let postOpenSyncInspection: RemoteFormatInspection?
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
        try await BackupV2RuntimeOpenErrorMapping.withOpenErrorNormalization {
            try await client.createDirectory(path: basePath)
        }

        let inspection = try await BackupV2RuntimeOpenErrorMapping.withOpenErrorNormalization {
            try await format.inspectRemoteFormat(client: client, profile: profile)
        }

        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profileID)
        let runID = RepoIdentity.newRunID()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)

        let action = BackupV2RepoOpenPlanner.plan(
            inspection: inspection,
            allowMigration: allowMigration
        )
        let resolvedRepoID: String
        // Exhaustively decided per-action so a new open action can't be added without
        // explicitly stating whether its post-open remote shape matches its pre-open
        // inspection. Only `.openExistingV2` preserves equivalence; mutating paths
        // must leave sync to re-inspect.
        let postOpenSyncInspection: RemoteFormatInspection?
        switch action {
        case .throwUnsupported(let minAppVersion):
            throw BackupV2RuntimeBuildError.unsupportedRemoteFormat(minAppVersion: minAppVersion)
        case .openExistingV2:
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
            postOpenSyncInspection = inspection
        case .openWithCleanupV2(let ownerWriterID):
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
            try await BackupV2RuntimeOpenErrorMapping.withOpenErrorNormalization {
                try await cleanup.runCleanupOnly(
                    ownerWriterID: ownerWriterID,
                    writerID: writerID,
                    runID: runID
                )
            }
            postOpenSyncInspection = nil
        case .bootstrapFresh:
            if let existing = try await identity.findRepoStateByProfile(profileID: profileID) {
                throw BackupV2RuntimeBuildError.repoFormatRegression(repoID: existing.repoID)
            }
            resolvedRepoID = try await BackupV2RuntimeOpenErrorMapping.withOpenErrorNormalization {
                try await bootstrap.initializeFreshRepo(writerID: writerID)
            }
            await onBootstrap?()
            postOpenSyncInspection = nil
        case .throwRequiresForegroundMigration:
            throw BackupV2RuntimeBuildError.requiresForegroundMigration
        case .migrateFromV1:
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
            _ = try await BackupV2RuntimeOpenErrorMapping.withOpenErrorNormalization {
                try await migration.runFullMigration(
                    profileID: profileID,
                    repoID: resolvedRepoID,
                    writerID: writerID,
                    runID: runID,
                    onMigrationStart: onMigrationStart,
                    onMigrationComplete: onMigrationComplete
                )
            }
            postOpenSyncInspection = nil
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
            isLocalVolume: profile.resolvedStorageType == .externalVolume,
            postOpenSyncInspection: postOpenSyncInspection
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
        try await BackupV2RuntimeOpenErrorMapping.withOpenErrorNormalization {
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
}

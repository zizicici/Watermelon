import Foundation

enum BackupV2RuntimeBuildError: Error {
    case profileMissingID
    case unsupportedRemoteFormat(minAppVersion: String?)
    case requiresForegroundMigration
    case repoIdentityMismatch(stored: String, observed: String)
    case repoFormatRegression(repoID: String)
    case damagedV2Repo
}

enum BackupV2RuntimeBuilder {
    static func build(
        client: any RemoteStorageClientProtocol,
        metadataClient: any RemoteStorageClientProtocol,
        ownsMetadataClient: Bool = true,
        maintenanceStartupMode: RepoMaintenanceStartupMode = .enabled,
        profile: ServerProfileRecord,
        databaseManager: DatabaseManager,
        format: RemoteFormatCompatibilityService = RemoteFormatCompatibilityService(),
        allowMigration: Bool,
        onMigrationStart: (() async -> Void)? = nil,
        onMigrationComplete: ((Int) async -> Void)? = nil,
        onBootstrap: (() async -> Void)? = nil,
        compactionPolicy: RepoCompactionPolicy = .default
    ) async throws -> BackupV2RuntimeServices {
        let opened = try await BackupV2RepoOpenService(
            client: client,
            metadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            format: format,
            allowMigration: allowMigration,
            onMigrationStart: onMigrationStart,
            onMigrationComplete: onMigrationComplete,
            onBootstrap: onBootstrap
        ).open()
        let maintenance = try await RepoMaintenanceRuntimeBuilder().start(
            opened: opened,
            metadataClient: metadataClient,
            mode: maintenanceStartupMode
        )
        let services = BackupV2RuntimeServices(
            writerID: opened.writerID,
            repoID: opened.repoID,
            runID: opened.runID,
            basePath: opened.basePath,
            postOpenSyncInspection: opened.postOpenSyncInspection,
            database: databaseManager,
            identity: opened.identity,
            seqAllocator: opened.seqAllocator,
            lamport: opened.lamport,
            commitWriter: opened.commitWriter,
            snapshotWriter: opened.snapshotWriter,
            liveness: maintenance.liveness,
            compactionPolicy: compactionPolicy,
            isLocalVolume: opened.isLocalVolume,
            metadataClient: metadataClient,
            ownsMetadataClient: ownsMetadataClient,
            initialMaterializeOutput: InitialMaterializeOutputBox(opened.initialMaterializeOutput),
            sweepTask: maintenance.sweepTask
        )
        try await RepoMaintenanceStartupRunner.runStartupRetentionIfEnabled(
            services: services,
            mode: maintenanceStartupMode
        )
        return services
    }
}

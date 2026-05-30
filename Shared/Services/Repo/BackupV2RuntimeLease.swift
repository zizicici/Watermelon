import Foundation

/// Unified V2 runtime open/shutdown boundary across foreground, background, and verify.
/// Wraps `BackupV2RuntimeBuilder.build` without changing its signature; absorbs each
/// call site's metadata-client ownership, startup mode, migration permission, and
/// error-normalization policy via three named factories.
struct BackupV2RuntimeLease: Sendable {
    let services: BackupV2RuntimeServices

    func shutdown() async {
        await services.shutdown()
    }
}

/// Background factory return shape. Distinguishes metadata-connect failures
/// (which preserve the current `profileConnectFailed` log path) from builder/open
/// failures (which dispatch through `handleRuntimeOpenFailure`).
enum BackgroundLeaseOpenFailure: Error, Sendable {
    /// `makeMetadataClient()` threw. Caller inspects via `RemoteWriteClassifier.isCancellation`
    /// to distinguish cancellation from non-cancellation, then logs `profileConnectFailed`
    /// for non-cancellation. Data client is NOT touched by the lease — caller owns it.
    case metadataConnect(Error)

    /// `BackupV2RuntimeBuilder.build()` threw. Lease has already disconnected the
    /// metadata client it created. Caller still disconnects the data client and
    /// dispatches the raw build error via `BackgroundBackupRunner.handleRuntimeOpenFailure(error, ...)`.
    case builderOpen(Error)
}

extension BackupV2RuntimeLease {

    /// Foreground entry point. Owns the metadata client lifecycle. Allows migration.
    /// `makeMetadataClient` is invoked once; its result is wrapped via `wrapIfSerial`.
    /// If `makeMetadataClient` throws (including connect-stage cancellation), the error
    /// propagates raw — NOT through compatibility mapping — matching current behavior at
    /// `BackupRunPreparation.prepareV2Runtime` where the raw makeClient/connect happens
    /// before `withCompatibilityMapping` is entered.
    static func forForegroundRun(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        databaseManager: DatabaseManager,
        format: RemoteFormatCompatibilityService,
        eventStream: BackupEventStream,
        makeMetadataClient: @Sendable () async throws -> any RemoteStorageClientProtocol
    ) async throws -> BackupV2RuntimeLease {
        let raw = try await makeMetadataClient()
        let metadataClient = wrapIfSerial(raw)
        let services = try await coreOpen(
            client: client,
            metadataClient: metadataClient,
            ownsMetadataClient: true,
            maintenanceStartupMode: .enabled,
            allowMigration: true,
            profile: profile,
            databaseManager: databaseManager,
            format: format,
            onMigrationStart: {
                eventStream.emitLog(String(localized: "backup.repo.migrationStarted"), level: .info)
            },
            onMigrationComplete: { processed in
                eventStream.emitLog(
                    String.localizedStringWithFormat(String(localized: "backup.repo.migrationCompleted"), processed),
                    level: .info
                )
            },
            onBootstrap: {
                eventStream.emitLog(String(localized: "backup.repo.bootstrapped"), level: .info)
            },
            errorMapping: .compatibilityMappingDisconnectOnError
        )
        return BackupV2RuntimeLease(services: services)
    }

    /// Background entry point. Owns the metadata client lifecycle. Disallows migration.
    /// Returns a typed `Result` so the caller can distinguish metadata-connect failures
    /// (which must preserve the current `profileConnectFailed` log path) from
    /// builder/open failures (which dispatch through `handleRuntimeOpenFailure`).
    static func forBackgroundRun(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        databaseManager: DatabaseManager,
        makeMetadataClient: @Sendable () async throws -> any RemoteStorageClientProtocol
    ) async -> Result<BackupV2RuntimeLease, BackgroundLeaseOpenFailure> {
        let raw: any RemoteStorageClientProtocol
        do {
            raw = try await makeMetadataClient()
        } catch {
            return .failure(.metadataConnect(error))
        }
        let metadataClient = wrapIfSerial(raw)
        do {
            let services = try await coreOpen(
                client: client,
                metadataClient: metadataClient,
                ownsMetadataClient: true,
                maintenanceStartupMode: .enabled,
                allowMigration: false,
                profile: profile,
                databaseManager: databaseManager,
                format: RemoteFormatCompatibilityService(),
                onMigrationStart: nil,
                onMigrationComplete: nil,
                onBootstrap: nil,
                errorMapping: .rawError
            )
            return .success(BackupV2RuntimeLease(services: services))
        } catch {
            await metadataClient.disconnectSafely()
            return .failure(.builderOpen(error))
        }
    }

    /// Verify entry point. Borrows the metadata client (caller already created and wrapped it).
    /// Disallows migration. Uses `.disabled(.verifyMonthTombstoneApply)` startup mode.
    /// Throws `BackupCompatibilityError` or `CancellationError` on build failure;
    /// the borrowed metadata client is NEVER disconnected by the lease.
    static func forVerifyMonth(
        client: any RemoteStorageClientProtocol,
        borrowedMetadataClient: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        databaseManager: DatabaseManager,
        format: RemoteFormatCompatibilityService
    ) async throws -> BackupV2RuntimeLease {
        let services = try await coreOpen(
            client: client,
            metadataClient: borrowedMetadataClient,
            ownsMetadataClient: false,
            maintenanceStartupMode: .disabled(.verifyMonthTombstoneApply),
            allowMigration: false,
            profile: profile,
            databaseManager: databaseManager,
            format: format,
            onMigrationStart: nil,
            onMigrationComplete: nil,
            onBootstrap: nil,
            errorMapping: .compatibilityMappingNoDisconnect
        )
        return BackupV2RuntimeLease(services: services)
    }

    private enum CoreErrorMapping {
        /// withCompatibilityMapping(disconnectOnError: true) — FG
        case compatibilityMappingDisconnectOnError
        /// withCompatibilityMapping(disconnectOnError: false) — verify
        case compatibilityMappingNoDisconnect
        /// No wrapping — BG factory carries the raw build error to the runner for disposition mapping.
        case rawError
    }

    private static func coreOpen(
        client: any RemoteStorageClientProtocol,
        metadataClient: any RemoteStorageClientProtocol,
        ownsMetadataClient: Bool,
        maintenanceStartupMode: RepoMaintenanceStartupMode,
        allowMigration: Bool,
        profile: ServerProfileRecord,
        databaseManager: DatabaseManager,
        format: RemoteFormatCompatibilityService,
        onMigrationStart: (() async -> Void)?,
        onMigrationComplete: ((Int) async -> Void)?,
        onBootstrap: (() async -> Void)?,
        errorMapping: CoreErrorMapping
    ) async throws -> BackupV2RuntimeServices {
        switch errorMapping {
        case .compatibilityMappingDisconnectOnError:
            return try await BackupV2RuntimeOpenErrorMapping.withCompatibilityMapping(
                metadataClient: metadataClient,
                disconnectOnError: true
            ) {
                try await BackupV2RuntimeBuilder.build(
                    client: client,
                    metadataClient: metadataClient,
                    ownsMetadataClient: ownsMetadataClient,
                    maintenanceStartupMode: maintenanceStartupMode,
                    profile: profile,
                    databaseManager: databaseManager,
                    format: format,
                    allowMigration: allowMigration,
                    onMigrationStart: onMigrationStart,
                    onMigrationComplete: onMigrationComplete,
                    onBootstrap: onBootstrap
                )
            }
        case .compatibilityMappingNoDisconnect:
            return try await BackupV2RuntimeOpenErrorMapping.withCompatibilityMapping(
                metadataClient: metadataClient,
                disconnectOnError: false
            ) {
                try await BackupV2RuntimeBuilder.build(
                    client: client,
                    metadataClient: metadataClient,
                    ownsMetadataClient: ownsMetadataClient,
                    maintenanceStartupMode: maintenanceStartupMode,
                    profile: profile,
                    databaseManager: databaseManager,
                    format: format,
                    allowMigration: allowMigration,
                    onMigrationStart: onMigrationStart,
                    onMigrationComplete: onMigrationComplete,
                    onBootstrap: onBootstrap
                )
            }
        case .rawError:
            return try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                ownsMetadataClient: ownsMetadataClient,
                maintenanceStartupMode: maintenanceStartupMode,
                profile: profile,
                databaseManager: databaseManager,
                format: format,
                allowMigration: allowMigration,
                onMigrationStart: onMigrationStart,
                onMigrationComplete: onMigrationComplete,
                onBootstrap: onBootstrap
            )
        }
    }
}

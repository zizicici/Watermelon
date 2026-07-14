import Foundation

enum RemoteLiteRepoGateway {
    struct WritePlan: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: RepoLeaseSession
        let monthsListing: LiteMonthsListingSnapshot
    }

    struct MaintenancePlan: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: RepoLeaseSession?
        let monthsListing: LiteMonthsListingSnapshot?
    }

    enum BackgroundOutcome {
        case proceed(WritePlan)
        case skip
    }

    static func prepareForegroundWrite(
        client: any RemoteStorageClientProtocol,
        lockClientHandle: LiteLockClientHandle,
        basePath: String,
        writerID: String?,
        allowsFreshOwnLockTakeover: Bool = false,
        freshOwnLockTakeoverScopes: Set<String> = [],
        ownLockTakeoverScope: String? = nil,
        now: Date = Date(),
        initialDecision: RepoFormatDecision? = nil,
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> WritePlan {
        lockClientHandle.transferToCoordinator()
        do {
            let outcome = try await LiteRepoTransitionEngine.prepareWrite(
                mode: .foreground,
                client: client,
                coordinator: remoteCoordinator(
                    client: client,
                    lockClientHandle: lockClientHandle,
                    reconnectLockClient: reconnectLockClient,
                    allowsFreshOwnLockTakeover: allowsFreshOwnLockTakeover,
                    freshOwnLockTakeoverScopes: freshOwnLockTakeoverScopes,
                    ownLockTakeoverScope: ownLockTakeoverScope,
                    onForeignWriterObserved: onForeignWriterObserved,
                    leaseDiagnosticLogger: leaseDiagnosticLogger
                ),
                basePath: basePath,
                writerID: writerID,
                now: now,
                initialDecision: initialDecision,
                onMigrationProgress: onMigrationProgress
            )
            return try requirePlan(outcome)
        } catch {
            await lockClientHandle.disconnectIfCoordinatorOwned()
            throw error
        }
    }

    static func prepareBackgroundWrite(
        client: any RemoteStorageClientProtocol,
        lockClientHandle: LiteLockClientHandle,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> BackgroundOutcome {
        lockClientHandle.transferToCoordinator()
        do {
            switch try await LiteRepoTransitionEngine.prepareWrite(
                mode: .background,
                client: client,
                coordinator: remoteCoordinator(
                    client: client,
                    lockClientHandle: lockClientHandle,
                    reconnectLockClient: reconnectLockClient,
                    allowsFreshOwnLockTakeover: false,
                    freshOwnLockTakeoverScopes: [],
                    ownLockTakeoverScope: nil,
                    onForeignWriterObserved: onForeignWriterObserved,
                    leaseDiagnosticLogger: leaseDiagnosticLogger
                ),
                basePath: basePath,
                writerID: writerID,
                now: now,
                initialDecision: nil,
                onMigrationProgress: onMigrationProgress
            ) {
            case .proceed(let plan):
                return .proceed(remotePlan(plan))
            case .skip:
                await lockClientHandle.disconnectIfCoordinatorOwned()
                return .skip
            }
        } catch {
            await lockClientHandle.disconnectIfCoordinatorOwned()
            throw error
        }
    }

    static func prepareMaintenance(
        client: any RemoteStorageClientProtocol,
        lockClientHandle: LiteLockClientHandle,
        basePath: String,
        writerID: String?,
        allowsFreshOwnLockTakeover: Bool = false,
        freshOwnLockTakeoverScopes: Set<String> = [],
        ownLockTakeoverScope: String? = nil,
        now: Date = Date(),
        reconnectLockClient: ConnectedLockClientProvider? = nil,
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        lockClientHandle.transferToCoordinator()
        do {
            let plan = try requirePlan(await LiteRepoTransitionEngine.prepareWrite(
                mode: .maintenance,
                client: client,
                coordinator: remoteCoordinator(
                    client: client,
                    lockClientHandle: lockClientHandle,
                    reconnectLockClient: reconnectLockClient,
                    allowsFreshOwnLockTakeover: allowsFreshOwnLockTakeover,
                    freshOwnLockTakeoverScopes: freshOwnLockTakeoverScopes,
                    ownLockTakeoverScope: ownLockTakeoverScope,
                    onForeignWriterObserved: onForeignWriterObserved,
                    leaseDiagnosticLogger: leaseDiagnosticLogger
                ),
                basePath: basePath,
                writerID: writerID,
                now: now,
                initialDecision: nil,
                onMigrationProgress: onMigrationProgress
            ))
            return MaintenancePlan(layout: plan.layout, session: plan.session, monthsListing: plan.monthsListing)
        } catch {
            await lockClientHandle.disconnectIfCoordinatorOwned()
            throw error
        }
    }

    static func prepareReload(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        writerID: String?,
        makeLockClient: @escaping @Sendable () async throws -> LiteLockClientHandle,
        allowsFreshOwnLockTakeover: Bool = false,
        freshOwnLockTakeoverScopes: Set<String> = [],
        ownLockTakeoverScope: String? = nil,
        now: Date = Date(),
        onForeignWriterObserved: (@Sendable () async -> Void)? = nil,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger? = nil,
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        switch try await LiteRepoTransitionEngine.reloadDisposition(client: client, basePath: basePath) {
        case .ready:
            return MaintenancePlan(layout: .lite, session: nil, monthsListing: LiteMonthsListingSnapshot())
        case .requiresWrite(let decision):
            let lock = try await makeLockClient()
            let plan = try await prepareForegroundWrite(
                client: client,
                lockClientHandle: lock,
                basePath: basePath,
                writerID: writerID,
                allowsFreshOwnLockTakeover: allowsFreshOwnLockTakeover,
                freshOwnLockTakeoverScopes: freshOwnLockTakeoverScopes,
                ownLockTakeoverScope: ownLockTakeoverScope,
                now: now,
                initialDecision: decision,
                reconnectLockClient: makeLockClient,
                onForeignWriterObserved: onForeignWriterObserved,
                leaseDiagnosticLogger: leaseDiagnosticLogger,
                onMigrationProgress: onMigrationProgress
            )
            return MaintenancePlan(layout: plan.layout, session: plan.session, monthsListing: plan.monthsListing)
        }
    }

    private static func requirePlan(
        _ outcome: LiteRepoTransitionEngine.PreparationOutcome<RepoLeaseSession>
    ) throws -> WritePlan {
        switch outcome {
        case .proceed(let plan):
            return remotePlan(plan)
        case .skip:
            throw LiteRepoError.repoDamaged
        }
    }

    private static func remotePlan(
        _ plan: LiteRepoTransitionEngine.WritePlan<RepoLeaseSession>
    ) -> WritePlan {
        WritePlan(layout: plan.layout, session: plan.session, monthsListing: plan.monthsListing)
    }

    private static func remoteCoordinator(
        client: any RemoteStorageClientProtocol,
        lockClientHandle: LiteLockClientHandle,
        reconnectLockClient: ConnectedLockClientProvider?,
        allowsFreshOwnLockTakeover: Bool,
        freshOwnLockTakeoverScopes: Set<String>,
        ownLockTakeoverScope: String?,
        onForeignWriterObserved: (@Sendable () async -> Void)?,
        leaseDiagnosticLogger: RepoLeaseDiagnosticLogger?
    ) -> RemoteRepoWriteCoordinator {
        RemoteRepoWriteCoordinator(
            client: client,
            lockClientHandle: lockClientHandle,
            reconnectLockClient: reconnectLockClient,
            allowsFreshOwnLockTakeover: allowsFreshOwnLockTakeover,
            freshOwnLockTakeoverScopes: freshOwnLockTakeoverScopes,
            ownLockTakeoverScope: ownLockTakeoverScope,
            onForeignWriterObserved: onForeignWriterObserved,
            diagnosticLogger: leaseDiagnosticLogger
        )
    }
}

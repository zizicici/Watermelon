import Foundation

enum LocalVolumeRepoGateway {
    struct WritePlan: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: LocalVolumeWriteSession
        let monthsListing: LiteMonthsListingSnapshot
    }

    struct MaintenancePlan: Sendable {
        let layout: MonthManifestStore.ManifestLayout
        let session: LocalVolumeWriteSession?
        let monthsListing: LiteMonthsListingSnapshot?
    }

    static func prepareForegroundWrite(
        client: LocalVolumeClient,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        initialDecision: RepoFormatDecision? = nil,
        upgradeExecution: RepoUpgradeExecution = RepoUpgradeExecution(),
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> WritePlan {
        try requirePlan(await LiteRepoTransitionEngine.prepareWrite(
            mode: .foreground,
            client: client,
            coordinator: LocalVolumeRepoWriteCoordinator(client: client),
            basePath: basePath,
            writerID: writerID,
            now: now,
            initialDecision: initialDecision,
            upgradeExecution: upgradeExecution,
            onMigrationProgress: onMigrationProgress
        ))
    }

    static func prepareMaintenance(
        client: LocalVolumeClient,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        upgradeExecution: RepoUpgradeExecution = RepoUpgradeExecution(),
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        let plan = try requirePlan(await LiteRepoTransitionEngine.prepareWrite(
            mode: .maintenance,
            client: client,
            coordinator: LocalVolumeRepoWriteCoordinator(client: client),
            basePath: basePath,
            writerID: writerID,
            now: now,
            initialDecision: nil,
            upgradeExecution: upgradeExecution,
            onMigrationProgress: onMigrationProgress
        ))
        return MaintenancePlan(layout: plan.layout, session: plan.session, monthsListing: plan.monthsListing)
    }

    static func prepareReload(
        client: LocalVolumeClient,
        basePath: String,
        writerID: String?,
        now: Date = Date(),
        upgradeExecution: RepoUpgradeExecution = RepoUpgradeExecution(),
        onMigrationProgress: (@Sendable (V1ToLiteMigrationProgress) async -> Void)? = nil
    ) async throws -> MaintenancePlan {
        switch try await LiteRepoTransitionEngine.reloadDisposition(client: client, basePath: basePath) {
        case .ready:
            return MaintenancePlan(layout: .lite, session: nil, monthsListing: LiteMonthsListingSnapshot())
        case .requiresWrite(let decision):
            let plan = try await prepareForegroundWrite(
                client: client,
                basePath: basePath,
                writerID: writerID,
                now: now,
                initialDecision: decision,
                upgradeExecution: upgradeExecution,
                onMigrationProgress: onMigrationProgress
            )
            return MaintenancePlan(layout: plan.layout, session: plan.session, monthsListing: plan.monthsListing)
        }
    }

    private static func requirePlan(
        _ outcome: LiteRepoTransitionEngine.PreparationOutcome<LocalVolumeWriteSession>
    ) throws -> WritePlan {
        switch outcome {
        case .proceed(let plan): return localPlan(plan)
        case .skip: throw LiteRepoError.repoDamaged
        }
    }

    private static func localPlan(
        _ plan: LiteRepoTransitionEngine.WritePlan<LocalVolumeWriteSession>
    ) -> WritePlan {
        WritePlan(layout: plan.layout, session: plan.session, monthsListing: plan.monthsListing)
    }
}

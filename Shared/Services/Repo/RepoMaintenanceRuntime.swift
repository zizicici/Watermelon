import Foundation

enum RepoMaintenanceStartupMode: Sendable, Equatable {
    case enabled
    case disabled(DisabledReason)

    enum DisabledReason: Sendable, Equatable {
        case verifyMonthTombstoneApply
        case test
    }

    var isEnabled: Bool {
        if case .enabled = self { return true }
        return false
    }
}

struct RepoMaintenanceRuntime: Sendable {}

struct RepoMaintenanceRuntimeBuilder: Sendable {
    func start(
        opened: OpenedBackupV2Repo,
        metadataClient: any RemoteStorageClientProtocol,
        mode: RepoMaintenanceStartupMode
    ) async throws -> RepoMaintenanceRuntime {
        guard mode.isEnabled else {
            return RepoMaintenanceRuntime()
        }

        _ = try await OrphanMetadataCleanup.sweepOwnStagings(
            client: metadataClient,
            basePath: opened.basePath,
            writerID: opened.writerID
        )
        try Task.checkCancellation()

        return RepoMaintenanceRuntime()
    }
}

struct RepoMaintenanceStartupRunner: Sendable {
    static func runStartupRetentionIfEnabled(
        services: BackupV2RuntimeServices,
        mode: RepoMaintenanceStartupMode
    ) async throws {
        guard mode.isEnabled else { return }
        do {
            let compaction = RepoCompactionService(services: services)
            // Heal corrupt-snapshot-only months first so they re-materialize clean and the
            // subsequent compaction pass can run normal GC on the fresh baseline.
            _ = try await compaction.repairCorruptSnapshotBaselines()
            _ = try await compaction.compactStartupMonths()
        } catch is CancellationError {
            await services.shutdown()
            throw CancellationError()
        } catch {
        }
    }
}

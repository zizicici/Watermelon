import Foundation
import os.log

private let maintenanceRuntimeLog = Logger(
    subsystem: "com.zizicici.watermelon",
    category: "RepoMaintenanceRuntime"
)

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
    @discardableResult
    static func runStartupRetentionIfEnabled(
        services: BackupV2RuntimeServices,
        mode: RepoMaintenanceStartupMode
    ) async throws -> RepoMaintenanceStartupDiagnostic {
        guard mode.isEnabled else { return .disabled(mode) }
        var diagnostic = RepoMaintenanceStartupDiagnostic(mode: mode, ran: true)
        var stage: RepoMaintenanceStartupStage = .repair
        do {
            let compaction = RepoCompactionService(services: services)
            // Heal corrupt-snapshot-only months first so they re-materialize clean and the
            // subsequent compaction pass can run normal GC on the fresh baseline.
            diagnostic.repairedCount = try await compaction.repairCorruptSnapshotBaselines()
            stage = .startupCompaction
            diagnostic.startupResult = try await compaction.compactStartupMonths()
        } catch is CancellationError {
            await services.shutdown()
            throw CancellationError()
        } catch {
            // Non-cancellation failures stay best-effort: capture for observability, never fail open.
            diagnostic.failureStage = stage
            diagnostic.failureDescription = String(describing: error)
            maintenanceRuntimeLog.error("startup maintenance \(String(describing: stage), privacy: .public) failed: \(String(describing: error), privacy: .public)")
        }
        return diagnostic
    }
}

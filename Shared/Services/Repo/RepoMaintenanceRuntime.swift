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

struct RepoMaintenanceRuntime: Sendable {
    let liveness: LivenessTracker
    let sweepTask: Task<Void, Never>?
}

struct RepoMaintenanceRuntimeBuilder: Sendable {
    func start(
        opened: OpenedBackupV2Repo,
        metadataClient: any RemoteStorageClientProtocol,
        mode: RepoMaintenanceStartupMode
    ) async throws -> RepoMaintenanceRuntime {
        let liveness = LivenessTracker(
            client: metadataClient,
            basePath: opened.basePath,
            writerID: opened.writerID,
            isLocalVolume: opened.isLocalVolume
        )
        guard mode.isEnabled else {
            return RepoMaintenanceRuntime(liveness: liveness, sweepTask: nil)
        }

        _ = try await OrphanMetadataCleanup.sweepOwnLivenessStagings(
            client: metadataClient,
            basePath: opened.basePath,
            writerID: opened.writerID
        )
        try Task.checkCancellation()
        await liveness.start()
        do {
            try Task.checkCancellation()
        } catch {
            await liveness.stopAndWait()
            throw CancellationError()
        }

        guard metadataClient.supportsLivenessSafeRenewal else {
            return RepoMaintenanceRuntime(liveness: liveness, sweepTask: nil)
        }

        do {
            let view = try await liveness.snapshotPeerStatuses()
            try Task.checkCancellation()
            guard view.isComplete else {
                return RepoMaintenanceRuntime(liveness: liveness, sweepTask: nil)
            }
            var protectedWriters = view.sweepProtectionSet
            protectedWriters.insert(opened.writerID)
            let sweepTask = Task(priority: .utility) { [metadataClient, protectedWriters, basePath = opened.basePath] in
                _ = await OrphanMetadataCleanup.sweep(
                    client: metadataClient,
                    directories: OrphanMetadataCleanup.standardSweepDirectories(basePath: basePath),
                    activeWriters: protectedWriters,
                    ageThresholdSeconds: 3600,
                    now: Date()
                )
            }
            return RepoMaintenanceRuntime(liveness: liveness, sweepTask: sweepTask)
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                await liveness.stopAndWait()
                throw CancellationError()
            }
            return RepoMaintenanceRuntime(liveness: liveness, sweepTask: nil)
        }
    }
}

struct RepoMaintenanceStartupRunner: Sendable {
    static func runStartupRetentionIfEnabled(
        services: BackupV2RuntimeServices,
        mode: RepoMaintenanceStartupMode
    ) async throws {
        guard mode.isEnabled else { return }
        do {
            _ = try await RepoRetentionStartupMaintenance(services: services).run()
        } catch is CancellationError {
            await services.shutdown()
            throw CancellationError()
        } catch {
        }
    }
}

//
//  BackgroundBackupRunner.swift
//  Watermelon
//

import Foundation
import Network
import Photos
import Security
import MoreKit

// Background backup is the foreground pipeline (`BackupCoordinator.runBackup`) scoped to the most recent
// months, upload-only, single worker. This type is only the outer orchestrator: Pro/Wi-Fi gating, the
// multi-profile loop, per-profile cooldown, and session logging.
final class BackgroundBackupRunner {
    static let taskIdentifier = "com.zizicici.watermelon.background-backup"

    static let flushInterval = 10
    private static let recentMonthCount = 2
    private static let profileCooldownHours = 18
    private static let profileCooldownInterval: TimeInterval = TimeInterval(profileCooldownHours) * 60 * 60

    private let databaseManager: DatabaseManager
    private let keychainService: KeychainService
    private let storageClientFactory: StorageClientFactory
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository

    init(dependencies: DependencyContainer) {
        self.databaseManager = dependencies.databaseManager
        self.keychainService = dependencies.keychainService
        self.storageClientFactory = dependencies.storageClientFactory
        self.photoLibraryService = dependencies.photoLibraryService
        self.hashIndexRepository = dependencies.hashIndexRepository
    }

    func run() async {
        guard await ProStatus.verifyEntitlement() else { return }
        guard BackgroundBackupSetting.getValue() == .enable else { return }
        guard await isWiFiAvailable() else { return }

        guard let profiles = try? databaseManager.fetchBackgroundBackupEnabledProfiles(),
              !profiles.isEmpty else { return }

        // Bail before touching any remote when there is nothing recent to back up. Uses the same scope resolver
        // as the run; both re-evaluate `now`, so across a midnight month rollover the windows can differ by one
        // month — benign (either the run finds nothing, or the next run picks it up).
        guard let scope = BackupRunPreparationService.resolveMonthScope(.recentMonths(Self.recentMonthCount)) else { return }
        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "creationDate >= %@", scope.cutoff as NSDate)
        guard PHAsset.fetchAssets(with: options).count > 0 else { return }

        let writer = ExecutionLogFileStore.beginSession(kind: .auto)
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.sessionStart"), profiles.count, Self.recentMonthCount),
            level: .info
        )

        for profile in profiles.shuffled() {
            if Task.isCancelled { break }
            if await shouldSkipProfileForCooldown(profile, writer: writer) {
                continue
            }
            let result = await backupProfile(profile, writer: writer)
            if result == .completed {
                markProfileCompleted(profile)
            }
        }

        await writer.appendLog(String(localized: "backup.auto.log.sessionEnd"), level: .info)
        await writer.finalize()
    }

    // MARK: - Per-Profile Backup

    private enum ProfileRunResult: Equatable {
        case completed
        case failed
        case skipped
        case cancelled
    }

    private func backupProfile(
        _ profile: ServerProfileRecord,
        writer: ExecutionLogSessionWriter
    ) async -> ProfileRunResult {
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.profileStart"), profile.name),
            level: .info
        )

        let password: String
        if profile.storageProfile.requiresPassword {
            do {
                password = try keychainService.readPassword(account: profile.credentialRef)
            } catch KeychainError.unhandled(let status) where status == errSecItemNotFound {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profileMissingCredentials"), profile.name),
                    level: .warning
                )
                return .skipped
            } catch {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profileCredentialsReadFailed"), profile.name, error.localizedDescription),
                    level: .warning
                )
                return .skipped
            }
        } else {
            password = ""
        }

        // Drain the run's events into the session log; the executor never finishes the stream itself.
        // Also reports whether the run got past preparation: `.started` is emitted only after prepareRun
        // succeeds — i.e. after any durable prepare-phase remote change (V1→Lite migration, version recovery,
        // cleanup) and before upload. Observed even when the run later throws (events precede the throw), so it
        // gates markProfileRan precisely: connect/prepare FAILURES throw before `.started` (no mark), while
        // every run that mutated remote state — prepare changes, uploads, or upload-then-fail — marks.
        let eventStream = BackupEventStream()
        let drainTask = Task.detached { () -> Bool in
            var didStart = false
            for await event in eventStream.stream {
                switch event {
                case .log(let message, let level):
                    await writer.appendLog(message, level: level)
                case .progress(let progress):
                    await writer.appendLog(progress.effectiveLogMessage, level: progress.logLevel)
                case .started:
                    didStart = true
                case .finished, .transferState, .monthChanged:
                    break
                }
            }
            return didStart
        }

        let coordinator = BackupCoordinator(
            photoLibraryService: photoLibraryService,
            storageClientFactory: storageClientFactory,
            hashIndexRepository: hashIndexRepository,
            databaseManager: databaseManager
        )
        let request = BackupRunRequest(
            profile: profile,
            password: password,
            onlyAssetLocalIdentifiers: nil,
            workerCountOverride: 1,
            iCloudPhotoBackupMode: ICloudPhotoBackupMode.getValue(),
            monthScope: .recentMonths(Self.recentMonthCount),
            monthOrdering: .newestMonthFirst,
            leaseMode: .background,
            incrementalFlushInterval: Self.flushInterval,
            onMonthUploaded: nil
        )

        let result: BackupExecutionResult?
        let caughtError: Error?
        do {
            result = try await coordinator.runBackup(request: request, eventStream: eventStream)
            caughtError = nil
        } catch {
            result = nil
            caughtError = error
        }
        eventStream.finish()
        let runStarted = await drainTask.value

        if caughtError is BackupRunSkipped {
            await writer.appendLog(
                String(format: String(localized: "backup.repo.backgroundSkipped"), profile.name),
                level: .info
            )
            return .skipped
        }

        // Record a run once it gets past preparation (`.started`). This covers every path that can mutate
        // remote state — prepare-phase migration/recovery/cleanup, uploads, and upload-then-fail — so the
        // foreground refreshes; a connect/prepare failure throws before `.started` and is correctly skipped.
        if runStarted {
            markProfileRan(profile)
        }

        // Classify wrapped cancellations too (e.g. LiteRepoError.probeFault(.cancelled) thrown during prepare
        // without the outer task being cancelled), so they read as cancelled rather than failed.
        if let caughtError, caughtError is CancellationError || RemoteFaultLite.classify(caughtError) == .cancelled || Task.isCancelled {
            return .cancelled
        }
        if Task.isCancelled || (result?.paused ?? false) {
            return .cancelled
        }
        if caughtError != nil || (result?.failed ?? 0) > 0 {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFailed"), profile.name),
                level: .error
            )
            return .failed
        }

        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.profileEnd"), profile.name),
            level: .info
        )
        return .completed
    }

    private func shouldSkipProfileForCooldown(
        _ profile: ServerProfileRecord,
        writer: ExecutionLogSessionWriter
    ) async -> Bool {
        guard let profileID = profile.id,
              let lastCompletedAt = try? databaseManager.backgroundBackupLastCompletedAt(profileID: profileID) else {
            return false
        }
        guard Date().timeIntervalSince(lastCompletedAt) < Self.profileCooldownInterval else {
            return false
        }
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.profileCooldownSkip"), profile.name, Self.profileCooldownHours),
            level: .info
        )
        return true
    }

    private func markProfileCompleted(_ profile: ServerProfileRecord) {
        guard let profileID = profile.id, liveDestinationMatches(profile) else { return }
        try? databaseManager.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
    }

    private func markProfileRan(_ profile: ServerProfileRecord) {
        guard let profileID = profile.id, liveDestinationMatches(profile) else { return }
        try? databaseManager.setBackgroundBackupLastRanAt(Date(), profileID: profileID)
    }

    // A foreground edit can repoint the captured profile id to a new remote mid-run; never stamp this run's
    // markers against the new endpoint they did not back up.
    private func liveDestinationMatches(_ captured: ServerProfileRecord) -> Bool {
        guard let id = captured.id,
              let live = try? databaseManager.fetchServerProfiles().first(where: { $0.id == id }) else {
            return false
        }
        return live.backgroundRunDestinationIdentity == captured.backgroundRunDestinationIdentity
    }

    // MARK: - Wi-Fi Check

    private func isWiFiAvailable() async -> Bool {
        await withCheckedContinuation { continuation in
            let monitor = NWPathMonitor(requiredInterfaceType: .wifi)
            // NWPathMonitor fires on every path change and cancel() can't dequeue an already-queued callback, so resume only once.
            let resumed = ResumeOnceFlag()
            monitor.pathUpdateHandler = { path in
                guard resumed.set() else { return }
                monitor.cancel()
                continuation.resume(returning: path.status == .satisfied)
            }
            monitor.start(queue: DispatchQueue(label: "bg-backup.wifi-check"))
        }
    }
}

extension ServerProfileRecord {
    // Remote destination a background run wrote to; markers stay valid only while this is unchanged.
    var backgroundRunDestinationIdentity: [String] {
        [
            storageType,
            host,
            String(port),
            shareName,
            basePath,
            username,
            domain ?? "",
            connectionParams?.base64EncodedString() ?? ""
        ]
    }
}

private final class ResumeOnceFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var fired = false

    func set() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !fired else { return false }
        fired = true
        return true
    }
}

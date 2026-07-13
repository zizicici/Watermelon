//
//  BackgroundBackupRunner.swift
//  Watermelon
//

import Foundation
import Network
import Security
import MoreKit

// Background backup is the foreground pipeline (`BackupCoordinator.runBackup`) scoped to the most recent
// months, upload-only, single worker. This type is only the outer orchestrator: Pro/Wi-Fi gating, the
// multi-profile loop, per-profile cooldown, and session logging.
final class BackgroundBackupRunner {
    static let taskIdentifier = "com.zizicici.watermelon.background-backup"

    static let flushInterval = 10
    private static let recentMonthCount = 2

    private let databaseManager: DatabaseManager
    private let keychainService: KeychainService
    private let storageClientFactory: StorageClientFactory
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository
    private let appRuntimeFlags: AppRuntimeFlags

    init(dependencies: DependencyContainer) {
        self.databaseManager = dependencies.databaseManager
        self.keychainService = dependencies.keychainService
        self.storageClientFactory = dependencies.storageClientFactory
        self.photoLibraryService = dependencies.photoLibraryService
        self.hashIndexRepository = dependencies.hashIndexRepository
        self.appRuntimeFlags = dependencies.appRuntimeFlags
    }

    func run() async {
        guard await ProStatus.verifyEntitlement() else { return }
        guard BackgroundBackupSetting.getValue() == .enable else { return }

        guard let preflightProfiles = try? databaseManager.fetchBackgroundBackupEnabledProfiles(),
              !preflightProfiles.isEmpty else { return }

        // Stay silent (no session log) when nothing is runnable right now — cooling down, offline, or needing
        // Wi-Fi we don't have. Keeps opportunistic wake-ups from spamming near-empty auto sessions.
        let net = await currentNetwork()
        guard preflightProfiles.contains(where: { isEligibleNow($0, net) }) else { return }

        guard appRuntimeFlags.tryEnterExecution() else { return }
        defer { appRuntimeFlags.exitExecution() }

        guard let profiles = try? databaseManager.fetchBackgroundBackupEnabledProfiles(),
              !profiles.isEmpty else { return }
        let orderedProfiles = profiles.shuffled()
        guard containsRunnableLiveProfile(orderedProfiles, net: net) else { return }

        // Freeze the session window now, but defer PhotoKit until a profile owns the remote write lease.
        let monthScopeNow = Date()
        let monthGroupingTimeZone = MonthGroupingTimeZonePreference.frozenCurrent()
        let monthCalendar = LibraryMonthKey.monthCalendar(preference: monthGroupingTimeZone)
        guard let scope = BackupRunPreparationService.resolveMonthScope(
            .recentMonths(Self.recentMonthCount),
            now: monthScopeNow,
            calendar: monthCalendar
        ) else { return }
        let monthAssetIDsCache = BackupMonthAssetIDsCache { [photoLibraryService] in
            let assetsResult = photoLibraryService.fetchAssetsResult(
                ascendingByCreationDate: true,
                since: scope.cutoff
            )
            return BackupMonthScheduler.buildMonthAssetIDsByMonth(
                from: assetsResult,
                calendar: monthCalendar
            ).filter { scope.months.contains($0.key) }
        }

        let writer = ExecutionLogFileStore.beginSession(kind: .auto)
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.sessionStart"), profiles.count, Self.recentMonthCount),
            level: .info
        )
        for line in AppExitMetricsMonitor.consumeSummaryLines() {
            await writer.appendLog(line, level: .debug)
        }
        await writer.appendLog(await MemoryDiagnostics.watermarkLine(), level: .debug)
        let memoryWatermarkTask = Task {
            while !Task.isCancelled {
                do {
                    try await Task.sleep(nanoseconds: MemoryDiagnostics.watermarkIntervalNanos)
                } catch {
                    return
                }
                let line = await MemoryDiagnostics.watermarkLine()
                guard !Task.isCancelled else { return }
                await writer.appendLog(line, level: .debug)
            }
        }
        defer { memoryWatermarkTask.cancel() }

        for capturedProfile in orderedProfiles {
            if Task.isCancelled { break }
            guard let profileID = capturedProfile.id,
                  let profile = try? databaseManager.fetchServerProfile(id: profileID),
                  profile.backgroundBackupEnabled else { continue }
            if isProfileCoolingDown(profile) {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profileCooldownSkip"), profile.name, profile.backgroundBackupMinIntervalMinutes / 60),
                    level: .info
                )
                continue
            }
            let liveNet = await currentNetwork()
            guard let latestProfile = try? databaseManager.fetchServerProfile(id: profileID),
                  latestProfile.backgroundBackupEnabled else { continue }
            if isProfileCoolingDown(latestProfile) {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profileCooldownSkip"), latestProfile.name, latestProfile.backgroundBackupMinIntervalMinutes / 60),
                    level: .info
                )
                continue
            }
            guard liveNet.hasConnectivity else { continue }
            if latestProfile.backgroundBackupRequiresWiFi, !liveNet.isUnmetered {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profileWiFiSkip"), latestProfile.name),
                    level: .info
                )
                continue
            }
            let result = await backupProfile(
                latestProfile,
                writer: writer,
                monthAssetIDsCache: monthAssetIDsCache,
                monthGroupingTimeZone: monthGroupingTimeZone,
                monthScopeNow: monthScopeNow
            )
            if result == .completed {
                markProfileCompleted(latestProfile)
            }
        }

        memoryWatermarkTask.cancel()
        await memoryWatermarkTask.value
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
        writer: ExecutionLogSessionWriter,
        monthAssetIDsCache: BackupMonthAssetIDsCache,
        monthGroupingTimeZone: MonthGroupingTimeZonePreference,
        monthScopeNow: Date
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
                case .started(_, _):
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
            monthAssetIDsProvider: { await monthAssetIDsCache.load() },
            monthOrdering: .newestMonthFirst,
            leaseMode: .background,
            incrementalFlushInterval: Self.flushInterval,
            monthGroupingTimeZone: monthGroupingTimeZone,
            monthScopeNow: monthScopeNow,
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

    private func isProfileCoolingDown(_ profile: ServerProfileRecord) -> Bool {
        guard let profileID = profile.id,
              let lastCompletedAt = try? databaseManager.backgroundBackupLastCompletedAt(profileID: profileID) else {
            return false
        }
        let interval = TimeInterval(max(1, profile.backgroundBackupMinIntervalMinutes)) * 60
        return Date().timeIntervalSince(lastCompletedAt) < interval
    }

    private func isEligibleNow(_ profile: ServerProfileRecord, _ net: (hasConnectivity: Bool, isUnmetered: Bool)) -> Bool {
        net.hasConnectivity && !isProfileCoolingDown(profile) && (net.isUnmetered || !profile.backgroundBackupRequiresWiFi)
    }

    private func containsRunnableLiveProfile(
        _ profiles: [ServerProfileRecord],
        net: (hasConnectivity: Bool, isUnmetered: Bool)
    ) -> Bool {
        profiles.contains { captured in
            guard let profileID = captured.id,
                  let live = try? databaseManager.fetchServerProfile(id: profileID),
                  live.backgroundBackupEnabled else { return false }
            return isEligibleNow(live, net)
        }
    }

    private func markProfileCompleted(_ profile: ServerProfileRecord) {
        guard let profileID = profile.id, liveDestinationMatches(profile) else { return }
        try? databaseManager.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
        notifyRunMarkerDidChange()
    }

    private func markProfileRan(_ profile: ServerProfileRecord) {
        guard let profileID = profile.id, liveDestinationMatches(profile) else { return }
        try? databaseManager.setBackgroundBackupLastRanAt(Date(), profileID: profileID)
        notifyRunMarkerDidChange()
    }

    // Wake an active foreground Home to re-run background-fingerprint pickup now that a marker landed, instead
    // of waiting for the next activation/execution-end/maintenance-end. A no-op when no foreground store observes.
    private func notifyRunMarkerDidChange() {
        NotificationCenter.default.post(name: .BackgroundBackupRunMarkerDidChange, object: nil)
    }

    // A foreground edit can repoint the captured profile id to a new remote mid-run; never stamp this run's
    // markers against the new endpoint they did not back up.
    private func liveDestinationMatches(_ captured: ServerProfileRecord) -> Bool {
        guard let id = captured.id,
              let live = try? databaseManager.fetchServerProfiles().first(where: { $0.id == id }) else {
            return false
        }
        return live.remoteDestinationIdentity == captured.remoteDestinationIdentity
    }

    // MARK: - Network Check

    // `isUnmetered` = satisfied and not expensive — treats Ethernet/Wi-Fi as OK and personal hotspot/cellular as not,
    // matching "avoid cellular charges" better than checking the Wi-Fi interface. Times out to offline so a stuck
    // monitor can't hang the expiry-bounded BGTask.
    private func currentNetwork() async -> (hasConnectivity: Bool, isUnmetered: Bool) {
        await withCheckedContinuation { continuation in
            let monitor = NWPathMonitor()
            let queue = DispatchQueue(label: "bg-backup.net-check")
            // NWPathMonitor fires on every path change and cancel() can't dequeue an already-queued callback, so resume only once.
            let resumed = ResumeOnceFlag()
            monitor.pathUpdateHandler = { path in
                guard resumed.set() else { return }
                monitor.cancel()
                continuation.resume(returning: (path.status == .satisfied, path.status == .satisfied && !path.isExpensive))
            }
            queue.asyncAfter(deadline: .now() + 3) {
                guard resumed.set() else { return }
                monitor.cancel()
                continuation.resume(returning: (false, false))
            }
            monitor.start(queue: queue)
        }
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

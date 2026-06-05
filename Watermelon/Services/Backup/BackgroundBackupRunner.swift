//
//  BackgroundBackupRunner.swift
//  Watermelon
//

import Foundation
import Network
import Photos
import Security
import MoreKit

enum BackgroundRuntimeOpenFailureDisposition: Equatable {
    case skippedForegroundMigration
    case failedUnsupportedRemoteFormat(minAppVersion: String?)
    case failedRepoIdentityMismatch
    case failedRepoFormatRegression
    case failedDamagedV2Repo
    case failedProfileMissingID
    case cancelled
    case failedTransientRemoteFailure
    case skippedOther
}

final class BackgroundBackupRunner {
    static let taskIdentifier = "com.zizicici.watermelon.background-backup"

    private static let flushInterval = BackupV2Constants.batchFlushInterval
    private static let recentMonthCount = 2
    private static let profileCooldownHours = 18
    private static let profileCooldownInterval: TimeInterval = TimeInterval(profileCooldownHours) * 60 * 60

    static func backgroundIntervalFlushIgnoresCancellation() -> Bool {
        false
    }

    static func backgroundFinalFlushIgnoresCancellation() -> Bool {
        true
    }

    /// A `.cancelled`-classified EOM flush is silent only on genuine teardown; with
    /// `ignoreCancellation` a transport NSURLErrorCancelled (mapped to a bare CancellationError by
    /// CommitLogWriter) can surface on a live task, where it is a real flush failure.
    static func backgroundEndOfMonthCancelledIsRealFailure(taskIsCancelled: Bool) -> Bool {
        !taskIsCancelled
    }

    private let databaseManager: DatabaseManager
    private let keychainService: KeychainService
    private let storageClientFactory: StorageClientFactory
    private let photoLibraryService: PhotoLibraryService
    private let hashIndexRepository: ContentHashIndexRepository
    private let assetProcessor: AssetProcessor

    init(dependencies: DependencyContainer) {
        self.databaseManager = dependencies.databaseManager
        self.keychainService = dependencies.keychainService
        self.storageClientFactory = dependencies.storageClientFactory
        self.photoLibraryService = dependencies.photoLibraryService
        self.hashIndexRepository = dependencies.hashIndexRepository

        let remoteIndexService = RemoteIndexSyncService()
        self.assetProcessor = AssetProcessor(
            photoLibraryService: photoLibraryService,
            hashIndexRepository: hashIndexRepository,
            remoteIndexService: remoteIndexService
        )
    }

    func run() async -> Bool {
        guard await ProStatus.verifyEntitlement() else { return true }
        guard BackgroundBackupSetting.getValue() == .enable else { return true }
        // Atomic process-wide claim: refuses to start while a foreground run or manual verify is
        // active, AND blocks foreground/verify from starting while this BG run owns the lease.
        // Released via `defer` so every exit (early-return, throw inside awaits, cancellation)
        // clears the flag — otherwise the lease would strand and block subsequent foreground work.
        let runtimeFlags = AppRuntimeFlags.shared
        guard runtimeFlags.tryBeginExecution() else { return true }
        defer { runtimeFlags.setExecuting(false) }
        guard await isWiFiAvailable() else { return true }

        let profiles: [ServerProfileRecord]
        do {
            profiles = try databaseManager.fetchBackgroundBackupEnabledProfiles()
        } catch {
            return false
        }
        guard !profiles.isEmpty else { return true }

        guard let cutoff = Calendar.current.date(
            byAdding: .month,
            value: -(Self.recentMonthCount - 1),
            to: Date()
        )?.startOfMonth() else { return true }

        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "creationDate >= %@", cutoff as NSDate)
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
        let recentAssets = PHAsset.fetchAssets(with: options)
        guard recentAssets.count > 0 else { return true }

        let monthAssetIDs = BackupMonthScheduler.buildMonthAssetIDsByMonth(from: recentAssets)
        let sortedMonths = monthAssetIDs.keys.sorted(by: >)

        let writer = ExecutionLogFileStore.beginSession(kind: .auto)
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.sessionStart"), profiles.count, sortedMonths.count),
            level: .info
        )

        var anyProfileFailed = false
        for profile in profiles.shuffled() {
            if Task.isCancelled { break }
            if await shouldSkipProfileForCooldown(profile, writer: writer) {
                continue
            }
            let result = await backupProfile(profile, monthAssetIDs: monthAssetIDs, sortedMonths: sortedMonths, writer: writer)
            if result == .completed {
                markProfileCompleted(profile)
            } else if result == .failed {
                anyProfileFailed = true
            }
        }

        await writer.appendLog(String(localized: "backup.auto.log.sessionEnd"), level: .info)
        await writer.finalize()
        if Task.isCancelled { return false }
        return !anyProfileFailed
    }

    // MARK: - Per-Profile Backup

    private enum ProfileRunResult: Equatable {
        case completed
        case failed
        case skipped
        case cancelled
    }

    static func runtimeOpenFailureDisposition(
        _ error: Error
    ) -> BackgroundRuntimeOpenFailureDisposition {
        if RemoteWriteClassifier.isCancellation(error) {
            return .cancelled
        }
        if let buildError = error as? BackupV2RuntimeBuildError {
            switch buildError {
            case .requiresForegroundMigration:
                return .skippedForegroundMigration
            case .unsupportedRemoteFormat(let minAppVersion):
                return .failedUnsupportedRemoteFormat(minAppVersion: minAppVersion)
            case .repoIdentityMismatch:
                return .failedRepoIdentityMismatch
            case .repoFormatRegression:
                return .failedRepoFormatRegression
            case .damagedV2Repo:
                return .failedDamagedV2Repo
            case .profileMissingID:
                return .failedProfileMissingID
            }
        }
        if RemoteWriteClassifier.isTransientVerifyFailure(error) {
            return .failedTransientRemoteFailure
        }
        return .skippedOther
    }

    private func backupProfile(
        _ profile: ServerProfileRecord,
        monthAssetIDs: [LibraryMonthKey: [PhotoKitLocalIdentifier]],
        sortedMonths: [LibraryMonthKey],
        writer: ExecutionLogSessionWriter
    ) async -> ProfileRunResult {
        // BG runner reuses one RemoteIndexSyncService across profiles; mixed-profile state would corrupt resume coverage.
        await assetProcessor.remoteIndexService.resetForProfileSwitch()
        // Same reason for the hash-index intent queue (keyed by month, not profile): leftover
        // intents from profile A would drain against profile B's commit deltas.
        await assetProcessor.clearAllPendingHashIndexIntents()
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.profileStart"), profile.name),
            level: .info
        )
        let password: String
        if profile.storageProfile.requiresPassword {
            do {
                let pw = try keychainService.readPassword(account: profile.credentialRef)
                password = pw
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

        let client: any RemoteStorageClientProtocol
        do {
            client = try storageClientFactory.makeClient(profile: profile, password: password)
            try await client.connect()
        } catch {
            if RemoteWriteClassifier.isCancellation(error) {
                return .cancelled
            }
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                level: .error
            )
            return .failed
        }

        let lease: BackupV2RuntimeLease
        switch await BackupV2RuntimeLease.forBackgroundRun(
            client: client,
            profile: profile,
            databaseManager: databaseManager,
            makeMetadataClient: { [storageClientFactory, profile, password] in
                let raw = try storageClientFactory.makeClient(profile: profile, password: password)
                try await raw.connect()
                return raw
            }
        ) {
        case .success(let openedLease):
            lease = openedLease
        case .failure(.metadataConnect(let error)):
            if RemoteWriteClassifier.isCancellation(error) {
                await client.disconnectSafely()
                return .cancelled
            }
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                level: .error
            )
            await client.disconnectSafely()
            return .failed
        case .failure(.builderOpen(let error)):
            await client.disconnectSafely()
            return await handleRuntimeOpenFailure(error, profile: profile, writer: writer)
        }
        let v2Services = lease.services

        do {
            let preMaterialized = await v2Services.initialMaterializeOutput.peek()
            // expectV2 rejects lingering V1 data instead of routing backup sync through V1.
            _ = try await assetProcessor.remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                preMaterialized: preMaterialized,
                preInspection: v2Services.postOpenSyncInspection,
                expectV2: true,
                localRepoID: v2Services.repoID
            )
            _ = await v2Services.initialMaterializeOutput.consume()
            await assetProcessor.remoteIndexService.markIsV2()
        } catch is CancellationError {
            await v2Services.shutdown()
            await client.disconnectSafely()
            return .cancelled
        } catch let compatError as BackupCompatibilityError {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatInspectFailed"), profile.name, compatError.localizedDescription),
                level: .error
            )
            await v2Services.shutdown()
            await client.disconnectSafely()
            return .failed
        } catch {
            if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                    level: .error
                )
                await v2Services.shutdown()
                await client.disconnectSafely()
                return .failed
            }
            // Non-transient sync failure: committed-view baseline is unknown. Skip rather than process months against stale or partial data.
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatInspectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                level: .error
            )
            await v2Services.shutdown()
            await client.disconnectSafely()
            return .failed
        }

        let anyMonthFailed = await runBackupLoop(client: client, profile: profile, monthAssetIDs: monthAssetIDs, sortedMonths: sortedMonths, writer: writer, v2Services: v2Services)
        await v2Services.shutdown()
        await client.disconnectSafely()
        if Task.isCancelled {
            return .cancelled
        }
        if anyMonthFailed {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFailed"), profile.name),
                level: .error
            )
            return .failed
        } else {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileEnd"), profile.name),
                level: .info
            )
            return .completed
        }
    }

    private func handleRuntimeOpenFailure(
        _ error: Error,
        profile: ServerProfileRecord,
        writer: ExecutionLogSessionWriter
    ) async -> ProfileRunResult {
        switch Self.runtimeOpenFailureDisposition(error) {
        case .skippedForegroundMigration:
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileNeedsForegroundMigration"), profile.name),
                level: .warning
            )
            return .skipped
        case .failedUnsupportedRemoteFormat(let minAppVersion):
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatUnsupported"), profile.name, minAppVersion ?? "?"),
                level: .error
            )
            return .failed
        case .failedRepoIdentityMismatch:
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileRepoIdentityMismatch"), profile.name),
                level: .error
            )
            return .failed
        case .failedRepoFormatRegression:
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileRepoFormatRegression"), profile.name),
                level: .error
            )
            return .failed
        case .failedDamagedV2Repo:
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatInspectFailed"), profile.name, BackupCompatibilityError.damagedV2Repo.errorDescription ?? ""),
                level: .error
            )
            return .failed
        case .failedProfileMissingID:
            let mapped = BackupV2RuntimeOpenErrorMapping.compatibilityError(for: error)
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatInspectFailed"), profile.name, mapped.localizedDescription),
                level: .error
            )
            return .failed
        case .cancelled:
            return .cancelled
        case .failedTransientRemoteFailure:
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                level: .error
            )
            return .failed
        case .skippedOther:
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatInspectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                level: .warning
            )
            return .skipped
        }
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
        guard let profileID = profile.id else { return }
        try? databaseManager.setBackgroundBackupLastCompletedAt(Date(), profileID: profileID)
    }

    // MARK: - Backup Loop

    private func runBackupLoop(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        monthAssetIDs: [LibraryMonthKey: [PhotoKitLocalIdentifier]],
        sortedMonths: [LibraryMonthKey],
        writer: ExecutionLogSessionWriter,
        v2Services: BackupV2RuntimeServices
    ) async -> Bool {

        let eventStream = BackupEventStream()
        let drainTask = Task.detached {
            for await event in eventStream.stream {
                switch event {
                case .log(let message, let level):
                    await writer.appendLog(message, level: level)
                case .progress(let progress):
                    await writer.appendLog(progress.effectiveLogMessage, level: progress.logLevel)
                case .started, .finished, .transferState, .monthChanged:
                    break
                }
            }
        }
        defer { eventStream.finish() }

        let iCloudMode = ICloudPhotoBackupMode.getValue()
        var flushCounter = AssetBatchFlushCounter(threshold: Self.flushInterval)
        var anyMonthFailed = false
        var connectionUnavailableAbort = false

        for monthKey in sortedMonths {
            if Task.isCancelled { break }
            if connectionUnavailableAbort { break }

            guard let assetIDs = monthAssetIDs[monthKey], !assetIDs.isEmpty else { continue }

            var monthHasAssetFailures = false
            var shouldFlushAfterDataConnectionAbort = false

            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.monthStart"), monthKey.displayText, assetIDs.count),
                level: .info
            )

            let monthStore: any BackupMonthStore
            do {
                monthStore = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
                    client: client,
                    basePath: profile.basePath,
                    month: monthKey,
                    v2Services: v2Services,
                    remoteIndexService: assetProcessor.remoteIndexService,
                    stepLogger: { message in
                        eventStream.emitLog(message, level: .error)
                    }
                )
            } catch {
                if Task.isCancelled { break }
                anyMonthFailed = true
                // Connection-unavailable means every subsequent loadOrCreate would re-trip the same failure.
                if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
                    connectionUnavailableAbort = true
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                        level: .error
                    )
                    break
                }
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.monthManifestFailed"), monthKey.displayText, profile.userFacingStorageErrorMessage(error)),
                    level: .error
                )
                continue
            }

            // W1: background uses the no-aggregator transaction variant — same drain/publish/abort
            // lifecycle as foreground, minus the progress-counter reconciliation.
            let transaction = MonthDurableTransaction(
                aggregator: nil,
                assetProcessor: assetProcessor,
                eventStream: eventStream,
                profile: profile,
                month: monthKey,
                workerID: 1
            )

            let fetchBatchSize = 500
            for batchStart in stride(from: 0, to: assetIDs.count, by: fetchBatchSize) {
                if Task.isCancelled { break }

                let batchEnd = min(batchStart + fetchBatchSize, assetIDs.count)
                let batchIDs = Array(assetIDs[batchStart ..< batchEnd])

                let batchLocalHashCache: [PhotoKitLocalIdentifier: LocalAssetHashCache]
                do {
                    batchLocalHashCache = try hashIndexRepository.fetchAssetHashCaches(
                        assetIDs: Set(batchIDs)
                    )
                } catch {
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.hashCacheWarning"), profile.userFacingStorageErrorMessage(error)),
                        level: .warning
                    )
                    batchLocalHashCache = [:]
                }
                let batchResult = PHAsset.fetchAssets(
                    withLocalIdentifiers: batchIDs.rawValues, options: nil
                )

                for i in 0 ..< batchResult.count {
                    if Task.isCancelled { break }
                    let asset = batchResult.object(at: i)
                    let resources = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(
                        from: PHAssetResource.assetResources(for: asset)
                    )
                    guard !resources.isEmpty else { continue }

                    let context = AssetProcessContext(
                        workerID: 1,
                        asset: asset,
                        selectedResources: resources,
                        cachedLocalHash: batchLocalHashCache[PhotoKitLocalIdentifier(asset)],
                        iCloudPhotoBackupMode: iCloudMode,
                        monthStore: monthStore,
                        profile: profile,
                        assetPosition: 0,
                        totalAssets: 0
                    )

                    let result: AssetProcessResult
                    do {
                        result = try await assetProcessor.process(
                            context: context,
                            client: client,
                            eventStream: eventStream,
                            cancellationController: nil
                        )
                    } catch {
                        let assetDispatch = BackupFlushFailureClassification.backgroundAssetErrorDispatch(
                            error: error,
                            profile: profile
                        )
                        switch assetDispatch.action {
                        case .breakAssetLoop:
                            break
                        case .abortMonthConnectionUnavailableBreakAssetLoop:
                            anyMonthFailed = true
                            connectionUnavailableAbort = true
                            shouldFlushAfterDataConnectionAbort = true
                            await writer.appendLog(
                                String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(assetDispatch.error)),
                                level: .error
                            )
                        case .logGenericFailureAndContinue:
                            let displayName = BackupAssetResourcePlanner.assetDisplayName(
                                asset: asset,
                                selectedResources: resources
                            )
                            anyMonthFailed = true
                            monthHasAssetFailures = true
                            await writer.appendLog(
                                String(format: String(localized: "backup.auto.log.assetFailed"), displayName, profile.userFacingStorageErrorMessage(assetDispatch.error)),
                                level: .error
                            )
                            continue
                        }
                        // .breakAssetLoop and .abortMonthConnectionUnavailableBreakAssetLoop fall through to here.
                        break
                    }

                    if result.status == .failed {
                        anyMonthFailed = true
                        monthHasAssetFailures = true
                    }
                    // Cached-reuse `.skipped` also writes asset rows; only `.failed` skips the batch counter.
                    guard result.status != .failed else { continue }
                    if flushCounter.recordSuccessAndCheckThreshold() {
                        var intervalOutcome: V2MonthFlushOutcome?
                        var shouldBreakAssetLoop = false
                        do {
                            intervalOutcome = try await BackupParallelExecutor.commitMonthStoreDefensively(
                                monthStore: monthStore,
                                ignoreCancellation: Self.backgroundIntervalFlushIgnoresCancellation()
                            )
                        } catch {
                            switch BackupFlushFailureClassification.classify(error, on: profile).backgroundIntervalAction {
                            case .continueAssetLoopAndResetCounter:
                                flushCounter.reset()
                                continue
                            case .ignoreSilently:
                                break
                            case .abortProfileLogError:
                                // U01 R03: do NOT clear intents here. EOM-skip branch (below)
                                // clears any remaining intents once we know the final flush won't
                                // commit them. (For paths where EOM still runs, applyDurableBatch
                                // drains durable intents and only then can stale ones be cleared.)
                                connectionUnavailableAbort = true
                                anyMonthFailed = true
                                await writer.appendLog(
                                    String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                                    level: .error
                                )
                                shouldBreakAssetLoop = true
                            case .logErrorAndContinue:
                                await writer.appendErrorLog(
                                    String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, profile.userFacingStorageErrorMessage(error)),
                                    unless: error
                                )
                            case .logErrorAndBreakAssetLoop:
                                // U01: V2 interval commit failed with nothing durable. Log and
                                // break the asset loop; the trailing end-of-month flush is the
                                // last chance to drain pending V2 ops in this session. If end-of-month
                                // also fails, its catch path (`.recordReasonLogError` /
                                // `.abortProfileLogError`) clears the queued hash-index intents.
                                await writer.appendErrorLog(
                                    String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, profile.userFacingStorageErrorMessage(error)),
                                    unless: error
                                )
                                anyMonthFailed = true
                                shouldBreakAssetLoop = true
                            }
                        }
                        if let outcome = intervalOutcome {
                            transaction.beginCommitDurable(outcome: outcome)
                            let drainOutcome = (try? await transaction.drainSideEffects()) ?? nil
                            if case .partial(_, let failedCount, let firstError)? = drainOutcome {
                                await writer.appendLog(
                                    String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, "hash-index drain partial (\(failedCount) failed): \(profile.userFacingStorageErrorMessage(firstError))"),
                                    level: .warning
                                )
                            }
                        }
                        if let outcome = intervalOutcome,
                           monthStore.hasUncommittedV2Ops {
                            // U01 R02: partial multi-chunk failure. Force a hard stop so the
                            // asset loop cannot accumulate past the 200-op boundary. R03: do NOT
                            // clear queued intents here — the trailing EOM flush may still commit
                            // chunk N+1 (with applyDurableBatchSideEffects draining its intents);
                            // the EOM-skip branch below clears intents when EOM is suppressed.
                            let displayError = outcome.displayError
                            let underlyingMessage = displayError.map {
                                profile.userFacingStorageErrorMessage($0)
                            } ?? ""
                            if let displayError,
                               profile.isConnectionUnavailableErrorIncludingFlushUnderlying(displayError) {
                                connectionUnavailableAbort = true
                                anyMonthFailed = true
                                await writer.appendLog(
                                    String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, underlyingMessage),
                                    level: .error
                                )
                            } else {
                                anyMonthFailed = true
                                await writer.appendErrorLog(
                                    String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, underlyingMessage),
                                    unless: displayError ?? NSError(domain: "U01Partial", code: 0)
                                )
                            }
                            shouldBreakAssetLoop = true
                        } else if let outcome = intervalOutcome {
                            // W1: publish trails the side-effect drain (gated off while uncommitted ops remain).
                            try? transaction.publishCommittedView(monthStore: monthStore)
                            if let dispatch = BackupFlushFailureClassification.backgroundIntervalPartialDispatch(
                                outcome: outcome,
                                profile: profile
                            ) {
                                switch dispatch.action {
                                case .ignoreSilently:
                                    break
                                case .abortProfileLogError:
                                    connectionUnavailableAbort = true
                                    anyMonthFailed = true
                                    await writer.appendLog(
                                        String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(dispatch.displayError)),
                                        level: .error
                                    )
                                    shouldBreakAssetLoop = true
                                case .logErrorAndContinue:
                                    await writer.appendErrorLog(
                                        String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, profile.userFacingStorageErrorMessage(dispatch.displayError)),
                                        unless: dispatch.displayError
                                    )
                                }
                            }
                        }
                        if shouldBreakAssetLoop { break }
                        flushCounter.reset()
                    }
                }
                if connectionUnavailableAbort { break }
            }

            var monthFlushFailureReason: String?
            if connectionUnavailableAbort && !shouldFlushAfterDataConnectionAbort {
                // Connection-unavailable already terminated this profile; another remote write would just re-log the same failure.
                // U01 R03: EOM is skipped. Any queued V2 hash-index intents from earlier
                // interval-fail / partial multi-chunk paths now have no chance of being committed
                // by a later flush in this session — clear them (and drop the stale optimistic
                // overlay) so the next session re-processes those assets
                // (`containsDurableAssetFingerprint` already short-circuits if any chunk landed
                // durably).
                await transaction.abort()
                break
            }
            var eomOutcome: V2MonthFlushOutcome?
            do {
                eomOutcome = try await BackupParallelExecutor.commitMonthStoreDefensively(
                    monthStore: monthStore,
                    ignoreCancellation: Self.backgroundFinalFlushIgnoresCancellation()
                )
            } catch {
                switch BackupFlushFailureClassification.classify(error, on: profile).backgroundEndOfMonthAction {
                case .continueMonthLoop:
                    await transaction.abort()
                    continue
                case .ignoreSilently:
                    // Genuine teardown stays silent; a transport cancel on a live task is a real
                    // EOM failure, so mark the month failed rather than completing it with stale state.
                    if Self.backgroundEndOfMonthCancelledIsRealFailure(taskIsCancelled: Task.isCancelled) {
                        await transaction.abort()
                        let reason = profile.userFacingStorageErrorMessage(error)
                        monthFlushFailureReason = reason
                        await writer.appendLog(
                            String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, reason),
                            level: .error
                        )
                    }
                case .abortProfileLogError:
                    await transaction.abort()
                    connectionUnavailableAbort = true
                    anyMonthFailed = true
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                        level: .error
                    )
                case .recordReasonLogError:
                    await transaction.abort()
                    let reason = profile.userFacingStorageErrorMessage(error)
                    monthFlushFailureReason = reason
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, reason),
                        level: .error
                    )
                }
            }
            if let outcome = eomOutcome {
                transaction.beginCommitDurable(outcome: outcome)
                let drainOutcome = (try? await transaction.drainSideEffects()) ?? nil
                if case .partial(_, let failedCount, let firstError)? = drainOutcome {
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, "hash-index drain partial (\(failedCount) failed): \(profile.userFacingStorageErrorMessage(firstError))"),
                        level: .warning
                    )
                }
            }
            if let outcome = eomOutcome,
               monthStore.hasUncommittedV2Ops {
                // U01 R02: end-of-month partial multi-chunk failure. The chunk-N+1 remainder dies
                // with this session — abort clears its queued hash-index intents and drops the
                // stale optimistic overlay; surface the failure through `monthFlushFailureReason`
                // (or profile abort for connection loss) so the month is reported failed in this
                // run. Counters are not maintained in background; no aggregator rollback needed.
                await transaction.abort()
                let displayError = outcome.displayError
                let underlyingMessage = displayError.map {
                    profile.userFacingStorageErrorMessage($0)
                } ?? ""
                if let displayError,
                   profile.isConnectionUnavailableErrorIncludingFlushUnderlying(displayError) {
                    connectionUnavailableAbort = true
                    anyMonthFailed = true
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, underlyingMessage),
                        level: .error
                    )
                } else {
                    monthFlushFailureReason = underlyingMessage
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, underlyingMessage),
                        level: .error
                    )
                }
            } else if let outcome = eomOutcome {
                // W1: publish trails the side-effect drain (gated off while uncommitted ops remain).
                try? transaction.publishCommittedView(monthStore: monthStore)
                if let dispatch = BackupFlushFailureClassification.backgroundEndOfMonthPartialDispatch(
                    outcome: outcome,
                    profile: profile
                ) {
                    switch dispatch.action {
                    case .ignoreSilently:
                        break
                    case .abortProfileLogError:
                        connectionUnavailableAbort = true
                        anyMonthFailed = true
                        await writer.appendLog(
                            String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(dispatch.displayError)),
                            level: .error
                        )
                    case .recordReasonLogError:
                        let reason = profile.userFacingStorageErrorMessage(dispatch.displayError)
                        monthFlushFailureReason = reason
                        await writer.appendLog(
                            String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, reason),
                            level: .error
                        )
                    }
                }
            }
            flushCounter.reset()
            if connectionUnavailableAbort {
                break
            }
            if Task.isCancelled {
                // Suppress per-month log; outer profile loop reports .cancelled.
            } else if let monthFlushFailureReason {
                anyMonthFailed = true
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.monthFailed"), monthKey.displayText, monthFlushFailureReason),
                    level: .error
                )
            } else if monthHasAssetFailures {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.monthFailed"), monthKey.displayText, String(localized: "backup.auto.log.monthAssetFailures")),
                    level: .error
                )
            } else {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.monthEnd"), monthKey.displayText),
                    level: .info
                )
            }
        }

        eventStream.finish()
        await drainTask.value
        return anyMonthFailed
    }

    // MARK: - Wi-Fi Check

    private func isWiFiAvailable() async -> Bool {
        await withCheckedContinuation { continuation in
            let monitor = NWPathMonitor(requiredInterfaceType: .wifi)
            monitor.pathUpdateHandler = { path in
                monitor.cancel()
                continuation.resume(returning: path.status == .satisfied)
            }
            monitor.start(queue: DispatchQueue(label: "bg-backup.wifi-check"))
        }
    }
}

private extension Date {
    func startOfMonth() -> Date {
        Calendar.current.date(from: Calendar.current.dateComponents([.year, .month], from: self)) ?? self
    }
}

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
        _ kind: BackupV2RuntimeOpenFailureKind
    ) -> BackgroundRuntimeOpenFailureDisposition {
        switch kind {
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
        case .cancellation:
            return .cancelled
        case .transientRemoteFailure:
            return .failedTransientRemoteFailure
        case .other:
            return .skippedOther
        }
    }

    private func backupProfile(
        _ profile: ServerProfileRecord,
        monthAssetIDs: [LibraryMonthKey: [String]],
        sortedMonths: [LibraryMonthKey],
        writer: ExecutionLogSessionWriter
    ) async -> ProfileRunResult {
        // BG runner reuses one RemoteIndexSyncService across profiles; mixed-profile state would corrupt resume coverage.
        await assetProcessor.remoteIndexService.resetForProfileSwitch()
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.profileStart"), profile.name),
            level: .info
        )
        let password: String
        if profile.storageProfile.requiresPassword {
            do {
                let pw = try keychainService.readPassword(account: profile.credentialRef)
                guard !pw.isEmpty else {
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.profileMissingPassword"), profile.name),
                        level: .warning
                    )
                    return .skipped
                }
                password = pw
            } catch KeychainError.unhandled(let status) where status == errSecItemNotFound {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profileMissingPassword"), profile.name),
                    level: .warning
                )
                return .skipped
            } catch {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profilePasswordReadFailed"), profile.name, error.localizedDescription),
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
        let metadataClient: any RemoteStorageClientProtocol
        do {
            let raw = try storageClientFactory.makeClient(profile: profile, password: password)
            try await raw.connect()
            // serialOnly backends — wrap to serialize concurrent metadata writes.
            metadataClient = wrapIfSerial(raw)
        } catch {
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
        }

        let v2Services: BackupV2RuntimeServices
        do {
            v2Services = try await BackupV2RuntimeBuilder.build(
                client: client,
                metadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                allowMigration: false
            )
        } catch {
            let failure = BackupV2RuntimeOpenErrorMapping.classifyBuildFailure(error)
            await metadataClient.disconnectSafely()
            await client.disconnectSafely()
            return await handleRuntimeOpenFailure(failure, profile: profile, writer: writer)
        }

        do {
            let preMaterialized = await v2Services.initialMaterializeOutput.peek()
            // expectV2 rejects lingering V1 data instead of routing backup sync through V1.
            _ = try await assetProcessor.remoteIndexService.syncIndex(
                client: client,
                profile: profile,
                preMaterialized: preMaterialized,
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
        _ failure: BackupV2RuntimeOpenFailure,
        profile: ServerProfileRecord,
        writer: ExecutionLogSessionWriter
    ) async -> ProfileRunResult {
        switch Self.runtimeOpenFailureDisposition(failure.kind) {
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
            let mapped = BackupV2RuntimeOpenErrorMapping.compatibilityError(for: failure)
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatInspectFailed"), profile.name, mapped.localizedDescription),
                level: .error
            )
            return .failed
        case .cancelled:
            return .cancelled
        case .failedTransientRemoteFailure:
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(failure.originalError)),
                level: .error
            )
            return .failed
        case .skippedOther:
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatInspectFailed"), profile.name, profile.userFacingStorageErrorMessage(failure.originalError)),
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
        monthAssetIDs: [LibraryMonthKey: [String]],
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
        var uploadsSinceFlush = 0
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
                let freshHashes = await assetProcessor.remoteIndexService.verifiedPhysicallyMissingHashes(for: monthKey)
                let failClosedHashes = freshHashes ?? assetProcessor.remoteIndexService.physicallyMissingHashes(for: monthKey)
                monthStore = try await V2MonthSession.loadOrCreate(
                    client: client,
                    basePath: profile.basePath,
                    year: monthKey.year,
                    month: monthKey.month,
                    v2Services: v2Services,
                    verifiedMissingHashes: failClosedHashes.isEmpty ? nil : failClosedHashes,
                    overlayIsAuthoritative: freshHashes != nil,
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

            // BG is single-worker sequential — this replace keeps the BG-local
            // RemoteIndexSyncService's committedView aligned with the V2MonthSession
            // we just loaded (resources/assets/links is the load-bearing half).
            // physicallyMissingHashes is defensive parity in BG (no cross-worker
            // reads). FG's BackupParallelExecutor does the equivalent for the
            // genuinely parallel case.
            let loadedSnapshot = monthStore.unsortedSnapshot()
            assetProcessor.remoteIndexService.replaceCachedMonth(
                monthKey,
                resources: loadedSnapshot.resources,
                assets: loadedSnapshot.assets,
                links: loadedSnapshot.links,
                physicallyMissingHashes: monthStore.physicallyMissingHashesAreAuthoritative
                    ? monthStore.physicallyMissingHashesSnapshot()
                    : nil
            )

            let fetchBatchSize = 500
            for batchStart in stride(from: 0, to: assetIDs.count, by: fetchBatchSize) {
                if Task.isCancelled { break }

                let batchEnd = min(batchStart + fetchBatchSize, assetIDs.count)
                let batchIDs = Array(assetIDs[batchStart ..< batchEnd])

                let batchLocalHashCache: [String: LocalAssetHashCache]
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
                    withLocalIdentifiers: batchIDs, options: nil
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
                        cachedLocalHash: batchLocalHashCache[asset.localIdentifier],
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
                    } catch is CancellationError {
                        break
                    } catch {
                        let displayName = BackupAssetResourcePlanner.assetDisplayName(
                            asset: asset,
                            selectedResources: resources
                        )
                        if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
                            anyMonthFailed = true
                            connectionUnavailableAbort = true
                            shouldFlushAfterDataConnectionAbort = true
                            await writer.appendLog(
                                String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                                level: .error
                            )
                            break
                        }
                        anyMonthFailed = true
                        monthHasAssetFailures = true
                        await writer.appendLog(
                            String(format: String(localized: "backup.auto.log.assetFailed"), displayName, profile.userFacingStorageErrorMessage(error)),
                            level: .error
                        )
                        continue
                    }

                    if result.status == .failed {
                        anyMonthFailed = true
                        monthHasAssetFailures = true
                    }
                    // Cached-reuse `.skipped` also writes asset rows; only `.failed` skips the batch counter.
                    guard result.status != .failed else { continue }
                    uploadsSinceFlush += 1
                    if uploadsSinceFlush >= Self.flushInterval {
                        do {
                            _ = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
                                monthStore: monthStore,
                                month: monthKey,
                                remoteIndexService: assetProcessor.remoteIndexService,
                                ignoreCancellation: Self.backgroundIntervalFlushIgnoresCancellation()
                            )
                        } catch {
                            if let flushError = error as? V2MonthSession.FlushError,
                               case .concurrentFlushRejected = flushError {
                                uploadsSinceFlush = 0
                                continue
                            }
                            let isCancel = error is CancellationError
                                || (error as? V2MonthSession.FlushError)?.cancellationCause != nil
                            if !isCancel {
                                if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
                                    connectionUnavailableAbort = true
                                    anyMonthFailed = true
                                    await writer.appendLog(
                                        String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                                        level: .error
                                    )
                                    break
                                }
                                await writer.appendErrorLog(
                                    String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, profile.userFacingStorageErrorMessage(error)),
                                    unless: error
                                )
                            }
                        }
                        uploadsSinceFlush = 0
                    }
                }
                if connectionUnavailableAbort { break }
            }

            var monthFlushFailureReason: String?
            if connectionUnavailableAbort && !shouldFlushAfterDataConnectionAbort {
                // Connection-unavailable already terminated this profile; another remote write would just re-log the same failure.
                break
            }
            do {
                _ = try await BackupParallelExecutor.flushMonthStorePublishingDefensiveCommits(
                    monthStore: monthStore,
                    month: monthKey,
                    remoteIndexService: assetProcessor.remoteIndexService,
                    ignoreCancellation: Self.backgroundFinalFlushIgnoresCancellation()
                )
            } catch {
                if let flushError = error as? V2MonthSession.FlushError,
                   case .concurrentFlushRejected = flushError {
                    continue
                }
                let isCancel = error is CancellationError
                    || (error as? V2MonthSession.FlushError)?.cancellationCause != nil
                if !isCancel {
                    if profile.isConnectionUnavailableErrorIncludingFlushUnderlying(error) {
                        connectionUnavailableAbort = true
                        anyMonthFailed = true
                        await writer.appendLog(
                            String(format: String(localized: "backup.auto.log.profileConnectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                            level: .error
                        )
                    } else {
                        let reason = profile.userFacingStorageErrorMessage(error)
                        monthFlushFailureReason = reason
                        await writer.appendLog(
                            String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, reason),
                            level: .error
                        )
                    }
                }
            }
            uploadsSinceFlush = 0
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

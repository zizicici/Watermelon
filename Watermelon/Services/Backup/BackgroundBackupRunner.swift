//
//  BackgroundBackupRunner.swift
//  Watermelon
//

import Foundation
import Network
import Photos
import Security
import MoreKit

final class BackgroundBackupRunner {
    static let taskIdentifier = "com.zizicici.watermelon.background-backup"

    private static let flushInterval = BackupV2Constants.batchFlushInterval
    private static let recentMonthCount = 2
    private static let profileCooldownHours = 18
    private static let profileCooldownInterval: TimeInterval = TimeInterval(profileCooldownHours) * 60 * 60

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

    func run() async {
        guard await ProStatus.verifyEntitlement() else { return }
        guard BackgroundBackupSetting.getValue() == .enable else { return }
        guard await isWiFiAvailable() else { return }

        guard let profiles = try? databaseManager.fetchBackgroundBackupEnabledProfiles(),
              !profiles.isEmpty else { return }

        guard let cutoff = Calendar.current.date(
            byAdding: .month,
            value: -(Self.recentMonthCount - 1),
            to: Date()
        )?.startOfMonth() else { return }

        let options = PHFetchOptions()
        options.predicate = NSPredicate(format: "creationDate >= %@", cutoff as NSDate)
        options.sortDescriptors = [NSSortDescriptor(key: "creationDate", ascending: false)]
        let recentAssets = PHAsset.fetchAssets(with: options)
        guard recentAssets.count > 0 else { return }

        let monthAssetIDs = BackupMonthScheduler.buildMonthAssetIDsByMonth(from: recentAssets)
        let sortedMonths = monthAssetIDs.keys.sorted(by: >)

        let writer = ExecutionLogFileStore.beginSession(kind: .auto)
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.sessionStart"), profiles.count, sortedMonths.count),
            level: .info
        )

        for profile in profiles.shuffled() {
            if Task.isCancelled { break }
            if await shouldSkipProfileForCooldown(profile, writer: writer) {
                continue
            }
            let result = await backupProfile(profile, monthAssetIDs: monthAssetIDs, sortedMonths: sortedMonths, writer: writer)
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
        monthAssetIDs: [LibraryMonthKey: [String]],
        sortedMonths: [LibraryMonthKey],
        writer: ExecutionLogSessionWriter
    ) async -> ProfileRunResult {
        // BG runner reuses one RemoteIndexSyncService across profiles. Full reset
        // (cache + overlay + inflight + active-profile-key) — without this, profile
        // A's resources mixed with profile B's physically-missing overlay produced
        // garbage in committedAssetFingerprintsByMonth.
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
        } catch BackupV2RuntimeBuildError.requiresForegroundMigration {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileNeedsForegroundMigration"), profile.name),
                level: .warning
            )
            await metadataClient.disconnectSafely()
            await client.disconnectSafely()
            return .skipped
        } catch BackupV2RuntimeBuildError.unsupportedRemoteFormat(let minAppVersion) {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatUnsupported"), profile.name, minAppVersion ?? "?"),
                level: .error
            )
            await metadataClient.disconnectSafely()
            await client.disconnectSafely()
            return .failed
        } catch BackupV2RuntimeBuildError.repoIdentityMismatch {
            // BG can't resolve identity drift; surface the specific error to the log.
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileRepoIdentityMismatch"), profile.name),
                level: .error
            )
            await metadataClient.disconnectSafely()
            await client.disconnectSafely()
            return .failed
        } catch BackupV2RuntimeBuildError.repoFormatRegression {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileRepoFormatRegression"), profile.name),
                level: .error
            )
            await metadataClient.disconnectSafely()
            await client.disconnectSafely()
            return .failed
        } catch {
            // Don't degrade to V1 — a V1 manifest into a V2 repo creates dual-format divergence.
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFormatInspectFailed"), profile.name, profile.userFacingStorageErrorMessage(error)),
                level: .warning
            )
            await metadataClient.disconnectSafely()
            await client.disconnectSafely()
            return .skipped
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

        let iCloudMode = ICloudPhotoBackupMode.getValue()
        var uploadsSinceFlush = 0
        var anyMonthFailed = false

        for monthKey in sortedMonths {
            if Task.isCancelled { break }

            guard let assetIDs = monthAssetIDs[monthKey], !assetIDs.isEmpty else { continue }

            var monthHasAssetFailures = false

            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.monthStart"), monthKey.displayText, assetIDs.count),
                level: .info
            )

            let monthStore: any BackupMonthStore
            do {
                monthStore = try await V2MonthSession.loadOrCreate(
                    client: client,
                    basePath: profile.basePath,
                    year: monthKey.year,
                    month: monthKey.month,
                    v2Services: v2Services,
                    stepLogger: { message in
                        eventStream.emitLog(message, level: .error)
                    }
                )
            } catch {
                if Task.isCancelled { break }
                anyMonthFailed = true
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.monthManifestFailed"), monthKey.displayText, profile.userFacingStorageErrorMessage(error)),
                    level: .error
                )
                continue
            }

            assetProcessor.remoteIndexService.markPhysicallyMissingV2(
                month: monthKey,
                hashes: monthStore.physicallyMissingHashesSnapshot()
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
                        // Stop the asset loop immediately on cancel — relying on the next
                        // iteration's Task.isCancelled check would still process 1-2 more
                        // assets before noticing. FG executor was already aligned this way.
                        break
                    } catch {
                        anyMonthFailed = true
                        monthHasAssetFailures = true
                        let displayName = BackupAssetResourcePlanner.assetDisplayName(
                            asset: asset,
                            selectedResources: resources
                        )
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
                            let delta = try await monthStore.flushToRemote(ignoreCancellation: true)
                            assetProcessor.remoteIndexService.markCommittedV2(
                                month: monthKey,
                                fingerprints: delta.committedV2AssetFingerprints
                                    .union(delta.committedV2TombstoneFingerprints)
                            )
                        } catch {
                            let isCancel = error is CancellationError
                                || (error as? V2MonthSession.FlushError)?.cancellationCause != nil
                            if isCancel {
                                // Background run is wrapped by Task.isCancelled at higher level;
                                // don't log this as a hard flush failure.
                            } else {
                                // Commit may be durable even on snapshot-write failure.
                                assetProcessor.remoteIndexService.recordCommittedFromFlushError(month: monthKey, error)
                                await writer.appendErrorLog(
                                    String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, profile.userFacingStorageErrorMessage(error)),
                                    unless: error
                                )
                            }
                        }
                        uploadsSinceFlush = 0
                    }
                }
            }

            var monthFlushFailureReason: String?
            do {
                let delta = try await monthStore.flushToRemote(ignoreCancellation: true)
                assetProcessor.remoteIndexService.markCommittedV2(
                    month: monthKey,
                    fingerprints: delta.committedV2AssetFingerprints
                        .union(delta.committedV2TombstoneFingerprints)
                )
            } catch {
                let isCancel = error is CancellationError
                    || (error as? V2MonthSession.FlushError)?.cancellationCause != nil
                if !isCancel {
                    // Commit may be durable even on snapshot-write failure.
                    assetProcessor.remoteIndexService.recordCommittedFromFlushError(month: monthKey, error)
                    let reason = profile.userFacingStorageErrorMessage(error)
                    monthFlushFailureReason = reason
                    await writer.appendLog(
                        String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, reason),
                        level: .error
                    )
                }
            }
            uploadsSinceFlush = 0
            if let monthFlushFailureReason {
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

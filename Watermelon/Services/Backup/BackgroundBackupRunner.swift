//
//  BackgroundBackupRunner.swift
//  Watermelon
//

import Foundation
import Network
import Photos
import MoreKit

final class BackgroundBackupRunner {
    static let taskIdentifier = "com.zizicici.watermelon.background-backup"

    private static let flushInterval = 10
    private static let recentMonthCount = 2

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
            await backupProfile(profile, monthAssetIDs: monthAssetIDs, sortedMonths: sortedMonths, writer: writer)
        }

        await writer.appendLog(String(localized: "backup.auto.log.sessionEnd"), level: .info)
        await writer.finalize()
    }

    // MARK: - Per-Profile Backup

    private func backupProfile(
        _ profile: ServerProfileRecord,
        monthAssetIDs: [LibraryMonthKey: [String]],
        sortedMonths: [LibraryMonthKey],
        writer: ExecutionLogSessionWriter
    ) async {
        await writer.appendLog(
            String(format: String(localized: "backup.auto.log.profileStart"), profile.name),
            level: .info
        )
        let password: String
        if profile.storageProfile.requiresPassword {
            guard let pw = try? keychainService.readPassword(account: profile.credentialRef),
                  !pw.isEmpty else {
                await writer.appendLog(
                    String(format: String(localized: "backup.auto.log.profileMissingPassword"), profile.name),
                    level: .warning
                )
                return
            }
            password = pw
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
            return
        }

        let anyMonthFailed = await runBackupLoop(client: client, profile: profile, monthAssetIDs: monthAssetIDs, sortedMonths: sortedMonths, writer: writer)
        await client.disconnectSafely()
        if anyMonthFailed {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileFailed"), profile.name),
                level: .error
            )
        } else {
            await writer.appendLog(
                String(format: String(localized: "backup.auto.log.profileEnd"), profile.name),
                level: .info
            )
        }
    }

    // MARK: - Backup Loop

    private func runBackupLoop(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        monthAssetIDs: [LibraryMonthKey: [String]],
        sortedMonths: [LibraryMonthKey],
        writer: ExecutionLogSessionWriter
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

            let monthStore: MonthManifestStore
            do {
                monthStore = try await MonthManifestStore.loadOrCreate(
                    client: client,
                    basePath: profile.basePath,
                    year: monthKey.year,
                    month: monthKey.month
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
                    } catch {
                        if !(error is CancellationError) {
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
                        }
                        continue
                    }

                    if result.status == .failed {
                        anyMonthFailed = true
                        monthHasAssetFailures = true
                    }
                    guard result.status == .success else { continue }
                    uploadsSinceFlush += 1
                    if uploadsSinceFlush >= Self.flushInterval {
                        do {
                            try await monthStore.flushToRemote(ignoreCancellation: true)
                        } catch {
                            await writer.appendErrorLog(
                                String(format: String(localized: "backup.auto.log.flushFailed"), monthKey.displayText, profile.userFacingStorageErrorMessage(error)),
                                unless: error
                            )
                        }
                        uploadsSinceFlush = 0
                    }
                }
            }

            var monthFlushFailureReason: String?
            do {
                try await monthStore.flushToRemote(ignoreCancellation: true)
            } catch {
                if !(error is CancellationError) {
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

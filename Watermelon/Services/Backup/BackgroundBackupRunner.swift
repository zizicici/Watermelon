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

        for profile in profiles.shuffled() {
            if Task.isCancelled { break }
            await backupProfile(profile, monthAssetIDs: monthAssetIDs, sortedMonths: sortedMonths)
        }
    }

    // MARK: - Per-Profile Backup

    private func backupProfile(
        _ profile: ServerProfileRecord,
        monthAssetIDs: [LibraryMonthKey: [String]],
        sortedMonths: [LibraryMonthKey]
    ) async {
        let password: String
        if profile.storageProfile.requiresPassword {
            guard let pw = try? keychainService.readPassword(account: profile.credentialRef),
                  !pw.isEmpty else { return }
            password = pw
        } else {
            password = ""
        }

        let client: any RemoteStorageClientProtocol
        do {
            client = try storageClientFactory.makeClient(profile: profile, password: password)
            try await client.connect()
        } catch {
            return
        }

        await runBackupLoop(client: client, profile: profile, monthAssetIDs: monthAssetIDs, sortedMonths: sortedMonths)
        await client.disconnectSafely()
    }

    // MARK: - Backup Loop

    private func runBackupLoop(
        client: any RemoteStorageClientProtocol,
        profile: ServerProfileRecord,
        monthAssetIDs: [LibraryMonthKey: [String]],
        sortedMonths: [LibraryMonthKey]
    ) async {

        let eventStream = BackupEventStream()
        let drainTask = Task.detached { for await _ in eventStream.stream {} }
        defer { eventStream.finish(); drainTask.cancel() }

        let iCloudMode = ICloudPhotoBackupMode.getValue()
        var uploadsSinceFlush = 0

        for monthKey in sortedMonths {
            if Task.isCancelled { break }

            guard let assetIDs = monthAssetIDs[monthKey], !assetIDs.isEmpty else { continue }

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
                continue
            }

            let fetchBatchSize = 500
            for batchStart in stride(from: 0, to: assetIDs.count, by: fetchBatchSize) {
                if Task.isCancelled { break }

                let batchEnd = min(batchStart + fetchBatchSize, assetIDs.count)
                let batchIDs = Array(assetIDs[batchStart ..< batchEnd])

                let batchLocalHashCache = (try? hashIndexRepository.fetchAssetHashCaches(
                    assetIDs: Set(batchIDs)
                )) ?? [:]
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

                    if let result = try? await assetProcessor.process(
                        context: context,
                        client: client,
                        eventStream: eventStream,
                        cancellationController: nil
                    ), result.status == .success {
                        uploadsSinceFlush += 1
                        if uploadsSinceFlush >= Self.flushInterval {
                            _ = try? await monthStore.flushToRemote(ignoreCancellation: true)
                            uploadsSinceFlush = 0
                        }
                    }
                }
            }

            _ = try? await monthStore.flushToRemote(ignoreCancellation: true)
            uploadsSinceFlush = 0
        }
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

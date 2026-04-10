import Foundation
import Photos

@MainActor
final class HomeExecutionCoordinator {

    // MARK: - Public State

    private(set) var isActive = false

    var currentState: HomeExecutionState? {
        guard isActive else { return nil }
        return HomeExecutionState(
            monthPlans: monthPlans,
            activeMonths: activeMonths,
            phase: currentPhase,
            processedCountByMonth: processedCountByMonth,
            assetCountByMonth: assetCountByMonth,
            uploadMonths: uploadMonths,
            downloadMonths: pendingDownloadMonths,
            syncMonths: pendingSyncMonths
        )
    }

    // MARK: - Callbacks

    var onStateChanged: (() -> Void)?
    var onAlert: ((String, String) -> Void)?

    // MARK: - Dependencies

    private let dependencies: DependencyContainer
    private let homeDataManager: HomeIncrementalDataManager

    // MARK: - Execution State

    private var monthPlans: [LibraryMonthKey: MonthPlan] = [:]
    private var activeMonths = Set<LibraryMonthKey>()
    private var backupObserverID: UUID?
    private var lastBackupControllerState: BackupSessionController.State = .idle

    private var assetCountByMonth: [LibraryMonthKey: Int] = [:]
    private var processedCountByMonth: [LibraryMonthKey: Int] = [:]

    private var uploadMonths: [LibraryMonthKey] = []
    private var pendingDownloadMonths: [LibraryMonthKey] = []
    private var pendingSyncMonths: [LibraryMonthKey] = []
    private var isDownloadPhase = false
    private var downloadTask: Task<Void, Never>?
    private var backupSessionController: BackupSessionController!

    private var isDownloadPaused = false
    private var downloadHadFailure = false

    private var currentPhase: ExecutionPhase {
        if !monthPlans.isEmpty && monthPlans.values.allSatisfy(\.isFullyCompleted) {
            return .completed
        }
        if isDownloadPhase {
            if downloadHadFailure && activeMonths.isEmpty {
                return .failed("部分月份下载失败")
            }
            return .downloading(isPaused: isDownloadPaused)
        }
        return .uploading(lastBackupControllerState)
    }

    // MARK: - Init

    init(dependencies: DependencyContainer, homeDataManager: HomeIncrementalDataManager) {
        self.dependencies = dependencies
        self.homeDataManager = homeDataManager
    }

    // MARK: - Enter / Exit

    func enter(upload: [LibraryMonthKey], download: [LibraryMonthKey], sync: [LibraryMonthKey]) {
        isActive = true
        uploadMonths = upload
        pendingDownloadMonths = download
        pendingSyncMonths = sync
        isDownloadPhase = false
        isDownloadPaused = false
        downloadHadFailure = false
        downloadTask = nil
        lastBackupControllerState = .idle

        monthPlans.removeAll()
        for m in upload   { monthPlans[m] = MonthPlan(needsUpload: true,  needsDownload: false) }
        for m in download { monthPlans[m] = MonthPlan(needsUpload: false, needsDownload: true) }
        for m in sync     { monthPlans[m] = MonthPlan(needsUpload: true,  needsDownload: true) }
        activeMonths.removeAll()
        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        backupSessionController = BackupSessionController(dependencies: dependencies)

        onStateChanged?()

        let backupTargetMonths = upload + sync
        if !backupTargetMonths.isEmpty {
            startUploadPhase(months: backupTargetMonths)
        } else {
            startDownloadPhase()
        }
    }

    func exit() {
        downloadTask?.cancel()
        downloadTask = nil
        isDownloadPhase = false
        isActive = false
        monthPlans.removeAll()
        activeMonths.removeAll()
        assetCountByMonth.removeAll()
        processedCountByMonth.removeAll()
        uploadMonths.removeAll()
        pendingDownloadMonths.removeAll()
        pendingSyncMonths.removeAll()

        if let observerID = backupObserverID {
            backupSessionController.removeObserver(observerID)
            backupObserverID = nil
        }

        onStateChanged?()
    }

    func pause() {
        if isDownloadPhase {
            downloadTask?.cancel()
            downloadTask = nil
            backupSessionController.stopBackup()
            isDownloadPaused = true
            onStateChanged?()
        } else {
            backupSessionController.pauseBackup()
        }
    }

    func resume() {
        if isDownloadPhase {
            isDownloadPaused = false
            startDownloadPhase()
        } else {
            backupSessionController.startBackup()
        }
    }

    func stop() {
        if isDownloadPhase {
            downloadTask?.cancel()
            downloadTask = nil
            backupSessionController.stopBackup()
            exit()
        } else {
            backupSessionController.stopBackup()
        }
    }

    // MARK: - Upload Phase

    private func startUploadPhase(months: [LibraryMonthKey]) {
        let syncMonthSet = Set(pendingSyncMonths)
        var allAssetIDs = Set<String>()
        for month in months {
            let ids = homeDataManager.localAssetIDs(for: month)
            allAssetIDs.formUnion(ids)
            if !syncMonthSet.contains(month) {
                assetCountByMonth[month] = ids.count
            }
        }

        let selection = BackupScopeSelection(
            selectedAssetIDs: allAssetIDs,
            selectedAssetCount: allAssetIDs.count,
            selectedEstimatedBytes: nil,
            totalAssetCount: allAssetIDs.count,
            totalEstimatedBytes: nil
        )
        backupSessionController.updateScopeSelection(selection)

        backupObserverID = backupSessionController.addObserver { [weak self] snapshot in
            self?.handleBackupSnapshot(snapshot)
        }

        backupSessionController.startBackup()
    }

    private func handleBackupSnapshot(_ snapshot: BackupSessionController.Snapshot) {
        guard isActive else { return }

        let previouslyCompleted = Set(monthPlans.filter { $0.value.isFullyCompleted }.map(\.key))

        for month in snapshot.flushedMonths where monthPlans[month] != nil {
            monthPlans[month]?.uploadDone = true
        }

        let nowCompleted = Set(monthPlans.filter { $0.value.isFullyCompleted }.map(\.key))
        let hasNewCompletions = !nowCompleted.subtracting(previouslyCompleted).isEmpty

        let executionMonthSet = Set(monthPlans.keys)
        activeMonths = snapshot.startedMonths.intersection(executionMonthSet).subtracting(nowCompleted)
        processedCountByMonth = snapshot.processedCountByMonth
        lastBackupControllerState = snapshot.state

        switch snapshot.state {
        case .completed:
            let backupTargets = Set(uploadMonths).union(pendingSyncMonths)
            for month in backupTargets where monthPlans[month] != nil {
                monthPlans[month]?.uploadDone = true
            }
            activeMonths.removeAll()

            if let id = backupObserverID {
                backupSessionController.removeObserver(id)
                backupObserverID = nil
            }

            syncRemoteData()

            if !pendingDownloadMonths.isEmpty || !pendingSyncMonths.isEmpty {
                startDownloadPhase()  // startDownloadPhase fires onStateChanged
            } else {
                showExecutionCompleted()
            }

        case .failed:
            onStateChanged?()
            onAlert?("上传失败", snapshot.statusText)

        case .stopped:
            exit()

        default:
            if hasNewCompletions {
                syncRemoteData()
            }
            onStateChanged?()
        }
    }

    // MARK: - Download Phase

    private func startDownloadPhase() {
        guard !pendingDownloadMonths.isEmpty || !pendingSyncMonths.isEmpty else {
            showExecutionCompleted()
            return
        }

        isDownloadPhase = true
        isDownloadPaused = false
        downloadHadFailure = false
        onStateChanged?()

        guard let profile = dependencies.appSession.activeProfile,
              let password = resolvedSessionPassword(for: profile) else {
            onAlert?("错误", "未连接远端存储")
            exit()
            return
        }

        let remainingDownloads = pendingDownloadMonths.filter { monthPlans[$0]?.isFullyCompleted != true }
        let remainingSyncs = pendingSyncMonths.filter { monthPlans[$0]?.isFullyCompleted != true }

        downloadTask = Task { [weak self] in
            guard let self else { return }

            for month in remainingDownloads {
                if Task.isCancelled { return }
                let ok = await self.ensureHashIndexAndDownload(month: month, phase: "下载", profile: profile, password: password)
                if !ok { await MainActor.run { self.downloadHadFailure = true } }
            }

            for month in remainingSyncs {
                if Task.isCancelled { return }
                let ok = await self.ensureHashIndexAndDownload(month: month, phase: "同步", profile: profile, password: password)
                if !ok { await MainActor.run { self.downloadHadFailure = true } }
            }

            if !Task.isCancelled {
                await MainActor.run {
                    self.activeMonths.removeAll()
                    if self.downloadHadFailure {
                        self.onStateChanged?()
                    } else {
                        self.showExecutionCompleted()
                    }
                }
            }
        }
    }

    private func runScopedBackup(assetIDs: Set<String>) async -> Bool {
        await withCheckedContinuation { continuation in
            Task { @MainActor in
                let selection = BackupScopeSelection(
                    selectedAssetIDs: assetIDs,
                    selectedAssetCount: assetIDs.count,
                    selectedEstimatedBytes: nil,
                    totalAssetCount: assetIDs.count,
                    totalEstimatedBytes: nil
                )
                self.backupSessionController.updateScopeSelection(selection)

                if let id = self.backupObserverID {
                    self.backupSessionController.removeObserver(id)
                    self.backupObserverID = nil
                }

                self.backupSessionController.startBackup()

                var hasResumed = false
                let observerID = self.backupSessionController.addObserver { [weak self] snapshot in
                    guard let self, !hasResumed else { return }
                    self.onStateChanged?()

                    switch snapshot.state {
                    case .completed:
                        hasResumed = true
                        self.backupSessionController.removeObserver(observerID)
                        self.backupObserverID = nil
                        continuation.resume(returning: true)
                    case .failed, .stopped:
                        hasResumed = true
                        self.backupSessionController.removeObserver(observerID)
                        self.backupObserverID = nil
                        continuation.resume(returning: false)
                    default:
                        break
                    }
                }
                self.backupObserverID = observerID
            }
        }
    }

    @discardableResult
    private func ensureHashIndexAndDownload(
        month: LibraryMonthKey,
        phase: String,
        profile: ServerProfileRecord,
        password: String
    ) async -> Bool {
        await MainActor.run {
            self.activeMonths = [month]
            self.onStateChanged?()
        }

        let assetIDs = homeDataManager.localAssetIDs(for: month)
        if !assetIDs.isEmpty {
            let uploadCompleted = await runScopedBackup(assetIDs: assetIDs)
            if Task.isCancelled { return false }
            if !uploadCompleted {
                await MainActor.run {
                    self.monthPlans[month]?.failed = true
                    self.onAlert?("\(phase)失败", "\(String(format: "%04d年%02d月", month.year, month.month)): 备份索引失败")
                }
                return false
            }
        }

        await MainActor.run { [self] in
            self.syncRemoteData()
            if !assetIDs.isEmpty {
                self.homeDataManager.refreshLocalIndex(forAssetIDs: assetIDs)
            }
        }

        return await processDownloadMonth(month, phase: phase, profile: profile, password: password)
    }

    private func processDownloadMonth(
        _ month: LibraryMonthKey,
        phase: String,
        profile: ServerProfileRecord,
        password: String
    ) async -> Bool {
        let remoteItems = homeDataManager.remoteOnlyItems(for: month)
        if !remoteItems.isEmpty {
            await MainActor.run {
                self.assetCountByMonth.removeValue(forKey: month)
                self.processedCountByMonth.removeValue(forKey: month)
            }

            do {
                _ = try await dependencies.restoreService.restoreItems(
                    items: remoteItems.map(\.resources),
                    profile: profile,
                    password: password,
                    onItemCompleted: { [weak self] _, _, restoredAsset in
                        guard let self else { return }
                        if let restoredAsset {
                            self.writeHashIndexForItem(restoredAsset, remoteItems: remoteItems)
                            self.homeDataManager.refreshLocalIndex(forAssetIDs: [restoredAsset.asset.localIdentifier])
                        }
                        self.onStateChanged?()
                    }
                )
            } catch {
                if Task.isCancelled { return false }
                await MainActor.run {
                    self.monthPlans[month]?.failed = true
                    self.onAlert?("\(phase)失败", "\(String(format: "%04d年%02d月", month.year, month.month)): \(error.localizedDescription)")
                }
                return false
            }
        }

        if Task.isCancelled { return false }
        await MainActor.run {
            self.monthPlans[month]?.downloadDone = true
            self.onStateChanged?()
        }
        return true
    }

    // MARK: - Helpers

    private func showExecutionCompleted() {
        for key in monthPlans.keys {
            monthPlans[key]?.uploadDone = true
            monthPlans[key]?.downloadDone = true
        }
        activeMonths.removeAll()
        onStateChanged?()
    }

    private func writeHashIndexForItem(_ result: RestoreService.IndexedRestoredAsset, remoteItems: [RemoteAlbumItem]) {
        guard result.itemIndex < remoteItems.count else { return }
        let remoteItem = remoteItems[result.itemIndex]

        var records: [LocalAssetResourceHashRecord] = []
        var totalSize: Int64 = 0
        for link in remoteItem.resourceLinks {
            if let resource = remoteItem.resources.first(where: { $0.contentHash == link.resourceHash }) {
                records.append(LocalAssetResourceHashRecord(
                    role: link.role,
                    slot: link.slot,
                    contentHash: link.resourceHash,
                    fileSize: resource.fileSize
                ))
                totalSize += resource.fileSize
            }
        }

        let hashRepo = ContentHashIndexRepository(databaseManager: dependencies.databaseManager)
        try? hashRepo.upsertAssetHashSnapshot(
            assetLocalIdentifier: result.asset.localIdentifier,
            assetFingerprint: remoteItem.assetFingerprint,
            resources: records,
            totalFileSizeBytes: totalSize
        )
    }

    private func resolvedSessionPassword(for profile: ServerProfileRecord) -> String? {
        profile.resolvedSessionPassword(from: dependencies.appSession)
    }

    private func syncRemoteData() {
        let active = homeDataManager.hasActiveConnection
        let snapshotState = dependencies.backupCoordinator.currentRemoteSnapshotState(
            since: homeDataManager.remoteSnapshotRevisionForQuery(hasActiveConnection: active)
        )
        homeDataManager.syncRemoteSnapshot(state: snapshotState, hasActiveConnection: active)
    }
}

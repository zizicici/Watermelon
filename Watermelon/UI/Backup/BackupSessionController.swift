import Foundation
import Photos
import UIKit

@MainActor
final class BackupSessionController {
    enum State {
        case idle
        case running
        case paused
        case stopped
        case failed
        case completed

        var buttonColor: UIColor {
            switch self {
            case .idle:
                return .systemBlue
            case .running:
                return .systemGreen
            case .paused:
                return .systemOrange
            case .stopped:
                return .systemRed
            case .failed:
                return .systemRed
            case .completed:
                return .systemBlue
            }
        }
    }

    struct FailedItem: Hashable, Identifiable {
        let jobID: Int64
        let assetLocalIdentifier: String
        let displayName: String
        let errorMessage: String
        let retryCount: Int
        let updatedAt: Date

        var id: String { assetLocalIdentifier }
    }

    struct ProcessedItem: Hashable, Identifiable {
        let assetLocalIdentifier: String
        let displayName: String
        let status: BackupItemStatus
        let reason: String?
        let resourceSummary: String?
        let updatedAt: Date

        var id: String { assetLocalIdentifier }
    }

    struct Snapshot {
        let state: State
        let primaryActionTitle: String
        let statusText: String
        let succeeded: Int
        let failed: Int
        let skipped: Int
        let total: Int
        let logs: [String]
        let processedItems: [ProcessedItem]
        let failedItems: [FailedItem]
    }

    private enum TerminationIntent {
        case none
        case pause
        case stop
    }

    private enum RunMode {
        case full
        case retry(assetIDs: Set<String>)

        var isRetry: Bool {
            if case .retry = self {
                return true
            }
            return false
        }

        var retryCount: Int {
            switch self {
            case .full:
                return 0
            case .retry(let assetIDs):
                return assetIDs.count
            }
        }
    }

    private let dependencies: DependencyContainer
    private var observers: [UUID: (Snapshot) -> Void] = [:]
    private var runTask: Task<Void, Never>?
    private var terminationIntent: TerminationIntent = .none
    private var currentRunMode: RunMode = .full
    private var lastPausedRunMode: RunMode?

    private(set) var state: State = .idle
    private(set) var statusText: String = "未开始"
    private(set) var succeeded: Int = 0
    private(set) var failed: Int = 0
    private(set) var skipped: Int = 0
    private(set) var total: Int = 0
    private(set) var logs: [String] = []
    private var processedItemsByAssetID: [String: ProcessedItem] = [:]
    private var retryCountByAssetID: [String: Int] = [:]
    private(set) var failedItems: [FailedItem] = []

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
    }

    func snapshot() -> Snapshot {
        Snapshot(
            state: state,
            primaryActionTitle: primaryActionTitle(for: state),
            statusText: statusText,
            succeeded: succeeded,
            failed: failed,
            skipped: skipped,
            total: total,
            logs: logs,
            processedItems: processedItemsByAssetID.values.sorted { $0.updatedAt > $1.updatedAt },
            failedItems: failedItems
        )
    }

    @discardableResult
    func addObserver(_ observer: @escaping (Snapshot) -> Void) -> UUID {
        let id = UUID()
        observers[id] = observer
        observer(snapshot())
        return id
    }

    func removeObserver(_ id: UUID) {
        observers[id] = nil
    }

    @discardableResult
    func startBackup() -> Bool {
        if state == .paused {
            return resumeFromPause()
        }
        return startBackup(mode: .full)
    }

    @discardableResult
    func retryFailedItems(assetIDs: Set<String>? = nil) -> Bool {
        let targetAssetIDs = assetIDs ?? Set(failedItems.map(\.assetLocalIdentifier))
        guard !targetAssetIDs.isEmpty else {
            statusText = "没有可重试项"
            appendLog("没有可重试的失败项")
            notifyObservers()
            return false
        }
        return startBackup(mode: .retry(assetIDs: targetAssetIDs))
    }

    func refreshFailedItems() {
        rebuildFailedItems()
        notifyObservers()
    }

    func loadPreviewImage(
        for item: FailedItem,
        targetSize: CGSize = CGSize(width: 1200, height: 1200)
    ) async -> UIImage? {
        let authStatus = dependencies.photoLibraryService.authorizationStatus()
        guard authStatus == .authorized || authStatus == .limited else { return nil }

        let fetched = PHAsset.fetchAssets(withLocalIdentifiers: [item.assetLocalIdentifier], options: nil)
        guard let asset = fetched.firstObject else { return nil }

        return await withCheckedContinuation { continuation in
            let manager = PHCachingImageManager()
            let options = PHImageRequestOptions()
            options.deliveryMode = .highQualityFormat
            options.resizeMode = .exact
            options.isNetworkAccessAllowed = true
            options.isSynchronous = false

            manager.requestImage(
                for: asset,
                targetSize: targetSize,
                contentMode: .aspectFit,
                options: options
            ) { image, info in
                let isDegraded = (info?[PHImageResultIsDegradedKey] as? Bool) ?? false
                if isDegraded { return }
                continuation.resume(returning: image)
            }
        }
    }

    @discardableResult
    private func startBackup(mode: RunMode) -> Bool {
        guard runTask == nil else {
            appendLog("已有备份任务正在执行")
            return false
        }
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            state = .failed
            statusText = "请先登录 SMB"
            appendLog("错误: 未登录 SMB 服务器")
            notifyObservers()
            return false
        }

        terminationIntent = .none
        currentRunMode = mode
        lastPausedRunMode = nil

        let shouldResetSessionItems =
            state == .idle ||
            state == .completed ||
            state == .failed ||
            (state == .stopped && !mode.isRetry)

        if shouldResetSessionItems {
            processedItemsByAssetID.removeAll()
            retryCountByAssetID.removeAll()
            failedItems.removeAll()
            logs.removeAll()
        }

        state = .running
        statusText = mode.isRetry ? "准备重试..." : "准备备份..."
        succeeded = 0
        failed = 0
        skipped = 0
        total = 0
        appendLog(mode.isRetry ? "开始重试失败 Asset（\(mode.retryCount)）" : "开始备份任务（按 Asset 计数）")
        notifyObservers()

        runTask = Task { [weak self] in
            guard let self else { return }
            do {
                let retryAssetIdentifiers: Set<String>?
                switch mode {
                case .full:
                    retryAssetIdentifiers = nil
                case .retry(let assetIDs):
                    retryAssetIdentifiers = assetIDs
                }

                let result = try await self.dependencies.backupExecutor.runBackup(
                    profile: profile,
                    password: password,
                    appVersion: self.dependencies.appVersion,
                    onlyAssetLocalIdentifiers: retryAssetIdentifiers,
                    onProgress: { [weak self] progress in
                        guard let self else { return }
                        self.succeeded = progress.succeeded
                        self.failed = progress.failed
                        self.skipped = progress.skipped
                        self.total = progress.total
                        self.statusText = progress.message
                        self.applyProgressEvent(progress.itemEvent)
                        self.notifyObservers()
                    },
                    onLog: { [weak self] line in
                        self?.appendLog(line)
                    }
                )
                self.finishRun(result: result, runMode: mode)
            } catch {
                self.runTask = nil
                self.terminationIntent = .none
                self.currentRunMode = .full

                self.state = .failed
                self.statusText = "备份失败"
                self.appendLog("错误: \(error.localizedDescription)")
                self.rebuildFailedItems()
                self.notifyObservers()
            }
        }

        return true
    }

    func pauseBackup() {
        guard runTask != nil else {
            state = .paused
            statusText = "备份已暂停"
            notifyObservers()
            return
        }
        terminationIntent = .pause
        statusText = "正在暂停..."
        runTask?.cancel()
        notifyObservers()
    }

    func stopBackup() {
        guard runTask != nil else {
            state = .stopped
            statusText = "备份已停止"
            notifyObservers()
            return
        }
        terminationIntent = .stop
        statusText = "正在停止..."
        runTask?.cancel()
        notifyObservers()
    }

    private func finishRun(result: BackupExecutionResult, runMode: RunMode) {
        runTask = nil
        let intent = terminationIntent
        terminationIntent = .none
        currentRunMode = .full

        succeeded = result.succeeded
        failed = result.failed
        skipped = result.skipped
        total = result.total

        if intent == .stop {
            lastPausedRunMode = nil
            state = .stopped
            statusText = "备份已停止"
            appendLog("任务已停止")
            rebuildFailedItems()
            notifyObservers()
            return
        }

        if result.paused || intent == .pause {
            lastPausedRunMode = runMode
            state = .paused
            statusText = "备份已暂停"
            appendLog("任务已暂停")
            rebuildFailedItems()
            notifyObservers()
            return
        }

        lastPausedRunMode = nil
        state = .completed
        let verb = runMode.isRetry ? "重试" : "备份"
        statusText = result.failed == 0 ? "\(verb)完成" : "\(verb)完成（部分失败）"
        appendLog("完成: 成功\(succeeded) 失败\(result.failed) 跳过\(result.skipped)")
        rebuildFailedItems()
        notifyObservers()
    }

    private func rebuildFailedItems() {
        let all = processedItemsByAssetID.values
        let failed = all
            .filter { $0.status == .failed }
            .sorted { $0.updatedAt > $1.updatedAt }

        failedItems = failed.map { item in
            FailedItem(
                jobID: 0,
                assetLocalIdentifier: item.assetLocalIdentifier,
                displayName: item.displayName,
                errorMessage: item.reason ?? "未知错误",
                retryCount: retryCountByAssetID[item.assetLocalIdentifier, default: 0],
                updatedAt: item.updatedAt
            )
        }
    }

    private func notifyObservers() {
        let latest = snapshot()
        observers.values.forEach { $0(latest) }
    }

    private func appendLog(_ line: String) {
        let timestamp = Self.timeFormatter.string(from: Date())
        logs.append("[\(timestamp)] \(line)")
        if logs.count > 800 {
            logs.removeFirst(logs.count - 800)
        }
        notifyObservers()
    }

    private func applyProgressEvent(_ event: BackupItemEvent?) {
        guard let event else { return }
        processedItemsByAssetID[event.assetLocalIdentifier] = ProcessedItem(
            assetLocalIdentifier: event.assetLocalIdentifier,
            displayName: event.displayName,
            status: event.status,
            reason: event.reason,
            resourceSummary: event.resourceSummary,
            updatedAt: event.updatedAt
        )

        if event.status == .failed {
            retryCountByAssetID[event.assetLocalIdentifier, default: 0] += 1
        }

        rebuildFailedItems()
    }

    @discardableResult
    private func resumeFromPause() -> Bool {
        guard runTask == nil else {
            appendLog("已有备份任务正在执行")
            return false
        }
        guard dependencies.appSession.activeProfile != nil,
              dependencies.appSession.activePassword != nil else {
            state = .failed
            statusText = "请先登录 SMB"
            appendLog("错误: 未登录 SMB 服务器")
            notifyObservers()
            return false
        }

        state = .running
        statusText = "正在准备继续..."
        appendLog("计算剩余备份 Asset...")
        notifyObservers()

        let pausedMode = lastPausedRunMode ?? .full
        runTask = Task { [weak self] in
            guard let self else { return }
            do {
                let pendingAssetIDs: Set<String>
                switch pausedMode {
                case .retry(let assetIDs):
                    pendingAssetIDs = assetIDs.subtracting(self.completedAssetIDs())
                case .full:
                    pendingAssetIDs = try await self.computePendingAssetIDsForFullRun()
                }

                self.runTask = nil

                guard self.state == .running else { return }
                guard !pendingAssetIDs.isEmpty else {
                    self.lastPausedRunMode = nil
                    self.state = .completed
                    self.statusText = "备份完成"
                    self.appendLog("无剩余 Asset，已完成")
                    self.rebuildFailedItems()
                    self.notifyObservers()
                    return
                }

                self.appendLog("继续备份剩余 \(pendingAssetIDs.count) 个 Asset")
                _ = self.startBackup(mode: .retry(assetIDs: pendingAssetIDs))
            } catch is CancellationError {
                self.runTask = nil
                let intent = self.terminationIntent
                self.terminationIntent = .none
                self.currentRunMode = .full
                self.state = intent == .stop ? .stopped : .paused
                self.statusText = intent == .stop ? "备份已停止" : "备份已暂停"
                self.appendLog(intent == .stop ? "任务已停止" : "任务已暂停")
                self.rebuildFailedItems()
                self.notifyObservers()
            } catch {
                self.runTask = nil
                self.terminationIntent = .none
                self.currentRunMode = .full
                self.state = .failed
                self.statusText = "继续备份失败"
                self.appendLog("继续失败: \(error.localizedDescription)")
                self.rebuildFailedItems()
                self.notifyObservers()
            }
        }

        return true
    }

    private func completedAssetIDs() -> Set<String> {
        Set(
            processedItemsByAssetID.values
                .filter { item in
                    item.status == .success || item.status == .skipped
                }
                .map(\.assetLocalIdentifier)
        )
    }

    private func computePendingAssetIDsForFullRun() async throws -> Set<String> {
        let status = dependencies.photoLibraryService.authorizationStatus()
        let authorized: Bool
        if status == .authorized || status == .limited {
            authorized = true
        } else {
            let requested = await dependencies.photoLibraryService.requestAuthorization()
            authorized = (requested == .authorized || requested == .limited)
        }
        guard authorized else {
            throw BackupError.photoPermissionDenied
        }

        let completed = completedAssetIDs()
        let assets = dependencies.photoLibraryService.fetchAssetsResult(ascendingByCreationDate: true)
        var pending = Set<String>()

        for index in 0 ..< assets.count {
            try Task.checkCancellation()
            let asset = assets.object(at: index)
            let resources = Self.selectedBackupResources(for: asset)
            if resources.isEmpty { continue }
            if !completed.contains(asset.localIdentifier) {
                pending.insert(asset.localIdentifier)
            }
        }

        return pending
    }

    private static func selectedBackupResources(for asset: PHAsset) -> [PHAssetResource] {
        let indexed = Array(PHAssetResource.assetResources(for: asset).enumerated())
        return indexed
            .sorted { lhs, rhs in
                let lhsRole = PhotoLibraryService.resourceTypeCode(lhs.1.type)
                let rhsRole = PhotoLibraryService.resourceTypeCode(rhs.1.type)
                if lhsRole != rhsRole { return lhsRole < rhsRole }

                let lhsName = lhs.1.originalFilename.lowercased()
                let rhsName = rhs.1.originalFilename.lowercased()
                if lhsName != rhsName { return lhsName < rhsName }
                return lhs.0 < rhs.0
            }
            .map(\.element)
    }

    private func primaryActionTitle(for state: State) -> String {
        switch state {
        case .idle:
            return "开始备份"
        case .running:
            return currentRunMode.isRetry ? "重试中..." : "备份中..."
        case .paused, .stopped, .failed:
            return "继续备份"
        case .completed:
            return "开始备份"
        }
    }

    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()
}

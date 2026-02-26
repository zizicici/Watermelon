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
        let resourceLocalIdentifier: String
        let originalFilename: String
        let errorMessage: String
        let retryCount: Int
        let updatedAt: Date

        var id: String { resourceLocalIdentifier }
    }

    struct ProcessedItem: Hashable, Identifiable {
        let assetLocalIdentifier: String
        let resourceLocalIdentifier: String
        let originalFilename: String
        let status: BackupItemStatus
        let reason: String?
        let updatedAt: Date

        var id: String { resourceLocalIdentifier }
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
        case retry(resourceIDs: Set<String>)

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
            case .retry(let resourceIDs):
                return resourceIDs.count
            }
        }
    }

    private let dependencies: DependencyContainer
    private var observers: [UUID: (Snapshot) -> Void] = [:]
    private var runTask: Task<Void, Never>?
    private var terminationIntent: TerminationIntent = .none
    private var currentRunMode: RunMode = .full
    private var hasStartedAtLeastOnce = false

    private(set) var state: State = .idle
    private(set) var statusText: String = "未开始"
    private(set) var succeeded: Int = 0
    private(set) var failed: Int = 0
    private(set) var skipped: Int = 0
    private(set) var total: Int = 0
    private(set) var logs: [String] = []
    private var processedItemsByResourceID: [String: ProcessedItem] = [:]
    private var retryCountByResourceID: [String: Int] = [:]
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
            processedItems: processedItemsByResourceID.values.sorted { $0.updatedAt > $1.updatedAt },
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
        startBackup(mode: .full)
    }

    @discardableResult
    func retryFailedItems(resourceIDs: Set<String>? = nil) -> Bool {
        let targetResourceIDs = resourceIDs ?? Set(failedItems.map(\.resourceLocalIdentifier))
        guard !targetResourceIDs.isEmpty else {
            statusText = "没有可重试项"
            appendLog("没有可重试的失败项")
            notifyObservers()
            return false
        }
        return startBackup(mode: .retry(resourceIDs: targetResourceIDs))
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
        hasStartedAtLeastOnce = true

        if state == .idle || state == .completed || state == .failed {
            processedItemsByResourceID.removeAll()
            retryCountByResourceID.removeAll()
            failedItems.removeAll()
            logs.removeAll()
        }

        state = .running
        statusText = mode.isRetry ? "准备重试..." : "准备备份..."
        succeeded = 0
        failed = 0
        skipped = 0
        total = 0
        appendLog(mode.isRetry ? "开始重试失败项（\(mode.retryCount)）" : "开始备份任务")
        notifyObservers()

        runTask = Task { [weak self] in
            guard let self else { return }
            do {
                let retryResourceIdentifiers: Set<String>?
                switch mode {
                case .full:
                    retryResourceIdentifiers = nil
                case .retry(let resourceIDs):
                    retryResourceIdentifiers = resourceIDs
                }

                let result = try await self.dependencies.backupExecutor.runBackup(
                    profile: profile,
                    password: password,
                    appVersion: self.dependencies.appVersion,
                    onlyResourceIdentifiers: retryResourceIdentifiers,
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

        succeeded = max(result.completed - result.skipped, 0)
        failed = result.failed
        skipped = result.skipped
        total = result.total

        if intent == .stop {
            state = .stopped
            statusText = "备份已停止"
            appendLog("任务已停止")
            rebuildFailedItems()
            notifyObservers()
            return
        }

        if result.paused || intent == .pause {
            state = .paused
            statusText = "备份已暂停"
            appendLog("任务已暂停")
            rebuildFailedItems()
            notifyObservers()
            return
        }

        state = .completed
        let verb = runMode.isRetry ? "重试" : "备份"
        statusText = result.failed == 0 ? "\(verb)完成" : "\(verb)完成（部分失败）"
        appendLog("完成: 成功\(succeeded) 失败\(result.failed) 跳过\(result.skipped)")
        rebuildFailedItems()
        notifyObservers()
    }

    private func rebuildFailedItems() {
        let all = processedItemsByResourceID.values
        let failed = all
            .filter { $0.status == .failed }
            .sorted { $0.updatedAt > $1.updatedAt }

        failedItems = failed.map { item in
            FailedItem(
                jobID: 0,
                assetLocalIdentifier: item.assetLocalIdentifier,
                resourceLocalIdentifier: item.resourceLocalIdentifier,
                originalFilename: item.originalFilename,
                errorMessage: item.reason ?? "未知错误",
                retryCount: retryCountByResourceID[item.resourceLocalIdentifier, default: 0],
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
        processedItemsByResourceID[event.resourceLocalIdentifier] = ProcessedItem(
            assetLocalIdentifier: event.assetLocalIdentifier,
            resourceLocalIdentifier: event.resourceLocalIdentifier,
            originalFilename: event.originalFilename,
            status: event.status,
            reason: event.reason,
            updatedAt: event.updatedAt
        )

        if event.status == .failed {
            retryCountByResourceID[event.resourceLocalIdentifier, default: 0] += 1
        }

        rebuildFailedItems()
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

import Foundation
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

        var buttonTitle: String {
            switch self {
            case .idle:
                return "开始备份"
            case .running:
                return "备份中..."
            case .paused:
                return "备份暂停"
            case .stopped:
                return "备份停止"
            case .failed:
                return "备份失败"
            case .completed:
                return "备份完成"
            }
        }

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

    struct Snapshot {
        let state: State
        let statusText: String
        let completed: Int
        let total: Int
        let logs: [String]
    }

    private enum TerminationIntent {
        case none
        case pause
        case stop
    }

    private let dependencies: DependencyContainer
    private var observers: [UUID: (Snapshot) -> Void] = [:]
    private var runTask: Task<Void, Never>?
    private var terminationIntent: TerminationIntent = .none

    private(set) var state: State = .idle
    private(set) var statusText: String = "未开始"
    private(set) var completed: Int = 0
    private(set) var total: Int = 0
    private(set) var logs: [String] = []

    init(dependencies: DependencyContainer) {
        self.dependencies = dependencies
    }

    func snapshot() -> Snapshot {
        Snapshot(
            state: state,
            statusText: statusText,
            completed: completed,
            total: total,
            logs: logs
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

    func startBackup() {
        guard runTask == nil else { return }
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            state = .failed
            statusText = "请先登录 SMB"
            appendLog("错误: 未登录 SMB 服务器")
            notifyObservers()
            return
        }

        terminationIntent = .none
        state = .running
        statusText = "准备备份..."
        completed = 0
        total = 0
        appendLog("开始备份任务")
        notifyObservers()

        runTask = Task { [weak self] in
            guard let self else { return }
            do {
                let result = try await self.dependencies.backupExecutor.runBackup(
                    profile: profile,
                    password: password,
                    appVersion: self.dependencies.appVersion,
                    onProgress: { [weak self] progress in
                        guard let self else { return }
                        self.completed = progress.completed
                        self.total = progress.total
                        self.statusText = progress.message
                        self.notifyObservers()
                    },
                    onLog: { [weak self] line in
                        self?.appendLog(line)
                    }
                )
                self.finishRun(result: result)
            } catch {
                let intent = self.terminationIntent
                self.runTask = nil
                self.terminationIntent = .none

                if error is CancellationError {
                    switch intent {
                    case .pause, .none:
                        self.state = .paused
                        self.statusText = "备份已暂停"
                        self.appendLog("任务已暂停")
                        self.syncManifestAfterTermination(reason: "暂停")
                    case .stop:
                        self.state = .stopped
                        self.statusText = "备份已停止"
                        self.appendLog("任务已停止")
                        self.syncManifestAfterTermination(reason: "中止")
                    }
                    self.notifyObservers()
                    return
                }

                self.state = .failed
                self.statusText = "备份失败"
                self.appendLog("错误: \(error.localizedDescription)")
                self.notifyObservers()
            }
        }
    }

    func pauseBackup() {
        guard runTask != nil else {
            state = .paused
            statusText = "备份已暂停"
            syncManifestAfterTermination(reason: "暂停")
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
            syncManifestAfterTermination(reason: "中止")
            notifyObservers()
            return
        }
        terminationIntent = .stop
        statusText = "正在停止..."
        runTask?.cancel()
        notifyObservers()
    }

    private func finishRun(result: BackupExecutionResult) {
        runTask = nil
        let intent = terminationIntent
        terminationIntent = .none

        if intent == .stop {
            state = .stopped
            statusText = "备份已停止"
            appendLog("任务已停止")
            syncManifestAfterTermination(reason: "中止")
            notifyObservers()
            return
        }

        if result.paused || intent == .pause {
            state = .paused
            statusText = "备份已暂停"
            appendLog("任务已暂停")
            syncManifestAfterTermination(reason: "暂停")
            notifyObservers()
            return
        }

        state = .completed
        statusText = result.failed == 0 ? "备份完成" : "备份完成（部分失败）"
        appendLog("完成: 成功\(result.completed) 失败\(result.failed) 跳过\(result.skipped)")
        notifyObservers()
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

    private func syncManifestAfterTermination(reason: String) {
        guard let profile = dependencies.appSession.activeProfile,
              let password = dependencies.appSession.activePassword else {
            appendLog("跳过远端索引同步：未登录 SMB")
            return
        }

        let backupExecutor = dependencies.backupExecutor
        let appVersion = dependencies.appVersion
        appendLog("正在同步远端索引（\(reason)）...")

        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            Task { [weak self] in
                guard let self else { return }
                do {
                    try await backupExecutor.syncManifest(
                        profile: profile,
                        password: password,
                        appVersion: appVersion
                    )
                    self.appendLog("远端索引已更新（\(reason)）")
                } catch {
                    self.appendLog("远端索引同步失败（\(reason)）: \(error.localizedDescription)")
                }
            }
        }
    }

    private static let timeFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter
    }()
}

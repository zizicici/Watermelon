import Foundation
import os.log

private let refreshLog = Logger(subsystem: "com.zizicici.watermelon", category: "HomeRefresh")

@MainActor
final class HomeRefreshScheduler {
    struct Work: OptionSet, Sendable {
        let rawValue: Int

        static let reloadLocal = Work(rawValue: 1 << 0)
        static let syncRemote = Work(rawValue: 1 << 1)
        static let notifyConnection = Work(rawValue: 1 << 2)
        static let notifyStructural = Work(rawValue: 1 << 3)
        static let reconcileLocal = Work(rawValue: 1 << 4)

        var description: String {
            var parts: [String] = []
            if contains(.reloadLocal) { parts.append("reloadLocal") }
            if contains(.syncRemote) { parts.append("syncRemote") }
            if contains(.notifyConnection) { parts.append("notifyConnection") }
            if contains(.notifyStructural) { parts.append("notifyStructural") }
            if contains(.reconcileLocal) { parts.append("reconcileLocal") }
            return parts.joined(separator: ",")
        }
    }

    struct Hooks {
        /// Returns `scopeChanged`.
        let normalizeBeforeReload: () -> Bool
        let isReloadBlocked: () -> Bool
        let reloadLocal: () async -> Void
        let reconcileLocal: () async -> Set<LibraryMonthKey>
        /// Returns `accessChanged`.
        let refreshAccessState: () -> Bool
        /// Called once per iteration after reloadLocal completes, before syncRemote runs.
        /// `hasMoreReloadPending` reflects whether another reloadLocal pass is queued.
        let afterReload: (_ scopeChanged: Bool, _ accessChanged: Bool, _ hasMoreReloadPending: Bool) -> Void
        let syncRemote: () async -> Void
        let onIterationComplete: (_ work: Work, _ scopeChanged: Bool, _ accessChanged: Bool, _ localMonths: Set<LibraryMonthKey>) -> Void
    }

    private let hooks: Hooks
    private let coalescingInterval: Duration
    private var coalescingTask: Task<Void, Never>?
    private var task: Task<Void, Never>?
    private var pending: Work = []
    private var isRunningReloadLocal = false
    private var isActive: Bool
    private var presentationRevision: UInt64 = 0

    var hasQueuedOrRunningReloadLocal: Bool {
        isRunningReloadLocal || pending.contains(.reloadLocal)
    }

    var isReloadUnderway: Bool {
        isRunningReloadLocal || (pending.contains(.reloadLocal) && !hooks.isReloadBlocked())
    }

    private var runnableWork: Work {
        hooks.isReloadBlocked() ? pending.subtracting(.reloadLocal) : pending
    }

    func resumeIfPossible() {
        startIfNeeded()
    }

    init(initiallyActive: Bool = true, coalescingInterval: Duration = .milliseconds(200), hooks: Hooks) {
        self.isActive = initiallyActive
        self.coalescingInterval = coalescingInterval
        self.hooks = hooks
    }

    deinit {
        coalescingTask?.cancel()
        task?.cancel()
    }

    func enqueue(_ work: Work, coalescing: Bool = false) {
        guard !work.isEmpty else { return }
        if !work.intersection([.reloadLocal, .notifyConnection]).isEmpty {
            presentationRevision &+= 1
        }
        pending.formUnion(work)
        if coalescing, runnableWork == .reconcileLocal || coalescingTask != nil {
            scheduleCoalescingWindow()
        } else {
            cancelCoalescingWindow()
        }
        startIfNeeded()
    }

    func setActive(_ active: Bool) {
        guard isActive != active else { return }
        isActive = active
        presentationRevision &+= 1
        if active { startIfNeeded() }
        else { cancelCoalescingWindow() }
    }

    private func scheduleCoalescingWindow() {
        guard isActive, coalescingTask == nil else { return }
        let interval = coalescingInterval
        coalescingTask = Task { [weak self] in
            do { try await Task.sleep(for: interval) } catch { return }
            guard !Task.isCancelled else { return }
            self?.coalescingTask = nil
            self?.startIfNeeded()
        }
    }

    private func cancelCoalescingWindow() {
        coalescingTask?.cancel()
        coalescingTask = nil
    }

    private func startIfNeeded() {
        guard isActive, !runnableWork.isEmpty, task == nil, coalescingTask == nil else { return }
        task = Task { [weak self] in
            guard let self else { return }
            await self.runUntilDrained()
            self.task = nil
            self.startIfNeeded()
        }
    }

    #if DEBUG
    func _testWaitUntilIdle() async {
        while let scheduled = task ?? coalescingTask {
            await scheduled.value
        }
    }
    #endif

    private func runUntilDrained() async {
        while !Task.isCancelled, isActive {
            let work = runnableWork
            pending.subtract(work)
            cancelCoalescingWindow()
            guard !work.isEmpty else { return }
            let presentationRevision = self.presentationRevision

            let start = CFAbsoluteTimeGetCurrent()
            var scopeChanged = false
            var accessChanged = false
            var localMonths = Set<LibraryMonthKey>()
            var reloadElapsed = 0.0
            var syncElapsed = 0.0

            if work.contains(.reloadLocal) {
                let reloadStart = CFAbsoluteTimeGetCurrent()
                isRunningReloadLocal = true
                defer { isRunningReloadLocal = false }
                scopeChanged = hooks.normalizeBeforeReload()
                await hooks.reloadLocal()
                accessChanged = hooks.refreshAccessState()
                hooks.afterReload(
                    scopeChanged,
                    accessChanged,
                    pending.contains(.reloadLocal)
                )
                reloadElapsed = CFAbsoluteTimeGetCurrent() - reloadStart
                guard !Task.isCancelled else { break }
                guard isActive else {
                    pending.formUnion(work.subtracting([.reloadLocal, .reconcileLocal]))
                    pending.insert(.notifyStructural)
                    return
                }
            } else if work.contains(.reconcileLocal) {
                localMonths = await hooks.reconcileLocal()
                guard !Task.isCancelled else { break }
                guard isActive else {
                    pending.formUnion(work.subtracting(.reconcileLocal))
                    pending.insert(.notifyStructural)
                    return
                }
            }

            if work.contains(.syncRemote) {
                let syncStart = CFAbsoluteTimeGetCurrent()
                await hooks.syncRemote()
                syncElapsed = CFAbsoluteTimeGetCurrent() - syncStart
                guard !Task.isCancelled else { break }
                guard isActive else {
                    pending.formUnion(work.intersection([.notifyConnection, .notifyStructural]))
                    pending.insert(.notifyStructural)
                    return
                }
            }

            // Ordinary commits must not postpone presentation; scope, connection and foreground changes must.
            if presentationRevision != self.presentationRevision {
                pending.formUnion(work.intersection([.notifyConnection, .notifyStructural]))
                pending.insert(.notifyStructural)
                if coalescingTask != nil { return }
                continue
            }

            let postStart = CFAbsoluteTimeGetCurrent()
            hooks.onIterationComplete(work, scopeChanged, accessChanged, localMonths)
            let postElapsed = CFAbsoluteTimeGetCurrent() - postStart

            let elapsed = CFAbsoluteTimeGetCurrent() - start
            refreshLog.info("[HomeRefresh] work=\(work.description, privacy: .public), reload=\(String(format: "%.3f", reloadElapsed))s, sync=\(String(format: "%.3f", syncElapsed))s, post=\(String(format: "%.3f", postElapsed))s, total=\(String(format: "%.3f", elapsed))s")
            if coalescingTask != nil { return }
        }
    }
}

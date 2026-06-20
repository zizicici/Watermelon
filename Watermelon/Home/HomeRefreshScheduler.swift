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
        // Lighter than reloadLocal: re-read DB fingerprints only, no PhotoKit re-scan.
        static let refreshFingerprints = Work(rawValue: 1 << 4)

        var description: String {
            var parts: [String] = []
            if contains(.reloadLocal) { parts.append("reloadLocal") }
            if contains(.refreshFingerprints) { parts.append("refreshFingerprints") }
            if contains(.syncRemote) { parts.append("syncRemote") }
            if contains(.notifyConnection) { parts.append("notifyConnection") }
            if contains(.notifyStructural) { parts.append("notifyStructural") }
            return parts.joined(separator: ",")
        }
    }

    struct Hooks {
        /// Returns `scopeChanged`.
        let normalizeBeforeReload: () -> Bool
        let reloadLocal: () async -> Void
        /// Re-read DB fingerprints without re-scanning PhotoKit (connect-path fast lane).
        let refreshFingerprints: () async -> Void
        /// Returns `accessChanged`.
        let refreshAccessState: () -> Bool
        /// Called once per iteration after reloadLocal completes, before syncRemote runs.
        /// `hasMoreReloadPending` reflects whether another reloadLocal pass is queued.
        let afterReload: (_ scopeChanged: Bool, _ accessChanged: Bool, _ hasMoreReloadPending: Bool) -> Void
        let syncRemote: () async -> Void
        /// Called once per iteration after syncRemote (refresh row lookup, rebuild sections).
        let postProcess: () -> Void
        /// Called once per iteration after postProcess to drive UI notifications.
        let onIterationComplete: (_ work: Work, _ scopeChanged: Bool, _ accessChanged: Bool) -> Void
    }

    private let hooks: Hooks
    private var task: Task<Void, Never>?
    private var pending: Work = []

    init(hooks: Hooks) {
        self.hooks = hooks
    }

    deinit {
        task?.cancel()
    }

    func enqueue(_ work: Work) {
        pending.formUnion(work)
        guard task == nil else { return }
        task = Task { [weak self] in
            guard let self else { return }
            await self.runUntilDrained()
            self.task = nil
        }
    }

    func cancel() {
        task?.cancel()
        task = nil
        pending = []
    }

    #if DEBUG
    /// Await the in-flight task, if any. Used by tests to drain the scheduler before
    /// asserting against hook recorders. Production code does not call this.
    func _testWaitUntilIdle() async {
        await task?.value
    }
    #endif

    private func runUntilDrained() async {
        while !Task.isCancelled {
            let work = self.pending
            self.pending = []
            guard !work.isEmpty else { return }

            let start = CFAbsoluteTimeGetCurrent()
            var scopeChanged = false
            var accessChanged = false
            var reloadElapsed = 0.0
            var syncElapsed = 0.0

            if work.contains(.reloadLocal) {
                let reloadStart = CFAbsoluteTimeGetCurrent()
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
            }

            // reloadLocal already loads fresh DB fingerprints, so only run the fast lane on its own.
            if work.contains(.refreshFingerprints), !work.contains(.reloadLocal) {
                let fpStart = CFAbsoluteTimeGetCurrent()
                await hooks.refreshFingerprints()
                reloadElapsed = CFAbsoluteTimeGetCurrent() - fpStart
                guard !Task.isCancelled else { break }
            }

            if work.contains(.syncRemote) {
                let syncStart = CFAbsoluteTimeGetCurrent()
                await hooks.syncRemote()
                syncElapsed = CFAbsoluteTimeGetCurrent() - syncStart
                guard !Task.isCancelled else { break }
            }

            let postStart = CFAbsoluteTimeGetCurrent()
            hooks.postProcess()
            let postElapsed = CFAbsoluteTimeGetCurrent() - postStart

            let elapsed = CFAbsoluteTimeGetCurrent() - start
            refreshLog.info("[HomeRefresh] work=\(work.description, privacy: .public), reload=\(String(format: "%.3f", reloadElapsed))s, sync=\(String(format: "%.3f", syncElapsed))s, post=\(String(format: "%.3f", postElapsed))s, total=\(String(format: "%.3f", elapsed))s")

            hooks.onIterationComplete(work, scopeChanged, accessChanged)
        }
    }
}

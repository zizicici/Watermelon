import Foundation

@MainActor
final class HomeLocalIndexReloadCoordinator {
    struct Hooks {
        let isBlocked: @MainActor () -> Bool
        let hasQueuedOrRunningReload: @MainActor () -> Bool
        let enqueue: @MainActor (HomeRefreshScheduler.Work) -> Void
        let notifyAvailabilityChanged: @MainActor () -> Void
    }

    private let hooks: Hooks
    private var pendingWork: HomeRefreshScheduler.Work = []
    private var pendingOnEnqueued: (@MainActor () -> Void)?

    var isReloading: Bool {
        !pendingWork.isEmpty || hooks.hasQueuedOrRunningReload()
    }

    init(hooks: Hooks) {
        self.hooks = hooks
    }

    func schedule(
        _ work: HomeRefreshScheduler.Work,
        onEnqueued: (@MainActor () -> Void)? = nil
    ) {
        guard !work.isEmpty else { return }
        guard !deferIfBlocked(work, onEnqueued: onEnqueued) else { return }
        let pending: (work: HomeRefreshScheduler.Work, onEnqueued: (@MainActor () -> Void)?)
        if hooks.isBlocked() {
            pending = (work, onEnqueued)
        } else {
            pending = takePendingWork(mergingInto: work, onEnqueued: onEnqueued)
        }
        hooks.enqueue(pending.work)
        pending.onEnqueued?()
    }

    @discardableResult
    func deferIfBlocked(
        _ work: HomeRefreshScheduler.Work,
        onEnqueued: (@MainActor () -> Void)? = nil
    ) -> Bool {
        guard work.contains(.reloadLocal), hooks.isBlocked() else { return false }
        deferWork(work, onEnqueued: onEnqueued)
        return true
    }

    private func deferWork(
        _ work: HomeRefreshScheduler.Work,
        onEnqueued: (@MainActor () -> Void)?
    ) {
        pendingWork.formUnion(work)
        appendPendingOnEnqueued(onEnqueued)
        hooks.notifyAvailabilityChanged()
    }

    func replayIfPossible() {
        guard !hooks.isBlocked(), !pendingWork.isEmpty else { return }
        let work = pendingWork
        let onEnqueued = pendingOnEnqueued
        pendingWork = []
        pendingOnEnqueued = nil
        schedule(work, onEnqueued: onEnqueued)
    }

    private func appendPendingOnEnqueued(_ onEnqueued: (@MainActor () -> Void)?) {
        pendingOnEnqueued = combine(pendingOnEnqueued, onEnqueued)
    }

    private func takePendingWork(
        mergingInto work: HomeRefreshScheduler.Work,
        onEnqueued: (@MainActor () -> Void)?
    ) -> (work: HomeRefreshScheduler.Work, onEnqueued: (@MainActor () -> Void)?) {
        guard !pendingWork.isEmpty else {
            return (work, onEnqueued)
        }
        var mergedWork = work
        mergedWork.formUnion(pendingWork)
        let pending = pendingOnEnqueued
        pendingWork = []
        pendingOnEnqueued = nil
        return (mergedWork, combine(pending, onEnqueued))
    }

    private func combine(
        _ first: (@MainActor () -> Void)?,
        _ second: (@MainActor () -> Void)?
    ) -> (@MainActor () -> Void)? {
        switch (first, second) {
        case (nil, nil):
            return nil
        case (let callback?, nil), (nil, let callback?):
            return callback
        case (let first?, let second?):
            return {
                first()
                second()
            }
        }
    }
}

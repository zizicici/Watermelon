import Foundation

@MainActor
final class PiPExecutionBridge {
    private let coordinator: HomeExecutionCoordinator
    private let pip: PiPProgressManager

    private var lastPhase: ExecutionPhase?
    private var lastStatusText: String?
    private var lastEmittedLogID: UUID?
    private var logObserverID: UUID?

    init(coordinator: HomeExecutionCoordinator) {
        self.coordinator = coordinator
        self.pip = .shared
    }

    func attach() {
        guard logObserverID == nil else { return }
        logObserverID = coordinator.addLogObserver { [weak self] snapshot in
            self?.apply(snapshot)
        }
    }

    func detach() {
        if let id = logObserverID {
            coordinator.removeLogObserver(id)
            logObserverID = nil
        }
    }

    func observeStateChange() {
        let current = coordinator.phase
        let previous = lastPhase
        lastPhase = current

        let wasRunning = isRunning(previous)
        let isNowRunning = isRunning(current)

        if !wasRunning && isNowRunning {
            let title = coordinator.currentLogSnapshot.statusText
            lastStatusText = title
            pip.taskDidStart(title: title)
            return
        }

        if wasRunning && !isNowRunning {
            switch current {
            case .some(.completed):
                pip.taskDidComplete()
            case .some(.failed(let reason)):
                pip.taskDidFail(message: reason)
            case nil:
                pip.taskDidCancel()
            default:
                break
            }
        }
    }

    private func apply(_ snapshot: HomeExecutionLogSnapshot) {
        if snapshot.statusText != lastStatusText {
            lastStatusText = snapshot.statusText
            pip.updateStatus(snapshot.statusText)
        }

        // Diff by id, not count: the live buffer is bounded and drops its oldest entries.
        let newEntries: ArraySlice<ExecutionLogEntry>
        if let lastEmittedLogID,
           let lastIndex = snapshot.entries.lastIndex(where: { $0.id == lastEmittedLogID }) {
            newEntries = snapshot.entries[(lastIndex + 1)...]
        } else {
            newEntries = snapshot.entries[...]
        }
        for entry in newEntries {
            pip.appendLog(entry)
        }
        lastEmittedLogID = snapshot.entries.last?.id
    }

    private func isRunning(_ phase: ExecutionPhase?) -> Bool {
        switch phase {
        case .some(.uploading), .some(.uploadPaused), .some(.downloading), .some(.downloadPaused):
            return true
        case .some(.completed), .some(.failed), nil:
            return false
        }
    }
}

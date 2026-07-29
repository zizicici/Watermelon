import Foundation

@MainActor
final class PiPExecutionBridge {
    private let coordinator: HomeExecutionCoordinator
    private let pip: PiPProgressManager

    private var lastPhase: ExecutionPhase?
    private var lastStatusText: String?
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

        for event in PiPExecutionTransitionResolver.events(from: previous, to: current) {
            switch event {
            case .start:
                let title = coordinator.currentLogSnapshot.statusText
                lastStatusText = title
                pip.taskDidStart(title: title)
            case .resume:
                pip.taskDidResume()
            case .setPaused(let paused):
                pip.setPaused(paused)
            case .complete:
                pip.taskDidComplete()
            case .fail:
                pip.taskDidFail()
            case .cancel:
                pip.taskDidCancel()
            }
        }
    }

    private func apply(_ snapshot: HomeExecutionLogSnapshot) {
        if snapshot.statusText != lastStatusText {
            lastStatusText = snapshot.statusText
            pip.updateStatus(snapshot.statusText)
        }
        pip.updateTransferMetrics(snapshot.transferMetrics)
    }
}

import Foundation

struct PiPElapsedTimeTracker {
    private var accumulatedTime: TimeInterval = 0
    private var runningSince: TimeInterval?

    mutating func start(at now: TimeInterval) {
        accumulatedTime = 0
        runningSince = now
    }

    mutating func setPaused(_ paused: Bool, at now: TimeInterval) {
        if paused {
            guard let runningSince else { return }
            accumulatedTime += max(0, now - runningSince)
            self.runningSince = nil
        } else if runningSince == nil {
            runningSince = now
        }
    }

    func elapsed(at now: TimeInterval) -> TimeInterval {
        guard let runningSince else { return accumulatedTime }
        return accumulatedTime + max(0, now - runningSince)
    }

    mutating func stop(at now: TimeInterval) -> TimeInterval {
        let elapsedTime = elapsed(at: now)
        accumulatedTime = elapsedTime
        runningSince = nil
        return elapsedTime
    }

    mutating func reset() {
        accumulatedTime = 0
        runningSince = nil
    }
}

import Foundation

enum PiPExecutionTransitionEvent: Equatable {
    case start
    case resume
    case setPaused(Bool)
    case complete
    case fail
    case cancel
}

enum PiPExecutionTransitionResolver {
    static func events(
        from previous: ExecutionPhase?,
        to current: ExecutionPhase?
    ) -> [PiPExecutionTransitionEvent] {
        let wasRunning = isRunning(previous)
        let isNowRunning = isRunning(current)
        var events: [PiPExecutionTransitionEvent] = []

        if !wasRunning && isNowRunning {
            events.append(.start)
        } else if isPaused(previous) && isActive(current) {
            events.append(.resume)
        }

        events.append(.setPaused(isPaused(current)))

        if wasRunning && !isNowRunning {
            switch current {
            case .some(.completed):
                events.append(.complete)
            case .some(.failed):
                events.append(.fail)
            case nil:
                events.append(.cancel)
            default:
                break
            }
        }
        return events
    }

    private static func isRunning(_ phase: ExecutionPhase?) -> Bool {
        switch phase {
        case .some(.uploading), .some(.uploadPaused), .some(.downloading), .some(.downloadPaused):
            return true
        case .some(.completed), .some(.failed), nil:
            return false
        }
    }

    private static func isPaused(_ phase: ExecutionPhase?) -> Bool {
        switch phase {
        case .some(.uploadPaused), .some(.downloadPaused):
            return true
        default:
            return false
        }
    }

    private static func isActive(_ phase: ExecutionPhase?) -> Bool {
        switch phase {
        case .some(.uploading), .some(.downloading):
            return true
        default:
            return false
        }
    }
}

enum MacRunningTaskWindowCloseAction: Equatable {
    case keepOpen
    case close
    case stopThenClose
}

enum MacRunningTaskWindowClosePolicy {
    static func action(
        stopConfirmed: Bool,
        isTaskRunning: Bool
    ) -> MacRunningTaskWindowCloseAction {
        guard stopConfirmed else {
            return .keepOpen
        }
        return isTaskRunning ? .stopThenClose : .close
    }
}

enum MacPhotoBrowserBatchDeleteOutcome: Equatable, Sendable {
    case completed(
        localChanged: Bool,
        remoteChanged: Bool,
        failed: Int
    )
    case cancelled(
        localChanged: Bool,
        remoteChanged: Bool
    )

    var localChanged: Bool {
        switch self {
        case .completed(let localChanged, _, _),
             .cancelled(let localChanged, _):
            return localChanged
        }
    }

    var remoteChanged: Bool {
        switch self {
        case .completed(_, let remoteChanged, _),
             .cancelled(_, let remoteChanged):
            return remoteChanged
        }
    }

    var failedCount: Int {
        switch self {
        case .completed(_, _, let failed):
            return failed
        case .cancelled:
            return 0
        }
    }

    var shouldReload: Bool {
        switch self {
        case .completed:
            return true
        case .cancelled:
            return localChanged || remoteChanged
        }
    }
}

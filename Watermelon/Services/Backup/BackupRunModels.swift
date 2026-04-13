import Foundation

enum BackupRunMode: Sendable {
    case full
    case scoped(assetIDs: Set<String>)
    case retry(assetIDs: Set<String>)

    var isRetry: Bool {
        if case .retry = self {
            return true
        }
        return false
    }

    var targetAssetIdentifiers: Set<String>? {
        switch self {
        case .full:
            return nil
        case .retry(let assetIDs):
            return assetIDs
        case .scoped(let assetIDs):
            return assetIDs
        }
    }
}

enum BackupTerminationIntent: Sendable {
    case none
    case pause
    case stop
}

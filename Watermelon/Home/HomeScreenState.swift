import Foundation
import Photos

// MARK: - Connection State

enum ConnectionState {
    case disconnected
    case connecting(ServerProfileRecord)
    case connected(ServerProfileRecord)

    var isConnected: Bool {
        if case .connected = self { return true }
        return false
    }

    var isConnecting: Bool {
        if case .connecting = self { return true }
        return false
    }

    var activeProfile: ServerProfileRecord? {
        switch self {
        case .connected(let p), .connecting(let p): return p
        case .disconnected: return nil
        }
    }
}

enum LocalPhotoAccessState: Equatable {
    case authorized
    case notDetermined
    case denied

    init(authorizationStatus: PHAuthorizationStatus) {
        switch authorizationStatus {
        case .authorized, .limited:
            self = .authorized
        case .notDetermined:
            self = .notDetermined
        case .denied, .restricted:
            self = .denied
        @unknown default:
            self = .denied
        }
    }

    var isAuthorized: Bool {
        self == .authorized
    }
}

// MARK: - Selection State

enum SelectionSide {
    case local, remote
}

struct SelectionState {
    var localMonths = Set<LibraryMonthKey>()
    var remoteMonths = Set<LibraryMonthKey>()

    var isEmpty: Bool { localMonths.isEmpty && remoteMonths.isEmpty }

    mutating func clear() {
        localMonths.removeAll()
        remoteMonths.removeAll()
    }

    func arrowDirection(for month: LibraryMonthKey) -> HomeArrowDirection? {
        switch (localMonths.contains(month), remoteMonths.contains(month)) {
        case (true, false):  return .toRemote
        case (false, true):  return .toLocal
        case (true, true):   return .sync
        case (false, false): return nil
        }
    }

    func selectionState(for months: Set<LibraryMonthKey>, side: SelectionSide) -> HomeSelectionState {
        let selected = side == .local ? localMonths : remoteMonths
        guard !months.isEmpty else { return .none }
        if months.isSubset(of: selected) { return .all }
        if !months.isDisjoint(with: selected) { return .partial }
        return .none
    }

    func counts() -> (backup: Int, download: Int, sync: Int) {
        let allSelected = localMonths.union(remoteMonths)
        var backup = 0, download = 0, sync = 0
        for month in allSelected {
            switch arrowDirection(for: month) {
            case .toRemote: backup += 1
            case .toLocal:  download += 1
            case .sync:     sync += 1
            case nil:       break
            }
        }
        return (backup, download, sync)
    }

    func months(for direction: HomeArrowDirection) -> [LibraryMonthKey] {
        localMonths.union(remoteMonths)
            .filter { arrowDirection(for: $0) == direction }
            .sorted()
    }
}

// MARK: - Month Event

enum MonthEvent {
    case uploadStarted
    case uploadPaused
    case uploadResumed
    case uploadCompleted
    case partiallyFailed(count: Int)
    case downloadStarted
    case downloadPaused
    case downloadResumed
    case downloadCompleted
    case failed(reason: String)
    case completed
}

// MARK: - Execution State

struct MonthPlan {
    let needsUpload: Bool
    let needsDownload: Bool
    var phase: Phase = .pending
    var failedItemCount: Int = 0
    var failureMessage: String?

    enum Phase {
        case pending
        case uploading
        case uploadPaused
        case uploadDone
        case downloading
        case downloadPaused
        case completed
        case partiallyFailed
        case failed
    }

    var isTerminal: Bool { phase == .completed || phase == .failed || phase == .partiallyFailed }
    var isFullyCompleted: Bool { phase == .completed }
    var isDone: Bool { phase == .completed || phase == .partiallyFailed }
    var isFailed: Bool { phase == .failed }
    var isActive: Bool { phase == .uploading || phase == .downloading }
    var direction: HomeArrowDirection? {
        switch (needsUpload, needsDownload) {
        case (true, false): return .toRemote
        case (false, true): return .toLocal
        case (true, true): return .sync
        case (false, false): return nil
        }
    }

    mutating func apply(_ event: MonthEvent) {
        switch (phase, event) {
        case (.pending, .uploadStarted):
            phase = .uploading
        case (.uploading, .uploadPaused):
            phase = .uploadPaused
        case (.uploadPaused, .uploadResumed):
            phase = .uploading
        case (.uploading, .uploadCompleted),
             (.pending, .uploadCompleted):
            phase = needsDownload ? .uploadDone : .completed
        case (.uploading, .partiallyFailed(let count)),
             (.uploadDone, .partiallyFailed(let count)),
             (.completed, .partiallyFailed(let count)):
            // partiallyFailed can override uploadDone/completed because
            // failedCountByMonth is only available at session completion,
            // after individual month completions have already been reported
            // by progress snapshots.
            phase = .partiallyFailed
            failedItemCount = count
            failureMessage = String(format: String(localized: "home.execution.failedItems"), count)
        case (.uploadDone, .downloadStarted),
             (.pending, .downloadStarted):
            phase = .downloading
        case (.downloading, .downloadPaused):
            phase = .downloadPaused
        case (.downloadPaused, .downloadResumed):
            phase = .downloading
        case (.downloading, .downloadCompleted):
            phase = .completed
        case (_, .failed(let reason)) where !isTerminal:
            phase = .failed
            failureMessage = reason
        case (_, .completed) where !isTerminal:
            phase = .completed
        default:
            break
        }
    }
}

enum ExecutionPhase: Equatable {
    case uploading
    case uploadPaused
    case downloading
    case downloadPaused
    case completed
    case failed(String)
}

enum ExecutionControlState: Equatable {
    case idle
    case starting
    case resuming
    case pausing
    case stopping
}

struct MonthFailureInfo {
    let month: LibraryMonthKey
    let message: String
}

struct HomeExecutionState {
    let monthPlans: [LibraryMonthKey: MonthPlan]
    let phase: ExecutionPhase
    let controlState: ExecutionControlState
    let statusText: String
    let processedCountByMonth: [LibraryMonthKey: Int]
    let assetCountByMonth: [LibraryMonthKey: Int]
    let uploadMonths: [LibraryMonthKey]
    let downloadMonths: [LibraryMonthKey]
    let syncMonths: [LibraryMonthKey]

    var executionMonths: Set<LibraryMonthKey> { Set(monthPlans.keys) }

    var failedMonthInfos: [MonthFailureInfo] {
        monthPlans.compactMap { month, plan in
            switch plan.phase {
            case .failed:
                return MonthFailureInfo(month: month, message: plan.failureMessage ?? String(localized: "home.execution.failed"))
            case .partiallyFailed:
                return MonthFailureInfo(month: month, message: plan.failureMessage ?? String(format: String(localized: "home.execution.failedItems"), plan.failedItemCount))
            default:
                return nil
            }
        }.sorted { $0.month < $1.month }
    }

    func direction(for month: LibraryMonthKey) -> HomeArrowDirection? {
        monthPlans[month]?.direction
    }

    func progressPercent(for month: LibraryMonthKey, row: HomeMonthRow?, direction fallbackDirection: HomeArrowDirection?, matchedCount: Int) -> Double? {
        let basePercent = HomeProgressCalculator.basePercent(
            row: row,
            direction: direction(for: month) ?? fallbackDirection,
            matchedCount: matchedCount
        )

        if monthPlans[month] != nil {
            if monthPlans[month]?.isFullyCompleted == true {
                return 100.0
            }
            if let total = assetCountByMonth[month], total > 0,
               let processed = processedCountByMonth[month], processed > 0 {
                let sessionPercent = Double(processed) / Double(total) * 100
                return max(sessionPercent, basePercent ?? 0)
            }
        }

        return basePercent
    }
}

// MARK: - Change Kind

enum HomeChangeKind {
    case data(Set<LibraryMonthKey>)
    case fileSizes(Set<LibraryMonthKey>)
    case selection
    case execution(Set<LibraryMonthKey>)
    case connection
    case connectionProgress
    case structural
}

enum HomeProgressCalculator {
    static func basePercent(
        row: HomeMonthRow?,
        direction: HomeArrowDirection?,
        matchedCount: Int
    ) -> Double? {
        guard let row, let direction else { return nil }

        let localCount = row.local?.assetCount ?? 0
        let remoteCount = row.remote?.assetCount ?? 0

        switch direction {
        case .toRemote:
            return localCount > 0 ? Double(matchedCount) / Double(localCount) * 100 : nil
        case .toLocal:
            return remoteCount > 0 ? Double(matchedCount) / Double(remoteCount) * 100 : nil
        case .sync:
            let remoteOnly = max(0, remoteCount - matchedCount)
            let total = localCount + remoteOnly
            return total > 0 ? Double(matchedCount) / Double(total) * 100 : nil
        }
    }
}

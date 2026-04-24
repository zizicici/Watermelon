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

    func intent(for month: LibraryMonthKey) -> MonthIntent? {
        switch (localMonths.contains(month), remoteMonths.contains(month)) {
        case (true, false):  return .backup
        case (false, true):  return .download
        case (true, true):   return .complement
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

    func counts() -> (backup: Int, download: Int, complement: Int) {
        let allSelected = localMonths.union(remoteMonths)
        var backup = 0, download = 0, complement = 0
        for month in allSelected {
            switch intent(for: month) {
            case .backup:     backup += 1
            case .download:   download += 1
            case .complement: complement += 1
            case nil:         break
            }
        }
        return (backup, download, complement)
    }

    func months(for targetIntent: MonthIntent) -> [LibraryMonthKey] {
        localMonths.union(remoteMonths)
            .filter { intent(for: $0) == targetIntent }
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
    var intent: MonthIntent? {
        switch (needsUpload, needsDownload) {
        case (true, false): return .backup
        case (false, true): return .download
        case (true, true): return .complement
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
    let backupMonths: [LibraryMonthKey]
    let downloadMonths: [LibraryMonthKey]
    let complementMonths: [LibraryMonthKey]

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

    func intent(for month: LibraryMonthKey) -> MonthIntent? {
        monthPlans[month]?.intent
    }

    func progressPercent(for month: LibraryMonthKey, row: HomeMonthRow?, intent fallbackIntent: MonthIntent?, matchedCount: Int) -> Double? {
        let basePercent = HomeProgressCalculator.basePercent(
            row: row,
            intent: intent(for: month) ?? fallbackIntent,
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
        intent: MonthIntent?,
        matchedCount: Int
    ) -> Double? {
        guard let row, let intent else { return nil }

        let localCount = row.local?.assetCount ?? 0
        let remoteCount = row.remote?.assetCount ?? 0

        switch intent {
        case .backup:
            return localCount > 0 ? Double(matchedCount) / Double(localCount) * 100 : nil
        case .download:
            return remoteCount > 0 ? Double(matchedCount) / Double(remoteCount) * 100 : nil
        case .complement:
            let remoteOnly = max(0, remoteCount - matchedCount)
            let total = localCount + remoteOnly
            return total > 0 ? Double(matchedCount) / Double(total) * 100 : nil
        }
    }
}

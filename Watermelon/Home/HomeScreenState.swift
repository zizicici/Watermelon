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
    case downloadStarted
    case downloadPaused
    case downloadResumed
    case downloadCompleted
    case downloadAttemptFinished
    case completed
    case recordUploadFailures(observedFailedItemCount: Int)
    case recordIncomplete(BackupMonthIncompleteSummary)
    case recordTerminalFailure(MonthTerminalFailure)
}

// MARK: - Execution State

struct MonthFailureFacts: Equatable, Sendable {
    var uploadFailedItemCount: Int = 0
    var incomplete: BackupMonthIncompleteSummary = .init()
    var terminalFailure: MonthTerminalFailure?

    var isEmpty: Bool {
        uploadFailedItemCount == 0 && incomplete.isEmpty && terminalFailure == nil
    }

    var hasTerminalFailure: Bool { terminalFailure != nil }
    var hasNonFatalIssues: Bool {
        uploadFailedItemCount > 0 || !incomplete.isEmpty
    }
    var hasUserVisibleFailure: Bool {
        hasTerminalFailure || hasNonFatalIssues
    }

    mutating func recordUploadFailures(observedFailedItemCount: Int) {
        uploadFailedItemCount = max(uploadFailedItemCount, max(observedFailedItemCount, 0))
    }

    mutating func recordIncomplete(_ summary: BackupMonthIncompleteSummary) {
        incomplete.mergeObserved(summary)
    }

    mutating func recordTerminalFailure(_ failure: MonthTerminalFailure) {
        terminalFailure = failure
    }

    func displayMessage(for month: LibraryMonthKey) -> String {
        var parts: [String] = []
        if uploadFailedItemCount > 0 {
            parts.append(String.localizedStringWithFormat(
                String(localized: "home.execution.failedItems"),
                uploadFailedItemCount
            ))
        }
        parts.append(contentsOf: BackupMonthIncompleteSummaryRenderer.messageParts(
            for: incomplete,
            month: month
        ))
        if let message = terminalFailure?.message, !message.isEmpty {
            parts.append(message)
        }
        return parts.isEmpty ? String(localized: "home.execution.failed") : parts.joined(separator: ". ")
    }
}

struct MonthWorkFacts: Equatable, Sendable {
    var uploadFinished = false
    var downloadStarted = false
    var downloadFinished = false

    var hasActiveDownloadAttempt: Bool {
        downloadStarted && !downloadFinished
    }
}

struct MonthTerminalFailure: Equatable, Sendable {
    enum Kind: Equatable, Sendable {
        case missingConnection
        case localIndexIncomplete
        case backupStartFailed
        case uploadRunFailed
        case downloadRunFailed
        case compatibility
        case generic
    }

    let kind: Kind
    let message: String
}

struct MonthPlan {
    let needsUpload: Bool
    let needsDownload: Bool
    var phase: Phase = .pending
    var failureFacts = MonthFailureFacts()
    var workFacts = MonthWorkFacts()

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
    var hasTerminalFailure: Bool { failureFacts.hasTerminalFailure }
    var hasUserVisibleFailure: Bool { failureFacts.hasUserVisibleFailure }
    var isActive: Bool { phase == .uploading || phase == .downloading }
    var hasPendingDownloadWork: Bool {
        needsDownload && !workFacts.downloadFinished && !failureFacts.hasTerminalFailure
    }
    var canStartInlineComplementDownload: Bool {
        needsUpload && needsDownload && hasPendingDownloadWork
    }
    var intent: MonthIntent? {
        switch (needsUpload, needsDownload) {
        case (true, false): return .backup
        case (false, true): return .download
        case (true, true): return .complement
        case (false, false): return nil
        }
    }

    mutating func apply(_ event: MonthEvent) {
        switch event {
        case .recordUploadFailures(let observedFailedItemCount):
            failureFacts.recordUploadFailures(observedFailedItemCount: observedFailedItemCount)
            refreshTerminalPhaseProjection()
        case .recordIncomplete(let summary):
            failureFacts.recordIncomplete(summary)
            if summary.metadataSnapshotDeferredMessage != nil {
                workFacts.uploadFinished = true
            }
            refreshTerminalPhaseProjection()
        case .recordTerminalFailure(let failure):
            failureFacts.recordTerminalFailure(failure)
            refreshTerminalPhaseProjection()
        case .uploadStarted:
            if phase == .pending {
                phase = .uploading
            }
            refreshTerminalPhaseProjection()
        case .uploadPaused:
            if phase == .uploading {
                phase = .uploadPaused
            }
            refreshTerminalPhaseProjection()
        case .uploadResumed:
            if phase == .uploadPaused {
                phase = .uploading
            }
            refreshTerminalPhaseProjection()
        case .uploadCompleted:
            workFacts.uploadFinished = true
            if phase == .uploading || phase == .pending {
                phase = needsDownload ? .uploadDone : .completed
            }
            refreshTerminalPhaseProjection(defaultWhenNoFacts: phase == .completed ? .completed : nil)
        case .downloadStarted:
            guard hasPendingDownloadWork else {
                refreshTerminalPhaseProjection(defaultWhenNoFacts: phase == .completed ? .completed : nil)
                break
            }
            workFacts.downloadStarted = true
            if phase == .uploadDone || phase == .pending || phase == .partiallyFailed {
                phase = .downloading
            }
        case .downloadPaused:
            if phase == .downloading {
                phase = .downloadPaused
            }
            if failureFacts.hasTerminalFailure {
                refreshTerminalPhaseProjection()
            }
        case .downloadResumed:
            if phase == .downloadPaused {
                phase = .downloading
            }
            if failureFacts.hasTerminalFailure {
                refreshTerminalPhaseProjection()
            }
        case .downloadCompleted:
            if phase == .downloading {
                phase = .completed
            }
            refreshTerminalPhaseProjection(defaultWhenNoFacts: phase == .completed ? .completed : nil)
        case .downloadAttemptFinished:
            guard workFacts.downloadStarted else { break }
            workFacts.downloadFinished = true
            refreshTerminalPhaseProjection(defaultWhenNoFacts: .completed)
        case .completed:
            refreshTerminalPhaseProjection(defaultWhenNoFacts: .completed)
        }
    }

    mutating func finalizeIfOpen() {
        workFacts.uploadFinished = true
        workFacts.downloadFinished = true
        guard !isTerminal else {
            refreshTerminalPhaseProjection()
            return
        }
        refreshTerminalPhaseProjection(defaultWhenNoFacts: .completed)
    }

    private mutating func refreshTerminalPhaseProjection(defaultWhenNoFacts: Phase? = nil) {
        if failureFacts.hasTerminalFailure {
            phase = .failed
        } else if failureFacts.hasNonFatalIssues {
            phase = .partiallyFailed
        } else if let defaultWhenNoFacts {
            phase = defaultWhenNoFacts
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
            guard plan.failureFacts.hasUserVisibleFailure else { return nil }
            return MonthFailureInfo(month: month, message: plan.failureFacts.displayMessage(for: month))
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

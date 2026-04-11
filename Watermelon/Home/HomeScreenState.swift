import Foundation

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

// MARK: - Execution State

struct MonthPlan {
    let needsUpload: Bool
    let needsDownload: Bool
    var phase: Phase = .pending

    enum Phase {
        case pending
        case uploading
        case uploadDone
        case downloading
        case completed
        case failed
    }

    var isFullyCompleted: Bool { phase == .completed }
    var isFailed: Bool { phase == .failed }
}

enum ExecutionPhase: Equatable {
    case uploading
    case uploadPaused
    case downloading
    case downloadPaused
    case completed
    case failed(String)
}

struct HomeExecutionState {
    let monthPlans: [LibraryMonthKey: MonthPlan]
    let activeMonths: Set<LibraryMonthKey>
    let phase: ExecutionPhase
    let processedCountByMonth: [LibraryMonthKey: Int]
    let assetCountByMonth: [LibraryMonthKey: Int]
    let uploadMonths: [LibraryMonthKey]
    let downloadMonths: [LibraryMonthKey]
    let syncMonths: [LibraryMonthKey]

    let executionMonths: Set<LibraryMonthKey>
    let completedMonths: Set<LibraryMonthKey>

    init(monthPlans: [LibraryMonthKey: MonthPlan],
         activeMonths: Set<LibraryMonthKey>,
         phase: ExecutionPhase,
         processedCountByMonth: [LibraryMonthKey: Int],
         assetCountByMonth: [LibraryMonthKey: Int],
         uploadMonths: [LibraryMonthKey],
         downloadMonths: [LibraryMonthKey],
         syncMonths: [LibraryMonthKey]) {
        self.monthPlans = monthPlans
        self.activeMonths = activeMonths
        self.phase = phase
        self.processedCountByMonth = processedCountByMonth
        self.assetCountByMonth = assetCountByMonth
        self.uploadMonths = uploadMonths
        self.downloadMonths = downloadMonths
        self.syncMonths = syncMonths
        self.executionMonths = Set(monthPlans.keys)
        self.completedMonths = Set(monthPlans.filter { $0.value.isFullyCompleted }.map(\.key))
        self.failedMonths = Set(monthPlans.filter { $0.value.isFailed }.map(\.key))
    }

    let failedMonths: Set<LibraryMonthKey>

    func progressPercent(for month: LibraryMonthKey, row: HomeMonthRow?, direction: HomeArrowDirection?, matchedCount: Int) -> Double? {
        guard let row, let direction else { return nil }

        let localCount = row.local?.assetCount ?? 0
        let remoteCount = row.remote?.assetCount ?? 0

        let basePercent: Double?
        switch direction {
        case .toRemote:
            basePercent = localCount > 0 ? Double(matchedCount) / Double(localCount) * 100 : nil
        case .toLocal:
            basePercent = remoteCount > 0 ? Double(matchedCount) / Double(remoteCount) * 100 : nil
        case .sync:
            let remoteOnly = max(0, remoteCount - matchedCount)
            let total = localCount + remoteOnly
            basePercent = total > 0 ? Double(matchedCount) / Double(total) * 100 : nil
        }

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

    func panelPhases() -> (backup: SelectionActionPanel.CategoryPhase?, download: SelectionActionPanel.CategoryPhase?, sync: SelectionActionPanel.CategoryPhase?) {
        let isPaused = (phase == .uploadPaused || phase == .downloadPaused)

        func categoryPhase(for months: [LibraryMonthKey]) -> SelectionActionPanel.CategoryPhase? {
            guard !months.isEmpty else { return nil }
            let monthSet = Set(months)
            let completed = monthSet.intersection(completedMonths).count
            let failed = monthSet.intersection(failedMonths).count
            let done = completed + failed
            let active = !monthSet.isDisjoint(with: activeMonths)
            if done == monthSet.count {
                if failed > 0 {
                    return .failed(completed: completed, failed: failed, total: monthSet.count)
                }
                return .completed(total: monthSet.count)
            } else if active {
                return .running(completed: done, total: monthSet.count)
            } else if isPaused {
                return .paused(completed: done, total: monthSet.count)
            } else if failed > 0 {
                return .failed(completed: completed, failed: failed, total: monthSet.count)
            } else if done > 0 {
                return .running(completed: done, total: monthSet.count)
            } else {
                return .pending(total: monthSet.count)
            }
        }

        return (categoryPhase(for: uploadMonths), categoryPhase(for: downloadMonths), categoryPhase(for: syncMonths))
    }

}

// MARK: - Change Kind

enum HomeChangeKind {
    case data(Set<LibraryMonthKey>)
    case selection
    case execution
    case connection
    case structural
}

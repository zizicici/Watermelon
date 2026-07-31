import Foundation

enum MacMonthExecutionPhase: Equatable, Sendable {
    case pending
    case uploading
    case uploadPaused
    case downloading
    case downloadPaused
    case completed
    case partiallyFailed
    case failed

    var isTerminal: Bool {
        self == .completed
            || self == .partiallyFailed
            || self == .failed
    }
}

struct MacMonthExecutionTracker: Sendable {
    private(set) var phases:
        [LibraryMonthKey: MacMonthExecutionPhase]
    private(set) var progress:
        [LibraryMonthKey: MacMonthExecutionProgress]
    private var monthsWithItemFailures:
        Set<LibraryMonthKey> = []
    private var processedUploadAssetIDs:
        [LibraryMonthKey: Set<String>] = [:]
    private let backupMonths: Set<LibraryMonthKey>
    private let complementMonths: Set<LibraryMonthKey>

    init(
        backupMonths: Set<LibraryMonthKey> = [],
        downloadMonths: Set<LibraryMonthKey> = [],
        complementMonths: Set<LibraryMonthKey> = [],
        localAssetIDsByMonth:
            [LibraryMonthKey: Set<String>] = [:]
    ) {
        self.backupMonths = backupMonths
        self.complementMonths = complementMonths
        let allMonths = backupMonths
            .union(downloadMonths)
            .union(complementMonths)
        phases = Dictionary(
            uniqueKeysWithValues: allMonths.map { ($0, .pending) }
        )
        progress = Dictionary(
            uniqueKeysWithValues: allMonths.map {
                (
                    $0,
                    MacMonthExecutionProgress(
                        uploadProcessedCount: 0,
                        uploadTotalCount:
                            backupMonths.contains($0)
                                ? localAssetIDsByMonth[$0]?.count ?? 0
                                : 0,
                        downloadFraction: nil
                    )
                )
            }
        )
    }

    init(plan: MacBackupExecutionPlan) {
        self.init(
            backupMonths: plan.backupMonths,
            downloadMonths: plan.downloadMonths,
            complementMonths: plan.complementMonths,
            localAssetIDsByMonth: plan.localAssetIDsByMonth
        )
    }

    mutating func apply(_ event: BackupEvent) {
        switch event {
        case .progress(let progress):
            if let itemEvent = progress.itemEvent {
                recordUploadProgress(itemEvent)
                if itemEvent.status == .failed {
                    recordItemFailure(itemEvent.month)
                }
            }
        case .monthChanged(let change):
            let month = LibraryMonthKey(
                year: change.year,
                month: change.month
            )
            switch change.action {
            case .started:
                beginUpload(month)
            case .completed:
                complete(month)
            case .uploadFailed(_, let failedItemCount):
                guard failedItemCount > 0 else { return }
                recordUploadCommitFailure(month)
            }
        case .log, .transferState, .writeBoundaryReached,
             .started, .finished:
            break
        }
    }

    mutating func beginUpload(_ month: LibraryMonthKey) {
        set(.uploading, for: month)
    }

    mutating func beginDownload(_ month: LibraryMonthKey) {
        set(.downloading, for: month)
        guard phases[month] == .downloading else { return }
        progress[month]?.downloadFraction = 0
    }

    mutating func recordDownloadTransfer(
        _ transfer: BackupTransferState,
        month: LibraryMonthKey
    ) {
        guard transfer.kind == .download,
              phases[month] == .downloading,
              transfer.totalAssets > 0 else {
            return
        }
        let resourceFraction = (
            Double(max(transfer.resourcePosition - 1, 0))
                + Double(min(max(transfer.resourceFraction, 0), 1))
        ) / Double(max(transfer.totalResources, 1))
        let fraction = (
            Double(max(transfer.assetPosition - 1, 0))
                + resourceFraction
        ) / Double(transfer.totalAssets)
        let displayFraction =
            (min(max(fraction, 0), 1) * 1_000).rounded(.down)
                / 1_000
        guard var monthProgress = progress[month] else { return }
        monthProgress.downloadFraction = max(
            monthProgress.downloadFraction ?? 0,
            displayFraction
        )
        progress[month] = monthProgress
    }

    mutating func recordItemFailure(
        _ month: LibraryMonthKey
    ) {
        guard phases[month]?.isTerminal == false else { return }
        monthsWithItemFailures.insert(month)
    }

    mutating func recordUploadCommitFailure(
        _ month: LibraryMonthKey
    ) {
        if complementMonths.contains(month) {
            fail(month)
        } else {
            complete(month, hasIssues: true)
        }
    }

    mutating func complete(
        _ month: LibraryMonthKey,
        hasIssues: Bool = false
    ) {
        if hasIssues {
            monthsWithItemFailures.insert(month)
        }
        set(
            monthsWithItemFailures.contains(month)
                ? .partiallyFailed
                : .completed,
            for: month
        )
    }

    mutating func fail(_ month: LibraryMonthKey) {
        set(.failed, for: month)
    }

    mutating func pause(_ stage: MacBackupExecutionStage) {
        switch stage {
        case .preflight:
            break
        case .upload:
            replace(.uploading, with: .uploadPaused)
        case .download:
            replace(.downloading, with: .downloadPaused)
        }
    }

    mutating func resume(_ stage: MacBackupExecutionStage) {
        switch stage {
        case .preflight:
            break
        case .upload:
            replace(.uploadPaused, with: .uploading)
        case .download:
            replace(.downloadPaused, with: .downloading)
        }
    }

    mutating func completeRemaining() {
        for month in Array(phases.keys)
        where phases[month]?.isTerminal == false {
            phases[month] = monthsWithItemFailures.contains(month)
                ? .partiallyFailed
                : .completed
        }
    }

    mutating func failRemaining() {
        for month in Array(phases.keys)
        where phases[month]?.isTerminal == false {
            phases[month] = .failed
        }
    }

    mutating func clear() {
        phases.removeAll()
        progress.removeAll()
        monthsWithItemFailures.removeAll()
        processedUploadAssetIDs.removeAll()
    }

    private mutating func set(
        _ phase: MacMonthExecutionPhase,
        for month: LibraryMonthKey
    ) {
        guard phases[month]?.isTerminal == false else { return }
        phases[month] = phase
    }

    private mutating func replace(
        _ current: MacMonthExecutionPhase,
        with replacement: MacMonthExecutionPhase
    ) {
        for month in Array(phases.keys)
        where phases[month] == current {
            phases[month] = replacement
        }
    }

    private mutating func recordUploadProgress(
        _ itemEvent: BackupItemEvent
    ) {
        let month = itemEvent.month
        guard backupMonths.contains(month),
              (progress[month]?.uploadTotalCount ?? 0) > 0 else {
            return
        }
        processedUploadAssetIDs[month, default: []].insert(
            itemEvent.assetLocalIdentifier
        )
        guard var monthProgress = progress[month] else { return }
        monthProgress.uploadProcessedCount = min(
            processedUploadAssetIDs[month]?.count ?? 0,
            monthProgress.uploadTotalCount
        )
        progress[month] = monthProgress
    }
}

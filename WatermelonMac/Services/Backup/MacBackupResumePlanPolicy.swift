enum MacBackupInterruptionDisposition: Equatable {
    case finish
    case pause
}

enum MacBackupRunFailureSource: Equatable {
    case taskCancellation
    case classifiedCancellation
    case recoverable
    case fatal
}

enum MacBackupRunFailureDisposition: Equatable {
    case complete
    case pause
    case cancel
    case fail
}

enum MacBackupRunFailurePolicy {
    static func disposition(
        intent: BackupTerminationIntent,
        source: MacBackupRunFailureSource,
        allOperationsCommitted: Bool
    ) -> MacBackupRunFailureDisposition {
        if source == .taskCancellation {
            switch intent {
            case .pause:
                return allOperationsCommitted ? .complete : .pause
            case .none, .stop:
                return .cancel
            }
        }

        switch intent {
        case .stop:
            return .cancel
        case .pause:
            return allOperationsCommitted ? .complete : .pause
        case .none:
            switch source {
            case .classifiedCancellation, .recoverable:
                return allOperationsCommitted ? .complete : .pause
            case .fatal:
                return .fail
            case .taskCancellation:
                return .cancel
            }
        }
    }
}

enum MacBackupResumePlanPolicy {
    static func remainingPlan(
        from plan: MacBackupExecutionPlan,
        completedUploadMonths: Set<LibraryMonthKey>,
        completedDownloadMonths: Set<LibraryMonthKey>
    ) -> MacBackupExecutionPlan {
        var backupMonths = plan.backupMonths.subtracting(
            completedUploadMonths
        )
        var downloadMonths = plan.downloadMonths.subtracting(
            completedDownloadMonths
        )
        var complementMonths = Set<LibraryMonthKey>()

        for month in plan.complementMonths {
            let uploadCompleted = completedUploadMonths.contains(month)
            let downloadCompleted = completedDownloadMonths.contains(month)
            switch (uploadCompleted, downloadCompleted) {
            case (false, false):
                complementMonths.insert(month)
            case (true, false):
                downloadMonths.insert(month)
            case (false, true):
                backupMonths.insert(month)
            case (true, true):
                break
            }
        }

        return MacBackupExecutionPlan(
            backupMonths: backupMonths,
            downloadMonths: downloadMonths,
            complementMonths: complementMonths,
            localAssetIDsByMonth: plan.localAssetIDsByMonth,
            monthGroupingTimeZone: plan.monthGroupingTimeZone,
            incompleteDownloadPolicy: plan.incompleteDownloadPolicy
        )
    }

    static func interruptionDisposition(
        for plan: MacBackupExecutionPlan,
        completedUploadMonths: Set<LibraryMonthKey>,
        completedDownloadMonths: Set<LibraryMonthKey>
    ) -> MacBackupInterruptionDisposition {
        remainingPlan(
            from: plan,
            completedUploadMonths: completedUploadMonths,
            completedDownloadMonths: completedDownloadMonths
        ).allMonths.isEmpty ? .finish : .pause
    }
}

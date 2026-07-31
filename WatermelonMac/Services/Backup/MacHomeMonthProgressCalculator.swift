import Foundation

struct MacMonthExecutionProgress: Equatable, Sendable {
    var uploadProcessedCount = 0
    var uploadTotalCount = 0
    var downloadFraction: Double?
}

enum MacHomeMonthProgressCalculator {
    static func percent(
        row: HomeMonthRow,
        intent: MonthIntent?,
        phase: MacMonthExecutionPhase?,
        executionProgress: MacMonthExecutionProgress?
    ) -> Double? {
        let matchedCount = row.local?.backedUpCount ?? 0
        let basePercent = HomeProgressCalculator.basePercent(
            row: row,
            intent: intent,
            matchedCount: matchedCount
        )

        if phase == .completed {
            return basePercent ?? 100
        }

        guard let intent, let executionProgress else {
            return basePercent
        }

        if case .backup = intent,
           executionProgress.uploadTotalCount > 0,
           executionProgress.uploadProcessedCount > 0 {
            let sessionPercent =
                Double(executionProgress.uploadProcessedCount)
                / Double(executionProgress.uploadTotalCount)
                * 100
            return clamped(max(sessionPercent, basePercent ?? 0))
        }

        guard let downloadFraction =
                executionProgress.downloadFraction,
              phase == .downloading
                || phase == .downloadPaused
                || phase == .partiallyFailed else {
            return basePercent
        }

        let localCount = row.local?.assetCount ?? 0
        let remoteCount = row.remote?.assetCount ?? 0
        let remoteOnly = max(0, remoteCount - matchedCount)
        let completedDownloads =
            Double(remoteOnly) * clampedFraction(downloadFraction)

        switch intent {
        case .backup:
            return basePercent
        case .download:
            guard remoteCount > 0 else { return basePercent }
            return clamped(
                (Double(matchedCount) + completedDownloads)
                    / Double(remoteCount)
                    * 100
            )
        case .complement:
            let total = localCount + remoteOnly
            guard total > 0 else { return basePercent }
            return clamped(
                (Double(localCount) + completedDownloads)
                    / Double(total)
                    * 100
            )
        }
    }

    private static func clamped(_ percent: Double) -> Double {
        min(max(percent, 0), 100)
    }

    private static func clampedFraction(
        _ fraction: Double
    ) -> Double {
        min(max(fraction, 0), 1)
    }
}

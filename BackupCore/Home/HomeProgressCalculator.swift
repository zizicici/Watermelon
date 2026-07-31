import Foundation

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
            return localCount > 0
                ? Double(matchedCount) / Double(localCount) * 100
                : nil
        case .download:
            return remoteCount > 0
                ? Double(matchedCount) / Double(remoteCount) * 100
                : nil
        case .complement:
            let remoteOnly = max(0, remoteCount - matchedCount)
            let total = localCount + remoteOnly
            return total > 0
                ? Double(matchedCount) / Double(total) * 100
                : nil
        }
    }
}

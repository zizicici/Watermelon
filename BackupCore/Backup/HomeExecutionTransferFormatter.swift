import Foundation

enum HomeExecutionTransferFormatter {
    static func speed(_ bytesPerSecond: Double?) -> String? {
        guard let bytesPerSecond,
              bytesPerSecond.isFinite else {
            return nil
        }
        guard let bytes = Int64(
            exactly: max(0, bytesPerSecond).rounded()
        ) else {
            return nil
        }
        let formatted = ByteCountFormatter.string(
            fromByteCount: bytes,
            countStyle: .file
        )
        return "\(formatted)/s"
    }

    static func remainingTime(
        _ seconds: TimeInterval?
    ) -> String? {
        guard let seconds, seconds.isFinite else { return nil }
        let formatter = DateComponentsFormatter()
        formatter.unitsStyle = .abbreviated
        formatter.maximumUnitCount = 2
        if seconds <= 0 {
            formatter.allowedUnits = [.second]
            return formatter.string(from: 0) ?? "0s"
        }
        let roundedSeconds = max(1, seconds.rounded(.up))
        if roundedSeconds >= 3600 {
            formatter.allowedUnits = [.hour, .minute]
        } else if roundedSeconds >= 60 {
            formatter.allowedUnits = [.minute, .second]
        } else {
            formatter.allowedUnits = [.second]
        }
        return formatter.string(from: roundedSeconds)
    }
}

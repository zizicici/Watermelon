import FirebaseCrashlytics
import Foundation
import MetricKit
import os.log

// Jetsam/watchdog kills never reach Crashlytics as crashes; MetricKit exit counts are the only field signal.
final class AppExitMetricsMonitor: NSObject, MXMetricManagerSubscriber {
    static let shared = AppExitMetricsMonitor()

    private static let defaultsKey = "diagnostics.appExitSummaries"
    private static let processedWindowsKey = "diagnostics.appExitProcessedWindows"
    private static let maxStoredSummaries = 8
    private static let maxProcessedWindows = 16
    private static let storeLock = NSLock()
    private static let log = Logger(subsystem: "com.zizicici.watermelon", category: "Diagnostics")

    func start() {
        MXMetricManager.shared.add(self)
    }

    func didReceive(_ payloads: [MXMetricPayload]) {
        // Ascending order keeps the stored summaries chronological.
        for payload in payloads.sorted(by: { $0.timeStampEnd < $1.timeStampEnd }) {
            guard let exits = payload.applicationExitMetrics else { continue }
            // MetricKit can redeliver windows, including out of order; process each at most once.
            let windowID = "\(payload.timeStampBegin.timeIntervalSince1970)-\(payload.timeStampEnd.timeIntervalSince1970)"
            guard Self.reserveWindow(windowID) else { continue }
            let summary = Self.summaryLine(payload: payload, exits: exits)
            Self.log.info("\(summary, privacy: .public)")
            Crashlytics.crashlytics().log(summary)
            Self.storeSummary(summary)

            let memoryKills = exits.foregroundExitData.cumulativeMemoryResourceLimitExitCount
                + exits.backgroundExitData.cumulativeMemoryResourceLimitExitCount
                + exits.backgroundExitData.cumulativeMemoryPressureExitCount
            if memoryKills > 0 {
                // Issues group by domain+code, so the code must stay fixed; the summary rides in userInfo.
                Crashlytics.crashlytics().record(error: NSError(
                    domain: "AppExitMetrics",
                    code: 1,
                    userInfo: [NSLocalizedDescriptionKey: summary]
                ))
            }
            Self.markWindowProcessed(windowID)
        }
    }

    // Injected into the next execution session log, then cleared — one jetsam report lands in exactly
    // one session instead of echoing through every later log.
    static func consumeSummaryLines() -> [String] {
        storeLock.lock()
        defer { storeLock.unlock() }
        let lines = UserDefaults.standard.stringArray(forKey: defaultsKey) ?? []
        if !lines.isEmpty {
            UserDefaults.standard.removeObject(forKey: defaultsKey)
        }
        return lines
    }

    // In-memory only: a crash between reserve and mark leaves the window unprocessed for redelivery.
    private static var inFlightWindows = Set<String>()

    private static func reserveWindow(_ windowID: String) -> Bool {
        storeLock.lock()
        defer { storeLock.unlock() }
        let processed = UserDefaults.standard.stringArray(forKey: processedWindowsKey) ?? []
        guard !processed.contains(windowID), !inFlightWindows.contains(windowID) else { return false }
        inFlightWindows.insert(windowID)
        return true
    }

    private static func markWindowProcessed(_ windowID: String) {
        storeLock.lock()
        defer { storeLock.unlock() }
        inFlightWindows.remove(windowID)
        var ids = UserDefaults.standard.stringArray(forKey: processedWindowsKey) ?? []
        ids.append(windowID)
        if ids.count > maxProcessedWindows {
            ids.removeFirst(ids.count - maxProcessedWindows)
        }
        UserDefaults.standard.set(ids, forKey: processedWindowsKey)
    }

    private static func storeSummary(_ summary: String) {
        storeLock.lock()
        defer { storeLock.unlock() }
        var lines = UserDefaults.standard.stringArray(forKey: defaultsKey) ?? []
        lines.append(summary)
        if lines.count > maxStoredSummaries {
            lines.removeFirst(lines.count - maxStoredSummaries)
        }
        UserDefaults.standard.set(lines, forKey: defaultsKey)
    }

    private static func summaryLine(payload: MXMetricPayload, exits: MXAppExitMetric) -> String {
        let window = windowFormatter.string(from: payload.timeStampBegin)
            + "~" + windowFormatter.string(from: payload.timeStampEnd)
        let fg = exits.foregroundExitData
        let bg = exits.backgroundExitData
        let fgTokens = tokens([
            ("normal", fg.cumulativeNormalAppExitCount),
            ("memory", fg.cumulativeMemoryResourceLimitExitCount),
            ("watchdog", fg.cumulativeAppWatchdogExitCount),
            ("abnormal", fg.cumulativeAbnormalExitCount),
            ("badAccess", fg.cumulativeBadAccessExitCount),
            ("illegalInstruction", fg.cumulativeIllegalInstructionExitCount)
        ])
        let bgTokens = tokens([
            ("normal", bg.cumulativeNormalAppExitCount),
            ("memory", bg.cumulativeMemoryResourceLimitExitCount),
            ("memoryPressure", bg.cumulativeMemoryPressureExitCount),
            ("watchdog", bg.cumulativeAppWatchdogExitCount),
            ("abnormal", bg.cumulativeAbnormalExitCount),
            ("badAccess", bg.cumulativeBadAccessExitCount),
            ("illegalInstruction", bg.cumulativeIllegalInstructionExitCount),
            ("cpuLimit", bg.cumulativeCPUResourceLimitExitCount),
            ("taskTimeout", bg.cumulativeBackgroundTaskAssertionTimeoutExitCount),
            ("lockedFile", bg.cumulativeSuspendedWithLockedFileExitCount)
        ])
        return String(
            format: String(localized: "diagnostics.log.appExitSummary"),
            window,
            fgTokens,
            bgTokens
        )
    }

    private static func tokens(_ pairs: [(String, Int)]) -> String {
        let nonzero = pairs.filter { $0.1 > 0 }.map { "\($0.0)=\($0.1)" }
        return nonzero.isEmpty ? "-" : nonzero.joined(separator: " ")
    }

    private static let windowFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }()
}

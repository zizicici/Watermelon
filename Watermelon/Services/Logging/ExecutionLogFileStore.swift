import Foundation

enum ExecutionLogKind: String, CaseIterable, Sendable {
    case manual
    case auto

    var directoryName: String { rawValue }
}

enum ExecutionLogFileStore {
    static let retentionInterval: TimeInterval = 14 * 24 * 60 * 60

    static var rootDirectory: URL {
        let caches = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0]
        return caches.appendingPathComponent("ExecutionLogs", isDirectory: true)
    }

    static func directory(for kind: ExecutionLogKind) -> URL {
        rootDirectory.appendingPathComponent(kind.directoryName, isDirectory: true)
    }

    static func beginSession(kind: ExecutionLogKind, startedAt: Date = Date()) -> ExecutionLogSessionWriter {
        let dir = directory(for: kind)
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        try? FileProtection.enableBackgroundAccess(at: dir)
        let name = Self.fileNameFormatter.string(from: startedAt) + ".log"
        let url = dir.appendingPathComponent(name)
        return ExecutionLogSessionWriter(fileURL: url, kind: kind, startedAt: startedAt)
    }

    static func prepareForBackgroundUse() {
        try? FileManager.default.createDirectory(at: rootDirectory, withIntermediateDirectories: true)
        try? FileProtection.enableBackgroundAccess(at: rootDirectory)
        for kind in ExecutionLogKind.allCases {
            let dir = directory(for: kind)
            try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
            try? FileProtection.enableBackgroundAccess(at: dir)
        }
    }

    static func listSessions() -> [ExecutionLogSessionInfo] {
        var results: [ExecutionLogSessionInfo] = []
        for kind in ExecutionLogKind.allCases {
            let dir = directory(for: kind)
            guard let entries = try? FileManager.default.contentsOfDirectory(
                at: dir,
                includingPropertiesForKeys: [.fileSizeKey, .contentModificationDateKey, .creationDateKey],
                options: [.skipsHiddenFiles]
            ) else { continue }
            for url in entries where url.pathExtension == "log" {
                if let info = ExecutionLogSessionInfo.parse(url: url, kind: kind) {
                    results.append(info)
                }
            }
        }
        results.sort { $0.startedAt > $1.startedAt }
        return results
    }

    static func purgeExpired(now: Date = Date()) {
        let cutoff = now.addingTimeInterval(-retentionInterval)
        for kind in ExecutionLogKind.allCases {
            let dir = directory(for: kind)
            guard let entries = try? FileManager.default.contentsOfDirectory(
                at: dir,
                includingPropertiesForKeys: [.contentModificationDateKey],
                options: [.skipsHiddenFiles]
            ) else { continue }
            for url in entries where url.pathExtension == "log" {
                let attrs = try? FileManager.default.attributesOfItem(atPath: url.path)
                let mtime = (attrs?[.modificationDate] as? Date) ?? (attrs?[.creationDate] as? Date) ?? .distantPast
                if mtime < cutoff {
                    try? FileManager.default.removeItem(at: url)
                }
            }
        }
    }

    static let fileNameFormatter: DateFormatter = {
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        f.dateFormat = "yyyyMMdd'T'HHmmss'Z'"
        return f
    }()

    static let lineTimestampFormatter: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()
}

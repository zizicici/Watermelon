import Foundation

actor ExecutionLogSessionWriter {
    nonisolated let fileURL: URL
    nonisolated let kind: ExecutionLogKind
    nonisolated let startedAt: Date

    private var handle: FileHandle?
    private var pendingBytes: Int = 0
    private var closed = false

    private static let flushThresholdBytes = 16 * 1024

    init(fileURL: URL, kind: ExecutionLogKind, startedAt: Date) {
        self.fileURL = fileURL
        self.kind = kind
        self.startedAt = startedAt
    }

    func appendLog(_ message: String, level: ExecutionLogLevel, at date: Date = Date()) {
        guard !closed else { return }
        let line = Self.format(message: message, level: level, date: date)
        guard let data = line.data(using: .utf8) else { return }

        do {
            let handle = try ensureHandle()
            try handle.write(contentsOf: data)
            pendingBytes += data.count
            if pendingBytes >= Self.flushThresholdBytes {
                try handle.synchronize()
                pendingBytes = 0
            }
        } catch {
            closed = true
            try? handle?.close()
            handle = nil
        }
    }

    func finalize() {
        guard !closed else { return }
        closed = true
        try? handle?.synchronize()
        try? handle?.close()
        handle = nil
    }

    private func ensureHandle() throws -> FileHandle {
        if let handle { return handle }
        let dir = fileURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        if !FileManager.default.fileExists(atPath: fileURL.path) {
            FileManager.default.createFile(atPath: fileURL.path, contents: nil)
        }
        let h = try FileHandle(forWritingTo: fileURL)
        try h.seekToEnd()
        handle = h
        return h
    }

    private static func format(message: String, level: ExecutionLogLevel, date: Date) -> String {
        let timestamp = ExecutionLogFileStore.lineTimestampFormatter.string(from: date)
        let paddedTag = ExecutionLogPalette.tag(for: level).padding(toLength: 5, withPad: " ", startingAt: 0)
        let sanitized = Self.sanitize(message)
        return "\(timestamp) [\(paddedTag)] \(sanitized)\n"
    }

    private static func sanitize(_ message: String) -> String {
        guard message.contains(where: { $0 == "\n" || $0 == "\r" }) else { return message }
        return message
            .replacingOccurrences(of: "\r\n", with: " ")
            .replacingOccurrences(of: "\n", with: " ")
            .replacingOccurrences(of: "\r", with: " ")
    }

    static func level(forTag tag: String) -> ExecutionLogLevel? {
        switch tag.trimmingCharacters(in: .whitespaces) {
        case "DEBUG": return .debug
        case "INFO":  return .info
        case "WARN":  return .warning
        case "ERROR": return .error
        default: return nil
        }
    }
}

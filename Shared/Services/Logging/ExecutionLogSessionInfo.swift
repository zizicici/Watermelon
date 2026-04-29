import Foundation

struct ExecutionLogSessionInfo: Hashable, Sendable {
    let url: URL
    let kind: ExecutionLogKind
    let startedAt: Date
    let sizeBytes: Int64

    static func parse(url: URL, kind: ExecutionLogKind) -> ExecutionLogSessionInfo? {
        let base = url.deletingPathExtension().lastPathComponent
        guard let startedAt = ExecutionLogFileStore.fileNameFormatter.date(from: base) else {
            return nil
        }
        let attrs = try? FileManager.default.attributesOfItem(atPath: url.path)
        let size = (attrs?[.size] as? NSNumber)?.int64Value ?? 0
        return ExecutionLogSessionInfo(url: url, kind: kind, startedAt: startedAt, sizeBytes: size)
    }

    func readEntries() throws -> [ExecutionLogEntry] {
        let text = try String(contentsOf: url, encoding: .utf8)
        var entries: [ExecutionLogEntry] = []
        text.enumerateLines { line, _ in
            guard !line.isEmpty else { return }
            if let parsed = Self.parseLine(line) {
                entries.append(parsed)
            } else if let last = entries.popLast() {
                entries.append(ExecutionLogEntry(
                    timestamp: last.timestamp,
                    message: last.message + "\n" + line,
                    level: last.level
                ))
            }
        }
        return entries
    }

    private static func parseLine(_ line: String) -> ExecutionLogEntry? {
        guard let firstSpace = line.firstIndex(of: " ") else { return nil }
        let tsPart = String(line[..<firstSpace])
        let rest = line[line.index(after: firstSpace)...]
        guard let date = ExecutionLogFileStore.lineTimestampFormatter.date(from: tsPart) else {
            return nil
        }
        guard rest.first == "[", let closing = rest.firstIndex(of: "]") else { return nil }
        let tagSubstring = rest[rest.index(after: rest.startIndex)..<closing]
        guard let level = ExecutionLogSessionWriter.level(forTag: String(tagSubstring)) else { return nil }
        let afterClose = rest.index(after: closing)
        guard afterClose < rest.endIndex else { return nil }
        let message = String(rest[rest.index(after: afterClose)...])
        return ExecutionLogEntry(timestamp: date, message: message, level: level)
    }
}

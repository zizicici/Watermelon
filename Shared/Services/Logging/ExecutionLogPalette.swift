import Foundation

enum ExecutionLogPalette {
    static func tag(for level: ExecutionLogLevel) -> String {
        switch level {
        case .debug: return "DEBUG"
        case .info:  return "INFO"
        case .warning: return "WARN"
        case .error: return "ERROR"
        }
    }

    static let timestampFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm:ss"
        return f
    }()
}

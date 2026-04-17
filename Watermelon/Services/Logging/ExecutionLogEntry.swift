import Foundation

struct ExecutionLogEntry {
    let id = UUID()
    let timestamp: Date
    let message: String
    let level: ExecutionLogLevel
}

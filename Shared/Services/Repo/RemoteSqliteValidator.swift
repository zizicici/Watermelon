import Foundation
import GRDB

// Shared SQLite soundness check for remote-derived manifests: opens a GRDB queue and runs
// `PRAGMA quick_check`, treating `["ok"]` as the only pass.
enum RemoteSqliteValidator {
    // Any open/read failure or non-"ok" result reads as unsound (false).
    static func passesQuickCheck(at url: URL) -> Bool {
        ((try? quickCheckResults(at: url)) ?? []) == ["ok"]
    }

    // Raw `PRAGMA quick_check` rows; throws on open/read failure so callers can surface integrity faults.
    static func quickCheckResults(at url: URL) throws -> [String] {
        let queue = try DatabaseQueue(path: url.path)
        defer { try? queue.close() }
        return try queue.read { try String.fetchAll($0, sql: "PRAGMA quick_check") }
    }
}

import Foundation
import GRDB

/// SHA-256 → 8-byte perceptual hash. Shared across profiles since identical bytes always produce
/// the same dHash. Cleared via app menu.
final class PerceptualHashCache {
    static let shared = PerceptualHashCache()

    private let dbQueue: DatabaseQueue?
    private let dbURL: URL?

    private init() {
        let watermelonDir = DatabaseManager.defaultDatabaseURL().deletingLastPathComponent()
        try? FileManager.default.createDirectory(at: watermelonDir, withIntermediateDirectories: true)
        let url = watermelonDir.appendingPathComponent("perceptual-cache.sqlite")

        var configuration = Configuration()
        configuration.busyMode = .timeout(2.0)

        do {
            let queue = try DatabaseQueue(path: url.path, configuration: configuration)
            try queue.write { db in
                try db.execute(sql: """
                    CREATE TABLE IF NOT EXISTS dhash_cache (
                        content_sha256 BLOB PRIMARY KEY NOT NULL,
                        dhash BLOB NOT NULL,
                        computed_at INTEGER NOT NULL
                    )
                """)
            }
            self.dbQueue = queue
            self.dbURL = url
        } catch {
            self.dbQueue = nil
            self.dbURL = url
        }
    }

    func lookup(contentHash: Data) -> Data? {
        guard let dbQueue else { return nil }
        return try? dbQueue.read { db in
            try Data.fetchOne(
                db,
                sql: "SELECT dhash FROM dhash_cache WHERE content_sha256 = ?",
                arguments: [contentHash]
            )
        }
    }

    /// Single-query bulk lookup. Use when iterating many resources at once (e.g. a target
    /// manifest's full image list) to avoid N round-trips through the SQL engine.
    func lookupAll(contentHashes: [Data]) -> [Data: Data] {
        guard let dbQueue, !contentHashes.isEmpty else { return [:] }
        let chunkSize = 500
        var result: [Data: Data] = [:]
        var i = 0
        while i < contentHashes.count {
            let end = min(i + chunkSize, contentHashes.count)
            let chunk = Array(contentHashes[i..<end])
            i = end
            let placeholders = Array(repeating: "?", count: chunk.count).joined(separator: ",")
            let rows = (try? dbQueue.read { db in
                try Row.fetchAll(
                    db,
                    sql: "SELECT content_sha256, dhash FROM dhash_cache WHERE content_sha256 IN (\(placeholders))",
                    arguments: StatementArguments(chunk)
                )
            }) ?? []
            for row in rows {
                let sha: Data = row["content_sha256"]
                let dhash: Data = row["dhash"]
                result[sha] = dhash
            }
        }
        return result
    }

    func store(contentHash: Data, dhash: Data) {
        guard let dbQueue else { return }
        let now = Int64(Date().timeIntervalSince1970 * 1_000)
        try? dbQueue.write { db in
            try db.execute(
                sql: """
                INSERT INTO dhash_cache (content_sha256, dhash, computed_at) VALUES (?, ?, ?)
                ON CONFLICT(content_sha256) DO UPDATE SET dhash = excluded.dhash, computed_at = excluded.computed_at
                """,
                arguments: [contentHash, dhash, now]
            )
        }
    }

    func count() -> Int {
        guard let dbQueue else { return 0 }
        return (try? dbQueue.read { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM dhash_cache") ?? 0
        }) ?? 0
    }

    func dbSize() -> Int64 {
        guard let dbURL else { return 0 }
        let fm = FileManager.default
        var total: Int64 = 0
        for suffix in ["", "-wal", "-shm"] {
            let path = dbURL.path + suffix
            if let attrs = try? fm.attributesOfItem(atPath: path),
               let size = attrs[.size] as? Int64 {
                total += size
            }
        }
        return total
    }

    @discardableResult
    func clearAll() -> Bool {
        guard let dbQueue else { return false }
        do {
            try dbQueue.write { db in
                try db.execute(sql: "DELETE FROM dhash_cache")
            }
            try? dbQueue.write { db in
                try db.execute(sql: "VACUUM")
            }
            return true
        } catch {
            return false
        }
    }
}

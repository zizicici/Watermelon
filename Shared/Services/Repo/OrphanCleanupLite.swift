import Foundation

// Scoped Lite maintenance cleanup. Deletes only a fixed whitelist of stale *metadata* siblings:
// month-manifest temp/backup scratch, relocated V1 manifests, and (foreground only) expired write
// locks. Photo/resource bytes under <YYYY>/<MM>/<filename> are never touched and there is intentionally
// no data-byte garbage collection. Best-effort: a missing directory or any transport fault is swallowed
// so cleanup can never change the caller's outcome.
struct OrphanCleanupLite {
    enum Mode: Sendable {
        case foreground
        case background   // never deletes locks
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let lockExpiry: TimeInterval

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        lockExpiry: TimeInterval = WriteLockService.expiry
    ) {
        self.client = client
        self.basePath = basePath
        self.lockExpiry = lockExpiry
    }

    @discardableResult
    func run(mode: Mode, now: Date = Date()) async -> [String] {
        var deleted: [String] = []
        deleted += await cleanMonthsScratch()
        deleted += await cleanRelocatedV1Manifests()
        if mode == .foreground {
            deleted += await cleanExpiredLocks(now: now)
        }
        return deleted
    }

    // MARK: - .watermelon/months/*.tmp and *.bak

    private func cleanMonthsScratch() async -> [String] {
        var deleted: [String] = []
        for entry in await listChildren(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
        where !entry.isDirectory && Self.isScratch(entry.name) {
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    // MARK: - Old V1 manifests at <YYYY>/<MM>/.watermelon_manifest.sqlite

    private func cleanRelocatedV1Manifests() async -> [String] {
        let normalizedBase = RemotePathBuilder.normalizePath(basePath)
        var deleted: [String] = []
        let years = (await listChildren(normalizedBase)).filter {
            $0.isDirectory && Self.parseYear($0.name) != nil
        }
        for year in years {
            let months = (await listChildren(year.path)).filter {
                $0.isDirectory && Self.parseMonth($0.name) != nil
            }
            for month in months {
                for child in await listChildren(month.path)
                where !child.isDirectory && child.name == MonthManifestStore.manifestFileName {
                    if await deleteWhitelisted(child.path) { deleted.append(child.path) }
                }
            }
        }
        return deleted
    }

    // MARK: - Foreground: expired .watermelon/locks/*.lock

    private func cleanExpiredLocks(now: Date) async -> [String] {
        let suffix = ".\(RepoLayoutLite.lockFileExtension)"
        var deleted: [String] = []
        for entry in await listChildren(RepoLayoutLite.locksDirectoryPath(basePath: basePath))
        where !entry.isDirectory && entry.name.hasSuffix(suffix) {
            guard isExpired(entry.modificationDate, now: now) else { continue }
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    // A nil (unknown) mtime is never treated as expired — it could still be fresh.
    private func isExpired(_ modificationDate: Date?, now: Date) -> Bool {
        guard let modificationDate else { return false }
        return now.timeIntervalSince(modificationDate) > lockExpiry
    }

    // MARK: - Primitives

    private func listChildren(_ path: String) async -> [RemoteStorageEntry] {
        (try? await client.list(path: path)) ?? []
    }

    // notFound at delete time is success-equivalent; any other fault is swallowed (best-effort).
    private func deleteWhitelisted(_ path: String) async -> Bool {
        do {
            try await client.delete(path: path)
            return true
        } catch {
            return false
        }
    }

    private static func isScratch(_ name: String) -> Bool {
        name.hasSuffix(".tmp") || name.hasSuffix(".bak")
    }

    private static func parseYear(_ value: String) -> Int? {
        guard value.count == 4, let number = Int(value), number >= 1900 else { return nil }
        return number
    }

    private static func parseMonth(_ value: String) -> Int? {
        guard value.count == 2, let number = Int(value), (1 ... 12).contains(number) else { return nil }
        return number
    }
}

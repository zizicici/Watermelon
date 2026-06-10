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
    // The foreground writer running this cleanup; its own active lock is never a deletion candidate.
    private let currentWriterID: String?

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        currentWriterID: String? = nil,
        lockExpiry: TimeInterval = WriteLockService.expiry
    ) {
        self.client = client
        self.basePath = basePath
        self.currentWriterID = currentWriterID
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

    // Excludes the current writer's own lock and second-confirms every foreign expired lock (same body,
    // unchanged mtime, still expired across two reads) before deleting it, so a just-refreshed foreign
    // lock observed as expired in the first LIST is never removed.
    private func cleanExpiredLocks(now: Date) async -> [String] {
        let suffix = ".\(RepoLayoutLite.lockFileExtension)"
        let ownFilename = currentWriterID.flatMap { RepoLayoutLite.lockFilename(writerID: $0) }
        var deleted: [String] = []
        for entry in await listChildren(RepoLayoutLite.locksDirectoryPath(basePath: basePath))
        where !entry.isDirectory && entry.name.hasSuffix(suffix) {
            if let ownFilename, entry.name == ownFilename { continue }
            guard isExpired(entry.modificationDate, now: now) else { continue }
            guard await confirmForeignExpiredUnchanged(path: entry.path, snapshotDate: entry.modificationDate, now: now) else { continue }
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    // Best-effort: any unreadable/absent read, an empty/partial/legacy/undecodable body (no token proof),
    // a freshened mtime, or a changed body returns false so the lock is left intact.
    private func confirmForeignExpiredUnchanged(path: String, snapshotDate: Date?, now: Date) async -> Bool {
        guard case .present(let rawBody1, let mtime1) = await RemoteLockReader.read(client: client, path: path),
              let body1 = rawBody1, isExpired(mtime1, now: now) else { return false }
        if let snapshotDate, let mtime1, mtime1 != snapshotDate { return false }
        guard case .present(let rawBody2, let mtime2) = await RemoteLockReader.read(client: client, path: path),
              let body2 = rawBody2, isExpired(mtime2, now: now) else { return false }
        return body1 == body2 && mtime1 == mtime2
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

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

    // MARK: - .watermelon/months/*.tmp and *.bak (repair-first)

    // Repair-first: a crash can leave a month's only surviving manifest in scratch (an unflushed `.tmp`
    // upload, or a `.bak` backed up mid-rename). Restore a sound candidate to its canonical month before
    // deleting, so cleanup never destroys the sole recoverable copy.
    private func cleanMonthsScratch() async -> [String] {
        let entries = (await listChildren(RepoLayoutLite.monthsDirectoryPath(basePath: basePath)))
            .filter { !$0.isDirectory }

        // Canonical month sqlites already present — their leftover scratch is safe to drop.
        var canonicalMonths: Set<LibraryMonthKey> = []
        for entry in entries where !Self.isScratch(entry.name) {
            if let month = RepoLayoutLite.month(fromFilename: entry.name) {
                canonicalMonths.insert(month)
            }
        }

        // Bucket scratch by the canonical month it claims; final-derived names parse, legacy/opaque don't.
        var scratchByMonth: [LibraryMonthKey: [RemoteStorageEntry]] = [:]
        var unparseableScratch: [RemoteStorageEntry] = []
        for entry in entries where Self.isScratch(entry.name) {
            if let month = RepoLayoutLite.month(fromScratchFilename: entry.name) {
                scratchByMonth[month, default: []].append(entry)
            } else {
                unparseableScratch.append(entry)
            }
        }

        var deleted: [String] = []
        deleted += await cleanUnparseableScratch(unparseableScratch)
        for (month, scratch) in scratchByMonth {
            deleted += await cleanMonthScratch(
                month: month, scratch: scratch, canonicalPresent: canonicalMonths.contains(month)
            )
        }
        return deleted
    }

    // Outcome of validating a scratch candidate. `.inconclusive` (a download/read fault) is NOT proof of
    // corruption: it must never license deletion, or a transient blink could destroy the only recovery copy.
    private enum ScratchValidation {
        case valid          // downloaded and proven a sound month manifest
        case invalid        // downloaded, but proven not a sound month manifest
        case inconclusive   // could not be read/validated (download or read fault); recoverability unknown
    }

    // Opaque scratch (no parseable target, e.g. legacy "manifest_<uuid>.tmp"): delete only when its bytes
    // are *proven* unsound. Sound bytes of unknown month, and fault-inconclusive bytes, are left in place —
    // either may be the only recoverable copy.
    private func cleanUnparseableScratch(_ entries: [RemoteStorageEntry]) async -> [String] {
        var deleted: [String] = []
        for entry in entries {
            guard case .invalid = await validateMonthManifest(entry.path) else { continue }
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    // One month's scratch. Canonical present → leftover scratch is droppable. Canonical absent → restore
    // from exactly one sound candidate (then drop the proven-unsound rest); zero sound candidates → drop
    // only the proven-unsound junk; ambiguous (≥2 sound) → leave everything. A candidate whose validation
    // is inconclusive (read fault) blocks confidence for the whole month: restore nothing and delete
    // nothing this pass, so a later (fault-cleared) pass can resolve it without destroying recovery material.
    private func cleanMonthScratch(
        month: LibraryMonthKey,
        scratch: [RemoteStorageEntry],
        canonicalPresent: Bool
    ) async -> [String] {
        if canonicalPresent {
            var deleted: [String] = []
            for entry in scratch {
                if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
            }
            return deleted
        }

        var valid: [RemoteStorageEntry] = []
        var invalid: [RemoteStorageEntry] = []
        var anyInconclusive = false
        for entry in scratch {
            switch await validateMonthManifest(entry.path) {
            case .valid: valid.append(entry)
            case .invalid: invalid.append(entry)
            case .inconclusive: anyInconclusive = true
            }
        }

        // Any unread candidate could be recovery material → leave the whole month untouched this pass.
        if anyInconclusive { return [] }

        if valid.count == 1 {
            // Exactly one sound candidate, all others proven unsound: publish it, then drop the junk.
            let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
            guard await restoreCanonical(from: valid[0].path, to: canonicalPath) else { return [] }
            var deleted: [String] = []
            for entry in invalid {
                if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
            }
            return deleted
        }

        // Ambiguous (≥2 sound) → leave all. No sound candidate (only proven junk) → drop the junk.
        guard valid.isEmpty else { return [] }
        var deleted: [String] = []
        for entry in invalid {
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    // Downloads a remote scratch sqlite and classifies it. A `client.download` fault is inconclusive (the
    // bytes were never obtained, so soundness is unknown); a successful download is then proven sound/unsound.
    private func validateMonthManifest(_ remotePath: String) async -> ScratchValidation {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("orphan-validate-\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: localURL) }
        do {
            try await client.download(remotePath: remotePath, localURL: localURL)
        } catch {
            return .inconclusive
        }
        return MonthManifestStore.isValidMonthManifestFile(at: localURL) ? .valid : .invalid
    }

    // Publishes a validated scratch sqlite to its canonical month path. Best-effort: a move fault leaves
    // the scratch in place (returns false) so nothing recoverable is lost.
    private func restoreCanonical(from scratchPath: String, to canonicalPath: String) async -> Bool {
        do {
            try await client.move(from: scratchPath, to: canonicalPath)
            return true
        } catch {
            return false
        }
    }

    // MARK: - Old V1 manifests at <YYYY>/<MM>/.watermelon_manifest.sqlite

    // Once a full pass has cleared every relocated V1 manifest, a `.watermelon` completion marker lets
    // steady-state cleanup skip the whole base/year/month tree scan. The marker is written only after a
    // clean pass: a swallowed scan fault, or any unresolved non-notFound delete fault, leaves it unwritten
    // so a later (fault-cleared) pass retries. The caller runs this only on a Lite-current, owned path, so
    // the marker is never written before the repo is safely current and owned.
    private func cleanRelocatedV1Manifests() async -> [String] {
        if await v1CleanupMarkerPresent() { return [] }

        let manifests: [V1ManifestScanner.Manifest]
        do {
            manifests = try await V1ManifestScanner(client: client, basePath: basePath).scan()
        } catch {
            return []   // best-effort: swallow the scan fault, but do not authorize the marker
        }

        var deleted: [String] = []
        var allResolved = true
        for manifest in manifests {
            switch await classifiedDelete(manifest.manifestPath) {
            case .deleted: deleted.append(manifest.manifestPath)
            case .absent: break   // notFound at delete time is success-equivalent (confirmed absent)
            case .faulted: allResolved = false   // unresolved non-notFound delete fault → no marker
            }
        }

        if allResolved { await writeV1CleanupMarker() }
        return deleted
    }

    private func v1CleanupMarkerPresent() async -> Bool {
        ((try? await client.exists(path: RepoLayoutLite.v1CleanupMarkerPath(basePath: basePath))) ?? false)
    }

    // Best-effort marker write: a failure just means the next pass re-scans (and may re-mark). The marker
    // is presence-only, so even a truncated write still correctly signals "already cleaned once".
    private func writeV1CleanupMarker() async {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("v1cleanup-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: localURL) }
        guard (try? Self.v1CleanupMarkerBody.write(to: localURL)) != nil else { return }
        do {
            try await client.createDirectory(path: RepoLayoutLite.repoDirectoryPath(basePath: basePath))
            try await client.upload(
                localURL: localURL,
                remotePath: RepoLayoutLite.v1CleanupMarkerPath(basePath: basePath),
                respectTaskCancellation: false,
                onProgress: nil
            )
        } catch {
            // Swallowed: marker write is an optimization, never required for correctness.
        }
    }

    private static let v1CleanupMarkerBody = Data(#"{"v1ManifestsCleared":true}"#.utf8)

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

    // Three-way delete outcome so the V1 cleanup pass can tell "confirmed absent" (success-equivalent) from
    // an unresolved transport fault that must block the completion marker.
    private enum DeleteOutcome {
        case deleted   // delete succeeded
        case absent    // notFound at delete time — success-equivalent
        case faulted   // any other (non-notFound) fault
    }

    private func classifiedDelete(_ path: String) async -> DeleteOutcome {
        do {
            try await client.delete(path: path)
            return .deleted
        } catch {
            return RemoteFaultLite.classify(error) == .notFound ? .absent : .faulted
        }
    }

    // notFound at delete time is success-equivalent; any other fault is swallowed (best-effort).
    private func deleteWhitelisted(_ path: String) async -> Bool {
        await classifiedDelete(path) == .deleted
    }

    private static func isScratch(_ name: String) -> Bool {
        name.hasSuffix(".tmp") || name.hasSuffix(".bak")
    }
}

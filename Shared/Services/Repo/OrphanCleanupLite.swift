import Foundation

// Scoped Lite maintenance cleanup. Deletes only a fixed whitelist of stale *metadata* siblings:
// month-manifest temp/backup scratch and (foreground only) expired write locks. Photo/resource bytes under
// <YYYY>/<MM>/<filename> are never touched. Best-effort: a missing directory or any transport fault is
// swallowed so cleanup can never change the caller's outcome.
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
    private let ownershipGate: CleanupOwnershipGate
    private let monthsListing: LiteMonthsListingSnapshot?

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        currentWriterID: String? = nil,
        lockExpiry: TimeInterval = WriteLockService.expiry,
        assertOwnership: MonthManifestOwnershipAssertion? = nil,
        assertLeaseConfidence: MonthManifestOwnershipAssertion? = nil,
        monthsListing: LiteMonthsListingSnapshot? = nil
    ) {
        self.client = client
        self.basePath = basePath
        self.currentWriterID = currentWriterID
        self.lockExpiry = lockExpiry
        self.ownershipGate = CleanupOwnershipGate(
            assertOwnership: assertOwnership,
            assertLeaseConfidence: assertLeaseConfidence
        )
        self.monthsListing = monthsListing
    }

    @discardableResult
    func run(mode: Mode, now: Date = Date()) async -> [String] {
        var deleted: [String] = []
        if mode == .foreground {
            deleted += await cleanVersionScratch()
        }
        deleted += await cleanMonthsScratch()
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
        let entries = (await listMonthsChildren()).filter { !$0.isDirectory }

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
        var migrationScratch: [RemoteStorageEntry] = []
        for entry in entries where Self.isScratch(entry.name) {
            if RepoLayoutLite.isMigrationPublishScratch(entry.name) {
                migrationScratch.append(entry)
            } else if let month = RepoLayoutLite.month(fromScratchFilename: entry.name) {
                scratchByMonth[month, default: []].append(entry)
            } else {
                unparseableScratch.append(entry)
            }
        }

        var deleted: [String] = []
        deleted += await cleanMigrationPublishScratch(migrationScratch)
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

    // A stranded V1→Lite migration publish temp is transient residue, never a recovery copy: the migration
    // re-uploads a fresh temp on resume, so reclaim it unconditionally (ownership still gates the delete).
    private func cleanMigrationPublishScratch(_ entries: [RemoteStorageEntry]) async -> [String] {
        var deleted: [String] = []
        for entry in entries {
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    // One month's scratch. Valid canonical → delete only proven-unsound scratch and leave valid/unknown
    // recovery copies. Missing or invalid canonical → restore from a confidently preferred candidate, then
    // drop only proven-unsound siblings. Any inconclusive read blocks destructive cleanup for that month.
    private func cleanMonthScratch(
        month: LibraryMonthKey,
        scratch: [RemoteStorageEntry],
        canonicalPresent: Bool
    ) async -> [String] {
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        if canonicalPresent {
            switch await validateMonthManifest(canonicalPath) {
            case .valid:
                return await deleteInvalidScratchOnly(scratch)
            case .invalid:
                return await repairMonthFromScratch(
                    scratch,
                    canonicalPath: canonicalPath,
                    shouldReplaceInvalidCanonical: true
                )
            case .inconclusive:
                return []
            }
        }

        return await repairMonthFromScratch(
            scratch,
            canonicalPath: canonicalPath,
            shouldReplaceInvalidCanonical: false
        )
    }

    private func repairMonthFromScratch(
        _ scratch: [RemoteStorageEntry],
        canonicalPath: String,
        shouldReplaceInvalidCanonical: Bool
    ) async -> [String] {
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

        if let candidate = preferredRecoveryCandidate(valid) {
            let restored: Bool
            if shouldReplaceInvalidCanonical {
                restored = await replaceInvalidCanonical(from: candidate.path, canonicalPath: canonicalPath)
            } else {
                restored = await restoreCanonical(from: candidate.path, to: canonicalPath)
            }
            guard restored else { return [] }
            var deleted: [String] = []
            for entry in invalid {
                if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
            }
            return deleted
        }

        // Ambiguous sound candidates → leave all. No sound candidate (only proven junk) → drop the junk.
        guard valid.isEmpty else { return [] }
        var deleted: [String] = []
        for entry in invalid {
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    private func deleteInvalidScratchOnly(_ scratch: [RemoteStorageEntry]) async -> [String] {
        var deleted: [String] = []
        for entry in scratch {
            guard case .invalid = await validateMonthManifest(entry.path) else { continue }
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    private func preferredRecoveryCandidate(_ valid: [RemoteStorageEntry]) -> RemoteStorageEntry? {
        guard !valid.isEmpty else { return nil }
        if valid.count == 1 { return valid[0] }

        let tempCandidates = valid.filter { $0.name.hasSuffix(".tmp") }
        if tempCandidates.count == 1 {
            return tempCandidates[0]
        }

        let dated = valid.compactMap { entry -> (RemoteStorageEntry, Date)? in
            guard let modificationDate = entry.modificationDate else { return nil }
            return (entry, modificationDate)
        }
        guard dated.count == valid.count,
              let newest = dated.max(by: { $0.1 < $1.1 }) else {
            return nil
        }
        let newestCount = dated.filter { $0.1 == newest.1 }.count
        return newestCount == 1 ? newest.0 : nil
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
        switch MonthManifestStore.validateMonthManifestFile(at: localURL) {
        case .valid:
            return .valid
        case .invalid:
            return .invalid
        case .inconclusive:
            return .inconclusive
        }
    }

    // Publishes a validated scratch sqlite to its canonical month path and proves the canonical copy reads
    // back before any sibling scratch is deleted.
    private func restoreCanonical(from scratchPath: String, to canonicalPath: String) async -> Bool {
        guard await stillOwnedForDestructiveCleanup() else { return false }
        guard await isConfirmedAbsent(canonicalPath) else { return false }
        do {
            try await client.copy(from: scratchPath, to: canonicalPath)
        } catch {
            return false
        }
        guard case .valid = await validateMonthManifest(canonicalPath) else { return false }
        await monthsListing?.invalidate(basePath: basePath)
        _ = await deleteWhitelisted(scratchPath)
        return true
    }

    private func replaceInvalidCanonical(from scratchPath: String, canonicalPath: String) async -> Bool {
        guard await stillOwnedForDestructiveCleanup() else { return false }
        let backupPath = canonicalPath + ".repair-\(UUID().uuidString).bak"
        do {
            try await client.move(from: canonicalPath, to: backupPath)
        } catch {
            return false
        }

        do {
            try await client.copy(from: scratchPath, to: canonicalPath)
            guard case .valid = await validateMonthManifest(canonicalPath) else {
                await restoreInvalidCanonicalBackup(from: backupPath, to: canonicalPath)
                return false
            }
        } catch {
            await restoreInvalidCanonicalBackup(from: backupPath, to: canonicalPath)
            return false
        }

        await monthsListing?.invalidate(basePath: basePath)
        _ = await deleteWhitelisted(scratchPath)
        _ = await deleteWhitelisted(backupPath)
        return true
    }

    private func restoreInvalidCanonicalBackup(from backupPath: String, to canonicalPath: String) async {
        guard await stillOwnedForDestructiveCleanup() else { return }
        if (try? await client.exists(path: canonicalPath)) == true {
            try? await client.delete(path: canonicalPath)
        }
        try? await client.move(from: backupPath, to: canonicalPath)
        await monthsListing?.invalidate(basePath: basePath)
    }

    // MARK: - .watermelon/version_*.json.tmp and *.bak

    private enum VersionValidation {
        case current
        case invalid
        case inconclusive
    }

    private func cleanVersionScratch() async -> [String] {
        let repoDir = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
        let entries = (await listChildren(repoDir))
            .filter { !$0.isDirectory && VersionManifestLite.isVersionScratchFileName($0.name) }
        guard !entries.isEmpty else { return [] }
        guard case .current = await validateVersionManifest(RepoLayoutLite.versionPath(basePath: basePath)) else {
            return []
        }

        var deleted: [String] = []
        for entry in entries {
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    private func validateVersionManifest(_ remotePath: String) async -> VersionValidation {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("orphan-version-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: localURL) }
        do {
            try await client.download(remotePath: remotePath, localURL: localURL)
        } catch {
            return RemoteFaultLite.classify(error) == .notFound ? .invalid : .inconclusive
        }
        guard let data = try? Data(contentsOf: localURL),
              let manifest = try? VersionManifestLite.decode(data),
              VersionManifestLite.isCurrent(manifest) else {
            return .invalid
        }
        return .current
    }

    // MARK: - Foreground: expired .watermelon/locks/*.lock

    // Excludes the current writer's own lock and second-confirms every foreign expired lock (same bytes,
    // unchanged mtime, still expired across two reads) before deleting it, so a just-refreshed foreign
    // lock observed as expired in the first LIST is never removed.
    private func cleanExpiredLocks(now: Date) async -> [String] {
        let suffix = ".\(RepoLayoutLite.lockFileExtension)"
        let ownFilename = currentWriterID.flatMap { RepoLayoutLite.lockFilename(writerID: $0) }
        var deleted: [String] = []
        for entry in await listChildren(RepoLayoutLite.locksDirectoryPath(basePath: basePath))
        where !entry.isDirectory && entry.name.hasSuffix(suffix) {
            if let ownFilename, entry.name == ownFilename { continue }
            if let modificationDate = entry.modificationDate,
               !isExpired(modificationDate, now: now) {
                continue
            }
            guard await confirmForeignExpiredUnchanged(path: entry.path, snapshotDate: entry.modificationDate, now: now) else { continue }
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    // Best-effort: any unreadable/absent read, a freshened mtime, or changed bytes returns false so the
    // lock is left intact.
    private func confirmForeignExpiredUnchanged(path: String, snapshotDate: Date?, now: Date) async -> Bool {
        guard case .present(let snapshot1) = await RemoteLockReader.read(client: client, path: path),
              isExpired(snapshot1, now: now) else { return false }
        if let snapshotDate {
            guard let mtime1 = snapshot1.modificationDate,
                  Self.sameSecond(snapshotDate, mtime1) else {
                return false
            }
        }
        guard case .present(let snapshot2) = await RemoteLockReader.read(client: client, path: path),
              isExpired(snapshot2, now: now) else { return false }
        return snapshot1.rawData == snapshot2.rawData && snapshot1.modificationDate == snapshot2.modificationDate
    }

    private func isExpired(_ snapshot: RemoteLockReader.Snapshot, now: Date) -> Bool {
        if let modificationDate = snapshot.modificationDate {
            return isExpired(modificationDate, now: now)
        }
        guard let writtenAt = snapshot.body?.writtenAt else { return false }
        return isExpired(writtenAt, now: now)
    }

    // A nil mtime needs body timestamp evidence; old/undecodable bodies are not expired.
    private func isExpired(_ modificationDate: Date?, now: Date) -> Bool {
        guard let modificationDate else { return false }
        return now.timeIntervalSince(modificationDate) > lockExpiry
    }

    // MARK: - Primitives

    private func listChildren(_ path: String) async -> [RemoteStorageEntry] {
        (try? await client.list(path: path)) ?? []
    }

    private func listMonthsChildren() async -> [RemoteStorageEntry] {
        if let monthsListing {
            return (try? await monthsListing.entries(client: client, basePath: basePath)) ?? []
        }
        return await listChildren(RepoLayoutLite.monthsDirectoryPath(basePath: basePath))
    }

    // Three-way delete outcome so cleanup can tell confirmed absence from an unresolved transport fault.
    private enum DeleteOutcome {
        case deleted   // delete succeeded
        case absent    // notFound at delete time — success-equivalent
        case faulted   // any other (non-notFound) fault
    }

    private func classifiedDelete(_ path: String) async -> DeleteOutcome {
        guard await stillOwnedForDestructiveCleanup() else { return .faulted }
        do {
            try await client.delete(path: path)
            await monthsListing?.noteDeleted(path: path)
            return .deleted
        } catch {
            if RemoteFaultLite.classify(error) == .notFound {
                await monthsListing?.noteDeleted(path: path)
                return .absent
            }
            return .faulted
        }
    }

    // notFound at delete time is success-equivalent; any other fault is swallowed (best-effort).
    private func deleteWhitelisted(_ path: String) async -> Bool {
        await classifiedDelete(path) == .deleted
    }

    private func stillOwnedForDestructiveCleanup() async -> Bool {
        await ownershipGate.assertBeforeDestructiveAction()
    }

    private func isConfirmedAbsent(_ path: String) async -> Bool {
        do {
            return try await client.metadata(path: path) == nil
        } catch {
            return RemoteFaultLite.classify(error) == .notFound
        }
    }

    private static func isScratch(_ name: String) -> Bool {
        name.hasSuffix(".tmp") || name.hasSuffix(".bak")
    }

    // LIST and HEAD/metadata can differ in sub-second precision on S3-compatible backends; compare at second resolution.
    private static func sameSecond(_ a: Date?, _ b: Date?) -> Bool {
        switch (a, b) {
        case let (lhs?, rhs?): return Int(lhs.timeIntervalSince1970) == Int(rhs.timeIntervalSince1970)
        case (nil, nil): return true
        default: return false
        }
    }

    private actor CleanupOwnershipGate {
        private let assertOwnership: MonthManifestOwnershipAssertion?
        private let assertLeaseConfidence: MonthManifestOwnershipAssertion?
        private var hasFullAssertion = false

        init(
            assertOwnership: MonthManifestOwnershipAssertion?,
            assertLeaseConfidence: MonthManifestOwnershipAssertion?
        ) {
            self.assertOwnership = assertOwnership
            self.assertLeaseConfidence = assertLeaseConfidence
        }

        func assertBeforeDestructiveAction() async -> Bool {
            guard let assertOwnership else { return true }
            if !hasFullAssertion {
                do {
                    try await assertOwnership()
                    hasFullAssertion = true
                    return true
                } catch {
                    return false
                }
            }
            guard let assertLeaseConfidence else {
                do {
                    try await assertOwnership()
                    return true
                } catch {
                    return false
                }
            }
            do {
                try await assertLeaseConfidence()
                return true
            } catch {
                return false
            }
        }
    }
}

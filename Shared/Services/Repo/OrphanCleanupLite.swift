import CryptoKit
import Foundation

// Scoped Lite maintenance cleanup. Deletes only a fixed whitelist of stale *metadata* siblings:
// month-manifest temp/backup scratch, expired/invalid write locks, and explicitly enabled post-migration
// V1 manifests. Photo/resource bytes under <YYYY>/<MM>/<filename> are never touched. Best-effort: a
// missing directory or any transport fault is swallowed so cleanup can never change the caller's outcome.
struct OrphanCleanupLite {
    enum Mode: Sendable {
        case foreground
        case background
    }

    private let client: any RemoteStorageClientProtocol
    private let basePath: String
    private let lockExpiry: TimeInterval
    // The writer running this cleanup; its own active lock is never a deletion candidate.
    private let currentWriterID: String?
    private let ownershipGate: CleanupOwnershipGate
    private let monthsListing: LiteMonthsListingSnapshot?
    private let repoDirectoryEntries: [RemoteStorageEntry]?
    private let pruneLegacyV1Manifests: Bool
    private let hasOwnershipAssertion: Bool

    init(
        client: any RemoteStorageClientProtocol,
        basePath: String,
        currentWriterID: String? = nil,
        // Match WriteLockService's skew-widened stale band: never delete a foreign lock the writer
        // policy still treats as fresh (i.e. takeover-ineligible).
        lockExpiry: TimeInterval = WriteLockService.expiry + WriteLockService.clockSkewTolerance,
        assertOwnership: MonthManifestOwnershipAssertion? = nil,
        monthsListing: LiteMonthsListingSnapshot? = nil,
        repoDirectoryEntries: [RemoteStorageEntry]? = nil,
        pruneLegacyV1Manifests: Bool = false
    ) {
        self.client = client
        self.basePath = basePath
        self.currentWriterID = currentWriterID
        self.lockExpiry = lockExpiry
        self.ownershipGate = CleanupOwnershipGate(assertOwnership: assertOwnership)
        self.monthsListing = monthsListing
        self.repoDirectoryEntries = repoDirectoryEntries
        self.pruneLegacyV1Manifests = pruneLegacyV1Manifests
        self.hasOwnershipAssertion = assertOwnership != nil
    }

    @discardableResult
    func run(mode: Mode, now: Date = Date()) async -> [String] {
        var deleted: [String] = []
        if mode == .foreground {
            deleted += await cleanVersionScratch()
            deleted += await cleanMoveProbeScratch()
        }
        deleted += await cleanMonthsScratch()
        deleted += await cleanLegacyV1Manifests()
        deleted += await cleanExpiredLocks(now: now)
        return deleted
    }

    // MARK: - .watermelon/months/*.tmp and *.bak (repair-first)

    // Repair-first: a crash can leave a month's only surviving manifest in scratch (an unflushed `.tmp`
    // upload, or a `.bak` backed up mid-rename). Restore a sound candidate to its canonical month before
    // deleting, so cleanup never destroys the sole recoverable copy.
    private func cleanMonthsScratch() async -> [String] {
        let entries = (await listMonthsChildren()).filter { !$0.isDirectory }

        var canonicalMonths: [LibraryMonthKey: RemoteStorageEntry] = [:]
        for entry in entries where !Self.isScratch(entry.name) {
            if let month = RepoLayoutLite.month(fromFilename: entry.name) {
                canonicalMonths[month] = entry
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
        // Warm the MOVE-independence verdict once before any month repair: a first WebDAV call runs the multi-request
        // probe, and doing it here (before the per-month ownership proofs) keeps the memoized `resolve` inside the
        // repair helpers instant — so a repair's ownership proof still immediately precedes its canonical write, and
        // the lease can't lapse across a probe in that gap. No-op for known-independent backends (instant resolve).
        if !scratchByMonth.isEmpty {
            _ = await client.resolveMoveIsNonIndependent(basePath: basePath)
        }
        for (month, scratch) in scratchByMonth {
            deleted += await cleanMonthScratch(
                month: month, scratch: scratch, canonicalEntry: canonicalMonths[month]
            )
        }
        return deleted
    }

    // Outcome of validating a scratch candidate. `.inconclusive` (a download/read fault) is NOT proof of
    // corruption: it must never license deletion, or a transient blink could destroy the only recovery copy.
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
        guard !entries.isEmpty else { return [] }
        // On a non-independent MOVE backend a legacy temp→MOVE migration scratch can ALIAS the migrated canonical
        // (shared blob), so deleting it would destroy the canonical too. New code publishes migrations by direct PUT
        // (no alias), but historical residue can exist — so never reclaim it there.
        guard await client.resolveMoveIsNonIndependent(basePath: basePath) == false else { return [] }
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
        canonicalEntry: RemoteStorageEntry?
    ) async -> [String] {
        let canonicalPath = RepoLayoutLite.monthPath(basePath: basePath, month: month)
        if let canonicalEntry {
            switch await validateMonthManifest(canonicalEntry) {
            case .valid(let canonical):
                return await deleteRedundantScratch(scratch, canonical: canonical)
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
            case .valid(_): valid.append(entry)
            case .invalid: invalid.append(entry)
            case .inconclusive: anyInconclusive = true
            }
        }

        // Any unread candidate could be recovery material → leave the whole month untouched this pass.
        if anyInconclusive { return [] }

        // On a non-independent MOVE backend the temp→MOVE interrupted-flush pattern never occurs; a stale leaked
        // direct-PUT `.tmp` must not be preferred over a newer `.bak` (that would restore an older ledger).
        let nonIndependent = await client.resolveMoveIsNonIndependent(basePath: basePath)
        if let candidate = preferredRecoveryCandidate(valid, nonIndependent: nonIndependent) {
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

    private func deleteRedundantScratch(_ scratch: [RemoteStorageEntry], canonical: ValidMonthManifest) async -> [String] {
        // On a non-independent MOVE backend a legacy temp→MOVE scratch can ALIAS the valid canonical (shared blob),
        // so reclaiming a byte-identical "redundant" scratch would destroy the canonical too. New code never creates
        // such aliases, but historical residue can — so never reclaim valid redundant scratch there. Proven-invalid
        // scratch is byte-different (never an alias of a valid canonical), so it stays reclaimable.
        let nonIndependent = await client.resolveMoveIsNonIndependent(basePath: basePath)
        var deleted: [String] = []
        for entry in scratch {
            switch await validateMonthManifest(entry) {
            case .invalid:
                if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
            case .valid(let candidate):
                guard isRedundantScratch(candidate, canonical: canonical) else { continue }
                // In compat mode a scratch byte-identical to the canonical could be a legacy MOVE alias (shared
                // blob) — never reclaim it. A byte-different redundant scratch cannot be an alias, so it stays
                // reclaimable (this clears a stale leaked direct-PUT `.tmp` once the canonical advances past it).
                if nonIndependent, candidate.data == canonical.data { continue }
                if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
            case .inconclusive:
                continue
            }
        }
        return deleted
    }

    private func isRedundantScratch(_ scratch: ValidMonthManifest, canonical: ValidMonthManifest) -> Bool {
        if scratch.data == canonical.data { return true }
        // A `.bak` is the prior canonical backed up before an overwrite; behind a valid-but-unverified
        // replacement it may be the last verified-good copy, and cleanup cannot prove the canonical read
        // back byte-exact. A successful flush drops its own backup inline, so a surviving byte-different
        // `.bak` is recovery material — never reclaim it by mtime alone.
        if RepoLayoutLite.isBackupScratch(scratch.entry.name) { return false }
        guard let scratchDate = scratch.entry.modificationDate,
              let canonicalDate = canonical.entry.modificationDate else {
            return false
        }
        return scratchDate < canonicalDate
    }

    private func preferredRecoveryCandidate(_ valid: [RemoteStorageEntry], nonIndependent: Bool) -> RemoteStorageEntry? {
        guard !valid.isEmpty else { return nil }
        if valid.count == 1 { return valid[0] }

        // Independent backends: prefer the sole `.tmp` — on a temp→MOVE interrupted flush it is the intended new
        // manifest. Non-independent backends never produce that pattern (a `.tmp` there is either the current
        // recovery or a stale leaked one), so skip the shortcut and pick the newest good state by mtime below.
        if !nonIndependent {
            let tempCandidates = valid.filter { $0.name.hasSuffix(".tmp") }
            if tempCandidates.count == 1 {
                return tempCandidates[0]
            }
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

    private struct ValidMonthManifest {
        let entry: RemoteStorageEntry
        let data: Data
    }

    private enum MonthManifestValidation {
        case valid(ValidMonthManifest)
        case invalid
        case inconclusive
    }

    // Downloads a remote month sqlite and classifies it. A `client.download` fault is inconclusive (the
    // bytes were never obtained, so soundness is unknown); a successful download is then proven sound/unsound.
    private func validateMonthManifest(_ remotePath: String) async -> MonthManifestValidation {
        let entry = RemoteStorageEntry(
            path: remotePath,
            name: (remotePath as NSString).lastPathComponent,
            isDirectory: false,
            size: 0,
            creationDate: nil,
            modificationDate: nil
        )
        return await validateMonthManifest(entry)
    }

    private func validateMonthManifest(
        _ entry: RemoteStorageEntry,
        month: LibraryMonthKey? = nil,
        layout: MonthManifestStore.ManifestLayout? = nil
    ) async -> MonthManifestValidation {
        let localURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("orphan-validate-\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: localURL) }
        do {
            try await client.download(remotePath: entry.path, localURL: localURL)
        } catch {
            return .inconclusive
        }
        let validation: MonthManifestStore.ManifestFileValidation
        if let month, let layout {
            validation = MonthManifestStore.validateMonthManifestFile(
                at: localURL,
                year: month.year,
                month: month.month,
                client: client,
                basePath: basePath,
                layout: layout
            )
        } else {
            validation = MonthManifestStore.validateMonthManifestFile(at: localURL)
        }
        switch validation {
        case .valid:
            guard let data = try? Data(contentsOf: localURL) else { return .inconclusive }
            return .valid(ValidMonthManifest(entry: entry, data: data))
        case .invalid:
            return .invalid
        case .inconclusive:
            return .inconclusive
        }
    }

    // Publishes the bytes at `sourcePath` onto `canonicalPath`. On non-independent-MOVE backends a server-side
    // copy would alias source and destination (deleting either later destroys both), so download the source and
    // PUT it as an independent blob; atomic backends use the fast server-side copy.
    // The capability is resolved (and the WebDAV probe warmed) once up front in `cleanMonthsScratch`, so this
    // `resolve` is an instant memoized return — the caller's ownership proof still immediately precedes the write.
    private func materializeCanonical(fromScratch sourcePath: String, to canonicalPath: String) async -> Bool {
        if await client.resolveMoveIsNonIndependent(basePath: basePath) {
            let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            defer { try? FileManager.default.removeItem(at: localURL) }
            do {
                try await client.download(remotePath: sourcePath, localURL: localURL)
                // Re-prove ownership after the download await, before the canonical PUT: a lease that lapsed during
                // the download must not let this stale writer clobber a successor's canonical.
                guard await stillOwnedForDestructiveCleanup() else { return false }
                try await client.upload(localURL: localURL, remotePath: canonicalPath, respectTaskCancellation: false, onProgress: nil)
                return true
            } catch {
                // The PUT may have landed server-side before the client saw a failure: drop the cached listing so a
                // same-session pass re-lists the actual canonical state rather than trusting a stale listing.
                await monthsListing?.invalidate(basePath: basePath)
                return false
            }
        }
        do {
            try await client.copy(from: sourcePath, to: canonicalPath)
            return true
        } catch {
            await monthsListing?.invalidate(basePath: basePath)
            return false
        }
    }

    // Publishes a validated scratch sqlite to its canonical month path and proves the canonical copy reads
    // back before any sibling scratch is deleted.
    private func restoreCanonical(from scratchPath: String, to canonicalPath: String) async -> Bool {
        guard await stillOwnedForDestructiveCleanup() else { return false }
        guard await isConfirmedAbsent(canonicalPath) else { return false }
        // Re-prove after the awaited probe because the publish overwrites on common backends.
        guard await stillOwnedForDestructiveCleanup() else { return false }
        guard await materializeCanonical(fromScratch: scratchPath, to: canonicalPath) else { return false }
        // Publish landed the canonical; reflect it even when the read-back below is inconclusive.
        await monthsListing?.invalidate(basePath: basePath)
        guard case .valid(_) = await validateMonthManifest(canonicalPath) else { return false }
        _ = await deleteWhitelisted(scratchPath)
        return true
    }

    private func replaceInvalidCanonical(from scratchPath: String, canonicalPath: String) async -> Bool {
        guard await stillOwnedForDestructiveCleanup() else { return false }
        // Compat mode: overwrite the invalid canonical directly with the scratch bytes (independent PUT). A
        // server-side move/copy backup would alias on these backends, and an invalid canonical is worthless — a
        // failed PUT just leaves it invalid for the next cleanup pass, so no backup is needed.
        if await client.resolveMoveIsNonIndependent(basePath: basePath) {
            guard await materializeCanonical(fromScratch: scratchPath, to: canonicalPath) else { return false }
            await monthsListing?.invalidate(basePath: basePath)
            guard case .valid(_) = await validateMonthManifest(canonicalPath) else { return false }
            _ = await deleteWhitelisted(scratchPath)
            return true
        }
        let backupPath = RepoLayoutLite.repairBackupPath(forCanonicalPath: canonicalPath)
        do {
            try await client.move(from: canonicalPath, to: backupPath)
        } catch {
            return false
        }
        // Move emptied the canonical name; reflect it before the early-returns below (incl. restore).
        await monthsListing?.invalidate(basePath: basePath)

        // Re-prove after the awaited backup move because copy() overwrites on common backends.
        guard await stillOwnedForDestructiveCleanup() else {
            await restoreInvalidCanonicalBackup(from: backupPath, to: canonicalPath)
            return false
        }

        do {
            try await client.copy(from: scratchPath, to: canonicalPath)
            guard case .valid(_) = await validateMonthManifest(canonicalPath) else {
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
            // Do not delete over a successor after the awaited probe.
            guard await stillOwnedForDestructiveCleanup() else { return }
            try? await client.delete(path: canonicalPath)
            // Reflect the delete even if the restore move below is skipped on a lost lease.
            await monthsListing?.invalidate(basePath: basePath)
        }
        guard await stillOwnedForDestructiveCleanup() else { return }
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
        let entries = (await listRepoDirectoryChildren(repoDir))
            .filter { !$0.isDirectory && VersionManifestLite.isVersionScratchFileName($0.name) }
        guard !entries.isEmpty else { return [] }
        // On a non-independent MOVE backend a legacy temp→MOVE version scratch can ALIAS version.json (shared blob),
        // so deleting it would destroy the canonical too. New code commits version.json by direct PUT (no alias),
        // but historical residue can exist — so never reclaim it there.
        guard await client.resolveMoveIsNonIndependent(basePath: basePath) == false else { return [] }
        guard case .current = await validateVersionManifest(RepoLayoutLite.versionPath(basePath: basePath)) else {
            return []
        }

        var deleted: [String] = []
        for entry in entries {
            if await deleteWhitelisted(entry.path) { deleted.append(entry.path) }
        }
        return deleted
    }

    // MOVE-independence probe scratch (.watermelon/movecheck_<uuid>.src|dst) is throwaway diagnostic state, never a
    // recovery source, so a crash-leaked one is always safe to reclaim unconditionally (ownership still gates it).
    private func cleanMoveProbeScratch() async -> [String] {
        let repoDir = RepoLayoutLite.repoDirectoryPath(basePath: basePath)
        let entries = (await listRepoDirectoryChildren(repoDir))
            .filter { !$0.isDirectory && RepoLayoutLite.isMoveProbeScratchFileName($0.name) }
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

    // MARK: - Committed V1 manifests (post-migration compensation)

    private func cleanLegacyV1Manifests() async -> [String] {
        guard pruneLegacyV1Manifests, hasOwnershipAssertion else { return [] }
        guard let marker = await loadLegacyPruneMarker(), marker.isSupported else { return [] }
        guard case .current = await validateVersionManifest(RepoLayoutLite.versionPath(basePath: basePath)) else {
            return []
        }

        var deleted: [String] = []
        var resolvedAll = true
        for source in marker.sources {
            switch await cleanLegacyV1Manifest(source) {
            case .deleted:
                deleted.append(source.manifestPath)
            case .alreadyResolved:
                break
            case .pending:
                resolvedAll = false
            }
        }
        if resolvedAll {
            _ = await deleteLegacyPruneMarker()
        }
        return deleted
    }

    private func loadLegacyPruneMarker() async -> LegacyV1PruneMarker? {
        let markerPath = RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath)
        let markerURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("legacy-v1-prune-marker-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: markerURL) }
        do {
            if let repoDirectoryEntries {
                guard repoDirectoryEntries.contains(where: {
                    !$0.isDirectory && $0.name == RepoLayoutLite.legacyV1PrunePendingFileName
                }) else {
                    return nil
                }
            } else {
                guard let entry = try await client.metadata(path: markerPath),
                      !entry.isDirectory else {
                    return nil
                }
            }
            try await client.download(remotePath: markerPath, localURL: markerURL)
            let data = try Data(contentsOf: markerURL)
            return try JSONDecoder().decode(LegacyV1PruneMarker.self, from: data)
        } catch {
            return nil
        }
    }

    private enum LegacyV1PruneOutcome {
        case deleted
        case alreadyResolved
        case pending
    }

    private func cleanLegacyV1Manifest(_ source: LegacyV1PruneMarker.Source) async -> LegacyV1PruneOutcome {
        guard source.isCanonicalV1ManifestPath(basePath: basePath) else { return .pending }
        let v1Entry: RemoteStorageEntry
        do {
            guard let entry = try await client.metadata(path: source.manifestPath) else {
                return .alreadyResolved
            }
            guard !entry.isDirectory else { return .pending }
            v1Entry = entry
        } catch {
            return RemoteFaultLite.classify(error) == .notFound ? .alreadyResolved : .pending
        }

        guard case .valid(let v1Manifest) = await validateMonthManifest(v1Entry, month: source.monthKey, layout: .v1),
              Self.sha256Hex(v1Manifest.data) == source.sha256Hex else {
            return .pending
        }

        let litePath = RepoLayoutLite.monthPath(basePath: basePath, month: source.monthKey)
        let liteEntry = RemoteStorageEntry(
            path: litePath,
            name: (litePath as NSString).lastPathComponent,
            isDirectory: false,
            size: 0,
            creationDate: nil,
            modificationDate: nil
        )
        guard case .valid(_) = await validateMonthManifest(liteEntry, month: source.monthKey, layout: .lite) else {
            return .pending
        }

        guard await stillOwnedForDestructiveCleanup(),
              case .current = await validateVersionManifest(RepoLayoutLite.versionPath(basePath: basePath)) else {
            return .pending
        }
        guard await legacyV1ManifestMatchesMarker(source) else { return .pending }
        switch await classifiedDelete(source.manifestPath) {
        case .deleted:
            return .deleted
        case .absent:
            return .alreadyResolved
        case .faulted:
            return .pending
        }
    }

    private func legacyV1ManifestMatchesMarker(_ source: LegacyV1PruneMarker.Source) async -> Bool {
        let entry = RemoteStorageEntry(
            path: source.manifestPath,
            name: (source.manifestPath as NSString).lastPathComponent,
            isDirectory: false,
            size: 0,
            creationDate: nil,
            modificationDate: nil
        )
        guard case .valid(let manifest) = await validateMonthManifest(entry, month: source.monthKey, layout: .v1) else {
            return false
        }
        return Self.sha256Hex(manifest.data) == source.sha256Hex
    }

    private func deleteLegacyPruneMarker() async -> Bool {
        guard await stillOwnedForDestructiveCleanup() else { return false }
        return await classifiedDelete(RepoLayoutLite.legacyV1PrunePendingPath(basePath: basePath)) == .deleted
    }

    private static func sha256Hex(_ data: Data) -> String {
        Data(SHA256.hash(data: data)).hexString
    }

    // MARK: - Expired/invalid .watermelon/locks/*.lock

    // Excludes the current writer's own lock and second-confirms every foreign candidate before deleting it.
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

    // Best-effort: any unreadable/absent read, a freshened mtime, or changed bytes leaves the lock intact.
    private func confirmForeignExpiredUnchanged(
        path: String,
        snapshotDate: Date?,
        now: Date
    ) async -> Bool {
        guard case .present(let snapshot1) = await RemoteLockReader.read(client: client, path: path),
              isExpiredOrInvalid(snapshot1, now: now) else { return false }
        if let snapshotDate {
            guard let mtime1 = snapshot1.modificationDate,
                  RemoteTimestampComparison.sameSecond(snapshotDate, mtime1) else {
                return false
            }
        }
        guard case .present(let snapshot2) = await RemoteLockReader.read(client: client, path: path),
              isExpiredOrInvalid(snapshot2, now: now),
              snapshot1.rawData == snapshot2.rawData,
              snapshot1.modificationDate == snapshot2.modificationDate else { return false }
        return true
    }

    private func isExpiredOrInvalid(_ snapshot: RemoteLockReader.Snapshot, now: Date) -> Bool {
        let sources = [snapshot.modificationDate, snapshot.body?.writtenAt].compactMap { $0 }
        guard !sources.isEmpty else { return true }
        return sources.allSatisfy { isExpired($0, now: now) }
    }

    // A nil mtime needs body timestamp evidence; old/undecodable bodies are invalid locks.
    private func isExpired(_ modificationDate: Date?, now: Date) -> Bool {
        guard let modificationDate else { return false }
        return now.timeIntervalSince(modificationDate) > lockExpiry
    }

    // MARK: - Primitives

    private func listChildren(_ path: String) async -> [RemoteStorageEntry] {
        (try? await client.list(path: path)) ?? []
    }

    private func listRepoDirectoryChildren(_ path: String) async -> [RemoteStorageEntry] {
        if path == RepoLayoutLite.repoDirectoryPath(basePath: basePath), let repoDirectoryEntries {
            return repoDirectoryEntries
        }
        return await listChildren(path)
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
        RepoLayoutLite.isScratchFileName(name)
    }

    // Every destructive cleanup action re-proves ownership strongly: cleanup is bounded and rare, so the
    // per-action remote proof is negligible, and a destructive delete must never run on in-memory confidence.
    private actor CleanupOwnershipGate {
        private let assertOwnership: MonthManifestOwnershipAssertion?

        init(assertOwnership: MonthManifestOwnershipAssertion?) {
            self.assertOwnership = assertOwnership
        }

        func assertBeforeDestructiveAction() async -> Bool {
            guard let assertOwnership else { return true }
            do {
                try await assertOwnership()
                return true
            } catch {
                return false
            }
        }
    }
}

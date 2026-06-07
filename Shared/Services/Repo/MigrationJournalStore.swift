import Foundation
import os.log

private let migrationJournalStoreLog = Logger(
    subsystem: "com.zizicici.watermelon",
    category: "MigrationJournalStore"
)

/// Terminal per-month decision recorded by V1 migration.
nonisolated enum MigrationJournalOutcome: String, Sendable, Equatable, CaseIterable {
    case imported
    case quarantined
    case failed

    /// `.imported`/`.quarantined` are safe terminal decisions that resolve a month for open authority;
    /// `.failed` leaves it unresolved so the existing foreground-migration/refusal route still fires.
    var isSafeTerminal: Bool {
        switch self {
        case .imported, .quarantined: return true
        case .failed: return false
        }
    }
}

enum MigrationJournalError: Error, Equatable {
    case unsupportedVersion(Int)
    case unknownOutcome(String)
    case malformed(String)
}

/// One month's terminal migration outcome. Versioned and additive; readers tolerate unknown keys
/// and only require the fields this version writes.
nonisolated struct MigrationJournalRecord: Sendable, Equatable {
    static let currentVersion = 1

    let repoID: String
    let writerID: String
    let runID: String
    let year: Int
    let month: Int
    let outcome: MigrationJournalOutcome
    let createdAtMs: Int64
    let migratedAssetCount: Int
    let totalAssetCount: Int
    let skippedAssetCount: Int
    let reason: String?

    init(
        repoID: String,
        writerID: String,
        runID: String,
        year: Int,
        month: Int,
        outcome: MigrationJournalOutcome,
        createdAtMs: Int64,
        migratedAssetCount: Int,
        totalAssetCount: Int,
        skippedAssetCount: Int,
        reason: String?
    ) {
        self.repoID = repoID
        self.writerID = writerID
        self.runID = runID
        self.year = year
        self.month = month
        self.outcome = outcome
        self.createdAtMs = createdAtMs
        self.migratedAssetCount = migratedAssetCount
        self.totalAssetCount = totalAssetCount
        self.skippedAssetCount = skippedAssetCount
        self.reason = reason
    }

    init(data: Data) throws {
        guard let dict = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw MigrationJournalError.malformed("journal record is not a JSON object")
        }
        let version = try RepoWireValidator.requireInt(dict["v"], field: "v")
        guard version == Self.currentVersion else {
            throw MigrationJournalError.unsupportedVersion(version)
        }
        self.repoID = try RepoWireValidator.validateRepoID(
            try RepoWireValidator.requireString(dict, "repo_id"),
            field: "repo_id"
        )
        self.writerID = try RepoWireValidator.requireNonEmptyString(dict, "writer_id")
        self.runID = try RepoWireValidator.requireString(dict, "run_id")
        let month = try RepoWireValidator.requireInt(dict["month"], field: "month")
        guard (1...12).contains(month) else {
            throw MigrationJournalError.malformed("month out of range: \(month)")
        }
        self.year = try RepoWireValidator.requireInt(dict["year"], field: "year")
        self.month = month
        let outcomeRaw = try RepoWireValidator.requireString(dict, "outcome")
        guard let outcome = MigrationJournalOutcome(rawValue: outcomeRaw) else {
            throw MigrationJournalError.unknownOutcome(outcomeRaw)
        }
        self.outcome = outcome
        self.createdAtMs = try RepoWireValidator.validateNonNegativeInt64(dict["created_at_ms"], field: "created_at_ms")
        self.migratedAssetCount = try RepoWireValidator.validateNonNegativeInt(dict["migrated_asset_count"], field: "migrated_asset_count")
        self.totalAssetCount = try RepoWireValidator.validateNonNegativeInt(dict["total_asset_count"], field: "total_asset_count")
        self.skippedAssetCount = try RepoWireValidator.validateNonNegativeInt(dict["skipped_asset_count"], field: "skipped_asset_count")
        self.reason = dict["reason"] as? String
    }

    func encode() throws -> Data {
        var dict: [String: Any] = [
            "v": Self.currentVersion,
            "repo_id": repoID,
            "writer_id": writerID,
            "run_id": runID,
            "year": year,
            "month": month,
            "outcome": outcome.rawValue,
            "created_at_ms": createdAtMs,
            "migrated_asset_count": migratedAssetCount,
            "total_asset_count": totalAssetCount,
            "skipped_asset_count": skippedAssetCount
        ]
        if let reason { dict["reason"] = reason }
        return try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])
    }
}

nonisolated struct MigrationJournalSummary: Sendable, Equatable {
    let records: [MigrationJournalRecord]

    var isEmpty: Bool { records.isEmpty }

    func records(year: Int, month: Int) -> [MigrationJournalRecord] {
        records.filter { $0.year == year && $0.month == month }
    }

    /// Months a safe terminal record (`.imported`/`.quarantined`) resolved for `repoID`. A `.failed`-only
    /// month is absent. Records whose `repoID` differs are ignored: suppression must be scoped to the V2
    /// repo authority being opened (mirroring the `existingRepoIDsInV2Data` identity gate), so a foreign
    /// or planted record cannot strand a live V1 month.
    func safelyResolvedMonths(forRepoID repoID: String) -> Set<LibraryMonthKey> {
        var resolved: Set<LibraryMonthKey> = []
        for record in records where record.outcome.isSafeTerminal && record.repoID == repoID {
            resolved.insert(LibraryMonthKey(year: record.year, month: record.month))
        }
        return resolved
    }
}

/// Authority for the additive `.watermelon/migrations/journal/` record dir. Writes are byte-verified
/// like other durable repo metadata; summary reads list only the journal child dir and fail closed on a
/// malformed entry so an unsafe entry can never be silently dropped from an authoritative read.
nonisolated struct MigrationJournalStore: Sendable {
    struct InvalidRecord: Error, Sendable {
        let path: String
        let reason: String
    }

    let client: any RemoteStorageClientProtocol
    let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = client
        self.basePath = basePath
    }

    /// Writes one verified record under the journal child dir. The `eventID` suffix makes the path
    /// unique, so `.alreadyExists` only signals a UUID collision and retries with a fresh suffix.
    func record(_ record: MigrationJournalRecord) async throws {
        do {
            try await client.createDirectory(path: RepoLayout.migrationJournalDirectoryPath(base: basePath))
            let data = try record.encode()
            let temp = FileManager.default.temporaryDirectory
                .appendingPathComponent("migration-journal-\(UUID().uuidString).json")
            try data.write(to: temp, options: .atomic)
            defer { try? FileManager.default.removeItem(at: temp) }
            let month = LibraryMonthKey(year: record.year, month: record.month)
            for _ in 0..<4 {
                let eventID = UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
                let path = RepoLayout.migrationJournalRecordPath(
                    base: basePath,
                    month: month,
                    writerID: record.writerID,
                    runID: record.runID,
                    eventID: eventID
                )
                let outcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
                    client: client,
                    localURL: temp,
                    remotePath: path,
                    respectTaskCancellation: false,
                    finalizationPolicy: .allowBestEffort
                )
                if case .alreadyExists = outcome.result { continue }
                if outcome.verification != .verifiedLocalBytes {
                    try await verify(remotePath: path, localURL: temp)
                }
                return
            }
            throw NSError(domain: "MigrationJournalStore", code: -1, userInfo: [
                NSLocalizedDescriptionKey: "could not allocate unique journal record path for \(record.writerID)"
            ])
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    /// Lists only the journal child dir. Not-found is an empty summary; a `.json` whose filename or
    /// bytes don't parse fails closed (`InvalidRecord`) so summary authority can't be silently unsafe.
    func loadSummary() async throws -> MigrationJournalSummary {
        let dir = RepoLayout.migrationJournalDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            if isStorageNotFoundError(error) { return MigrationJournalSummary(records: []) }
            throw RemoteWriteClassifier.normalizedCancellation(error)
        }
        let jsonEntries = entries.filter { !$0.isDirectory && $0.name.hasSuffix(".json") }
        for entry in jsonEntries {
            guard RepoLayout.parseMigrationJournalRecordFilename(entry.name) != nil else {
                let path = RemotePathBuilder.absolutePath(basePath: dir, remoteRelativePath: entry.name)
                migrationJournalStoreLog.warning(
                    "non-record .json in reserved journal dir at \(path, privacy: .public)"
                )
                throw InvalidRecord(path: path, reason: "filename does not match journal record pattern")
            }
        }
        var records: [MigrationJournalRecord] = []
        for entry in jsonEntries {
            let path = RemotePathBuilder.absolutePath(basePath: dir, remoteRelativePath: entry.name)
            let temp = FileManager.default.temporaryDirectory
                .appendingPathComponent("migration-journal-read-\(UUID().uuidString).json")
            defer { try? FileManager.default.removeItem(at: temp) }
            let data = try await downloadListedRecordToleratingVisibilityLag(path: path, localURL: temp)
            do {
                records.append(try MigrationJournalRecord(data: data))
            } catch {
                migrationJournalStoreLog.warning(
                    "invalid journal record at \(path, privacy: .public): \(String(describing: error), privacy: .public)"
                )
                throw InvalidRecord(path: path, reason: String(describing: error))
            }
        }
        return MigrationJournalSummary(records: records)
    }

    /// A record surfaced by the listing exists, but on grace backends its data-path download can 404 while
    /// the listing already sees it. Spend the read-after-write grace budget on that not-found before failing
    /// closed — every other consulted inspection metadata read (version, identity, markers) already tolerates
    /// this lag, so a just-journaled month on a multi-device open must not be misread as a permanently invalid
    /// record. Mirroring `MigrationMarkerStore.downloadListedMarkerToleratingVisibilityLag`, a non-not-found
    /// error (transient transport, external-volume-unavailable) propagates raw — not as `InvalidRecord` — so
    /// the analyzer's `InvalidRecord → damagedV2Repo` mapping never brands a transient/unavailable failure as
    /// deterministic repo damage. Only a not-found that persists past grace (or a zero-grace listed-but-404)
    /// fails closed with `InvalidRecord`, since journal records are additive and never deleted.
    private func downloadListedRecordToleratingVisibilityLag(path: String, localURL: URL) async throws -> Data {
        let data = try await GracefulRead.retryWithinGrace(
            client: client,
            floorSeconds: 1,
            backoff: .exponential(baseMs: 200, maxShift: 3)
        ) {
            do {
                try await client.download(remotePath: path, localURL: localURL)
                return try Data(contentsOf: localURL)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                guard isStorageNotFoundError(error) else { throw error }
                return nil
            }
        }
        if let data { return data }
        throw InvalidRecord(path: path, reason: "journal record listed but unreadable within read-after-write grace")
    }

    private func verify(remotePath: String, localURL: URL) async throws {
        if try await MetadataCreateGate.verifyMatchesLocalWithRetries(
            client: client,
            remotePath: remotePath,
            localURL: localURL
        ) {
            return
        }
        throw NSError(domain: "MigrationJournalStore", code: -2, userInfo: [
            NSLocalizedDescriptionKey: "journal record bytes did not verify at \(remotePath)"
        ])
    }
}

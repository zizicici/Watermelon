import XCTest
@testable import Watermelon

final class MigrationJournalStoreTests: XCTestCase {
    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    // MARK: - Wire encode/decode

    func testEncodeDecodeRoundTrip() throws {
        let record = Self.makeRecord(outcome: .imported, year: 2025, month: 6, reason: nil)
        let decoded = try MigrationJournalRecord(data: try record.encode())
        XCTAssertEqual(decoded, record)
    }

    func testEncodeDecodeRoundTrip_quarantinedWithReason() throws {
        let record = Self.makeRecord(
            outcome: .quarantined,
            year: 2024,
            month: 11,
            migratedAssetCount: 0,
            totalAssetCount: 7,
            skippedAssetCount: 2,
            reason: "existing V2 month outcome ambiguous not clean"
        )
        let decoded = try MigrationJournalRecord(data: try record.encode())
        XCTAssertEqual(decoded, record)
        XCTAssertEqual(decoded.outcome, .quarantined)
        XCTAssertEqual(decoded.reason, "existing V2 month outcome ambiguous not clean")
        XCTAssertEqual(decoded.totalAssetCount, 7)
    }

    func testDecodeRejectsUnknownOutcome() throws {
        let data = try JSONSerialization.data(withJSONObject: Self.baseDict(outcome: "bogus"))
        XCTAssertThrowsError(try MigrationJournalRecord(data: data)) { error in
            guard case MigrationJournalError.unknownOutcome(let raw) = error else {
                return XCTFail("expected unknownOutcome, got \(error)")
            }
            XCTAssertEqual(raw, "bogus")
        }
    }

    func testDecodeRejectsUnsupportedVersion() throws {
        var dict = Self.baseDict(outcome: "imported")
        dict["v"] = 99
        let data = try JSONSerialization.data(withJSONObject: dict)
        XCTAssertThrowsError(try MigrationJournalRecord(data: data)) { error in
            guard case MigrationJournalError.unsupportedVersion(99) = error else {
                return XCTFail("expected unsupportedVersion(99), got \(error)")
            }
        }
    }

    func testDecodeRejectsOutOfRangeMonth() throws {
        var dict = Self.baseDict(outcome: "imported")
        dict["month"] = 13
        let data = try JSONSerialization.data(withJSONObject: dict)
        XCTAssertThrowsError(try MigrationJournalRecord(data: data))
    }

    // MARK: - Verified write under journal dir

    func testRecord_writesVerifiedFileUnderJournalDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let store = MigrationJournalStore(client: client, basePath: basePath)
        let record = Self.makeRecord(outcome: .imported, year: 2025, month: 6, reason: nil)
        try await store.record(record)

        let journalDir = RepoLayout.migrationJournalDirectoryPath(base: basePath)
        let entries = try await client.list(path: journalDir)
        let jsonFiles = entries.filter { !$0.isDirectory && $0.name.hasSuffix(".json") }
        XCTAssertEqual(jsonFiles.count, 1, "record must write exactly one journal record file")

        let recovered = try await Self.download(client, path: jsonFiles[0].path)
        XCTAssertEqual(try MigrationJournalRecord(data: recovered), record,
                       "remote bytes must decode back to the written record")
    }

    func testRecord_doesNotWriteDirectlyUnderMigrationsDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let store = MigrationJournalStore(client: client, basePath: basePath)
        try await store.record(Self.makeRecord(outcome: .imported, year: 2025, month: 6, reason: nil))

        let migrationsDir = RepoLayout.migrationsDirectoryPath(base: basePath)
        let entries = try await client.list(path: migrationsDir)
        let strayJSON = entries.filter { !$0.isDirectory && $0.name.hasSuffix(".json") }
        XCTAssertTrue(strayJSON.isEmpty, "journal records must not land directly under .watermelon/migrations/")
        XCTAssertTrue(
            entries.contains { $0.isDirectory && $0.name == RepoLayout.migrationJournalDirectory },
            "records live under the journal child directory"
        )
    }

    // MARK: - Root migration-marker namespace separation

    func testJournalDirectory_isNotSeenAsAMigrationMarker() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await MigrationJournalStore(client: client, basePath: basePath)
            .record(Self.makeRecord(outcome: .imported, year: 2025, month: 6, reason: nil))

        let markerStore = MigrationMarkerStore(client: client, basePath: basePath)
        let existsAny = try await markerStore.existsAny()
        XCTAssertFalse(existsAny, "the journal child directory must not register as a migration marker")
        let parsed = try await markerStore.parseEntries(markerStore.migrationsDirectoryEntries())
        XCTAssertTrue(parsed.isEmpty, "journal child directory must not fail closed as an invalid marker")
    }

    // MARK: - Summary reads

    func testLoadSummary_returnsWrittenRecords() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let store = MigrationJournalStore(client: client, basePath: basePath)
        try await store.record(Self.makeRecord(outcome: .imported, year: 2025, month: 6, reason: nil))
        try await store.record(Self.makeRecord(outcome: .quarantined, year: 2024, month: 3, reason: "deferred"))

        let summary = try await store.loadSummary()
        XCTAssertEqual(summary.records.count, 2)
        XCTAssertEqual(Set(summary.records.map { $0.outcome }), [.imported, .quarantined])
        XCTAssertEqual(summary.records(year: 2024, month: 3).first?.reason, "deferred")
    }

    func testLoadSummary_emptyWhenNoJournalDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let summary = try await MigrationJournalStore(client: client, basePath: basePath).loadSummary()
        XCTAssertTrue(summary.isEmpty)
    }

    func testLoadSummary_failsClosedOnMalformedRecordBytes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Valid journal record filename, but the bytes are not a decodable record.
        let path = RepoLayout.migrationJournalRecordPath(
            base: basePath,
            month: LibraryMonthKey(year: 2025, month: 6),
            writerID: "w",
            runID: "run-1",
            eventID: "deadbeefdeadbeefdeadbeefdeadbeef"
        )
        await client.injectFile(path: path, data: Data("not json".utf8))

        let store = MigrationJournalStore(client: client, basePath: basePath)
        do {
            _ = try await store.loadSummary()
            XCTFail("summary must fail closed on a malformed journal record")
        } catch is MigrationJournalStore.InvalidRecord {
            // expected
        }
    }

    func testLoadSummary_failsClosedOnNonRecordJsonFilename() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let journalDir = RepoLayout.migrationJournalDirectoryPath(base: basePath)
        await client.injectFile(path: "\(journalDir)/garbage.json", data: Data("{}".utf8))

        let store = MigrationJournalStore(client: client, basePath: basePath)
        do {
            _ = try await store.loadSummary()
            XCTFail("summary must fail closed on a .json filename outside the journal record pattern")
        } catch is MigrationJournalStore.InvalidRecord {
            // expected
        }
    }

    // MARK: - Resolved-month summary helpers

    func testSafelyResolvedMonths_importedResolvesMonth() {
        let summary = MigrationJournalSummary(records: [
            Self.makeRecord(outcome: .imported, year: 2025, month: 6, reason: nil)
        ])
        XCTAssertEqual(summary.safelyResolvedMonths(), [LibraryMonthKey(year: 2025, month: 6)])
    }

    func testSafelyResolvedMonths_quarantinedResolvesMonth() {
        let summary = MigrationJournalSummary(records: [
            Self.makeRecord(outcome: .quarantined, year: 2024, month: 11, reason: "deferred")
        ])
        XCTAssertEqual(summary.safelyResolvedMonths(), [LibraryMonthKey(year: 2024, month: 11)])
    }

    func testSafelyResolvedMonths_failedAloneDoesNotResolve() {
        let summary = MigrationJournalSummary(records: [
            Self.makeRecord(outcome: .failed, year: 2025, month: 6, reason: "boom")
        ])
        XCTAssertTrue(summary.safelyResolvedMonths().isEmpty,
                      "a month with only `.failed` records must stay unresolved")
    }

    func testSafelyResolvedMonths_safeWinsOverEarlierFailedForSameMonth() {
        // An earlier failure then a successful retry, both additive records for the same month.
        let summary = MigrationJournalSummary(records: [
            Self.makeRecord(outcome: .failed, year: 2025, month: 6, reason: "first attempt"),
            Self.makeRecord(outcome: .imported, year: 2025, month: 6, reason: nil)
        ])
        XCTAssertEqual(summary.safelyResolvedMonths(), [LibraryMonthKey(year: 2025, month: 6)],
                       "a safe terminal record supersedes an earlier failure for the same month")
    }

    func testSafelyResolvedMonths_failedMonthAlongsideResolvedMonth() {
        let summary = MigrationJournalSummary(records: [
            Self.makeRecord(outcome: .imported, year: 2025, month: 6, reason: nil),
            Self.makeRecord(outcome: .failed, year: 2025, month: 7, reason: "boom")
        ])
        XCTAssertEqual(summary.safelyResolvedMonths(), [LibraryMonthKey(year: 2025, month: 6)],
                       "only the safely-resolved month is suppressed; the failed-only month stays unresolved")
    }

    // MARK: - Helpers

    private static func makeRecord(
        outcome: MigrationJournalOutcome,
        year: Int,
        month: Int,
        migratedAssetCount: Int = 1,
        totalAssetCount: Int = 1,
        skippedAssetCount: Int = 0,
        reason: String?
    ) -> MigrationJournalRecord {
        MigrationJournalRecord(
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            writerID: "11111111-1111-1111-1111-aaaaaaaaaaaa",
            runID: "run-001",
            year: year,
            month: month,
            outcome: outcome,
            createdAtMs: 1_700_000_000_000,
            migratedAssetCount: migratedAssetCount,
            totalAssetCount: totalAssetCount,
            skippedAssetCount: skippedAssetCount,
            reason: reason
        )
    }

    private static func baseDict(outcome: String) -> [String: Any] {
        [
            "v": MigrationJournalRecord.currentVersion,
            "repo_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "writer_id": "w",
            "run_id": "run-001",
            "year": 2025,
            "month": 6,
            "outcome": outcome,
            "created_at_ms": 1_700_000_000_000,
            "migrated_asset_count": 1,
            "total_asset_count": 1,
            "skipped_asset_count": 0
        ]
    }

    private static func download(_ client: InMemoryRemoteStorageClient, path: String) async throws -> Data {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("journal-test-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        try await client.download(remotePath: path, localURL: temp)
        return try Data(contentsOf: temp)
    }
}

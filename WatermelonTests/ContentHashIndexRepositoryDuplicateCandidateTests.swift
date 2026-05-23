import GRDB
import XCTest
@testable import Watermelon

final class ContentHashIndexRepositoryDuplicateCandidateTests: XCTestCase {
    private var tempDir: URL!
    private var databaseManager: DatabaseManager!
    private var repository: ContentHashIndexRepository!

    override func setUpWithError() throws {
        tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        databaseManager = try DatabaseManager(databaseURL: tempDir.appendingPathComponent("db.sqlite"))
        repository = ContentHashIndexRepository(databaseManager: databaseManager)
    }

    override func tearDownWithError() throws {
        repository = nil
        databaseManager = nil
        try? FileManager.default.removeItem(at: tempDir)
    }

    func testDuplicateCandidateQueryReturnsFingerprintToAssetIDs() throws {
        let fpA = TestFixtures.fingerprint(0x0A)
        let fpB = TestFixtures.fingerprint(0x0B)
        try insertLocalAsset("asset-2", fingerprint: fpA)
        try insertLocalAsset("asset-1", fingerprint: fpA)
        try insertLocalAsset("asset-3", fingerprint: fpB)

        let candidates = try repository.fetchDuplicateIndexedAssetCandidates(
            minSelectionVersion: BackupAssetResourcePlanner.currentSelectionVersion
        )

        XCTAssertEqual(candidates.count, 1)
        XCTAssertEqual(candidates[0].assetFingerprint, fpA)
        XCTAssertEqual(candidates[0].rows.map(\.assetLocalIdentifier), ["asset-1", "asset-2"])
    }

    func testDuplicateCandidateQueryDoesNotReturnSingletons() throws {
        try insertLocalAsset("asset-1", fingerprint: TestFixtures.fingerprint(0x01))
        try insertLocalAsset("asset-2", fingerprint: TestFixtures.fingerprint(0x02))

        let candidates = try repository.fetchDuplicateIndexedAssetCandidates(
            minSelectionVersion: BackupAssetResourcePlanner.currentSelectionVersion
        )

        XCTAssertTrue(candidates.isEmpty)
    }

    func testDuplicateCandidateQueryFiltersSelectionVersionAndMissingSignatureBeforeGrouping() throws {
        let fp = TestFixtures.fingerprint(0x03)
        try insertLocalAsset("current", fingerprint: fp)
        try insertLocalAsset(
            "old-version",
            fingerprint: fp,
            selectionVersion: BackupAssetResourcePlanner.currentSelectionVersion - 1
        )
        try insertLocalAsset("missing-signature", fingerprint: fp, resourceSignature: nil)

        let candidates = try repository.fetchDuplicateIndexedAssetCandidates(
            minSelectionVersion: BackupAssetResourcePlanner.currentSelectionVersion
        )

        XCTAssertTrue(candidates.isEmpty)
    }

    func testDuplicateCandidateQueryKeepsAllRowsForLargeDuplicateGroup() throws {
        let fp = TestFixtures.fingerprint(0x04)
        for index in 0 ..< 50 {
            try insertLocalAsset(String(format: "asset-%02d", index), fingerprint: fp)
        }

        let candidates = try repository.fetchDuplicateIndexedAssetCandidates(
            minSelectionVersion: BackupAssetResourcePlanner.currentSelectionVersion
        )

        XCTAssertEqual(candidates.count, 1)
        XCTAssertEqual(candidates[0].rows.count, 50)
        XCTAssertEqual(candidates[0].rows.first?.assetLocalIdentifier, "asset-00")
        XCTAssertEqual(candidates[0].rows.last?.assetLocalIdentifier, "asset-49")
    }

    func testDuplicateCandidateQueryNilFingerprintNeverAppears() throws {
        try insertLocalAsset("asset-1", fingerprint: nil)
        try insertLocalAsset("asset-2", fingerprint: nil)

        let candidates = try repository.fetchDuplicateIndexedAssetCandidates(
            minSelectionVersion: BackupAssetResourcePlanner.currentSelectionVersion
        )

        XCTAssertTrue(candidates.isEmpty)
    }

    func testDuplicateCandidateQueryEmptyFingerprintCurrentBehavior() throws {
        try insertLocalAsset("asset-1", fingerprint: Data())
        try insertLocalAsset("asset-2", fingerprint: Data())

        let candidates = try repository.fetchDuplicateIndexedAssetCandidates(
            minSelectionVersion: BackupAssetResourcePlanner.currentSelectionVersion
        )

        XCTAssertEqual(candidates.count, 1)
        XCTAssertEqual(candidates[0].assetFingerprint, Data())
        XCTAssertEqual(candidates[0].rows.map(\.assetLocalIdentifier), ["asset-1", "asset-2"])
    }

    func testPotentiallyUsableIndexedAssetCountFiltersVersionAndSignature() throws {
        let fp = TestFixtures.fingerprint(0x05)
        try insertLocalAsset("usable", fingerprint: fp)
        try insertLocalAsset(
            "old-version",
            fingerprint: fp,
            selectionVersion: BackupAssetResourcePlanner.currentSelectionVersion - 1
        )
        try insertLocalAsset("missing-signature", fingerprint: fp, resourceSignature: nil)
        try insertLocalAsset("missing-fingerprint", fingerprint: nil)

        let count = try repository.fetchPotentiallyUsableIndexedAssetCount(
            minSelectionVersion: BackupAssetResourcePlanner.currentSelectionVersion
        )

        XCTAssertEqual(count, 1)
    }

    func testDuplicateCandidateExplainQueryPlanUsesCandidateIndex() throws {
        let fp = TestFixtures.fingerprint(0x06)
        try insertLocalAsset("asset-1", fingerprint: fp)
        try insertLocalAsset("asset-2", fingerprint: fp)

        let currentVersion = BackupAssetResourcePlanner.currentSelectionVersion
        let details = try databaseManager.read { db in
            try Row.fetchAll(
                db,
                sql: """
                EXPLAIN QUERY PLAN
                WITH candidate_fingerprints AS (
                    SELECT assetFingerprint
                    FROM local_assets
                    WHERE assetFingerprint IS NOT NULL
                      AND resourceSignature IS NOT NULL
                      AND selectionVersion >= ?
                    GROUP BY assetFingerprint
                    HAVING COUNT(*) > 1
                )
                SELECT
                    la.assetLocalIdentifier,
                    la.assetFingerprint,
                    la.updatedAt,
                    la.selectionVersion,
                    la.resourceSignature
                FROM local_assets la
                JOIN candidate_fingerprints cf
                  ON la.assetFingerprint = cf.assetFingerprint
                WHERE la.assetFingerprint IS NOT NULL
                  AND la.resourceSignature IS NOT NULL
                  AND la.selectionVersion >= ?
                ORDER BY la.assetFingerprint, la.assetLocalIdentifier
                """,
                arguments: [currentVersion, currentVersion]
            ).compactMap { $0["detail"] as String? }
        }

        XCTAssertTrue(
            details.contains { $0.contains("idx_local_assets_fingerprint_candidates") },
            details.joined(separator: "\n")
        )
    }

    private func insertLocalAsset(
        _ assetID: String,
        fingerprint: Data?,
        selectionVersion: Int = BackupAssetResourcePlanner.currentSelectionVersion,
        resourceSignature: Data? = Data([0xAA])
    ) throws {
        try databaseManager.write { db in
            try db.execute(
                sql: """
                INSERT INTO local_assets (
                    assetLocalIdentifier,
                    assetFingerprint,
                    resourceCount,
                    totalFileSizeBytes,
                    modificationDateMs,
                    updatedAt,
                    selectionVersion,
                    resourceSignature
                ) VALUES (?, ?, 1, 10, 0, ?, ?, ?)
                """,
                arguments: [
                    assetID,
                    fingerprint,
                    Date(),
                    selectionVersion,
                    resourceSignature
                ]
            )
        }
    }
}

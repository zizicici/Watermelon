import XCTest
@testable import Watermelon

final class ContentHashIndexRepositoryTests: XCTestCase {
    private var tempDBURL: URL!
    private var repository: ContentHashIndexRepository!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        let databaseManager = try DatabaseManager(databaseURL: tempDBURL)
        repository = ContentHashIndexRepository(databaseManager: databaseManager)
    }

    override func tearDownWithError() throws {
        repository = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    private func upsert(id: String, fingerprint: Data, modificationDateMs: Int64? = nil) throws {
        try repository.upsertAssetHashSnapshot(
            assetLocalIdentifier: id,
            assetFingerprint: fingerprint,
            resources: [],
            totalFileSizeBytes: 1,
            modificationDateMs: modificationDateMs
        )
    }

    // The download dedup looks up indexed rows by fingerprint (to find a present-but-hidden local copy of a
    // remote-only candidate). Only matching fingerprints come back.
    func testFetchIndexedRowsForFingerprintsReturnsOnlyMatches() throws {
        let fpA = Data([1, 2, 3, 4])
        let fpB = Data([5, 6, 7, 8])
        let fpC = Data([9, 9, 9, 9])
        try upsert(id: "a", fingerprint: fpA)
        try upsert(id: "b", fingerprint: fpB)

        let rows = try repository.fetchIndexedRows(forFingerprints: [fpA, fpC])
        XCTAssertEqual(rows.map(\.assetLocalIdentifier), ["a"])
        XCTAssertEqual(rows.first?.assetFingerprint, fpA)
    }

    // Two local assets sharing a fingerprint (e.g. an original + a hidden copy) both come back, so the dedup
    // can find any present-and-hidden holder of a remote-only fingerprint.
    func testFetchIndexedRowsForFingerprintsReturnsAllHoldersOfAFingerprint() throws {
        let fp = Data([42])
        try upsert(id: "visible", fingerprint: fp)
        try upsert(id: "hidden", fingerprint: fp)

        let rows = try repository.fetchIndexedRows(forFingerprints: [fp])
        XCTAssertEqual(Set(rows.map(\.assetLocalIdentifier)), ["visible", "hidden"])
    }

    func testFetchIndexedRowsForEmptyFingerprintsIsEmpty() throws {
        try upsert(id: "a", fingerprint: Data([1]))
        XCTAssertTrue(try repository.fetchIndexedRows(forFingerprints: []).isEmpty)
    }
}

import Photos
import XCTest
@testable import Watermelon

final class DuplicatesCandidateComputationTests: XCTestCase {
    func testComputeDataAllowsFastAssetCountButForbidsFullLibraryMaterialization() async {
        let fp = TestFixtures.fingerprint(0x10)
        let signature = Data([0x01])
        let repository = FakeDuplicateRepository(
            rawIndexedCount: 2,
            candidates: [
                candidate(fp, rows: [
                    row("asset-1", fingerprint: fp, resourceSignature: signature),
                    row("asset-2", fingerprint: fp, resourceSignature: signature)
                ])
            ]
        )
        let provider = FakeDuplicatePhotoLibraryProvider(
            totalCount: 50_000,
            snapshots: [
                snapshot("asset-1", signature: signature),
                snapshot("asset-2", signature: signature)
            ]
        )

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertEqual(provider.assetCountQueries, [.allAssets])
        XCTAssertTrue(provider.collectAssetIDsQueries.isEmpty)
        XCTAssertTrue(provider.fetchResultsQueries.isEmpty)
        XCTAssertEqual(provider.fetchTrustSnapshotCalls, [Set(["asset-1", "asset-2"])])
        XCTAssertEqual(data.groups.count, 1)
        XCTAssertEqual(data.groups[0].entries.map(\.assetLocalIdentifier), ["asset-1", "asset-2"])
    }

    func testComputeDataDropsGroupWhenValidationLeavesSingleton() async {
        let fp = TestFixtures.fingerprint(0x11)
        let signature = Data([0x02])
        let repository = FakeDuplicateRepository(
            rawIndexedCount: 3,
            candidates: [
                candidate(fp, rows: [
                    row("trusted", fingerprint: fp, resourceSignature: signature),
                    row("missing", fingerprint: fp, resourceSignature: signature),
                    row("stale", fingerprint: fp, updatedAt: Date(timeIntervalSince1970: 100), resourceSignature: signature)
                ])
            ]
        )
        let provider = FakeDuplicatePhotoLibraryProvider(
            totalCount: 3,
            snapshots: [
                snapshot("trusted", signature: signature),
                snapshot("stale", modificationDate: Date(timeIntervalSince1970: 200), signature: signature)
            ]
        )

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertTrue(data.groups.isEmpty)
        XCTAssertTrue(data.indexCoverageWarning)
    }

    func testComputeDataDropsDeletedOrPermissionInvisibleRows() async {
        let fp = TestFixtures.fingerprint(0x12)
        let signature = Data([0x03])
        let repository = FakeDuplicateRepository(
            rawIndexedCount: 2,
            candidates: [
                candidate(fp, rows: [
                    row("visible", fingerprint: fp, resourceSignature: signature),
                    row("missing", fingerprint: fp, resourceSignature: signature)
                ])
            ]
        )
        let provider = FakeDuplicatePhotoLibraryProvider(
            totalCount: 2,
            snapshots: [snapshot("visible", signature: signature)]
        )

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertTrue(data.groups.isEmpty)
        XCTAssertTrue(data.indexCoverageWarning)
    }

    func testComputeDataPreservesTrustChecksEvenWhenRepositoryReturnsBadRows() async {
        let fp = TestFixtures.fingerprint(0x13)
        let signature = Data([0x04])
        let repository = FakeDuplicateRepository(
            rawIndexedCount: 5,
            candidates: [
                candidate(fp, rows: [
                    row("trusted", fingerprint: fp, resourceSignature: signature),
                    row(
                        "old-version",
                        fingerprint: fp,
                        selectionVersion: BackupAssetResourcePlanner.currentSelectionVersion - 1,
                        resourceSignature: signature
                    ),
                    row("nil-signature", fingerprint: fp, resourceSignature: nil),
                    row("stale-mtime", fingerprint: fp, updatedAt: Date(timeIntervalSince1970: 100), resourceSignature: signature),
                    row("signature-mismatch", fingerprint: fp, resourceSignature: Data([0xFF]))
                ])
            ]
        )
        let provider = FakeDuplicatePhotoLibraryProvider(
            totalCount: 5,
            snapshots: [
                snapshot("trusted", signature: signature),
                snapshot("old-version", signature: signature),
                snapshot("nil-signature", signature: signature),
                snapshot("stale-mtime", modificationDate: Date(timeIntervalSince1970: 200), signature: signature),
                snapshot("signature-mismatch", signature: signature)
            ]
        )

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertTrue(data.groups.isEmpty)
        XCTAssertTrue(data.indexCoverageWarning)
    }

    func testComputeDataBuildsSortedTrustedGroup() async {
        let fp = TestFixtures.fingerprint(0x14)
        let signature = Data([0x05])
        let repository = FakeDuplicateRepository(
            rawIndexedCount: 4,
            candidates: [
                candidate(fp, rows: [
                    row("nil-date", fingerprint: fp, resourceSignature: signature),
                    row("same-b", fingerprint: fp, resourceSignature: signature),
                    row("early", fingerprint: fp, resourceSignature: signature),
                    row("same-a", fingerprint: fp, resourceSignature: signature)
                ])
            ]
        )
        let sameDate = Date(timeIntervalSince1970: 300)
        let provider = FakeDuplicatePhotoLibraryProvider(
            totalCount: 4,
            snapshots: [
                snapshot("nil-date", creationDate: nil, signature: signature),
                snapshot("same-b", creationDate: sameDate, signature: signature),
                snapshot("early", creationDate: Date(timeIntervalSince1970: 100), signature: signature),
                snapshot("same-a", creationDate: sameDate, signature: signature)
            ]
        )

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertEqual(data.groups.count, 1)
        XCTAssertEqual(
            data.groups[0].entries.map(\.assetLocalIdentifier),
            ["early", "same-a", "same-b", "nil-date"]
        )
    }

    func testComputeDataGroupOrderingByFingerprint() async {
        let fpA = TestFixtures.fingerprint(0x01)
        let fpB = TestFixtures.fingerprint(0x02)
        let signature = Data([0x06])
        let repository = FakeDuplicateRepository(
            rawIndexedCount: 4,
            candidates: [
                candidate(fpB, rows: [
                    row("b1", fingerprint: fpB, resourceSignature: signature),
                    row("b2", fingerprint: fpB, resourceSignature: signature)
                ]),
                candidate(fpA, rows: [
                    row("a1", fingerprint: fpA, resourceSignature: signature),
                    row("a2", fingerprint: fpA, resourceSignature: signature)
                ])
            ]
        )
        let provider = FakeDuplicatePhotoLibraryProvider(
            totalCount: 4,
            snapshots: [
                snapshot("a1", signature: signature),
                snapshot("a2", signature: signature),
                snapshot("b1", signature: signature),
                snapshot("b2", signature: signature)
            ]
        )

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertEqual(data.groups.map(\.fingerprint), [fpA, fpB])
    }

    func testComputeDataShowsGateForUnderIndexedFastCount() async {
        let repository = FakeDuplicateRepository(rawIndexedCount: 9, candidates: [])
        let provider = FakeDuplicatePhotoLibraryProvider(totalCount: 10, snapshots: [])

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertEqual(data.scopeIndexed, 9)
        XCTAssertTrue(data.indexCoverageWarning)
    }

    func testComputeDataShowsGateForOverIndexedFastCount() async {
        let repository = FakeDuplicateRepository(rawIndexedCount: 12, candidates: [])
        let provider = FakeDuplicatePhotoLibraryProvider(totalCount: 10, snapshots: [])

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertEqual(data.scopeIndexed, 10)
        XCTAssertTrue(data.indexCoverageWarning)
    }

    func testComputeDataShowsGateWhenCandidateTrustDropsRows() async {
        let fp = TestFixtures.fingerprint(0x15)
        let signature = Data([0x07])
        let repository = FakeDuplicateRepository(
            rawIndexedCount: 2,
            candidates: [
                candidate(fp, rows: [
                    row("visible", fingerprint: fp, resourceSignature: signature),
                    row("missing", fingerprint: fp, resourceSignature: signature)
                ])
            ]
        )
        let provider = FakeDuplicatePhotoLibraryProvider(
            totalCount: 2,
            snapshots: [snapshot("visible", signature: signature)]
        )

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertTrue(data.indexCoverageWarning)
    }

    func testComputeDataDoesNotCreateGroupForUndetectableStaleSingleton() async {
        let repository = FakeDuplicateRepository(rawIndexedCount: 1, candidates: [])
        let provider = FakeDuplicatePhotoLibraryProvider(totalCount: 1, snapshots: [])

        let data = await DuplicatesViewController.computeData(
            repository: repository,
            photoLibraryProvider: provider
        )

        XCTAssertTrue(data.groups.isEmpty)
        XCTAssertFalse(data.indexCoverageWarning)
    }
}

private final class FakeDuplicateRepository: DuplicateCandidateRepository, @unchecked Sendable {
    let rawIndexedCount: Int
    let candidates: [DuplicateIndexedAssetCandidate]
    var validRows: [String: IndexedAssetRow] = [:]

    init(rawIndexedCount: Int, candidates: [DuplicateIndexedAssetCandidate]) {
        self.rawIndexedCount = rawIndexedCount
        self.candidates = candidates
    }

    func fetchPotentiallyUsableIndexedAssetCount(minSelectionVersion: Int) throws -> Int {
        rawIndexedCount
    }

    func fetchDuplicateIndexedAssetCandidates(minSelectionVersion: Int) throws -> [DuplicateIndexedAssetCandidate] {
        candidates
    }

    func fetchValidIndexedRows(assetIDs: Set<String>) throws -> [String: IndexedAssetRow] {
        validRows.filter { assetIDs.contains($0.key) }
    }
}

private final class FakeDuplicatePhotoLibraryProvider: DuplicatePhotoLibraryProvider, @unchecked Sendable {
    let totalCount: Int
    let snapshotsByID: [String: IndexedAssetTrustSnapshot]
    var assetCountQueries: [PhotoLibraryQuery] = []
    var fetchTrustSnapshotCalls: [Set<String>] = []
    var collectAssetIDsQueries: [PhotoLibraryQuery] = []
    var fetchResultsQueries: [PhotoLibraryQuery] = []

    init(totalCount: Int, snapshots: [IndexedAssetTrustSnapshot]) {
        self.totalCount = totalCount
        self.snapshotsByID = Dictionary(uniqueKeysWithValues: snapshots.map { ($0.localIdentifier, $0) })
    }

    func assetCount(query: PhotoLibraryQuery) -> Int {
        assetCountQueries.append(query)
        return totalCount
    }

    func fetchTrustSnapshots(localIdentifiers: Set<String>) -> [IndexedAssetTrustSnapshot] {
        fetchTrustSnapshotCalls.append(localIdentifiers)
        return localIdentifiers.compactMap { snapshotsByID[$0] }
    }

    func collectAssetIDs(query: PhotoLibraryQuery) -> Set<String> {
        collectAssetIDsQueries.append(query)
        return []
    }

    func fetchResults(query: PhotoLibraryQuery) -> [PHFetchResult<PHAsset>] {
        fetchResultsQueries.append(query)
        return []
    }
}

private func candidate(
    _ fingerprint: Data,
    rows: [DuplicateIndexedAssetRow]
) -> DuplicateIndexedAssetCandidate {
    DuplicateIndexedAssetCandidate(assetFingerprint: fingerprint, rows: rows)
}

private func row(
    _ assetID: String,
    fingerprint: Data,
    updatedAt: Date = Date(timeIntervalSince1970: 150),
    selectionVersion: Int = BackupAssetResourcePlanner.currentSelectionVersion,
    resourceSignature: Data?
) -> DuplicateIndexedAssetRow {
    DuplicateIndexedAssetRow(
        assetLocalIdentifier: assetID,
        assetFingerprint: fingerprint,
        updatedAt: updatedAt,
        selectionVersion: selectionVersion,
        resourceSignature: resourceSignature
    )
}

private func snapshot(
    _ assetID: String,
    creationDate: Date? = Date(timeIntervalSince1970: 100),
    modificationDate: Date? = Date(timeIntervalSince1970: 100),
    mediaType: PHAssetMediaType = .image,
    signature: Data
) -> IndexedAssetTrustSnapshot {
    IndexedAssetTrustSnapshot(
        localIdentifier: assetID,
        creationDate: creationDate,
        modificationDate: modificationDate,
        mediaType: mediaType,
        currentResourceSignature: signature
    )
}

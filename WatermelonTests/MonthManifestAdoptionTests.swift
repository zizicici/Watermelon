import GRDB
import XCTest
@testable import Watermelon

final class MonthManifestAdoptionTests: XCTestCase {
    private let basePath = "/photos"
    private let month = LibraryMonthKey(year: 2024, month: 3)

    private func makeStore() throws -> (MonthManifestStore, DatabaseQueue) {
        let localURL = MonthManifestStore.makeLocalManifestURL(
            year: month.year,
            month: month.month
        )
        try? FileManager.default.removeItem(at: localURL)
        let queue = try DatabaseQueue(path: localURL.path)
        try MonthManifestStore.migrate(queue)
        let store = MonthManifestStore(
            client: InMemoryRemoteStorageClient(),
            basePath: basePath,
            year: month.year,
            month: month.month,
            localManifestURL: localURL,
            dbQueue: queue,
            remoteFilesByName: [:],
            dirty: false,
            layout: .lite,
            liteWriteOwnership: .uniform({})
        )
        return (store, queue)
    }

    private func resource(
        _ name: String,
        hashByte: UInt8,
        role: Int
    ) -> LeftoverAdoptionResource {
        LeftoverAdoptionResource(
            file: LeftoverFile(
                month: month,
                fileName: name,
                path: "\(basePath)/2024/03/\(name)",
                size: 100,
                modificationDateMs: 1_700_000_000_000
            ),
            contentHash: Data([hashByte]),
            fileSize: 100,
            role: role,
            creationDateMs: 1_700_000_000_000
        )
    }

    func testAdoptLivePhotoWritesResourcesAssetAndLinksTogether() throws {
        let (store, _) = try makeStore()
        let candidate = LeftoverAdoptionCandidate(
            resources: [
                resource("IMG_0001.HEIC", hashByte: 1, role: ResourceTypeCode.photo),
                resource("IMG_0001.MOV", hashByte: 2, role: ResourceTypeCode.pairedVideo)
            ]
        )

        try store.adoptLeftoverAsset(candidate, backedUpAtMs: 2_000)
        let snapshot = store.unsortedSnapshot()

        XCTAssertEqual(snapshot.resources.count, 2)
        XCTAssertEqual(snapshot.assets.first?.resourceCount, 2)
        XCTAssertEqual(snapshot.assets.first?.totalFileSizeBytes, 200)
        XCTAssertEqual(Set(snapshot.links.map(\.role)), Set([
            ResourceTypeCode.photo,
            ResourceTypeCode.pairedVideo
        ]))
        XCTAssertTrue(store.containsAssetFingerprint(candidate.assetFingerprint))
    }

    func testAdoptedLivePhotoIsCompleteForMainDownloadFlow() throws {
        let (store, _) = try makeStore()
        let candidate = LeftoverAdoptionCandidate(
            resources: [
                resource("IMG_0001.HEIC", hashByte: 1, role: ResourceTypeCode.photo),
                resource("IMG_0001.MOV", hashByte: 2, role: ResourceTypeCode.pairedVideo)
            ]
        )

        try store.adoptLeftoverAsset(candidate, backedUpAtMs: 2_000)
        let snapshot = store.unsortedSnapshot()
        let items = HomeAlbumMatching.buildRemoteItems(
            assets: snapshot.assets,
            resources: snapshot.resources,
            links: snapshot.links
        )

        let item = try XCTUnwrap(items.first)
        XCTAssertEqual(items.count, 1)
        XCTAssertEqual(item.mediaKind, .livePhoto)
        XCTAssertFalse(item.isIncomplete)
        XCTAssertEqual(item.assetFingerprint, candidate.assetFingerprint)
        XCTAssertEqual(item.instances.map(\.role), [
            ResourceTypeCode.photo,
            ResourceTypeCode.pairedVideo
        ])
        XCTAssertTrue(item.instances.allSatisfy(RestoreService.isSafeRestoreResource))
        XCTAssertEqual(RestoreImportPlan.normalize(item.instances), item.instances)
    }

    func testDatabaseFailureRollsBackAllCandidateRows() throws {
        let (store, queue) = try makeStore()
        try queue.write { db in
            try db.execute(
                sql: """
                INSERT INTO resources (
                    fileName, contentHash, fileSize, resourceType, creationDateMs, backedUpAtMs
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                arguments: ["already.jpg", Data([9]), 100, ResourceTypeCode.photo, nil, 0]
            )
        }
        let candidate = LeftoverAdoptionCandidate(
            resources: [
                resource("new.HEIC", hashByte: 1, role: ResourceTypeCode.photo),
                resource("already.jpg", hashByte: 9, role: ResourceTypeCode.pairedVideo)
            ]
        )

        XCTAssertThrowsError(try store.adoptLeftoverAsset(candidate))

        let persistedNames = try queue.read { db in
            try String.fetchAll(db, sql: "SELECT fileName FROM resources ORDER BY fileName")
        }
        XCTAssertEqual(persistedNames, ["already.jpg"])
        XCTAssertTrue(store.unsortedSnapshot().resources.isEmpty)
        XCTAssertFalse(store.containsAssetFingerprint(candidate.assetFingerprint))
    }

    func testRejectsInvalidResourceShape() throws {
        let (store, _) = try makeStore()
        let candidate = LeftoverAdoptionCandidate(
            resources: [
                resource("IMG_0001.MOV", hashByte: 1, role: ResourceTypeCode.pairedVideo)
            ]
        )

        XCTAssertThrowsError(try store.adoptLeftoverAsset(candidate))
        XCTAssertTrue(store.unsortedSnapshot().resources.isEmpty)
    }

    func testRejectsUnsafeManifestFileName() throws {
        let (store, _) = try makeStore()
        let candidate = LeftoverAdoptionCandidate(
            resources: [resource("folder/photo.jpg", hashByte: 1, role: ResourceTypeCode.photo)]
        )

        XCTAssertThrowsError(try store.adoptLeftoverAsset(candidate))
        XCTAssertTrue(store.unsortedSnapshot().resources.isEmpty)
    }

}

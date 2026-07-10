import XCTest
@testable import Watermelon

// Pins incremental upsertResource equal to the wholesale replaceMonth rebuild.
final class RemoteSnapshotCacheUpsertTests: XCTestCase {
    private let year = 2024
    private let month = 3
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: month) }

    private func resource(_ name: String, hashByte: UInt8, size: Int64, storedSize: Int64? = nil) -> RemoteManifestResource {
        TestFixtures.remoteResource(
            year: year,
            month: month,
            contentHash: Data([hashByte]),
            fileSize: size,
            fileName: name,
            storedFileSize: storedSize
        )
    }

    func testIncrementalUpsertResourceMatchesReplaceMonth() {
        // Two resources share a content hash (distinct ids) to exercise the dedup set.
        let resources = [
            resource("a.jpg", hashByte: 0x01, size: 100),
            resource("b.jpg", hashByte: 0x02, size: 200),
            resource("c.jpg", hashByte: 0x01, size: 100),
        ]
        let presentHash = Data([0x01])
        let completeFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: presentHash)]
        )
        let completeAsset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: completeFingerprint)
        let completeLink = TestFixtures.remoteLink(
            year: year,
            month: month,
            assetFingerprint: completeFingerprint,
            resourceHash: presentHash,
            resourceFileName: "a.jpg"
        )
        let missingAsset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: Data([0xBB]))
        let missingLink = TestFixtures.remoteLink(year: year, month: month, assetFingerprint: Data([0xBB]), resourceHash: Data([0x09]))

        let incremental = RemoteLibrarySnapshotCache()
        for r in resources { incremental.upsertResource(r) }
        incremental.upsertAsset(completeAsset, links: [completeLink])
        incremental.upsertAsset(missingAsset, links: [missingLink])

        let wholesale = RemoteLibrarySnapshotCache()
        wholesale.replaceMonth(monthKey, resources: resources, assets: [], assetResourceLinks: [])
        wholesale.upsertAsset(completeAsset, links: [completeLink])
        wholesale.upsertAsset(missingAsset, links: [missingLink])

        let di = incremental.healthDigest()
        let dw = wholesale.healthDigest()

        XCTAssertEqual(di.totalResources, 3)
        XCTAssertEqual(di.totalResources, dw.totalResources)
        XCTAssertEqual(di.totalSizeBytes, 400)
        XCTAssertEqual(di.totalSizeBytes, dw.totalSizeBytes)
        // The complete asset is complete only if its hash is in the maintained set.
        XCTAssertEqual(Set(di.incompleteAssets.map(\.id)), Set(dw.incompleteAssets.map(\.id)))
        XCTAssertEqual(Set(di.incompleteAssets.map(\.id)), [Data([0xBB])])
        XCTAssertEqual(incremental.counts().resourceCount, wholesale.counts().resourceCount)
        XCTAssertEqual(Set(incremental.current().resources), Set(wholesale.current().resources))
    }

    func testUpsertResourceBumpsRevisionOnlyOnChange() {
        let cache = RemoteLibrarySnapshotCache()
        let base = cache.state(since: nil)

        let r1 = resource("a.jpg", hashByte: 0x01, size: 100)
        cache.upsertResource(r1)
        let afterAdd = cache.state(since: base.revision)
        XCTAssertGreaterThan(afterAdd.revision, base.revision)
        XCTAssertTrue(afterAdd.monthDeltas.contains { $0.month == monthKey })

        // Identical re-upsert is a no-op: no revision bump, empty delta.
        cache.upsertResource(r1)
        let afterNoop = cache.state(since: afterAdd.revision)
        XCTAssertEqual(afterNoop.revision, afterAdd.revision)
        XCTAssertTrue(afterNoop.monthDeltas.isEmpty)
    }

    func testHealthDigestUsesStoredFileSizeWhenAvailable() {
        let resources = [
            resource("plain.jpg", hashByte: 0x11, size: 100),
            resource("encrypted.wmenc", hashByte: 0x12, size: 100, storedSize: 160)
        ]

        let incremental = RemoteLibrarySnapshotCache()
        for r in resources { incremental.upsertResource(r) }

        let wholesale = RemoteLibrarySnapshotCache()
        wholesale.replaceMonth(monthKey, resources: resources, assets: [], assetResourceLinks: [])

        XCTAssertEqual(incremental.healthDigest().totalSizeBytes, 260)
        XCTAssertEqual(wholesale.healthDigest().totalSizeBytes, 260)
    }
}

import XCTest
import GRDB
@testable import Watermelon

// existingCollisionKeys() (the maintained fold) must stay equal to a fresh fold of existingFileNames(),
// so the upload path can read the cache instead of re-folding every name per resource (was O(N^2)/month).
final class CollisionKeyCacheTests: XCTestCase {
    private let basePath = "/photos"
    private let year = 2024
    private let month = 7

    private func makeStore() throws -> MonthManifestStore {
        let localURL = MonthManifestStore.makeLocalManifestURL(year: year, month: month)
        try? FileManager.default.removeItem(at: localURL)
        let queue = try DatabaseQueue(path: localURL.path)
        try MonthManifestStore.migrate(queue)
        return MonthManifestStore(
            client: InMemoryRemoteStorageClient(), basePath: basePath, year: year, month: month,
            localManifestURL: localURL, dbQueue: queue, remoteFilesByName: [:], dirty: false,
            layout: .lite, liteWriteOwnership: {}
        )
    }

    private func resource(_ fileName: String, hashByte: UInt8) -> RemoteManifestResource {
        RemoteManifestResource(
            year: year, month: month, fileName: fileName,
            contentHash: Data([hashByte]), fileSize: 100 + Int64(hashByte),
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
    }

    private func assertCacheMatchesFreshFold(_ store: MonthManifestStore, _ message: String) {
        XCTAssertEqual(
            store.existingCollisionKeys(),
            RemoteFileNaming.collisionKeySet(from: store.existingFileNames()),
            message
        )
    }

    func testCacheStaysConsistentAcrossUpsertAndMarkRemote() throws {
        let store = try makeStore()

        assertCacheMatchesFreshFold(store, "empty store")

        let names = ["IMG_1.HEIC", "Café.jpg", "naïve_PHOTO.JPG", "vidéo.mov", "plain.png"]
        for (i, name) in names.enumerated() {
            // Mirror the upload loop: read the cache, then upsert + mark.
            _ = store.existingCollisionKeys()
            _ = try store.upsertResource(resource(name, hashByte: UInt8(i + 1)))
            store.markRemoteFile(name: name, size: 100)
            assertCacheMatchesFreshFold(store, "after inserting \(name)")
        }

        // Folded names (case/diacritic) collide in the cache.
        let keys = store.existingCollisionKeys()
        XCTAssertTrue(keys.contains(RemoteFileNaming.collisionKey(for: "img_1.heic")))
        XCTAssertTrue(keys.contains(RemoteFileNaming.collisionKey(for: "cafe.jpg")))
        XCTAssertEqual(keys.count, names.count)
    }

    func testCacheConsistentAfterRemoval() throws {
        let store = try makeStore()
        let kept = resource("keep.heic", hashByte: 1)
        let dropped = resource("drop.heic", hashByte: 2)
        _ = try store.upsertResource(kept)
        _ = try store.upsertResource(dropped)
        // No markRemoteFile: a marked name persists as a remote file and would not leave the set on removal.
        _ = store.existingCollisionKeys()

        _ = try store.cleanupMissingResources(missingHashes: [dropped.contentHash])

        assertCacheMatchesFreshFold(store, "after removal")
        XCTAssertFalse(
            store.existingFileNames().contains("drop.heic"),
            "removed resource still present"
        )
    }
}

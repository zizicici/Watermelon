import XCTest
@testable import Watermelon

// Pins the cached-original / direct-read manifest-hash decision: divergent or unreadable bytes are an
// evictable mismatch; only a cancelled read must not evict (a scrolled-away cell's cancelled hash
// would otherwise thrash the cache).
final class RemoteOriginalVerificationTests: XCTestCase {
    private func makeTempFile(_ data: Data) throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try data.write(to: url)
        addTeardownBlock { try? FileManager.default.removeItem(at: url) }
        return url
    }

    func testMatchAgainstManifestHash() throws {
        let url = try makeTempFile(Data("original-bytes".utf8))
        let expected = try AssetProcessor.contentHash(of: url)
        XCTAssertEqual(RemoteThumbnailService.contentHashCheck(at: url, expectedContentHash: expected), .match)
    }

    func testMismatchAgainstManifestHash() throws {
        let url = try makeTempFile(Data("replaced-bytes".utf8))
        let other = try makeTempFile(Data("original-bytes".utf8))
        let expected = try AssetProcessor.contentHash(of: other)
        XCTAssertEqual(RemoteThumbnailService.contentHashCheck(at: url, expectedContentHash: expected), .mismatch)
    }

    func testUnreadableFileCountsAsMismatch() {
        let missing = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let expected = Data(repeating: 0xAB, count: 32)
        XCTAssertEqual(RemoteThumbnailService.contentHashCheck(at: missing, expectedContentHash: expected), .mismatch)
    }

    func testRemoveEvictsSingleEntry() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("RemoteOriginalVerificationTests-\(UUID().uuidString)", isDirectory: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }
        let cache = OriginalPhotoCache(root: root)
        _ = cache.store(movingFrom: try makeTempFile(Data([1])), forKey: "aa-keep")
        _ = cache.store(movingFrom: try makeTempFile(Data([2])), forKey: "ab-evict")
        cache.remove(forKey: "ab-evict")
        XCTAssertNil(cache.url(forKey: "ab-evict"))
        XCTAssertNotNil(cache.url(forKey: "aa-keep"))
    }

    func testVerifiedLatchIsKeyedByHashNotJustKey() {
        // Same-fingerprint twin records share one cache key but can carry different manifest hashes —
        // bytes verified for one record's hash must never be trusted for the other's.
        let latch = RemoteThumbnailService.VerifiedOriginalLatch()
        let h1 = Data(repeating: 0x01, count: 32)
        let h2 = Data(repeating: 0x02, count: 32)
        latch.mark(key: "shared-fp-key", contentHash: h2)
        XCTAssertTrue(latch.isVerified(key: "shared-fp-key", contentHash: h2))
        XCTAssertFalse(latch.isVerified(key: "shared-fp-key", contentHash: h1))
        latch.clear(key: "shared-fp-key")
        XCTAssertFalse(latch.isVerified(key: "shared-fp-key", contentHash: h2))
    }

    func testStoreCollisionKeepsResidentAndReportsIt() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("RemoteOriginalVerificationTests-\(UUID().uuidString)", isDirectory: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }
        let cache = OriginalPhotoCache(root: root)
        let second = try makeTempFile(Data([2]))
        let stored = try XCTUnwrap(cache.store(movingFrom: try makeTempFile(Data([1])), forKey: "aa-key"))
        XCTAssertTrue(stored.storedIncoming)
        let collided = try XCTUnwrap(cache.store(movingFrom: second, forKey: "aa-key"))
        XCTAssertFalse(collided.storedIncoming, "a collision must be reported so the caller never latches bytes it didn't hash")
        XCTAssertEqual(try Data(contentsOf: collided.url), Data([1]), "the resident entry is kept")
        XCTAssertTrue(FileManager.default.fileExists(atPath: second.path), "the incoming file is left to the caller")
    }
}

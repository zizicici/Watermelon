import XCTest
@testable import Watermelon

final class OriginalPhotoCacheTests: XCTestCase {
    private var root: URL!
    private var cache: OriginalPhotoCache!

    override func setUpWithError() throws {
        root = FileManager.default.temporaryDirectory
            .appendingPathComponent("OriginalPhotoCacheTests-\(UUID().uuidString)", isDirectory: true)
        cache = OriginalPhotoCache(root: root)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: root)
    }

    private func key(_ byte: UInt8) -> String { String(format: "%02xabcd", byte) }

    private func makeTempFile(bytes: Int) throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x7, count: bytes).write(to: url)
        return url
    }

    func testStoreMovesFileAndHitsByKey() throws {
        let k = key(0x10)
        XCTAssertNil(cache.url(forKey: k))
        let src = try makeTempFile(bytes: 128)
        let stored = cache.store(movingFrom: src, forKey: k)
        XCTAssertNotNil(stored)
        XCTAssertFalse(FileManager.default.fileExists(atPath: src.path), "source should be moved, not copied")
        XCTAssertNotNil(cache.url(forKey: k))
        XCTAssertNil(cache.url(forKey: key(0x99)))
    }

    func testPhotoAndVideoKeysDoNotCollide() throws {
        let hex = "abcdef"
        _ = cache.store(movingFrom: try makeTempFile(bytes: 100), forKey: OriginalPhotoCache.photoKey(fingerprintHex: hex))
        _ = cache.store(movingFrom: try makeTempFile(bytes: 200), forKey: OriginalPhotoCache.videoKey(fingerprintHex: hex))
        XCTAssertNotNil(cache.url(forKey: OriginalPhotoCache.photoKey(fingerprintHex: hex)))
        XCTAssertNotNil(cache.url(forKey: OriginalPhotoCache.videoKey(fingerprintHex: hex)))
        XCTAssertEqual(cache.diskSizeBytes(), 300)
    }

    func testStoreKeepsExistingEntryOnCollision() throws {
        let k = key(0x11)
        _ = cache.store(movingFrom: try makeTempFile(bytes: 100), forKey: k)
        let second = try makeTempFile(bytes: 100)
        XCTAssertNotNil(cache.store(movingFrom: second, forKey: k))
        XCTAssertFalse(FileManager.default.fileExists(atPath: second.path), "incoming duplicate should be discarded")
        XCTAssertEqual(cache.diskSizeBytes(), 100)
    }

    func testDiskSizeBytesSumsEntries() throws {
        XCTAssertEqual(cache.diskSizeBytes(), 0)
        _ = cache.store(movingFrom: try makeTempFile(bytes: 200), forKey: key(0x01))
        _ = cache.store(movingFrom: try makeTempFile(bytes: 300), forKey: key(0x02))
        XCTAssertEqual(cache.diskSizeBytes(), 500)
    }

    func testEnforceCapEvictsLeastRecentlyUsed() throws {
        let older = key(0x01)
        let newer = key(0x02)
        let olderURL = try XCTUnwrap(cache.store(movingFrom: try makeTempFile(bytes: 400), forKey: older))
        let newerURL = try XCTUnwrap(cache.store(movingFrom: try makeTempFile(bytes: 400), forKey: newer))
        try FileManager.default.setAttributes([.modificationDate: Date(timeIntervalSince1970: 1000)], ofItemAtPath: olderURL.path)
        try FileManager.default.setAttributes([.modificationDate: Date(timeIntervalSince1970: 2000)], ofItemAtPath: newerURL.path)
        cache.enforceCap(maxBytes: 500) // only one 400-byte entry fits
        XCTAssertNil(cache.url(forKey: older), "oldest entry should be evicted")
        XCTAssertNotNil(cache.url(forKey: newer))
    }

    func testEnforceCapNoOpWhenUnderLimit() throws {
        _ = cache.store(movingFrom: try makeTempFile(bytes: 100), forKey: key(0x03))
        cache.enforceCap(maxBytes: 1_000)
        XCTAssertEqual(cache.diskSizeBytes(), 100)
    }

    func testClearRemovesEverything() throws {
        _ = cache.store(movingFrom: try makeTempFile(bytes: 100), forKey: key(0x05))
        cache.clear()
        XCTAssertEqual(cache.diskSizeBytes(), 0)
        XCTAssertNil(cache.url(forKey: key(0x05)))
    }
}

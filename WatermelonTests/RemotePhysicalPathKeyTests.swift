import XCTest
@testable import Watermelon

final class RemotePhysicalPathKeyTests: XCTestCase {
    // "café.jpg": U+00E9 (NFC) vs e + U+0301 (NFD). Byte-distinct, Swift-String-equal.
    private let nfc = "caf\u{00E9}.jpg"
    private let nfd = "cafe\u{0301}.jpg"

    func testNFCAndNFDAreDistinctByBytes() {
        // Premise guard: the two spellings compare equal as Swift Strings.
        XCTAssertEqual(nfc, nfd)
        XCTAssertNotEqual(Array(nfc.utf8), Array(nfd.utf8))

        let keyNFC = RemotePhysicalPathKey(nfc)
        let keyNFD = RemotePhysicalPathKey(nfd)
        XCTAssertNotEqual(keyNFC, keyNFD, "byte-distinct paths must be distinct keys")
        XCTAssertNotEqual(keyNFC.hashValue, keyNFD.hashValue)
    }

    func testIdenticalStringsAreEqualKeys() {
        let a = RemotePhysicalPathKey("2026/01/photo.jpg")
        let b = RemotePhysicalPathKey("2026/01/photo.jpg")
        XCTAssertEqual(a, b)
        XCTAssertEqual(a.hashValue, b.hashValue)
    }

    func testNonEquivalentPathsAreDistinctKeys() {
        let a = RemotePhysicalPathKey("2026/01/a.jpg")
        let b = RemotePhysicalPathKey("2026/01/b.jpg")
        XCTAssertNotEqual(a, b)
    }

    func testKeyRetainsOriginalSpelling() {
        XCTAssertEqual(RemotePhysicalPathKey(nfc).path, nfc)
        XCTAssertEqual(RemotePhysicalPathKey(nfd).path, nfd)
        // The retained strings round-trip byte-for-byte even though they are String-equal.
        XCTAssertEqual(Array(RemotePhysicalPathKey(nfc).path.utf8), Array(nfc.utf8))
        XCTAssertEqual(Array(RemotePhysicalPathKey(nfd).path.utf8), Array(nfd.utf8))
    }

    func testTwoKeysCoexistInDictionary() {
        var dict: [RemotePhysicalPathKey: Int] = [:]
        dict[RemotePhysicalPathKey(nfc)] = 1
        dict[RemotePhysicalPathKey(nfd)] = 2
        XCTAssertEqual(dict.count, 2)
        XCTAssertEqual(dict[RemotePhysicalPathKey(nfc)], 1)
        XCTAssertEqual(dict[RemotePhysicalPathKey(nfd)], 2)
    }
}

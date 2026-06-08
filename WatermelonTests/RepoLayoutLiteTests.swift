import XCTest
@testable import Watermelon

final class RepoLayoutLiteTests: XCTestCase {
    // MARK: - Path composition

    func testVersionPathComposesUnderBasePath() {
        XCTAssertEqual(
            RepoLayoutLite.versionPath(basePath: "/photos"),
            "/photos/.watermelon/version.json"
        )
    }

    func testVersionPathNormalizesSlashes() {
        let expected = "/photos/.watermelon/version.json"
        XCTAssertEqual(RepoLayoutLite.versionPath(basePath: "photos"), expected)
        XCTAssertEqual(RepoLayoutLite.versionPath(basePath: "/photos/"), expected)
        XCTAssertEqual(RepoLayoutLite.versionPath(basePath: "//photos//"), expected)
        XCTAssertEqual(
            RepoLayoutLite.versionPath(basePath: "/photos/sub"),
            "/photos/sub/.watermelon/version.json"
        )
    }

    func testVersionPathAtRootBase() {
        XCTAssertEqual(RepoLayoutLite.versionPath(basePath: ""), "/.watermelon/version.json")
        XCTAssertEqual(RepoLayoutLite.versionPath(basePath: "/"), "/.watermelon/version.json")
    }

    func testRepoDirectoryPath() {
        XCTAssertEqual(RepoLayoutLite.repoDirectoryPath(basePath: "/photos"), "/photos/.watermelon")
    }

    func testLocksDirectoryAndPathCompose() {
        XCTAssertEqual(
            RepoLayoutLite.locksDirectoryPath(basePath: "/photos"),
            "/photos/.watermelon/locks"
        )
        let writerID = "1b4e28ba-2fa1-11d2-883f-0016d3cca427"
        XCTAssertEqual(
            RepoLayoutLite.lockPath(basePath: "/photos", writerID: writerID),
            "/photos/.watermelon/locks/\(writerID).lock"
        )
    }

    func testLockPathReturnsNilForInvalidWriterID() {
        XCTAssertNil(RepoLayoutLite.lockPath(basePath: "/photos", writerID: "not-a-uuid"))
    }

    func testMonthsDirectoryAndPathCompose() {
        XCTAssertEqual(
            RepoLayoutLite.monthsDirectoryPath(basePath: "/photos"),
            "/photos/.watermelon/months"
        )
        let month = LibraryMonthKey(year: 2024, month: 1)
        XCTAssertEqual(
            RepoLayoutLite.monthPath(basePath: "/photos", month: month),
            "/photos/.watermelon/months/2024-01.sqlite"
        )
    }

    // MARK: - Month filename round-trip

    func testMonthFilenameRoundTrips() {
        let keys = [
            LibraryMonthKey(year: 2024, month: 1),
            LibraryMonthKey(year: 1970, month: 12),
            LibraryMonthKey(year: 2000, month: 6),
            LibraryMonthKey(year: 2099, month: 10)
        ]
        for key in keys {
            let filename = RepoLayoutLite.monthFilename(month: key)
            XCTAssertEqual(filename, "\(key.text).sqlite")
            XCTAssertEqual(
                RepoLayoutLite.month(fromFilename: filename),
                key,
                "round-trip failed for \(filename)"
            )
        }
    }

    func testMonthFilenameRejectsMalformed() {
        let bad = [
            "2024-1.sqlite",       // month not zero-padded (invalid format)
            "2024-13.sqlite",      // month out of range
            "2024-00.sqlite",      // month out of range
            "2024-01.txt",         // wrong extension
            "2024-01",             // missing suffix
            ".sqlite",             // empty base name
            "2024/01.sqlite",      // path separator
            "sub/2024-01.sqlite",  // extra path component
            "202401.sqlite",       // missing separator
            "20a4-01.sqlite",      // non-digit year
            "2024-0b.sqlite",      // non-digit month
            "2024-01.sqlite.sqlite" // trailing component
        ]
        for name in bad {
            XCTAssertNil(RepoLayoutLite.month(fromFilename: name), "should reject \(name)")
        }
    }

    // MARK: - Lock filename round-trip

    func testLockFilenameRoundTripsForWriterID() throws {
        let writerID = UUID().uuidString.lowercased()
        let filename = try XCTUnwrap(RepoLayoutLite.lockFilename(writerID: writerID))
        XCTAssertEqual(filename, "\(writerID).lock")
        XCTAssertEqual(RepoLayoutLite.writerID(fromLockFilename: filename), writerID)
    }

    func testLockFilenameRejectsNonCanonicalWriterID() {
        XCTAssertNil(RepoLayoutLite.lockFilename(writerID: ""))
        XCTAssertNil(RepoLayoutLite.lockFilename(writerID: "not-a-uuid"))
        // P01 writer IDs are lowercased; an uppercase UUID is rejected.
        XCTAssertNil(RepoLayoutLite.lockFilename(writerID: UUID().uuidString.uppercased()))
        XCTAssertNil(RepoLayoutLite.lockFilename(writerID: "1b4e28ba-2fa1-11d2-883f-0016d3cca427/evil"))
    }

    func testWriterIDFromLockFilenameRejectsMalformed() {
        let valid = UUID().uuidString.lowercased()
        let bad = [
            valid,                       // missing suffix
            ".lock",                     // empty base name
            "not-a-uuid.lock",           // non-uuid base
            "\(valid.uppercased()).lock", // non-lowercased uuid
            "locks/\(valid).lock",       // path separator
            "\(valid).lock.lock"         // trailing component -> base is not a uuid
        ]
        for name in bad {
            XCTAssertNil(RepoLayoutLite.writerID(fromLockFilename: name), "should reject \(name)")
        }
    }
}

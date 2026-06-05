import XCTest
@testable import Watermelon

final class RepoLayoutTests: XCTestCase {
    func testFormat16HexZeroPads() {
        XCTAssertEqual(RepoLayout.format16Hex(0), "0000000000000000")
        XCTAssertEqual(RepoLayout.format16Hex(1), "0000000000000001")
        XCTAssertEqual(RepoLayout.format16Hex(0xff), "00000000000000ff")
        XCTAssertEqual(RepoLayout.format16Hex(UInt64.max), "ffffffffffffffff")
    }

    func testSnapshotFilenameRoundTrip() {
        let month = LibraryMonthKey(year: 2026, month: 5)
        let writer = "11112222-3333-4444-5555-666677778888"
        let runID = "abcdefabcdefabcdefabcdef"
        let name = RepoLayout.snapshotFileName(month: month, lamport: 42, writerID: writer, runID: runID)
        XCTAssertEqual(name, "2026-05--000000000000002a--\(writer)--abcdef.jsonl")

        let parsed = RepoLayout.parseSnapshotFilename(name)
        XCTAssertEqual(parsed?.month, month)
        XCTAssertEqual(parsed?.lamport, 42)
        XCTAssertEqual(parsed?.writerID, writer)
        XCTAssertEqual(parsed?.runIDPrefix, "abcdef")
    }

    func testLegacySnapshotFilenameParsesWithNilDigest() {
        let month = LibraryMonthKey(year: 2026, month: 5)
        let writer = "11112222-3333-4444-5555-666677778888"
        let name = RepoLayout.snapshotFileName(month: month, lamport: 42, writerID: writer, runID: "abcdef")
        let parsed = RepoLayout.parseSnapshotFilename(name)
        XCTAssertNil(parsed?.digest, "a legacy 4-segment filename carries no coverage digest")
    }

    func testAttestedSnapshotFilenameRoundTrip() {
        let month = LibraryMonthKey(year: 2026, month: 5)
        let writer = "11112222-3333-4444-5555-666677778888"
        let digest = String(repeating: "ab", count: 32) // 64 lowercase hex chars
        let name = RepoLayout.snapshotFileName(month: month, lamport: 42, writerID: writer, runID: "abcdef", digest: digest)
        XCTAssertEqual(name, "2026-05--000000000000002a--\(writer)--abcdef--\(digest).jsonl")

        let parsed = RepoLayout.parseSnapshotFilename(name)
        XCTAssertEqual(parsed?.month, month)
        XCTAssertEqual(parsed?.lamport, 42)
        XCTAssertEqual(parsed?.writerID, writer)
        XCTAssertEqual(parsed?.runIDPrefix, "abcdef")
        XCTAssertEqual(parsed?.digest, digest)
    }

    func testParseRejectsMalformedSnapshotDigest() {
        let writer = "11112222-3333-4444-5555-666677778888"
        // 63 hex chars (too short).
        XCTAssertNil(RepoLayout.parseSnapshotFilename("2026-05--000000000000002a--\(writer)--abcdef--\(String(repeating: "a", count: 63)).jsonl"))
        // Uppercase hex in the digest segment.
        XCTAssertNil(RepoLayout.parseSnapshotFilename("2026-05--000000000000002a--\(writer)--abcdef--\(String(repeating: "A", count: 64)).jsonl"))
        // Non-hex character in the digest segment.
        XCTAssertNil(RepoLayout.parseSnapshotFilename("2026-05--000000000000002a--\(writer)--abcdef--\(String(repeating: "g", count: 64)).jsonl"))
        // Six segments — neither legacy nor attested shape.
        XCTAssertNil(RepoLayout.parseSnapshotFilename("2026-05--000000000000002a--\(writer)--abcdef--\(String(repeating: "a", count: 64))--extra.jsonl"))
    }

    func testSnapshotCoverageDigestIsDeterministicAndBindsCovered() {
        let month = LibraryMonthKey(year: 2026, month: 5)
        let writer = "11112222-3333-4444-5555-666677778888"
        let coveredA = CoveredRanges(rangesByWriter: [writer: [ClosedSeqRange(low: 1, high: 3)]])
        let coveredB = CoveredRanges(rangesByWriter: [writer: [ClosedSeqRange(low: 1, high: 4)]])
        let d1 = SnapshotCoverageDigest.digest(
            version: 1, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", month: month,
            writerID: writer, filenameLamport: 42, filenameRunIDPrefix: "abcdef", covered: coveredA
        )
        let d2 = SnapshotCoverageDigest.digest(
            version: 1, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", month: month,
            writerID: writer, filenameLamport: 42, filenameRunIDPrefix: "abcdef", covered: coveredA
        )
        let d3 = SnapshotCoverageDigest.digest(
            version: 1, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", month: month,
            writerID: writer, filenameLamport: 42, filenameRunIDPrefix: "abcdef", covered: coveredB
        )
        XCTAssertEqual(d1, d2, "digest must be deterministic for identical inputs")
        XCTAssertNotEqual(d1, d3, "changing covered must change the digest")
        XCTAssertEqual(d1.count, 64, "digest is a lowercase SHA-256 hex string")
        XCTAssertEqual(d1, d1.lowercased())
    }

    func testCommitFilenameRoundTrip() {
        let month = LibraryMonthKey(year: 2026, month: 12)
        let writer = "11112222-3333-4444-5555-666677778888"
        let name = RepoLayout.commitFileName(month: month, writerID: writer, seq: 0xabcdef)
        XCTAssertEqual(name, "2026-12--\(writer)--0000000000abcdef.jsonl")

        let parsed = RepoLayout.parseCommitFilename(name)
        XCTAssertEqual(parsed?.month, month)
        XCTAssertEqual(parsed?.writerID, writer)
        XCTAssertEqual(parsed?.seq, 0xabcdef)
    }

    func testParseRejectsMalformedNames() {
        XCTAssertNil(RepoLayout.parseSnapshotFilename("not-a-snapshot.jsonl"))
        XCTAssertNil(RepoLayout.parseSnapshotFilename("2026-13--00000000000000ff--writer--abc123.jsonl"))
        XCTAssertNil(RepoLayout.parseCommitFilename("2026-05--writer.jsonl"))
        XCTAssertNil(RepoLayout.parseCommitFilename("2026-05--writer--zzzzzz.jsonl"))
        XCTAssertNil(RepoLayout.parseCommitFilename("2026-05--11112222-3333-4444-5555-666677778888--1.jsonl"))
        XCTAssertNil(RepoLayout.parseCommitFilename("2026-05--11112222-3333-4444-5555-666677778888--0000000000000001"))
        XCTAssertNil(RepoLayout.parseCommitFilename("2026-5--11112222-3333-4444-5555-666677778888--0000000000000001.jsonl"))
        XCTAssertNil(RepoLayout.parseSnapshotFilename("2026-05--1--11112222-3333-4444-5555-666677778888--abcdef.jsonl"))
        XCTAssertNil(RepoLayout.parseSnapshotFilename("2026-05--0000000000000001--11112222-3333-4444-5555-666677778888--ABCDEF.jsonl"))
    }

    func testPathHelpersStartWithLeadingSlash() {
        XCTAssertTrue(RepoLayout.versionFilePath(base: "/srv").hasPrefix("/"))
        XCTAssertTrue(RepoLayout.commitsDirectoryPath(base: "/srv").hasPrefix("/"))
        XCTAssertTrue(RepoLayout.snapshotsDirectoryPath(base: "srv").hasPrefix("/"))
    }

    func testRunIDPrefixTakesFirst6HexCharsLowercased() {
        XCTAssertEqual(RepoLayout.runIDPrefix("ABCDEF-1234-5678"), "abcdef")
        XCTAssertEqual(RepoLayout.runIDPrefix("ab"), "ab")
    }

    func testParseWriterIDJSONFilename() {
        let uuid = "11112222-3333-4444-5555-666677778888"
        XCTAssertEqual(RepoLayout.parseWriterIDJSONFilename("\(uuid).json"), uuid)
        XCTAssertNil(RepoLayout.parseWriterIDJSONFilename("\(uuid).txt"))
        XCTAssertNil(RepoLayout.parseWriterIDJSONFilename(".DS_Store.json"))
        XCTAssertNil(RepoLayout.parseWriterIDJSONFilename("uuid-1234.json"))
    }
}

import XCTest
@testable import Watermelon

/// `RemoteResourcePresence` + `RemoteMonthPresenceMap` are the unified vocabulary
/// for V2MonthSession / RemoteIndexSyncService overlay / RepoVerifyMonthService.
/// These tests pin the boundary semantics so a callsite drift can't quietly
/// re-introduce the bool/Set scatter the type was created to eliminate.
final class RemoteResourcePresenceTests: XCTestCase {

    // MARK: - Per-path queries

    func testIsHashVerified_onlyTrueForHashVerified() {
        var map = RemoteMonthPresenceMap()
        map.mark(path: "/a", .hashVerified)
        map.mark(path: "/b", .listedSizeMatched)
        map.mark(path: "/c", .missing)
        map.mark(path: "/d", .inconclusive(.neverProbed))

        XCTAssertTrue(map.isHashVerified("/a"))
        XCTAssertFalse(map.isHashVerified("/b"), "size-matched is NOT content-trusted")
        XCTAssertFalse(map.isHashVerified("/c"))
        XCTAssertFalse(map.isHashVerified("/d"))
        XCTAssertFalse(map.isHashVerified("/missing"))
    }

    func testIsUsableCandidate_acceptsListedOrVerified_rejectsMissingOrInconclusive() {
        var map = RemoteMonthPresenceMap()
        map.mark(path: "/a", .hashVerified)
        map.mark(path: "/b", .listedSizeMatched)
        map.mark(path: "/c", .missing)
        map.mark(path: "/d", .inconclusive(.verifyBudgetExhausted))
        map.mark(path: "/e", .inconclusive(.probeFailure))
        map.mark(path: "/f", .inconclusive(.neverProbed))

        XCTAssertTrue(map.isUsableCandidate("/a"))
        XCTAssertTrue(map.isUsableCandidate("/b"))
        XCTAssertFalse(map.isUsableCandidate("/c"))
        XCTAssertFalse(map.isUsableCandidate("/d"))
        XCTAssertFalse(map.isUsableCandidate("/e"))
        XCTAssertFalse(map.isUsableCandidate("/f"))
        XCTAssertFalse(map.isUsableCandidate("/never-marked"))
    }

    func testIsMissing_onlyTrueForMissing() {
        var map = RemoteMonthPresenceMap()
        map.mark(path: "/a", .missing)
        map.mark(path: "/b", .inconclusive(.probeFailure))

        XCTAssertTrue(map.isMissing("/a"))
        XCTAssertFalse(map.isMissing("/b"),
                       "inconclusive is NOT missing — treating it as missing would issue tombstones against unprobed bytes")
        XCTAssertFalse(map.isMissing("/never-marked"))
    }

    // MARK: - Per-hash projection

    func testFullyMissingHashes_allPathsMustBeMissing() {
        var map = RemoteMonthPresenceMap()
        map.mark(path: "/h1-p1", .missing)
        map.mark(path: "/h1-p2", .missing)
        map.mark(path: "/h2-p1", .missing)
        map.mark(path: "/h2-p2", .listedSizeMatched)
        map.mark(path: "/h3-p1", .inconclusive(.probeFailure))

        let h1 = Data([0x01]); let h2 = Data([0x02]); let h3 = Data([0x03])
        let pathsByHash: [Data: Set<String>] = [
            h1: ["/h1-p1", "/h1-p2"],
            h2: ["/h2-p1", "/h2-p2"],
            h3: ["/h3-p1"]
        ]

        let missing = map.fullyMissingHashes(pathsByHash: pathsByHash)
        XCTAssertEqual(missing, [h1], "h2 has a usable path; h3 is inconclusive (not missing)")
    }

    func testFullyMissingHashes_skipsHashesWithoutPaths() {
        let map = RemoteMonthPresenceMap()
        let pathsByHash: [Data: Set<String>] = [Data([0x99]): []]
        XCTAssertEqual(map.fullyMissingHashes(pathsByHash: pathsByHash), [],
                       "empty path set must not be classified as fully-missing")
    }

    // MARK: - Map-level state

    func testIsFullyResolved_emptyMapIsResolved() {
        XCTAssertTrue(RemoteMonthPresenceMap().isFullyResolved)
    }

    func testIsFullyResolved_anyInconclusiveBlocks() {
        var map = RemoteMonthPresenceMap()
        map.mark(path: "/a", .hashVerified)
        map.mark(path: "/b", .missing)
        XCTAssertTrue(map.isFullyResolved)

        map.mark(path: "/c", .inconclusive(.verifyBudgetExhausted))
        XCTAssertFalse(map.isFullyResolved)

        map.clear(path: "/c")
        XCTAssertTrue(map.isFullyResolved, "after the inconclusive entry leaves, the map is resolved again")
    }

    // MARK: - Mutation

    func testClear_removesPath() {
        var map = RemoteMonthPresenceMap()
        map.mark(path: "/a", .missing)
        XCTAssertTrue(map.isMissing("/a"))
        map.clear(path: "/a")
        XCTAssertFalse(map.isMissing("/a"))
        XCTAssertNil(map.presence(for: "/a"))
    }
}

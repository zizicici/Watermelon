import XCTest
@testable import Watermelon

/// `RemoteResourcePresence` + `RemoteMonthPresenceMap` are the unified vocabulary
/// for V2MonthSession / RemoteIndexSyncService overlay / RepoVerifyMonthService.
/// These tests pin the boundary semantics so a callsite drift can't quietly
/// re-introduce the bool/Set scatter the type was created to eliminate.
final class RemoteResourcePresenceTests: XCTestCase {


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

    // Bug-X P07 R03 ClaudeReviewerA F1: byte-distinct NFC/NFD twin paths compare equal as Swift
    // Strings, so a String-keyed map collapses their presence into one entry — on exact-name
    // backends that store both spellings the missing twin would inherit the present twin's state
    // (heal suppressed) or vice-versa. The map must track them as the byte-distinct resources they are.
    func testMark_nfcAndNfdTwinPathsTrackedSeparately() {
        let nfc = "2026/01/caf\u{00E9}.jpg"
        let nfd = "2026/01/cafe\u{0301}.jpg"
        XCTAssertEqual(nfc, nfd, "premise: the two spellings are Swift-String-equal")
        XCTAssertNotEqual(Array(nfc.utf8), Array(nfd.utf8), "premise: but byte-distinct")

        var map = RemoteMonthPresenceMap()
        map.mark(path: nfc, .listedSizeMatched)
        map.mark(path: nfd, .missing)

        XCTAssertTrue(map.isUsableCandidate(nfc), "present NFC twin must stay usable")
        XCTAssertFalse(map.isUsableCandidate(nfd), "missing NFD twin must NOT inherit the NFC twin's presence")
        XCTAssertTrue(map.isMissing(nfd))
        XCTAssertFalse(map.isMissing(nfc))
        XCTAssertEqual(map.byPath.count, 2, "twin paths must occupy distinct entries, not collapse to one")
    }

    func testFullyMissingHashes_nfcAndNfdTwinsDoNotCrossContaminate() {
        let nfc = "2026/01/caf\u{00E9}.jpg"
        let nfd = "2026/01/cafe\u{0301}.jpg"
        var map = RemoteMonthPresenceMap()
        map.mark(path: nfc, .listedSizeMatched)
        map.mark(path: nfd, .missing)

        let hPresent = Data([0xA1]); let hMissing = Data([0xB2])
        let pathsByHash: [Data: Set<RemotePhysicalPathKey>] = [
            hPresent: [RemotePhysicalPathKey(nfc)],
            hMissing: [RemotePhysicalPathKey(nfd)]
        ]

        XCTAssertEqual(map.fullyMissingHashes(pathsByHash: pathsByHash), [hMissing],
                       "only the genuinely-missing NFD twin's hash is fully-missing; the present NFC twin is not")
    }

    // Bug-X P07 R07 CodexChecker F2: one content hash committed at two byte-distinct NFC/NFD twin
    // paths (multi-writer same-content upload). A `Set<String>` folds the twins to one entry before
    // the all-missing test, so if the retained spelling is the missing twin the hash is falsely
    // reported fully-missing even though the other twin's bytes are present.
    func testFullyMissingHashes_sameHashNfcAndNfdTwins_presentTwinSuppressesMissing() {
        let nfc = "2026/01/caf\u{00E9}.jpg"
        let nfd = "2026/01/cafe\u{0301}.jpg"
        XCTAssertEqual(nfc, nfd, "premise: the two spellings are Swift-String-equal")
        XCTAssertNotEqual(Array(nfc.utf8), Array(nfd.utf8), "premise: but byte-distinct")

        var map = RemoteMonthPresenceMap()
        map.mark(path: nfc, .missing)
        map.mark(path: nfd, .listedSizeMatched)

        let h = Data([0xC3])
        let pathsByHash: [Data: Set<RemotePhysicalPathKey>] = [
            h: [RemotePhysicalPathKey(nfc), RemotePhysicalPathKey(nfd)]
        ]
        XCTAssertEqual(pathsByHash[h]?.count, 2, "byte-exact keys keep both twins; a Set<String> folds to one")
        XCTAssertEqual(map.fullyMissingHashes(pathsByHash: pathsByHash), [],
                       "the present NFD twin must keep the shared hash off the fully-missing set")
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


    func testFullyMissingHashes_allPathsMustBeMissing() {
        var map = RemoteMonthPresenceMap()
        map.mark(path: "/h1-p1", .missing)
        map.mark(path: "/h1-p2", .missing)
        map.mark(path: "/h2-p1", .missing)
        map.mark(path: "/h2-p2", .listedSizeMatched)
        map.mark(path: "/h3-p1", .inconclusive(.probeFailure))

        let h1 = Data([0x01]); let h2 = Data([0x02]); let h3 = Data([0x03])
        let pathsByHash: [Data: Set<RemotePhysicalPathKey>] = [
            h1: [RemotePhysicalPathKey("/h1-p1"), RemotePhysicalPathKey("/h1-p2")],
            h2: [RemotePhysicalPathKey("/h2-p1"), RemotePhysicalPathKey("/h2-p2")],
            h3: [RemotePhysicalPathKey("/h3-p1")]
        ]

        let missing = map.fullyMissingHashes(pathsByHash: pathsByHash)
        XCTAssertEqual(missing, [h1], "h2 has a usable path; h3 is inconclusive (not missing)")
    }

    func testFullyMissingHashes_skipsHashesWithoutPaths() {
        let map = RemoteMonthPresenceMap()
        let pathsByHash: [Data: Set<RemotePhysicalPathKey>] = [Data([0x99]): []]
        XCTAssertEqual(map.fullyMissingHashes(pathsByHash: pathsByHash), [],
                       "empty path set must not be classified as fully-missing")
    }


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


    func testClear_removesPath() {
        var map = RemoteMonthPresenceMap()
        map.mark(path: "/a", .missing)
        XCTAssertTrue(map.isMissing("/a"))
        map.clear(path: "/a")
        XCTAssertFalse(map.isMissing("/a"))
        XCTAssertNil(map.presence(for: "/a"))
    }
}

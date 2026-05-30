import Foundation
import XCTest
@testable import Watermelon

/// Arch-VII A-I D5: `BackendNameCaseSensitivity.unknown` must be fail-closed in BOTH directions —
/// byte-exact presence (never merges two distinct names) and case-folded collision avoidance
/// (never lets a colliding name through). Guards the invariant every V2 consumer relies on.
final class BackendCapabilityFailClosedTests: XCTestCase {

    func testUnknownIsFailClosedInBothDirections() {
        XCTAssertTrue(BackendNameCaseSensitivity.unknown.usesExactNameMatchingForPresence,
                      ".unknown must use byte-exact presence — must not merge two distinct names")
        XCTAssertTrue(BackendNameCaseSensitivity.unknown.foldsCaseForCollisionAvoidance,
                      ".unknown must fold case for collision avoidance — must not allow a colliding name")
    }

    func testHelperBooleansForKnownCases() {
        XCTAssertTrue(BackendNameCaseSensitivity.caseSensitive.usesExactNameMatchingForPresence)
        XCTAssertFalse(BackendNameCaseSensitivity.caseSensitive.foldsCaseForCollisionAvoidance)
        XCTAssertFalse(BackendNameCaseSensitivity.caseInsensitive.usesExactNameMatchingForPresence)
        XCTAssertTrue(BackendNameCaseSensitivity.caseInsensitive.foldsCaseForCollisionAvoidance)
    }

    /// Collision avoidance: two case-only variants fold to the SAME collision key under `.unknown`,
    /// so the second upload is treated as colliding and suffixed — no silent overwrite.
    func testUnknownCollisionKeyFoldsCase() {
        let upper = RemoteFileNaming.nameKey(for: "IMG_1.JPG", caseSensitivity: .unknown)
        let lower = RemoteFileNaming.nameKey(for: "img_1.jpg", caseSensitivity: .unknown)
        XCTAssertEqual(upper, lower, ".unknown collision key must fold case (fail-closed against collision)")
    }

    /// Presence: the same two case-only variants keep DISTINCT presence keys under `.unknown`,
    /// so a listed lowercase object never marks an absent uppercase committed key as present.
    func testUnknownPresenceKeyStaysByteExact() {
        let upper = BackendNameCaseSensitivity.unknown.presenceKey(for: "IMG_1.JPG")
        let lower = BackendNameCaseSensitivity.unknown.presenceKey(for: "img_1.jpg")
        XCTAssertNotEqual(upper, lower, ".unknown presence key must stay byte-exact (no name merge)")
    }

    /// Contrast: a case-insensitive backend deliberately merges case in presence (its filesystem does too).
    func testCaseInsensitivePresenceKeyMergesCase() {
        let upper = BackendNameCaseSensitivity.caseInsensitive.presenceKey(for: "IMG_1.JPG")
        let lower = BackendNameCaseSensitivity.caseInsensitive.presenceKey(for: "img_1.jpg")
        XCTAssertEqual(upper, lower)
    }
}

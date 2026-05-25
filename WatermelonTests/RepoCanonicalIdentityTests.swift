import XCTest
@testable import Watermelon

final class RepoCanonicalIdentityTests: XCTestCase {
    private let sampleUUIDLower = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let sampleUUIDUpper = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"
    private let nonUUID = "not-a-uuid"
    private let empty = ""

    func testNormalize_UUIDLowercased_ReturnsLowercased() {
        XCTAssertEqual(RepoCanonicalIdentity.normalize(sampleUUIDLower), sampleUUIDLower)
    }

    func testNormalize_UUIDUppercased_ReturnsLowercased() {
        XCTAssertEqual(RepoCanonicalIdentity.normalize(sampleUUIDUpper), sampleUUIDLower)
    }

    func testNormalize_NonUUID_ReturnsNil() {
        XCTAssertNil(RepoCanonicalIdentity.normalize(nonUUID))
    }

    func testNormalize_Empty_ReturnsNil() {
        XCTAssertNil(RepoCanonicalIdentity.normalize(empty))
    }

    func testNormalizeLossy_UUID_ReturnsLowercasedUUID() {
        XCTAssertEqual(RepoCanonicalIdentity.normalizeLossy(sampleUUIDLower), sampleUUIDLower)
    }

    func testNormalizeLossy_UUIDUppercased_ReturnsLowercasedUUID() {
        XCTAssertEqual(RepoCanonicalIdentity.normalizeLossy(sampleUUIDUpper), sampleUUIDLower)
    }

    func testNormalizeLossy_NonUUID_ReturnsLowercased() {
        XCTAssertEqual(RepoCanonicalIdentity.normalizeLossy("NOT-A-UUID"), "not-a-uuid")
    }

    func testNormalizeLossy_Empty_ReturnsEmpty() {
        XCTAssertEqual(RepoCanonicalIdentity.normalizeLossy(empty), empty)
    }

    func testValidate_UUID_ReturnsLowercased() throws {
        XCTAssertEqual(try RepoCanonicalIdentity.validate(sampleUUIDUpper, field: "repoID"), sampleUUIDLower)
    }

    func testValidate_NonUUID_Throws() {
        XCTAssertThrowsError(try RepoCanonicalIdentity.validate(nonUUID, field: "repoID"))
    }

    func testValidate_Empty_Throws() {
        XCTAssertThrowsError(try RepoCanonicalIdentity.validate(empty, field: "repoID"))
    }
}

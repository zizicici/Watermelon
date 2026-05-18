import XCTest
@testable import Watermelon

final class RepoWireValidatorTests: XCTestCase {
    func testValidateHash_acceptsExact32Bytes() throws {
        let hex = String(repeating: "ab", count: 32)
        let result = try RepoWireValidator.validateHash(hex, field: "test")
        XCTAssertEqual(result.count, 32)
    }

    func testValidateHash_rejectsEmpty() {
        XCTAssertThrowsError(try RepoWireValidator.validateHash("", field: "fp")) { err in
            guard case WireValidationError.wrongHashLength(let f, let n) = err else {
                XCTFail("expected wrongHashLength, got \(err)"); return
            }
            XCTAssertEqual(f, "fp")
            XCTAssertEqual(n, 0)
        }
    }

    func testValidateHash_rejectsTruncated() {
        let hex = String(repeating: "ab", count: 16)
        XCTAssertThrowsError(try RepoWireValidator.validateHash(hex, field: "fp")) { err in
            guard case WireValidationError.wrongHashLength(_, let n) = err else {
                XCTFail("expected wrongHashLength, got \(err)"); return
            }
            XCTAssertEqual(n, 16)
        }
    }

    func testValidateHash_rejectsTooLong() {
        let hex = String(repeating: "ab", count: 64)
        XCTAssertThrowsError(try RepoWireValidator.validateHash(hex, field: "fp")) { err in
            guard case WireValidationError.wrongHashLength(_, let n) = err else {
                XCTFail("expected wrongHashLength, got \(err)"); return
            }
            XCTAssertEqual(n, 64)
        }
    }

    func testValidateHash_rejectsOddLengthHex() {
        XCTAssertThrowsError(try RepoWireValidator.validateHash("abc", field: "fp")) { err in
            guard case WireValidationError.invalidHex = err else {
                XCTFail("expected invalidHex, got \(err)"); return
            }
        }
    }

    func testValidateUInt64InIntRange_acceptsZero() throws {
        let v = try RepoWireValidator.validateUInt64InIntRange(UInt64(0), field: "x")
        XCTAssertEqual(v, 0)
    }

    func testValidateUInt64InIntRange_acceptsIntMax() throws {
        let v = try RepoWireValidator.validateUInt64InIntRange(UInt64(Int.max), field: "x")
        XCTAssertEqual(v, Int.max)
    }

    func testValidateUInt64InIntRange_rejectsAboveIntMax() {
        let above: UInt64 = UInt64(Int.max) &+ 1
        XCTAssertThrowsError(try RepoWireValidator.validateUInt64InIntRange(above, field: "opSeq")) { err in
            guard case WireValidationError.uint64OutOfIntRange(let f, let n) = err else {
                XCTFail("expected uint64OutOfIntRange, got \(err)"); return
            }
            XCTAssertEqual(f, "opSeq")
            XCTAssertEqual(n, above)
        }
    }

    func testValidateNonNegativeInt_rejectsNegative() {
        XCTAssertThrowsError(try RepoWireValidator.validateNonNegativeInt(-1, field: "fileSize")) { err in
            guard case WireValidationError.nonNegative = err else {
                XCTFail("expected nonNegative, got \(err)"); return
            }
        }
    }

    func testRequireInt64_rejectsFractionalNSNumber() {
        let n = NSNumber(value: 1.9)
        XCTAssertThrowsError(try RepoWireValidator.requireInt64(n, field: "clockMin")) { err in
            guard case WireValidationError.fractionalNumber = err else {
                XCTFail("expected fractionalNumber, got \(err)"); return
            }
        }
    }

    func testValidateRelativePath_rejectsParentTraversal() {
        XCTAssertThrowsError(try RepoWireValidator.validateRelativePath("2026/01/../../../etc/passwd")) { err in
            guard case WireValidationError.pathContainsTraversal = err else {
                XCTFail("expected pathContainsTraversal, got \(err)"); return
            }
        }
    }

    func testValidateRelativePath_acceptsBenignPath() throws {
        let p = try RepoWireValidator.validateRelativePath("2026/01/photo.jpg")
        XCTAssertEqual(p, "2026/01/photo.jpg")
    }

    func testValidateMonthScope_acceptsValid() throws {
        let key = try RepoWireValidator.validateMonthScope("month:2026-05")
        XCTAssertEqual(key, LibraryMonthKey(year: 2026, month: 5))
    }

    func testValidateMonthScope_rejectsMalformed() {
        XCTAssertThrowsError(try RepoWireValidator.validateMonthScope("not-a-scope")) { err in
            guard case WireValidationError.malformedMonthScope = err else {
                XCTFail("expected malformedMonthScope, got \(err)"); return
            }
        }
    }

    func testRequireUInt64_rejectsJSONBooleanTrueAsOne() throws {
        let data = "{\"v\":true}".data(using: .utf8)!
        let parsed = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        XCTAssertThrowsError(try RepoWireValidator.requireUInt64(parsed["v"], field: "v")) { err in
            guard case WireValidationError.missingField(let field) = err else {
                XCTFail("expected missingField, got \(err)"); return
            }
            XCTAssertEqual(field, "v")
        }
    }

    func testRequireUInt64_rejectsJSONBooleanFalseAsZero() throws {
        let data = "{\"v\":false}".data(using: .utf8)!
        let parsed = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        XCTAssertThrowsError(try RepoWireValidator.requireUInt64(parsed["v"], field: "v")) { err in
            guard case WireValidationError.missingField = err else {
                XCTFail("expected missingField, got \(err)"); return
            }
        }
    }

    func testRequireInt64_rejectsJSONBoolean() throws {
        let data = "{\"v\":true}".data(using: .utf8)!
        let parsed = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        XCTAssertThrowsError(try RepoWireValidator.requireInt64(parsed["v"], field: "v")) { err in
            guard case WireValidationError.missingField = err else {
                XCTFail("expected missingField, got \(err)"); return
            }
        }
    }

    func testRequireInt_rejectsJSONBoolean() throws {
        let data = "{\"v\":false}".data(using: .utf8)!
        let parsed = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        XCTAssertThrowsError(try RepoWireValidator.requireInt(parsed["v"], field: "v")) { err in
            guard case WireValidationError.missingField = err else {
                XCTFail("expected missingField, got \(err)"); return
            }
        }
    }

    func testRequireInt64_acceptsInt8One() throws {
        let n = NSNumber(value: Int8(1))
        let v = try RepoWireValidator.requireInt64(n, field: "v")
        XCTAssertEqual(v, 1)
    }
}

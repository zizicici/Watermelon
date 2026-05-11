import XCTest
@testable import Watermelon

/// Single source of truth for V2 wire input validation. Every rule guards a
/// specific bug class that materialized in production review:
/// - Truncated hash → fingerprint collisions
/// - opSeq > Int.max → materializer trap (DoS)
/// - `..` segment → restore reads outside basePath
/// - Negative integers → corrupted aggregations
/// - Fractional NSNumber → silent truncation, off-by-one ordering
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

    /// Direct `Int(UInt64.max)` traps. validator must throw, not crash.
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

    /// `uint64Value` of fractional NSNumber silently truncates 1.9 → 1, so a
    /// malformed snapshot could shrink covered ranges and force commits inside
    /// the original range to be replayed.
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
}

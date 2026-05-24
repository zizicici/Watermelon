import XCTest
@testable import Watermelon

/// Pins WireValidationError.translated routing + payload formatting across all
/// 9 cases, and confirms CommitOpMapper / SnapshotRowMapper mapValidation still
/// produce the same target wire errors after delegating to the shared switch.
final class WireValidationErrorTranslatorTests: XCTestCase {

    private enum TranslationProbe: Equatable {
        case missingField(String)
        case malformed(String)
    }

    private func translate(_ err: WireValidationError) -> TranslationProbe {
        err.translated(
            missingField: TranslationProbe.missingField,
            malformed: TranslationProbe.malformed
        )
    }

    // MARK: - Per-case translator tests (Tests 1-9)

    func testTranslated_missingField() {
        let result = translate(.missingField("foo"))
        XCTAssertEqual(result, .missingField("foo"),
                       ".missingField must route to missingField with field verbatim")
    }

    func testTranslated_wrongHashLength() {
        let result = translate(.wrongHashLength(field: "fp", actual: 16))
        XCTAssertEqual(result, .malformed("fp must be 32-byte hex (got 16)"),
                       ".wrongHashLength must route to malformed with the field-and-count format")
    }

    func testTranslated_invalidHex() {
        let result = translate(.invalidHex(field: "contentHash"))
        XCTAssertEqual(result, .malformed("contentHash invalid hex"),
                       ".invalidHex must route to malformed with the suffix format")
    }

    func testTranslated_nonNegative() {
        let result = translate(.nonNegative(field: "fileSize", actual: -1))
        XCTAssertEqual(result, .malformed("fileSize must be non-negative"),
                       ".nonNegative must route to malformed; actual payload is intentionally dropped")
    }

    func testTranslated_uint64OutOfIntRange() {
        let result = translate(.uint64OutOfIntRange(field: "opSeq", actual: 99999))
        XCTAssertEqual(result, .malformed("opSeq exceeds Int.max"),
                       ".uint64OutOfIntRange must route to malformed; actual payload is intentionally dropped")
    }

    func testTranslated_fractionalNumber() {
        let result = translate(.fractionalNumber(field: "clockMin"))
        XCTAssertEqual(result, .missingField("clockMin"),
                       ".fractionalNumber must route to missingField (not malformed)")
    }

    func testTranslated_pathContainsTraversal() {
        let result = translate(.pathContainsTraversal("../etc"))
        XCTAssertEqual(result, .malformed("physicalRemotePath rejected: containsParentTraversal(\"../etc\")"),
                       ".pathContainsTraversal must route to malformed with quoted-path format")
    }

    func testTranslated_malformedMonthScope() {
        let result = translate(.malformedMonthScope("not-a-scope"))
        XCTAssertEqual(result, .malformed("malformed month scope: not-a-scope"),
                       ".malformedMonthScope must route to malformed with prefixed format")
    }

    func testTranslated_malformed() {
        let result = translate(.malformed("anything goes"))
        XCTAssertEqual(result, .malformed("anything goes"),
                       ".malformed must route to malformed with payload verbatim, no prefix")
    }

    // MARK: - Mapper parity (Tests 10-11)

    private struct ParityCase {
        let name: String
        let input: WireValidationError
        let expectedCommit: CommitWireError
        let expectedSnapshot: SnapshotWireError
    }

    private func parityCases() -> [ParityCase] {
        [
            ParityCase(
                name: "missingField",
                input: .missingField("foo"),
                expectedCommit: .missingField("foo"),
                expectedSnapshot: .missingField("foo")
            ),
            ParityCase(
                name: "wrongHashLength",
                input: .wrongHashLength(field: "fp", actual: 16),
                expectedCommit: .malformed("fp must be 32-byte hex (got 16)"),
                expectedSnapshot: .malformed("fp must be 32-byte hex (got 16)")
            ),
            ParityCase(
                name: "invalidHex",
                input: .invalidHex(field: "contentHash"),
                expectedCommit: .malformed("contentHash invalid hex"),
                expectedSnapshot: .malformed("contentHash invalid hex")
            ),
            ParityCase(
                name: "nonNegative",
                input: .nonNegative(field: "fileSize", actual: -1),
                expectedCommit: .malformed("fileSize must be non-negative"),
                expectedSnapshot: .malformed("fileSize must be non-negative")
            ),
            ParityCase(
                name: "uint64OutOfIntRange",
                input: .uint64OutOfIntRange(field: "opSeq", actual: 99999),
                expectedCommit: .malformed("opSeq exceeds Int.max"),
                expectedSnapshot: .malformed("opSeq exceeds Int.max")
            ),
            ParityCase(
                name: "fractionalNumber",
                input: .fractionalNumber(field: "clockMin"),
                expectedCommit: .missingField("clockMin"),
                expectedSnapshot: .missingField("clockMin")
            ),
            ParityCase(
                name: "pathContainsTraversal",
                input: .pathContainsTraversal("../etc"),
                expectedCommit: .malformed("physicalRemotePath rejected: containsParentTraversal(\"../etc\")"),
                expectedSnapshot: .malformed("physicalRemotePath rejected: containsParentTraversal(\"../etc\")")
            ),
            ParityCase(
                name: "malformedMonthScope",
                input: .malformedMonthScope("not-a-scope"),
                expectedCommit: .malformed("malformed month scope: not-a-scope"),
                expectedSnapshot: .malformed("malformed month scope: not-a-scope")
            ),
            ParityCase(
                name: "malformed",
                input: .malformed("anything goes"),
                expectedCommit: .malformed("anything goes"),
                expectedSnapshot: .malformed("anything goes")
            ),
        ]
    }

    func testParity_CommitOpMapperRoutesAllCasesIdentically() {
        for c in parityCases() {
            XCTAssertThrowsError(try CommitOpMapper.mapValidation { throw c.input }) { thrown in
                XCTAssertEqual(thrown as? CommitWireError, c.expectedCommit,
                               "\(c.name): CommitOpMapper.mapValidation produced unexpected target")
            }
        }
    }

    func testParity_SnapshotRowMapperRoutesAllCasesIdentically() {
        for c in parityCases() {
            XCTAssertThrowsError(try SnapshotRowMapper.mapValidation { throw c.input }) { thrown in
                XCTAssertEqual(thrown as? SnapshotWireError, c.expectedSnapshot,
                               "\(c.name): SnapshotRowMapper.mapValidation produced unexpected target")
            }
        }
    }
}

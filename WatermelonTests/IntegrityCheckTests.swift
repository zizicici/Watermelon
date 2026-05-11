import XCTest
@testable import Watermelon

final class IntegrityCheckTests: XCTestCase {
    func testSha256OfSingleLine() {
        var acc = IntegrityAccumulator()
        acc.absorbLine(#"{"t":"header"}"#)
        let sha = acc.finalize()
        XCTAssertEqual(sha.count, 64)
        XCTAssertEqual(acc.rowCount, 1)
    }

    /// `absorbLine` joins lines with a single `\n` for hashing but counts each absorbed
    /// line as one row. Two absorbLines producing the same hash as one absorbLine of the
    /// joined string is the wire-format invariant; rowCount diverging is what tells the
    /// reader/writer which side is right when integrity check fires.
    func testSha256JoinsLinesWithNewline() {
        var a = IntegrityAccumulator()
        a.absorbLine("line1")
        a.absorbLine("line2")
        let shaA = a.finalize()

        var b = IntegrityAccumulator()
        b.absorbLine("line1\nline2")
        let shaB = b.finalize()

        XCTAssertEqual(shaA, shaB)
        XCTAssertEqual(a.rowCount, 2)
        XCTAssertEqual(b.rowCount, 1, "row count is per absorbLine call, not per `\\n` in input")
    }
}

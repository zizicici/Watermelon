import XCTest
@testable import Watermelon

final class CommitLogParseTests: XCTestCase {
    private func sampleHeader() -> CommitHeader {
        CommitHeader(
            version: 1,
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            writerID: "w",
            seq: 1,
            runID: "run",
            scope: "month:2026-05",
            clockMin: 1,
            clockMax: 2,
            bodyKind: "plain"
        )
    }

    private func sampleOp(seq: Int, clock: UInt64) -> CommitOp {
        // Wire boundary requires 32-byte SHA-256 fingerprints; fill with `seq` for distinguishability.
        let fp = Data(repeating: UInt8(seq), count: 32)
        return CommitOp(opSeq: seq, clock: clock, body: .tombstoneAsset(
            CommitTombstoneBody(assetFingerprint: fp, reason: .userDeleted)
        ))
    }

    func testRoundTripWithIntegrity() throws {
        let header = sampleHeader()
        let ops = [sampleOp(seq: 0, clock: 1), sampleOp(seq: 1, clock: 2)]

        var acc = IntegrityAccumulator()
        let h = try CommitOpMapper.encodeHeaderLine(header)
        acc.absorbLine(h)
        var lines = [h]
        for op in ops {
            let line = try CommitOpMapper.encodeOpLine(op)
            acc.absorbLine(line)
            lines.append(line)
        }
        let sha = acc.finalize()
        let endLine = try CommitOpMapper.encodeEndLine(sha256Hex: sha, rowCount: acc.rowCount)
        lines.append(endLine)

        let raw = lines.joined(separator: "\n") + "\n"
        let parsed = try CommitLogReader.parse(text: raw)
        XCTAssertEqual(parsed.header, header)
        XCTAssertEqual(parsed.ops, ops)
        XCTAssertEqual(parsed.rowCount, ops.count + 1)
        XCTAssertEqual(parsed.sha256Hex, sha)
    }

    func testTamperedShaThrows() throws {
        let header = sampleHeader()
        let ops = [sampleOp(seq: 0, clock: 1)]
        var acc = IntegrityAccumulator()
        let h = try CommitOpMapper.encodeHeaderLine(header)
        acc.absorbLine(h)
        var lines = [h]
        for op in ops {
            let line = try CommitOpMapper.encodeOpLine(op)
            acc.absorbLine(line)
            lines.append(line)
        }
        let badSha = "deadbeef" + String(repeating: "0", count: 56)
        let endLine = try CommitOpMapper.encodeEndLine(sha256Hex: badSha, rowCount: acc.rowCount)
        lines.append(endLine)
        let raw = lines.joined(separator: "\n") + "\n"
        XCTAssertThrowsError(try CommitLogReader.parse(text: raw)) { err in
            switch err {
            case CommitLogReader.ReadError.integrityMismatch: break
            default: XCTFail("expected integrity mismatch, got \(err)")
            }
        }
    }

    func testHeaderSeqZeroIsRejected() throws {
        var header = sampleHeader()
        header = CommitHeader(
            version: header.version,
            repoID: header.repoID,
            writerID: header.writerID,
            seq: 0,
            runID: header.runID,
            scope: header.scope,
            clockMin: header.clockMin,
            clockMax: header.clockMax,
            bodyKind: header.bodyKind
        )
        let line = try CommitOpMapper.encodeHeaderLine(header)
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(line)) { err in
            if !(err is CommitWireError) {
                XCTFail("expected CommitWireError, got \(err)")
            }
        }
    }
}

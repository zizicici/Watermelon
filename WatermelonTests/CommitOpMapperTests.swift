import XCTest
@testable import Watermelon

final class CommitOpMapperTests: XCTestCase {
    func testHeaderRoundTrip() throws {
        let header = CommitHeader(
            version: 1,
            repoID: "repo-uuid",
            writerID: "writer-uuid",
            seq: 42,
            runID: "run-uuid",
            scope: "month:2026-05",
            clockMin: 1001,
            clockMax: 1010,
            bodyKind: "plain"
        )
        let line = try CommitOpMapper.encodeHeaderLine(header)
        let decoded = try CommitOpMapper.decodeLine(line)
        guard case .header(let parsed) = decoded else {
            XCTFail("expected header"); return
        }
        XCTAssertEqual(parsed, header)
    }

    func testAddAssetOpRoundTrip() throws {
        let body = CommitAddAssetBody(
            assetFingerprint: Data(repeating: 0xAB, count: 32),
            creationDateMs: 1778284800000,
            backedUpAtMs: 1778285000000,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: "2026/05/IMG_0001.HEIC",
                    logicalName: "IMG_0001.HEIC",
                    contentHash: Data(repeating: 0x01, count: 32),
                    fileSize: 12345,
                    resourceType: 1,
                    role: 0,
                    slot: 0,
                    crypto: nil
                )
            ]
        )
        let op = CommitOp(opSeq: 0, clock: 1001, body: .addAsset(body))
        let line = try CommitOpMapper.encodeOpLine(op)
        let decoded = try CommitOpMapper.decodeLine(line)
        guard case .op(let parsed) = decoded else {
            XCTFail("expected op"); return
        }
        XCTAssertEqual(parsed, op)
    }

    func testTombstoneOpRoundTrip() throws {
        let body = CommitTombstoneBody(
            assetFingerprint: Data(repeating: 0xDE, count: 32),
            reason: .verifyFailed
        )
        let op = CommitOp(opSeq: 7, clock: 9999, body: .tombstoneAsset(body))
        let line = try CommitOpMapper.encodeOpLine(op)
        let decoded = try CommitOpMapper.decodeLine(line)
        guard case .op(let parsed) = decoded else {
            XCTFail("expected op"); return
        }
        XCTAssertEqual(parsed, op)
    }

    func testEndRowDecode() throws {
        let line = try CommitOpMapper.encodeEndLine(sha256Hex: "abcd", rowCount: 5)
        let decoded = try CommitOpMapper.decodeLine(line)
        guard case .end(let sha, let count) = decoded else {
            XCTFail("expected end"); return
        }
        XCTAssertEqual(sha, "abcd")
        XCTAssertEqual(count, 5)
    }

    func testUnknownOpKindThrows() {
        let raw = #"{"t":"op","opSeq":0,"clock":1,"kind":"unknown","body":{}}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            XCTAssertEqual(err as? CommitWireError, .unknownOpKind("unknown"))
        }
    }

    func testEmptyHexFingerprint_addAsset_throws() {
        let raw = #"{"t":"op","opSeq":0,"clock":1,"kind":"addAsset","body":{"assetFingerprint":"","backedUpAtMs":1,"creationDateMs":null,"resources":[]}}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testEmptyHexFingerprint_tombstone_throws() {
        let raw = #"{"t":"op","opSeq":0,"clock":1,"kind":"tombstoneAsset","body":{"assetFingerprint":"","reason":"userDeleted"}}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testEmptyHexContentHash_throws() {
        let raw = #"{"t":"op","opSeq":0,"clock":1,"kind":"addAsset","body":{"assetFingerprint":"aa","backedUpAtMs":1,"creationDateMs":null,"resources":[{"physicalRemotePath":"p","logicalName":"l","contentHash":"","fileSize":1,"resourceType":1,"role":0,"slot":0,"crypto":null}]}}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testUnsupportedHeaderVersion_throws() {
        let raw = #"{"t":"header","v":99,"repoID":"r","writerID":"w","seq":1,"runID":"r","scope":"month:2026-01","clockMin":1,"clockMax":1,"bodyKind":"plain"}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.unsupportedVersion(99) = err else {
                XCTFail("expected unsupportedVersion(99), got \(err)"); return
            }
        }
    }

    func testHeaderRejectsBooleanVersion() {
        let raw = #"{"t":"header","v":true,"repoID":"r","writerID":"w","seq":1,"runID":"r","scope":"month:2026-01","clockMin":1,"clockMax":1,"bodyKind":"plain"}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.missingField = err else {
                XCTFail("expected .missingField (boolean rejection), got \(err)"); return
            }
        }
    }

    func testEndRowRejectsBooleanRowCount() {
        let raw = #"{"t":"end","sha256":"deadbeef","rowCount":true}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.missingField = err else {
                XCTFail("expected .missingField (boolean rejection), got \(err)"); return
            }
        }
    }

    func testHeaderRejectsFractionalClockValue() {
        let raw = #"{"t":"header","v":1,"repoID":"r","writerID":"w","seq":1,"runID":"r","scope":"month:2026-01","clockMin":1.9,"clockMax":2,"bodyKind":"plain"}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.missingField = err else {
                XCTFail("expected .missingField (rejection), got \(err)"); return
            }
        }
    }

    func testHeaderRejectsMalformedScope() {
        let raw = #"{"t":"header","v":1,"repoID":"r","writerID":"w","seq":1,"runID":"r","scope":"not-a-scope","clockMin":1,"clockMax":1,"bodyKind":"plain"}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsClockMinAboveClockMax() {
        let raw = #"{"t":"header","v":1,"repoID":"r","writerID":"w","seq":1,"runID":"r","scope":"month:2026-01","clockMin":10,"clockMax":5,"bodyKind":"plain"}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsEmptyRepoID() {
        let raw = #"{"t":"header","v":1,"repoID":"","writerID":"w","seq":1,"runID":"r","scope":"month:2026-01","clockMin":1,"clockMax":1,"bodyKind":"plain"}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.malformed = err else {
                XCTFail("expected .malformed for empty repoID, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsEmptyWriterID() {
        let raw = #"{"t":"header","v":1,"repoID":"r","writerID":"","seq":1,"runID":"r","scope":"month:2026-01","clockMin":1,"clockMax":1,"bodyKind":"plain"}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.malformed = err else {
                XCTFail("expected .malformed for empty writerID, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsUnknownBodyKind() {
        let raw = #"{"t":"header","v":1,"repoID":"r","writerID":"w","seq":1,"runID":"r","scope":"month:2026-01","clockMin":1,"clockMax":1,"bodyKind":"future-format"}"#
        XCTAssertThrowsError(try CommitOpMapper.decodeLine(raw)) { err in
            guard case CommitWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testCryptoOptionalRoundTrip() throws {
        let crypto = ResourceCryptoMetadata(scheme: "aes-gcm-256", payload: ["nonce": "abcdef"])
        let body = CommitAddAssetBody(
            assetFingerprint: Data(repeating: 0xFF, count: 32),
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: "p",
                    logicalName: "l",
                    contentHash: Data(repeating: 0x01, count: 32),
                    fileSize: 1,
                    resourceType: 1,
                    role: 0,
                    slot: 0,
                    crypto: crypto
                )
            ]
        )
        let op = CommitOp(opSeq: 0, clock: 1, body: .addAsset(body))
        let line = try CommitOpMapper.encodeOpLine(op)
        let decoded = try CommitOpMapper.decodeLine(line)
        guard case .op(let parsed) = decoded,
              case .addAsset(let parsedBody) = parsed.body else {
            XCTFail("expected addAsset op"); return
        }
        XCTAssertEqual(parsedBody.resources.first?.crypto, crypto)
    }

    func testTombstoneBasis_perWriterMaxSeq_aboveInt64Max_roundTrips() throws {
        let highSeq = UInt64(Int64.max) + 1
        let basis = TombstoneObservationBasis(
            perWriterMaxSeq: ["writer-high": highSeq, "writer-max": UInt64.max, "writer-low": 7],
            lamportWatermark: UInt64(Int64.max) + 42
        )
        let body = CommitTombstoneBody(
            assetFingerprint: Data(repeating: 0xBE, count: 32),
            reason: .verifyFailed,
            observedBasis: basis
        )
        let op = CommitOp(opSeq: 0, clock: UInt64(Int64.max) + 99, body: .tombstoneAsset(body))
        let line = try CommitOpMapper.encodeOpLine(op)
        let decoded = try CommitOpMapper.decodeLine(line)
        guard case .op(let parsed) = decoded,
              case .tombstoneAsset(let parsedBody) = parsed.body,
              let parsedBasis = parsedBody.observedBasis else {
            XCTFail("expected tombstone with basis, got \(decoded)")
            return
        }
        XCTAssertEqual(parsedBasis.perWriterMaxSeq["writer-high"], highSeq)
        XCTAssertEqual(parsedBasis.perWriterMaxSeq["writer-max"], UInt64.max)
        XCTAssertEqual(parsedBasis.perWriterMaxSeq["writer-low"], 7)
        XCTAssertEqual(parsedBasis.lamportWatermark, basis.lamportWatermark)
        XCTAssertEqual(parsed.clock, op.clock)
    }
}

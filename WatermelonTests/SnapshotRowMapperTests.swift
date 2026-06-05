import XCTest
@testable import Watermelon

final class SnapshotRowMapperTests: XCTestCase {
    func testHeaderWithCoveredRoundTrip() throws {
        let covered = CoveredRanges(rangesByWriter: [
            "writer-A": [ClosedSeqRange(low: 1, high: 100)],
            "writer-B": [ClosedSeqRange(low: 1, high: 5), ClosedSeqRange(low: 12, high: 20)]
        ])
        let header = SnapshotHeader(
            version: 1,
            scope: "month:2026-05",
            writerID: "writer-A",
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            covered: covered, createdAtMs: nil
        )
        let line = try SnapshotRowMapper.encodeHeaderLine(header)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .header(let parsed) = decoded else { XCTFail("header"); return }
        XCTAssertEqual(parsed.scope, header.scope)
        XCTAssertEqual(parsed.writerID, header.writerID)
        XCTAssertEqual(parsed.repoID, header.repoID)
        XCTAssertEqual(parsed.covered.rangesByWriter, covered.rangesByWriter)
    }

    func testLegacyHeaderDecodesWithNilAttestation() throws {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{}}"#
        let decoded = try SnapshotRowMapper.decodeLine(raw)
        guard case .header(let parsed) = decoded else { XCTFail("header"); return }
        XCTAssertNil(parsed.coverageAttestation, "a header without the key is legacy/unattested")
    }

    func testAttestedHeaderRoundTrip() throws {
        let header = SnapshotHeader(
            version: SnapshotHeader.checkpointVersion,
            scope: "month:2026-05",
            writerID: "writer-A",
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            covered: CoveredRanges(rangesByWriter: ["writer-A": [ClosedSeqRange(low: 1, high: 4)]]),
            createdAtMs: 123,
            coverageAttestation: SnapshotCoverageAttestation()
        )
        let line = try SnapshotRowMapper.encodeHeaderLine(header)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .header(let parsed) = decoded else { XCTFail("header"); return }
        XCTAssertEqual(parsed.coverageAttestation?.version, SnapshotCoverageAttestation.currentVersion)
        XCTAssertEqual(parsed.covered.rangesByWriter, header.covered.rangesByWriter)
    }

    /// Legacy (unattested) headers must serialize byte-identically to pre-A1a — no extra key.
    func testLegacyHeaderOmitsAttestationKey() throws {
        let header = SnapshotHeader(
            version: 1, scope: "month:2026-05", writerID: "writer-A",
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", covered: .empty, createdAtMs: nil
        )
        let line = try SnapshotRowMapper.encodeHeaderLine(header)
        XCTAssertFalse(line.contains("coverageAttestation"),
            "an unattested header must not emit the coverageAttestation key")
    }

    func testHeaderRejectsMalformedAttestation() {
        let raw = #"{"t":"header","v":2,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{},"coverageAttestation":"not-an-object"}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsUnsupportedAttestationVersion() {
        let raw = #"{"t":"header","v":2,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{},"coverageAttestation":{"v":2}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed for unsupported attestation version, got \(err)"); return
            }
        }
    }

    func testAssetRowRoundTrip() throws {
        let row = SnapshotAssetRow(
            assetFingerprint: TestFixtures.assetFingerprint(0x01),
            creationDateMs: 100,
            backedUpAtMs: 200,
            resourceCount: 3,
            totalFileSizeBytes: 999
        )
        let line = try SnapshotRowMapper.encodeAssetLine(row)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .asset(let parsed) = decoded else { XCTFail("asset"); return }
        XCTAssertEqual(parsed, row)
    }

    func testResourceRowRoundTripWithCrypto() throws {
        let row = SnapshotResourceRow(
            physicalRemotePath: "2026/05/IMG.HEIC",
            contentHash: Data(repeating: 0x10, count: 32),
            fileSize: 1024,
            resourceType: 1,
            creationDateMs: 100,
            backedUpAtMs: 200,
            crypto: ResourceCryptoMetadata(scheme: "aes-gcm-256", payload: [:])
        )
        let line = try SnapshotRowMapper.encodeResourceLine(row)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .resource(let parsed) = decoded else { XCTFail("resource"); return }
        XCTAssertEqual(parsed, row)
    }

    func testAssetResourceRoundTrip() throws {
        let row = SnapshotAssetResourceRow(
            assetFingerprint: TestFixtures.assetFingerprint(0x10),
            role: 1,
            slot: 0,
            resourceHash: Data(repeating: 0x20, count: 32),
            logicalName: "IMG_0001.HEIC"
        )
        let line = try SnapshotRowMapper.encodeAssetResourceLine(row)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .assetResource(let parsed) = decoded else { XCTFail("assetResource"); return }
        XCTAssertEqual(parsed, row)
    }

    /// Bad `covered` entries (non-array, wrong type, non-Int) must throw rather than
    /// silently degrade to empty — an "empty covered" snapshot would still get picked
    /// as baseline and yield wrong replay decisions.
    func testHeaderRejectsCoveredWithNonArrayPair() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{"writer-A":[[1,"abc"]]}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsCoveredWithStringValue() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{"writer-A":"not-an-array"}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsMissingRepoID() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","covered":{}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.missingField("repoID") = err else {
                XCTFail("expected missing repoID, got \(err)")
                return
            }
        }
    }

    func testHeaderRejectsExplicitEmptyRepoID() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"","covered":{}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsNonUUIDRepoID() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"not-a-uuid","covered":{}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// A stamp with seq 0 signals corruption; production writers do not emit it.
    func testAssetRowRejectsStampWithSeqZero() {
        let raw = #"{"t":"asset","r":{"assetFingerprint":"\#(String(repeating: "ab", count: 32))","backedUpAtMs":1,"resourceCount":1,"totalFileSizeBytes":1,"lastWriterID":"00000000-0000-0000-0000-000000000000","lastSeq":0,"lastClock":1}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// A stamp with non-canonical writerID could affect LWW tiebreaker ordering.
    func testAssetRowRejectsStampWithNonCanonicalWriterID() {
        let raw = #"{"t":"asset","r":{"assetFingerprint":"\#(String(repeating: "ab", count: 32))","backedUpAtMs":1,"resourceCount":1,"totalFileSizeBytes":1,"lastWriterID":"not-a-uuid","lastSeq":1,"lastClock":1}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsEmptyWriterID() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// Negative numbers in `covered` must be rejected rather than wrapping to ~UInt64.max
    /// via NSNumber bridging, which would silently absorb every later commit into "covered".
    func testHeaderRejectsNegativeCoveredValue() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{"writer-A":[[-1,5]]}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// Fractional NSNumber values must also be rejected — `uint64Value` truncates 1.9 to 1
    /// silently, so a malformed snapshot could shrink the covered range and cause commits
    /// inside the original range to be replayed.
    func testHeaderRejectsFractionalCoveredValue() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{"writer-A":[[1.9,5]]}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// No production writer emits seq 0; a covered range starting at 0 signals corruption
    /// and could cause the materializer to skip legitimate commits.
    func testHeaderRejectsCoveredRangeStartingAtZero() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{"writer-A":[[0,10]]}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// JSON `true`/`false` bridges through `as? UInt64` (and the NSNumber roundtrip)
    /// as 1/0. A malformed `covered` value of `[[true,true]]` would otherwise be
    /// accepted as the range `[1,1]` and silently mark writer-A seq 1 as covered,
    /// causing the materializer to skip a commit whose effects aren't in the snapshot.
    func testHeaderRejectsBooleanCoveredValue_true() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{"writer-A":[[true,true]]}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsBooleanCoveredValue_false() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{"writer-A":[[false,false]]}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// JSON `true` for the header `v` field also bridges to Int as 1, the current
    /// version. The decoder must reject it rather than silently accepting a v=true
    /// header as a valid v1 row.
    func testHeaderRejectsBooleanVersion() {
        let raw = #"{"t":"header","v":true,"scope":"month:2026-05","writerID":"w","repoID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","covered":{}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            // CFBoolean rejection lands as missingField via mapValidation.
            guard case SnapshotWireError.missingField = err else {
                XCTFail("expected .missingField (boolean rejection), got \(err)"); return
            }
        }
    }

    /// Legitimate high UInt64 covered ranges (> Int64.max) must still parse — the
    /// validator's NSNumber roundtrip covers the full UInt64 range.
    func testHeaderAcceptsHighUInt64CoveredValue() throws {
        let high = UInt64(Int64.max) + 1
        let raw = "{\"t\":\"header\",\"v\":1,\"scope\":\"month:2026-05\",\"writerID\":\"w\",\"repoID\":\"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\",\"covered\":{\"writer-A\":[[\(high),\(high)]]}}"
        let row = try SnapshotRowMapper.decodeLine(raw)
        guard case .header(let parsed) = row else { XCTFail("expected header"); return }
        let ranges = parsed.covered.rangesByWriter["writer-A"]
        XCTAssertEqual(ranges?.first?.low, high)
        XCTAssertEqual(ranges?.first?.high, high)
    }

    func testDeletedKeyRoundTrip() throws {
        let row = SnapshotDeletedKeyRow(keyType: .asset, keyValue: String(repeating: "ab", count: 32))
        let line = try SnapshotRowMapper.encodeDeletedKeyLine(row)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .deletedKey(let parsed) = decoded else { XCTFail("deletedKey"); return }
        XCTAssertEqual(parsed, row)
    }

    func testDeletedKeyNonAssetKeyTypeRequiresStamp() throws {
        let raw = #"{"t":"deleted_key","r":{"keyType":"resource","keyValue":"some-resource-id","lastWriterID":"00000000-0000-0000-0000-000000000000","lastSeq":1,"lastClock":1}}"#
        let decoded = try SnapshotRowMapper.decodeLine(raw)
        guard case .deletedKey(let row) = decoded else { XCTFail("deletedKey"); return }
        XCTAssertEqual(row.keyType, .resource)
        XCTAssertEqual(row.keyValue, "some-resource-id")
        XCTAssertEqual(row.stamp, OpStamp(writerID: "00000000-0000-0000-0000-000000000000", seq: 1, clock: 1))
    }

    /// asset deletedKey is the dedup-suppression boundary; truncated hex would collide
    /// trivially and let a poisoned key shadow real fingerprints.
    func testDeletedKeyAssetRejectsShortHex() {
        let raw = #"{"t":"deleted_key","r":{"keyType":"asset","keyValue":"abc123"}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testDeletedKeyAssetRejectsNon32ByteHex() {
        let raw = "{\"t\":\"deleted_key\",\"r\":{\"keyType\":\"asset\",\"keyValue\":\"\(String(repeating: "ab", count: 16))\"}}"
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// resource/assetResource keyType currently uses path strings, not hashes — the
    /// 32-byte rule must apply ONLY to asset keys.
    func testDeletedKeyResourceAcceptsArbitraryString() throws {
        let row = SnapshotDeletedKeyRow(keyType: .resource, keyValue: "2026/05/IMG_0001.HEIC")
        let line = try SnapshotRowMapper.encodeDeletedKeyLine(row)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .deletedKey(let parsed) = decoded else { XCTFail("deletedKey"); return }
        XCTAssertEqual(parsed, row)
    }
}

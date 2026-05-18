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
            repoID: "repo-test-id",
            covered: covered
        )
        let line = try SnapshotRowMapper.encodeHeaderLine(header)
        let decoded = try SnapshotRowMapper.decodeLine(line)
        guard case .header(let parsed) = decoded else { XCTFail("header"); return }
        XCTAssertEqual(parsed.scope, header.scope)
        XCTAssertEqual(parsed.writerID, header.writerID)
        XCTAssertEqual(parsed.repoID, header.repoID)
        XCTAssertEqual(parsed.covered.rangesByWriter, covered.rangesByWriter)
    }

    func testAssetRowRoundTrip() throws {
        let row = SnapshotAssetRow(
            assetFingerprint: Data(repeating: 0x01, count: 32),
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
            assetFingerprint: Data(repeating: 0x10, count: 32),
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
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"r","covered":{"writer-A":[[1,"abc"]]}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsCoveredWithStringValue() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"r","covered":{"writer-A":"not-an-array"}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderAcceptsMissingRepoIDAsLegacy() throws {
        // Legacy snapshots (pre-Iter4) don't carry repoID; decoder yields empty string
        // and materializer accepts (foreign-id filter then no-ops on empty).
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","covered":{}}"#
        let row = try SnapshotRowMapper.decodeLine(raw)
        guard case .header(let header) = row else { XCTFail("expected header"); return }
        XCTAssertEqual(header.repoID, "")
    }

    func testHeaderRejectsExplicitEmptyRepoID() {
        // Field-absent legacy snapshots are tolerated; explicit empty string is corruption.
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"","covered":{}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsEmptyWriterID() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"","repoID":"r","covered":{}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    /// Negative numbers in `covered` must be rejected rather than wrapping to ~UInt64.max
    /// via NSNumber bridging, which would silently absorb every later commit into "covered".
    func testHeaderRejectsNegativeCoveredValue() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"r","covered":{"writer-A":[[-1,5]]}}"#
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
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"r","covered":{"writer-A":[[1.9,5]]}}"#
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
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"r","covered":{"writer-A":[[true,true]]}}"#
        XCTAssertThrowsError(try SnapshotRowMapper.decodeLine(raw)) { err in
            guard case SnapshotWireError.malformed = err else {
                XCTFail("expected .malformed, got \(err)"); return
            }
        }
    }

    func testHeaderRejectsBooleanCoveredValue_false() {
        let raw = #"{"t":"header","v":1,"scope":"month:2026-05","writerID":"w","repoID":"r","covered":{"writer-A":[[false,false]]}}"#
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
        let raw = #"{"t":"header","v":true,"scope":"month:2026-05","writerID":"w","repoID":"r","covered":{}}"#
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
        let raw = "{\"t\":\"header\",\"v\":1,\"scope\":\"month:2026-05\",\"writerID\":\"w\",\"repoID\":\"r\",\"covered\":{\"writer-A\":[[\(high),\(high)]]}}"
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

    /// Forward-compat: stamps on non-asset keyTypes (future V3 resource-level tombstones)
    /// must NOT make the whole snapshot unreadable. Decoder accepts the row, strips the
    /// stamp (asset-only field semantically), and the materializer's skip-by-keyType
    /// handles the rest.
    func testDeletedKeyNonAssetKeyTypeAcceptsAndIgnoresStamp() throws {
        let raw = #"{"t":"deleted_key","r":{"keyType":"resource","keyValue":"some-resource-id","lastWriterID":"w","lastSeq":1,"lastClock":1}}"#
        let decoded = try SnapshotRowMapper.decodeLine(raw)
        guard case .deletedKey(let row) = decoded else { XCTFail("deletedKey"); return }
        XCTAssertEqual(row.keyType, .resource)
        XCTAssertEqual(row.keyValue, "some-resource-id")
        XCTAssertNil(row.stamp, "non-asset keyType drops the stamp at decode")
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

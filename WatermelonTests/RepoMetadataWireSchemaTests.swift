import XCTest
@testable import Watermelon

final class RepoMetadataWireSchemaTests: XCTestCase {
    func testVersionManifestWire_roundTripsAndRejectsBooleanFormatVersion() throws {
        let wire = VersionManifestWire(
            formatVersion: RepoLayout.formatVersion,
            minAppVersion: "1.0",
            createdAtMs: 123,
            createdByWriter: "writer"
        )
        let parsed = try VersionManifestWire(data: wire.encode())

        XCTAssertEqual(parsed.formatVersion, RepoLayout.formatVersion)
        XCTAssertEqual(parsed.minAppVersion, "1.0")
        XCTAssertEqual(parsed.createdAtMs, 123)
        XCTAssertEqual(parsed.createdByWriter, "writer")

        let malformed = try JSONSerialization.data(withJSONObject: [
            "format_version": true,
            "created_at_ms": 1
        ])
        XCTAssertThrowsError(try VersionManifestWire(data: malformed))
    }

    func testVersionManifestWire_malformedAdvisoryTimestampBecomesNil() throws {
        let data = try JSONSerialization.data(withJSONObject: [
            "format_version": RepoLayout.formatVersion,
            "created_at_ms": true
        ])
        let parsed = try VersionManifestWire(data: data)
        XCTAssertNil(parsed.createdAtMs)
    }

    func testMigrationMarkerWire_legacyMissingVersionDefaultsMissingPhaseToPhase1() throws {
        let parsed = try MigrationMarkerWire(data: Data("{}".utf8))
        XCTAssertEqual(parsed.phase, .phase1)
        XCTAssertNil(parsed.writerID)
    }

    func testMigrationMarkerWire_presentMalformedOrUnsupportedVersionRejects() throws {
        let boolVersion = try JSONSerialization.data(withJSONObject: ["v": true, "phase": 1])
        XCTAssertThrowsError(try MigrationMarkerWire(data: boolVersion))

        let futureVersion = try JSONSerialization.data(withJSONObject: ["v": 999, "phase": 1])
        XCTAssertThrowsError(try MigrationMarkerWire(data: futureVersion)) { error in
            guard case MigrationMarkerError.unsupportedVersion(let raw) = error else {
                XCTFail("expected unsupportedVersion, got \(error)")
                return
            }
            XCTAssertEqual(raw, 999)
        }
    }

    func testMigrationMarkerWire_presentVersionRequiresValidPhase() throws {
        let missingPhase = try JSONSerialization.data(withJSONObject: ["v": 2])
        XCTAssertThrowsError(try MigrationMarkerWire(data: missingPhase)) { error in
            guard case MigrationMarkerError.phaseWrongType = error else {
                XCTFail("expected phaseWrongType, got \(error)")
                return
            }
        }

        let booleanPhase = try JSONSerialization.data(withJSONObject: ["v": 2, "phase": false])
        XCTAssertThrowsError(try MigrationMarkerWire(data: booleanPhase)) { error in
            guard case MigrationMarkerError.phaseWrongType = error else {
                XCTFail("expected phaseWrongType, got \(error)")
                return
            }
        }
    }

    func testIdentityClaimWire_acceptsMissingVersionAndRejectsBadPresentVersion() throws {
        let legacy = try JSONSerialization.data(withJSONObject: [
            "repo_id": "repo",
            "created_at_ms": 1,
            "writer_id": "writer"
        ])
        let parsed = try IdentityClaimWire(data: legacy)
        XCTAssertEqual(parsed.repoID, "repo")

        let unsupported = try JSONSerialization.data(withJSONObject: [
            "v": 999,
            "repo_id": "repo",
            "created_at_ms": 1,
            "writer_id": "writer"
        ])
        XCTAssertThrowsError(try IdentityClaimWire(data: unsupported))

        let malformed = try JSONSerialization.data(withJSONObject: [
            "v": true,
            "repo_id": "repo",
            "created_at_ms": 1,
            "writer_id": "writer"
        ])
        XCTAssertThrowsError(try IdentityClaimWire(data: malformed))
    }

    func testRepoIdentityFinalizationAndCacheWireRequireRepoIDOnlyForAuthority() throws {
        let final = try RepoIdentityFinalizationWire(data: Data(#"{"repo_id":"repo"}"#.utf8))
        XCTAssertEqual(final.repoID, "repo")
        XCTAssertNil(final.formatVersion)

        let cache = try RepoCacheWire(repoID: "repo", createdAtMs: 10, createdByWriter: "writer").encode()
        XCTAssertEqual(try RepoCacheWire(data: cache).repoID, "repo")

        XCTAssertThrowsError(try RepoIdentityFinalizationWire(data: Data(#"{"v":999,"repo_id":"repo"}"#.utf8)))
        XCTAssertThrowsError(try RepoCacheWire(data: Data(#"{"v":true,"repo_id":"repo"}"#.utf8)))
    }
}

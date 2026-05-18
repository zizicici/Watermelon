import XCTest
@testable import Watermelon

final class RemoteIndexFormatRouteDecisionTests: XCTestCase {
    func testV2AllowsPreMaterialized() throws {
        let route = try RemoteIndexFormatRouteDecision.decide(
            inspection: .v2(formatVersion: 2),
            alreadyV2: false,
            expectV2: false
        )
        XCTAssertEqual(route, .v2(allowPreMaterialized: true))
    }

    func testPendingMigrationCleanupDisallowsPreMaterialized() throws {
        let route = try RemoteIndexFormatRouteDecision.decide(
            inspection: .v2WithPendingMigrationCleanup(formatVersion: 2, ownerWriterID: "writer"),
            alreadyV2: false,
            expectV2: false
        )
        XCTAssertEqual(route, .v2(allowPreMaterialized: false))
    }

    func testV2WithV1ManifestsRequiresForegroundMigration() {
        XCTAssertThrowsError(try RemoteIndexFormatRouteDecision.decide(
            inspection: .v2WithV1Manifests(formatVersion: 2),
            alreadyV2: false,
            expectV2: false
        )) { error in
            guard case BackupCompatibilityError.requiresForegroundMigration = error else {
                return XCTFail("expected requiresForegroundMigration")
            }
        }
    }

    func testV1WithV2ExpectationRequiresForegroundMigration() {
        XCTAssertThrowsError(try RemoteIndexFormatRouteDecision.decide(
            inspection: .v1,
            alreadyV2: false,
            expectV2: true
        )) { error in
            guard case BackupCompatibilityError.requiresForegroundMigration = error else {
                return XCTFail("expected requiresForegroundMigration")
            }
        }
    }

    func testV1AfterV2RequiresForegroundMigration() {
        XCTAssertThrowsError(try RemoteIndexFormatRouteDecision.decide(
            inspection: .v1,
            alreadyV2: true,
            expectV2: false
        )) { error in
            guard case BackupCompatibilityError.requiresForegroundMigration = error else {
                return XCTFail("expected requiresForegroundMigration")
            }
        }
    }

    func testFreshWithV2ExpectationIsDamaged() {
        XCTAssertThrowsError(try RemoteIndexFormatRouteDecision.decide(
            inspection: .fresh,
            alreadyV2: false,
            expectV2: true
        )) { error in
            guard case BackupCompatibilityError.damagedV2Repo = error else {
                return XCTFail("expected damagedV2Repo")
            }
        }
    }

    func testFreshAfterV2IsDamaged() {
        XCTAssertThrowsError(try RemoteIndexFormatRouteDecision.decide(
            inspection: .fresh,
            alreadyV2: true,
            expectV2: false
        )) { error in
            guard case BackupCompatibilityError.damagedV2Repo = error else {
                return XCTFail("expected damagedV2Repo")
            }
        }
    }

    func testUnsupportedPreservesMinAppVersion() {
        XCTAssertThrowsError(try RemoteIndexFormatRouteDecision.decide(
            inspection: .unsupported(minAppVersion: "9.9.9"),
            alreadyV2: false,
            expectV2: false
        )) { error in
            guard case BackupCompatibilityError.remoteFormatUnsupported(let minAppVersion) = error else {
                return XCTFail("expected remoteFormatUnsupported")
            }
            XCTAssertEqual(minAppVersion, "9.9.9")
        }
    }
}

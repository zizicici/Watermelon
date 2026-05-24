import XCTest
@testable import Watermelon

/// Pins the AssetIntegrityState ↔ VerifyMonthReportKind adapter so the cleanup
/// predicate cannot drift across the two vocabularies. Read alongside
/// RemoteAssetIntegrityClassifierTests (state-side coverage) and
/// RepoVerifyMonthServiceTests (end-to-end verify coverage).
final class AssetIntegrityKindAdapterTests: XCTestCase {

    func testKindFromState_healthy_returnsNil() {
        XCTAssertNil(VerifyMonthReportKind(from: .healthy))
    }

    func testKindFromState_allFiveNonHealthyCases_mapToExpectedKind() {
        XCTAssertEqual(VerifyMonthReportKind(from: .phantom), .phantomAsset)
        XCTAssertEqual(VerifyMonthReportKind(from: .fullyMissing), .allResourcesGone)
        XCTAssertEqual(VerifyMonthReportKind(from: .metadataOnlyLeft), .metadataOnlyLeft)
        XCTAssertEqual(
            VerifyMonthReportKind(from: .fingerprintMismatch(recomputed: TestFixtures.fingerprint(0x01))),
            .fingerprintMismatch
        )
        XCTAssertEqual(
            VerifyMonthReportKind(from: .partiallyMissing(missingHashes: [TestFixtures.fingerprint(0x02)])),
            .partiallyMissing
        )
    }

    func testKindAllowsCleanup_matchesDecisionSet() {
        XCTAssertTrue(VerifyMonthReportKind.phantomAsset.allowsCleanup)
        XCTAssertTrue(VerifyMonthReportKind.allResourcesGone.allowsCleanup)
        XCTAssertTrue(VerifyMonthReportKind.metadataOnlyLeft.allowsCleanup)
        XCTAssertFalse(VerifyMonthReportKind.partiallyMissing.allowsCleanup)
        XCTAssertFalse(VerifyMonthReportKind.fingerprintMismatch.allowsCleanup)
        XCTAssertFalse(VerifyMonthReportKind.verificationIncomplete.allowsCleanup)
    }

    func testItemAllowsCleanup_delegatesToKind() {
        let fp = TestFixtures.fingerprint(0xAA)
        let cases: [VerifyMonthReportKind] = [
            .phantomAsset, .allResourcesGone, .metadataOnlyLeft,
            .partiallyMissing, .fingerprintMismatch, .verificationIncomplete,
        ]
        for kind in cases {
            let item = VerifyMonthReportItem(kind: kind, assetFingerprint: fp, detail: nil)
            XCTAssertEqual(item.allowsCleanup, kind.allowsCleanup, "\(kind) parity")
        }
    }

    func testStateAllowsCleanup_delegatesThroughKindAdapter() {
        let states: [AssetIntegrityState] = [
            .healthy,
            .phantom,
            .fullyMissing,
            .metadataOnlyLeft,
            .fingerprintMismatch(recomputed: TestFixtures.fingerprint(0x11)),
            .partiallyMissing(missingHashes: [TestFixtures.fingerprint(0x22)]),
        ]
        for state in states {
            let viaAdapter = VerifyMonthReportKind(from: state)?.allowsCleanup ?? false
            XCTAssertEqual(state.allowsCleanup, viaAdapter, "\(state) cleanup parity through adapter")
        }
    }

    func testItemFactory_healthy_returnsNil() {
        let result = VerifyMonthReportItem.from(
            state: .healthy,
            fingerprint: TestFixtures.fingerprint(0x33),
            linkCount: 0
        )
        XCTAssertNil(result)
    }

    func testItemFactory_detailStrings_matchPreEditFormat() {
        let fp = TestFixtures.fingerprint(0x44)

        let phantom = VerifyMonthReportItem.from(state: .phantom, fingerprint: fp, linkCount: 0)
        XCTAssertEqual(phantom?.kind, .phantomAsset)
        XCTAssertEqual(phantom?.detail, "no asset_resources rows; fingerprint=\(fp.hexString)")

        let fullyMissing = VerifyMonthReportItem.from(state: .fullyMissing, fingerprint: fp, linkCount: 3)
        XCTAssertEqual(fullyMissing?.kind, .allResourcesGone)
        XCTAssertEqual(fullyMissing?.detail, "all 3 resources missing on remote")

        let metadataOnly = VerifyMonthReportItem.from(state: .metadataOnlyLeft, fingerprint: fp, linkCount: 0)
        XCTAssertEqual(metadataOnly?.kind, .metadataOnlyLeft)
        XCTAssertEqual(metadataOnly?.detail, "only adjustment-data roles remain")

        let mismatch = VerifyMonthReportItem.from(
            state: .fingerprintMismatch(recomputed: TestFixtures.fingerprint(0x55)),
            fingerprint: fp,
            linkCount: 2
        )
        XCTAssertEqual(mismatch?.kind, .fingerprintMismatch)
        XCTAssertEqual(mismatch?.detail, "stored fp does not match recomputed from 2 link(s)")

        let partial = VerifyMonthReportItem.from(
            state: .partiallyMissing(missingHashes: [
                TestFixtures.fingerprint(0x66),
                TestFixtures.fingerprint(0x77),
            ]),
            fingerprint: fp,
            linkCount: 5
        )
        XCTAssertEqual(partial?.kind, .partiallyMissing)
        XCTAssertEqual(partial?.detail, "2/5 resources missing")
    }

    func testKindTombstoneReason_matchesExpectedMapping() {
        XCTAssertEqual(VerifyMonthReportKind.phantomAsset.tombstoneReason, .manifestOrphan)
        XCTAssertEqual(VerifyMonthReportKind.metadataOnlyLeft.tombstoneReason, .manifestOrphan)
        XCTAssertEqual(VerifyMonthReportKind.allResourcesGone.tombstoneReason, .verifyFailed)
        XCTAssertNil(VerifyMonthReportKind.partiallyMissing.tombstoneReason)
        XCTAssertNil(VerifyMonthReportKind.fingerprintMismatch.tombstoneReason)
        XCTAssertNil(VerifyMonthReportKind.verificationIncomplete.tombstoneReason)
    }

    func testKindTombstoneReason_nilParityWithAllowsCleanup() {
        let cases: [VerifyMonthReportKind] = [
            .phantomAsset, .allResourcesGone, .metadataOnlyLeft,
            .partiallyMissing, .fingerprintMismatch, .verificationIncomplete,
        ]
        for kind in cases {
            XCTAssertEqual(
                kind.tombstoneReason != nil,
                kind.allowsCleanup,
                "\(kind) tombstoneReason-nil parity with allowsCleanup"
            )
        }
    }
}

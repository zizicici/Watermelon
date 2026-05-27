import XCTest
@testable import Watermelon

/// Pins `isIncomplete(...) == !classify(...).isHealthy` across every
/// AssetIntegrityState case and across both surviving overloads.
final class RemoteAssetIntegrityClassifierIsIncompleteTests: XCTestCase {
    private func link(role: Int, slot: Int = 0, hash: Data, fp: AssetFingerprint) -> RemoteAssetResourceLink {
        RemoteAssetResourceLink(
            year: 2026, month: 1,
            assetFingerprint: fp, resourceHash: hash,
            role: role, slot: slot, logicalName: "x"
        )
    }

    private func computedFP(role: Int = ResourceTypeCode.photo, slot: Int = 0, hash: Data) -> AssetFingerprint {
        BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: role, slot: slot, contentHash: hash)]
        )
    }

    // MARK: - Fixture builders (shared by per-case tests and the overload-parity test)

    private struct Fixture {
        let name: String
        let assetFingerprint: AssetFingerprint
        let remoteLinks: [RemoteAssetResourceLink]
        let isResourceAvailable: (Data) -> Bool
    }

    private func makeHealthyFixture() -> Fixture {
        let h = TestFixtures.fingerprint(0xAA)
        let fp = computedFP(hash: h)
        return Fixture(
            name: "healthy",
            assetFingerprint: fp,
            remoteLinks: [link(role: ResourceTypeCode.photo, hash: h, fp: fp)],
            isResourceAvailable: { _ in true }
        )
    }

    private func makePhantomFixture() -> Fixture {
        Fixture(
            name: "phantom",
            assetFingerprint: TestFixtures.assetFingerprint(0x10),
            remoteLinks: [],
            isResourceAvailable: { _ in true }
        )
    }

    private func makeFullyMissingFixture() -> Fixture {
        let h = TestFixtures.fingerprint(0xBB)
        let fp = computedFP(hash: h)
        return Fixture(
            name: "fullyMissing",
            assetFingerprint: fp,
            remoteLinks: [link(role: ResourceTypeCode.photo, hash: h, fp: fp)],
            isResourceAvailable: { _ in false }
        )
    }

    private func makePartiallyMissingFixture() -> Fixture {
        let presentHash = TestFixtures.fingerprint(0xC1)
        let missingHash = TestFixtures.fingerprint(0xC2)
        let fp = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: [
            (role: ResourceTypeCode.photo, slot: 0, contentHash: presentHash),
            (role: ResourceTypeCode.video, slot: 0, contentHash: missingHash),
        ])
        return Fixture(
            name: "partiallyMissing",
            assetFingerprint: fp,
            remoteLinks: [
                link(role: ResourceTypeCode.photo, hash: presentHash, fp: fp),
                link(role: ResourceTypeCode.video, hash: missingHash, fp: fp),
            ],
            isResourceAvailable: { $0 == presentHash }
        )
    }

    private func makeMetadataOnlyLeftFixture() -> Fixture {
        let metadataHash = TestFixtures.fingerprint(0xD1)
        let missingPhotoHash = TestFixtures.fingerprint(0xD2)
        let fp = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: [
            (role: ResourceTypeCode.photo, slot: 0, contentHash: missingPhotoHash),
            (role: ResourceTypeCode.adjustmentData, slot: 0, contentHash: metadataHash),
        ])
        return Fixture(
            name: "metadataOnlyLeft",
            assetFingerprint: fp,
            remoteLinks: [
                link(role: ResourceTypeCode.photo, hash: missingPhotoHash, fp: fp),
                link(role: ResourceTypeCode.adjustmentData, hash: metadataHash, fp: fp),
            ],
            isResourceAvailable: { $0 == metadataHash }
        )
    }

    private func makeFingerprintMismatchFixture() -> Fixture {
        let h = TestFixtures.fingerprint(0xE1)
        let wrongFP = TestFixtures.assetFingerprint(0xEF)
        return Fixture(
            name: "fingerprintMismatch",
            assetFingerprint: wrongFP,
            remoteLinks: [link(role: ResourceTypeCode.photo, hash: h, fp: wrongFP)],
            isResourceAvailable: { _ in true }
        )
    }

    // MARK: - Per-case 3-part equivalence tests

    func testHealthy_isIncompleteMatchesClassify() {
        let fixture = makeHealthyFixture()
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(state, .healthy)
        let isIncomplete = RemoteAssetIntegrityClassifier.isIncomplete(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(isIncomplete, !state.isHealthy,
                       "isIncomplete(...) must equal !classify(...).isHealthy")
        XCTAssertFalse(isIncomplete, "healthy must report isIncomplete=false")
    }

    func testPhantom_isIncompleteMatchesClassify() {
        let fixture = makePhantomFixture()
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(state, .phantom)
        let isIncomplete = RemoteAssetIntegrityClassifier.isIncomplete(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(isIncomplete, !state.isHealthy,
                       "isIncomplete(...) must equal !classify(...).isHealthy")
        XCTAssertTrue(isIncomplete, "phantom must report isIncomplete=true")
    }

    func testFullyMissing_isIncompleteMatchesClassify() {
        let fixture = makeFullyMissingFixture()
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(state, .fullyMissing)
        let isIncomplete = RemoteAssetIntegrityClassifier.isIncomplete(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(isIncomplete, !state.isHealthy,
                       "isIncomplete(...) must equal !classify(...).isHealthy")
        XCTAssertTrue(isIncomplete, "fullyMissing must report isIncomplete=true")
    }

    func testPartiallyMissing_isIncompleteMatchesClassify() {
        let fixture = makePartiallyMissingFixture()
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        guard case .partiallyMissing(let missing) = state else {
            XCTFail("expected partiallyMissing, got \(state)")
            return
        }
        XCTAssertEqual(missing, [TestFixtures.fingerprint(0xC2)],
                       "partiallyMissing payload must list the video hash that was absent")
        let isIncomplete = RemoteAssetIntegrityClassifier.isIncomplete(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(isIncomplete, !state.isHealthy,
                       "isIncomplete(...) must equal !classify(...).isHealthy")
        XCTAssertTrue(isIncomplete, "partiallyMissing must report isIncomplete=true")
    }

    func testMetadataOnlyLeft_isIncompleteMatchesClassify() {
        let fixture = makeMetadataOnlyLeftFixture()
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(state, .metadataOnlyLeft)
        let isIncomplete = RemoteAssetIntegrityClassifier.isIncomplete(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(isIncomplete, !state.isHealthy,
                       "isIncomplete(...) must equal !classify(...).isHealthy")
        XCTAssertTrue(isIncomplete, "metadataOnlyLeft must report isIncomplete=true")
    }

    func testFingerprintMismatch_isIncompleteMatchesClassify() {
        let fixture = makeFingerprintMismatchFixture()
        let state = RemoteAssetIntegrityClassifier.classify(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        guard case .fingerprintMismatch(let recomputed) = state else {
            XCTFail("expected fingerprintMismatch, got \(state)")
            return
        }
        XCTAssertEqual(recomputed, computedFP(hash: TestFixtures.fingerprint(0xE1)),
                       "fingerprintMismatch payload must carry the recomputed fingerprint")
        let isIncomplete = RemoteAssetIntegrityClassifier.isIncomplete(
            assetFingerprint: fixture.assetFingerprint,
            links: fixture.remoteLinks,
            isResourceAvailable: fixture.isResourceAvailable
        )
        XCTAssertEqual(isIncomplete, !state.isHealthy,
                       "isIncomplete(...) must equal !classify(...).isHealthy")
        XCTAssertTrue(isIncomplete, "fingerprintMismatch must report isIncomplete=true")
    }

    // MARK: - Overload parity

    func testOverloadParity_AssetIntegrityLink_matchesRemoteAssetResourceLink() {
        let fixtures: [Fixture] = [
            makeHealthyFixture(),
            makePhantomFixture(),
            makeFullyMissingFixture(),
            makePartiallyMissingFixture(),
            makeMetadataOnlyLeftFixture(),
            makeFingerprintMismatchFixture(),
        ]

        for fixture in fixtures {
            let integrityLinks = fixture.remoteLinks.map(\.integrityLink)

            let boolFromRemoteLink = RemoteAssetIntegrityClassifier.isIncomplete(
                assetFingerprint: fixture.assetFingerprint,
                links: fixture.remoteLinks,
                isResourceAvailable: fixture.isResourceAvailable
            )
            let boolFromIntegrityLink = RemoteAssetIntegrityClassifier.isIncomplete(
                assetFingerprint: fixture.assetFingerprint,
                links: integrityLinks,
                isResourceAvailable: fixture.isResourceAvailable
            )

            XCTAssertEqual(
                boolFromRemoteLink, boolFromIntegrityLink,
                "\(fixture.name): isIncomplete([AssetIntegrityLink]) must equal isIncomplete([RemoteAssetResourceLink])"
            )
        }
    }
}

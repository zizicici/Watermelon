import XCTest
@testable import Watermelon

// Pins the live-cache probes the browser delete gates re-verify against when the committed presence
// build is stale (a mid-flight in-place sync applied the asset's month after the build).
final class RemoteSnapshotFingerprintProbeTests: XCTestCase {
    private let monthKey = LibraryMonthKey(year: 2024, month: 3)
    private let fingerprint = Data([0xAA, 0x01])

    private func seed(_ cache: RemoteLibrarySnapshotCache) {
        let asset = TestFixtures.remoteAsset(year: 2024, month: 3, fingerprint: fingerprint)
        let link = TestFixtures.remoteLink(year: 2024, month: 3, assetFingerprint: fingerprint, resourceHash: Data([0x01]))
        let resource = TestFixtures.remoteResource(year: 2024, month: 3, contentHash: Data([0x01]), fileSize: 10, fileName: "a.jpg")
        cache.replaceMonth(monthKey, resources: [resource], assets: [asset], assetResourceLinks: [link])
    }

    func testProbeSeesMonthAppliedAfterAPresenceBuild() {
        let cache = RemoteLibrarySnapshotCache()
        cache.setProfileKey("profile-a")
        let builtRevision = cache.currentRevision()
        XCTAssertFalse(cache.containsAssetFingerprint(fingerprint).contains)

        seed(cache)   // the in-place sync applies the month after the presence build
        let probe = cache.containsAssetFingerprint(fingerprint)
        XCTAssertTrue(probe.contains)
        XCTAssertEqual(probe.profileKey, "profile-a")
        XCTAssertNotEqual(cache.currentRevision(), builtRevision)

        let otherMonth = LibraryMonthKey(year: 2024, month: 4)
        cache.replaceMonth(otherMonth, resources: [],
            assets: [TestFixtures.remoteAsset(year: 2024, month: 4, fingerprint: fingerprint)], assetResourceLinks: [])
        cache.removeMonth(monthKey)
        XCTAssertTrue(cache.containsAssetFingerprint(fingerprint).contains)
        cache.removeMonth(otherMonth)
        XCTAssertFalse(cache.containsAssetFingerprint(fingerprint).contains)
    }

    func testProbesDoNotConsumePostResetFullSnapshot() {
        let cache = RemoteLibrarySnapshotCache()
        seed(cache)
        let before = cache.state(since: nil)
        cache.reset()
        seed(cache)
        // Probes cannot consume another reader's full-rebuild boundary.
        _ = cache.currentRevision()
        _ = cache.containsAssetFingerprint(fingerprint)
        let after = cache.state(since: before.revision)
        XCTAssertTrue(after.isFullSnapshot)
    }

    func testNilBaseReadsDoNotConsumePostResetFullSnapshot() {
        let cache = RemoteLibrarySnapshotCache()
        seed(cache)
        let before = cache.state(since: nil)
        cache.reset()
        seed(cache)
        // A Browser full read cannot consume Home's rebuild boundary.
        let browserRead = cache.state(since: nil)
        XCTAssertTrue(browserRead.isFullSnapshot)
        let after = cache.state(since: before.revision)
        XCTAssertTrue(after.isFullSnapshot)
    }

    func testLivePresenceClassification() {
        // A foreign-tagged or untagged (mid-switch reset) live cache never confirms absence — the delete
        // gates must not report an un-executed delete as done during a connect-to-B window.
        XCTAssertEqual(LibraryPresenceIndex.classifyLivePresence(contains: false, liveProfileKey: "b", currentKey: "a"), .unknown)
        XCTAssertEqual(LibraryPresenceIndex.classifyLivePresence(contains: true, liveProfileKey: "b", currentKey: "a"), .unknown)
        XCTAssertEqual(LibraryPresenceIndex.classifyLivePresence(contains: false, liveProfileKey: nil, currentKey: "a"), .unknown)
        XCTAssertEqual(LibraryPresenceIndex.classifyLivePresence(contains: true, liveProfileKey: "a", currentKey: "a"), .present)
        XCTAssertEqual(LibraryPresenceIndex.classifyLivePresence(contains: false, liveProfileKey: "a", currentKey: "a"), .absent)
    }
}

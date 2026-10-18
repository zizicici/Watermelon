import XCTest
@testable import Watermelon

final class RemoteSnapshotCacheTests: XCTestCase {
    private let year = 2024
    private let month = 3
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: month) }

    func testScanKeepsIndependentMonthsButRejectsChangedMonthsAndReplacedContexts() throws {
        let cache = RemoteLibrarySnapshotCache()
        let scan = try cache.beginScan(profileKey: "a", allowContextSwitch: true)
        let april = LibraryMonthKey(year: year, month: 4)
        let committed = RemoteLibraryMonthDelta(month: monthKey,
            resources: [resource("new.jpg", hashByte: 2, size: 200)], assets: [], assetResourceLinks: [])
        cache.snapshotObserver(profileKey: "a")(.confirmed(committed, remoteChanged: true))
        let old = RemoteLibraryMonthDelta(month: monthKey,
            resources: [resource("old.jpg", hashByte: 1, size: 100)], assets: [], assetResourceLinks: [])
        XCTAssertFalse(cache.applyScannedMonth(old, scan: scan))
        let independent = RemoteLibraryMonthDelta(month: april,
            resources: [TestFixtures.remoteResource(year: year, month: 4, contentHash: Data([3]), fileName: "april.jpg")],
            assets: [], assetResourceLinks: [])
        XCTAssertTrue(cache.applyScannedMonth(independent, scan: scan))
        let digests = Dictionary(uniqueKeysWithValues: [monthKey, april].map {
            ($0, RemoteMonthManifestDigest(month: $0, manifestSize: 100, manifestModifiedAtMs: 1))
        })
        try cache.finishScan(scan, digests: digests, monthFilter: nil)
        XCTAssertEqual(Set(cache.current().resources.map(\.fileName)), ["new.jpg", "april.jpg"])
        let next = try cache.beginScan(profileKey: "a", allowContextSwitch: true)
        XCTAssertNil(next.digests[monthKey])
        XCTAssertNotNil(next.digests[april])
        cache.removeConfirmedMonth(monthKey, profileKey: "a")
        XCTAssertFalse(cache.applyScannedMonth(old, scan: next))
        try cache.finishScan(next, digests: digests, monthFilter: [monthKey])
        XCTAssertEqual(cache.current().resources.map(\.fileName), ["april.jpg"])
        _ = try cache.beginScan(profileKey: "b", allowContextSwitch: true)
        _ = try cache.beginScan(profileKey: "a", allowContextSwitch: true)
        XCTAssertFalse(cache.applyScannedMonth(old, scan: next))
        XCTAssertThrowsError(try cache.finishScan(next, digests: digests, monthFilter: nil))
        XCTAssertTrue(cache.current().resources.isEmpty)
    }

    private func resource(_ name: String, hashByte: UInt8, size: Int64) -> RemoteManifestResource {
        TestFixtures.remoteResource(year: year, month: month, contentHash: Data([hashByte]), fileSize: size, fileName: name)
    }

    func testEverySnapshotMutationPublishesAfterUnlockAndNoopsStayQuiet() {
        let cache = RemoteLibrarySnapshotCache()
        let observed = MutationCounter()
        let token = cache.changes.addObserver {
            _ = cache.current()
            observed.record()
        }
        defer { cache.changes.removeObserver(token) }

        cache.setProfileKey("owner")
        let resource = resource("a.jpg", hashByte: 1, size: 100)
        cache.replaceMonth(monthKey, resources: [resource], assets: [], assetResourceLinks: [], onlyIfOwnedBy: "owner")
        let asset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: Data([2]))
        cache.replaceMonth(monthKey, resources: [resource], assets: [asset], assetResourceLinks: [], onlyIfOwnedBy: "owner")
        XCTAssertEqual(observed.count, 3)
        cache.setProfileKey("owner")
        cache.replaceMonth(monthKey, resources: [resource], assets: [asset], assetResourceLinks: [], onlyIfOwnedBy: "owner")
        cache.replaceMonth(monthKey, resources: [], assets: [], assetResourceLinks: [], onlyIfOwnedBy: "other")
        XCTAssertFalse(cache.resetIfOwned(by: "other"))
        XCTAssertEqual(observed.count, 3)

        cache.replaceMonth(monthKey, resources: [resource], assets: [], assetResourceLinks: [])
        cache.removeMonth(monthKey)
        cache.markSynced(Date(timeIntervalSince1970: 1))
        XCTAssertTrue(cache.resetIfOwned(by: "owner"))
        cache.reset()
        XCTAssertEqual(observed.count, 8)
    }

    private final class MutationCounter: @unchecked Sendable {
        private let lock = NSLock()
        private var value = 0
        func record() { lock.withLock { value += 1 } }
        var count: Int { lock.withLock { value } }
    }

    func testMonthReplacementUpdatesHealthStatisticsAndIncompleteAssets() {
        // Two resources share a content hash (distinct ids) to exercise the dedup set.
        let resources = [
            resource("a.jpg", hashByte: 0x01, size: 100),
            resource("b.jpg", hashByte: 0x02, size: 200),
            resource("c.jpg", hashByte: 0x01, size: 100),
        ]
        let presentHash = Data([0x01])
        let completeFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: presentHash)]
        )
        let completeAsset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: completeFingerprint)
        let completeLink = TestFixtures.remoteLink(year: year, month: month, assetFingerprint: completeFingerprint, resourceHash: presentHash)
        let missingAsset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: Data([0xBB]))
        let missingLink = TestFixtures.remoteLink(year: year, month: month, assetFingerprint: Data([0xBB]), resourceHash: Data([0x09]))

        let cache = RemoteLibrarySnapshotCache()
        cache.replaceMonth(monthKey, resources: resources, assets: [completeAsset, missingAsset],
            assetResourceLinks: [completeLink, missingLink])
        let digest = cache.healthDigest()
        XCTAssertEqual(digest.totalResources, 3)
        XCTAssertEqual(digest.totalSizeBytes, 400)
        XCTAssertEqual(Set(digest.incompleteAssets.map(\.id)), [Data([0xBB])])
        XCTAssertEqual(Set(cache.current().resources), Set(resources))
        cache.replaceMonth(monthKey, resources: [resources[0]], assets: [completeAsset], assetResourceLinks: [completeLink])
        XCTAssertEqual(cache.healthDigest().totalSizeBytes, 100)
        XCTAssertTrue(cache.healthDigest().incompleteAssets.isEmpty)

        cache.replaceMonth(monthKey, resources: [resources[0]], assets: [completeAsset], assetResourceLinks: [])
        XCTAssertEqual(cache.healthDigest().incompleteAssets.map(\.id), [completeFingerprint])
        XCTAssertEqual(cache.healthDigest().totalSizeBytes, 100)
        let beforeRemoval = cache.currentRevision()
        XCTAssertTrue(cache.removeMonth(monthKey))
        XCTAssertTrue(cache.allKnownMonths().isEmpty)
        XCTAssertEqual(cache.healthDigest().totalSizeBytes, 0)
        XCTAssertTrue(cache.healthDigest().incompleteAssets.isEmpty)
        XCTAssertEqual(cache.state(since: beforeRemoval).monthDeltas.map(\.month), [monthKey])
        XCTAssertFalse(cache.removeMonth(monthKey))
    }

    func testMonthReplacementBumpsRevisionOnlyOnChange() {
        let cache = RemoteLibrarySnapshotCache()
        let base = cache.state(since: nil)

        let r1 = resource("a.jpg", hashByte: 0x01, size: 100)
        cache.replaceMonth(monthKey, resources: [r1], assets: [], assetResourceLinks: [])
        let afterAdd = cache.state(since: base.revision)
        XCTAssertGreaterThan(afterAdd.revision, base.revision)
        XCTAssertTrue(afterAdd.monthDeltas.contains { $0.month == monthKey })

        // Identical replacement must not trigger consumers.
        cache.replaceMonth(monthKey, resources: [r1], assets: [], assetResourceLinks: [])
        let afterNoop = cache.state(since: afterAdd.revision)
        XCTAssertEqual(afterNoop.revision, afterAdd.revision)
        XCTAssertTrue(afterNoop.monthDeltas.isEmpty)
    }

    func testRemovingMonthPreservesSameResourceAndAssetKeysInAnotherMonth() throws {
        let cache = RemoteLibrarySnapshotCache()
        let fingerprint = Data([1])
        let deltas = [3, 4].map { month in
            RemoteLibraryMonthDelta(month: LibraryMonthKey(year: year, month: month),
                resources: [TestFixtures.remoteResource(year: year, month: month,
                    contentHash: Data([2]), fileName: "same.jpg")],
                assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fingerprint)],
                assetResourceLinks: [TestFixtures.remoteLink(year: year, month: month,
                    assetFingerprint: fingerprint, resourceHash: Data([2]))])
        }
        for delta in deltas {
            cache.replaceMonth(delta.month, resources: delta.resources, assets: delta.assets,
                assetResourceLinks: delta.assetResourceLinks)
        }
        XCTAssertEqual(cache.current().resources.count, 2)
        XCTAssertEqual(cache.current().assets.count, 2)
        XCTAssertEqual(cache.current().assetResourceLinks.count, 2)

        cache.removeMonth(deltas[0].month)
        XCTAssertNil(cache.monthRawData(for: deltas[0].month))
        let retained = try XCTUnwrap(cache.monthRawData(for: deltas[1].month))
        XCTAssertEqual(retained.resources, deltas[1].resources)
        XCTAssertEqual(retained.assets, deltas[1].assets)
        XCTAssertEqual(retained.assetResourceLinks, deltas[1].assetResourceLinks)
    }

    func testHealthDigestCacheInvalidatesWhenSyncDateChanges() {
        let cache = RemoteLibrarySnapshotCache()
        XCTAssertNil(cache.healthDigest().lastIndexSyncedAt)

        let syncedAt = Date(timeIntervalSince1970: 1_700_000_000)
        cache.markSynced(syncedAt)

        XCTAssertEqual(cache.healthDigest().lastIndexSyncedAt, syncedAt)
    }

    func testHealthDigestCacheDoesNotSurviveReset() {
        let cache = RemoteLibrarySnapshotCache()
        cache.replaceMonth(monthKey, resources: [resource("before.jpg", hashByte: 0x01, size: 100)], assets: [], assetResourceLinks: [])
        XCTAssertEqual(cache.healthDigest().totalSizeBytes, 100)

        cache.reset()
        cache.replaceMonth(monthKey, resources: [resource("after.jpg", hashByte: 0x02, size: 250)], assets: [], assetResourceLinks: [])

        XCTAssertEqual(cache.healthDigest().totalSizeBytes, 250)
    }

    func testIndependentConsumersBothReceiveFullSnapshotAcrossReset() {
        let cache = RemoteLibrarySnapshotCache()
        cache.setProfileKey("a")
        cache.replaceMonth(monthKey, resources: [resource("before.jpg", hashByte: 1, size: 100)], assets: [], assetResourceLinks: [])
        let oldRevision = cache.currentRevision()
        cache.reset()
        cache.setProfileKey("b")
        cache.replaceMonth(monthKey, resources: [resource("after.jpg", hashByte: 2, size: 200)], assets: [], assetResourceLinks: [])

        let first = cache.state(since: oldRevision)
        let second = cache.state(since: oldRevision)
        XCTAssertGreaterThan(first.revision, oldRevision)
        XCTAssertTrue(first.isFullSnapshot)
        XCTAssertTrue(second.isFullSnapshot)
        XCTAssertEqual(first.revision, second.revision)
        XCTAssertEqual(second.profileKey, "b")
        XCTAssertEqual(second.monthDeltas.first?.resources.map(\.fileName), ["after.jpg"])
        XCTAssertFalse(cache.state(since: first.revision).isFullSnapshot)
        XCTAssertTrue(cache.state(since: first.revision).monthDeltas.isEmpty)
    }

    func testUncertainOutcomeRetainsDataAndOnlyFullReconciliationRestoresAuthority() {
        let cache = RemoteLibrarySnapshotCache()
        cache.setProfileKey("a")
        cache.replaceMonth(monthKey, resources: [resource("confirmed.jpg", hashByte: 1, size: 100)], assets: [], assetResourceLinks: [])
        let before = cache.currentRevision()
        cache.invalidateIfOwned(by: "b")
        XCTAssertEqual(cache.currentRevision(), before)
        cache.invalidateIfOwned(by: "a")
        let invalidated = cache.state(since: before)
        XCTAssertFalse(invalidated.isAuthoritative)
        XCTAssertTrue(invalidated.isFullSnapshot)
        XCTAssertEqual(invalidated.monthDeltas.first?.resources.count, 1)
        cache.invalidateIfOwned(by: "a")
        XCTAssertEqual(cache.currentRevision(), invalidated.revision)
        cache.markSynced(Date(), fullyReconciled: false)
        XCTAssertTrue(cache.requiresReconciliation)
        cache.markSynced(Date())
        let repaired = cache.state(since: invalidated.revision)
        XCTAssertTrue(repaired.isAuthoritative)
        XCTAssertTrue(repaired.isFullSnapshot)
        XCTAssertGreaterThan(repaired.revision, invalidated.revision)
    }
}

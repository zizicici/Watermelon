import XCTest
@testable import Watermelon

/// Slice 2 (M1/M10) invariant pins for `RemoteLibraryMonthDelta.presence` — the new typed
/// physical-presence field on the Home read-view delta. These tests prove that the two
/// `RepoCommittedView` delta-producing constructors (`state(since:)` and `monthRawData(for:)`)
/// actually propagate the overlay into the typed field, and that `HomeAlbumMatching`'s
/// `presenceByMonth: [LibraryMonthKey: RemotePresenceSnapshot.Month]` parameter treats an
/// absent entry as "no missing hashes" (matching today's `[:] ?? []` default semantics).
final class RemoteLibraryMonthDeltaPresenceTests: XCTestCase {
    private let monthA = LibraryMonthKey(year: 2025, month: 3)
    private let year = 2025
    private let month = 3

    // MARK: - RepoCommittedView source constructors

    /// Seeds the cache + overlay through the public service surface, then reads
    /// `currentState(since: nil)`. The returned delta MUST carry the overlay as
    /// `presence.missingHashes` with `isAuthoritative == false` (slice 3 plumbs freshness).
    func testRepoCommittedView_state_propagatesOverlayAsPresenceMissingHashes() {
        let service = RemoteIndexSyncService()
        let resourceHash = TestFixtures.fingerprint(0x71)
        let missingHash = TestFixtures.fingerprint(0x72)
        let asset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: TestFixtures.fingerprint(0x70))
        let resource = TestFixtures.remoteResource(year: year, month: month, contentHash: resourceHash)
        let link = TestFixtures.remoteLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint, resourceHash: resourceHash
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        writer.appendAsset(asset, links: [link])
        service.markPhysicallyMissingV2(month: monthA, hashes: [missingHash])

        let state = service.currentState(since: nil)
        let delta = state.monthDeltas.first { $0.month == monthA }
        XCTAssertNotNil(delta, "state(since:) must surface the seeded month")
        XCTAssertEqual(delta?.presence.missingHashes, [missingHash],
                       "overlay must propagate into delta.presence.missingHashes bit-equivalently to today's physicallyMissingHashes")
        XCTAssertEqual(delta?.presence.isAuthoritative, false,
                       "slice 2 invariant: read-view delta carries isAuthoritative=false; slice 3 will plumb freshness")
    }

    /// Same propagation contract via `remoteMonthRawData(for:)` — the other delta producer.
    func testRepoCommittedView_monthRawData_propagatesOverlayAsPresenceMissingHashes() {
        let service = RemoteIndexSyncService()
        let resourceHash = TestFixtures.fingerprint(0x81)
        let missingHash = TestFixtures.fingerprint(0x82)
        let asset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: TestFixtures.fingerprint(0x80))
        let resource = TestFixtures.remoteResource(year: year, month: month, contentHash: resourceHash)
        let link = TestFixtures.remoteLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint, resourceHash: resourceHash
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        writer.appendAsset(asset, links: [link])
        service.markPhysicallyMissingV2(month: monthA, hashes: [missingHash])

        let delta = service.remoteMonthRawData(for: monthA)
        XCTAssertNotNil(delta, "remoteMonthRawData must surface the seeded month")
        XCTAssertEqual(delta?.presence.missingHashes, [missingHash])
        XCTAssertEqual(delta?.presence.isAuthoritative, false,
                       "slice 2 invariant: read-view delta carries isAuthoritative=false")
    }

    /// Empty overlay path: when no hashes are marked physically missing, the read-view
    /// delta still surfaces a (default-constructed) `presence` with empty `missingHashes`
    /// and `isAuthoritative == false`. Equivalent to today's `physicallyMissingHashes == []`.
    func testRepoCommittedView_monthRawData_absentOverlay_yieldsEmptyPresence() {
        let service = RemoteIndexSyncService()
        let resourceHash = TestFixtures.fingerprint(0x91)
        let asset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: TestFixtures.fingerprint(0x90))
        let resource = TestFixtures.remoteResource(year: year, month: month, contentHash: resourceHash)
        let link = TestFixtures.remoteLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint, resourceHash: resourceHash
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        writer.appendAsset(asset, links: [link])

        let delta = service.remoteMonthRawData(for: monthA)
        XCTAssertNotNil(delta)
        XCTAssertEqual(delta?.presence.missingHashes, [],
                       "no overlay ⇒ empty missingHashes on the typed read-view")
        XCTAssertEqual(delta?.presence.isAuthoritative, false)
    }

    // MARK: - HomeAlbumMatching presenceByMonth default-path

    /// `presenceByMonth: [:]` (or any month not present in the dict) MUST behave as
    /// `.absent` ⇒ no hash is filtered. Pins Invariant 3 from the binding plan.
    func testHomeAlbumMatching_presenceByMonth_absentEntry_treatedAsNoMissingHashes() {
        let hash = TestFixtures.fingerprint(0xA1)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let asset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)
        let resource = TestFixtures.remoteResource(year: year, month: month, contentHash: hash)
        let link = TestFixtures.remoteLink(
            year: year, month: month,
            assetFingerprint: fp, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0
        )

        // No presenceByMonth argument — relies on `= [:]` default + `.absent` fallback inside body.
        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [resource], links: [link]
        )
        XCTAssertEqual(items.count, 1, "absent presence entry ⇒ no hash filtered ⇒ item surfaces")
        XCTAssertEqual(items.first?.isRestorable, true,
                       "absent presence is bit-equivalent to today's empty-dict default — restorable")
    }

    /// Explicit empty-`Month` entry in `presenceByMonth` MUST behave identically to an absent
    /// entry — both have `missingHashes == []`. Defends against a future regression where
    /// the lookup accidentally treats a present-but-empty entry differently from a missing key.
    func testHomeAlbumMatching_presenceByMonth_emptyMonthEntry_treatedAsNoMissingHashes() {
        let hash = TestFixtures.fingerprint(0xB1)
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let asset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)
        let resource = TestFixtures.remoteResource(year: year, month: month, contentHash: hash)
        let link = TestFixtures.remoteLink(
            year: year, month: month,
            assetFingerprint: fp, resourceHash: hash,
            role: ResourceTypeCode.photo, slot: 0
        )

        let items = HomeAlbumMatching.buildRemoteItems(
            assets: [asset], resources: [resource], links: [link],
            presenceByMonth: [LibraryMonthKey(year: year, month: month): .absent]
        )
        XCTAssertEqual(items.count, 1, "explicit .absent entry ⇒ same as missing dict key")
        XCTAssertEqual(items.first?.isRestorable, true)
    }
}

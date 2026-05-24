import XCTest
@testable import Watermelon

/// Invariant pins for `RemoteLibraryMonthDelta.presence` — the typed physical-presence field
/// on the Home read-view delta. The `markPhysicallyMissingV2`-seeded cases assert
/// `isAuthoritative == false` because the manual marker is freshness-neutral (slice 3 keeps
/// that semantic); the `applyPresenceSnapshotForTest`-seeded cases assert `isAuthoritative ==
/// true` because slice 3 wires freshness through the read-view source. The HomeAlbumMatching
/// tests pin that `presenceByMonth: [:]` treats an absent entry as "no missing hashes".
final class RemoteLibraryMonthDeltaPresenceTests: XCTestCase {
    private let monthA = LibraryMonthKey(year: 2025, month: 3)
    private let year = 2025
    private let month = 3

    // MARK: - RepoCommittedView source constructors

    /// Manual `markPhysicallyMissingV2` seeds the overlay without claiming freshness, so the
    /// delta carries the missing hashes with `isAuthoritative == false`.
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
                       "markPhysicallyMissingV2 is freshness-neutral; no apply ⇒ authority stays false")
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
                       "markPhysicallyMissingV2 is freshness-neutral; no apply ⇒ authority stays false")
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

    /// `applyPresenceSnapshotForTest` seeds an authoritative overlay, so the read-view delta
    /// returned by `currentState(since: nil)` carries `isAuthoritative == true`.
    func testRepoCommittedView_state_authoritativeOverlay_yieldsIsAuthoritativeTrue() {
        let service = RemoteIndexSyncService()
        let resourceHash = TestFixtures.fingerprint(0xC1)
        let missingHash = TestFixtures.fingerprint(0xC2)
        let asset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: TestFixtures.fingerprint(0xC0))
        let resource = TestFixtures.remoteResource(year: year, month: month, contentHash: resourceHash)
        let link = TestFixtures.remoteLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint, resourceHash: resourceHash
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        writer.appendAsset(asset, links: [link])
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [missingHash], isAuthoritative: true)
        XCTAssertTrue(service.applyPresenceSnapshotForTest(builder.build()))

        let state = service.currentState(since: nil)
        let delta = state.monthDeltas.first { $0.month == monthA }
        XCTAssertNotNil(delta)
        XCTAssertEqual(delta?.presence.missingHashes, [missingHash])
        XCTAssertEqual(delta?.presence.isAuthoritative, true,
                       "authoritative overlay must propagate into delta.presence.isAuthoritative")
    }

    /// Same authoritative-overlay propagation via `remoteMonthRawData(for:)`.
    func testRepoCommittedView_monthRawData_authoritativeOverlay_yieldsIsAuthoritativeTrue() {
        let service = RemoteIndexSyncService()
        let resourceHash = TestFixtures.fingerprint(0xD1)
        let missingHash = TestFixtures.fingerprint(0xD2)
        let asset = TestFixtures.remoteAsset(year: year, month: month, fingerprint: TestFixtures.fingerprint(0xD0))
        let resource = TestFixtures.remoteResource(year: year, month: month, contentHash: resourceHash)
        let link = TestFixtures.remoteLink(
            year: year, month: month,
            assetFingerprint: asset.assetFingerprint, resourceHash: resourceHash
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        writer.appendAsset(asset, links: [link])
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [missingHash], isAuthoritative: true)
        XCTAssertTrue(service.applyPresenceSnapshotForTest(builder.build()))

        let delta = service.remoteMonthRawData(for: monthA)
        XCTAssertNotNil(delta)
        XCTAssertEqual(delta?.presence.missingHashes, [missingHash])
        XCTAssertEqual(delta?.presence.isAuthoritative, true)
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

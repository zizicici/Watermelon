import XCTest
@testable import Watermelon

final class EngineTests: XCTestCase {
    // MARK: - Helpers

    private func makeEngine() -> HomeLocalIndexEngine { HomeLocalIndexEngine() }

    private func reload(
        _ engine: HomeLocalIndexEngine,
        _ collections: [LibraryAssetCollection],
        fingerprints: [String: Data] = [:],
        remoteFingerprintsForMonth: ((LibraryMonthKey) -> Set<Data>)? = nil
    ) -> Set<LibraryMonthKey> {
        engine.reload(
            collections: collections,
            fingerprintByAsset: fingerprints,
            remoteFingerprintsForMonth: remoteFingerprintsForMonth ?? TestFixtures.emptyRemoteFingerprints
        )
    }

    // MARK: - reload

    func testReload_singleFetchResult_populatesMonthsAndAggregates() {
        let engine = makeEngine()
        let collection = TestAssetCollection([
            TestFixtures.snapshot(id: "a", year: 2024, month: 1, kind: .photo),
            TestFixtures.snapshot(id: "b", year: 2024, month: 1, kind: .video),
            TestFixtures.snapshot(id: "c", year: 2024, month: 2, kind: .photo)
        ])

        _ = reload(engine, [collection])

        XCTAssertEqual(engine.allMonths, [LibraryMonthKey(year: 2024, month: 1), LibraryMonthKey(year: 2024, month: 2)])
        let jan = engine.localMonthSummary(for: LibraryMonthKey(year: 2024, month: 1))
        XCTAssertEqual(jan?.assetCount, 2)
        XCTAssertEqual(jan?.photoCount, 1)
        XCTAssertEqual(jan?.videoCount, 1)
        let feb = engine.localMonthSummary(for: LibraryMonthKey(year: 2024, month: 2))
        XCTAssertEqual(feb?.assetCount, 1)
        XCTAssertEqual(feb?.photoCount, 1)
    }

    func testReload_assetSharedAcrossAlbums_survivesRemovalFromOne() {
        let engine = makeEngine()
        let s = TestFixtures.snapshot(id: "shared", year: 2024, month: 3)
        let other = TestFixtures.snapshot(id: "lonely", year: 2024, month: 3)
        let albumA = TestAssetCollection([s, other])
        let albumB = TestAssetCollection([s])

        _ = reload(engine, [albumA, albumB])

        XCTAssertEqual(engine.localAssetIDs(for: LibraryMonthKey(year: 2024, month: 3)), ["shared", "lonely"])
        let provider = TestChangeProvider()
        let nextA = TestAssetCollection([other])
        provider.setChange(for: albumA, LibraryCollectionChange(
            nextCollection: nextA,
            hasIncrementalChanges: true,
            removedAssetIDs: ["shared"],
            insertedAssets: [],
            changedAssets: [],
            movedAssets: []
        ))
        _ = engine.applyChange(
            provider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )
        XCTAssertEqual(engine.localAssetIDs(for: LibraryMonthKey(year: 2024, month: 3)), ["shared", "lonely"])
    }

    func testReload_emptyInput_clearsIndex() {
        let engine = makeEngine()
        _ = reload(engine, [TestAssetCollection([TestFixtures.snapshot(id: "x")])])
        XCTAssertFalse(engine.allMonths.isEmpty)

        _ = reload(engine, [])
        XCTAssertTrue(engine.allMonths.isEmpty)
        XCTAssertFalse(engine.hasLoadedIndex)
    }

    // MARK: - applyChange (incremental)

    func testApplyChange_incrementalInsert_addsToIndex() {
        let engine = makeEngine()
        let initial = TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 5)])
        _ = reload(engine, [initial])

        let next = TestAssetCollection([
            TestFixtures.snapshot(id: "a", year: 2024, month: 5),
            TestFixtures.snapshot(id: "b", year: 2024, month: 6)
        ])
        let provider = TestChangeProvider()
        provider.setChange(for: initial, LibraryCollectionChange(
            nextCollection: next,
            hasIncrementalChanges: true,
            removedAssetIDs: [],
            insertedAssets: [TestFixtures.snapshot(id: "b", year: 2024, month: 6)],
            changedAssets: [],
            movedAssets: []
        ))

        let changedMonths = engine.applyChange(
            provider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        XCTAssertTrue(changedMonths.contains(LibraryMonthKey(year: 2024, month: 6)))
        XCTAssertEqual(engine.localAssetIDs(for: LibraryMonthKey(year: 2024, month: 6)), ["b"])
    }

    func testApplyChange_incrementalRemove_evictsFromIndex() {
        let engine = makeEngine()
        let initial = TestAssetCollection([
            TestFixtures.snapshot(id: "a", year: 2024, month: 7),
            TestFixtures.snapshot(id: "b", year: 2024, month: 7)
        ])
        _ = reload(engine, [initial])

        let next = TestAssetCollection([TestFixtures.snapshot(id: "b", year: 2024, month: 7)])
        let provider = TestChangeProvider()
        provider.setChange(for: initial, LibraryCollectionChange(
            nextCollection: next,
            hasIncrementalChanges: true,
            removedAssetIDs: ["a"],
            insertedAssets: [],
            changedAssets: [],
            movedAssets: []
        ))

        _ = engine.applyChange(
            provider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        XCTAssertEqual(engine.localAssetIDs(for: LibraryMonthKey(year: 2024, month: 7)), ["b"])
        XCTAssertNil(engine.monthForAsset("a"))
    }

    func testApplyChange_incrementalChange_updatesMonth() {
        // Asset "a" reclassifies from month 8 to month 9 — e.g., user edits creation date.
        let engine = makeEngine()
        let initial = TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 8)])
        _ = reload(engine, [initial])

        let updated = TestFixtures.snapshot(id: "a", year: 2024, month: 9)
        let next = TestAssetCollection([updated])
        let provider = TestChangeProvider()
        provider.setChange(for: initial, LibraryCollectionChange(
            nextCollection: next,
            hasIncrementalChanges: true,
            removedAssetIDs: [],
            insertedAssets: [],
            changedAssets: [updated],
            movedAssets: []
        ))

        _ = engine.applyChange(
            provider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        XCTAssertEqual(engine.monthForAsset("a"), LibraryMonthKey(year: 2024, month: 9))
        XCTAssertTrue(engine.localAssetIDs(for: LibraryMonthKey(year: 2024, month: 8)).isEmpty)
    }

    func testApplyChange_nonIncremental_rebuildsAssetSet() {
        let engine = makeEngine()
        let initial = TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 1)])
        _ = reload(engine, [initial])

        let next = TestAssetCollection([
            TestFixtures.snapshot(id: "b", year: 2024, month: 2),
            TestFixtures.snapshot(id: "c", year: 2024, month: 3)
        ])
        let provider = TestChangeProvider()
        provider.setChange(for: initial, LibraryCollectionChange(
            nextCollection: next,
            hasIncrementalChanges: false,
            removedAssetIDs: [],
            insertedAssets: [],
            changedAssets: [],
            movedAssets: []
        ))

        _ = engine.applyChange(
            provider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        XCTAssertNil(engine.monthForAsset("a"))
        XCTAssertEqual(engine.monthForAsset("b"), LibraryMonthKey(year: 2024, month: 2))
        XCTAssertEqual(engine.monthForAsset("c"), LibraryMonthKey(year: 2024, month: 3))
    }

    // MARK: - Multi-album membership

    func testApplyChange_multiAlbumMembership_removeFromOneKeepsAsset() {
        let engine = makeEngine()
        let s = TestFixtures.snapshot(id: "shared", year: 2024, month: 4)
        let albumA = TestAssetCollection([s])
        let albumB = TestAssetCollection([s])
        _ = reload(engine, [albumA, albumB])

        let provider = TestChangeProvider()
        provider.setChange(for: albumA, LibraryCollectionChange(
            nextCollection: TestAssetCollection([]),
            hasIncrementalChanges: true,
            removedAssetIDs: ["shared"],
            insertedAssets: [],
            changedAssets: [],
            movedAssets: []
        ))

        _ = engine.applyChange(
            provider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        XCTAssertEqual(engine.localAssetIDs(for: LibraryMonthKey(year: 2024, month: 4)), ["shared"])
    }

    func testApplyChange_multiAlbumMembership_lastRemoveEvicts() {
        // Remove from both albums in the same change pass — refcount 2→0, evicted.
        let engine = makeEngine()
        let s = TestFixtures.snapshot(id: "shared", year: 2024, month: 4)
        let albumA = TestAssetCollection([s])
        let albumB = TestAssetCollection([s])
        _ = reload(engine, [albumA, albumB])

        let provider = TestChangeProvider()
        provider.setChange(for: albumA, LibraryCollectionChange(
            nextCollection: TestAssetCollection([]),
            hasIncrementalChanges: true,
            removedAssetIDs: ["shared"],
            insertedAssets: [],
            changedAssets: [],
            movedAssets: []
        ))
        provider.setChange(for: albumB, LibraryCollectionChange(
            nextCollection: TestAssetCollection([]),
            hasIncrementalChanges: true,
            removedAssetIDs: ["shared"],
            insertedAssets: [],
            changedAssets: [],
            movedAssets: []
        ))

        _ = engine.applyChange(
            provider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        XCTAssertNil(engine.monthForAsset("shared"))
        XCTAssertTrue(engine.localAssetIDs(for: LibraryMonthKey(year: 2024, month: 4)).isEmpty)
    }

    // MARK: - eagerlyInsert (Critical Invariant #1)

    func testEagerlyInsert_ratifyingPHChange_subsequentRemovalEvicts() {
        // Critical Invariant: PHChange's `insertedIndexes` ratifies eagerly-inserted
        // assets via applyMembershipDelta (0→1). Once ratified, a later removal
        // PHChange (1→0) evicts normally.
        let engine = makeEngine()
        let backingCollection = TestAssetCollection([TestFixtures.snapshot(id: "tracked", year: 2024, month: 5)])
        _ = reload(engine, [backingCollection])

        let snap = TestFixtures.snapshot(id: "newcomer", year: 2024, month: 7)
        _ = engine.eagerlyInsert(
            ["newcomer": snap],
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        let nextAfterInsert = TestAssetCollection([
            TestFixtures.snapshot(id: "tracked", year: 2024, month: 5),
            snap
        ])
        let ratifyProvider = TestChangeProvider()
        ratifyProvider.setChange(for: backingCollection, LibraryCollectionChange(
            nextCollection: nextAfterInsert,
            hasIncrementalChanges: true,
            removedAssetIDs: [],
            insertedAssets: [snap],
            changedAssets: [],
            movedAssets: []
        ))
        _ = engine.applyChange(
            ratifyProvider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        let removalProvider = TestChangeProvider()
        let nextAfterRemove = TestAssetCollection([TestFixtures.snapshot(id: "tracked", year: 2024, month: 5)])
        removalProvider.setChange(for: nextAfterInsert, LibraryCollectionChange(
            nextCollection: nextAfterRemove,
            hasIncrementalChanges: true,
            removedAssetIDs: ["newcomer"],
            insertedAssets: [],
            changedAssets: [],
            movedAssets: []
        ))
        _ = engine.applyChange(
            removalProvider,
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        XCTAssertNil(engine.monthForAsset("newcomer"))
    }

    func testEagerlyInsert_phantomClearedByNextReload() {
        // Phantom entries (never-ratified eager inserts) must not survive a reload that
        // doesn't include them — otherwise they'd inflate UI counts indefinitely.
        let engine = makeEngine()
        let initial = TestAssetCollection([TestFixtures.snapshot(id: "tracked", year: 2024, month: 5)])
        _ = reload(engine, [initial])

        _ = engine.eagerlyInsert(
            ["phantom": TestFixtures.snapshot(id: "phantom", year: 2024, month: 6)],
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )
        XCTAssertEqual(engine.monthForAsset("phantom"), LibraryMonthKey(year: 2024, month: 6))

        _ = reload(engine, [TestAssetCollection([TestFixtures.snapshot(id: "tracked", year: 2024, month: 5)])])
        XCTAssertNil(engine.monthForAsset("phantom"))
    }

    func testEagerlyInsert_idempotent_skipsAlreadyTracked() {
        let engine = makeEngine()
        let initial = TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 1)])
        _ = reload(engine, [initial])

        // Misleading month on the snapshot — must be ignored since "a" is already tracked.
        let snap = TestFixtures.snapshot(id: "a", year: 2099, month: 12)
        let changedMonths = engine.eagerlyInsert(
            ["a": snap],
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )

        XCTAssertTrue(changedMonths.isEmpty)
        XCTAssertEqual(engine.monthForAsset("a"), LibraryMonthKey(year: 2024, month: 1))
    }

    // MARK: - refreshExisting

    func testRefreshExisting_unknownIDs_silentlySkipped() {
        let engine = makeEngine()
        _ = reload(engine, [TestAssetCollection([TestFixtures.snapshot(id: "a")])])

        let result = engine.refreshExisting(
            assetIDs: ["unknown1", "unknown2"],
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )
        XCTAssertTrue(result.isEmpty)
    }

    func testRefreshExisting_includesMonthOfKnownIDs() {
        let engine = makeEngine()
        _ = reload(engine, [TestAssetCollection([
            TestFixtures.snapshot(id: "a", year: 2024, month: 6),
            TestFixtures.snapshot(id: "b", year: 2024, month: 7)
        ])])

        let result = engine.refreshExisting(
            assetIDs: ["a"],
            fingerprintsForIDs: TestFixtures.emptyFingerprint,
            remoteFingerprintsForMonth: TestFixtures.emptyRemoteFingerprints
        )
        XCTAssertEqual(result, [LibraryMonthKey(year: 2024, month: 6)])
    }

    // MARK: - clearIfNeeded

    func testClearIfNeeded_resetsAllState() {
        let engine = makeEngine()
        _ = reload(engine, [TestAssetCollection([TestFixtures.snapshot(id: "a")])])
        engine.setMonthFileSize(1234, for: LibraryMonthKey(year: 2024, month: 1))

        let cleared = engine.clearIfNeeded()
        XCTAssertFalse(cleared.isEmpty)

        XCTAssertFalse(engine.hasLoadedIndex)
        XCTAssertTrue(engine.allMonths.isEmpty)
        XCTAssertNil(engine.monthForAsset("a"))
        XCTAssertTrue(engine.monthFileSizes.isEmpty)
    }

    // MARK: - monthFileSizes wipe (Critical Invariant #4)

    func testReload_wipesMonthFileSizes() {
        let engine = makeEngine()
        let key = LibraryMonthKey(year: 2024, month: 3)
        _ = reload(engine, [TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 3)])])
        engine.setMonthFileSize(9999, for: key)
        XCTAssertEqual(engine.monthFileSizes[key], 9999)

        _ = reload(engine, [TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 3)])])
        XCTAssertNil(engine.monthFileSizes[key], "reload wipes monthFileSizes even when content is unchanged")
    }

    // MARK: - recomputeAggregates dedup

    func testReload_dedupsBackedUpFingerprintsByValue() {
        // Two locals share a fingerprint → backedUpCount = 1 (one remote asset).
        let engine = makeEngine()
        let fp = Data([0xDE, 0xAD, 0xBE, 0xEF])
        let collection = TestAssetCollection([
            TestFixtures.snapshot(id: "a", year: 2024, month: 3),
            TestFixtures.snapshot(id: "b", year: 2024, month: 3)
        ])
        _ = engine.reload(
            collections: [collection],
            fingerprintByAsset: ["a": fp, "b": fp],
            remoteFingerprintsForMonth: { _ in [fp] }
        )

        let summary = engine.localMonthSummary(for: LibraryMonthKey(year: 2024, month: 3))
        XCTAssertEqual(summary?.assetCount, 2)
        XCTAssertEqual(summary?.backedUpCount, 1)
    }

    // MARK: - refreshBackedUpState

    func testRefreshBackedUpState_updatesBackedUpCount() {
        let engine = makeEngine()
        let fp = Data([0x01])
        var remoteFps: [LibraryMonthKey: Set<Data>] = [:]
        let key = LibraryMonthKey(year: 2024, month: 3)

        _ = engine.reload(
            collections: [TestAssetCollection([TestFixtures.snapshot(id: "a", year: 2024, month: 3)])],
            fingerprintByAsset: ["a": fp],
            remoteFingerprintsForMonth: { remoteFps[$0] ?? [] }
        )
        XCTAssertEqual(engine.localMonthSummary(for: key)?.backedUpCount, 0)

        remoteFps[key] = [fp]
        let touched = engine.refreshBackedUpState(
            affectedMonths: [key],
            remoteFingerprintsForMonth: { remoteFps[$0] ?? [] }
        )
        XCTAssertEqual(touched, [key])
        XCTAssertEqual(engine.localMonthSummary(for: key)?.backedUpCount, 1)
    }
}

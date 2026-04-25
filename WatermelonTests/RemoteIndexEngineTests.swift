import XCTest
@testable import Watermelon

final class RemoteIndexEngineTests: XCTestCase {
    private let key202401 = LibraryMonthKey(year: 2024, month: 1)
    private let key202402 = LibraryMonthKey(year: 2024, month: 2)

    private func makeEngine() -> HomeRemoteIndexEngine { HomeRemoteIndexEngine() }

    /// Build a single-asset, single-resource, single-link delta whose role/type defaults
    /// classify the asset as a plain photo. Reuse for "happy-path resolvable" scenarios.
    private func resolvablePhotoDelta(
        _ key: LibraryMonthKey,
        fingerprint: Data,
        resourceSize: Int64 = 100
    ) -> RemoteLibraryMonthDelta {
        let hash = Data([0xA0]) + fingerprint
        return TestFixtures.remoteMonthDelta(
            key,
            assets: [TestFixtures.remoteAsset(
                year: key.year, month: key.month,
                fingerprint: fingerprint, totalFileSizeBytes: resourceSize
            )],
            resources: [TestFixtures.remoteResource(
                year: key.year, month: key.month,
                contentHash: hash, fileSize: resourceSize
            )],
            links: [TestFixtures.remoteLink(
                year: key.year, month: key.month,
                assetFingerprint: fingerprint, resourceHash: hash
            )]
        )
    }

    // MARK: - apply / state

    func testApply_fullSnapshot_clearsAndReplaces() {
        let engine = makeEngine()
        let fpA = Data([0x01])
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 1, isFullSnapshot: true,
                deltas: [resolvablePhotoDelta(key202401, fingerprint: fpA)]
            ),
            hasActiveConnection: true
        )
        XCTAssertEqual(engine.fingerprints(for: key202401), [fpA])

        // A second full snapshot for a different month wipes the first month's state.
        let fpB = Data([0x02])
        let delta = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 2, isFullSnapshot: true,
                deltas: [resolvablePhotoDelta(key202402, fingerprint: fpB)]
            ),
            hasActiveConnection: true
        )
        XCTAssertTrue(delta.changedMonths.contains(key202401), "old months are wiped on fullSnapshot")
        XCTAssertTrue(delta.changedMonths.contains(key202402))
        XCTAssertTrue(engine.fingerprints(for: key202401).isEmpty)
        XCTAssertEqual(engine.fingerprints(for: key202402), [fpB])
    }

    func testApply_partialSnapshot_replacesOnlyDeltaMonths() {
        // After a full snapshot, a partial only touches its delta months and leaves the
        // others alone. This is the steady-state path during incremental remote sync.
        let engine = makeEngine()
        let fpA = Data([0x10])
        let fpB = Data([0x20])
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 1, isFullSnapshot: true,
                deltas: [
                    resolvablePhotoDelta(key202401, fingerprint: fpA),
                    resolvablePhotoDelta(key202402, fingerprint: fpB)
                ]
            ),
            hasActiveConnection: true
        )

        let fpC = Data([0x30])
        let delta = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 2, isFullSnapshot: false,
                deltas: [resolvablePhotoDelta(key202401, fingerprint: fpC)]
            ),
            hasActiveConnection: true
        )
        XCTAssertEqual(delta.changedMonths, [key202401])
        XCTAssertEqual(engine.fingerprints(for: key202401), [fpC])
        XCTAssertEqual(engine.fingerprints(for: key202402), [fpB], "Feb stays untouched on partial")
    }

    func testApply_revisionUnchanged_partialNoOp() {
        // A partial sync that lands with the same revision (e.g., re-emitted notification)
        // returns immediately with no changes. The same-revision check is gated on
        // !isFullSnapshot to allow forced full reapply.
        let engine = makeEngine()
        let fpA = Data([0x40])
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 5, isFullSnapshot: true,
                deltas: [resolvablePhotoDelta(key202401, fingerprint: fpA)]
            ),
            hasActiveConnection: true
        )
        XCTAssertEqual(engine.snapshotRevision, 5)

        let delta = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 5, isFullSnapshot: false,
                deltas: [resolvablePhotoDelta(key202402, fingerprint: Data([0x99]))]
            ),
            hasActiveConnection: true
        )
        XCTAssertTrue(delta.changedMonths.isEmpty, "same-revision partial is a no-op")
        XCTAssertTrue(engine.fingerprints(for: key202402).isEmpty, "Feb should not have been ingested")
    }

    func testApply_disconnect_clearsAllState() {
        let engine = makeEngine()
        let fpA = Data([0x50])
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 1, isFullSnapshot: true,
                deltas: [resolvablePhotoDelta(key202401, fingerprint: fpA)]
            ),
            hasActiveConnection: true
        )

        let delta = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 2, isFullSnapshot: false,
                deltas: []
            ),
            hasActiveConnection: false
        )
        XCTAssertEqual(delta.changedMonths, [key202401], "every previously-known month is reported as changed")
        XCTAssertTrue(engine.fingerprints(for: key202401).isEmpty)
        XCTAssertNil(engine.summary(for: key202401))
        XCTAssertEqual(engine.snapshotRevision, 2, "revision is still recorded across disconnect")
    }

    // MARK: - resolveMonth dropping rules

    func testApply_dropsAssetsWithoutResolvableLinks() {
        // Critical Invariant: in a partial-flush window, assets + links may have been
        // pulled from the remote manifest before the resource rows are visible. Such
        // assets must not appear in the engine, otherwise "matched count" would over-
        // report against locals whose hashes we can't actually serve.
        let engine = makeEngine()
        let fp = Data([0x60])
        let absentHash = Data([0xFF])
        let delta = TestFixtures.remoteMonthDelta(
            key202401,
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp)],
            resources: [],
            links: [TestFixtures.remoteLink(
                year: 2024, month: 1,
                assetFingerprint: fp, resourceHash: absentHash
            )]
        )
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 1, isFullSnapshot: true,
                deltas: [delta]
            ),
            hasActiveConnection: true
        )
        XCTAssertTrue(engine.fingerprints(for: key202401).isEmpty)
        XCTAssertNil(engine.summary(for: key202401))
    }

    func testApply_pairedVideoPlusPhotoLike_classifiedAsPhoto() {
        // livePhoto folds into photoCount: the engine's two-bucket taxonomy mirrors
        // HomeLocalIndexEngine, which has no livePhoto bucket of its own.
        let engine = makeEngine()
        let fp = Data([0x70])
        let photoHash = Data([0x71])
        let videoHash = Data([0x72])
        let delta = TestFixtures.remoteMonthDelta(
            key202401,
            assets: [TestFixtures.remoteAsset(
                year: 2024, month: 1, fingerprint: fp, totalFileSizeBytes: 200
            )],
            resources: [
                TestFixtures.remoteResource(
                    year: 2024, month: 1,
                    contentHash: photoHash, fileSize: 80,
                    resourceType: ResourceTypeCode.photo
                ),
                TestFixtures.remoteResource(
                    year: 2024, month: 1,
                    contentHash: videoHash, fileSize: 120,
                    resourceType: ResourceTypeCode.pairedVideo
                )
            ],
            links: [
                TestFixtures.remoteLink(
                    year: 2024, month: 1,
                    assetFingerprint: fp, resourceHash: photoHash,
                    role: ResourceTypeCode.photo, slot: 0
                ),
                TestFixtures.remoteLink(
                    year: 2024, month: 1,
                    assetFingerprint: fp, resourceHash: videoHash,
                    role: ResourceTypeCode.pairedVideo, slot: 1
                )
            ]
        )
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 1, isFullSnapshot: true,
                deltas: [delta]
            ),
            hasActiveConnection: true
        )
        let summary = engine.summary(for: key202401)
        XCTAssertEqual(summary?.assetCount, 1)
        XCTAssertEqual(summary?.photoCount, 1, "livePhoto folds into photoCount")
        XCTAssertEqual(summary?.videoCount, 0)
        // Bytes are summed over deduped resolvable hashes (photo + pairedVideo).
        XCTAssertEqual(summary?.totalSizeBytes, 200)
    }
}

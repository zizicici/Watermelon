import XCTest
@testable import Watermelon

final class RemoteIndexEngineTests: XCTestCase {
    private let key202401 = LibraryMonthKey(year: 2024, month: 1)
    private let key202402 = LibraryMonthKey(year: 2024, month: 2)

    private func makeEngine() -> HomeRemoteIndexEngine { HomeRemoteIndexEngine() }

    /// Photo delta with canonical fingerprint; `discriminator` only varies the hash to avoid collisions.
    private func resolvablePhoto(
        _ key: LibraryMonthKey,
        discriminator: Data,
        resourceSize: Int64 = 100
    ) -> (delta: RemoteLibraryMonthDelta, fingerprint: Data) {
        let hash = Data([0xA0]) + discriminator
        let fp = TestFixtures.computedFingerprint(for: [(ResourceTypeCode.photo, 0, hash)])
        let delta = TestFixtures.remoteMonthDelta(
            key,
            assets: [TestFixtures.remoteAsset(
                year: key.year, month: key.month,
                fingerprint: fp, totalFileSizeBytes: resourceSize
            )],
            resources: [TestFixtures.remoteResource(
                year: key.year, month: key.month,
                contentHash: hash, fileSize: resourceSize
            )],
            links: [TestFixtures.remoteLink(
                year: key.year, month: key.month,
                assetFingerprint: fp, resourceHash: hash
            )]
        )
        return (delta, fp)
    }

    // MARK: - apply / state

    func testApply_fullSnapshot_clearsAndReplaces() {
        let engine = makeEngine()
        let janA = resolvablePhoto(key202401, discriminator: Data([0x01]))
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 1, isFullSnapshot: true,
                deltas: [janA.delta]
            ),
            hasActiveConnection: true
        )
        XCTAssertEqual(engine.fingerprints(for: key202401), [janA.fingerprint])

        let febB = resolvablePhoto(key202402, discriminator: Data([0x02]))
        let delta = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 2, isFullSnapshot: true,
                deltas: [febB.delta]
            ),
            hasActiveConnection: true
        )
        XCTAssertTrue(delta.changedMonths.contains(key202401), "old months are wiped on fullSnapshot")
        XCTAssertTrue(delta.changedMonths.contains(key202402))
        XCTAssertTrue(engine.fingerprints(for: key202401).isEmpty)
        XCTAssertEqual(engine.fingerprints(for: key202402), [febB.fingerprint])
    }

    func testApply_partialSnapshot_replacesOnlyDeltaMonths() {
        let engine = makeEngine()
        let janA = resolvablePhoto(key202401, discriminator: Data([0x10]))
        let febB = resolvablePhoto(key202402, discriminator: Data([0x20]))
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 1, isFullSnapshot: true,
                deltas: [janA.delta, febB.delta]
            ),
            hasActiveConnection: true
        )

        let janC = resolvablePhoto(key202401, discriminator: Data([0x30]))
        let delta = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 2, isFullSnapshot: false,
                deltas: [janC.delta]
            ),
            hasActiveConnection: true
        )
        XCTAssertEqual(delta.changedMonths, [key202401])
        XCTAssertEqual(engine.fingerprints(for: key202401), [janC.fingerprint])
        XCTAssertEqual(engine.fingerprints(for: key202402), [febB.fingerprint], "Feb stays untouched on partial")
    }

    func testApply_revisionUnchanged_partialNoOp() {
        // Same-revision partials early-return; same-revision FULL snapshots still apply.
        let engine = makeEngine()
        let janA = resolvablePhoto(key202401, discriminator: Data([0x40]))
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 5, isFullSnapshot: true,
                deltas: [janA.delta]
            ),
            hasActiveConnection: true
        )
        XCTAssertEqual(engine.snapshotRevision, 5)

        let febOther = resolvablePhoto(key202402, discriminator: Data([0x99]))
        let delta = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 5, isFullSnapshot: false,
                deltas: [febOther.delta]
            ),
            hasActiveConnection: true
        )
        XCTAssertTrue(delta.changedMonths.isEmpty, "same-revision partial is a no-op")
        XCTAssertTrue(engine.fingerprints(for: key202402).isEmpty, "Feb should not have been ingested")
    }

    func testApply_disconnect_clearsAllState() {
        let engine = makeEngine()
        let janA = resolvablePhoto(key202401, discriminator: Data([0x50]))
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(
                revision: 1, isFullSnapshot: true,
                deltas: [janA.delta]
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
        // After disconnect, snapshotRevision is dropped to nil so that a reconnect with
        // the same cache revision will go through the full-snapshot path instead of an
        // empty-delta early-return (see HomeRemoteIndexEngine.apply guard).
        XCTAssertNil(engine.snapshotRevision, "revision cleared on disconnect to force a full apply on reconnect")
    }

    // MARK: - resolveMonth dropping rules

    func testApply_dropsAssetsWithoutResolvableLinks() {
        // Critical Invariant: partial-flush windows can land assets+links before
        // resource rows. Those orphans must not contribute to the engine, otherwise
        // matchedCount would over-report against locals whose hashes we can't serve.
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

    /// Partially-missing assets used to count as backed-up because "any link resolves"
    /// gated inclusion. Classifier gating excludes them — UI's backed-up count must match
    /// what restore can actually deliver.
    func testApply_partiallyMissingAsset_excludedFromBackedUp() {
        let engine = makeEngine()
        let photoHash = Data([0xB0])
        let videoHash = Data([0xB1])
        let fp = TestFixtures.computedFingerprint(for: [
            (ResourceTypeCode.photo, 0, photoHash),
            (ResourceTypeCode.pairedVideo, 1, videoHash)
        ])
        let delta = TestFixtures.remoteMonthDelta(
            key202401,
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp, totalFileSizeBytes: 200)],
            resources: [
                TestFixtures.remoteResource(year: 2024, month: 1, contentHash: photoHash, fileSize: 80),
                TestFixtures.remoteResource(year: 2024, month: 1, contentHash: videoHash, fileSize: 120)
            ],
            links: [
                TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fp, resourceHash: photoHash, role: ResourceTypeCode.photo, slot: 0),
                TestFixtures.remoteLink(year: 2024, month: 1, assetFingerprint: fp, resourceHash: videoHash, role: ResourceTypeCode.pairedVideo, slot: 1)
            ]
        )
        let withMissing = RemoteLibraryMonthDelta(
            month: delta.month, resources: delta.resources, assets: delta.assets,
            assetResourceLinks: delta.assetResourceLinks,
            physicallyMissingHashes: [videoHash]
        )
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(revision: 1, isFullSnapshot: true, deltas: [withMissing]),
            hasActiveConnection: true
        )
        XCTAssertTrue(engine.fingerprints(for: key202401).isEmpty,
                      "partiallyMissing classifier state → excluded from backed-up count")
        XCTAssertNil(engine.summary(for: key202401))
    }

    func testApply_videoOnly_classifiedAsVideo() {
        let engine = makeEngine()
        let videoHash = Data([0x81])
        let fp = TestFixtures.computedFingerprint(for: [(ResourceTypeCode.video, 0, videoHash)])
        let delta = TestFixtures.remoteMonthDelta(
            key202401,
            assets: [TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fp, totalFileSizeBytes: 500)],
            resources: [TestFixtures.remoteResource(
                year: 2024, month: 1,
                contentHash: videoHash, fileSize: 500,
                resourceType: ResourceTypeCode.video
            )],
            links: [TestFixtures.remoteLink(
                year: 2024, month: 1,
                assetFingerprint: fp, resourceHash: videoHash,
                role: ResourceTypeCode.video
            )]
        )
        _ = engine.apply(
            state: TestFixtures.remoteSnapshotState(revision: 1, isFullSnapshot: true, deltas: [delta]),
            hasActiveConnection: true
        )
        let summary = engine.summary(for: key202401)
        XCTAssertEqual(summary?.videoCount, 1)
        XCTAssertEqual(summary?.photoCount, 0)
    }

    func testApply_pairedVideoPlusPhotoLike_classifiedAsPhoto() {
        // livePhoto folds into photoCount — HomeRemoteIndexEngine has no livePhoto bucket.
        let engine = makeEngine()
        let photoHash = Data([0x71])
        let videoHash = Data([0x72])
        let fp = TestFixtures.computedFingerprint(for: [
            (ResourceTypeCode.photo, 0, photoHash),
            (ResourceTypeCode.pairedVideo, 1, videoHash)
        ])
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
        XCTAssertEqual(summary?.totalSizeBytes, 200, "bytes summed across deduped resolvable hashes")
    }
}

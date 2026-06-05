import XCTest
@testable import Watermelon

/// View-layer pins for the slice-3 freshness contract on `RepoCommittedView`:
/// freshness state lives under `missingLock`; every freshness-flipping mutator
/// participates in `cache.markMonthsChanged` so the authoritative bit is visible
/// to incremental `state(since:)` consumers. Service-level tests (#10, #11) pin
/// the end-to-end contract via `RemoteIndexSyncService` accessors.
final class RepoCommittedViewPresenceTests: XCTestCase {
    private let monthA = LibraryMonthKey(year: 2025, month: 3)
    private let monthB = LibraryMonthKey(year: 2025, month: 4)

    private func resource(for month: LibraryMonthKey, hash: Data) -> RemoteManifestResource {
        TestFixtures.remoteResource(year: month.year, month: month.month, contentHash: hash)
    }

    // Bug-X P07 R07 CodexChecker F1: two byte-distinct NFC/NFD twin resources committed in the same
    // month on an exact-name backend. The published index keyed resources by the raw physicalRemotePath
    // String, which folds the twins and silently drops one committed row — making its content hash, and
    // any asset linking to it, look unavailable to Home/restore. Byte-exact keys keep both rows.
    func testReplaceMonth_nfcAndNfdTwinResources_bothSurviveInPublishedSnapshot() {
        let view = RepoCommittedView()
        let nfcPath = "2025/03/caf\u{00E9}.jpg"
        let nfdPath = "2025/03/cafe\u{0301}.jpg"
        XCTAssertNotEqual(Array(nfcPath.utf8), Array(nfdPath.utf8), "premise: twin paths are byte-distinct")
        let hNFC = TestFixtures.fingerprint(0xD1)
        let hNFD = TestFixtures.fingerprint(0xD2)

        let rNFC = RemoteManifestResource(
            year: monthA.year, month: monthA.month, physicalRemotePath: nfcPath,
            contentHash: hNFC, fileSize: 11, resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )
        let rNFD = RemoteManifestResource(
            year: monthA.year, month: monthA.month, physicalRemotePath: nfdPath,
            contentHash: hNFD, fileSize: 22, resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0, crypto: nil
        )

        _ = view.replaceMonth(monthA, resources: [rNFC, rNFD], assets: [], assetResourceLinks: [])

        let published = view.current().resources
            .filter { LibraryMonthKey(year: $0.year, month: $0.month) == monthA }
        XCTAssertEqual(Set(published.map(\.physicalRemotePath)), [nfcPath, nfdPath],
            "both byte-distinct twin resource rows must survive publication")
        XCTAssertEqual(Set(published.map(\.contentHash)), [hNFC, hNFD],
            "neither twin's content hash may be dropped from the published index")
    }

    // MARK: - replaceMonth(..., freshness:)

    func testReplaceMonth_withFreshMarker_setsAuthoritative() {
        let view = RepoCommittedView()
        let h = TestFixtures.fingerprint(0x01)
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: TestFixtures.fingerprint(0xA0))],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [h],
            freshness: .markFresh
        )
        XCTAssertEqual(view.verifiedPhysicallyMissingHashes(for: monthA), Set<Data>(),
                       "missing hashes intersected with stillPresent: h is not in stillPresent, so refined missing is empty")
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)
    }

    func testReplaceMonth_withFreshMarker_andHashInStillPresent_setsAuthoritativeAndPreservesMissing() {
        let view = RepoCommittedView()
        // The "missing" hash must NOT match any stillPresent resource for the refined-missing path,
        // but the freshness flag is set regardless.
        let resourceHash = TestFixtures.fingerprint(0xA0)
        let missingHash = TestFixtures.fingerprint(0xA1)
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: resourceHash)],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [missingHash],
            freshness: .markFresh
        )
        // refined = [missingHash] ∩ {resourceHash} = ∅, so missing collapses to empty.
        // verifiedPhysicallyMissingHashes still returns Some([]) because freshness is set.
        XCTAssertEqual(view.verifiedPhysicallyMissingHashes(for: monthA), Set<Data>())
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)
    }

    func testReplaceMonth_withStaleMarker_clearsAuthoritative() {
        let view = RepoCommittedView()
        let resourceHash = TestFixtures.fingerprint(0xB0)
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: resourceHash)],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [],
            freshness: .markFresh
        )
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)

        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: resourceHash)],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: nil,
            freshness: .markStale
        )
        XCTAssertNil(view.verifiedPhysicallyMissingHashes(for: monthA))
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, false)
    }

    /// Regression guard: V1-path callers (`RemoteIndexSyncService.syncIndexV1Apply`,
    /// `RepoVerifyMonthService.verifyMonth`) pass default `.keep`. Freshness for the month
    /// must remain whatever it was, even after replacing the manifest payload.
    func testReplaceMonth_defaultKeep_doesNotToggleFreshness() {
        let view = RepoCommittedView()
        let resourceHash = TestFixtures.fingerprint(0xC0)
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: resourceHash)],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [],
            freshness: .markFresh
        )
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)

        // Default `.keep`: freshness untouched.
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: resourceHash)],
            assets: [],
            assetResourceLinks: []
        )
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true,
                       "default .keep MUST preserve freshness; V1-path callers depend on this")
    }

    // MARK: - applyPresenceSnapshot

    func testApplyPresenceSnapshot_revisionMatch_setsFreshness() {
        let view = RepoCommittedView()
        let hA = TestFixtures.fingerprint(0xD0)
        let hB = TestFixtures.fingerprint(0xE0)
        _ = view.replaceMonth(monthA, resources: [resource(for: monthA, hash: hA)], assets: [], assetResourceLinks: [])
        _ = view.replaceMonth(monthB, resources: [resource(for: monthB, hash: hB)], assets: [], assetResourceLinks: [])
        let rev = view.currentRevision()

        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [], isAuthoritative: true)
        builder.set(monthB, missingHashes: [], isAuthoritative: false)
        XCTAssertTrue(view.applyPresenceSnapshot(builder.build(), expectedRevision: rev))

        XCTAssertEqual(view.verifiedPhysicallyMissingHashes(for: monthA), Set<Data>())
        XCTAssertNil(view.verifiedPhysicallyMissingHashes(for: monthB),
                     "non-authoritative entry must NOT add the month to freshness")
    }

    func testApplyPresenceSnapshot_revisionStale_returnsFalseAndClearsFreshness() {
        let view = RepoCommittedView()
        let hA = TestFixtures.fingerprint(0xF0)
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: hA)],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [],
            freshness: .markFresh
        )
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)

        // Stale expectedRevision (rev - 1 always lags actual).
        let actualRev = view.currentRevision()
        XCTAssertFalse(view.applyPresenceSnapshot(RemotePresenceSnapshot(), expectedRevision: actualRev &- 1))
        XCTAssertNil(view.verifiedPhysicallyMissingHashes(for: monthA),
                     "stale revision MUST clear freshness on the previously-fresh month")
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, false)
    }

    /// PROMOTED FROM OPTIONAL per Codex re-review LOW: the stale-revision clear branch in
    /// `syncOverlayAndCaptureHandle` (which now delegates to `clearPresenceFreshness` via
    /// `applyPresenceSnapshot` with stale expectedRevision) must produce a revision-visible
    /// change for previously-fresh months. Today's silent service-side clear is replaced by
    /// a `cache.markMonthsChanged(previouslyFresh)` so incremental `state(since:)` consumers
    /// observe `isAuthoritative` dropping to false.
    func testApplyPresenceSnapshot_revisionStale_clearMarksPreviouslyFreshMonths() {
        let view = RepoCommittedView()
        let hA = TestFixtures.fingerprint(0x11)
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: hA)],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [],
            freshness: .markFresh
        )
        let preStaleRev = view.currentRevision()
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)

        let actualRev = view.currentRevision()
        XCTAssertFalse(view.applyPresenceSnapshot(RemotePresenceSnapshot(), expectedRevision: actualRev &- 1))

        // Revision MUST advance: the stale-clear marked monthA changed.
        let postStaleRev = view.currentRevision()
        XCTAssertGreaterThan(postStaleRev, preStaleRev,
                             "stale-clear MUST bump revision so incremental state(since:) observes the drop")
        // monthA MUST appear in the incremental state delta with isAuthoritative == false.
        let stateDelta = view.state(since: preStaleRev)
        let entry = stateDelta.monthDeltas.first(where: { $0.month == monthA })
        XCTAssertNotNil(entry, "previously-fresh month MUST appear in incremental delta after stale-clear")
        XCTAssertEqual(entry?.presence.isAuthoritative, false)
    }

    // MARK: - clearPresenceFreshness

    func testClearPresenceFreshness_marksPreviouslyFreshMonths() {
        let view = RepoCommittedView()
        let hA = TestFixtures.fingerprint(0x21)
        let hB = TestFixtures.fingerprint(0x22)
        _ = view.replaceMonth(monthA, resources: [resource(for: monthA, hash: hA)], assets: [], assetResourceLinks: [],
                              physicallyMissingHashes: [], freshness: .markFresh)
        _ = view.replaceMonth(monthB, resources: [resource(for: monthB, hash: hB)], assets: [], assetResourceLinks: [],
                              physicallyMissingHashes: [], freshness: .markFresh)
        let pre = view.currentRevision()

        view.clearPresenceFreshness()

        XCTAssertGreaterThan(view.currentRevision(), pre,
                             "clearPresenceFreshness MUST bump revision when there were previously-fresh months")
        XCTAssertNil(view.verifiedPhysicallyMissingHashes(for: monthA))
        XCTAssertNil(view.verifiedPhysicallyMissingHashes(for: monthB))
        let stateDelta = view.state(since: pre)
        let months = Set(stateDelta.monthDeltas.map(\.month))
        XCTAssertTrue(months.contains(monthA))
        XCTAssertTrue(months.contains(monthB))
    }

    // MARK: - removeMonth (Reviewer A HIGH / Reviewer B LOW regression)

    /// `removeMonth(_:)` on an authoritative-empty month (seeded via `applyPresenceSnapshot`
    /// with `isAuthoritative: true, missingHashes: []`) has no cache payload, so
    /// `cache.removeMonth` returns false without bumping revision. The freshness clear MUST
    /// still call `cache.markMonthsChanged([month])` so an incremental `state(since:)`
    /// consumer observes `isAuthoritative` dropping from true to false.
    func testRemoveMonth_authoritativeEmpty_freshnessClearIsRevisionVisible() {
        let view = RepoCommittedView()
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [], isAuthoritative: true)
        XCTAssertTrue(view.applyPresenceSnapshot(builder.build()))
        let base = view.currentRevision()
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)

        // Cache has no payload for monthA, so cache.removeMonth returns false.
        XCTAssertFalse(view.removeMonth(monthA))

        XCTAssertGreaterThan(view.currentRevision(), base,
                             "removeMonth on authoritative-empty month MUST bump revision via markMonthsChanged")
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, false)
        let stateDelta = view.state(since: base)
        let entry = stateDelta.monthDeltas.first(where: { $0.month == monthA })
        XCTAssertNotNil(entry,
                        "freshness clear via removeMonth on cache-empty month MUST surface in incremental state")
        XCTAssertEqual(entry?.presence.isAuthoritative, false)
    }

    /// Pre-existing behavior: `removeMonth(_:)` on a non-fresh, cache-empty month is a no-op
    /// (returns false, no revision bump). This pins that the new `wasFresh && !removed`
    /// branch does NOT fire when there was nothing to clear.
    func testRemoveMonth_nothingToClear_noRevisionBump() {
        let view = RepoCommittedView()
        let base = view.currentRevision()
        XCTAssertFalse(view.removeMonth(monthA))
        XCTAssertEqual(view.currentRevision(), base,
                       "removeMonth on a month that was neither in cache nor fresh MUST be a no-op")
    }

    /// Unit-027 residual (Reviewer B): `removeMonth(_:)` on a cache-empty month carrying
    /// non-authoritative missing-hashes (seeded via `markPhysicallyMissing`) silently dropped
    /// the overlay entry without bumping revision. After unit-032, the missing-set drop MUST
    /// be visible to an incremental `state(since:)` consumer, mirroring the unit-027
    /// freshness-clear path.
    func testRemoveMonth_cacheEmptyNonFreshMissingHashes_revisionVisible() {
        let view = RepoCommittedView()
        let missingHash = TestFixtures.fingerprint(0xC1)

        // markPhysicallyMissing itself calls cache.markMonthsChanged, so the seed bumps revision once.
        view.markPhysicallyMissing(month: monthA, hashes: [missingHash])

        XCTAssertEqual(view.presenceSnapshot(for: monthA).missingHashes, [missingHash])
        XCTAssertFalse(view.presenceSnapshot(for: monthA).isAuthoritative,
                       "precondition: month must be non-authoritative for the gap scenario")

        // Capture base AFTER the seed so state(since: base) cannot match the seed delta.
        let base = view.currentRevision()

        // Cache has no payload for monthA, so cache.removeMonth returns false.
        XCTAssertFalse(view.removeMonth(monthA))

        XCTAssertGreaterThan(view.currentRevision(), base,
                             "removeMonth on cache-empty non-fresh missing-hash month MUST bump revision via markMonthsChanged")

        let stateDelta = view.state(since: base)
        let entry = stateDelta.monthDeltas.first(where: { $0.month == monthA })
        XCTAssertNotNil(entry,
                        "missing-hash drop via removeMonth on cache-empty month MUST surface in incremental state")
        XCTAssertEqual(entry?.presence.missingHashes, Set<Data>(),
                       "post-removal delta MUST emit empty missing-hashes")
        XCTAssertEqual(entry?.presence.isAuthoritative, false,
                       "non-fresh precondition + freshness clear ⇒ delta authority remains false")

        XCTAssertEqual(view.presenceSnapshot(for: monthA).missingHashes, Set<Data>())
    }

    // MARK: - loadFromMaterialize / reset

    func testLoadFromMaterialize_clearsFreshness() async {
        let view = RepoCommittedView()
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: TestFixtures.fingerprint(0x31))],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [],
            freshness: .markFresh
        )
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)

        let output = RepoMaterializer.MaterializeOutput(
            state: RepoSnapshotState(months: [:], observedClock: 0),
            observedSeqByWriter: [:],
            coveredByMonth: [:],
            acceptedSnapshotBaselinesByMonth: [:],
            trustByMonth: [:],
            repoID: nil
        )
        _ = view.loadFromMaterialize(output)

        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, false,
                       "loadFromMaterialize MUST clear freshness as its first step")
    }

    func testLoadFromMaterialize_returnsNonAuthoritativeSnapshot() {
        let view = RepoCommittedView()
        let h = TestFixtures.fingerprint(0x71)
        let physicalPath = String(format: "%04d/%02d/%@", monthA.year, monthA.month, h.hexString)

        _ = view.replaceMonth(
            monthA,
            resources: [TestFixtures.remoteResource(year: monthA.year, month: monthA.month, contentHash: h)],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [h],
            freshness: .markFresh
        )
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)
        XCTAssertEqual(view.physicallyMissingHashes(for: monthA), [h])

        let monthState = RepoMonthState(
            assets: [:],
            resources: [
                RemotePhysicalPathKey(physicalPath): SnapshotResourceRow(
                    physicalRemotePath: physicalPath,
                    contentHash: h,
                    fileSize: 100,
                    resourceType: ResourceTypeCode.photo,
                    creationDateMs: nil,
                    backedUpAtMs: 2,
                    crypto: nil
                )
            ],
            assetResources: [:],
            deletedAssetStamps: [:]
        )
        let output = RepoMaterializer.MaterializeOutput(
            state: RepoSnapshotState(months: [monthA: monthState], observedClock: 0),
            observedSeqByWriter: [:],
            coveredByMonth: [:],
            acceptedSnapshotBaselinesByMonth: [:],
            trustByMonth: [:],
            repoID: nil
        )
        let returned: RemotePresenceSnapshot = view.loadFromMaterialize(output)

        let entryA = returned.month(monthA)
        XCTAssertEqual(entryA.missingHashes, [h])
        XCTAssertEqual(entryA.isAuthoritative, false,
                       "loadFromMaterialize wraps preservedOverlay as failClosed — all months non-authoritative")
        XCTAssertTrue(returned.freshMonths.isEmpty,
                      "slice 3 cleared freshness; slice 4 reflects that in the return type")

        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, false)
        XCTAssertEqual(view.physicallyMissingHashes(for: monthA), [h],
                       "view retains [h] after intersection with stillPresent")
    }

    func testLoadFromMaterialize_populatesNonCleanOutcomeMonths() {
        let view = RepoCommittedView()
        let ambiguous = RepoMaterializer.MonthTrust(reasons: [
            RepoMaterializer.MonthTrustReason(kind: .ambiguousSnapshotCoverage, category: .ambiguous)
        ])
        XCTAssertEqual(ambiguous.outcome, .ambiguous)
        let output = RepoMaterializer.MaterializeOutput(
            state: RepoSnapshotState(months: [:], observedClock: 0),
            observedSeqByWriter: [:],
            coveredByMonth: [:],
            acceptedSnapshotBaselinesByMonth: [:],
            trustByMonth: [monthA: ambiguous, monthB: .clean],
            repoID: nil
        )
        _ = view.loadFromMaterialize(output)
        XCTAssertEqual(view.monthsWithNonCleanOutcome(), [monthA],
                       "non-clean months come from materialize outcomes; clean months are excluded")

        view.reset()
        XCTAssertTrue(view.monthsWithNonCleanOutcome().isEmpty, "reset clears non-clean months")
    }

    func testReset_clearsFreshness() {
        let view = RepoCommittedView()
        _ = view.replaceMonth(
            monthA,
            resources: [resource(for: monthA, hash: TestFixtures.fingerprint(0x41))],
            assets: [],
            assetResourceLinks: [],
            physicallyMissingHashes: [],
            freshness: .markFresh
        )
        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, true)

        view.reset()

        XCTAssertEqual(view.presenceSnapshot(for: monthA).isAuthoritative, false)
        XCTAssertNil(view.verifiedPhysicallyMissingHashes(for: monthA))
    }

    // MARK: - state(since:) / monthRawData(for:) propagate isAuthoritative

    func testStateAndMonthRawData_propagateIsAuthoritative() {
        let view = RepoCommittedView()
        let hA = TestFixtures.fingerprint(0x51)
        _ = view.replaceMonth(monthA, resources: [resource(for: monthA, hash: hA)], assets: [], assetResourceLinks: [])
        let rev = view.currentRevision()
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [], isAuthoritative: true)
        XCTAssertTrue(view.applyPresenceSnapshot(builder.build(), expectedRevision: rev))

        let state = view.state(since: nil)
        let monthDelta = state.monthDeltas.first(where: { $0.month == monthA })
        XCTAssertNotNil(monthDelta)
        XCTAssertEqual(monthDelta?.presence.isAuthoritative, true)

        let raw = view.monthRawData(for: monthA)
        XCTAssertNotNil(raw)
        XCTAssertEqual(raw?.presence.isAuthoritative, true)
    }

    // MARK: - fullPresenceSnapshot union

    func testFullPresenceSnapshot_includesAuthoritativeEmptyMonths() {
        let view = RepoCommittedView()
        // Seed nothing; apply an authoritative-empty snapshot for monthA.
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [], isAuthoritative: true)
        XCTAssertTrue(view.applyPresenceSnapshot(builder.build()))

        let full = view.fullPresenceSnapshot()
        let entry = full.entries.first(where: { $0.month == monthA })
        XCTAssertNotNil(entry,
                        "authoritative-empty month MUST appear in fullPresenceSnapshot.entries via the union")
        XCTAssertEqual(entry?.value.missingHashes, Set<Data>())
        XCTAssertEqual(entry?.value.isAuthoritative, true)
    }

    // MARK: - replaceMonth freshness-flip-only marks the cache changed (Codex HIGH test #12)

    func testReplaceMonth_freshnessFlipOnly_marksMonthChanged() {
        let view = RepoCommittedView()
        let resourceHash = TestFixtures.fingerprint(0x61)
        let resources = [resource(for: monthA, hash: resourceHash)]
        _ = view.replaceMonth(monthA, resources: resources, assets: [], assetResourceLinks: [],
                              physicallyMissingHashes: nil, freshness: .markFresh)
        let mid = view.currentRevision()

        // Flip ONLY freshness (same resources, nil missing-hashes, markStale).
        _ = view.replaceMonth(monthA, resources: resources, assets: [], assetResourceLinks: [],
                              physicallyMissingHashes: nil, freshness: .markStale)
        let after = view.currentRevision()
        XCTAssertGreaterThan(after, mid,
                             "freshness-only flip MUST advance cache revision (Codex HIGH contract)")

        let stateDelta = view.state(since: mid)
        let entry = stateDelta.monthDeltas.first(where: { $0.month == monthA })
        XCTAssertNotNil(entry, "freshness-flipped month MUST appear in incremental view delta")
        XCTAssertEqual(entry?.presence.isAuthoritative, false)
    }

    // MARK: - Service-level end-to-end (Codex HIGH tests #10, #11)

    /// Test #10: applyPresenceSnapshotForTest flips authority false→true.
    /// `currentState(since: baseRevision)` MUST advance revision and surface the month.
    func testCurrentState_authoritativeFlipFromFalseToTrue_advancesRevision_andIncludesMonth() {
        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(TestFixtures.remoteResource(
            year: monthA.year, month: monthA.month,
            contentHash: TestFixtures.fingerprint(0x71)
        ))
        let base = service.currentState(since: nil).revision

        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [], isAuthoritative: true)
        XCTAssertTrue(service.applyPresenceSnapshotForTest(builder.build()))

        let after = service.currentState(since: base)
        XCTAssertGreaterThan(after.revision, base,
                             "freshness flip MUST advance revision (Codex HIGH contract)")
        let delta = after.monthDeltas.first(where: { $0.month == monthA })
        XCTAssertNotNil(delta, "freshness-flipped month MUST appear in incremental state delta")
        XCTAssertEqual(delta?.presence.isAuthoritative, true)
        XCTAssertEqual(delta?.presence.missingHashes, Set<Data>())
    }

    /// Test #11: clearing freshness with unchanged missing hashes.
    /// `currentState(since: midRevision)` MUST advance and surface the month with authority false.
    func testCurrentState_freshnessClear_unchangedMissing_advancesRevision_andIncludesMonth() {
        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(TestFixtures.remoteResource(
            year: monthA.year, month: monthA.month,
            contentHash: TestFixtures.fingerprint(0x81)
        ))
        var seed = RemotePresenceSnapshot.Builder()
        seed.set(monthA, missingHashes: [], isAuthoritative: true)
        XCTAssertTrue(service.applyPresenceSnapshotForTest(seed.build()))
        let mid = service.currentState(since: nil).revision

        // Clear freshness via applying an empty snapshot (no entries; freshMonths == []).
        XCTAssertTrue(service.applyPresenceSnapshotForTest(RemotePresenceSnapshot()))

        let after = service.currentState(since: mid)
        XCTAssertGreaterThan(after.revision, mid,
                             "freshness clear MUST advance revision (Codex HIGH contract)")
        let delta = after.monthDeltas.first(where: { $0.month == monthA })
        XCTAssertNotNil(delta, "freshness-cleared month MUST appear in incremental state delta")
        XCTAssertEqual(delta?.presence.isAuthoritative, false)
    }
}

import XCTest
@testable import Watermelon

// The single presence derivation every source, the viewer, and upload success now share. (The index's
// refresh/profile-gating touches a real hash index + snapshot and is manually regressed.)
final class LibraryPresenceIndexTests: XCTestCase {
    private actor RefreshInvocationCounter {
        private(set) var count = 0
        private(set) var cancellationCount = 0

        func committedAfterDelay() async -> PresenceRefreshSingleFlight.Outcome {
            count += 1
            do {
                try await Task.sleep(nanoseconds: 50_000_000)
            } catch {
                cancellationCount += 1
                return .failed
            }
            return .committed
        }
    }

    func testRefreshSingleFlightCoalescesOnlyMatchingGenerationAndProfile() async {
        let singleFlight = PresenceRefreshSingleFlight()
        let counter = RefreshInvocationCounter()
        let key = PresenceRefreshSingleFlight.Key(
            localGeneration: 1,
            remoteGeneration: 1,
            profileKey: "A",
            remoteRevision: 1
        )

        async let first = singleFlight.run(key: key, notifyOnCommit: false) {
            await counter.committedAfterDelay()
        }
        await Task.yield()
        async let second = singleFlight.run(key: key, notifyOnCommit: true) {
            await counter.committedAfterDelay()
        }
        let results = await [first, second]
        let invocationCount = await counter.count

        XCTAssertEqual(invocationCount, 1)
        XCTAssertEqual(results.filter(\.shouldNotify).count, 1)
        XCTAssertTrue(results.allSatisfy { $0.outcome == .committed })
    }

    func testRefreshSingleFlightCancelsSupersededGeneration() async {
        let singleFlight = PresenceRefreshSingleFlight()
        let counter = RefreshInvocationCounter()
        let old = PresenceRefreshSingleFlight.Key(
            localGeneration: 1,
            remoteGeneration: 1,
            profileKey: "A",
            remoteRevision: 1
        )
        let current = PresenceRefreshSingleFlight.Key(
            localGeneration: 2,
            remoteGeneration: 1,
            profileKey: "A",
            remoteRevision: 1
        )

        async let first = singleFlight.run(key: old, notifyOnCommit: false) {
            await counter.committedAfterDelay()
        }
        while await counter.count == 0 {
            await Task.yield()
        }
        async let second = singleFlight.run(key: current, notifyOnCommit: false) {
            await counter.committedAfterDelay()
        }
        let results = await [first, second]
        let invocationCount = await counter.count
        let cancellationCount = await counter.cancellationCount

        XCTAssertEqual(invocationCount, 2)
        XCTAssertEqual(cancellationCount, 1)
        XCTAssertEqual(results.filter { $0.outcome == .committed }.count, 1)
    }

    func testSuspendedInvalidationsCoalesceAtOutermostResume() {
        var gate = PresenceInvalidationGate()

        gate.suspend()
        gate.suspend()
        XCTAssertFalse(gate.recordInvalidation())
        XCTAssertFalse(gate.recordInvalidation())
        XCTAssertFalse(gate.resume())
        XCTAssertTrue(gate.resume())
        XCTAssertFalse(gate.resume())
        XCTAssertTrue(gate.recordInvalidation())
    }

    func testBackupPresenceVerdictFailsClosedUntilCurrentAndAuthoritative() {
        XCTAssertEqual(
            LibraryPresenceIndex.classifyBackupPresence(
                isCurrent: false,
                isAuthoritative: true,
                isComplete: true,
                isBackedUp: true
            ),
            .unknown
        )
        XCTAssertEqual(
            LibraryPresenceIndex.classifyBackupPresence(
                isCurrent: true,
                isAuthoritative: false,
                isComplete: true,
                isBackedUp: true
            ),
            .unknown
        )
        XCTAssertEqual(
            LibraryPresenceIndex.classifyBackupPresence(
                isCurrent: true,
                isAuthoritative: true,
                isComplete: true,
                isBackedUp: true
            ),
            .complete
        )
        XCTAssertEqual(
            LibraryPresenceIndex.classifyBackupPresence(
                isCurrent: true,
                isAuthoritative: true,
                isComplete: true,
                isBackedUp: false
            ),
            .absent
        )
        XCTAssertEqual(
            LibraryPresenceIndex.classifyBackupPresence(
                isCurrent: true,
                isAuthoritative: true,
                isComplete: false,
                isBackedUp: true
            ),
            .incomplete
        )
        XCTAssertEqual(
            LibraryPresenceIndex.classifyBackupPresence(
                isCurrent: true,
                isAuthoritative: true,
                isComplete: false,
                isBackedUp: false
            ),
            .absent
        )
    }

    func testDisconnectedRemoteInputIsAuthoritativeEmpty() {
        let input = LibraryPresenceInputLoader.disconnectedRemoteInput(
            revision: 9
        )

        XCTAssertTrue(input.isAuthoritative)
        XCTAssertEqual(input.state.revision, 9)
        XCTAssertNil(input.state.profileKey)
        XCTAssertTrue(input.state.monthDeltas.isEmpty)
        XCTAssertEqual(input.monthCount, 0)
    }

    func testProjectionRenderabilityDependsOnProfilesNotRevision() {
        XCTAssertTrue(
            LibraryPresenceIndex.isRemoteBrowserProjectionRenderable(
                projectionProfileKey: "profile",
                currentProfileKey: "profile",
                expectedProfileKey: "profile"
            )
        )
        XCTAssertFalse(
            LibraryPresenceIndex.isRemoteBrowserProjectionRenderable(
                projectionProfileKey: nil,
                currentProfileKey: "profile",
                expectedProfileKey: "profile"
            )
        )
        XCTAssertFalse(
            LibraryPresenceIndex.isRemoteBrowserProjectionRenderable(
                projectionProfileKey: "profile",
                currentProfileKey: "other",
                expectedProfileKey: "profile"
            )
        )
        XCTAssertFalse(
            LibraryPresenceIndex.isRemoteBrowserProjectionRenderable(
                projectionProfileKey: "other",
                currentProfileKey: "profile",
                expectedProfileKey: "profile"
            )
        )
    }

    func testRemoteSnapshotOwnershipRequiresExactProfileMatch() {
        XCTAssertTrue(
            RemoteSnapshotOwnership.matches(
                ownerProfileKey: "profile",
                expectedProfileKey: "profile"
            )
        )
        XCTAssertTrue(
            RemoteSnapshotOwnership.matches(
                ownerProfileKey: nil,
                expectedProfileKey: nil
            )
        )
        XCTAssertFalse(
            RemoteSnapshotOwnership.matches(
                ownerProfileKey: nil,
                expectedProfileKey: "profile"
            )
        )
        XCTAssertFalse(
            RemoteSnapshotOwnership.matches(
                ownerProfileKey: "other",
                expectedProfileKey: "profile"
            )
        )
    }

    func testRemoteStateIsCurrentRequiresAuthoritativeMatchingOwner() {
        XCTAssertTrue(
            LibraryPresenceIndex.remoteStateIsCurrent(
                isBuilt: true,
                isAuthoritative: true,
                ownerProfileKey: "profile",
                expectedProfileKey: "profile",
                committedRevision: 7,
                liveRevision: 7
            )
        )
        XCTAssertFalse(
            LibraryPresenceIndex.remoteStateIsCurrent(
                isBuilt: true,
                isAuthoritative: false,
                ownerProfileKey: "profile",
                expectedProfileKey: "profile",
                committedRevision: 7,
                liveRevision: 7
            )
        )
        XCTAssertFalse(
            LibraryPresenceIndex.remoteStateIsCurrent(
                isBuilt: true,
                isAuthoritative: true,
                ownerProfileKey: nil,
                expectedProfileKey: "profile",
                committedRevision: 7,
                liveRevision: 7
            )
        )
    }

    func testFullLibraryScanSelection() {
        XCTAssertTrue(
            LibraryPresenceIndex.shouldScanFullLibrary(
                requestedCount: 23_339,
                libraryCount: 23_346
            )
        )
        XCTAssertFalse(
            LibraryPresenceIndex.shouldScanFullLibrary(
                requestedCount: 999,
                libraryCount: 999
            )
        )
        XCTAssertFalse(
            LibraryPresenceIndex.shouldScanFullLibrary(
                requestedCount: 10_000,
                libraryCount: 30_000
            )
        )
    }

    func testStaleHashRowRule() {
        // Home's staleness rule, now shared by the browser: a row older than the asset's Photos edit no
        // longer fingerprints the current bytes — the item must not read backed-up nor render as that
        // fingerprint (shared L1 / L2 sidecar poisoning).
        let rowAt = Date(timeIntervalSince1970: 1000)
        XCTAssertTrue(LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: rowAt, assetModificationDate: nil))
        XCTAssertTrue(LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: rowAt, assetModificationDate: Date(timeIntervalSince1970: 900)))
        XCTAssertTrue(LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: rowAt, assetModificationDate: rowAt))
        XCTAssertFalse(LibraryPresenceIndex.isRowCurrent(recordUpdatedAt: rowAt, assetModificationDate: Date(timeIntervalSince1970: 1001)))
    }

    func testCurrentFingerprintsDropRules() {
        // The batch validator behind remote/merged handle binding and the upload success verdicts: a stale
        // row must not bind a device handle to its pre-edit fingerprint (`.both` projection, local-first
        // full-size/share, Delete-from-Device) nor count a skipped upload as backed up; an unfetchable
        // asset (deleted, or outside a limited-access selection) proves nothing and is dropped too.
        let fp1 = Data([0x01]), fp2 = Data([0x02])
        let rowAt = Date(timeIntervalSince1970: 1000)
        let records = [
            "current": LocalAssetFingerprintRecord(fingerprint: fp1, updatedAt: rowAt),
            "stale": LocalAssetFingerprintRecord(fingerprint: fp2, updatedAt: rowAt),
            "unfetched": LocalAssetFingerprintRecord(fingerprint: fp1, updatedAt: rowAt),
            "noModificationDate": LocalAssetFingerprintRecord(fingerprint: fp2, updatedAt: rowAt),
        ]
        let modificationDates: [String: Date?] = [
            "current": Date(timeIntervalSince1970: 900),
            "stale": Date(timeIntervalSince1970: 1001),
            "noModificationDate": nil,
            // "unfetched" absent: PHAsset fetch miss
        ]
        let result = LibraryPresenceIndex.currentFingerprints(records: records, modificationDateByAssetID: modificationDates)
        XCTAssertEqual(result, ["current": fp1, "noModificationDate": fp2])
    }

    func testSelectCurrentHandlesFallback() {
        // The reverse map keeps one arbitrary row per fingerprint. After downloading an edited-after-backup
        // asset back, the older stale row can be the map's candidate while the fresh import's current row
        // exists — the validator must bind the current row instead of dropping the handle (else the record
        // keeps offering Download for bytes already on device and every re-tap imports a duplicate).
        let shadowed = Data([0x0A]), healthy = Data([0x0B]), gone = Data([0x0C]), moved = Data([0x0D])
        let mapHits = [shadowed: "staleOld", healthy: "ok", gone: "staleOnly", moved: "rehashed"]
        let alternatives = [
            shadowed: ["freshImport", "staleTwin"],
            gone: ["alsoStale"],
        ]
        let current = [
            "ok": healthy,           // candidate row current → candidate wins
            "freshImport": shadowed, // candidate stale, alternative current → alternative binds
            "rehashed": Data([0x0E]),   // row moved to another fingerprint → candidate identity check fails
            // "staleOld"/"staleTwin"/"alsoStale" absent: stale or unfetchable
        ]
        let result = LibraryPresenceIndex.selectCurrentHandles(
            mapHits: mapHits,
            alternativesByFingerprint: alternatives,
            currentFingerprintsByAssetID: current
        )
        XCTAssertEqual(result, [healthy: "ok", shadowed: "freshImport"])
    }

    func testPrevalidatedAlternativesUseOnlyRequestedCurrentFingerprints() {
        let requested = Data([0x01])
        let unrelated = Data([0x02])
        let result = LibraryPresenceIndex.prevalidatedAlternatives(
            for: [requested],
            currentFingerprintsByAssetID: [
                "first": requested,
                "second": requested,
                "unrelated": unrelated,
            ]
        )

        XCTAssertEqual(Set(result[requested] ?? []), ["first", "second"])
        XCTAssertNil(result[unrelated])
    }
}

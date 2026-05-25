import CryptoKit
import XCTest
@testable import Watermelon

final class RemotePresenceSnapshotTests: XCTestCase {
    private let monthA = LibraryMonthKey(year: 2025, month: 1)
    private let monthB = LibraryMonthKey(year: 2025, month: 2)

    // MARK: - Value-type contract

    func testMonth_returnsAbsent_whenNeverTouched() {
        let snapshot = RemotePresenceSnapshot()
        XCTAssertEqual(snapshot.month(monthA), .absent)
        XCTAssertEqual(snapshot.month(monthA).missingHashes, [])
        XCTAssertFalse(snapshot.month(monthA).isAuthoritative)
        XCTAssertTrue(snapshot.entries.isEmpty)
        XCTAssertTrue(snapshot.freshMonths.isEmpty)
    }

    func testMonth_returnsHashesAndAuthoritative_whenBuilderSetsBoth() {
        let h1 = TestFixtures.fingerprint(0x01)
        let h2 = TestFixtures.fingerprint(0x02)
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [h1, h2], isAuthoritative: true)
        let snapshot = builder.build()
        XCTAssertEqual(snapshot.month(monthA).missingHashes, [h1, h2])
        XCTAssertTrue(snapshot.month(monthA).isAuthoritative)
        XCTAssertEqual(snapshot.freshMonths, [monthA])
    }

    func testMonth_returnsHashesAndNotAuthoritative_whenAuthoritativeFalse() {
        let h1 = TestFixtures.fingerprint(0x03)
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [h1], isAuthoritative: false)
        let snapshot = builder.build()
        XCTAssertEqual(snapshot.month(monthA).missingHashes, [h1])
        XCTAssertFalse(snapshot.month(monthA).isAuthoritative)
        XCTAssertTrue(snapshot.freshMonths.isEmpty)
    }

    func testMonth_emptyHashesAuthoritative_isLegalAndRoundTrips() {
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [], isAuthoritative: true)
        let snapshot = builder.build()
        XCTAssertEqual(snapshot.month(monthA).missingHashes, [])
        XCTAssertTrue(snapshot.month(monthA).isAuthoritative)
        XCTAssertEqual(snapshot.freshMonths, [monthA])
        XCTAssertEqual(snapshot.entries.count, 1, "authoritative empty entry MUST be preserved in entries")
    }

    /// Pins the load-bearing stale-clear contract: explicit empty non-authoritative entries
    /// MUST round-trip through `entries` so apply paths still call `markPhysicallyMissing(hashes: [])`.
    func testMonth_emptyHashesNonAuthoritative_isPreserved_inEntries() {
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [], isAuthoritative: false)
        let snapshot = builder.build()
        XCTAssertEqual(snapshot.entries.count, 1, "empty non-authoritative entry MUST be preserved")
        let entry = snapshot.entries.first!
        XCTAssertEqual(entry.month, monthA)
        XCTAssertEqual(entry.value.missingHashes, [])
        XCTAssertFalse(entry.value.isAuthoritative)
        XCTAssertTrue(snapshot.freshMonths.isEmpty)
    }

    func testFailClosed_wrapsRawDictionaryAsNonAuthoritativeEntries() {
        let h1 = TestFixtures.fingerprint(0x10)
        let h2 = TestFixtures.fingerprint(0x11)
        let raw: [LibraryMonthKey: Set<Data>] = [monthA: [h1, h2], monthB: [h1]]
        let snapshot = RemotePresenceSnapshot.failClosed(missingByMonth: raw)
        XCTAssertEqual(snapshot.month(monthA).missingHashes, [h1, h2])
        XCTAssertEqual(snapshot.month(monthB).missingHashes, [h1])
        XCTAssertFalse(snapshot.month(monthA).isAuthoritative,
                       "syncIndexV2 fallback must NOT mark months fresh — `failClosed` is the named conversion")
        XCTAssertFalse(snapshot.month(monthB).isAuthoritative)
        XCTAssertTrue(snapshot.freshMonths.isEmpty)
        XCTAssertEqual(snapshot.entries.count, 2)
    }

    // MARK: - RemoteIndexSyncService accessor contract

    func testService_presenceSnapshot_forUntouchedMonth_isAbsent() {
        let service = RemoteIndexSyncService()
        let presence = service.presenceSnapshot(for: monthA)
        XCTAssertEqual(presence, .absent)
        XCTAssertEqual(presence.missingHashes, [])
        XCTAssertFalse(presence.isAuthoritative)
    }

    func testService_presenceSnapshot_afterMarkOnly_isFailClosedNotAuthoritative() {
        let service = RemoteIndexSyncService()
        let hash = TestFixtures.fingerprint(0x20)
        service.markPhysicallyMissingV2(month: monthA, hashes: [hash])
        let presence = service.presenceSnapshot(for: monthA)
        XCTAssertEqual(presence.missingHashes, [hash])
        XCTAssertFalse(presence.isAuthoritative,
                       "markPhysicallyMissingV2 alone does NOT mark the month fresh; freshness comes from the probe")
    }

    func testService_presenceSnapshot_afterFreshProbe_isAuthoritative() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        let bytes = Data("presence-snapshot-fresh".utf8)
        let hash = Data(SHA256.hash(data: bytes))
        let name = "f.jpg"
        let resource = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/\(name)",
            contentHash: hash, fileSize: Int64(bytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        writer.appendResource(resource)
        await client.injectFile(path: "\(basePath)/\(monthRel)/\(name)", data: bytes)

        _ = try await service.refreshPhysicalPresenceOverlay(
            client: client,
            basePath: basePath
        )

        let presence = service.presenceSnapshot(for: monthA)
        XCTAssertTrue(presence.isAuthoritative,
                      "fresh probe must surface as isAuthoritative=true on the typed accessor")
        XCTAssertEqual(presence.missingHashes, [])
    }

    // MARK: - Stale-clear integration (per re-review A Low #2 + reviewer B Medium #1)

    /// hPresent verifies present (hash match) and hIncon is listed at the expected size but its
    /// download fails persistently — verifyHashResult returns `.inconclusive(.probeFailure)`.
    /// Under `.preserveFallback`, `monthFresh = inconclusiveHashes.isEmpty` is false, so the month
    /// is NOT marked authoritative. The publish gate (`monthFresh || !missing.isEmpty ||
    /// !priorFallback.isEmpty`) still emits an empty-missing entry because the prior fallback was
    /// non-empty, clearing the stale hPresent overlay. Pins all three plan invariants.
    func testService_freshProbeClearsStaleMissing_whenOtherHashInconclusive_viaTypedAccessor() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()

        // hPresent: listed at expected size, downloads successfully ⇒ presence==.hashVerified.
        let presentBytes = Data("typed-stale-clear-present".utf8)
        let hPresent = Data(SHA256.hash(data: presentBytes))
        let presentName = "present.jpg"
        writer.appendResource(RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/\(presentName)",
            contentHash: hPresent, fileSize: Int64(presentBytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        ))
        await client.injectFile(path: "\(basePath)/\(monthRel)/\(presentName)", data: presentBytes)

        // hIncon: listed at expected size, but download fails with notFound ⇒ verifyHashResult
        // returns .inconclusive ⇒ presence==.inconclusive(.probeFailure). Same deterministic
        // pattern as testRefreshPhysicalPresenceOverlay_preserveFallback_healedHashClearedDespiteUnrelatedInconclusives.
        let inconBytes = Data("typed-stale-clear-inconclusive".utf8)
        let hIncon = Data(SHA256.hash(data: inconBytes))
        let inconName = "incon.jpg"
        writer.appendResource(RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/\(inconName)",
            contentHash: hIncon, fileSize: Int64(inconBytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        ))
        await client.injectFile(path: "\(basePath)/\(monthRel)/\(inconName)", data: inconBytes)
        await client.injectPersistentDownloadError(.notFound, for: "\(basePath)/\(monthRel)/\(inconName)")

        // Stale prior overlay covers ONLY hPresent.
        service.markPhysicallyMissingV2(month: monthA, hashes: [hPresent])
        let priorFallback = service.fullPresenceSnapshot()
        XCTAssertEqual(priorFallback.month(monthA).missingHashes, [hPresent],
                       "precondition: prior fallback must contain hPresent so the !stale.isEmpty publish-gate branch fires")

        _ = try await service.refreshPhysicalPresenceOverlay(
            client: client,
            basePath: basePath,
            fallback: priorFallback
        )

        // (1) Stale entry cleared in the committed view.
        let published = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertEqual(published, [],
                       "stale hPresent must be cleared via the empty-entry publish gate, even when hIncon is still inconclusive")
        // (2) Typed accessor's missingHashes is empty.
        let typed = service.presenceSnapshot(for: monthA)
        XCTAssertEqual(typed.missingHashes, [],
                       "typed accessor must report empty missing hashes after the stale-clear round-trip")
        // (3) Month is NOT authoritative because hIncon stayed inconclusive under .preserveFallback.
        XCTAssertFalse(typed.isAuthoritative,
                       ".preserveFallback with an unresolved inconclusive MUST leave the month non-authoritative")
    }

    /// `fullPresenceSnapshot()` must represent authoritative-empty months via the
    /// `missingMap.keys ∪ physicalPresenceOverlayFreshMonths` union; the committed view drops empty
    /// per-month entries, so without the union the freshly-probed empty month would disappear.
    func testFullPresenceSnapshot_authoritativeEmptyMonth_appearsInEntries() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        let bytes = Data("full-presence-fresh-empty".utf8)
        let hash = Data(SHA256.hash(data: bytes))
        let name = "f.jpg"
        writer.appendResource(RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/\(name)",
            contentHash: hash, fileSize: Int64(bytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        ))
        await client.injectFile(path: "\(basePath)/\(monthRel)/\(name)", data: bytes)

        _ = try await service.refreshPhysicalPresenceOverlay(
            client: client,
            basePath: basePath
        )

        let full = service.fullPresenceSnapshot()
        let entry = full.entries.first(where: { $0.month == monthA })
        XCTAssertNotNil(entry,
                        "authoritative-empty month must appear in fullPresenceSnapshot().entries via the union")
        XCTAssertEqual(entry?.value.missingHashes, [])
        XCTAssertEqual(entry?.value.isAuthoritative, true,
                       "fullPresenceSnapshot must mark the freshly-probed empty month authoritative")
        XCTAssertEqual(full.freshMonths, [monthA])
    }

    // MARK: - V2 materialize fallback (per re-review B High + re-review A Low #1)

    /// Pins the V2 fallback path's contract: applying `failClosed(missingByMonth: priorOverlay)`
    /// restores fail-closed missing hashes WITHOUT marking the overlay authoritative.
    /// Exercised via the narrow `applyPresenceSnapshotForTest(_:)` seam because the production call
    /// chain (syncIndexV2 → refresh fails → apply) requires a full materialize engine setup.
    func testService_applyFailClosedSnapshot_restoresFailClosed_andDoesNotMarkFresh() {
        let service = RemoteIndexSyncService()
        let h1 = TestFixtures.fingerprint(0x30)
        let h2 = TestFixtures.fingerprint(0x31)
        let priorOverlay: [LibraryMonthKey: Set<Data>] = [monthA: [h1, h2]]

        let applied = service.applyPresenceSnapshotForTest(
            RemotePresenceSnapshot.failClosed(missingByMonth: priorOverlay)
        )
        XCTAssertTrue(applied)

        XCTAssertEqual(service.physicallyMissingHashesForTest(month: monthA), [h1, h2],
                       "fail-closed missing hashes restored from priorOverlay")
        let typed = service.presenceSnapshot(for: monthA)
        XCTAssertEqual(typed.missingHashes, [h1, h2])
        XCTAssertFalse(typed.isAuthoritative,
                       "syncIndexV2 cancellation/error fallback MUST NOT mark months fresh")
    }
}

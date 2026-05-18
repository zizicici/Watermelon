import CryptoKit
import XCTest
@testable import Watermelon

final class RemoteIndexSyncServiceTests: XCTestCase {
    private let monthA = LibraryMonthKey(year: 2025, month: 1)
    private let monthB = LibraryMonthKey(year: 2025, month: 2)

    private func seedCompleteAsset(
        in service: RemoteIndexSyncService,
        month: LibraryMonthKey,
        contentHash: Data
    ) -> Data {
        let role = ResourceTypeCode.photo
        let slot = 0
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: role, slot: slot, contentHash: contentHash)]
        )
        let resource = RemoteManifestResource(
            year: month.year,
            month: month.month,
            physicalRemotePath: String(format: "%04d/%02d/%@.jpg", month.year, month.month, contentHash.hexString),
            contentHash: contentHash,
            fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        let asset = RemoteManifestAsset(
            year: month.year, month: month.month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: month.year, month: month.month,
            assetFingerprint: fp, resourceHash: contentHash,
            role: role, slot: slot, logicalName: "x.jpg"
        )
        writer.appendAsset(asset, links: [link], markUncommitted: false)
        return fp
    }

    func testCrossMonthSameFingerprint_committingMonthAKeepsMonthBPending() {
        let service = RemoteIndexSyncService()
        let hash = TestFixtures.fingerprint(0xAB)

        // Seed BOTH months complete so committed-by-month sees them. Without the
        // resource+link, the asset would be filtered as phantom and the test
        // wouldn't exercise the per-month uncommitted subtraction at all.
        let fp = seedCompleteAsset(in: service, month: monthA, contentHash: hash)
        _ = seedCompleteAsset(in: service, month: monthB, contentHash: hash)

        // Both workers upsert the same fingerprint optimistically.
        let writer = service.makeOptimisticAssetWriter()
        writer.markUncommitted(month: monthA, fingerprints: [fp])
        writer.markUncommitted(month: monthB, fingerprints: [fp])

        // A's flush completes — clears uncommitted only for monthA.
        service.markCommittedV2(month: monthA, fingerprints: [fp])

        let byMonth = service.committedAssetFingerprintsByMonth()
        // monthA: cached + uncommitted cleared → committed contains fp
        XCTAssertTrue(byMonth[monthA]?.contains(fp) == true,
                      "month A's flush succeeded — fp must be in committed for A")
        // monthB: monthB's uncommitted still has fp → must NOT be committed for B.
        XCTAssertNil(byMonth[monthB],
                     "month B's flush hasn't run — fp must NOT be in committed for B")
    }

    func testResetUncommittedV2ClearsAllMonths() {
        let service = RemoteIndexSyncService()
        let h1 = TestFixtures.fingerprint(0xAA)
        let h2 = TestFixtures.fingerprint(0xBB)
        let fp1 = seedCompleteAsset(in: service, month: monthA, contentHash: h1)
        let fp2 = seedCompleteAsset(in: service, month: monthB, contentHash: h2)
        let writer = service.makeOptimisticAssetWriter()
        writer.markUncommitted(month: monthA, fingerprints: [fp1])
        writer.markUncommitted(month: monthB, fingerprints: [fp2])

        // Before reset: both months have fp uncommitted → committed by-month is empty.
        let before = service.committedAssetFingerprintsByMonth()
        XCTAssertTrue(before.isEmpty, "uncommitted everywhere → no committed yet")

        service.resetUncommittedV2()

        let after = service.committedAssetFingerprintsByMonth()
        XCTAssertEqual(after[monthA], [fp1])
        XCTAssertEqual(after[monthB], [fp2])
    }

    func testMarkCommittedV2EmptySetNoOp() {
        let service = RemoteIndexSyncService()
        service.markCommittedV2(month: monthA, fingerprints: [])
        XCTAssertTrue(service.committedAssetFingerprintsByMonth().isEmpty)
    }

    func testCommittedFingerprints_excludesPhantomAsset() {
        let service = RemoteIndexSyncService()
        let fp = TestFixtures.fingerprint(0x42)
        let phantom = RemoteManifestAsset(
            year: monthA.year, month: monthA.month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        service.makeOptimisticAssetWriter().appendAsset(phantom, links: nil, markUncommitted: false)
        let byMonth = service.committedAssetFingerprintsByMonth()
        XCTAssertNil(byMonth[monthA],
                     "phantom asset (no links) must not appear as committed")
    }

    func testCommittedFingerprints_excludesAssetWithMissingResource() {
        let service = RemoteIndexSyncService()
        let hash = TestFixtures.fingerprint(0x55)
        let role = ResourceTypeCode.photo
        let slot = 0
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: role, slot: slot, contentHash: hash)]
        )
        // No upsertCachedResource — link points to a hash with no resource row.
        let asset = RemoteManifestAsset(
            year: monthA.year, month: monthA.month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: monthA.year, month: monthA.month,
            assetFingerprint: fp, resourceHash: hash,
            role: role, slot: slot, logicalName: "x.jpg"
        )
        service.makeOptimisticAssetWriter().appendAsset(asset, links: [link], markUncommitted: false)
        let byMonth = service.committedAssetFingerprintsByMonth()
        XCTAssertNil(byMonth[monthA],
                     "asset with missing resource must not appear as committed")
    }

    func testCommittedFingerprints_subtractsPhysicallyMissingHash() {
        let service = RemoteIndexSyncService()
        let hash = TestFixtures.fingerprint(0x99)
        let role = ResourceTypeCode.photo
        let slot = 0
        let fp = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: role, slot: slot, contentHash: hash)]
        )
        let resource = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "2026/05/x.jpg",
            contentHash: hash, fileSize: 1,
            resourceType: role,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: monthA.year, month: monthA.month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: monthA.year, month: monthA.month,
            assetFingerprint: fp, resourceHash: hash,
            role: role, slot: slot, logicalName: "x.jpg"
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        writer.appendAsset(asset, links: [link], markUncommitted: false)

        XCTAssertTrue(service.committedAssetFingerprintsByMonth()[monthA]?.contains(fp) ?? false)

        service.markPhysicallyMissingV2(month: monthA, hashes: [hash])
        XCTAssertNil(service.committedAssetFingerprintsByMonth()[monthA])

        writer.appendResource(resource)
        XCTAssertTrue(service.committedAssetFingerprintsByMonth()[monthA]?.contains(fp) ?? false,
                      "fresh upload must clear the overlay entry")
    }

    func testApplyOptimisticUpsert_clearingOverlay_bumpsRevisionEvenWhenRowUnchanged() {
        let service = RemoteIndexSyncService()
        let hash = TestFixtures.fingerprint(0xA1)
        let resource = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "2026/05/y.jpg",
            contentHash: hash, fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        let revisionAfterFirstInsert = service.currentState(since: nil).revision
        service.markPhysicallyMissingV2(month: monthA, hashes: [hash])
        let revisionAfterMissing = service.currentState(since: nil).revision
        XCTAssertGreaterThan(revisionAfterMissing, revisionAfterFirstInsert)

        // Same bytes → cache.upsertResource is a no-op, but overlay clears.
        writer.appendResource(resource)
        let revisionAfterRetry = service.currentState(since: nil).revision
        XCTAssertGreaterThan(revisionAfterRetry, revisionAfterMissing,
                             "overlay clear must bump revision so partial-sync consumers re-fetch")
    }

    func testRefreshPhysicalPresenceOverlay_populatesAllCommittedMonths() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let service = RemoteIndexSyncService()
        // Sub-threshold file → overlay verifies via SHA-256, so the test content's hash must match.
        let presentBytes = Data("present-overlay-bytes".utf8)
        let hashPresent = Data(SHA256.hash(data: presentBytes))
        let hashGone = TestFixtures.fingerprint(0xBB)
        let role = ResourceTypeCode.photo

        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        let resPresent = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/present.jpg",
            contentHash: hashPresent, fileSize: Int64(presentBytes.count), resourceType: role,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let resGone = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/gone.jpg",
            contentHash: hashGone, fileSize: 1, resourceType: role,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resPresent)
        writer.appendResource(resGone)

        // Stage only one of the two on remote; the other is "physically missing".
        await client.injectFile(path: "\(basePath)/\(monthRel)/present.jpg", data: presentBytes)
        try await client.createDirectory(path: "\(basePath)/\(monthRel)")

        try await service.refreshPhysicalPresenceOverlay(client: client, basePath: basePath)

        XCTAssertFalse(service.physicallyMissingHashesForTest(month: monthA).contains(hashPresent),
                       "present hash must NOT be in missing overlay")
        XCTAssertTrue(service.physicallyMissingHashesForTest(month: monthA).contains(hashGone),
                      "absent hash must be in missing overlay")
    }

    func testRefreshPhysicalPresenceOverlay_perMonthBestEffort() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let service = RemoteIndexSyncService()
        let hashBad = TestFixtures.fingerprint(0xCE)
        let hashGood = TestFixtures.fingerprint(0xDE)
        let monthRelBad = String(format: "%04d/%02d", monthA.year, monthA.month)
        let monthRelGood = String(format: "%04d/%02d", monthB.year, monthB.month)
        let resourceBad = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRelBad)/x.jpg",
            contentHash: hashBad, fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let resourceGood = RemoteManifestResource(
            year: monthB.year, month: monthB.month,
            physicalRemotePath: "\(monthRelGood)/y.jpg",
            contentHash: hashGood, fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        let writer = service.makeOptimisticAssetWriter()
        writer.appendResource(resourceBad)
        writer.appendResource(resourceGood)
        // monthA dir absent → its probe throws. monthB dir exists but empty.
        try await client.createDirectory(path: "\(basePath)/\(monthRelGood)")

        // Function returns normally; failure is logged.
        try await service.refreshPhysicalPresenceOverlay(client: client, basePath: basePath)
        XCTAssertTrue(service.physicallyMissingHashesForTest(month: monthB).contains(hashGood),
                      "monthB probe succeeded → hashGood detected missing")
    }

    func testSyncIndex_damagedV2_throwsRatherThanFallingBackToV1() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        // Commits dir populated but version.json + repo.json gone.
        let commits = RepoLayout.commitsDirectoryPath(base: basePath)
        try await client.createDirectory(path: commits)
        await client.injectFile(path: "\(commits)/leftover.jsonl", contents: "stale")
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)

        let service = RemoteIndexSyncService()
        do {
            _ = try await service.syncIndex(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testSyncIndex_refusesUnsupportedFormat() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9")
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "future-id")
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)

        let service = RemoteIndexSyncService()
        do {
            _ = try await service.syncIndex(client: client, profile: profile)
            XCTFail("expected remoteFormatUnsupported")
        } catch BackupCompatibilityError.remoteFormatUnsupported(let minApp) {
            XCTAssertEqual(minApp, "9.9.9")
        }
    }

    func testSyncOverlayAndCaptureHandle_budgetExhausted_remainsFreshUnderFailClosedPolicy() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        try await client.createDirectory(path: "\(basePath)/\(monthRel)")

        // 65 distinct resources → budget cap of 64 files forces the 65th onto the
        // `.inconclusive(.verifyBudgetExhausted)` branch on at least one iteration order.
        for index in 0..<65 {
            let bytes = Data("overlay-probe-budget-\(index)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            let name = "f\(index).jpg"
            let resource = RemoteManifestResource(
                year: monthA.year, month: monthA.month,
                physicalRemotePath: "\(monthRel)/\(name)",
                contentHash: hash, fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil, backedUpAtMs: 0
            )
            writer.appendResource(resource)
            await client.injectFile(path: "\(basePath)/\(monthRel)/\(name)", data: bytes)
        }

        let handle = try await service.syncOverlayAndCaptureHandle(client: client, basePath: basePath)
        XCTAssertEqual(handle.overlayFreshness, .fresh,
                       "budget-exhausted inconclusives folded into missing must not block resume freshness")
        // Load-bearing side effect: the budget-exhausted inconclusive must also land
        // in the missing overlay, otherwise resume would dedup against unverified bytes.
        let monthMissing = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertFalse(monthMissing.isEmpty,
                       "budget-exhausted inconclusive hashes must be folded into missing under fail-closed")
        let verified = await service.verifiedPhysicallyMissingHashes(for: monthA)
        XCTAssertEqual(verified, monthMissing,
                       "freshness-aware accessor must publish the same missing set for the fresh month")
    }

    func testSyncOverlayAndCaptureHandle_partialFallback_failClosedFoldsRemainingInconclusives() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        try await client.createDirectory(path: "\(basePath)/\(monthRel)")

        for index in 0..<65 {
            let bytes = Data("overlay-probe-partial-fallback-\(index)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            let name = "f\(index).jpg"
            let resource = RemoteManifestResource(
                year: monthA.year, month: monthA.month,
                physicalRemotePath: "\(monthRel)/\(name)",
                contentHash: hash, fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil, backedUpAtMs: 0
            )
            writer.appendResource(resource)
            await client.injectFile(path: "\(basePath)/\(monthRel)/\(name)", data: bytes)
        }

        let foreignHash = Data(SHA256.hash(data: Data("foreign-hash-not-in-current-month".utf8)))
        service.markPhysicallyMissingV2(month: monthA, hashes: [foreignHash])

        let handle = try await service.syncOverlayAndCaptureHandle(client: client, basePath: basePath)
        XCTAssertEqual(handle.overlayFreshness, .fresh,
                       "fail-closed policy must fold all unresolved inconclusives into missing even when a partial fallback exists")
        // Load-bearing side effect: unresolved inconclusives must be folded into the
        // missing overlay even though a foreign fallback hash is seeded for the month.
        let monthMissing = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertFalse(monthMissing.isEmpty,
                       "fail-closed policy must populate missing with unresolved inconclusives")
        let verified = await service.verifiedPhysicallyMissingHashes(for: monthA)
        XCTAssertEqual(verified, monthMissing,
                       "freshness-aware accessor must publish the same missing set under partial fallback")
    }

    func testRefreshPhysicalPresenceOverlay_preserveFallback_budgetExhaustedNotFreshEvenWhenCovered() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        try await client.createDirectory(path: "\(basePath)/\(monthRel)")

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        var allHashes: Set<Data> = []
        for index in 0..<65 {
            let bytes = Data("preserve-fallback-budget-\(index)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            allHashes.insert(hash)
            let name = "f\(index).jpg"
            let resource = RemoteManifestResource(
                year: monthA.year, month: monthA.month,
                physicalRemotePath: "\(monthRel)/\(name)",
                contentHash: hash, fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil, backedUpAtMs: 0
            )
            writer.appendResource(resource)
            await client.injectFile(path: "\(basePath)/\(monthRel)/\(name)", data: bytes)
        }

        // Seed prior overlay covering EVERY hash — the cross-policy gate would have
        // marked the month fresh because resolvedInconclusives ⊇ inconclusiveHashes.
        service.markPhysicallyMissingV2(month: monthA, hashes: allHashes)

        _ = try await service.refreshPhysicalPresenceOverlay(
            client: client,
            basePath: basePath,
            fallback: [monthA: allHashes]
        )
        let verified = await service.verifiedPhysicallyMissingHashes(for: monthA)
        XCTAssertNil(verified,
                     ".preserveFallback must not advertise the fallback set as verified-missing for a budget-exhausted month")
    }

    func testRefreshPhysicalPresenceOverlay_monthDirNotFound_preservesPriorFallbackInsteadOfAllMissing() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        var allHashes: Set<Data> = []
        for index in 0..<3 {
            let bytes = Data("month-dir-404-\(index)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            allHashes.insert(hash)
            let name = "f\(index).jpg"
            let resource = RemoteManifestResource(
                year: monthA.year, month: monthA.month,
                physicalRemotePath: "\(monthRel)/\(name)",
                contentHash: hash, fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil, backedUpAtMs: 0
            )
            writer.appendResource(resource)
        }

        // Prior overlay had one hash flagged missing (fallback). The month directory
        // is NOT created on remote, so the probe's `list(monthAbs)` throws not-found.
        let priorMissing = Set([allHashes.first!])
        service.markPhysicallyMissingV2(month: monthA, hashes: priorMissing)

        _ = try await service.refreshPhysicalPresenceOverlay(
            client: client,
            basePath: basePath,
            fallback: [monthA: priorMissing]
        )

        let published = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertEqual(published, priorMissing,
                       "month-dir 404 must preserve prior fallback; widening to allHashes would publish a transient 404 as verified absence")
        XCTAssertFalse(published == allHashes,
                       "month-dir 404 must NOT publish every manifest hash as physicallyMissing")
        let verified = await service.verifiedPhysicallyMissingHashes(for: monthA)
        XCTAssertNil(verified,
                     "month-dir 404 must leave the month not-fresh so callers don't read the fallback as authoritative")
    }

    func testSyncOverlayAndCaptureHandle_failureArm_preservesFallbackInsteadOfAllMissing() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        try await client.createDirectory(path: "\(basePath)/\(monthRel)")

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        var allHashes: Set<Data> = []
        for index in 0..<3 {
            let bytes = Data("failure-arm-preserve-\(index)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            allHashes.insert(hash)
            let name = "f\(index).jpg"
            let resource = RemoteManifestResource(
                year: monthA.year, month: monthA.month,
                physicalRemotePath: "\(monthRel)/\(name)",
                contentHash: hash, fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil, backedUpAtMs: 0
            )
            writer.appendResource(resource)
            await client.injectFile(path: "\(basePath)/\(monthRel)/\(name)", data: bytes)
        }
        let priorMissing = Set([allHashes.first!])
        service.markPhysicallyMissingV2(month: monthA, hashes: priorMissing)

        // Force the month directory list to fail with a transport error so the
        // probe lands in the .failure arm with a non-empty fallback.
        await client.injectListError(.transport, for: "\(basePath)/\(monthRel)")

        let handle = try await service.syncOverlayAndCaptureHandle(client: client, basePath: basePath)
        XCTAssertEqual(handle.overlayFreshness, .stale,
                       "transport blip on a whole-month probe must yield a stale handle")
        let published = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertEqual(published, priorMissing,
                       ".failure arm must preserve the prior fallback subset, not widen to every manifest hash")
        XCTAssertFalse(published == allHashes,
                       ".failure arm must NOT publish every hash as missing under fail-closed (Home consumers bypass freshness gate)")
    }

    func testSyncOverlayAndCaptureHandle_monthDirNotFound_keepsHandleStaleAndPreservesFallback() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        // Intentionally do NOT create the month directory. `list(monthAbs)`
        // throws not-found, which `probeMonthForMissing` maps to
        // `.inconclusive(.probeFailure)` for every manifest resource.

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        var allHashes: Set<Data> = []
        for index in 0..<3 {
            let bytes = Data("syncoverlay-404-\(index)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            allHashes.insert(hash)
            let name = "f\(index).jpg"
            let resource = RemoteManifestResource(
                year: monthA.year, month: monthA.month,
                physicalRemotePath: "\(monthRel)/\(name)",
                contentHash: hash, fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil, backedUpAtMs: 0
            )
            writer.appendResource(resource)
        }
        let priorMissing = Set([allHashes.first!])
        service.markPhysicallyMissingV2(month: monthA, hashes: priorMissing)

        let handle = try await service.syncOverlayAndCaptureHandle(client: client, basePath: basePath)

        XCTAssertEqual(handle.overlayFreshness, .stale,
                       "month-dir 404 must yield a stale handle — probe-failure inconclusives carry no signal")
        let verified = await service.verifiedPhysicallyMissingHashes(for: monthA)
        XCTAssertNil(verified,
                     "month-dir 404 must NOT publish a fresh overlay for the affected month under fail-closed")
        let published = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertEqual(published, priorMissing,
                       "month-dir 404 must preserve the prior fallback subset; widening to allHashes under fail-closed would publish a transient 404 as verified absence")
        XCTAssertFalse(published == allHashes,
                       "month-dir 404 must NOT publish every manifest hash as physicallyMissing")
    }

    func testSyncOverlayAndCaptureHandle_monthDirNotFound_fallbackCoversAll_keepsHandleStale() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        // Intentionally do NOT create the month directory — list(monthAbs)
        // throws not-found and probeMonthForMissing maps every manifest
        // resource to .inconclusive(.probeFailure).

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        var allHashes: Set<Data> = []
        for index in 0..<3 {
            let bytes = Data("syncoverlay-404-allcovered-\(index)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            allHashes.insert(hash)
            let name = "f\(index).jpg"
            let resource = RemoteManifestResource(
                year: monthA.year, month: monthA.month,
                physicalRemotePath: "\(monthRel)/\(name)",
                contentHash: hash, fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil, backedUpAtMs: 0
            )
            writer.appendResource(resource)
        }
        service.markPhysicallyMissingV2(month: monthA, hashes: allHashes)

        let handle = try await service.syncOverlayAndCaptureHandle(client: client, basePath: basePath)

        XCTAssertEqual(handle.overlayFreshness, .stale,
                       "fallback covering all probe-failure hashes must NOT flip the month to fresh — .probeFailure carries no current signal")
        let verified = await service.verifiedPhysicallyMissingHashes(for: monthA)
        XCTAssertNil(verified,
                     "all-probe-failure inconclusives must keep verifiedPhysicallyMissingHashes nil even when fully covered by fallback")
        let published = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertEqual(published, allHashes,
                       "prior fallback must still be preserved in the missing set so replace-semantics don't drop it")
    }

    func testVerifyMonth_v1Path_refusesWhenVersionJSONExists() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        await client.injectFile(path: versionPath, contents: #"{"format_version":2}"#)

        let service = RemoteIndexSyncService()
        do {
            try await service.verifyMonth(client: client, basePath: basePath, month: monthA)
            XCTFail("expected V2 fence to refuse V1 verifyMonth")
        } catch let err as NSError {
            XCTAssertEqual(err.domain, "RemoteIndexSyncService")
            XCTAssertEqual(err.code, -60)
        }
    }
}

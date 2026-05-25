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
        seedAsset(
            in: service,
            month: month,
            resources: [(role: ResourceTypeCode.photo, slot: 0, hash: contentHash, logicalName: "x.jpg")]
        )
    }

    private func seedAsset(
        in service: RemoteIndexSyncService,
        month: LibraryMonthKey,
        resources: [(role: Int, slot: Int, hash: Data, logicalName: String)],
        assetFingerprint overrideFingerprint: Data? = nil
    ) -> Data {
        let computedFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: resources.map { (role: $0.role, slot: $0.slot, contentHash: $0.hash) }
        )
        let fp = overrideFingerprint ?? computedFingerprint
        let writer = service.makeOptimisticAssetWriter()
        for item in resources {
            let resource = RemoteManifestResource(
                year: month.year,
                month: month.month,
                physicalRemotePath: String(
                    format: "%04d/%02d/%@-%d-%d.dat",
                    month.year,
                    month.month,
                    item.hash.hexString,
                    item.role,
                    item.slot
                ),
                contentHash: item.hash,
                fileSize: 1,
                resourceType: item.role,
                creationDateMs: nil,
                backedUpAtMs: 1
            )
            writer.appendResource(resource)
        }
        let asset = RemoteManifestAsset(
            year: month.year, month: month.month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1,
            resourceCount: resources.count, totalFileSizeBytes: Int64(resources.count)
        )
        let links = resources.map { item in
            RemoteAssetResourceLink(
                year: month.year, month: month.month,
                assetFingerprint: fp, resourceHash: item.hash,
                role: item.role, slot: item.slot, logicalName: item.logicalName
            )
        }
        writer.appendAsset(asset, links: links)
        return fp
    }

    func testOptimisticallyAppendedAssetIsCommittedWithoutClearCall() {
        let service = RemoteIndexSyncService()
        let hash = TestFixtures.fingerprint(0xAB)
        let fp = seedCompleteAsset(in: service, month: monthA, contentHash: hash)
        let byMonth = service.resumeSafeToSkipAssetFingerprintsByMonth()
        XCTAssertEqual(byMonth[monthA], [fp],
                       "append happens after per-asset commit, so no separate commit-clear is needed")
    }

    func testFullSnapshotPresence_authoritativeEmptyMonth_visibleOnSnapshotField() {
        let service = RemoteIndexSyncService()
        // Pre-slice-4 the snapshot's dict-form overlay was built from physicallyMissingByMonth.months
        // and dropped authoritative-empty entries. Slice 4 routes current()/currentSnapshotWithRevision()
        // through fullPresenceSnapshotLocked(), which unions missingMap.keys with the fresh-months set.
        var builder = RemotePresenceSnapshot.Builder()
        builder.set(monthA, missingHashes: [], isAuthoritative: true)
        XCTAssertTrue(service.applyPresenceSnapshotForTest(builder.build()))

        let snapshot = service.fullSnapshot()
        XCTAssertEqual(snapshot.presence.month(monthA).isAuthoritative, true,
                       "fullSnapshot().presence MUST surface authoritative-empty months via fullPresenceSnapshotLocked()")
        XCTAssertTrue(snapshot.presence.month(monthA).missingHashes.isEmpty)
        XCTAssertTrue(snapshot.presence.freshMonths.contains(monthA),
                      "fresh months union into snapshot.presence.freshMonths")
    }

    func testResumeSafeCoverage_excludesPhantomAsset() {
        let service = RemoteIndexSyncService()
        let fp = TestFixtures.fingerprint(0x42)
        let phantom = RemoteManifestAsset(
            year: monthA.year, month: monthA.month, assetFingerprint: fp,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        service.makeOptimisticAssetWriter().appendAsset(phantom, links: nil)
        let byMonth = service.resumeSafeToSkipAssetFingerprintsByMonth()
        XCTAssertNil(byMonth[monthA],
                     "phantom asset (no links) must not be safe to skip")
    }

    func testResumeSafeCoverage_excludesAssetWithMissingResource() {
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
        service.makeOptimisticAssetWriter().appendAsset(asset, links: [link])
        let byMonth = service.resumeSafeToSkipAssetFingerprintsByMonth()
        XCTAssertNil(byMonth[monthA],
                     "asset with missing resource must not be safe to skip")
    }

    func testResumeSafeCoverage_subtractsPhysicallyMissingHash() {
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
        writer.appendAsset(asset, links: [link])

        XCTAssertTrue(service.resumeSafeToSkipAssetFingerprintsByMonth()[monthA]?.contains(fp) ?? false)

        service.markPhysicallyMissingV2(month: monthA, hashes: [hash])
        XCTAssertNil(service.resumeSafeToSkipAssetFingerprintsByMonth()[monthA])

        writer.appendResource(resource)
        XCTAssertTrue(service.resumeSafeToSkipAssetFingerprintsByMonth()[monthA]?.contains(fp) ?? false,
                      "fresh upload must clear the overlay entry")
    }

    func testResumeCoverage_marksSupersetWithStrictSubsetSurvivorHealingRequired() {
        let service = RemoteIndexSyncService()
        let photoHash = TestFixtures.fingerprint(0xA1)
        let videoHash = TestFixtures.fingerprint(0xA2)
        let partialFP = seedAsset(
            in: service,
            month: monthA,
            resources: [(role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg")]
        )
        let fullFP = seedAsset(
            in: service,
            month: monthA,
            resources: [
                (role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg"),
                (role: ResourceTypeCode.pairedVideo, slot: 0, hash: videoHash, logicalName: "photo.mov")
            ]
        )

        let coverage = service.resumeCoverageForCurrentView()

        XCTAssertTrue(coverage.containsSafeToSkip(partialFP, in: monthA))
        XCTAssertFalse(coverage.containsSafeToSkip(fullFP, in: monthA))
        XCTAssertTrue(coverage.healingRequiredAssetFingerprintsByMonth.contains(fullFP, in: monthA))
    }

    func testResumeCoverage_marksSupersetHealingRequiredForMetadataOnlySubsetSurvivor() {
        let service = RemoteIndexSyncService()
        let adjustmentHash = TestFixtures.fingerprint(0xB1)
        let photoHash = TestFixtures.fingerprint(0xB2)
        let metadataFP = seedAsset(
            in: service,
            month: monthA,
            resources: [
                (role: ResourceTypeCode.adjustmentData, slot: 0, hash: adjustmentHash, logicalName: "adjustment.dat")
            ]
        )
        let fullFP = seedAsset(
            in: service,
            month: monthA,
            resources: [
                (role: ResourceTypeCode.adjustmentData, slot: 0, hash: adjustmentHash, logicalName: "adjustment.dat"),
                (role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg")
            ]
        )

        let coverage = service.resumeCoverageForCurrentView()

        XCTAssertFalse(coverage.containsSafeToSkip(metadataFP, in: monthA))
        XCTAssertFalse(coverage.containsSafeToSkip(fullFP, in: monthA))
        XCTAssertTrue(coverage.healingRequiredAssetFingerprintsByMonth.contains(fullFP, in: monthA))
    }

    func testResumeCoverage_marksSupersetHealingRequiredForFingerprintMismatchSubsetSurvivor() {
        let service = RemoteIndexSyncService()
        let photoHash = TestFixtures.fingerprint(0xB3)
        let videoHash = TestFixtures.fingerprint(0xB4)
        let mismatchFP = TestFixtures.fingerprint(0xF7)
        let recomputedPartialFP = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: photoHash)]
        )
        XCTAssertNotEqual(mismatchFP, recomputedPartialFP)
        _ = seedAsset(
            in: service,
            month: monthA,
            resources: [(role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg")],
            assetFingerprint: mismatchFP
        )
        let fullFP = seedAsset(
            in: service,
            month: monthA,
            resources: [
                (role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg"),
                (role: ResourceTypeCode.pairedVideo, slot: 0, hash: videoHash, logicalName: "photo.mov")
            ]
        )

        let coverage = service.resumeCoverageForCurrentView()

        XCTAssertFalse(coverage.containsSafeToSkip(mismatchFP, in: monthA))
        XCTAssertFalse(coverage.containsSafeToSkip(fullFP, in: monthA))
        XCTAssertTrue(coverage.healingRequiredAssetFingerprintsByMonth.contains(fullFP, in: monthA))
    }

    func testResumeCoverage_keepsUnrelatedHealthyAssetSafeToSkip() {
        let service = RemoteIndexSyncService()
        let photoHash = TestFixtures.fingerprint(0xA3)
        let videoHash = TestFixtures.fingerprint(0xA4)
        _ = seedAsset(
            in: service,
            month: monthA,
            resources: [(role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg")]
        )
        _ = seedAsset(
            in: service,
            month: monthA,
            resources: [
                (role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg"),
                (role: ResourceTypeCode.pairedVideo, slot: 0, hash: videoHash, logicalName: "photo.mov")
            ]
        )
        let unrelatedFP = seedAsset(
            in: service,
            month: monthA,
            resources: [(role: ResourceTypeCode.photo, slot: 0, hash: TestFixtures.fingerprint(0xC1), logicalName: "other.jpg")]
        )

        let coverage = service.resumeCoverageForCurrentView()

        XCTAssertTrue(coverage.containsSafeToSkip(unrelatedFP, in: monthA))
        XCTAssertFalse(coverage.healingRequiredAssetFingerprintsByMonth.contains(unrelatedFP, in: monthA))
    }

    func testResumeCoverage_equalLinkSetRemainsSafeToSkip() {
        let service = RemoteIndexSyncService()
        let fp = seedAsset(
            in: service,
            month: monthA,
            resources: [(role: ResourceTypeCode.photo, slot: 0, hash: TestFixtures.fingerprint(0xD1), logicalName: "same.jpg")]
        )

        let coverage = service.resumeCoverageForCurrentView()

        XCTAssertTrue(coverage.containsSafeToSkip(fp, in: monthA))
        XCTAssertFalse(coverage.healingRequiredAssetFingerprintsByMonth.contains(fp, in: monthA))
    }

    func testResumeCoverage_physicallyMissingResourceIsNotSafeOrHealingRequired() {
        let service = RemoteIndexSyncService()
        let photoHash = TestFixtures.fingerprint(0xE1)
        let videoHash = TestFixtures.fingerprint(0xE2)
        _ = seedAsset(
            in: service,
            month: monthA,
            resources: [(role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg")]
        )
        let fullFP = seedAsset(
            in: service,
            month: monthA,
            resources: [
                (role: ResourceTypeCode.photo, slot: 0, hash: photoHash, logicalName: "photo.jpg"),
                (role: ResourceTypeCode.pairedVideo, slot: 0, hash: videoHash, logicalName: "photo.mov")
            ]
        )

        service.markPhysicallyMissingV2(month: monthA, hashes: [videoHash])
        let coverage = service.resumeCoverageForCurrentView()

        XCTAssertFalse(coverage.containsSafeToSkip(fullFP, in: monthA))
        XCTAssertFalse(coverage.healingRequiredAssetFingerprintsByMonth.contains(fullFP, in: monthA))
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)

        let service = RemoteIndexSyncService()
        do {
            _ = try await service.syncIndex(client: client, profile: profile)
            XCTFail("expected remoteFormatUnsupported")
        } catch BackupCompatibilityError.remoteFormatUnsupported(let minApp) {
            XCTAssertEqual(minApp, "9.9.9")
        }
    }

    func testSyncIndex_freshRemoteLeavesRepoFormatUnknown() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)

        let service = RemoteIndexSyncService()
        _ = try await service.syncIndex(client: client, profile: profile)

        let format = await service.currentRepoIsV2()
        XCTAssertNil(format, "fresh route must not mark the remote as V1")
    }

    func testSyncIndex_unchangedV1DigestSecondSyncEmitsNoWorkAndKeepsSnapshot() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await seedV1Manifest(client: client, basePath: basePath, month: monthA, marker: 0x11)
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)
        let service = RemoteIndexSyncService()

        let firstDigest = try await service.syncIndex(client: client, profile: profile)
        let firstSnapshot = service.fullSnapshot()
        let secondProgress = RemoteSyncProgressRecorder()

        let secondDigest = try await service.syncIndex(
            client: client,
            profile: profile,
            onSyncProgress: { progress in
                secondProgress.append(progress)
            }
        )
        let secondSnapshot = service.fullSnapshot()

        XCTAssertEqual(firstDigest.assetCount, secondDigest.assetCount)
        XCTAssertEqual(firstDigest.resourceCount, secondDigest.resourceCount)
        XCTAssertEqual(Set(firstSnapshot.assets), Set(secondSnapshot.assets))
        XCTAssertEqual(Set(firstSnapshot.resources), Set(secondSnapshot.resources))
        let captured = secondProgress.values()
        XCTAssertEqual(captured.map(\.current), [0])
        XCTAssertEqual(captured.map(\.total), [0])
    }

    func testSyncIndex_removedOnlyV1SyncAppliesRemovalProgressAndClearsDigest() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await seedV1Manifest(client: client, basePath: basePath, month: monthA, marker: 0x12)
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)
        let service = RemoteIndexSyncService()
        _ = try await service.syncIndex(client: client, profile: profile)
        XCTAssertEqual(Set(service.fullSnapshot().assets.map { LibraryMonthKey(year: $0.year, month: $0.month) }), [monthA])

        try await client.delete(path: String(format: "%@/%04d/%02d", basePath, monthA.year, monthA.month))
        let removalProgress = RemoteSyncProgressRecorder()
        _ = try await service.syncIndex(
            client: client,
            profile: profile,
            onSyncProgress: { progress in
                removalProgress.append(progress)
            }
        )

        let removedSnapshot = service.fullSnapshot()
        XCTAssertTrue(removedSnapshot.assets.isEmpty)
        XCTAssertTrue(removedSnapshot.resources.isEmpty)
        XCTAssertEqual(removalProgress.values().map(\.current), [0, 1])
        XCTAssertEqual(removalProgress.values().map(\.total), [1, 1])

        let noWorkProgress = RemoteSyncProgressRecorder()
        _ = try await service.syncIndex(
            client: client,
            profile: profile,
            onSyncProgress: { progress in
                noWorkProgress.append(progress)
            }
        )
        XCTAssertEqual(noWorkProgress.values().map(\.current), [0])
        XCTAssertEqual(noWorkProgress.values().map(\.total), [0])
    }

    func testSyncIndex_nonNotFoundYearListErrorDoesNotMutateSnapshotOrAdvanceDigests() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await seedV1Manifest(client: client, basePath: basePath, month: monthA, marker: 0x21)
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)
        let service = RemoteIndexSyncService()
        _ = try await service.syncIndex(client: client, profile: profile)
        let snapshotBeforeFailure = service.fullSnapshot()

        try await seedV1Manifest(client: client, basePath: basePath, month: monthB, marker: 0x22)
        await client.injectListError(.transport, for: "\(basePath)/\(monthA.year)")

        do {
            _ = try await service.syncIndex(client: client, profile: profile)
            XCTFail("expected transport error to propagate")
        } catch {
            XCTAssertFalse(isStorageNotFoundError(error))
        }
        let snapshotAfterFailure = service.fullSnapshot()
        XCTAssertEqual(Set(snapshotBeforeFailure.assets), Set(snapshotAfterFailure.assets))
        XCTAssertEqual(Set(snapshotBeforeFailure.resources), Set(snapshotAfterFailure.resources))

        let recoveryProgress = RemoteSyncProgressRecorder()
        _ = try await service.syncIndex(
            client: client,
            profile: profile,
            onSyncProgress: { progress in
                recoveryProgress.append(progress)
            }
        )

        let recovered = service.fullSnapshot()
        XCTAssertEqual(Set(recovered.assets.map { LibraryMonthKey(year: $0.year, month: $0.month) }), [monthA, monthB])
        XCTAssertEqual(recoveryProgress.values().first?.total, 1)
    }

    func testPhysicalPresenceOverlayProbe_serialOnlyClientRunsOneOperationAtATime() async throws {
        let basePath = "/repo"
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        var resources: [RemoteManifestResource] = []

        for offset in 0..<3 {
            let month = LibraryMonthKey(year: 2025, month: offset + 1)
            let monthRel = String(format: "%04d/%02d", month.year, month.month)
            let bytes = Data("serial-overlay-\(offset)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            let name = "f\(offset).jpg"
            resources.append(RemoteManifestResource(
                year: month.year,
                month: month.month,
                physicalRemotePath: "\(monthRel)/\(name)",
                contentHash: hash,
                fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil,
                backedUpAtMs: 0
            ))
            await inner.injectFile(path: "\(basePath)/\(monthRel)/\(name)", data: bytes)
        }
        let serialOnly = SerialOnlyOverlayProbeClient(inner: inner)

        let result = try await RemoteIndexPhysicalPresenceOverlayProbe().probe(
            snapshot: RemoteLibrarySnapshot(resources: resources, assets: []),
            client: serialOnly,
            basePath: basePath,
            fallback: RemotePresenceSnapshot(),
            budget: nil,
            staleFallbackPolicy: .preserveFallback,
            concurrencyCap: 4
        )

        XCTAssertTrue(result.allMonthsFresh)
        XCTAssertTrue(result.presence.entries.allSatisfy { $0.value.missingHashes.isEmpty })
        let maxOperations = await serialOnly.maxConcurrentOperations()
        let listCount = await serialOnly.listCount()
        let downloadCount = await serialOnly.downloadCount()
        XCTAssertEqual(maxOperations, 1)
        XCTAssertEqual(listCount, 3)
        XCTAssertEqual(downloadCount, 3)
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
        let verified = service.verifiedPhysicallyMissingHashes(for: monthA)
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
        let verified = service.verifiedPhysicallyMissingHashes(for: monthA)
        XCTAssertEqual(verified, monthMissing,
                       "freshness-aware accessor must publish the same missing set under partial fallback")
    }

    func testRefreshPhysicalPresenceOverlay_preserveFallback_healedHashClearedDespiteUnrelatedInconclusives() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        try await client.createDirectory(path: "\(basePath)/\(monthRel)")

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()

        // Two resources. The "healed" one downloads cleanly and the probe verifies it
        // present. The "unrelated" one is listed at the expected size, but downloads
        // fail with notFound — verifyHashResult returns .inconclusive, so the probe
        // emits .inconclusive(.probeFailure) for that hash. This is deterministic;
        // avoids depending on the iteration order of resourcesByHash inside the probe.
        let healedBytes = Data("overlay-heal-healed".utf8)
        let healed = Data(SHA256.hash(data: healedBytes))
        let healedName = "healed.jpg"
        let healedResource = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/\(healedName)",
            contentHash: healed, fileSize: Int64(healedBytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        writer.appendResource(healedResource)
        await client.injectFile(path: "\(basePath)/\(monthRel)/\(healedName)", data: healedBytes)

        let unrelatedBytes = Data("overlay-heal-unrelated".utf8)
        let unrelatedHash = Data(SHA256.hash(data: unrelatedBytes))
        let unrelatedName = "unrelated.jpg"
        let unrelatedResource = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/\(unrelatedName)",
            contentHash: unrelatedHash, fileSize: Int64(unrelatedBytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        writer.appendResource(unrelatedResource)
        await client.injectFile(path: "\(basePath)/\(monthRel)/\(unrelatedName)", data: unrelatedBytes)
        await client.injectPersistentDownloadError(.notFound, for: "\(basePath)/\(monthRel)/\(unrelatedName)")

        // Prior overlay flagged ONLY the healed hash as missing.
        service.markPhysicallyMissingV2(month: monthA, hashes: [healed])

        _ = try await service.refreshPhysicalPresenceOverlay(
            client: client,
            basePath: basePath,
            fallback: RemotePresenceSnapshot.failClosed(missingByMonth: [monthA: [healed]])
        )

        let published = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertFalse(published.contains(healed),
                       "probe verified the prior-missing hash is present — overlay must drop it even though an unrelated hash stays inconclusive under preserveFallback")
        XCTAssertFalse(published.contains(unrelatedHash),
                       "unrelated inconclusive hash was not in priorFallback, so it must not be promoted into the overlay")
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
            fallback: RemotePresenceSnapshot.failClosed(missingByMonth: [monthA: allHashes])
        )
        let verified = service.verifiedPhysicallyMissingHashes(for: monthA)
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
            fallback: RemotePresenceSnapshot.failClosed(missingByMonth: [monthA: priorMissing])
        )

        let published = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertEqual(published, priorMissing,
                       "month-dir 404 must preserve prior fallback; widening to allHashes would publish a transient 404 as verified absence")
        XCTAssertFalse(published == allHashes,
                       "month-dir 404 must NOT publish every manifest hash as physicallyMissing")
        let verified = service.verifiedPhysicallyMissingHashes(for: monthA)
        XCTAssertNil(verified,
                     "month-dir 404 must leave the month not-fresh so callers don't read the fallback as authoritative")
    }

    func testRefreshPhysicalPresenceOverlay_monthDirNotFound_noFallback_preservesPriorState() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)

        let service = RemoteIndexSyncService()
        let writer = service.makeOptimisticAssetWriter()
        var allHashes: Set<Data> = []
        for index in 0..<3 {
            let bytes = Data("month-dir-404-nofallback-\(index)".utf8)
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

        // No fallback passed — the probe sees not-found and has nothing to
        // cover the inconclusive hashes. The overlay must NOT clear the
        // prior missing state by applying an empty missing set.
        _ = try await service.refreshPhysicalPresenceOverlay(
            client: client,
            basePath: basePath
        )

        let published = service.physicallyMissingHashesForTest(month: monthA)
        XCTAssertEqual(published, priorMissing,
                       "month-dir 404 with no fallback must preserve prior missing state, not clear it")
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
        let verified = service.verifiedPhysicallyMissingHashes(for: monthA)
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
        let verified = service.verifiedPhysicallyMissingHashes(for: monthA)
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

    func testVerifyMonth_v1Path_refusesWhenVersionJSONIsDirectory() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        try await client.createDirectory(path: versionPath)

        let service = RemoteIndexSyncService()
        do {
            try await service.verifyMonth(client: client, basePath: basePath, month: monthA)
            XCTFail("expected directory version fence to refuse V1 verifyMonth")
        } catch let err as NSError {
            XCTAssertEqual(err.domain, "RemoteIndexSyncService")
            XCTAssertEqual(err.code, -62)
        }
    }

    func testVerifyMonth_v1Path_refusesWhenV2DataExistsWithoutVersionJSON() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commitsPath = RepoLayout.commitsDirectoryPath(base: basePath)
        try await client.createDirectory(path: commitsPath)
        await client.injectFile(path: "\(commitsPath)/leftover.jsonl", contents: "stale")

        let service = RemoteIndexSyncService()
        do {
            try await service.verifyMonth(client: client, basePath: basePath, month: monthA)
            XCTFail("expected V2 data fence to refuse V1 verifyMonth")
        } catch let err as NSError {
            XCTAssertEqual(err.domain, "RemoteIndexSyncService")
            XCTAssertEqual(err.code, -61)
        }
    }

    func testVerifyMonth_v1Path_v2DataProbeTransportErrorPropagatesWithoutMutation() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let service = RemoteIndexSyncService()
        let hash = TestFixtures.fingerprint(0x61)
        _ = seedCompleteAsset(in: service, month: monthA, contentHash: hash)
        let snapshotBeforeFailure = service.fullSnapshot()
        await client.injectListError(.transport, for: RepoLayout.commitsDirectoryPath(base: basePath))

        do {
            try await service.verifyMonth(client: client, basePath: basePath, month: monthA)
            XCTFail("expected V2-data probe transport error to propagate")
        } catch {
            XCTAssertFalse(isStorageNotFoundError(error))
        }
        let snapshotAfterFailure = service.fullSnapshot()
        XCTAssertEqual(Set(snapshotBeforeFailure.assets), Set(snapshotAfterFailure.assets))
        XCTAssertEqual(Set(snapshotBeforeFailure.resources), Set(snapshotAfterFailure.resources))
    }

    func testPhysicalPresenceOverlayProbe_monthListURLCancellationPropagates() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let monthRel = String(format: "%04d/%02d", monthA.year, monthA.month)
        try await client.createDirectory(path: "\(basePath)/\(monthRel)")
        let bytes = Data("cancel-overlay-list".utf8)
        let hash = Data(SHA256.hash(data: bytes))
        let resource = RemoteManifestResource(
            year: monthA.year, month: monthA.month,
            physicalRemotePath: "\(monthRel)/x.jpg",
            contentHash: hash, fileSize: Int64(bytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil, backedUpAtMs: 0
        )
        await client.injectListWrappedURLCancellation(for: "\(basePath)/\(monthRel)")
        do {
            _ = try await RemoteIndexPhysicalPresenceOverlayProbe().probe(
                snapshot: RemoteLibrarySnapshot(resources: [resource], assets: []),
                client: client,
                basePath: basePath,
                fallback: RemotePresenceSnapshot(),
                budget: nil,
                staleFallbackPolicy: .preserveFallback,
                concurrencyCap: 4
            )
            XCTFail("expected cancellation from month list")
        } catch is CancellationError {}
    }

    // MARK: - localRepoID route guard

    func testSyncIndex_localRepoID_freshRemote_throwsDamagedV2Repo() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await client.createDirectory(path: basePath)
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)

        let service = RemoteIndexSyncService()
        do {
            _ = try await service.syncIndex(
                client: client,
                profile: profile,
                localRepoID: "bound-repo-id"
            )
            XCTFail("expected damagedV2Repo — local V2 binding must reject fresh remote")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testSyncIndex_localRepoID_v1Remote_throwsRequiresForegroundMigration() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await seedV1Manifest(client: client, basePath: basePath, month: monthA, marker: 0x31)
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)

        let service = RemoteIndexSyncService()
        do {
            _ = try await service.syncIndex(
                client: client,
                profile: profile,
                localRepoID: "bound-repo-id"
            )
            XCTFail("expected requiresForegroundMigration — local V2 binding must reject V1 remote")
        } catch BackupCompatibilityError.requiresForegroundMigration {
            // expected
        }
    }

    func testSyncIndex_nilLocalRepoID_v2Repo_storesMaterializedRepoID() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "w")
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)

        let service = RemoteIndexSyncService()
        _ = try await service.syncIndex(client: client, profile: profile, localRepoID: nil)

        let cachedID = await service.materializedRepoID()
        XCTAssertEqual(cachedID, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    }

    func testMaterializedRepoID_clearedOnProfileSwitch() async throws {
        let basePath = "/repo"
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.createDirectory(path: basePath)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: "w")
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        let profile = TestFixtures.makeServerProfile(id: 1, storageType: .webdav, basePath: basePath)

        let service = RemoteIndexSyncService()
        _ = try await service.syncIndex(client: client, profile: profile, localRepoID: nil)
        let storedID = await service.materializedRepoID()
        XCTAssertEqual(storedID, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

        await service.resetForProfileSwitch()
        let clearedID = await service.materializedRepoID()
        XCTAssertNil(clearedID)
    }

    private func seedV1Manifest(
        client: InMemoryRemoteStorageClient,
        basePath: String,
        month: LibraryMonthKey,
        marker: UInt8
    ) async throws {
        let bytes = Data(repeating: marker, count: 8)
        let hash = TestFixtures.fingerprint(marker)
        let fingerprint = TestFixtures.computedFingerprint(for: [
            (role: ResourceTypeCode.photo, slot: 0, contentHash: hash)
        ])
        let resource = TestFixtures.remoteResource(
            year: month.year,
            month: month.month,
            contentHash: hash,
            fileSize: Int64(bytes.count),
            fileName: "asset-\(marker).jpg"
        )
        let asset = TestFixtures.remoteAsset(
            year: month.year,
            month: month.month,
            fingerprint: fingerprint,
            totalFileSizeBytes: Int64(bytes.count)
        )
        let link = TestFixtures.remoteLink(
            year: month.year,
            month: month.month,
            assetFingerprint: fingerprint,
            resourceHash: hash,
            logicalName: "asset-\(marker).jpg"
        )
        await client.injectFile(path: "\(basePath)/\(resource.physicalRemotePath)", data: bytes)
        let store = try await MonthManifestStore.loadSeeded(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            seed: MonthManifestStore.Seed(resources: [resource], assets: [asset], assetResourceLinks: [link])
        )
        let manifestPath = RemotePathBuilder.absolutePath(
            basePath: basePath,
            remoteRelativePath: String(format: "%04d/%02d/%@", month.year, month.month, MonthManifestStore.manifestFileName)
        )
        try await client.upload(
            localURL: store.localManifestURL,
            remotePath: manifestPath,
            respectTaskCancellation: true,
            onProgress: nil
        )
        await client.setModificationDateForTest(Date(timeIntervalSince1970: TimeInterval(marker)), path: manifestPath)
    }
}

private final class RemoteSyncProgressRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [RemoteSyncProgress] = []

    func append(_ value: RemoteSyncProgress) {
        lock.withLock {
            storage.append(value)
        }
    }

    func values() -> [RemoteSyncProgress] {
        lock.withLock { storage }
    }
}

private actor SerialOnlyOverlayProbeClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .serialOnly }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize _: Int64, remotePath _: String) -> CreateGuarantee {
        .exclusive
    }

    private let inner: InMemoryRemoteStorageClient
    private var activeOperations = 0
    private var maxOperations = 0
    private var totalLists = 0
    private var totalDownloads = 0

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    func maxConcurrentOperations() -> Int { maxOperations }
    func listCount() -> Int { totalLists }
    func downloadCount() -> Int { totalDownloads }

    func connect() async throws {
        try await inner.connect()
    }

    func disconnect() async {
        await inner.disconnect()
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        try await inner.storageCapacity()
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        recordOperationStart()
        totalLists += 1
        defer { recordOperationEnd() }
        try await Task.sleep(nanoseconds: 5_000_000)
        return try await inner.list(path: path)
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        try await inner.metadata(path: path)
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try await inner.upload(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }

    func download(remotePath: String, localURL: URL) async throws {
        recordOperationStart()
        totalDownloads += 1
        defer { recordOperationEnd() }
        try await Task.sleep(nanoseconds: 5_000_000)
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }

    func exists(path: String) async throws -> Bool {
        try await inner.exists(path: path)
    }

    func delete(path: String) async throws {
        try await inner.delete(path: path)
    }

    func createDirectory(path: String) async throws {
        try await inner.createDirectory(path: path)
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }

    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private func recordOperationStart() {
        activeOperations += 1
        maxOperations = max(maxOperations, activeOperations)
    }

    private func recordOperationEnd() {
        activeOperations -= 1
    }
}

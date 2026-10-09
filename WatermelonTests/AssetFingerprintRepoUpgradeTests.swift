import GRDB
import UIKit
import XCTest
@testable import Watermelon

final class AssetFingerprintRepoUpgradeTests: XCTestCase {
    private let base = "/photos"
    private var versionPath: String { RepoLayoutLite.versionPath(basePath: base) }
    private func monthPath(_ month: Int) -> String { RepoLayoutLite.monthPath(basePath: base, month: .init(year: 2026, month: month)) }

    private func seed(_ client: InMemoryRemoteStorageClient, month: Int = 1, duplicates: Bool = false) async throws -> Data {
        let fixture = try makeMonth(month, duplicates: duplicates)
        await client.seedFile(path: monthPath(month), data: fixture.bytes)
        for (name, data) in fixture.files {
            await client.seedFile(path: String(format: "%@/2026/%02d/%@", base, month, name), data: data)
        }
        await client.seedFile(path: versionPath, data: try VersionManifestLite.encode(.init(
            formatVersion: 2, minAppVersion: "1.5.0", createdAt: "original-date", createdBy: "original-writer")))
        return fixture.fingerprint
    }

    private func makeMonth(_ month: Int, duplicates: Bool) throws -> (bytes: Data, fingerprint: Data, files: [String: Data], legacyFingerprints: [Data]) {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let queue = try MonthManifestStore.makeManifestQueue(path: url.path)
        try MonthManifestStore.migrate(queue)
        let client = InMemoryRemoteStorageClient()
        let store = MonthManifestStore(client: client, basePath: base, year: 2026, month: month,
            localManifestURL: url, dbQueue: queue, remoteFilesByName: [:], dirty: false, layout: .v1)
        var files: [String: Data] = [:]
        var expected = Data()
        var legacyFingerprints: [Data] = []
        for copy in 0..<(duplicates ? 2 : 1) {
            let adjustment = ContentIdentityFixtures.adjustment(timestamp: Double(copy + 1))
            let inputs: [(Int, String, Data)] = [(1, "original.jpg", Data("original".utf8)),
                (5, "edited.jpg", Data("edited".utf8)), (7, "edit-\(copy).AAE", adjustment)]
            let resources = inputs.map { AssetContentFingerprint.Resource(role: $0.0, slot: 0, hash: ContentIdentityFixtures.hash($0.2)) }
            let raw = BackupAssetResourcePlanner.legacyAssetFingerprint(resourceRoleSlotHashes: resources.map { ($0.role, $0.slot, $0.hash) })
            legacyFingerprints.append(raw)
            expected = AssetContentFingerprint.fingerprint(resources: resources)
            for (role, name, data) in inputs {
                files[name] = data
                _ = try store.upsertResource(.init(year: 2026, month: month, fileName: name,
                    contentHash: ContentIdentityFixtures.hash(data), fileSize: Int64(data.count), resourceType: role,
                    creationDateMs: 100, backedUpAtMs: 200))
            }
            try store.upsertAsset(.init(year: 2026, month: month, assetFingerprint: raw, creationDateMs: 100,
                backedUpAtMs: Int64(200 + copy), resourceCount: 3, totalFileSizeBytes: inputs.reduce(0) { $0 + Int64($1.2.count) }),
                links: resources.map { .init(year: 2026, month: month, assetFingerprint: raw, resourceHash: $0.hash, role: $0.role, slot: 0) })
        }
        return (try Data(contentsOf: url), expected, files, legacyFingerprints)
    }

    private func thumbnail(_ color: UIColor = .red) -> Data {
        UIGraphicsImageRenderer(size: CGSize(width: 4, height: 4)).jpegData(withCompressionQuality: 0.8) { context in
            color.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 4, height: 4))
        }
    }

    private func seedLegacyThumbnail(_ client: InMemoryRemoteStorageClient, copy: Int = 0) async throws -> (path: String, data: Data) {
        let legacy = try makeMonth(1, duplicates: true).legacyFingerprints[copy]
        let path = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: legacy.hexString)
        let data = thumbnail()
        await client.seedDirectory(RemoteThumbnailPaths.rootAbsolutePath(basePath: base))
        await client.seedFile(path: path, data: data)
        return (path, data)
    }

    private func upgrade(_ client: InMemoryRemoteStorageClient, ownership: RepoOwnershipGates = .uniform({})) async throws {
        try await AssetFingerprintRepoUpgrade(client: client, basePath: base, assertOwnership: ownership,
            monthsListing: LiteMonthsListingSnapshot(), onProgress: nil).run(createdAt: "date", createdBy: "writer")
    }

    private func snapshot(_ client: InMemoryRemoteStorageClient, month: Int = 1) async throws -> MonthManifestStore {
        let result = try await MonthManifestStore.loadManifestDirect(client: client, basePath: base, year: 2026,
            month: month, layout: .lite, pushSchemaUpgrade: false, surfaceDownloadFailure: true)
        return try XCTUnwrap(result)
    }

    func testLegacyUpgradeChangesOnlyAssetIdentityAndKeepsRawIntegrityAndMediaFiles() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        let originalBytes = await client.fileData(path: base + "/2026/01/edit-0.AAE")
        let decision = try await RepoFormatRouter(client: client, basePath: base).classify()
        XCTAssertEqual(decision, .fingerprintUpgrade)
        try await upgrade(client)
        let store = try await snapshot(client)
        XCTAssertEqual(Set(store.assetsByFingerprint.keys), [expected])
        let links = try XCTUnwrap(store.assetLinksByFingerprint[expected])
        XCTAssertFalse(MonthManifestStore.isAssetIncomplete(links: links, isResourceAvailable: { store.itemsByHash[$0] != nil }, assetFingerprint: expected))
        XCTAssertEqual(links.first { $0.role == 7 }?.resourceHash, originalBytes.map(ContentIdentityFixtures.hash))
        XCTAssertTrue(MonthManifestStore.isAssetIncomplete(links: links.filter { $0.role != 5 }, isResourceAvailable: { _ in true }, assetFingerprint: expected))
        let bytes = await client.fileData(path: versionPath)
        let manifest = try VersionManifestLite.decode(XCTUnwrap(bytes))
        XCTAssertEqual(manifest.formatVersion, 3)
        XCTAssertEqual(manifest.minAppVersion, "1.11.0")
        XCTAssertNil(manifest.upgradePending)
        XCTAssertNotEqual(manifest.formatVersion, 2, "Released 1.10 clients reject any format other than 2")
        let downloads = await client.downloadAttemptPaths
        XCTAssertFalse(downloads.contains { $0.hasSuffix(".AAE") })
        XCTAssertFalse(downloads.contains { $0.hasSuffix(".jpg") })
        let uploads = await client.uploadedPaths
        XCTAssertTrue(uploads.allSatisfy { $0.hasPrefix(base + "/.watermelon/") })
        let raw = await client.fileData(path: base + "/2026/01/edit-0.AAE")
        XCTAssertEqual(raw, originalBytes)
    }

    func testEquivalentLegacyAssetsCollapseWhileResourceRowsRemain() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client, duplicates: true)
        try await upgrade(client)
        let store = try await snapshot(client)
        XCTAssertEqual(store.assetsByFingerprint.count, 1)
        XCTAssertEqual(store.assetsByFingerprint[expected]?.backedUpAtMs, 201)
        XCTAssertEqual(store.itemsByFileName.count, 4)
        XCTAssertEqual(store.assetLinksByFingerprint[expected]?.count, 3)
    }

    func testFormat2WithAppOwnedThumbnailsUpgradesThroughGateway() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        let thumbnailPath = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: expected.hexString)
        let legacy = try await seedLegacyThumbnail(client)
        XCTAssertNotEqual(legacy.path, thumbnailPath)
        let decision = try await RepoFormatRouter(client: client, basePath: base).classify()
        XCTAssertEqual(decision, .fingerprintUpgrade)
        let prepared = try await RemoteLiteRepoGateway.prepareForegroundWrite(
            client: client, lockClient: client, basePath: base, writerID: UUID().uuidString.lowercased()
        )
        await prepared.session.stopAndRelease()
        let store = try await snapshot(client)
        XCTAssertNotNil(store.assetsByFingerprint[expected])
        let thumbnail = await client.fileData(path: thumbnailPath)
        XCTAssertEqual(thumbnail, legacy.data)
        let original = await client.fileData(path: legacy.path)
        XCTAssertEqual(original, legacy.data)
        let preview = try await RemoteThumbnailService.readSidecar(remotePath: thumbnailPath, client: client)
        XCTAssertEqual(preview.data, legacy.data)
    }

    func testUpgradeWithoutLegacyThumbnailDoesNotCreateOne() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        await client.seedDirectory(RemoteThumbnailPaths.rootAbsolutePath(basePath: base))
        try await upgrade(client)
        let path = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: expected.hexString)
        let data = await client.fileData(path: path)
        XCTAssertNil(data)
        let uploads = await client.uploadedPaths
        XCTAssertFalse(uploads.contains { $0.hasSuffix(".jpg") })
    }

    func testThumbnailMigrationListsImplicitDirectoriesWithoutDirectoryObjects() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        let legacyFingerprint = try makeMonth(1, duplicates: false).legacyFingerprints[0]
        let oldPath = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: legacyFingerprint.hexString)
        let data = thumbnail()
        await client.seedFile(path: oldPath, data: data)
        let hasRootObject = try await client.exists(path: RemoteThumbnailPaths.rootAbsolutePath(basePath: base))
        XCTAssertFalse(hasRootObject)

        try await upgrade(client)

        let newPath = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: expected.hexString)
        let migrated = await client.fileData(path: newPath)
        XCTAssertEqual(migrated, data)
        let original = await client.fileData(path: oldPath)
        XCTAssertEqual(original, data)
    }

    func testUpgradePreservesExistingNewThumbnail() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        let legacy = try await seedLegacyThumbnail(client)
        let path = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: expected.hexString)
        let existing = thumbnail(.blue)
        await client.seedFile(path: path, data: existing)
        try await upgrade(client)
        let data = await client.fileData(path: path)
        XCTAssertEqual(data, existing)
        let oldData = await client.fileData(path: legacy.path)
        XCTAssertEqual(oldData, legacy.data)
        let uploads = await client.uploadedPaths
        XCTAssertFalse(uploads.contains(path))
    }

    func testMergedAssetsCanUseThumbnailFromDiscardedLegacyFingerprint() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client, duplicates: true)
        let legacy = try await seedLegacyThumbnail(client, copy: 0)
        try await upgrade(client)
        let path = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: expected.hexString)
        let data = await client.fileData(path: path)
        XCTAssertEqual(data, legacy.data)
        let store = try await snapshot(client)
        XCTAssertEqual(store.assetsByFingerprint[expected]?.backedUpAtMs, 201)
    }

    func testThumbnailMigrationUsesIndependentUploadsOnAliasingBackend() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let expected = try await seed(client)
        let legacy = try await seedLegacyThumbnail(client)
        try await upgrade(client)
        let path = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: expected.hexString)
        let preview = try await RemoteThumbnailService.readSidecar(remotePath: path, client: client)
        XCTAssertEqual(preview.data, legacy.data)
        try await client.delete(path: legacy.path)
        let newData = await client.fileData(path: path)
        XCTAssertEqual(newData, legacy.data)
        let copies = await client.copiedPaths
        XCTAssertFalse(copies.contains { $0.to == path })
    }

    func testFailedThumbnailUploadPreservesMonthAndRetryRepairsPartialDestination() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        let legacy = try await seedLegacyThumbnail(client)
        let path = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: expected.hexString)
        let originalMonth = await client.fileData(path: monthPath(1))
        await client.failUploadWritingCorruptBytes(Data("partial".utf8), forPathSuffix: path, error: RemoteErrorFixtures.retryable)
        do { try await upgrade(client); XCTFail("Thumbnail failure must leave the old month recoverable") } catch { }
        let unchangedMonth = await client.fileData(path: monthPath(1))
        XCTAssertEqual(unchangedMonth, originalMonth)
        let marker = await client.fileData(path: versionPath)
        XCTAssertEqual(try VersionManifestLite.decode(XCTUnwrap(marker)).upgradePending, true)
        let oldData = await client.fileData(path: legacy.path)
        XCTAssertEqual(oldData, legacy.data)

        try await upgrade(client)
        let preview = try await RemoteThumbnailService.readSidecar(remotePath: path, client: client)
        XCTAssertEqual(preview.data, legacy.data)
        let store = try await snapshot(client)
        XCTAssertNotNil(store.assetsByFingerprint[expected])
    }

    func testOwnershipLostWhileReadingThumbnailPreventsPublishingMonthAndThumbnail() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        let legacy = try await seedLegacyThumbnail(client)
        let originalMonth = await client.fileData(path: monthPath(1))
        let ownership = RepoOwnershipGates.uniform {
            if await client.downloadAttemptPaths.contains(legacy.path) { throw CancellationError() }
        }
        do { try await upgrade(client, ownership: ownership); XCTFail("Lost ownership must stop thumbnail migration") } catch { }
        let unchangedMonth = await client.fileData(path: monthPath(1))
        XCTAssertEqual(unchangedMonth, originalMonth)
        let path = RemoteThumbnailPaths.absolutePath(basePath: base, fingerprintHex: expected.hexString)
        let data = await client.fileData(path: path)
        XCTAssertNil(data)
        let oldData = await client.fileData(path: legacy.path)
        XCTAssertEqual(oldData, legacy.data)
    }

    func testFormat2StillRejectsForeignDirectoryAndThumbnailFile() async throws {
        for name in ["foreign", RemoteThumbnailPaths.repoChildDirectoryName] {
            let client = InMemoryRemoteStorageClient()
            _ = try await seed(client)
            let path = RepoLayoutLite.repoDirectoryPath(basePath: base) + "/" + name
            if name == "foreign" { await client.seedDirectory(path) }
            else { await client.seedFile(path: path) }
            let decision = try await RepoFormatRouter(client: client, basePath: base).classify()
            XCTAssertEqual(decision, .damaged)
        }
    }

    func testUpgradeUsesOnlyManifestHashesAndPreservesEveryResourceRow() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        let resourcePrefix = base + "/2026/01/"
        await client.setOnDownloadAttempt { path in
            if path.hasPrefix(resourcePrefix) { XCTFail("Fingerprint upgrades must never download resource files") }
        }
        try await upgrade(client)
        let store = try await snapshot(client)
        XCTAssertEqual(Set(store.assetsByFingerprint.keys), [expected])
        XCTAssertEqual(Set(store.itemsByFileName.keys), ["original.jpg", "edited.jpg", "edit-0.AAE"])
        XCTAssertEqual(Set(store.assetLinksByFingerprint[expected]?.map(\.role) ?? []), [1, 5, 7])
        try await store.dbQueue.read { db in
            XCTAssertFalse(try db.columns(in: "asset_resources").contains { $0.name == "fingerprintHash" })
            XCTAssertEqual(try Int.fetchOne(db, sql: "PRAGMA user_version"), AssetContentFingerprint.version)
        }
    }

    func testManifestDownloadFaultsDoNotRemoveResourcesOrFinishUpgrade() async throws {
        for error in [RemoteErrorFixtures.retryable, RemoteErrorFixtures.terminal, RemoteErrorFixtures.cancelled] {
            let client = InMemoryRemoteStorageClient()
            _ = try await seed(client)
            let path = monthPath(1)
            let originalManifest = await client.fileData(path: monthPath(1))
            await client.setOnDownloadAttempt { attempted in
                if attempted == path { await client.enqueueDownloadError(error) }
            }
            do { try await upgrade(client); XCTFail("Download faults must block the upgrade") } catch { }
            let bytes = await client.fileData(path: versionPath)
            XCTAssertEqual(try VersionManifestLite.decode(XCTUnwrap(bytes)).upgradePending, true)
            let manifestAfter = await client.fileData(path: monthPath(1))
            XCTAssertEqual(manifestAfter, originalManifest)
            let adjustmentAfter = await client.fileData(path: base + "/2026/01/edit-0.AAE")
            XCTAssertNotNil(adjustmentAfter)
        }
    }

    func testFailureKeepsPendingBoundaryAndReconnectionReusesConvertedMonth() async throws {
        let client = InMemoryRemoteStorageClient()
        let first = try await seed(client)
        let second = try await seed(client, month: 2)
        let corruptPath = monthPath(2)
        let originalBytes = await client.fileData(path: corruptPath)
        await client.seedFile(path: corruptPath, data: Data("corrupted".utf8))
        do { try await upgrade(client); XCTFail("Corrupt manifest must prevent final commit") } catch { }
        let decision = try await RepoFormatRouter(client: client, basePath: base).classify()
        XCTAssertEqual(decision, .fingerprintUpgrade)
        let pendingData = await client.fileData(path: versionPath)
        let pending = try VersionManifestLite.decode(XCTUnwrap(pendingData))
        XCTAssertEqual(pending.formatVersion, 3)
        XCTAssertEqual(pending.upgradePending, true)
        let uploadsBefore = await client.uploadedPaths.count
        await client.seedFile(path: corruptPath, data: try XCTUnwrap(originalBytes))
        try await upgrade(client)
        let uploads = await client.uploadedPaths
        XCTAssertFalse(uploads.dropFirst(uploadsBefore).contains { $0.contains("2026-01") })
        let month1 = try await snapshot(client)
        let month2 = try await snapshot(client, month: 2)
        XCTAssertNotNil(month1.assetsByFingerprint[first])
        XCTAssertNotNil(month2.assetsByFingerprint[second])
    }

    func testPendingBarrierIsPublishedBeforeAnyMonthReplacement() async throws {
        let client = InMemoryRemoteStorageClient()
        _ = try await seed(client)
        let versionPath = versionPath
        await client.setOnMove { _, target in
            guard target.hasSuffix("2026-01.sqlite") else { return }
            let data = await client.fileData(path: versionPath)
            let manifest = try? VersionManifestLite.decode(data ?? Data())
            XCTAssertEqual(manifest?.formatVersion, 3)
            XCTAssertEqual(manifest?.upgradePending, true)
        }
        try await upgrade(client)
    }

    func testDirectPutBackendKeepsCanonicalVersionAndMediaFingerprint() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let expected = try await seed(client)
        try await upgrade(client)
        let decision = try await RepoFormatRouter(client: client, basePath: base).classify()
        XCTAssertEqual(decision, .current)
        let store = try await snapshot(client)
        XCTAssertNotNil(store.assetsByFingerprint[expected])
        let moves = await client.movedPaths
        XCTAssertTrue(moves.isEmpty)
    }

    func testLostOwnershipMakesNoWrites() async throws {
        let client = InMemoryRemoteStorageClient()
        _ = try await seed(client)
        do { try await upgrade(client, ownership: .uniform({ throw CancellationError() })); XCTFail() } catch { }
        let uploads = await client.uploadedPaths
        XCTAssertTrue(uploads.isEmpty)
    }

    func testGatewayUpgradesUnderExistingLeaseAndSecondConnectionDoesNotRewriteMonths() async throws {
        let client = InMemoryRemoteStorageClient()
        let expected = try await seed(client)
        let writerID = UUID().uuidString.lowercased()
        let first = try await RemoteLiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: base, writerID: writerID)
        await first.session.stopAndRelease()
        let month = try await snapshot(client)
        XCTAssertNotNil(month.assetsByFingerprint[expected])
        let before = await client.uploadedPaths.count
        let second = try await RemoteLiteRepoGateway.prepareForegroundWrite(client: client, lockClient: client, basePath: base, writerID: writerID)
        await second.session.stopAndRelease()
        let uploads = await client.uploadedPaths
        XCTAssertFalse(uploads.dropFirst(before).contains { $0.contains("/months/") })
    }

    private func interruptedOneDriveUpgrade() async throws -> (
        backing: InMemoryRemoteStorageClient, client: InterruptedOneDriveUpgradeClient,
        fingerprint: Data, writerID: String, scratch: [String: Data]
    ) {
        let backing = InMemoryRemoteStorageClient()
        let fingerprint = try await seed(backing)
        let client = InterruptedOneDriveUpgradeClient(backing: backing)
        let writerID = UUID().uuidString.lowercased()
        do {
            let prepared = try await RemoteLiteRepoGateway.prepareForegroundWrite(
                client: client, lockClient: backing, basePath: base, writerID: writerID)
            await prepared.session.stopAndRelease()
            XCTFail("The first publish must stop after moving the canonical to backup")
        } catch is CancellationError { }
        let canonical = await backing.fileData(path: monthPath(1))
        XCTAssertNil(canonical)
        let entries = try await backing.list(path: RepoLayoutLite.monthsDirectoryPath(basePath: base))
        var scratch: [String: Data] = [:]
        for entry in entries {
            let data = await backing.fileData(path: entry.path)
            scratch[entry.path] = try XCTUnwrap(data)
        }
        XCTAssertEqual(scratch.keys.filter { $0.hasSuffix(".bak") }.count, 1)
        XCTAssertEqual(scratch.keys.filter { $0.hasSuffix(".tmp") }.count, 1)
        return (backing, client, fingerprint, writerID, scratch)
    }

    func testInterruptedOneDriveUpgradeRecoversMonthAndPreservesResourceBytes() async throws {
        let state = try await interruptedOneDriveUpgrade()
        var originalFiles: [String: Data] = [:]
        for name in ["original.jpg", "edited.jpg", "edit-0.AAE"] {
            let data = await state.backing.fileData(path: base + "/2026/01/" + name)
            originalFiles[name] = try XCTUnwrap(data)
        }
        let prepared = try await RemoteLiteRepoGateway.prepareForegroundWrite(
            client: state.client, lockClient: state.backing, basePath: base, writerID: state.writerID)
        await prepared.session.stopAndRelease()
        let store = try await snapshot(state.backing)
        XCTAssertEqual(Set(store.assetsByFingerprint.keys), [state.fingerprint])
        XCTAssertEqual(store.assetLinksByFingerprint[state.fingerprint]?.count, 3)
        XCTAssertEqual(store.itemsByFileName.count, 3)
        XCTAssertFalse(store.isAssetIncomplete(state.fingerprint))
        for (name, original) in originalFiles {
            let after = await state.backing.fileData(path: base + "/2026/01/" + name)
            XCTAssertEqual(after, original)
        }
        let decision = try await RepoFormatRouter(client: state.client, basePath: base).classify()
        XCTAssertEqual(decision, .current)
        let copies = await state.backing.copiedPaths
        XCTAssertTrue(copies.contains { $0.to == monthPath(1) })
    }

    func testOneDriveUpgradeKeepsRecoveryFilesWhenTheirDownloadFails() async throws {
        let state = try await interruptedOneDriveUpgrade()
        await state.client.setRecoveryDownloadError(RemoteErrorFixtures.retryable)
        do {
            let prepared = try await RemoteLiteRepoGateway.prepareForegroundWrite(
                client: state.client, lockClient: state.backing, basePath: base, writerID: state.writerID)
            await prepared.session.stopAndRelease()
            XCTFail("Unverified recovery files must block the completed upgrade")
        } catch { }
        for (path, expected) in state.scratch {
            let actual = await state.backing.fileData(path: path)
            XCTAssertEqual(actual, expected)
        }
        let canonical = await state.backing.fileData(path: monthPath(1))
        XCTAssertNil(canonical)
        let pending = await state.backing.fileData(path: versionPath)
        XCTAssertEqual(try VersionManifestLite.decode(XCTUnwrap(pending)).upgradePending, true)
        await state.client.setRecoveryDownloadError(nil)
        let retry = try await RemoteLiteRepoGateway.prepareForegroundWrite(
            client: state.client, lockClient: state.backing, basePath: base, writerID: state.writerID)
        await retry.session.stopAndRelease()
        let store = try await snapshot(state.backing)
        XCTAssertNotNil(store.assetsByFingerprint[state.fingerprint])
    }

    func testOneDriveUpgradeStopsWhenRecoveryCopyFails() async throws {
        for error in [RemoteErrorFixtures.retryable, LiteRepoError.ownershipLost, CancellationError()] {
            let state = try await interruptedOneDriveUpgrade()
            await state.client.setRecoveryCopyError(error)
            do {
                let prepared = try await RemoteLiteRepoGateway.prepareForegroundWrite(
                    client: state.client, lockClient: state.backing, basePath: base, writerID: state.writerID)
                await prepared.session.stopAndRelease()
                XCTFail("A failed recovery must not complete the upgrade")
            } catch { }
            for (path, expected) in state.scratch {
                let actual = await state.backing.fileData(path: path)
                XCTAssertEqual(actual, expected)
            }
            let canonical = await state.backing.fileData(path: monthPath(1))
            XCTAssertNil(canonical)
            let pending = await state.backing.fileData(path: versionPath)
            XCTAssertEqual(try VersionManifestLite.decode(XCTUnwrap(pending)).upgradePending, true)
        }
    }

    func testUnrecoverableOneDriveMonthCannotDisappearFromUpgradeChecks() async throws {
        let state = try await interruptedOneDriveUpgrade()
        let corrupt = Data("incomplete sqlite".utf8)
        for path in state.scratch.keys { await state.backing.seedFile(path: path, data: corrupt) }
        do {
            let prepared = try await RemoteLiteRepoGateway.prepareForegroundWrite(
                client: state.client, lockClient: state.backing, basePath: base, writerID: state.writerID)
            await prepared.session.stopAndRelease()
            XCTFail("An unrecoverable historical month cannot be omitted from the upgrade")
        } catch { }
        for path in state.scratch.keys {
            let actual = await state.backing.fileData(path: path)
            XCTAssertEqual(actual, corrupt)
        }
        let pending = await state.backing.fileData(path: versionPath)
        XCTAssertEqual(try VersionManifestLite.decode(XCTUnwrap(pending)).upgradePending, true)
        let decision = try await RepoFormatRouter(client: state.client, basePath: base).classify()
        XCTAssertEqual(decision, .fingerprintUpgrade)
    }
}

private actor InterruptedOneDriveUpgradeClient: RemoteStorageClientProtocol, OneDriveManifestItemIDClient {
    let backing: InMemoryRemoteStorageClient
    private var stopAfterBackup = true
    private var recoveryDownloadError: Error?
    private var recoveryCopyError: Error?
    func setRecoveryDownloadError(_ error: Error?) { recoveryDownloadError = error }
    func setRecoveryCopyError(_ error: Error?) { recoveryCopyError = error }
    init(backing: InMemoryRemoteStorageClient) { self.backing = backing }
    nonisolated func repairsMonthScratch() -> Bool { false }
    func connect() async throws { try await backing.connect() }
    func disconnect() async { await backing.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await backing.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await backing.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await backing.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await backing.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws {
        if let recoveryDownloadError, remotePath.contains("/months/"), remotePath.hasSuffix(".tmp") || remotePath.hasSuffix(".bak") {
            throw recoveryDownloadError
        }
        try await backing.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { try await backing.exists(path: path) }
    func delete(path: String) async throws { try await backing.delete(path: path) }
    func createDirectory(path: String) async throws { try await backing.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await backing.move(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        if let recoveryCopyError { throw recoveryCopyError }
        try await backing.copy(from: sourcePath, to: destinationPath)
    }
    func publishUploadedManifest(tempPath: String, finalPath: String, backupPath: String, ignoreCancellation: Bool,
        assertOwnership: nonisolated(nonsending) @escaping @Sendable () async throws -> Void) async throws -> OneDriveManifestPublishOutcome {
        try await assertOwnership()
        let hasFinal = try await backing.exists(path: finalPath)
        if hasFinal {
            try await backing.move(from: finalPath, to: backupPath)
            if stopAfterBackup { stopAfterBackup = false; throw CancellationError() }
        }
        try await assertOwnership()
        try await backing.move(from: tempPath, to: finalPath)
        return .init(finalFile: .init(itemID: finalPath), backupFile: hasFinal ? .init(itemID: backupPath) : nil)
    }
    func downloadKnownFileForReadBackVerification(_ file: OneDriveKnownFile, localURL: URL) async throws { try await backing.download(remotePath: file.itemID, localURL: localURL) }
    func deleteKnownPresentFile(_ file: OneDriveKnownFile) async throws { try await backing.delete(path: file.itemID) }
    func deleteKnownPresentFile(path: String) async throws { try await backing.delete(path: path) }
}

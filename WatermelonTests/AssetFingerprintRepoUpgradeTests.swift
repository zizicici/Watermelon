import GRDB
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

    private func makeMonth(_ month: Int, duplicates: Bool) throws -> (bytes: Data, fingerprint: Data, files: [String: Data]) {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let queue = try MonthManifestStore.makeManifestQueue(path: url.path)
        try MonthManifestStore.migrate(queue)
        let client = InMemoryRemoteStorageClient()
        let store = MonthManifestStore(client: client, basePath: base, year: 2026, month: month,
            localManifestURL: url, dbQueue: queue, remoteFilesByName: [:], dirty: false, layout: .v1)
        var files: [String: Data] = [:]
        var expected = Data()
        for copy in 0..<(duplicates ? 2 : 1) {
            let adjustment = ContentIdentityFixtures.adjustment(timestamp: Double(copy + 1))
            let inputs: [(Int, String, Data)] = [(1, "original.jpg", Data("original".utf8)),
                (5, "edited.jpg", Data("edited".utf8)), (7, "edit-\(copy).AAE", adjustment)]
            let resources = inputs.map { AssetContentFingerprint.Resource(role: $0.0, slot: 0, hash: ContentIdentityFixtures.hash($0.2)) }
            let raw = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: resources.map { ($0.role, $0.slot, $0.hash) })
            expected = try AssetContentFingerprint.fingerprint(resources: resources, adjustmentData: [ContentIdentityFixtures.hash(adjustment): adjustment])
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
        try queue.write { try $0.execute(sql: "ALTER TABLE asset_resources DROP COLUMN fingerprintHash") }
        return (try Data(contentsOf: url), expected, files)
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
        XCTAssertNotEqual(links.first { $0.role == 7 }?.resourceHash, links.first { $0.role == 7 }?.fingerprintHash)
        XCTAssertTrue(MonthManifestStore.isAssetIncomplete(links: Array(links.dropLast()), isResourceAvailable: { _ in true }, assetFingerprint: expected))
        let bytes = await client.fileData(path: versionPath)
        let manifest = try VersionManifestLite.decode(XCTUnwrap(bytes))
        XCTAssertEqual(manifest.formatVersion, 3)
        XCTAssertEqual(manifest.minAppVersion, "1.11.0")
        XCTAssertNil(manifest.upgradePending)
        XCTAssertNotEqual(manifest.formatVersion, 2, "Released 1.10 clients reject any format other than 2")
        let downloads = await client.downloadAttemptPaths
        XCTAssertEqual(downloads.filter { $0.hasSuffix(".AAE") }, [base + "/2026/01/edit-0.AAE"])
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
        await client.seedDirectory(RemoteThumbnailPaths.rootAbsolutePath(basePath: base))
        await client.seedFile(path: thumbnailPath, data: Data("thumbnail".utf8))
        let decision = try await RepoFormatRouter(client: client, basePath: base).classify()
        XCTAssertEqual(decision, .fingerprintUpgrade)
        let prepared = try await RemoteLiteRepoGateway.prepareForegroundWrite(
            client: client, lockClient: client, basePath: base, writerID: UUID().uuidString.lowercased()
        )
        await prepared.session.stopAndRelease()
        let store = try await snapshot(client)
        XCTAssertNotNil(store.assetsByFingerprint[expected])
        let thumbnail = await client.fileData(path: thumbnailPath)
        XCTAssertEqual(thumbnail, Data("thumbnail".utf8))
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

    func testMissingAdjustmentKeepsPartialMediaAndAllowsOtherMonthsToUpgrade() async throws {
        let client = InMemoryRemoteStorageClient()
        _ = try await seed(client)
        let second = try await seed(client, month: 2)
        let oldResult = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: base, year: 2026, month: 1, layout: .v1,
            manifestAbsolutePath: monthPath(1), pushSchemaUpgrade: false, surfaceDownloadFailure: true
        )
        let oldStore = try XCTUnwrap(oldResult)
        let oldFingerprint = try XCTUnwrap(oldStore.assetsByFingerprint.keys.first)
        try await client.delete(path: base + "/2026/01/edit-0.AAE")
        let prepared = try await RemoteLiteRepoGateway.prepareForegroundWrite(
            client: client, lockClient: client, basePath: base, writerID: UUID().uuidString.lowercased()
        )
        await prepared.session.stopAndRelease()
        let firstStore = try await snapshot(client)
        XCTAssertEqual(Set(firstStore.assetsByFingerprint.keys), [oldFingerprint])
        XCTAssertEqual(Set(firstStore.itemsByFileName.keys), ["original.jpg", "edited.jpg"])
        let links = try XCTUnwrap(firstStore.assetLinksByFingerprint[oldFingerprint])
        XCTAssertEqual(Set(links.map(\.role)), [1, 5, 7])
        XCTAssertEqual(Set(links.filter { firstStore.itemsByHash[$0.resourceHash] != nil }.map(\.role)), [1, 5])
        XCTAssertTrue(MonthManifestStore.isAssetIncomplete(links: links,
            isResourceAvailable: { firstStore.itemsByHash[$0] != nil }, assetFingerprint: oldFingerprint))
        let secondStore = try await snapshot(client, month: 2)
        XCTAssertNotNil(secondStore.assetsByFingerprint[second])
        let decision = try await RepoFormatRouter(client: client, basePath: base).classify()
        XCTAssertEqual(decision, .current)
    }

    func testAdjustmentDownloadFaultsDoNotRemoveResourcesOrFinishUpgrade() async throws {
        for error in [RemoteErrorFixtures.retryable, RemoteErrorFixtures.terminal, RemoteErrorFixtures.cancelled] {
            let client = InMemoryRemoteStorageClient()
            _ = try await seed(client)
            let path = base + "/2026/01/edit-0.AAE"
            let originalManifest = await client.fileData(path: monthPath(1))
            await client.setOnDownloadAttempt { attempted in
                if attempted == path { await client.enqueueDownloadError(error) }
            }
            do { try await upgrade(client); XCTFail("Download faults must block the upgrade") } catch { }
            let bytes = await client.fileData(path: versionPath)
            XCTAssertEqual(try VersionManifestLite.decode(XCTUnwrap(bytes)).upgradePending, true)
            let manifestAfter = await client.fileData(path: monthPath(1))
            XCTAssertEqual(manifestAfter, originalManifest)
            let adjustmentAfter = await client.fileData(path: path)
            XCTAssertNotNil(adjustmentAfter)
        }
    }

    func testFailureKeepsPendingBoundaryAndReconnectionReusesConvertedMonth() async throws {
        let client = InMemoryRemoteStorageClient()
        let first = try await seed(client)
        let second = try await seed(client, month: 2)
        let corruptPath = base + "/2026/02/edit-0.AAE"
        let originalBytes = await client.fileData(path: corruptPath)
        await client.seedFile(path: corruptPath, data: Data("corrupted".utf8))
        do { try await upgrade(client); XCTFail("Corrupt adjustment must prevent final commit") } catch { }
        let decision = try await RepoFormatRouter(client: client, basePath: base).classify()
        XCTAssertEqual(decision, .fingerprintUpgrade)
        let pendingData = await client.fileData(path: versionPath)
        let pending = try VersionManifestLite.decode(XCTUnwrap(pendingData))
        XCTAssertEqual(pending.formatVersion, 3)
        XCTAssertEqual(pending.upgradePending, true)
        let downloadsBefore = await client.downloadAttemptPaths.count
        await client.seedFile(path: corruptPath, data: try XCTUnwrap(originalBytes))
        try await upgrade(client)
        let downloads = await client.downloadAttemptPaths
        XCTAssertFalse(downloads.dropFirst(downloadsBefore).contains(base + "/2026/01/edit-0.AAE"))
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

    func testDirectPutBackendKeepsCanonicalVersionAndNormalizedMonth() async throws {
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
}

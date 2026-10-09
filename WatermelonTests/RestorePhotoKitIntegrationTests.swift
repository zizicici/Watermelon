import GRDB
import Photos
import XCTest
@testable import Watermelon

final class RestorePhotoKitIntegrationTests: XCTestCase {
    private struct Resource: Decodable {
        let role: Int
        let slot: Int
        let hash: String
        let fileName: String
        let fileSize: Int64
        let month: String
        let creationDateMs: Int64?
        let fixturePath: String

        var instance: RemoteAssetResourceInstance {
            let chars = Array(hash)
            let data = Data(stride(from: 0, to: chars.count, by: 2).map {
                UInt8(String(chars[$0...($0 + 1)]), radix: 16)!
            })
            return RemoteAssetResourceInstance(role: role, slot: slot, resourceHash: data,
                fileName: fileName, fileSize: fileSize,
                remoteRelativePath: month.replacingOccurrences(of: "-", with: "/") + "/" + fileName,
                creationDateMs: creationDateMs)
        }
    }

    private struct Fixture: Decodable {
        let id: String
        let expectedKind: String
        let resources: [Resource]
        let expectedResources: [Resource]
        let creationDateMs: Int64?
    }

    private var createdIDs: [String] = []

    func testRestoreRoundTripAndDeduplication() async throws {
        #if targetEnvironment(simulator)
        let directory = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("RestoreSimulatorMatrixFixtures")
        guard FileManager.default.fileExists(atPath: directory.appendingPathComponent("RUN_RESTORE_INTEGRATION").path) else {
            throw XCTSkip("Requires opt-in local simulator fixtures")
        }
        let fixtures = try JSONDecoder().decode([Fixture].self, from: Data(contentsOf: directory.appendingPathComponent("fixed-cases.json")))
        let status = await PHPhotoLibrary.requestAuthorization(for: .readWrite)
        XCTAssertEqual(status, .authorized)
        guard status == .authorized else { return }
        addTeardownBlock { [self] in
            let assets = PHAsset.fetchAssets(withLocalIdentifiers: createdIDs, options: nil)
            guard assets.count > 0 else { return }
            try await PHPhotoLibrary.shared().performChanges { PHAssetChangeRequest.deleteAssets(assets) }
            XCTAssertEqual(PHAsset.fetchAssets(withLocalIdentifiers: createdIDs, options: nil).count, 0)
        }

        var results: [[String: Any]] = []
        for fixture in fixtures {
            let result = try await restoreAndVerify(fixture, directory: directory)
            results.append(result)
            let data = try JSONSerialization.data(withJSONObject: results, options: [.prettyPrinted, .sortedKeys])
            try data.write(to: directory.appendingPathComponent("fixed-results.json"), options: .atomic)
            print("SIMULATOR_MATRIX \(fixture.id) kind=\(result["actualKind"]!) mediaBytesMatch=\(result["mediaBytesMatch"]!) fullFingerprintMatches=\(result["fullFingerprintMatches"]!)")
        }
        XCTAssertEqual(results.count, fixtures.count)
        #else
        throw XCTSkip("Simulator-only diagnostic")
        #endif
    }

    func testMigrationPreservesCopiedLibraryAndRekeysCachedHashes() throws {
        #if targetEnvironment(simulator)
        let fixtures = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("RestoreSimulatorMatrixFixtures")
        guard FileManager.default.fileExists(atPath: fixtures.appendingPathComponent("RUN_RESTORE_INTEGRATION").path),
              FileManager.default.fileExists(atPath: fixtures.appendingPathComponent("database-before.sqlite").path) else {
            throw XCTSkip("Requires opt-in database copy")
        }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = directory.appendingPathComponent("database.sqlite")
        try FileManager.default.copyItem(at: fixtures.appendingPathComponent("database-before.sqlite"), to: url)
        let before = try DatabaseQueue(path: url.path).read { db in
            (try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_assets")!,
             try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_asset_resources")!,
             try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_assets WHERE assetFingerprint IS NOT NULL AND assetLocalIdentifier IN (SELECT assetLocalIdentifier FROM local_asset_resources WHERE role = 7)")!)
        }
        let migrated = try DatabaseManager(databaseURL: url)
        try migrated.read { db in
            XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_assets"), before.0)
            XCTAssertEqual(try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_asset_resources"), before.1)
            let assets = try Row.fetchAll(db, sql: "SELECT assetLocalIdentifier, assetFingerprint FROM local_assets WHERE assetFingerprint IS NOT NULL AND assetLocalIdentifier IN (SELECT assetLocalIdentifier FROM local_asset_resources WHERE role = 7)")
            for asset in assets {
                let resources = try Row.fetchAll(db, sql: "SELECT role, slot, contentHash FROM local_asset_resources WHERE assetLocalIdentifier = ?", arguments: [asset[0] as String])
                let expected = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: resources.map {
                    (role: $0[0] as Int, slot: $0[1] as Int, contentHash: $0[2] as Data)
                })
                XCTAssertEqual(asset[1] as Data, expected)
            }
            XCTAssertEqual(try String.fetchOne(db, sql: "PRAGMA integrity_check"), "ok")
        }
        print("RESTORE_MIGRATION assets=\(before.0) resources=\(before.1) rekeyCandidates=\(before.2)")
        #else
        throw XCTSkip("Simulator-only fixture")
        #endif
    }

    private func restoreAndVerify(_ fixture: Fixture, directory: URL) async throws -> [String: Any] {
        let client = InMemoryRemoteStorageClient()
        var enqueued = Set<String>()
        for resource in fixture.resources where enqueued.insert(resource.hash).inserted {
            await client.enqueueDownloadData(try Data(contentsOf: directory.appendingPathComponent(resource.fixturePath)))
        }
        let instances = fixture.resources.map(\.instance)
        let expected = fixture.expectedResources.map(\.instance)
        let expectedFingerprint = fingerprint(expected)
        let profile = ServerProfileRecord(id: nil, name: "fixture", storageType: StorageType.webdav.rawValue,
            connectionParams: nil, sortOrder: 0, host: "fixture.local", port: 0, shareName: "fixture", basePath: "/p",
            username: "fixture", domain: nil, credentialRef: "fixture", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil)
        let dbDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dbDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dbDirectory) }
        let database = try DatabaseManager(databaseURL: dbDirectory.appendingPathComponent("index.sqlite"))
        let repository = ContentHashIndexRepository(databaseManager: database)
        let profileKey = RemoteIndexSyncService.remoteProfileKey(profile)
        let service = RestoreService(makeClient: { _, _ in client }, hashIndexRepository: repository)
        let restored = try await service.restoreItems(
            items: [.init(instances: instances, identity: fingerprint(instances), creationDate: fixture.creationDateMs.map { Date(millisecondsSinceEpoch: $0) })], profile: profile,
            password: "", onItemCompleted: { _, _, _ in }
        )
        let item = try XCTUnwrap(restored.first)
        createdIDs.append(item.asset.localIdentifier)
        XCTAssertEqual(item.asset.importedInstances.map(\.role).sorted(), expected.map(\.role).sorted(), fixture.id)
        XCTAssertTrue(item.asset.indexWriteHandled)
        let asset = try XCTUnwrap(PHAsset.fetchAssets(withLocalIdentifiers: [item.asset.localIdentifier], options: nil).firstObject)
        if let date = fixture.creationDateMs {
            XCTAssertEqual(asset.creationDate?.millisecondsSinceEpoch, date)
        }
        let kind = asset.mediaType == .video ? "video" : asset.mediaSubtypes.contains(.photoLive) ? "livePhoto" : "photo"
        XCTAssertEqual(kind, fixture.expectedKind, fixture.id)
        if kind == "video" { XCTAssertGreaterThan(asset.duration, 0, fixture.id) }

        let selected = BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(from: PHAssetResource.assetResources(for: asset))
        XCTAssertEqual(selected.map(\.role).sorted(), expected.map(\.role).sorted(), fixture.id)
        var tokens: [(role: Int, slot: Int, contentHash: Data)] = []
        var checks: [[String: Any]] = []
        var mediaMatches = true
        for entry in selected {
            let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            defer { try? FileManager.default.removeItem(at: url) }
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                PHAssetResourceManager.default().writeData(for: entry.resource, toFile: url, options: nil) { error in
                    if let error { continuation.resume(throwing: error) } else { continuation.resume() }
                }
            }
            let original = try XCTUnwrap(fixture.expectedResources.first { $0.role == entry.role && $0.slot == entry.slot })
            let hash = try AssetProcessor.contentHash(of: url)
            let matches = hash.hexString == original.hash
            let check: [String: Any] = ["role": entry.role, "slot": entry.slot, "sha256Matches": matches]
            if entry.role != ResourceTypeCode.adjustmentData {
                XCTAssertTrue(matches, "\(fixture.id) role=\(entry.role)")
                mediaMatches = mediaMatches && matches
            }
            checks.append(check)
            tokens.append((entry.role, entry.slot, hash))
        }
        let importedFingerprint = AssetContentFingerprint.fingerprint(resources: tokens.map { .init(role: $0.role, slot: $0.slot, hash: $0.contentHash) })
        XCTAssertEqual(importedFingerprint, expectedFingerprint, fixture.id)
        let ids: Set<String> = [item.asset.localIdentifier]
        let initial = try XCTUnwrap(repository.fetchAssetHashCaches(assetIDs: ids)[item.asset.localIdentifier])
        XCTAssertEqual(initial.assetFingerprint, importedFingerprint, fixture.id)
        let sourceFingerprint = fingerprint(instances)
        let complete = expectedFingerprint == sourceFingerprint
        XCTAssertEqual(item.asset.isCompleteRestore, complete, fixture.id)
        try database.write { db in
            try db.execute(sql: "DELETE FROM sync_state WHERE stateKey = 'local_asset_fingerprint_version'")
        }
        let restartedRepository = ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: dbDirectory.appendingPathComponent("index.sqlite")))
        XCTAssertTrue(try restartedRepository.fetchInvalidatedFingerprintAssetIDs().isEmpty)
        XCTAssertEqual(try restartedRepository.fetchAssetHashCaches(assetIDs: ids)[item.asset.localIdentifier]?.assetFingerprint, importedFingerprint)
        try restartedRepository.clearLocalHashIndex()
        let builder = LocalHashIndexBuildService(photoLibraryService: PhotoLibraryService(), repository: restartedRepository)
        try await LocalDownloadIndexPreflight.run(
            assetIDs: ids, buildService: builder, iCloudPhotoBackupMode: .disable,
            onReady: { XCTAssertEqual($0, ids) }
        )
        XCTAssertEqual(try restartedRepository.fetchAssetHashCaches(assetIDs: ids)[item.asset.localIdentifier]?.assetFingerprint, importedFingerprint)
        let month = LibraryMonthKey.from(date: asset.creationDate, calendar: LibraryMonthKey.currentPreferenceMonthCalendar())
        let remoteResources = instances.map { RemoteManifestResource(year: month.year, month: month.month,
            fileName: $0.fileName, contentHash: $0.resourceHash, fileSize: $0.fileSize, resourceType: $0.role,
            creationDateMs: $0.creationDateMs, backedUpAtMs: 0) }
        let remoteAsset = RemoteManifestAsset(year: month.year, month: month.month, assetFingerprint: sourceFingerprint,
            creationDateMs: asset.creationDate?.millisecondsSinceEpoch, backedUpAtMs: 0, resourceCount: instances.count,
            totalFileSizeBytes: instances.reduce(0) { $0 + $1.fileSize })
        let links = instances.map { RemoteAssetResourceLink(year: month.year, month: month.month,
            assetFingerprint: sourceFingerprint, resourceHash: $0.resourceHash, role: $0.role, slot: $0.slot) }
        var destinationProfile = profile
        destinationProfile.host = "second-fixture.local"
        let destinationKey = RemoteIndexSyncService.remoteProfileKey(destinationProfile)
        XCTAssertNotEqual(destinationKey, profileKey)
        for resource in fixture.resources where resource.role == ResourceTypeCode.adjustmentData {
            let path = String(format: "/p/%04d/%02d/%@", month.year, month.month, resource.fileName)
            await client.seedFile(path: path, data: try Data(contentsOf: directory.appendingPathComponent(resource.fixturePath)))
        }
        let delta = RemoteLibraryMonthDelta(month: month, resources: remoteResources, assets: [remoteAsset], assetResourceLinks: links)
        let worker = HomeDataProcessingWorker(photoLibraryService: PhotoLibraryService(), contentHashIndexRepository: restartedRepository,
            remoteMonthSnapshot: { $0 == month ? delta : nil })
        _ = await worker.loadLocalIndex(forceReload: true, scope: .device(.all))
        _ = await worker.syncRemoteSnapshot(state: .init(revision: 1, isFullSnapshot: true, monthDeltas: [delta], profileKey: destinationKey), hasActiveConnection: true)
        let remaining = await worker.remoteOnlyItems(for: month, expectedScope: .device(.all))
        XCTAssertEqual(remaining.count, complete ? 0 : 1, fixture.id)
        let remoteCache = RemoteLibrarySnapshotCache()
        remoteCache.setProfileKey(destinationKey)
        _ = remoteCache.replaceMonth(month, resources: remoteResources, assets: [remoteAsset], assetResourceLinks: links)
        let coordinator = BackupCoordinator(photoLibraryService: PhotoLibraryService(), storageClientFactory: StorageClientFactory(),
            hashIndexRepository: restartedRepository, databaseManager: database,
            remoteIndexService: RemoteIndexSyncService(snapshotCache: remoteCache))
        let presence = LibraryPresenceIndex(hashIndexRepository: restartedRepository, coordinator: coordinator, profileKey: { destinationKey })
        let handles = presence.repositoryLocalIdentifiersForCurrentBytes([sourceFingerprint])
        XCTAssertEqual(handles[sourceFingerprint], complete ? item.asset.localIdentifier : nil)
        let presenceReady = await presence.refresh(notifyOnCommit: false)
        XCTAssertTrue(presenceReady)
        XCTAssertEqual(presence.localIdentifierForCurrentBytes(sourceFingerprint), complete ? item.asset.localIdentifier : nil)
        XCTAssertEqual(presence.currentAssetsMatch([item.asset.localIdentifier: sourceFingerprint]), complete)
        if complete {
            let manifestURL = dbDirectory.appendingPathComponent("manifest.sqlite")
            let queue = try DatabaseQueue(path: manifestURL.path)
            try MonthManifestStore.migrate(queue)
            let store = MonthManifestStore(client: client, basePath: "/p", year: month.year, month: month.month,
                localManifestURL: manifestURL, dbQueue: queue, remoteFilesByName: [:], dirty: false,
                layout: .lite, liteWriteOwnership: .uniform({}))
            for resource in remoteResources { _ = try store.upsertResource(resource) }
            try store.upsertAsset(remoteAsset, links: links)
            let lock = try XCTUnwrap(WriteLockService(basePath: "/p", writerID: UUID().uuidString.lowercased(), client: client))
            let context = AssetProcessContext(workerID: 0, asset: asset, selectedResources: selected,
                cachedLocalHash: try restartedRepository.fetchAssetHashCaches(assetIDs: ids)[item.asset.localIdentifier],
                iCloudPhotoBackupMode: .disable, pass: .localResources, monthStore: store, profile: destinationProfile,
                assetPosition: 1, totalAssets: 1, writeMode: .lite(RepoLeaseSession(lock: lock), nil))
            let processor = AssetProcessor(photoLibraryService: PhotoLibraryService(), hashIndexRepository: restartedRepository,
                remoteIndexService: RemoteIndexSyncService())
            let outcome = try await processor.process(context: context, client: client, eventStream: BackupEventStream(), cancellationController: nil)
            XCTAssertEqual(outcome.status, .skipped)
            XCTAssertTrue(BackupParallelExecutor.shouldEmitResultCredit(outcome))
            XCTAssertEqual(outcome.uploadedFileSizeBytes, 0)
            XCTAssertEqual(store.assetsByFingerprint.count, 1)
        }
        return ["case": fixture.id, "expectedKind": fixture.expectedKind, "actualKind": kind,
                "roles": selected.map(\.role), "mediaBytesMatch": mediaMatches, "duration": asset.duration,
                "fullFingerprintMatches": importedFingerprint == expectedFingerprint,
                "resourceChecks": checks, "localIdentifier": item.asset.localIdentifier,
                "actualIndexMatches": initial.assetFingerprint == importedFingerprint, "complete": complete,
                "remoteOnlyAfterRebuild": remaining.count, "crossRemoteWithoutReceipt": true, "sourceDate": fixture.creationDateMs as Any? ?? NSNull(),
                "restoredDate": asset.creationDate?.millisecondsSinceEpoch as Any? ?? NSNull()]
    }

    func testCreationDateBackupRoundTrip() async throws {
        #if targetEnvironment(simulator)
        let directory = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("RestoreSimulatorMatrixFixtures")
        guard FileManager.default.fileExists(atPath: directory.appendingPathComponent("RUN_RESTORE_INTEGRATION").path) else {
            throw XCTSkip("Requires opt-in local simulator fixtures")
        }
        let fixtures = try JSONDecoder().decode([Fixture].self, from: Data(contentsOf: directory.appendingPathComponent("fixed-cases.json")))
            .filter { ["ordinary-photo", "edited-photo"].contains($0.id) }
        guard fixtures.count == 2 else { throw XCTSkip("Requires ordinary and edited photo fixtures") }
        let authorization = await PHPhotoLibrary.requestAuthorization(for: .readWrite)
        XCTAssertEqual(authorization, .authorized)
        guard authorization == .authorized else { return }
        addTeardownBlock { [self] in
            let assets = PHAsset.fetchAssets(withLocalIdentifiers: createdIDs, options: nil)
            guard assets.count > 0 else { return }
            try await PHPhotoLibrary.shared().performChanges { PHAssetChangeRequest.deleteAssets(assets) }
            XCTAssertEqual(PHAsset.fetchAssets(withLocalIdentifiers: createdIDs, options: nil).count, 0)
        }
        for fixture in fixtures {
            try await verifyCreationDateBackupRoundTrip(fixture, directory: directory)
        }
        #else
        throw XCTSkip("Simulator-only diagnostic")
        #endif
    }

    private func verifyCreationDateBackupRoundTrip(_ fixture: Fixture, directory: URL) async throws {
        let oldDate = Date(millisecondsSinceEpoch: 1_768_478_400_000)
        let newDate = oldDate.addingTimeInterval(86_400)
        let instances = fixture.resources.map { resource in
            RemoteAssetResourceInstance(role: resource.role, slot: resource.slot, resourceHash: resource.instance.resourceHash,
                fileName: resource.fileName, fileSize: resource.fileSize,
                remoteRelativePath: "2026/01/" + resource.fileName, creationDateMs: oldDate.millisecondsSinceEpoch)
        }
        let remoteFingerprint = fingerprint(instances)
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory("/p/2026/01")
        for (instance, resource) in zip(instances, fixture.resources) {
            await client.seedFile(path: "/p/" + instance.remoteRelativePath,
                data: try Data(contentsOf: directory.appendingPathComponent(resource.fixturePath)))
        }
        let dbDirectory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dbDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dbDirectory) }
        let database = try DatabaseManager(databaseURL: dbDirectory.appendingPathComponent("index.sqlite"))
        let repository = ContentHashIndexRepository(databaseManager: database)
        let profile = ServerProfileRecord(id: nil, name: "date-test", storageType: StorageType.webdav.rawValue,
            connectionParams: nil, sortOrder: 0, host: "fixture.local", port: 0, shareName: "fixture", basePath: "/p",
            username: "fixture", domain: nil, credentialRef: "fixture", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil)
        let profileKey = RemoteIndexSyncService.remoteProfileKey(profile)
        let restore = RestoreService(makeClient: { _, _ in client }, hashIndexRepository: repository)
        let imported = try await restore.restoreItems(items: [.init(instances: instances, identity: remoteFingerprint, creationDate: oldDate)],
            profile: profile, password: "", onItemCompleted: { _, _, _ in })
        let localID = try XCTUnwrap(imported.first?.asset.localIdentifier)
        createdIDs.append(localID)
        let original = try XCTUnwrap(PHAsset.fetchAssets(withLocalIdentifiers: [localID], options: nil).firstObject)
        try await PHPhotoLibrary.shared().performChanges { PHAssetChangeRequest(for: original).creationDate = newDate }
        let builder = LocalHashIndexBuildService(photoLibraryService: PhotoLibraryService(), repository: repository)
        let index = try await builder.buildIndex(for: [localID], workerCount: 1)
        XCTAssertEqual(index.readyAssetIDs, [localID])
        let asset = try XCTUnwrap(PHAsset.fetchAssets(withLocalIdentifiers: [localID], options: nil).firstObject)
        let cached = try XCTUnwrap(repository.fetchAssetHashCaches(assetIDs: [localID])[localID])
        XCTAssertEqual(cached.assetFingerprint, remoteFingerprint)
        let manifestURL = dbDirectory.appendingPathComponent("manifest.sqlite")
        let queue = try DatabaseQueue(path: manifestURL.path)
        try MonthManifestStore.migrate(queue)
        let store = MonthManifestStore(client: client, basePath: "/p", year: 2026, month: 1,
            localManifestURL: manifestURL, dbQueue: queue, remoteFilesByName: [:], dirty: false,
            layout: .lite, liteWriteOwnership: .uniform({}))
        for instance in instances {
            _ = try store.upsertResource(.init(year: 2026, month: 1, fileName: instance.fileName,
                contentHash: instance.resourceHash, fileSize: instance.fileSize, resourceType: instance.role,
                creationDateMs: oldDate.millisecondsSinceEpoch, backedUpAtMs: 0))
        }
        try store.upsertAsset(.init(year: 2026, month: 1, assetFingerprint: remoteFingerprint,
            creationDateMs: oldDate.millisecondsSinceEpoch, backedUpAtMs: 0, resourceCount: instances.count,
            totalFileSizeBytes: instances.reduce(0) { $0 + $1.fileSize }), links: instances.map {
                .init(year: 2026, month: 1, assetFingerprint: remoteFingerprint, resourceHash: $0.resourceHash, role: $0.role, slot: $0.slot)
            })
        _ = try await store.flushToRemote()
        let uploadsBefore = await client.uploadedPaths.count
        let snapshotCache = RemoteLibrarySnapshotCache()
        snapshotCache.setProfileKey(profileKey)
        let remoteIndex = RemoteIndexSyncService(snapshotCache: snapshotCache)
        let initial = store.unsortedSnapshot()
        remoteIndex.replaceCachedMonth(.init(year: 2026, month: 1), resources: initial.resources,
            assets: initial.assets, links: initial.links, expectedProfileKey: profileKey)
        let processor = AssetProcessor(photoLibraryService: PhotoLibraryService(), hashIndexRepository: repository, remoteIndexService: remoteIndex)
        let executor = BackupParallelExecutor(hashIndexRepository: repository, assetProcessor: processor, remoteIndexService: remoteIndex)
        XCTAssertFalse(executor.monthAlreadyFullyBackedUp(monthAssetIDs: [localID], monthStore: store))
        let lock = try XCTUnwrap(WriteLockService(basePath: "/p", writerID: UUID().uuidString.lowercased(), client: client))
        let context = AssetProcessContext(workerID: 0, asset: asset,
            selectedResources: BackupAssetResourcePlanner.orderedResourcesWithRoleSlot(from: PHAssetResource.assetResources(for: asset)),
            cachedLocalHash: cached, iCloudPhotoBackupMode: .disable, pass: .localResources,
            monthStore: store, profile: profile, assetPosition: 1, totalAssets: 1, writeMode: .lite(RepoLeaseSession(lock: lock), nil))
        let updated = try await processor.process(context: context, client: client, eventStream: BackupEventStream(), cancellationController: nil)
        XCTAssertEqual(updated.status, .success)
        XCTAssertEqual(updated.reason, AssetProcessor.assetDateUpdatedReason)
        XCTAssertEqual(updated.uploadedFileSizeBytes, 0)
        XCTAssertEqual(store.assetsByFingerprint.count, 1)
        XCTAssertEqual(store.assetsByFingerprint[remoteFingerprint]?.creationDateMs, newDate.millisecondsSinceEpoch)
        XCTAssertEqual(remoteIndex.fullSnapshot().assets.first?.creationDateMs, newDate.millisecondsSinceEpoch)
        let uploadsAfterProcessing = await client.uploadedPaths.count
        XCTAssertEqual(uploadsAfterProcessing, uploadsBefore)
        _ = try await store.flushToRemote()
        let uploadsAfterFlush = await client.uploadedPaths.count
        let repeated = try await processor.process(context: context, client: client, eventStream: BackupEventStream(), cancellationController: nil)
        XCTAssertEqual(repeated.status, .skipped)
        XCTAssertFalse(store.dirty)
        let repeatedFlush = try await store.flushToRemote()
        XCTAssertFalse(repeatedFlush)
        let finalUploads = await client.uploadedPaths.count
        XCTAssertEqual(finalUploads, uploadsAfterFlush)
        if fixture.id == "ordinary-photo" { XCTAssertTrue(executor.monthAlreadyFullyBackedUp(monthAssetIDs: [localID], monthStore: store)) }
        let reloaded = try await MonthManifestStore.loadOrCreate(client: client, basePath: "/p", year: 2026, month: 1, layout: .lite)
        let saved = try XCTUnwrap(reloaded.assetsByFingerprint[remoteFingerprint])
        let restored = try await restore.restoreItems(items: [.init(instances: instances, identity: remoteFingerprint, creationDate: saved.creationDate)],
            profile: profile, password: "", onItemCompleted: { _, _, _ in })
        let restoredID = try XCTUnwrap(restored.first?.asset.localIdentifier)
        createdIDs.append(restoredID)
        let photo = try XCTUnwrap(PHAsset.fetchAssets(withLocalIdentifiers: [restoredID], options: nil).firstObject)
        XCTAssertEqual(photo.creationDate?.millisecondsSinceEpoch, newDate.millisecondsSinceEpoch)
        print("DATE_ROUND_TRIP \(fixture.id) date=\(newDate.millisecondsSinceEpoch) mediaUploads=0 repeatWrites=0")
    }

    private func fingerprint(_ instances: [RemoteAssetResourceInstance]) -> Data {
        AssetContentFingerprint.fingerprint(resources: instances.map(\.contentIdentityResource))
    }
}

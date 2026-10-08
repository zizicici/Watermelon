import CryptoKit
import XCTest
@testable import Watermelon

final class RestoreCompletionTests: XCTestCase {
    private func profile() -> ServerProfileRecord {
        ServerProfileRecord(id: nil, name: "test", storageType: StorageType.webdav.rawValue,
            connectionParams: nil, sortOrder: 0, host: "fixture.local", port: 0, shareName: "test", basePath: "/p",
            username: "test", domain: nil, credentialRef: "test", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil)
    }

    func testImportPersistsActualHashesAndNormalizedFingerprintDespiteCancellation() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let repository = ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")))
        let client = InMemoryRemoteStorageClient()
        var source: [RemoteAssetResourceInstance] = []
        var actual: [RemoteAssetResourceInstance] = []
        let sourceAdjustment = ContentIdentityFixtures.adjustment()
        let actualAdjustment = ContentIdentityFixtures.adjustment(timestamp: 2)
        for role in [2, 5, 6, 7] {
            let bytes = role == 7 ? sourceAdjustment : Data("resource-\(role)".utf8)
            await client.enqueueDownloadData(bytes)
            func instance(_ data: Data) -> RemoteAssetResourceInstance {
                RemoteAssetResourceInstance(role: role, slot: 0, resourceHash: Data(SHA256.hash(data: data)),
                    fileName: "resource-\(role)", fileSize: Int64(data.count), remoteRelativePath: "2026/01/resource-\(role)", creationDateMs: 1_000)
            }
            source.append(instance(bytes))
            actual.append(instance(role == 7 ? actualAdjustment : bytes))
        }
        let imported = actual
        let remoteFingerprint = try AssetContentFingerprint.fingerprint(resources: source.map(\.contentIdentityResource), adjustmentData: [ContentIdentityFixtures.hash(sourceAdjustment): sourceAdjustment])
        let actualFingerprint = try AssetContentFingerprint.fingerprint(resources: actual.map(\.contentIdentityResource), adjustmentData: [ContentIdentityFixtures.hash(actualAdjustment): actualAdjustment])
        let expectedDate = Date(timeIntervalSince1970: 123_456)
        let modified = Date().millisecondsSinceEpoch - 1_000
        let service = RestoreService(makeClient: { _, _ in client }, importAsset: { _, date in
            XCTAssertEqual(date, expectedDate)
            withUnsafeCurrentTask { $0?.cancel() }
            return "restored"
        }, inspectImportedAsset: { _, _ in
            XCTAssertFalse(Task.isCancelled)
            return .init(instances: imported, modificationDateMs: modified,
                adjustmentData: [ContentIdentityFixtures.hash(actualAdjustment): actualAdjustment])
        }, hashIndexRepository: repository)
        let profile = profile()
        let item = RestoreService.RestoreItemDescriptor(instances: source, identity: remoteFingerprint, creationDate: expectedDate)
        let restored = try await Task {
            try await service.restoreItems(items: [item], profile: profile, password: "", onItemCompleted: { _, _, result in
                XCTAssertTrue(result?.asset.indexWriteHandled == true)
                XCTAssertTrue(result?.asset.isCompleteRestore == true)
                XCTAssertEqual(try repository.fetchAssetHashCaches(assetIDs: ["restored"])["restored"]?.assetFingerprint, actualFingerprint)
            })
        }.value
        XCTAssertEqual(restored.count, 1)
        XCTAssertEqual(remoteFingerprint, actualFingerprint)
        XCTAssertNotEqual(source.last?.resourceHash, actual.last?.resourceHash)
    }

    func testInspectionFailureDoesNotRecordUnverifiedSourceHashes() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let repository = ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite")))
        let bytes = Data("photo".utf8)
        let resource = RemoteAssetResourceInstance(role: 1, slot: 0, resourceHash: Data(SHA256.hash(data: bytes)),
            fileName: "photo.jpg", fileSize: Int64(bytes.count), remoteRelativePath: "2026/01/photo.jpg", creationDateMs: 1_000)
        let fingerprint = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: [(role: 1, slot: 0, contentHash: resource.resourceHash)])
        let client = InMemoryRemoteStorageClient()
        await client.enqueueDownloadData(bytes)
        let probe = PendingImportProbe()
        let modified = Date().millisecondsSinceEpoch - 1_000
        let service = RestoreService(makeClient: { _, _ in client }, importAsset: { _, _ in
            await probe.imported()
            return "pending"
        }, inspectImportedAsset: { _, _ in
            if await probe.shouldFailInspection() { throw NSError(domain: "inspection", code: 1) }
            return .init(instances: [resource], modificationDateMs: modified)
        }, hashIndexRepository: repository)
        let descriptor = RestoreService.RestoreItemDescriptor(instances: [resource], identity: fingerprint)
        let first = try await service.restoreItems(items: [descriptor], profile: profile(), password: "", onItemCompleted: { _, _, _ in })
        XCTAssertFalse(first.first?.asset.isCompleteRestore ?? true)
        XCTAssertTrue(first.first?.asset.indexWriteHandled == true)
        XCTAssertTrue(try repository.fetchAssetHashCaches(assetIDs: ["pending"]).isEmpty)
        let imports = await probe.imports
        XCTAssertEqual(imports, 1)
    }

    func testBrowserResolutionAndDescriptorMergeKeepAuthoritativeAssetDate() {
        let resource = TestFixtures.remoteResource(year: 2026, month: 1, contentHash: Data([1]), resourceType: 2)
        let fingerprint = BackupAssetResourcePlanner.assetFingerprint(resourceRoleSlotHashes: [(role: 2, slot: 0, contentHash: resource.contentHash)])
        let asset = RemoteManifestAsset(year: 2026, month: 1, assetFingerprint: fingerprint, creationDateMs: 123_456_000,
            backedUpAtMs: 124_000_000, resourceCount: 1, totalFileSizeBytes: resource.fileSize)
        let link = TestFixtures.remoteLink(year: 2026, month: 1, assetFingerprint: fingerprint, resourceHash: resource.contentHash, role: 2)
        let state = RemoteLibrarySnapshotState(revision: 1, isFullSnapshot: true, monthDeltas: [
            RemoteLibraryMonthDelta(month: .init(year: 2026, month: 1), resources: [resource], assets: [asset], assetResourceLinks: [link])
        ], profileKey: nil)
        let resolved = MediaBrowserActionRunner.resolveInstances(from: state, fingerprint: fingerprint, preferredMonth: nil)
        XCTAssertEqual(resolved.creationDate, asset.creationDate)
        let descriptor = RestoreService.RestoreItemDescriptor(instances: resolved.instances, identity: fingerprint,
            creationDate: resolved.creationDate, isIncomplete: resolved.isIncomplete)
        let deduped = MediaBrowserActionRunner.dedupeResolvedDescriptors([descriptor, descriptor])
        XCTAssertEqual(deduped.count, 1)
        XCTAssertEqual(deduped.first?.creationDate, asset.creationDate)
        XCTAssertFalse(deduped.first?.isIncomplete ?? true)
    }
}

private actor PendingImportProbe {
    private(set) var imports = 0
    private var inspections = 0
    func imported() { imports += 1 }
    func shouldFailInspection() -> Bool {
        inspections += 1
        return inspections == 1
    }
}

import XCTest
@testable import Watermelon

// Pins the write-side owner gate on the shared snapshot cache: a browser action's cache write-back (or a
// backup run's flush upsert) prepared for profile A must be dropped once a cross-profile connect re-tagged
// the cache for B — and a stale run's preflight sync must refuse to steal the cache back.
final class RemoteSnapshotOwnershipGateTests: XCTestCase {
    private let monthKey = LibraryMonthKey(year: 2024, month: 5)
    private let fingerprint = Data([0xCC, 0x02])

    private func monthData(_ hash: Data = Data([0x01])) -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        (
            [TestFixtures.remoteResource(year: 2024, month: 5, contentHash: hash, fileSize: 10, fileName: "a.jpg")],
            [TestFixtures.remoteAsset(year: 2024, month: 5, fingerprint: fingerprint)],
            [TestFixtures.remoteLink(year: 2024, month: 5, assetFingerprint: fingerprint, resourceHash: hash)]
        )
    }

    private func profile(_ name: String) -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil, name: name, storageType: StorageType.s3.rawValue, connectionParams: nil, sortOrder: 0,
            host: "host.local", port: 0, shareName: "share", basePath: "/\(name)", username: "u",
            domain: nil, credentialRef: "ref", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date(), writerID: nil
        )
    }

    func testForeignOwnedWritesAreDropped() {
        let cache = RemoteLibrarySnapshotCache()
        cache.setProfileKey("profile-b")
        let data = monthData()

        XCTAssertFalse(cache.replaceMonth(monthKey, resources: data.resources, assets: data.assets, assetResourceLinks: data.links, onlyIfOwnedBy: "profile-a"))
        cache.upsertResource(data.resources[0], onlyIfOwnedBy: "profile-a")
        cache.upsertAsset(data.assets[0], links: data.links, onlyIfOwnedBy: "profile-a")
        XCTAssertTrue(cache.allKnownMonths().isEmpty, "a foreign-owner write must not land in B's cache")
        XCTAssertFalse(cache.resetIfOwned(by: "profile-a"))
        XCTAssertEqual(cache.currentProfileKey(), "profile-b")
    }

    func testOwnedWritesApplyAndNilTagFailsClosed() {
        let cache = RemoteLibrarySnapshotCache()
        let data = monthData()
        // Untagged (mid-reset window): an owner-expecting write fails closed.
        XCTAssertFalse(cache.replaceMonth(monthKey, resources: data.resources, assets: data.assets, assetResourceLinks: data.links, onlyIfOwnedBy: "profile-a"))

        cache.setProfileKey("profile-a")
        XCTAssertTrue(cache.replaceMonth(monthKey, resources: data.resources, assets: data.assets, assetResourceLinks: data.links, onlyIfOwnedBy: "profile-a"))
        XCTAssertTrue(cache.containsAssetFingerprint(fingerprint).contains)
        XCTAssertTrue(cache.resetIfOwned(by: "profile-a"))
        XCTAssertTrue(cache.allKnownMonths().isEmpty)
    }

    func testDeleteWritebackLandingAfterRetagIsDropped() async {
        // The browser delete's write-back queues behind a connect sync on the FIFO gate; by the time it
        // applies, the cache belongs to B — A's post-delete month must be dropped, B's view untouched.
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        let dataB = monthData(Data([0xB1]))
        cache.setProfileKey("profile-b")
        cache.replaceMonth(monthKey, resources: dataB.resources, assets: dataB.assets, assetResourceLinks: dataB.links)

        let dataA = monthData(Data([0xA1]))
        await service.replaceCachedMonthSynchronized(monthKey, resources: dataA.resources, assets: dataA.assets, links: dataA.links, expectedProfileKey: "profile-a")
        XCTAssertEqual(cache.fileNames(for: monthKey), ["a.jpg"])
        XCTAssertEqual(cache.monthRawData(for: monthKey)?.resources.first?.contentHash, Data([0xB1]), "B's month must survive A's late write-back")

        await service.resetSnapshotCache(expectedProfileKey: "profile-a")
        XCTAssertFalse(cache.allKnownMonths().isEmpty, "A's repo-gone reset must not wipe B's cache")
        XCTAssertEqual(cache.currentProfileKey(), "profile-b")

        await service.resetSnapshotCache(expectedProfileKey: "profile-b")
        XCTAssertTrue(cache.allKnownMonths().isEmpty)
    }

    func testPreflightSyncRefusesToStealForeignContext() async throws {
        let cache = RemoteLibrarySnapshotCache()
        let service = RemoteIndexSyncService(snapshotCache: cache)
        let dataB = monthData(Data([0xB1]))
        cache.setProfileKey("profile-b-key")
        cache.replaceMonth(monthKey, resources: dataB.resources, assets: dataB.assets, assetResourceLinks: dataB.links)
        let ownerKey = cache.currentProfileKey()

        do {
            _ = try await service.syncIndex(client: EmptyRemoteClient(), profile: profile("a"), layout: .lite, contextPolicy: .claimIfUnowned)
            XCTFail("a preflight sync must not take over a cache another profile owns")
        } catch {
            XCTAssertEqual((error as NSError).domain, "RemoteIndexSyncService")
            XCTAssertEqual((error as NSError).code, -40)
        }
        XCTAssertEqual(cache.currentProfileKey(), ownerKey, "the refused sync must leave the owner untouched")
        XCTAssertFalse(cache.allKnownMonths().isEmpty, "the refused sync must leave the owner's months untouched")

        // The connect/reload path (.claimAlways) still takes the cache over — that IS the profile switch.
        _ = try await service.syncIndex(client: EmptyRemoteClient(), profile: profile("a"), layout: .lite)
        XCTAssertEqual(cache.currentProfileKey(), RemoteIndexSyncService.remoteProfileKey(profile("a")))
    }
}

// Minimal fake remote with no months (list of a missing months directory reports not-found).
private final class EmptyRemoteClient: RemoteStorageClientProtocol, @unchecked Sendable {
    func list(path: String) async throws -> [RemoteStorageEntry] { throw RemoteErrorFixtures.notFound }
    func connect() async throws {}
    func disconnect() async {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { nil }
    func metadata(path: String) async throws -> RemoteStorageEntry? { nil }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {}
    func setModificationDate(_ date: Date, forPath path: String) async throws {}
    func download(remotePath: String, localURL: URL) async throws {}
    func exists(path: String) async throws -> Bool { false }
    func delete(path: String) async throws {}
    func createDirectory(path: String) async throws {}
    func move(from sourcePath: String, to destinationPath: String) async throws {}
    func copy(from sourcePath: String, to destinationPath: String) async throws {}
}

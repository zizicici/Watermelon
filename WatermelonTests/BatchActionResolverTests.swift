import XCTest
@testable import Watermelon

final class BatchActionResolverTests: XCTestCase {
    private let month = LibraryMonthKey(year: 2024, month: 1)

    private func item(id: String, presence: MediaPresence, local: String?, fp: Data?) -> MediaBrowserItem {
        if presence == .localOnly {
            return MediaBrowserItem(
                kind: .photo,
                creationDateMs: 1_700_000_000_000,
                localIdentifier: local!,
                fingerprint: fp,
                isBackedUp: false
            )
        }
        return MediaBrowserItem(
            kind: .photo,
            creationDateMs: 1_700_000_000_000,
            localIdentifier: local,
            remote: RemoteMediaReference(
                fingerprint: fp!,
                photoRelativePath: "2024-1/\(id).jpg",
                videoRelativePath: nil,
                photoContentHash: nil,
                videoContentHash: nil,
                storageMonth: month,
                isIncomplete: false
            )
        )
    }

    private func localOnly(_ id: String) -> MediaBrowserItem { item(id: id, presence: .localOnly, local: id, fp: nil) }
    private func remoteOnly(_ id: String) -> MediaBrowserItem { item(id: id, presence: .remoteOnly, local: nil, fp: Data([UInt8(id.count)])) }
    private func both(_ id: String) -> MediaBrowserItem { item(id: id, presence: .both, local: id, fp: Data([200, UInt8(id.count)])) }

    func testEmptySelectionOffersNothing() {
        let r = BatchActionResolver.resolve([])
        XCTAssertEqual(r, BatchActionResolver.Result(showsUpload: false, showsDownload: false, deviceCount: 0, remoteCount: 0))
        XCTAssertFalse(r.showsDelete)
    }

    func testLazySelectionIsResolvedOnceAndIgnoresRemovedItems() {
        let local = localOnly("local")
        let backedUp = MediaBrowserItem(kind: .photo, creationDateMs: 0,
            localIdentifier: "backed-up", fingerprint: Data([1]), isBackedUp: true)
        let remote = remoteOnly("remote")
        let snapshot = MediaBrowserSnapshot(sections: [
            .init(month: month, items: [local, backedUp, remote]),
        ])
        let selectedIDs: Set<MediaBrowserItemID> = [local.id, backedUp.id, remote.id, .local("removed")]
        var lookups = 0
        let items = selectedIDs.lazy.compactMap { id -> MediaBrowserItem? in
            lookups += 1
            return snapshot.item(id: id)
        }

        let result = BatchActionResolver.resolve(items)

        XCTAssertEqual(lookups, selectedIDs.count)
        XCTAssertEqual(result, .init(showsUpload: false, showsDownload: false, deviceCount: 2, remoteCount: 2))
        XCTAssertFalse(BatchActionResolver.resolve([MediaBrowserItemID.local("removed")].lazy.compactMap {
            snapshot.item(id: $0)
        }).showsDelete)
    }

    func testAllLocalOnlyOffersUploadAndDeviceDelete() {
        let r = BatchActionResolver.resolve([localOnly("a"), localOnly("b")])
        XCTAssertTrue(r.showsUpload)
        XCTAssertFalse(r.showsDownload)
        XCTAssertTrue(r.showsDelete)
        XCTAssertEqual(r.deviceCount, 2)
        XCTAssertEqual(r.remoteCount, 0, "local-only items are not on the backup")
    }

    func testAllRemoteOnlyOffersDownloadAndBackupDelete() {
        let r = BatchActionResolver.resolve([remoteOnly("a"), remoteOnly("bb")])
        XCTAssertFalse(r.showsUpload)
        XCTAssertTrue(r.showsDownload)
        XCTAssertTrue(r.showsDelete)
        XCTAssertEqual(r.deviceCount, 0, "remote-only items have no device handle")
        XCTAssertEqual(r.remoteCount, 2)
    }

    func testMixedLocalAndRemoteOffersNeitherUploadNorDownload() {
        // The key rule: a mix must never offer upload or download (no "complement"); only delete remains.
        let r = BatchActionResolver.resolve([localOnly("a"), remoteOnly("bb")])
        XCTAssertFalse(r.showsUpload)
        XCTAssertFalse(r.showsDownload)
        XCTAssertTrue(r.showsDelete)
        XCTAssertEqual(r.deviceCount, 1, "the one local-only item")
        XCTAssertEqual(r.remoteCount, 1, "the one remote-only item")
    }

    func testAllBothOffersNeitherUploadNorDownloadButAllowsEitherDeleteTarget() {
        let r = BatchActionResolver.resolve([both("x"), both("yy")])
        XCTAssertFalse(r.showsUpload)
        XCTAssertFalse(r.showsDownload)
        XCTAssertTrue(r.showsDelete)
        XCTAssertEqual(r.deviceCount, 2, "both-items have a device handle")
        XCTAssertEqual(r.remoteCount, 2, "…and are on the backup")
        XCTAssertEqual(r.deleteTargets, [.local, .remote, .all])
    }

    func testDeleteTargetsAndItemsStayOnTheChosenSide() {
        let local = localOnly("local")
        let remote = remoteOnly("remote")
        let shared = both("shared")
        let backedUpLocal = MediaBrowserItem(kind: .photo, creationDateMs: 0,
            localIdentifier: "backed-up-local", fingerprint: Data([4]), isBackedUp: true)
        let items = [local, remote, shared, backedUpLocal]

        XCTAssertEqual(BatchActionResolver.resolve([]).deleteTargets, [])
        XCTAssertEqual(BatchActionResolver.resolve([local]).deleteTargets, [.local])
        XCTAssertEqual(BatchActionResolver.resolve([local, backedUpLocal]).deleteTargets, [.local, .remote, .all])
        XCTAssertEqual(BatchActionResolver.resolve([remote]).deleteTargets, [.remote])
        XCTAssertEqual(BatchActionResolver.resolve(items).deleteTargets, [.local, .remote, .all])
        XCTAssertEqual(MediaBrowserDeleteTarget.local.items(in: items), [local, shared, backedUpLocal])
        XCTAssertEqual(MediaBrowserDeleteTarget.remote.items(in: items), [remote, shared, backedUpLocal])
        XCTAssertEqual(MediaBrowserDeleteTarget.all.items(in: items), items)
    }

    func testDeleteBreakdownCountsBothItemsOnBothSides() {
        // 1 local-only + 1 remote-only + 1 both → device = local + both = 2; backup = remote + both = 2.
        let r = BatchActionResolver.resolve([localOnly("a"), remoteOnly("bb"), both("ccc")])
        XCTAssertFalse(r.showsUpload)
        XCTAssertFalse(r.showsDownload)
        XCTAssertEqual(r.deviceCount, 2)
        XCTAssertEqual(r.remoteCount, 2)
    }

    func testLocalTabBackedUpItemOffersBothDeletionTargets() {
        let item = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 0,
            localIdentifier: "L1",
            fingerprint: Data([1]),
            isBackedUp: true
        )
        XCTAssertTrue(item.isDeviceDeletable)
        XCTAssertTrue(item.isRemoteDeletable)
        XCTAssertNil(item.remoteMonth)
        let r = BatchActionResolver.resolve([item])
        XCTAssertEqual(r.deviceCount, 1)
        XCTAssertEqual(r.remoteCount, 1)
        XCTAssertEqual(r.deleteTargets, [.local, .remote, .all])
        XCTAssertFalse(r.showsUpload, "already backed up → not local-only")
        XCTAssertFalse(r.showsDownload, "already on device")
        XCTAssertTrue(r.showsDelete)
    }

    func testLocalAndUnifiedScopesAllowExplicitRemoteDeletionWhileTransferStaysLocal() {
        let backedUpLocal = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 0,
            localIdentifier: "L1",
            remote: RemoteMediaReference(
                fingerprint: Data([1]),
                photoRelativePath: "2024/01/a.jpg",
                videoRelativePath: nil,
                photoContentHash: nil,
                videoContentHash: nil,
                storageMonth: month,
                isIncomplete: false
            )
        )
        XCTAssertEqual(
            MediaBrowserActionPolicy.actions(
                for: backedUpLocal,
                scope: .local
            ),
            [.share, .deleteLocal, .deleteRemote]
        )
        XCTAssertEqual(
            MediaBrowserActionPolicy.actions(
                for: backedUpLocal,
                scope: .unified
            ),
            [.share, .deleteLocal, .deleteRemote]
        )
        XCTAssertEqual(
            MediaBrowserActionPolicy.actions(
                for: localOnly("transfer"),
                scope: .transfer
            ),
            [.share, .deleteLocal]
        )
        XCTAssertEqual(
            MediaBrowserActionPolicy.actions(
                for: remoteOnly("transfer"),
                scope: .transfer
            ),
            []
        )
    }

    func testLocalRemoteDeletionUsesMatchingStorageMonthsAndChecksSnapshotOwnership() throws {
        let fingerprint = Data([42])
        let january = LibraryMonthKey(year: 2024, month: 1)
        let february = LibraryMonthKey(year: 2024, month: 2)
        let local = MediaBrowserItem(kind: .photo, creationDateMs: 0, localIdentifier: "both",
            fingerprint: fingerprint, isBackedUp: true)
        let remote = MediaBrowserItem(kind: .photo, creationDateMs: 0, localIdentifier: nil,
            remote: RemoteMediaReference(fingerprint: fingerprint, photoRelativePath: "2024/02/photo.jpg",
                videoRelativePath: nil, photoContentHash: nil, videoContentHash: nil,
                storageMonth: february, isIncomplete: false))
        let localOnly = self.localOnly("local-only")
        var state = RemoteLibrarySnapshotState(revision: 1, isFullSnapshot: true, monthDeltas: [
            .init(month: february, resources: [], assets: [], assetResourceLinks: [
                TestFixtures.remoteLink(year: 2024, month: 2, assetFingerprint: fingerprint, resourceHash: Data([9])),
            ]),
            .init(month: january, resources: [], assets: [
                TestFixtures.remoteAsset(year: 2024, month: 1, fingerprint: fingerprint),
            ], assetResourceLinks: []),
        ], profileKey: "owner")
        let items = [local, remote, localOnly]
        let plan = try XCTUnwrap(MediaBrowserActionRunner.remoteDeletionPlan(for: items, snapshot: state, expectedProfileKey: "owner"))
        XCTAssertEqual(plan[local.id], [january, february])
        XCTAssertEqual(plan[remote.id], [february])
        XCTAssertNil(plan[localOnly.id])
        XCTAssertNil(MediaBrowserActionRunner.remoteDeletionPlan(for: items, snapshot: state, expectedProfileKey: "another-node"))
        state.isAuthoritative = false
        XCTAssertNil(MediaBrowserActionRunner.remoteDeletionPlan(for: items, snapshot: state, expectedProfileKey: "owner"))
    }

}

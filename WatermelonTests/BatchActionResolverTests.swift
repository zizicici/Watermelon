import XCTest
@testable import Watermelon

// The grid multi-select toolbar is driven entirely by BatchActionResolver, so its rules live here (user-specified):
// Upload only when EVERY item is local-only; Download only when EVERY item is remote-only; a mixed selection offers
// neither (no download-then-upload "complement"). Delete is always available and reports a from-backup / from-device
// breakdown, counting a "both" item on both sides.
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

    func testAllBothOffersNeitherUploadNorDownloadButDeletesEverywhere() {
        // A "both" item is neither local-only nor remote-only → no upload/download; delete counts it on BOTH sides.
        let r = BatchActionResolver.resolve([both("x"), both("yy")])
        XCTAssertFalse(r.showsUpload)
        XCTAssertFalse(r.showsDownload)
        XCTAssertTrue(r.showsDelete)
        XCTAssertEqual(r.deviceCount, 2, "both-items have a device handle")
        XCTAssertEqual(r.remoteCount, 2, "…and are on the backup")
    }

    func testDeleteBreakdownCountsBothItemsOnBothSides() {
        // 1 local-only + 1 remote-only + 1 both → device = local + both = 2; backup = remote + both = 2.
        let r = BatchActionResolver.resolve([localOnly("a"), remoteOnly("bb"), both("ccc")])
        XCTAssertFalse(r.showsUpload)
        XCTAssertFalse(r.showsDownload)
        XCTAssertEqual(r.deviceCount, 2)
        XCTAssertEqual(r.remoteCount, 2)
    }

    // MARK: - Deletability predicates (the Local-tab safety invariant)

    func testLocalTabBackedUpItemIsNotRemoteDeletable() {
        // A backed-up on-device item shown in the LOCAL tab is .both with a fingerprint but NO remote month
        // (LocalMediaSource carries none). It must be device-deletable but NOT backup-deletable — a delete in the
        // on-device view must never silently remove the cloud backup. This is the safety-critical invariant.
        let item = MediaBrowserItem(
            kind: .photo,
            creationDateMs: 0,
            localIdentifier: "L1",
            fingerprint: Data([1]),
            isBackedUp: true
        )
        XCTAssertTrue(item.isDeviceDeletable)
        XCTAssertFalse(item.isRemoteDeletable, "no remote month → cannot (and must not) delete from the backup")
        let r = BatchActionResolver.resolve([item])
        XCTAssertEqual(r.deviceCount, 1)
        XCTAssertEqual(r.remoteCount, 0, "a Local-tab backed-up item is device-only")
        XCTAssertFalse(r.showsUpload, "already backed up → not local-only")
        XCTAssertFalse(r.showsDownload, "already on device")
        XCTAssertTrue(r.showsDelete)
    }

    func testSharedActionPolicyKeepsLocalAndUnifiedScopesDistinct() {
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
            [.share, .deleteLocal]
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

}

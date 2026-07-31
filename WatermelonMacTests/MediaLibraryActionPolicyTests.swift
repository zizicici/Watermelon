import XCTest
@testable import WatermelonMac

final class MediaLibraryActionPolicyTests: XCTestCase {
    func testSingleItemActionsMatchLocalAndUnifiedBrowserRules() {
        XCTAssertEqual(
            MediaLibraryActionPolicy.actions(
                for: .localOnly,
                scope: .local
            ),
            [.share, .upload, .deleteLocal]
        )
        XCTAssertEqual(
            MediaLibraryActionPolicy.actions(
                for: .both,
                scope: .local
            ),
            [.share, .deleteLocal]
        )
        XCTAssertEqual(
            MediaLibraryActionPolicy.actions(
                for: .remoteOnly,
                scope: .local
            ),
            []
        )
        XCTAssertEqual(
            MediaLibraryActionPolicy.actions(
                for: .remoteOnly,
                scope: .unified
            ),
            [.share, .download, .deleteRemote]
        )
        XCTAssertEqual(
            MediaLibraryActionPolicy.actions(
                for: .both,
                scope: .unified
            ),
            [.share, .deleteLocal, .deleteRemote]
        )
    }

    func testBatchActionsRequireUniformPresence() {
        let local = MediaLibraryBatchItem(
            presence: .localOnly,
            canDeleteLocal: true,
            canDeleteRemote: false
        )
        let remote = MediaLibraryBatchItem(
            presence: .remoteOnly,
            canDeleteLocal: false,
            canDeleteRemote: true
        )

        let localSummary = MediaLibraryActionPolicy.batchSummary(
            for: [local, local]
        )
        XCTAssertTrue(localSummary.showsUpload)
        XCTAssertFalse(localSummary.showsDownload)
        XCTAssertEqual(localSummary.localDeleteCount, 2)
        XCTAssertEqual(localSummary.remoteDeleteCount, 0)

        let remoteSummary = MediaLibraryActionPolicy.batchSummary(
            for: [remote, remote]
        )
        XCTAssertFalse(remoteSummary.showsUpload)
        XCTAssertTrue(remoteSummary.showsDownload)
        XCTAssertEqual(remoteSummary.localDeleteCount, 0)
        XCTAssertEqual(remoteSummary.remoteDeleteCount, 2)

        let mixedSummary = MediaLibraryActionPolicy.batchSummary(
            for: [local, remote]
        )
        XCTAssertFalse(mixedSummary.showsUpload)
        XCTAssertFalse(mixedSummary.showsDownload)
        XCTAssertEqual(mixedSummary.localDeleteCount, 1)
        XCTAssertEqual(mixedSummary.remoteDeleteCount, 1)
    }

    func testBatchDeleteCanKeepRemoteCopyOutOfLocalScope() {
        let summary = MediaLibraryActionPolicy.batchSummary(
            for: [
                MediaLibraryBatchItem(
                    presence: .both,
                    canDeleteLocal: true,
                    canDeleteRemote: false
                )
            ]
        )

        XCTAssertFalse(summary.showsUpload)
        XCTAssertFalse(summary.showsDownload)
        XCTAssertEqual(summary.localDeleteCount, 1)
        XCTAssertEqual(summary.remoteDeleteCount, 0)
        XCTAssertTrue(summary.showsDelete)
    }
}

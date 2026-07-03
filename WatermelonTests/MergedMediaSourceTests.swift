import XCTest
@testable import Watermelon

final class MergedMediaSourceTests: XCTestCase {
    private var utcCalendar: Calendar {
        var c = Calendar(identifier: .gregorian)
        c.timeZone = TimeZone(identifier: "UTC")!
        return c
    }

    private func item(id: String, fp: Data?, local: String?, ms: Int64, presence: MediaPresence, incomplete: Bool = false) -> MediaBrowserItem {
        MediaBrowserItem(
            id: id,
            kind: .photo,
            creationDateMs: ms,
            presence: presence,
            localIdentifier: local,
            fingerprint: fp,
            photoRemoteRelativePath: local == nil ? "2024/01/x.jpg" : nil,
            videoRemoteRelativePath: nil,
            remoteMonth: nil,
            isIncomplete: incomplete
        )
    }

    func testIncompleteButMeaningfulRemoteDedupsLocalTwin() {
        // Every shown remote is a real backup (the builder drops config-only/phantom upstream), even when flagged
        // incomplete. So the remote is authoritative: its local twin dedups away, and the kept item keeps the badge.
        let fp = Data([7, 7, 7])
        let ms: Int64 = 1_700_000_000_000
        let remoteIncomplete = item(id: "fpBad", fp: fp, local: nil, ms: ms, presence: .remoteOnly, incomplete: true)
        let localTwin = item(id: "L7", fp: fp, local: "L7", ms: ms, presence: .localOnly)
        let all = MergedMediaSource.merge(remoteItems: [remoteIncomplete], localItems: [localTwin], calendar: utcCalendar).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpBad"], "the backed-up remote is authoritative; its local twin dedups away")
        XCTAssertTrue(all.first?.isIncomplete == true, "the kept remote item still carries the incomplete badge")
    }

    func testIncompleteRemoteWithoutLocalTwinIsShown() {
        let fp = Data([8, 8, 8])
        let ms: Int64 = 1_700_000_000_000
        let remoteIncomplete = item(id: "fpLone", fp: fp, local: nil, ms: ms, presence: .remoteOnly, incomplete: true)
        let all = MergedMediaSource.merge(remoteItems: [remoteIncomplete], localItems: [], calendar: utcCalendar).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpLone"], "an incomplete remote with no local copy is still shown (marked)")
    }

    func testLocalDuplicateOfRemoteIsDeduped() {
        let fp = Data([1, 2, 3])
        let ms: Int64 = 1_700_000_000_000
        let remote = item(id: "fpA", fp: fp, local: "L1", ms: ms, presence: .both)
        let local = item(id: "L1", fp: fp, local: "L1", ms: ms, presence: .both)
        let all = MergedMediaSource.merge(remoteItems: [remote], localItems: [local], calendar: utcCalendar).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpA"], "the local duplicate should collapse into the remote item")
    }

    func testMergeGraftsLocalHandleOntoHandlelessRemoteTwin() {
        // Safety net for a transiently-stale shared index: the remote source built a handle-less item before the
        // presence index knew this fingerprint is on device, but the local source (reading the repo live) sees it.
        // Merge grafts the live handle so the deduped item is `.both` (no Download) instead of a `.remoteOnly`
        // that would re-import an on-device asset.
        let fp = Data([4, 5, 6])
        let ms: Int64 = 1_700_000_000_000
        let remote = item(id: "fpR", fp: fp, local: nil, ms: ms, presence: .remoteOnly)
        let localTwin = item(id: "L9", fp: fp, local: "L9", ms: ms, presence: .localOnly)
        let all = MergedMediaSource.merge(remoteItems: [remote], localItems: [localTwin], calendar: utcCalendar).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpR"], "the local twin dedups into the remote item")
        XCTAssertEqual(all.first?.localIdentifier, "L9", "merge grafts the live local handle when the remote lacks one")
        XCTAssertEqual(all.first?.presence, .both, "grafted → .both, so no Download is offered for an on-device asset")
    }

    func testLocalOnlyAndRemoteOnlyBothKept() {
        let ms: Int64 = 1_700_000_000_000
        let remote = item(id: "fpR", fp: Data([1]), local: nil, ms: ms, presence: .remoteOnly)
        let localOnly = item(id: "L2", fp: Data([9]), local: "L2", ms: ms, presence: .localOnly)
        let all = MergedMediaSource.merge(remoteItems: [remote], localItems: [localOnly], calendar: utcCalendar).flatMap { $0.items }
        XCTAssertEqual(Set(all.map { $0.id }), ["fpR", "L2"])
    }

    func testLocalWithoutFingerprintIsKept() {
        let ms: Int64 = 1_700_000_000_000
        let noFp = item(id: "L3", fp: nil, local: "L3", ms: ms, presence: .localOnly)
        let all = MergedMediaSource.merge(remoteItems: [], localItems: [noFp], calendar: utcCalendar).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["L3"])
    }

    func testGroupsByMonthNewestFirst() {
        let jan = Int64(1_704_067_200_000)      // 2024-01-01 UTC
        let janLater = Int64(1_704_153_600_000) // 2024-01-02 UTC
        let feb = Int64(1_706_745_600_000)      // 2024-02-01 UTC
        let a = item(id: "A", fp: Data([1]), local: nil, ms: jan, presence: .remoteOnly)
        let b = item(id: "B", fp: Data([2]), local: nil, ms: feb, presence: .remoteOnly)
        let c = item(id: "C", fp: Data([3]), local: nil, ms: janLater, presence: .remoteOnly)
        let sections = MergedMediaSource.merge(remoteItems: [a, b, c], localItems: [], calendar: utcCalendar)
        XCTAssertEqual(sections.count, 2)
        XCTAssertEqual(sections.first?.items.map { $0.id }, ["B"], "newest month first")
        XCTAssertEqual(sections.last?.items.map { $0.id }, ["C", "A"], "within a month, newest first")
    }
}

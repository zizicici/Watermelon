import XCTest
@testable import Watermelon

final class MergedMediaSourceTests: XCTestCase {
    private var utcCalendar: Calendar {
        var c = Calendar(identifier: .gregorian)
        c.timeZone = TimeZone(identifier: "UTC")!
        return c
    }

    private func item(id: String, fp: Data?, local: String?, ms: Int64, presence: MediaPresence) -> MediaBrowserItem {
        MediaBrowserItem(
            id: id,
            kind: .photo,
            creationDateMs: ms,
            presence: presence,
            localIdentifier: local,
            fingerprint: fp,
            photoRemoteRelativePath: local == nil ? "2024/01/x.jpg" : nil,
            videoRemoteRelativePath: nil
        )
    }

    func testLocalDuplicateOfRemoteIsDeduped() {
        let fp = Data([1, 2, 3])
        let ms: Int64 = 1_700_000_000_000
        let remote = item(id: "fpA", fp: fp, local: "L1", ms: ms, presence: .both)
        let local = item(id: "L1", fp: fp, local: "L1", ms: ms, presence: .both)
        let all = MergedMediaSource.merge(remoteItems: [remote], localItems: [local], calendar: utcCalendar).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpA"], "the local duplicate should collapse into the remote item")
    }

    func testRemoteTwinWithoutHandleGraftsLocalIdentifierAndPromotesToBoth() {
        // The remote item arrived handle-less (the two fingerprint reads disagreed), but a local twin with
        // the same fingerprint is on device. Merge must graft the handle and promote presence to `.both`.
        let fp = Data([4, 5, 6])
        let ms: Int64 = 1_700_000_000_000
        let remote = item(id: "fpR", fp: fp, local: nil, ms: ms, presence: .remoteOnly)
        let localTwin = item(id: "L9", fp: fp, local: "L9", ms: ms, presence: .localOnly)
        let all = MergedMediaSource.merge(remoteItems: [remote], localItems: [localTwin], calendar: utcCalendar).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpR"], "the local twin should collapse into the remote item")
        XCTAssertEqual(all.first?.localIdentifier, "L9", "the on-device handle should be grafted on")
        XCTAssertEqual(all.first?.presence, .both, "presence should be promoted to both")
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

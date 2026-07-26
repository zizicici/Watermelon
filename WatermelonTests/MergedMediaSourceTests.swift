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

    private func sourceSections(_ items: [MediaBrowserItem]) -> [MediaBrowserSection] {
        var byMonth: [LibraryMonthKey: [MediaBrowserItem]] = [:]
        for item in items {
            let date = Date(timeIntervalSince1970: Double(item.creationDateMs) / 1_000)
            byMonth[LibraryMonthKey.from(date: date, calendar: utcCalendar), default: []].append(item)
        }
        return byMonth.keys.sorted(by: >).map { month in
            MediaBrowserSection(
                month: month,
                items: (byMonth[month] ?? []).sorted { $0.creationDateMs > $1.creationDateMs }
            )
        }
    }

    private func merge(
        remoteItems: [MediaBrowserItem],
        localItems: [MediaBrowserItem]
    ) -> [MediaBrowserSection] {
        MergedMediaSource.merge(
            remoteSections: sourceSections(remoteItems),
            localSections: sourceSections(localItems),
            calendar: utcCalendar
        )
    }

    func testIncompleteButMeaningfulRemoteDedupsLocalTwin() {
        // Every shown remote is a real backup (the builder drops config-only/phantom upstream), even when flagged
        // incomplete. So the remote is authoritative: its local twin dedups away, and the kept item keeps the badge.
        let fp = Data([7, 7, 7])
        let ms: Int64 = 1_700_000_000_000
        let remoteIncomplete = item(id: "fpBad", fp: fp, local: nil, ms: ms, presence: .remoteOnly, incomplete: true)
        let localTwin = item(id: "L7", fp: fp, local: "L7", ms: ms, presence: .localOnly)
        let all = merge(remoteItems: [remoteIncomplete], localItems: [localTwin]).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpBad"], "the backed-up remote is authoritative; its local twin dedups away")
        XCTAssertTrue(all.first?.isIncomplete == true, "the kept remote item still carries the incomplete badge")
    }

    func testIncompleteRemoteWithoutLocalTwinIsShown() {
        let fp = Data([8, 8, 8])
        let ms: Int64 = 1_700_000_000_000
        let remoteIncomplete = item(id: "fpLone", fp: fp, local: nil, ms: ms, presence: .remoteOnly, incomplete: true)
        let all = merge(remoteItems: [remoteIncomplete], localItems: []).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpLone"], "an incomplete remote with no local copy is still shown (marked)")
    }

    func testLocalDuplicateOfRemoteIsDeduped() {
        let fp = Data([1, 2, 3])
        let ms: Int64 = 1_700_000_000_000
        let remote = item(id: "fpA", fp: fp, local: "L1", ms: ms, presence: .both)
        let local = item(id: "L1", fp: fp, local: "L1", ms: ms, presence: .both)
        let all = merge(remoteItems: [remote], localItems: [local]).flatMap { $0.items }
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
        let all = merge(remoteItems: [remote], localItems: [localTwin]).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["fpR"], "the local twin dedups into the remote item")
        XCTAssertEqual(all.first?.localIdentifier, "L9", "merge grafts the live local handle when the remote lacks one")
        XCTAssertEqual(all.first?.presence, .both, "grafted → .both, so no Download is offered for an on-device asset")
    }

    func testLocalOnlyAndRemoteOnlyBothKept() {
        let ms: Int64 = 1_700_000_000_000
        let remote = item(id: "fpR", fp: Data([1]), local: nil, ms: ms, presence: .remoteOnly)
        let localOnly = item(id: "L2", fp: Data([9]), local: "L2", ms: ms, presence: .localOnly)
        let all = merge(remoteItems: [remote], localItems: [localOnly]).flatMap { $0.items }
        XCTAssertEqual(Set(all.map { $0.id }), ["fpR", "L2"])
    }

    func testLocalWithoutFingerprintIsKept() {
        let ms: Int64 = 1_700_000_000_000
        let noFp = item(id: "L3", fp: nil, local: "L3", ms: ms, presence: .localOnly)
        let all = merge(remoteItems: [], localItems: [noFp]).flatMap { $0.items }
        XCTAssertEqual(all.map { $0.id }, ["L3"])
    }

    func testGroupsByMonthNewestFirst() {
        let jan = Int64(1_704_067_200_000)      // 2024-01-01 UTC
        let janLater = Int64(1_704_153_600_000) // 2024-01-02 UTC
        let feb = Int64(1_706_745_600_000)      // 2024-02-01 UTC
        let a = item(id: "A", fp: Data([1]), local: nil, ms: jan, presence: .remoteOnly)
        let b = item(id: "B", fp: Data([2]), local: nil, ms: feb, presence: .remoteOnly)
        let c = item(id: "C", fp: Data([3]), local: nil, ms: janLater, presence: .remoteOnly)
        let sections = merge(remoteItems: [a, b, c], localItems: [])
        XCTAssertEqual(sections.count, 2)
        XCTAssertEqual(sections.first?.items.map { $0.id }, ["B"], "newest month first")
        XCTAssertEqual(sections.last?.items.map { $0.id }, ["C", "A"], "within a month, newest first")
    }

    func testMergeCalculatesMonthOncePerContiguousMonth() {
        let jan = Int64(1_704_067_200_000)
        let janLater = Int64(1_704_153_600_000)
        let feb = Int64(1_706_745_600_000)
        let items = [
            item(id: "B", fp: Data([2]), local: nil, ms: feb, presence: .remoteOnly),
            item(id: "C", fp: Data([3]), local: nil, ms: janLater, presence: .remoteOnly),
            item(id: "A", fp: Data([1]), local: nil, ms: jan, presence: .remoteOnly),
        ]
        var calculations = 0

        _ = MergedMediaSource.merge(
            remoteSections: sourceSections(items),
            localSections: [],
            calendar: utcCalendar,
            onMonthCalculationCount: { calculations = $0 }
        )

        XCTAssertEqual(calculations, 2)
    }

    func testSectionMergeRegroupsRemoteAndLinearlyMergesLocalItems() {
        let january = LibraryMonthKey(year: 2024, month: 1)
        let february = LibraryMonthKey(year: 2024, month: 2)
        let jan1 = Int64(1_704_067_200_000)
        let jan2 = Int64(1_704_153_600_000)
        let jan3 = Int64(1_704_240_000_000)
        let fingerprint = Data([0x01])
        let remote = item(
            id: "remote",
            fp: fingerprint,
            local: nil,
            ms: jan2,
            presence: .remoteOnly
        )
        let localTwin = item(
            id: "twin",
            fp: fingerprint,
            local: "twin",
            ms: jan2,
            presence: .localOnly
        )
        let localNew = item(
            id: "local-new",
            fp: nil,
            local: "local-new",
            ms: jan3,
            presence: .localOnly
        )
        let localOld = item(
            id: "local-old",
            fp: nil,
            local: "local-old",
            ms: jan1,
            presence: .localOnly
        )

        var usedFastPath = true
        let sections = MergedMediaSource.merge(
            remoteSections: [MediaBrowserSection(month: february, items: [remote])],
            localSections: [MediaBrowserSection(month: january, items: [localNew, localTwin, localOld])],
            calendar: utcCalendar,
            onFastPath: { usedFastPath = $0 }
        )

        XCTAssertEqual(sections.map(\.month), [january])
        XCTAssertEqual(sections[0].items.map(\.id), ["local-new", "remote", "local-old"])
        XCTAssertEqual(sections[0].items[1].localIdentifier, "twin")
        XCTAssertEqual(sections[0].items[1].presence, .both)
        XCTAssertFalse(usedFastPath)
    }

    func testSectionMergeFallsBackForMalformedInputSections() {
        let january = LibraryMonthKey(year: 2024, month: 1)
        let old = item(
            id: "old",
            fp: Data([0x01]),
            local: nil,
            ms: 1_704_067_200_000,
            presence: .remoteOnly
        )
        let new = item(
            id: "new",
            fp: Data([0x02]),
            local: nil,
            ms: 1_704_153_600_000,
            presence: .remoteOnly
        )
        var usedFastPath = true

        let sections = MergedMediaSource.merge(
            remoteSections: [MediaBrowserSection(month: january, items: [old, new])],
            localSections: [],
            calendar: utcCalendar,
            onFastPath: { usedFastPath = $0 }
        )

        XCTAssertEqual(sections[0].items.map(\.id), ["new", "old"])
        XCTAssertFalse(usedFastPath)
    }

    func testSectionMergeFallsBackWhenLocalSectionMonthIsStale() {
        let january = LibraryMonthKey(year: 2024, month: 1)
        let february = LibraryMonthKey(year: 2024, month: 2)
        let local = item(
            id: "local",
            fp: nil,
            local: "local",
            ms: 1_704_067_200_000,
            presence: .localOnly
        )
        var usedFastPath = true

        let sections = MergedMediaSource.merge(
            remoteSections: [],
            localSections: [MediaBrowserSection(month: february, items: [local])],
            calendar: utcCalendar,
            onFastPath: { usedFastPath = $0 }
        )

        XCTAssertFalse(usedFastPath)
        XCTAssertEqual(sections.map(\.month), [january])
        XCTAssertEqual(sections[0].items.map(\.id), ["local"])
    }

    func testSectionMergeReusesValidRemoteSections() {
        let january = LibraryMonthKey(year: 2024, month: 1)
        let february = LibraryMonthKey(year: 2024, month: 2)
        let jan1 = Int64(1_704_067_200_000)
        let jan2 = Int64(1_704_153_600_000)
        let jan3 = Int64(1_704_240_000_000)
        let feb1 = Int64(1_706_745_600_000)
        let fingerprint = Data([0x01])
        let remoteTwin = item(
            id: "remote-twin",
            fp: fingerprint,
            local: nil,
            ms: jan2,
            presence: .remoteOnly
        )
        let remoteFebruary = item(
            id: "remote-february",
            fp: Data([0x02]),
            local: nil,
            ms: feb1,
            presence: .remoteOnly
        )
        let localTwin = item(
            id: "local-twin",
            fp: fingerprint,
            local: "local-twin",
            ms: jan2,
            presence: .localOnly
        )
        let localNew = item(
            id: "local-new",
            fp: nil,
            local: "local-new",
            ms: jan3,
            presence: .localOnly
        )
        let localOld = item(
            id: "local-old",
            fp: nil,
            local: "local-old",
            ms: jan1,
            presence: .localOnly
        )
        var usedFastPath = false

        let sections = MergedMediaSource.merge(
            remoteSections: [
                MediaBrowserSection(month: february, items: [remoteFebruary]),
                MediaBrowserSection(month: january, items: [remoteTwin]),
            ],
            localSections: [
                MediaBrowserSection(month: january, items: [localNew, localTwin, localOld]),
            ],
            calendar: utcCalendar,
            onFastPath: { usedFastPath = $0 }
        )

        XCTAssertTrue(usedFastPath)
        XCTAssertEqual(sections.map(\.month), [february, january])
        XCTAssertEqual(sections[0].items.map(\.id), ["remote-february"])
        XCTAssertEqual(sections[1].items.map(\.id), ["local-new", "remote-twin", "local-old"])
        XCTAssertEqual(sections[1].items[1].localIdentifier, "local-twin")
        XCTAssertEqual(sections[1].items[1].presence, .both)
    }

    func testSectionMergeStopsWhenCancelled() {
        let january = LibraryMonthKey(year: 2024, month: 1)
        let items = (0 ..< 100).map { index in
            item(
                id: "\(index)",
                fp: Data([UInt8(index)]),
                local: "\(index)",
                ms: 1_704_067_200_000 - Int64(index),
                presence: .localOnly
            )
        }
        var checks = 0

        let sections = MergedMediaSource.merge(
            remoteSections: [],
            localSections: [MediaBrowserSection(month: january, items: items)],
            calendar: utcCalendar,
            shouldCancel: {
                checks += 1
                return checks > 20
            }
        )

        XCTAssertTrue(sections.isEmpty)
        XCTAssertGreaterThan(checks, 20)
    }

    func testFallbackStopsBeforeFinalSortWhenCancelled() {
        let january = LibraryMonthKey(year: 2024, month: 1)
        let februaryItem = item(
            id: "remote",
            fp: Data([1]),
            local: nil,
            ms: 1_706_745_600_000,
            presence: .remoteOnly
        )
        var checks = 0
        var usedFallback = false

        let sections = MergedMediaSource.merge(
            remoteSections: [MediaBrowserSection(month: january, items: [februaryItem])],
            localSections: [],
            calendar: utcCalendar,
            onFastPath: { usedFallback = !$0 },
            shouldCancel: {
                checks += 1
                return checks >= 6
            }
        )

        XCTAssertTrue(usedFallback)
        XCTAssertTrue(sections.isEmpty)
        XCTAssertEqual(checks, 6)
    }
}

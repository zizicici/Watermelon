import XCTest
@testable import Watermelon

final class MergedMediaSourceTests: XCTestCase {
    private let january = LibraryMonthKey(year: 2024, month: 1)
    private let february = LibraryMonthKey(year: 2024, month: 2)

    private func local(_ id: String, milliseconds: Int64) -> MediaBrowserItem {
        MediaBrowserItem(
            kind: .photo,
            creationDateMs: milliseconds,
            localIdentifier: id,
            fingerprint: nil,
            isBackedUp: false
        )
    }

    private func remote(_ id: UInt8, milliseconds: Int64) -> MediaBrowserItem {
        MediaBrowserItem(
            kind: .photo,
            creationDateMs: milliseconds,
            localIdentifier: nil,
            remote: RemoteMediaReference(
                fingerprint: Data([id]),
                photoRelativePath: "2024/01/\(id).jpg",
                videoRelativePath: nil,
                photoContentHash: nil,
                videoContentHash: nil,
                storageMonth: january,
                isIncomplete: false
            )
        )
    }

    func testPreparedMergeCombinesPreprojectedSectionsInDisplayOrder() {
        let remoteItem = remote(1, milliseconds: 2_000)
        let localNew = local("local-new", milliseconds: 3_000)
        let localOld = local("local-old", milliseconds: 1_000)

        let sections = MergedMediaSource.mergePrepared(
            remoteSections: [
                MediaBrowserSection(month: january, items: [remoteItem]),
            ],
            localOnlySections: [
                MediaBrowserSection(month: january, items: [localNew, localOld]),
            ]
        )

        XCTAssertEqual(sections.map(\.month), [january])
        XCTAssertEqual(
            sections[0].items.map(\.testLabel),
            ["local-new", "1", "local-old"]
        )
    }

    func testPreparedMergeKeepsDistinctDisplayMonths() {
        let sections = MergedMediaSource.mergePrepared(
            remoteSections: [
                MediaBrowserSection(
                    month: january,
                    items: [remote(1, milliseconds: 2_000)]
                ),
            ],
            localOnlySections: [
                MediaBrowserSection(
                    month: february,
                    items: [local("local", milliseconds: 1_000)]
                ),
            ]
        )

        XCTAssertEqual(sections.map(\.month), [february, january])
        XCTAssertEqual(
            sections.flatMap(\.items).map(\.testLabel),
            ["local", "1"]
        )
    }

    func testPreparedMergeUsesStableTieBreak() {
        let remoteOld = remote(2, milliseconds: 1_000)
        let remoteNew = remote(3, milliseconds: 3_000)
        let localTie = local("local", milliseconds: 3_000)

        let sections = MergedMediaSource.mergePrepared(
            remoteSections: [
                MediaBrowserSection(
                    month: january,
                    items: [remoteNew, remoteOld]
                ),
            ],
            localOnlySections: [
                MediaBrowserSection(month: january, items: [localTie]),
            ]
        )

        XCTAssertEqual(
            sections[0].items.map(\.creationDateMs),
            [3_000, 3_000, 1_000]
        )
        XCTAssertEqual(
            sections[0].items.map(\.id),
            [localTie.id, remoteNew.id, remoteOld.id]
        )
    }

    func testPreparedMergeStopsWhenCancelled() {
        let localItems = (0 ..< 100).map {
            local("\($0)", milliseconds: Int64(200 - ($0 * 2)))
        }
        let remoteItems = (0 ..< 100).map {
            remote(UInt8($0), milliseconds: Int64(199 - ($0 * 2)))
        }
        var checks = 0

        let sections = MergedMediaSource.mergePrepared(
            remoteSections: [
                MediaBrowserSection(month: january, items: remoteItems),
            ],
            localOnlySections: [
                MediaBrowserSection(month: january, items: localItems),
            ],
            shouldCancel: {
                checks += 1
                return checks > 20
            }
        )

        XCTAssertTrue(sections.isEmpty)
        XCTAssertGreaterThan(checks, 20)
    }
}

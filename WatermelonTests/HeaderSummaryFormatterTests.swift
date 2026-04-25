import XCTest
@testable import Watermelon

@MainActor
final class HeaderSummaryFormatterTests: XCTestCase {
    private func summary(asset: Int, photo: Int, video: Int, size: Int64?) -> HomeMonthSummary {
        HomeMonthSummary(
            month: LibraryMonthKey(year: 2024, month: 1),
            assetCount: asset,
            photoCount: photo,
            videoCount: video,
            backedUpCount: nil,
            totalSizeBytes: size
        )
    }

    private func row(localAsset: Int, photo: Int, video: Int, localSize: Int64?, remoteAsset: Int? = nil) -> HomeMonthRow {
        HomeMonthRow(
            month: LibraryMonthKey(year: 2024, month: 1),
            local: summary(asset: localAsset, photo: photo, video: video, size: localSize),
            remote: remoteAsset.map { summary(asset: $0, photo: $0, video: 0, size: 100) }
        )
    }

    func testAggregate_sumsPhotoVideoCount_acrossLocalSummaries() {
        let rows: [LibraryMonthKey: HomeMonthRow] = [
            LibraryMonthKey(year: 2024, month: 1): row(localAsset: 3, photo: 2, video: 1, localSize: 100),
            LibraryMonthKey(year: 2024, month: 2): row(localAsset: 5, photo: 4, video: 1, localSize: 200)
        ]
        let result = HomeHeaderSummaryFormatter.aggregate(rowLookup: rows, side: .local, treatsEmptyAsZero: false)
        XCTAssertEqual(result?.photoCount, 6)
        XCTAssertEqual(result?.videoCount, 2)
        XCTAssertEqual(result?.totalSizeBytes, 300)
    }

    func testAggregate_partialSizeCoverage_returnsNilTotal() {
        // Critical: a partial size value would show an undercount in the header,
        // misleading the user. The formatter must drop it entirely instead.
        let rows: [LibraryMonthKey: HomeMonthRow] = [
            LibraryMonthKey(year: 2024, month: 1): row(localAsset: 1, photo: 1, video: 0, localSize: 100),
            LibraryMonthKey(year: 2024, month: 2): row(localAsset: 1, photo: 1, video: 0, localSize: nil)
        ]
        let result = HomeHeaderSummaryFormatter.aggregate(rowLookup: rows, side: .local, treatsEmptyAsZero: false)
        XCTAssertNil(result?.totalSizeBytes)
        XCTAssertEqual(result?.photoCount, 2)
    }

    func testAggregate_emptyWithTreatsEmptyAsZero_returnsZeros() {
        let result = HomeHeaderSummaryFormatter.aggregate(rowLookup: [:], side: .local, treatsEmptyAsZero: true)
        XCTAssertEqual(result?.photoCount, 0)
        XCTAssertEqual(result?.videoCount, 0)
        XCTAssertEqual(result?.totalSizeBytes, 0)
    }

    func testAggregate_emptyWithoutTreatsEmptyAsZero_returnsNil() {
        let result = HomeHeaderSummaryFormatter.aggregate(rowLookup: [:], side: .local, treatsEmptyAsZero: false)
        XCTAssertNil(result)
    }

    func testAggregate_remoteSide_skipsRowsWithoutRemoteSummary() {
        let rows: [LibraryMonthKey: HomeMonthRow] = [
            LibraryMonthKey(year: 2024, month: 1): row(localAsset: 5, photo: 5, video: 0, localSize: 100),
            LibraryMonthKey(year: 2024, month: 2): row(localAsset: 3, photo: 3, video: 0, localSize: 200, remoteAsset: 2)
        ]
        let result = HomeHeaderSummaryFormatter.aggregate(rowLookup: rows, side: .remote, treatsEmptyAsZero: false)
        XCTAssertEqual(result?.photoCount, 2, "only the row with a remote summary contributes")
    }
}

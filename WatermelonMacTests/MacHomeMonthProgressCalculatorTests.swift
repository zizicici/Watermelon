import XCTest
@testable import WatermelonMac

final class MacHomeMonthProgressCalculatorTests: XCTestCase {
    private let month = LibraryMonthKey(year: 2026, month: 7)

    func testBasePercentUsesSharedIOSRules() {
        let row = makeRow(
            localCount: 10,
            remoteCount: 8,
            matchedCount: 4
        )

        XCTAssertEqual(percent(row, .backup), 40)
        XCTAssertEqual(percent(row, .download), 50)
        XCTAssertEqual(
            percent(row, .complement)!,
            4.0 / 14.0 * 100,
            accuracy: 0.000_001
        )
    }

    func testUploadProgressNeverMovesBehindReconciledBase() {
        let row = makeRow(
            localCount: 10,
            remoteCount: 2,
            matchedCount: 2
        )
        let progress = MacMonthExecutionProgress(
            uploadProcessedCount: 6,
            uploadTotalCount: 10,
            downloadFraction: nil
        )

        XCTAssertEqual(
            MacHomeMonthProgressCalculator.percent(
                row: row,
                intent: .backup,
                phase: .uploading,
                executionProgress: progress
            ),
            60
        )
    }

    func testDownloadProgressProjectsBothIntents() {
        let row = makeRow(
            localCount: 6,
            remoteCount: 10,
            matchedCount: 4
        )
        let progress = MacMonthExecutionProgress(
            downloadFraction: 0.5
        )

        XCTAssertEqual(
            MacHomeMonthProgressCalculator.percent(
                row: row,
                intent: .download,
                phase: .downloading,
                executionProgress: progress
            ),
            70
        )
        XCTAssertEqual(
            MacHomeMonthProgressCalculator.percent(
                row: row,
                intent: .complement,
                phase: .downloading,
                executionProgress: progress
            ),
            75
        )
    }

    func testCompletedMonthUsesReconciledBase() {
        let row = makeRow(
            localCount: 10,
            remoteCount: 9,
            matchedCount: 9
        )

        XCTAssertEqual(
            MacHomeMonthProgressCalculator.percent(
                row: row,
                intent: .backup,
                phase: .completed,
                executionProgress: nil
            ),
            90
        )
    }

    private func percent(
        _ row: HomeMonthRow,
        _ intent: MonthIntent
    ) -> Double? {
        MacHomeMonthProgressCalculator.percent(
            row: row,
            intent: intent,
            phase: nil,
            executionProgress: nil
        )
    }

    private func makeRow(
        localCount: Int,
        remoteCount: Int,
        matchedCount: Int
    ) -> HomeMonthRow {
        HomeMonthRow(
            month: month,
            local: HomeMonthSummary(
                month: month,
                assetCount: localCount,
                photoCount: localCount,
                videoCount: 0,
                backedUpCount: matchedCount,
                totalSizeBytes: nil
            ),
            remote: HomeMonthSummary(
                month: month,
                assetCount: remoteCount,
                photoCount: remoteCount,
                videoCount: 0,
                backedUpCount: nil,
                totalSizeBytes: nil
            )
        )
    }
}

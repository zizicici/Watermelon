import XCTest
@testable import WatermelonMac

final class RemoteSyncProgressPresentationTests: XCTestCase {
    func testIndexProgressUsesSharedHomeMessages() {
        XCTAssertEqual(
            RemoteSyncProgressPresentation.message(
                for: RemoteSyncProgress(
                    current: 0,
                    total: 0,
                    kind: .scanningRemoteIndex
                )
            ),
            String(localized: "home.overlay.scanningIndex")
        )
        XCTAssertEqual(
            RemoteSyncProgressPresentation.message(
                for: RemoteSyncProgress(
                    current: 2,
                    total: 5,
                    kind: .remoteIndex
                )
            ),
            String.localizedStringWithFormat(
                String(localized: "home.overlay.processingMonths"),
                2,
                5
            )
        )
    }

    func testUpgradeProgressUsesPhaseSpecificSharedMessages() {
        assertCountedUpgrade(
            phase: .copying,
            key: "home.overlay.upgradingRepoMonths"
        )
        assertCountedUpgrade(
            phase: .validating,
            key: "home.overlay.validatingRepoMonths"
        )
        assertCountedUpgrade(
            phase: .cleaning,
            key: "home.overlay.cleaningRepoMonths"
        )
        XCTAssertEqual(
            RemoteSyncProgressPresentation.message(
                for: RemoteSyncProgress(
                    current: 0,
                    total: 0,
                    kind: .repoUpgrade(.finalizing)
                )
            ),
            String(localized: "home.overlay.finalizingRepo")
        )
    }

    func testUpgradeWithoutCountUsesPhaseFallback() {
        XCTAssertEqual(
            RemoteSyncProgressPresentation.message(
                for: RemoteSyncProgress(
                    current: 0,
                    total: 0,
                    kind: .repoUpgrade(.copying)
                )
            ),
            String(localized: "home.overlay.upgradingRepo")
        )
        XCTAssertEqual(
            RemoteSyncProgressPresentation.message(
                for: RemoteSyncProgress(
                    current: 0,
                    total: 0,
                    kind: .repoUpgrade(.cleaning)
                )
            ),
            String(localized: "home.overlay.cleaningRepo")
        )
    }

    private func assertCountedUpgrade(
        phase: RepoUpgradePhase,
        key: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertEqual(
            RemoteSyncProgressPresentation.message(
                for: RemoteSyncProgress(
                    current: 2,
                    total: 5,
                    kind: .repoUpgrade(phase)
                )
            ),
            String.localizedStringWithFormat(
                String(localized: String.LocalizationValue(key)),
                2,
                5
            ),
            file: file,
            line: line
        )
    }
}

import XCTest
@testable import Watermelon

final class BackupSessionReducerTests: XCTestCase {

    // MARK: - resolveStartCancellation

    func testResolveStartCancellation_pausePreservesPendingRunConfiguration() {
        var state = BackupSessionState()
        let config = BackupRunConfigurationOverride(workerCountOverride: 4, iCloudPhotoBackupMode: .enable)
        state.controlPhase = .starting
        state.isStartCommandInFlight = true
        state.pendingRunConfiguration = config

        state.resolveStartCancellation(mode: .full)

        XCTAssertEqual(state.state, .paused)
        XCTAssertEqual(state.pendingRunConfiguration?.workerCountOverride, 4)
        XCTAssertEqual(state.pendingRunConfiguration?.iCloudPhotoBackupMode, .enable)
    }

    func testResolveStartCancellation_stopClearsPendingRunConfiguration() {
        var state = BackupSessionState()
        let config = BackupRunConfigurationOverride(workerCountOverride: 4, iCloudPhotoBackupMode: .enable)
        state.controlPhase = .stopping
        state.isStartCommandInFlight = true
        state.pendingRunConfiguration = config

        state.resolveStartCancellation(mode: .full)

        XCTAssertEqual(state.state, .stopped)
        XCTAssertNil(state.pendingRunConfiguration)
    }

    func testResolveStartCancellation_pausingPhaseResolvesToPause() {
        var state = BackupSessionState()
        state.controlPhase = .pausing
        state.isStartCommandInFlight = true
        state.pendingRunConfiguration = BackupRunConfigurationOverride(
            workerCountOverride: 2,
            iCloudPhotoBackupMode: .disable
        )

        state.resolveStartCancellation(mode: .full)

        XCTAssertEqual(state.state, .paused)
        XCTAssertNotNil(state.pendingRunConfiguration)
        if case .full = state.lastPausedRunMode {} else {
            XCTFail("expected .full, got \(state.lastPausedRunMode)")
        }
    }

    // MARK: - cancelResume

    func testCancelResume_pausePreservesPendingRunConfiguration() {
        var state = BackupSessionState()
        state.controlPhase = .pausing
        state.pendingRunConfiguration = BackupRunConfigurationOverride(
            workerCountOverride: 3,
            iCloudPhotoBackupMode: .enable
        )

        state.cancelResume(pausedMode: .full, pausedDisplayMode: .full)

        XCTAssertEqual(state.state, .paused)
        XCTAssertNotNil(state.pendingRunConfiguration)
    }

    func testCancelResume_stopClearsPendingRunConfiguration() {
        var state = BackupSessionState()
        state.controlPhase = .stopping
        state.pendingRunConfiguration = BackupRunConfigurationOverride(
            workerCountOverride: 3,
            iCloudPhotoBackupMode: .enable
        )

        state.cancelResume(pausedMode: .full, pausedDisplayMode: .full)

        XCTAssertEqual(state.state, .stopped)
        XCTAssertNil(state.pendingRunConfiguration)
    }
}

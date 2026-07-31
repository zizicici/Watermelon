import AppKit
import XCTest
@testable import WatermelonMac

final class MacToolsMenuAvailabilityPolicyTests: XCTestCase {
    func testToolCommandsResolveFromMenuActions() {
        XCTAssertEqual(
            MacToolsMenuCommand(
                action: NSSelectorFromString(
                    "openProfileManagement:"
                )
            ),
            .manageProfiles
        )
        XCTAssertEqual(
            MacToolsMenuCommand(
                action: NSSelectorFromString("openLocalIndex:")
            ),
            .localIndex
        )
        XCTAssertEqual(
            MacToolsMenuCommand(
                action: NSSelectorFromString("openDuplicates:")
            ),
            .duplicates
        )
        XCTAssertEqual(
            MacToolsMenuCommand(
                action: NSSelectorFromString(
                    "openRepositoryMaintenance:"
                )
            ),
            .repositoryMaintenance
        )
        XCTAssertEqual(
            MacToolsMenuCommand(
                action: NSSelectorFromString("openLogs:")
            ),
            .logs
        )
        XCTAssertNil(MacToolsMenuCommand(action: nil))
    }

    func testExecutionDisablesOnlyNewExclusiveToolWindows() {
        for command: MacToolsMenuCommand in [
            .localIndex,
            .duplicates,
            .repositoryMaintenance,
        ] {
            XCTAssertFalse(
                MacToolsMenuAvailabilityPolicy.isEnabled(
                    command: command,
                    executionActive: true,
                    hasVisibleWindow: false
                )
            )
            XCTAssertTrue(
                MacToolsMenuAvailabilityPolicy.isEnabled(
                    command: command,
                    executionActive: true,
                    hasVisibleWindow: true
                )
            )
        }
        for command: MacToolsMenuCommand in [
            .manageProfiles,
            .logs,
        ] {
            XCTAssertTrue(
                MacToolsMenuAvailabilityPolicy.isEnabled(
                    command: command,
                    executionActive: true,
                    hasVisibleWindow: false
                )
            )
        }
    }
}

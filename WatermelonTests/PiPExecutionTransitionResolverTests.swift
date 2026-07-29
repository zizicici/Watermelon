import XCTest
@testable import Watermelon

final class PiPExecutionTransitionResolverTests: XCTestCase {
    func testStartAndPauseResumeSequences() {
        XCTAssertEqual(
            PiPExecutionTransitionResolver.events(from: nil, to: .uploading),
            [.start, .setPaused(false)]
        )
        XCTAssertEqual(
            PiPExecutionTransitionResolver.events(from: .uploading, to: .uploadPaused),
            [.setPaused(true)]
        )
        XCTAssertEqual(
            PiPExecutionTransitionResolver.events(from: .uploadPaused, to: .uploading),
            [.resume, .setPaused(false)]
        )
        XCTAssertEqual(
            PiPExecutionTransitionResolver.events(from: .downloadPaused, to: .downloading),
            [.resume, .setPaused(false)]
        )
    }

    func testTerminalSequencesPreserveFinalEventOrder() {
        XCTAssertEqual(
            PiPExecutionTransitionResolver.events(from: .uploading, to: .completed),
            [.setPaused(false), .complete]
        )
        XCTAssertEqual(
            PiPExecutionTransitionResolver.events(from: .downloading, to: .failed("network")),
            [.setPaused(false), .fail]
        )
        XCTAssertEqual(
            PiPExecutionTransitionResolver.events(from: .downloadPaused, to: nil),
            [.setPaused(false), .cancel]
        )
    }

    func testPhaseSwitchDoesNotRestartPiP() {
        XCTAssertEqual(
            PiPExecutionTransitionResolver.events(from: .uploading, to: .downloading),
            [.setPaused(false)]
        )
    }
}

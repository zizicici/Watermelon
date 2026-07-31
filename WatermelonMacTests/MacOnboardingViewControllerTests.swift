import AppKit
import XCTest
@testable import WatermelonMac

@MainActor
final class MacOnboardingViewControllerTests: XCTestCase {
    func testContentUsesOneVerticalFourItemList() throws {
        let controller = MacOnboardingViewController(
            isFirstLaunch: true
        )
        controller.loadView()

        let expectedTitles = [
            String(localized: "onboarding.item.live_photo.title"),
            String(localized: "onboarding.item.edited.title"),
            String(localized: "onboarding.item.dedup.title"),
            String(
                localized: "onboarding.item.single_client.title"
            ),
        ]
        let featureList = try XCTUnwrap(
            descendants(of: controller.view)
                .compactMap { $0 as? NSStackView }
                .first {
                    $0.orientation == .vertical
                        && $0.arrangedSubviews.count == 4
                        && nonemptyLabels(in: $0) == expectedTitles
                }
        )

        XCTAssertEqual(featureList.orientation, .vertical)
        XCTAssertEqual(featureList.arrangedSubviews.count, 4)
        XCTAssertEqual(nonemptyLabels(in: featureList), expectedTitles)
        XCTAssertTrue(
            featureList.arrangedSubviews.allSatisfy {
                nonemptyLabels(in: $0).count == 1
            }
        )
    }

    func testContentHasNoExtraActionsOrStepIndicator() throws {
        let controller = MacOnboardingViewController(
            isFirstLaunch: true
        )
        controller.loadView()
        var continueCount = 0
        controller.onContinue = {
            continueCount += 1
        }

        let expectedLabels = [
            String(
                localized: "onboarding.title",
                defaultValue: "Before You Start"
            ),
            String(localized: "onboarding.item.live_photo.title"),
            String(localized: "onboarding.item.edited.title"),
            String(localized: "onboarding.item.dedup.title"),
            String(
                localized: "onboarding.item.single_client.title"
            ),
        ]
        let buttons = descendants(of: controller.view)
            .compactMap { $0 as? NSButton }
        let primaryButton = try XCTUnwrap(buttons.first)

        XCTAssertEqual(nonemptyLabels(in: controller.view), expectedLabels)
        XCTAssertEqual(buttons.count, 1)
        XCTAssertEqual(
            primaryButton.attributedTitle.string,
            String(
                localized: "onboarding.button.start",
                defaultValue: "Get Started"
            )
        )
        XCTAssertTrue(
            descendants(of: controller.view)
                .compactMap { $0 as? NSProgressIndicator }
                .isEmpty
        )
        XCTAssertTrue(
            descendants(of: controller.view)
                .compactMap { $0 as? NSSegmentedControl }
                .isEmpty
        )

        primaryButton.performClick(nil)

        XCTAssertEqual(continueCount, 1)
    }

    private func nonemptyLabels(in view: NSView) -> [String] {
        descendants(of: view)
            .compactMap { $0 as? NSTextField }
            .map(\.stringValue)
            .filter { !$0.isEmpty }
    }

    private func descendants(of view: NSView) -> [NSView] {
        view.subviews.flatMap {
            [$0] + descendants(of: $0)
        }
    }
}

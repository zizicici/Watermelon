import XCTest
@testable import WatermelonMac

final class MacHelpDestinationTests: XCTestCase {
    func testHelpDestinationsUseExpectedExternalTargets() {
        XCTAssertEqual(
            MacHelpDestination.contactSupport.url.absoluteString,
            "mailto:watermelon@zi.ci"
        )
        XCTAssertEqual(
            MacHelpDestination.privacyPolicy.url.absoluteString,
            "https://watermelonbackup.com/privacy.html"
        )
    }

    func testHelpDestinationsRemainUniqueAndComplete() {
        let destinations = MacHelpDestination.allCases
        XCTAssertEqual(destinations.count, 2)
        XCTAssertEqual(
            Set(destinations.map(\.url.absoluteString)).count,
            destinations.count
        )
        XCTAssertTrue(destinations.allSatisfy { !$0.title.isEmpty })
    }

    func testHelpMenuContainsOnlyWelcomeAndIOSHelpDestinations() async {
        await MainActor.run {
            let root = AppDelegate().makeHelpMenu()
            let titles = root.submenu?.items
                .filter { !$0.isSeparatorItem }
                .map(\.title)

            XCTAssertEqual(
                titles,
                [
                    String(
                        localized: "mac.menu.welcome",
                        defaultValue: "Welcome to Watermelon Backup"
                    ),
                    MacHelpDestination.contactSupport.title,
                    MacHelpDestination.privacyPolicy.title,
                ]
            )
        }
    }
}

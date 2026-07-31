import XCTest
import MoreKit

@testable import WatermelonMac

final class MacSettingsViewControllerTests: XCTestCase {
  @MainActor
  func testTimeZoneControlTracksLiveAvailability() {
    var canChange = true
    let controller = MacSettingsViewController {
      canChange
    }
    controller.loadView()

    XCTAssertTrue(controller.isTimeZoneSelectionEnabled)

    canChange = false
    controller.refreshAvailability()
    XCTAssertFalse(controller.isTimeZoneSelectionEnabled)

    canChange = true
    controller.refreshAvailability()
    XCTAssertTrue(controller.isTimeZoneSelectionEnabled)
  }

  @MainActor
  func testGeneralRowsAndControlsShareOneAlignment() {
    let controller = MacSettingsViewController { true }
    controller.loadViewIfNeeded()
    controller.view.frame = NSRect(
      x: 0,
      y: 0,
      width: 680,
      height: 420
    )
    controller.view.layoutSubtreeIfNeeded()
    let generalView = controller.tabViewItems[0]
      .viewController?.view
    generalView?.layoutSubtreeIfNeeded()

    let labels = descendants(
      of: try! XCTUnwrap(generalView)
    ).filter {
      $0.identifier?.rawValue == "settings.rowLabel"
    }
    let controls = descendants(
      of: try! XCTUnwrap(generalView)
    ).filter {
      $0.identifier?.rawValue == "settings.rowControl"
    }
    let grids = descendants(
      of: try! XCTUnwrap(generalView)
    ).compactMap { $0 as? NSGridView }
    let languagePopup = descendants(
      of: try! XCTUnwrap(generalView)
    ).compactMap { $0 as? NSPopUpButton }
      .first {
        $0.identifier?.rawValue == "settings.rowControl"
          && $0.itemTitles.contains("English")
      }

    XCTAssertEqual(labels.count, 3)
    XCTAssertEqual(controls.count, 3)
    XCTAssertEqual(grids.count, 1)
    XCTAssertNotNil(languagePopup)
    XCTAssertTrue(
      labels.allSatisfy {
        ($0 as? NSTextField)?.alignment == .right
      }
    )
    let labelFrames = labels.map {
      $0.convert($0.bounds, to: controller.view)
    }
    let controlFrames = controls.map {
      $0.convert($0.bounds, to: controller.view)
    }
    XCTAssertTrue(
      labelFrames.dropFirst().allSatisfy {
        abs($0.maxX - labelFrames[0].maxX) < 0.5
      }
    )
    XCTAssertTrue(
      controlFrames.dropFirst().allSatisfy {
        abs($0.minX - controlFrames[0].minX) < 0.5
      }
    )
    let contentFrames = labelFrames + controlFrames
    let contentMinX = contentFrames.map(\.minX).min()!
    let contentMaxX = contentFrames.map(\.maxX).max()!
    XCTAssertEqual(
      (contentMinX + contentMaxX) / 2,
      controller.view.bounds.midX,
      accuracy: 1
    )
    let grid = try! XCTUnwrap(grids.first)
    let container = try! XCTUnwrap(grid.superview)
    XCTAssertEqual(
      grid.frame.midX,
      container.bounds.midX,
      accuracy: 0.5
    )
    XCTAssertFalse(
      container.constraints.contains {
        ($0.firstItem as? NSGridView) === grid
          && $0.firstAttribute == .width
      }
    )
  }

  @MainActor
  func testSettingsUseFourNativePreferencePages() {
    let controller = MacSettingsViewController { true }
    controller.loadViewIfNeeded()

    XCTAssertEqual(
      controller.tabViewItems.map(\.label),
      [
        String(
          localized: "more.section.general",
          defaultValue: "General"
        ),
        String(
          localized: "more.section.backup",
          defaultValue: "Backup"
        ),
        String(
          localized: "more.section.imageBrowser",
          defaultValue: "Photo Browser"
        ),
        String(
          localized: "more.title",
          defaultValue: "More"
        ),
      ]
    )

    let expectedRowCounts = [3, 3, 2, 4]
    for (index, expectedCount) in expectedRowCounts.enumerated() {
      controller.selectedTabViewItemIndex = index
      let page = try! XCTUnwrap(
        controller.tabViewItems[index].viewController?.view
      )
      page.layoutSubtreeIfNeeded()
      XCTAssertEqual(
        descendants(of: page).filter {
          $0.identifier?.rawValue == "settings.rowLabel"
        }.count,
        expectedCount
      )
      XCTAssertEqual(
        descendants(of: page).compactMap {
          $0 as? NSGridView
        }.count,
        1
      )
      XCTAssertTrue(
        descendants(of: page).compactMap {
          $0 as? NSBox
        }.isEmpty
      )
    }

    let aboutPage = try! XCTUnwrap(
      controller.tabViewItems[3].viewController?.view
    )
    let aboutButtonTitles = descendants(of: aboutPage)
      .compactMap { $0 as? NSButton }
      .map(\.title)
    XCTAssertTrue(aboutButtonTitles.contains("watermelon@zi.ci"))
    XCTAssertTrue(
      aboutButtonTitles.contains(
        String(
          localized: "mediaBrowser.action.share",
          defaultValue: "Share"
        )
      )
    )
    XCTAssertTrue(
      aboutButtonTitles.contains(
        String(
          localized: "more.about.review",
          defaultValue: "Write Review"
        )
      )
    )
    XCTAssertTrue(
      aboutButtonTitles.contains(
        String(
          localized: "more.about.eula",
          defaultValue: "EULA"
        )
      )
    )
    XCTAssertTrue(
      aboutButtonTitles.contains("watermelonbackup.com")
    )
    XCTAssertFalse(
      aboutButtonTitles.contains(
        String(localized: "common.open", defaultValue: "Open")
      )
    )
    let aboutRowTitles = descendants(of: aboutPage)
      .compactMap { $0 as? NSTextField }
      .filter {
        $0.identifier?.rawValue == "settings.rowLabel"
      }
      .map(\.stringValue)
    XCTAssertTrue(aboutRowTitles.contains("App Store"))
    let aboutRowLabels = descendants(of: aboutPage)
      .compactMap { $0 as? NSTextField }
      .filter {
        $0.identifier?.rawValue == "settings.rowLabel"
      }
    XCTAssertTrue(
      aboutRowLabels.allSatisfy {
        $0.frame.width + 0.5 >= $0.fittingSize.width
      }
    )

    let showcase = try! XCTUnwrap(
      descendants(of: aboutPage)
        .compactMap { $0 as? AppShowcaseView }
        .first
    )
    let iconButtons = descendants(of: showcase)
      .compactMap { $0 as? NSButton }
      .filter { $0.image != nil }
    XCTAssertEqual(
      iconButtons.count,
      showcase.apps.count * 2
    )
    XCTAssertEqual(
      showcase.apps,
      AppInfo.App.allCases.filter { $0 != .watermelon }
    )
    XCTAssertEqual(showcase.frame.width, 320, accuracy: 0.5)
  }

  @MainActor
  func testSpecificationsUseMacTargetMetadataAndDependencies() {
    let configuration = MacSpecifications.current()
    let values = Dictionary(
      uniqueKeysWithValues: configuration.summaryItems.map {
        ($0.type, $0.value)
      }
    )

    XCTAssertEqual(
      values[.manufacturer],
      "@App君"
    )
    XCTAssertEqual(
      values[.publisher],
      "ZIZICICI LIMITED"
    )
    XCTAssertEqual(
      values[.dateOfProduction],
      "2026/07/29"
    )
    XCTAssertEqual(
      values[.license],
      "粤ICP备2025448771号-6A"
    )
    XCTAssertEqual(
      configuration.thirdPartyLibraries.map(\.name),
      ["AMSMB2", "Citadel", "GRDB", "MoreKit", "MSAL"]
    )
    XCTAssertEqual(
      configuration.thirdPartyLibraries.map(\.version),
      [
        "master",
        "fix/sftp-response-lock",
        "7.10.0",
        "codex/macos-support",
        "2.11.0",
      ]
    )
  }

  @MainActor
  func testSpecificationsRenderSummaryAndLibraryLinks() {
    let configuration = MacSpecifications.current()
    let controller = SpecificationsViewController(
      configuration: configuration
    )
    controller.loadViewIfNeeded()
    controller.view.layoutSubtreeIfNeeded()
    let views = descendants(of: controller.view)

    XCTAssertEqual(
      views.compactMap { $0 as? NSGridView }.count,
      2
    )
    let buttonTitles = Set(
      views.compactMap { ($0 as? NSButton)?.title }
    )
    XCTAssertTrue(
      Set(configuration.thirdPartyLibraries.map(\.name))
        .isSubset(of: buttonTitles)
    )
    XCTAssertTrue(
      views.compactMap { $0 as? NSBox }.isEmpty
    )
  }

  @MainActor
  private func descendants(of view: NSView) -> [NSView] {
    view.subviews.flatMap {
      [$0] + descendants(of: $0)
    }
  }
}

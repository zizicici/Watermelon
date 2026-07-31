import AppKit
import XCTest
@testable import WatermelonMac

@MainActor
final class MacLibraryMonthListViewControllerTests: XCTestCase {
    func testDeniedPhotoAccessShowsOnlyRequiredActionAndInvokesIt()
        throws
    {
        let controller = MacLibraryMonthListViewController()
        controller.loadView()
        var actionCount = 0
        controller.onLocalAccessAction = {
            actionCount += 1
        }

        controller.applyLocalAccessState(.denied)

        let actionTitle = String(
            localized: "home.overlay.goToSettings",
            defaultValue: "Go to Settings"
        )
        let actionButton = try XCTUnwrap(
            buttons(in: controller.view).first {
                buttonTitle($0) == actionTitle
            }
        )
        let stack = try XCTUnwrap(
            actionButton.superview as? NSStackView
        )
        let messages = descendants(of: stack)
            .compactMap { $0 as? NSTextField }
            .map(\.stringValue)
            .filter { !$0.isEmpty }

        XCTAssertEqual(
            messages,
            [
                String(
                    localized: "home.overlay.noAuth",
                    defaultValue: "Photo library access denied"
                )
            ]
        )
        XCTAssertEqual(
            stack.arrangedSubviews
                .compactMap { $0 as? NSButton }
                .map(buttonTitle),
            [actionTitle]
        )

        actionButton.performClick(nil)

        XCTAssertEqual(actionCount, 1)
    }

    func testNoDestinationRemoteStateContainsOnlyTwoActions()
        throws
    {
        let controller = MacLibraryMonthListViewController()
        controller.loadView()
        var createdType: StorageType?
        controller.onCreateDestination = {
            createdType = $0
        }

        controller.applyRemoteOverlay(
            mode: .emptySetup,
            profiles: [],
            interactionEnabled: true
        )

        let expectedTitles = [
            String(
                localized: "home.menu.externalStorage",
                defaultValue: "External Storage"
            ),
            String(
                localized: "home.menu.addStorage",
                defaultValue: "Add Destination"
            ),
        ]
        let actionButtons = try expectedTitles.map { title in
            try XCTUnwrap(
                buttons(in: controller.view).first {
                    buttonTitle($0) == title
                }
            )
        }
        let stack = try XCTUnwrap(
            actionButtons.first?.superview as? NSStackView
        )

        XCTAssertTrue(actionButtons.allSatisfy { $0.superview === stack })
        XCTAssertEqual(
            stack.arrangedSubviews
                .compactMap { $0 as? NSButton }
                .map(buttonTitle),
            expectedTitles
        )
        XCTAssertTrue(
            descendants(of: stack)
                .compactMap { $0 as? NSTextField }
                .allSatisfy { $0.stringValue.isEmpty }
        )

        actionButtons[0].performClick(nil)

        XCTAssertEqual(createdType, .externalVolume)
    }

    func testSavedDestinationRemoteStateContainsOnlyConnectAndAdd()
        throws
    {
        let controller = MacLibraryMonthListViewController()
        controller.loadView()
        let profile = makeProfile()

        controller.applyRemoteOverlay(
            mode: .profileSelection,
            profiles: [profile],
            interactionEnabled: true
        )

        let expectedTitles = [
            String(
                localized: "home.overlay.connectNode",
                defaultValue: "Connect Node"
            ),
            String(
                localized: "home.menu.addStorage",
                defaultValue: "Add Destination"
            ),
        ]
        let actionButtons = try expectedTitles.map { title in
            try XCTUnwrap(
                buttons(in: controller.view).first {
                    buttonTitle($0) == title
                }
            )
        }
        let stack = try XCTUnwrap(
            actionButtons.first?.superview as? NSStackView
        )

        XCTAssertTrue(actionButtons.allSatisfy { $0.superview === stack })
        XCTAssertTrue(actionButtons.allSatisfy(\.isEnabled))
        XCTAssertEqual(
            stack.arrangedSubviews
                .compactMap { $0 as? NSButton }
                .map(buttonTitle),
            expectedTitles
        )
        XCTAssertTrue(
            descendants(of: stack)
                .compactMap { $0 as? NSTextField }
                .allSatisfy { $0.stringValue.isEmpty }
        )
    }

    func testRemoteProgressContainsOneMessageAndOneAction()
        throws
    {
        let controller = MacLibraryMonthListViewController()
        controller.loadView()
        var actionCount = 0
        controller.onRemoteOverlayAction = {
            actionCount += 1
        }
        let message = "Indexing 2 / 3"
        let actionTitle = String(
            localized: "home.menu.disconnect",
            defaultValue: "Disconnect"
        )

        controller.applyRemoteOverlay(
            mode: .progress(
                message: message,
                actionTitle: actionTitle
            ),
            profiles: [],
            interactionEnabled: true
        )

        let actionButton = try XCTUnwrap(
            buttons(in: controller.view).first {
                buttonTitle($0) == actionTitle
            }
        )
        let stack = try XCTUnwrap(
            actionButton.superview as? NSStackView
        )

        XCTAssertEqual(
            descendants(of: stack)
                .compactMap { $0 as? NSTextField }
                .map(\.stringValue)
                .filter { !$0.isEmpty },
            [message]
        )
        XCTAssertEqual(
            stack.arrangedSubviews
                .compactMap { $0 as? NSButton }
                .map(buttonTitle),
            [actionTitle]
        )
        XCTAssertEqual(
            stack.arrangedSubviews
                .compactMap { $0 as? NSProgressIndicator }
                .count,
            1
        )

        actionButton.performClick(nil)

        XCTAssertEqual(actionCount, 1)
    }

    func testMissingRemoteMonthRendersZeroMediaCounts() throws {
        let controller = MacLibraryMonthListViewController()
        controller.loadView()
        controller.apply(snapshot: makeLocalOnlySnapshot())

        let tableView = try XCTUnwrap(
            descendants(of: controller.view)
                .compactMap { $0 as? NSTableView }
                .first
        )
        XCTAssertEqual(controller.numberOfRows(in: tableView), 2)

        let remoteCell = try XCTUnwrap(
            controller.tableView(
                tableView,
                viewFor: tableView.tableColumns[1],
                row: 1
            )
        )
        let fields = descendants(of: remoteCell)
            .compactMap { $0 as? NSTextField }
        let mediaCountField = try XCTUnwrap(
            fields.first {
                attachmentCount(in: $0.attributedStringValue) == 2
            }
        )

        XCTAssertEqual(
            numericTokens(in: mediaCountField.attributedStringValue),
            ["0", "0"]
        )
        XCTAssertTrue(
            fields.contains {
                !$0.stringValue.isEmpty
                    && attachmentCount(
                        in: $0.attributedStringValue
                    ) == 0
            }
        )
    }

    func testHeaderSeparatesPhotoAndVideoCountsOnBothSides() {
        let controller = MacLibraryMonthListViewController()
        controller.loadView()
        controller.apply(snapshot: makeTwoSidedSnapshot())

        let headerCounts = descendants(of: controller.view)
            .compactMap { $0 as? NSTextField }
            .filter {
                attachmentCount(in: $0.attributedStringValue) == 2
            }
            .map {
                numericTokens(in: $0.attributedStringValue)
            }

        XCTAssertTrue(headerCounts.contains(["7", "2", "1"]))
        XCTAssertTrue(headerCounts.contains(["3", "4", "2"]))
    }

    private func makeLocalOnlySnapshot()
        -> PhotoLibraryMonthlyIndexSnapshot
    {
        let month = LibraryMonthKey(year: 2026, month: 7)
        let local = HomeMonthSummary(
            month: month,
            assetCount: 5,
            photoCount: 4,
            videoCount: 1,
            backedUpCount: 0,
            totalSizeBytes: 1_000
        )
        return PhotoLibraryMonthlyIndexSnapshot(
            sections: [
                HomeMergedYearSection(
                    year: 2026,
                    rows: [
                        HomeMonthRow(
                            month: month,
                            local: local,
                            remote: nil
                        )
                    ]
                )
            ],
            totalAssetCount: 5,
            totalPhotoCount: 4,
            totalVideoCount: 1,
            totalSizeBytes: 1_000,
            remoteAssetCount: 0,
            remotePhotoCount: 0,
            remoteVideoCount: 0,
            remoteSizeBytes: nil,
            monthGroupingTimeZone: .frozenCurrent()
        )
    }

    private func makeTwoSidedSnapshot()
        -> PhotoLibraryMonthlyIndexSnapshot
    {
        PhotoLibraryMonthlyIndexSnapshot(
            sections: [],
            totalAssetCount: 9,
            totalPhotoCount: 7,
            totalVideoCount: 2,
            totalSizeBytes: 1_000,
            remoteAssetCount: 7,
            remotePhotoCount: 3,
            remoteVideoCount: 4,
            remoteSizeBytes: 2_000,
            monthGroupingTimeZone: .frozenCurrent()
        )
    }

    private func makeProfile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: 7,
            name: "Archive",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "nas.local",
            port: 445,
            shareName: "photos",
            basePath: "/Watermelon",
            username: "user",
            domain: nil,
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
    }

    private func buttons(in view: NSView) -> [NSButton] {
        descendants(of: view).compactMap { $0 as? NSButton }
    }

    private func buttonTitle(_ button: NSButton) -> String {
        button.attributedTitle.string
    }

    private func descendants(of view: NSView) -> [NSView] {
        view.subviews.flatMap {
            [$0] + descendants(of: $0)
        }
    }

    private func attachmentCount(
        in value: NSAttributedString
    ) -> Int {
        var count = 0
        value.enumerateAttribute(
            .attachment,
            in: NSRange(location: 0, length: value.length)
        ) { attachment, _, _ in
            if attachment != nil {
                count += 1
            }
        }
        return count
    }

    private func numericTokens(
        in value: NSAttributedString
    ) -> [String] {
        value.string.split {
            !$0.isNumber
        }.map(String.init)
    }
}

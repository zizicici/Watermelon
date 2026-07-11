import XCTest
import UIKit
@testable import Watermelon

@MainActor
final class SMBSelectionViewControllerTests: XCTestCase {
    func testShareSelectionShowsCurrentCheckmarkImmediately() {
        let viewController = SMBShareSelectionViewController(
            shares: [
                SMBShareInfo(name: "Photos", comment: ""),
                SMBShareInfo(name: "Archive", comment: "")
            ],
            selectedShareName: "Archive",
            onSelected: { _ in }
        )
        viewController.loadViewIfNeeded()

        let first = viewController.tableView(
            viewController.tableView,
            cellForRowAt: IndexPath(row: 0, section: 0)
        )
        let selected = viewController.tableView(
            viewController.tableView,
            cellForRowAt: IndexPath(row: 1, section: 0)
        )

        XCTAssertEqual(first.accessoryType, .none)
        XCTAssertEqual(selected.accessoryType, .checkmark)
    }

    func testFolderLoadingAndLoadedCellsRefreshWithoutLeavingScreen() async {
        let loaderStarted = expectation(description: "loader started")
        let loaderFinished = expectation(description: "loader finished")
        let entry = RemoteStorageEntry(
            path: "/Child",
            name: "Child",
            isDirectory: true,
            size: 0,
            creationDate: nil,
            modificationDate: nil
        )
        let viewController = SMBFolderSelectionViewController(
            auth: makeAuth(),
            shareName: "Photos",
            initialPath: "/",
            directoryLoader: { _, _, _ in
                loaderStarted.fulfill()
                try await Task.sleep(nanoseconds: 20_000_000)
                loaderFinished.fulfill()
                return [entry]
            },
            onSelected: { _ in }
        )
        viewController.loadViewIfNeeded()

        await fulfillment(of: [loaderStarted], timeout: 1)
        XCTAssertEqual(viewController.tableView(viewController.tableView, numberOfRowsInSection: 0), 1)
        let loadingActionCell = viewController.tableView(
            viewController.tableView,
            cellForRowAt: IndexPath(row: 0, section: 0)
        )
        let loadingActionContent = try? XCTUnwrap(loadingActionCell.contentConfiguration as? UIListContentConfiguration)
        XCTAssertEqual(loadingActionContent?.textProperties.color, .secondaryLabel)
        XCTAssertEqual(loadingActionCell.selectionStyle, .none)

        let loadingCell = viewController.tableView(
            viewController.tableView,
            cellForRowAt: IndexPath(row: 0, section: 1)
        )
        XCTAssertEqual(loadingCell.reuseIdentifier, "FolderStatusCell")
        XCTAssertTrue(loadingCell.accessoryView is UIActivityIndicatorView)

        await fulfillment(of: [loaderFinished], timeout: 1)
        var loadedCell: UITableViewCell?
        for _ in 0..<20 {
            await Task.yield()
            let candidate = viewController.tableView(
                viewController.tableView,
                cellForRowAt: IndexPath(row: 0, section: 1)
            )
            if candidate.reuseIdentifier == "FolderCell" {
                loadedCell = candidate
                break
            }
        }

        XCTAssertEqual(loadedCell?.reuseIdentifier, "FolderCell")
        let actionCell = viewController.tableView(
            viewController.tableView,
            cellForRowAt: IndexPath(row: 0, section: 0)
        )
        XCTAssertEqual(actionCell.reuseIdentifier, "FolderActionCell")
        XCTAssertNotEqual(actionCell.reuseIdentifier, loadedCell?.reuseIdentifier)
        let actionContent = try? XCTUnwrap(actionCell.contentConfiguration as? UIListContentConfiguration)
        XCTAssertNil(actionContent?.image)
        XCTAssertEqual(actionContent?.textProperties.alignment, .center)
        XCTAssertEqual(actionContent?.textProperties.color, .systemBlue)
        XCTAssertEqual(actionCell.selectionStyle, .default)
    }

    func testParentFolderActionUsesIndependentCenteredSectionWithoutIcon() {
        let viewController = SMBFolderSelectionViewController(
            auth: makeAuth(),
            shareName: "Photos",
            initialPath: "/Albums/2026",
            directoryLoader: { _, _, _ in [] },
            onSelected: { _ in }
        )
        viewController.loadViewIfNeeded()

        XCTAssertEqual(viewController.numberOfSections(in: viewController.tableView), 3)
        XCTAssertEqual(viewController.tableView(viewController.tableView, numberOfRowsInSection: 0), 1)
        XCTAssertEqual(viewController.tableView(viewController.tableView, numberOfRowsInSection: 1), 1)

        let parentCell = viewController.tableView(
            viewController.tableView,
            cellForRowAt: IndexPath(row: 0, section: 1)
        )
        let content = parentCell.contentConfiguration as? UIListContentConfiguration
        XCTAssertEqual(content?.text, String(localized: "auth.smb.share.parentDir"))
        XCTAssertEqual(content?.textProperties.alignment, .center)
        XCTAssertEqual(content?.textProperties.color, .systemBlue)
        XCTAssertNil(content?.image)
        XCTAssertEqual(parentCell.selectionStyle, .default)
        XCTAssertEqual(parentCell.accessoryType, .none)
    }

    private func makeAuth() -> SMBServerAuthContext {
        SMBServerAuthContext(
            name: "NAS",
            host: "nas.local",
            port: 445,
            username: "alice",
            password: "secret",
            domain: nil
        )
    }
}

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

    func testFolderFailureDetailsPreserveOriginalReasonAndRetryClearsReport() async throws {
        let failure = NSError(domain: NSPOSIXErrorDomain, code: Int(EACCES), userInfo: [
            NSLocalizedDescriptionKey: "Error code 13: STATUS_ACCESS_DENIED opening Photos/private"
        ])
        let firstLoad = expectation(description: "first folder load failed")
        var attempts = 0
        let controller = SMBFolderSelectionViewController(auth: makeAuth(), shareName: "Photos", initialPath: "/private",
            directoryLoader: { _, _, _ in
                attempts += 1
                if attempts == 1 {
                    firstLoad.fulfill()
                    throw failure
                }
                return []
            }, onSelected: { _ in })
        let scene = try XCTUnwrap(UIApplication.shared.connectedScenes.compactMap { $0 as? UIWindowScene }.first)
        let previousWindow = scene.windows.first { $0.isKeyWindow }
        let window = UIWindow(windowScene: scene)
        window.rootViewController = UINavigationController(rootViewController: controller)
        let animationsEnabled = UIView.areAnimationsEnabled
        UIView.setAnimationsEnabled(false)
        window.makeKeyAndVisible()
        controller.loadViewIfNeeded()
        defer {
            controller.dismiss(animated: false)
            window.isHidden = true
            previousWindow?.makeKeyAndVisible()
            UIView.setAnimationsEnabled(animationsEnabled)
        }
        await fulfillment(of: [firstLoad], timeout: 1)
        for _ in 0..<100 {
            if controller.navigationItem.rightBarButtonItem != nil { break }
            await Task.yield()
        }
        let failedRow = IndexPath(row: 0, section: 2)
        let cell = controller.tableView(controller.tableView, cellForRowAt: failedRow)
        let content = try XCTUnwrap(cell.contentConfiguration as? UIListContentConfiguration)
        XCTAssertEqual(content.text, String(localized: "auth.smb.share.readFailed"))
        XCTAssertEqual(content.secondaryText, String(localized: "storage.error.permissionDenied"))
        let detailsButton = try XCTUnwrap(controller.navigationItem.rightBarButtonItem)
        XCTAssertEqual(detailsButton.title, String(localized: "connection.failure.details"))
        let detailsAction = try XCTUnwrap(detailsButton.action)
        XCTAssertTrue(UIApplication.shared.sendAction(detailsAction, to: detailsButton.target, from: detailsButton, for: nil))
        await Task.yield()
        let navigation = try XCTUnwrap(controller.presentedViewController as? UINavigationController)
        let details = try XCTUnwrap(navigation.topViewController as? ConnectionFailureDetailsViewController)
        details.loadViewIfNeeded()
        let textView = try XCTUnwrap(details.view.subviews.compactMap { $0 as? UITextView }.first)
        XCTAssertTrue(textView.text.contains("STATUS_ACCESS_DENIED"))
        XCTAssertTrue(textView.text.contains("NSPOSIXErrorDomain/13"))
        XCTAssertTrue(textView.text.contains("connection.smb_directory"))
        XCTAssertTrue(textView.text.contains("file: /private"))
        XCTAssertFalse(textView.text.contains("secret"))
        let copyButton = try XCTUnwrap(details.navigationItem.rightBarButtonItem)
        let copyAction = try XCTUnwrap(copyButton.action)
        XCTAssertTrue(UIApplication.shared.sendAction(copyAction, to: copyButton.target, from: copyButton, for: nil))
        XCTAssertEqual(UIPasteboard.general.string, textView.text)
        controller.dismiss(animated: false)
        await Task.yield()
        XCTAssertEqual(attempts, 1)
        controller.tableView(controller.tableView, didSelectRowAt: failedRow)
        XCTAssertNil(controller.navigationItem.rightBarButtonItem)
        var loadedContent: UIListContentConfiguration?
        for _ in 0..<100 {
            await Task.yield()
            let row = controller.tableView(controller.tableView, cellForRowAt: failedRow)
            loadedContent = row.contentConfiguration as? UIListContentConfiguration
            if loadedContent?.text == String(localized: "common.none") { break }
        }
        XCTAssertEqual(attempts, 2)
        XCTAssertEqual(loadedContent?.text, String(localized: "common.none"))
        XCTAssertNil(controller.navigationItem.rightBarButtonItem)
    }

    func testCancelledFolderLoadDoesNotExposeErrorDetails() async {
        let started = expectation(description: "folder load started")
        let controller = SMBFolderSelectionViewController(auth: makeAuth(), shareName: "Photos", initialPath: "/",
            directoryLoader: { _, _, _ in
                started.fulfill()
                throw URLError(.cancelled)
            }, onSelected: { _ in })
        controller.loadViewIfNeeded()
        await fulfillment(of: [started], timeout: 1)
        for _ in 0..<20 { await Task.yield() }
        XCTAssertNil(controller.navigationItem.rightBarButtonItem)
        XCTAssertNil(controller.presentedViewController)
        let cell = controller.tableView(controller.tableView, cellForRowAt: IndexPath(row: 0, section: 1))
        XCTAssertNotEqual((cell.contentConfiguration as? UIListContentConfiguration)?.text,
                          String(localized: "auth.smb.share.readFailed"))
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

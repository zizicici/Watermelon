import UserNotifications
import XCTest
@testable import Watermelon

final class BackgroundBackupNotificationTests: XCTestCase {
    private func notificationContent(
        for profile: ServerProfileRecord,
        result: BackgroundBackupNotifications.Result,
        taskIsCancelled: Bool
    ) -> UNMutableNotificationContent? {
        var summary = BackgroundBackupNotifications.Summary()
        summary.record(result, for: profile)
        return summary.content(taskIsCancelled: taskIsCancelled)
    }

    private func profile(_ id: Int64 = 1) -> ServerProfileRecord {
        ServerProfileRecord(
            id: id, name: "NAS \(id)", storageType: StorageType.smb.rawValue, sortOrder: 0,
            host: "nas.local", port: 445, shareName: "Photos", basePath: "/\(id)",
            username: "user", credentialRef: "test", createdAt: Date(), updatedAt: Date()
        )
    }

    func testSuccessAndFailureSwitchesAreIndependent() {
        var node = profile()
        node.backgroundBackupNotifyOnSuccess = false
        XCTAssertNil(notificationContent(for: node, result: .success(transferred: 1), taskIsCancelled: false))
        XCTAssertNotNil(notificationContent(
            for: node, result: .failure(LocalDataSourceError.emptyAlbums), taskIsCancelled: false
        ))

        node.backgroundBackupNotifyOnSuccess = true
        node.backgroundBackupNotifyOnFailure = false
        XCTAssertNotNil(notificationContent(for: node, result: .success(transferred: 1), taskIsCancelled: false))
        XCTAssertNil(notificationContent(
            for: node, result: .failure(LocalDataSourceError.emptyAlbums), taskIsCancelled: false
        ))
    }

    func testSuccessfulRunThatTransferredNothingStaysSilent() {
        var summary = BackgroundBackupNotifications.Summary()
        summary.record(.success(transferred: 0), for: profile(1))
        XCTAssertNil(summary.content(taskIsCancelled: false))

        summary.record(.success(transferred: 3), for: profile(2))
        let content = summary.content(taskIsCancelled: false)
        XCTAssertEqual(content?.body, String(
            format: String(localized: "backgroundBackup.notification.success.body"), "NAS 2"
        ))
        XCTAssertFalse(content?.body.contains("NAS 1") ?? true)
    }

    func testSystemCancellationSuppressesEitherResult() {
        XCTAssertNil(notificationContent(for: profile(), result: .success(transferred: 1), taskIsCancelled: true))
        XCTAssertNil(notificationContent(
            for: profile(), result: .failure(LocalDataSourceError.emptyAlbums), taskIsCancelled: true
        ))
    }

    func testCancellationAndRepositorySkipAreNeverReportedAsFailures() {
        let errors: [Error] = [
            CancellationError(),
            NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled),
            LiteRepoError.probeFault(.cancelled),
            BackupRunSkipped()
        ]
        for error in errors {
            XCTAssertNil(notificationContent(
                for: profile(), result: .failure(error), taskIsCancelled: false
            ), "Unexpected notification for \(error)")
        }
    }

    func testPausedBackupIsSilentButFailedItemsProduceFailureNotification() {
        let paused = BackupExecutionResult(total: 2, succeeded: 1, failed: 0, skipped: 0, paused: true)
        XCTAssertThrowsError(try BackgroundBackupRunner.validateCompletion(paused)) { error in
            XCTAssertNil(notificationContent(for: profile(), result: .failure(error), taskIsCancelled: false))
        }
        let failed = BackupExecutionResult(total: 2, succeeded: 1, failed: 1, skipped: 0, paused: false)
        XCTAssertThrowsError(try BackgroundBackupRunner.validateCompletion(failed)) { error in
            XCTAssertNotNil(notificationContent(for: profile(), result: .failure(error), taskIsCancelled: false))
        }
    }

    func testFailureNotificationIdentifiesNodeAndUnavailableAlbums() throws {
        let node = profile()
        let error = LocalDataSourceError.unavailableAlbums(["Travel", "Family"])
        let content = try XCTUnwrap(notificationContent(
            for: node, result: .failure(error), taskIsCancelled: false
        ))
        XCTAssertTrue(content.body.contains(node.name))
        XCTAssertTrue(content.body.contains("Travel"))
        XCTAssertTrue(content.body.contains("Family"))
        XCTAssertEqual(content.title, String(localized: "backgroundBackup.notification.failure.title"))
        XCTAssertEqual(content.threadIdentifier, "background-backup")
    }

    func testMultipleSuccessfulNodesShareOneSummary() throws {
        var summary = BackgroundBackupNotifications.Summary()
        summary.record(.success(transferred: 1), for: profile(1))
        summary.record(.success(transferred: 1), for: profile(2))
        let content = try XCTUnwrap(summary.content(taskIsCancelled: false))
        XCTAssertEqual(content.title, String(localized: "backgroundBackup.notification.summary.title"))
        XCTAssertEqual(content.body, String(
            format: String(localized: "backgroundBackup.notification.summary.success.body"),
            ListFormatter.localizedString(byJoining: ["NAS 1", "NAS 2"])
        ))
    }

    func testMixedSummaryIncludesSuccessfulNodesAndEachFailureReason() throws {
        var summary = BackgroundBackupNotifications.Summary()
        summary.record(.success(transferred: 1), for: profile(1))
        summary.record(.failure(LocalDataSourceError.unavailableAlbums(["Travel"])), for: profile(2))
        let content = try XCTUnwrap(summary.content(taskIsCancelled: false))
        XCTAssertEqual(content.title, String(localized: "backgroundBackup.notification.summary.title"))
        let lines = content.body.components(separatedBy: "\n")
        XCTAssertEqual(lines.count, 2)
        XCTAssertTrue(lines[0].contains("NAS 1"))
        XCTAssertTrue(lines[1].contains("NAS 2"))
        XCTAssertTrue(lines[1].contains("Travel"))
        XCTAssertNil(summary.content(taskIsCancelled: true))
    }

    func testMultipleFailedNodesKeepTheirOwnReasons() throws {
        var summary = BackgroundBackupNotifications.Summary()
        summary.record(.failure(LocalDataSourceError.unavailableAlbums(["Travel"])), for: profile(1))
        summary.record(.failure(LocalDataSourceError.unavailableAlbums(["Family"])), for: profile(2))
        let content = try XCTUnwrap(summary.content(taskIsCancelled: false))
        XCTAssertEqual(content.title, String(localized: "backgroundBackup.notification.summary.title"))
        let lines = content.body.components(separatedBy: "\n")
        XCTAssertEqual(lines.count, 2)
        XCTAssertTrue(lines[0].contains("NAS 1"))
        XCTAssertTrue(lines[0].contains("Travel"))
        XCTAssertTrue(lines[1].contains("NAS 2"))
        XCTAssertTrue(lines[1].contains("Family"))
    }

    func testOnlyNotifiableResultsCountTowardSummary() throws {
        var summary = BackgroundBackupNotifications.Summary()
        var mutedSuccess = profile(1)
        mutedSuccess.backgroundBackupNotifyOnSuccess = false
        var mutedFailure = profile(2)
        mutedFailure.backgroundBackupNotifyOnFailure = false
        summary.record(.success(transferred: 1), for: mutedSuccess)
        summary.record(.failure(LocalDataSourceError.emptyAlbums), for: mutedFailure)
        summary.record(.failure(BackupRunSkipped()), for: profile(3))
        summary.record(.failure(CancellationError()), for: profile(4))
        XCTAssertNil(summary.content(taskIsCancelled: false))

        summary.record(.success(transferred: 1), for: profile(5))
        let content = try XCTUnwrap(summary.content(taskIsCancelled: false))
        XCTAssertEqual(content.title, String(localized: "backgroundBackup.notification.success.title"))
        XCTAssertEqual(content.body, String(format: String(localized: "backgroundBackup.notification.success.body"), "NAS 5"))
        summary.record(.failure(LocalDataSourceError.emptyAlbums), for: profile(6))
        let mixed = try XCTUnwrap(summary.content(taskIsCancelled: false))
        for id in 1...4 { XCTAssertFalse(mixed.body.contains("NAS \(id)")) }
        XCTAssertTrue(mixed.body.contains("NAS 5"))
        XCTAssertTrue(mixed.body.contains("NAS 6"))
    }

    func testEmptyOrNewRunDoesNotReuseEarlierResults() {
        var previous = BackgroundBackupNotifications.Summary()
        XCTAssertNil(previous.content(taskIsCancelled: false))
        previous.record(.success(transferred: 1), for: profile())
        XCTAssertNotNil(previous.content(taskIsCancelled: false))
        let next = BackgroundBackupNotifications.Summary()
        XCTAssertNil(next.content(taskIsCancelled: false))
    }

    func testDeliveryRequiresExistingSystemAuthorization() {
        XCTAssertFalse(BackgroundBackupNotifications.canDeliver(authorization: .notDetermined))
        XCTAssertFalse(BackgroundBackupNotifications.canDeliver(authorization: .denied))
        XCTAssertTrue(BackgroundBackupNotifications.canDeliver(authorization: .authorized))
        XCTAssertTrue(BackgroundBackupNotifications.canDeliver(authorization: .provisional))
        XCTAssertTrue(BackgroundBackupNotifications.canDeliver(authorization: .ephemeral))
    }

    func testNotificationSettingsPersistIndependentlyForEachNode() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let databaseURL = directory.appendingPathComponent("test.sqlite")
        let database = try DatabaseManager(databaseURL: databaseURL)
        var first = profile()
        first.id = nil
        var second = profile(2)
        second.id = nil
        try database.saveServerProfile(&first)
        try database.saveServerProfile(&second)
        let firstID = try XCTUnwrap(first.id)
        let secondID = try XCTUnwrap(second.id)
        try database.setBackgroundBackupNotificationEnabled(false, onSuccess: true, profileID: firstID)
        try database.setBackgroundBackupNotificationEnabled(false, onSuccess: false, profileID: secondID)
        let reopened = try DatabaseManager(databaseURL: databaseURL)
        let savedFirst = try XCTUnwrap(reopened.fetchServerProfile(id: firstID))
        let savedSecond = try XCTUnwrap(reopened.fetchServerProfile(id: secondID))
        XCTAssertFalse(savedFirst.backgroundBackupNotifyOnSuccess)
        XCTAssertTrue(savedFirst.backgroundBackupNotifyOnFailure)
        XCTAssertTrue(savedSecond.backgroundBackupNotifyOnSuccess)
        XCTAssertFalse(savedSecond.backgroundBackupNotifyOnFailure)
    }
}

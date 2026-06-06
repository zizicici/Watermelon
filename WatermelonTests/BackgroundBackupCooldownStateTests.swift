import XCTest
@testable import Watermelon

/// Repointing a background-enabled profile to a new remote must drop the per-remote cooldown so the
/// next background run does not skip the new (possibly never-backed-up) endpoint for up to the
/// cooldown window. Mirrors the repo_state / remoteVerifiedAt reset already covered for repoint.
final class BackgroundBackupCooldownStateTests: XCTestCase {
    private var tempDir: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        databaseManager = try DatabaseManager(databaseURL: tempDir.appendingPathComponent("db.sqlite"))
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        try? FileManager.default.removeItem(at: tempDir)
    }

    func testClearBackgroundBackupLastCompletedAtRemovesTimestamp() throws {
        let profileID: Int64 = 42
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        try databaseManager.setBackgroundBackupLastCompletedAt(now, profileID: profileID)
        XCTAssertNotNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: profileID))

        try databaseManager.clearBackgroundBackupLastCompletedAt(profileID: profileID)

        XCTAssertNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: profileID))
    }

    func testClearBackgroundBackupLastCompletedAtIsPerProfile() throws {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        try databaseManager.setBackgroundBackupLastCompletedAt(now, profileID: 1)
        try databaseManager.setBackgroundBackupLastCompletedAt(now, profileID: 2)

        try databaseManager.clearBackgroundBackupLastCompletedAt(profileID: 1)

        XCTAssertNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: 1))
        XCTAssertNotNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: 2))
    }

    func testClearBackgroundBackupLastCompletedAtIsIdempotentWhenAbsent() throws {
        XCTAssertNoThrow(try databaseManager.clearBackgroundBackupLastCompletedAt(profileID: 7))
        XCTAssertNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: 7))
    }
}

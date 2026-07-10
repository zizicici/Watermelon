import XCTest
@testable import Watermelon

final class ProfileRenameTests: XCTestCase {
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("WatermelonRenameTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        tempDBURL = directory.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let tempDBURL {
            try? FileManager.default.removeItem(at: tempDBURL.deletingLastPathComponent())
        }
    }

    private func makeProfile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil,
            name: "Old Name",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 3,
            host: "server.local",
            port: 445,
            shareName: "Photos",
            basePath: "/Watermelon",
            username: "user",
            domain: "WORKGROUP",
            credentialRef: "smb|server.local|445|Photos|WORKGROUP|user",
            backgroundBackupEnabled: true,
            backgroundBackupMinIntervalMinutes: 720,
            backgroundBackupRequiresWiFi: false,
            generateRemoteThumbnails: true,
            createdAt: Date(timeIntervalSince1970: 1_700_000_000),
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    func testRenameOnlyChangesNameAndUpdatedAt() throws {
        var profile = makeProfile()
        try databaseManager.saveServerProfile(&profile)
        let id = try XCTUnwrap(profile.id)
        let writerID = try XCTUnwrap(profile.writerID)
        let previousUpdatedAt = profile.updatedAt
        let markerDate = Date(timeIntervalSince1970: 1_700_100_000)
        try databaseManager.setBackgroundBackupLastRanAt(markerDate, profileID: id)
        try databaseManager.setBackgroundBackupLastCompletedAt(markerDate, profileID: id)
        try databaseManager.setRemoteVerifiedAt(markerDate, profileID: id)

        try databaseManager.setServerProfileName("New Name", profileID: id)

        let renamed = try XCTUnwrap(databaseManager.fetchServerProfiles().first)
        XCTAssertEqual(renamed.name, "New Name")
        XCTAssertGreaterThanOrEqual(renamed.updatedAt, previousUpdatedAt)
        XCTAssertEqual(renamed.writerID, writerID)
        XCTAssertEqual(renamed.host, profile.host)
        XCTAssertEqual(renamed.port, profile.port)
        XCTAssertEqual(renamed.shareName, profile.shareName)
        XCTAssertEqual(renamed.basePath, profile.basePath)
        XCTAssertEqual(renamed.username, profile.username)
        XCTAssertEqual(renamed.domain, profile.domain)
        XCTAssertEqual(renamed.credentialRef, profile.credentialRef)
        XCTAssertEqual(renamed.backgroundBackupEnabled, profile.backgroundBackupEnabled)
        XCTAssertEqual(renamed.backgroundBackupMinIntervalMinutes, profile.backgroundBackupMinIntervalMinutes)
        XCTAssertEqual(renamed.backgroundBackupRequiresWiFi, profile.backgroundBackupRequiresWiFi)
        XCTAssertEqual(renamed.generateRemoteThumbnails, profile.generateRemoteThumbnails)
        XCTAssertEqual(try databaseManager.backgroundBackupLastRanAt(profileID: id), markerDate)
        XCTAssertEqual(try databaseManager.backgroundBackupLastCompletedAt(profileID: id), markerDate)
        XCTAssertEqual(try databaseManager.remoteVerifiedAt(profileID: id), markerDate)
    }

    func testActiveSessionRenamePreservesConnectionAndPassword() throws {
        var profile = makeProfile()
        profile.id = 7
        let session = AppSession()
        session.activate(profile: profile, password: "secret")

        session.setActiveName("New Name", profileID: 7)

        XCTAssertEqual(session.activeProfile?.name, "New Name")
        XCTAssertEqual(session.activeProfile?.host, profile.host)
        XCTAssertEqual(session.activePassword, "secret")
    }

    func testSessionIgnoresRenameForAnotherProfile() {
        var profile = makeProfile()
        profile.id = 7
        let session = AppSession()
        session.activate(profile: profile, password: "secret")

        session.setActiveName("Wrong Name", profileID: 8)

        XCTAssertEqual(session.activeProfile?.name, "Old Name")
        XCTAssertEqual(session.activePassword, "secret")
    }
}

import XCTest
@testable import Watermelon

// Core-Bug-I P06: background run markers (lastRan/lastCompleted) are destination-scoped. A same-id repoint
// must not let the old destination's markers suppress automatic backup of the new endpoint's 18h cooldown.
final class BackgroundRunMarkerDestinationTests: XCTestCase {
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!
    private let base = Date(timeIntervalSince1970: 1_700_000_000)

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-bg-marker-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    private func makeProfile(
        host: String = "host.local",
        basePath: String = "/photos",
        connectionParams: Data? = nil
    ) -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil,
            name: "server",
            storageType: StorageType.smb.rawValue,
            connectionParams: connectionParams,
            sortOrder: 0,
            host: host,
            port: 445,
            shareName: "share",
            basePath: basePath,
            username: "user",
            domain: nil,
            credentialRef: "ref",
            backgroundBackupEnabled: true,
            createdAt: base,
            updatedAt: base,
            writerID: nil
        )
    }

    // MARK: - clearBackgroundBackupRunMarkers

    func testClearRemovesBothMarkersForProfile() throws {
        try databaseManager.setBackgroundBackupLastRanAt(base, profileID: 5)
        try databaseManager.setBackgroundBackupLastCompletedAt(base, profileID: 5)
        XCTAssertNotNil(try databaseManager.backgroundBackupLastRanAt(profileID: 5))
        XCTAssertNotNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: 5))

        try databaseManager.clearBackgroundBackupRunMarkers(profileID: 5)

        XCTAssertNil(try databaseManager.backgroundBackupLastRanAt(profileID: 5), "lastRan must be cleared")
        XCTAssertNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: 5), "lastCompleted must be cleared")
    }

    func testClearIsPerProfile() throws {
        try databaseManager.setBackgroundBackupLastCompletedAt(base, profileID: 5)
        try databaseManager.setBackgroundBackupLastCompletedAt(base, profileID: 9)

        try databaseManager.clearBackgroundBackupRunMarkers(profileID: 5)

        XCTAssertNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: 5))
        XCTAssertNotNil(try databaseManager.backgroundBackupLastCompletedAt(profileID: 9), "other profiles must be untouched")
    }

    func testClearOnAbsentMarkersIsNoOp() throws {
        XCTAssertNoThrow(try databaseManager.clearBackgroundBackupRunMarkers(profileID: 1))
        XCTAssertNil(try databaseManager.backgroundBackupLastRanAt(profileID: 1))
    }

    // MARK: - backgroundRunDestinationIdentity

    func testIdentityEqualForSameDestination() {
        XCTAssertEqual(
            makeProfile().backgroundRunDestinationIdentity,
            makeProfile().backgroundRunDestinationIdentity
        )
    }

    func testIdentityDiffersOnHostChange() {
        XCTAssertNotEqual(
            makeProfile(host: "old.local").backgroundRunDestinationIdentity,
            makeProfile(host: "new.local").backgroundRunDestinationIdentity
        )
    }

    func testIdentityDiffersOnBasePathChange() {
        XCTAssertNotEqual(
            makeProfile(basePath: "/photos").backgroundRunDestinationIdentity,
            makeProfile(basePath: "/photos-2").backgroundRunDestinationIdentity
        )
    }

    func testIdentityDiffersOnConnectionParamsChange() {
        XCTAssertNotEqual(
            makeProfile(connectionParams: Data("bucket-a".utf8)).backgroundRunDestinationIdentity,
            makeProfile(connectionParams: Data("bucket-b".utf8)).backgroundRunDestinationIdentity
        )
    }

    // MARK: - Foreground pickup marker domain

    // A run that already mutated a profile stamps its marker (gated by destination identity, not the enabled
    // flag), so disabling background backup for that profile before foreground pickup must not hide the run.
    @MainActor
    func testLatestBackgroundRunObservesDisabledProfileMarker() throws {
        var profile = makeProfile()   // backgroundBackupEnabled: true
        try databaseManager.saveServerProfile(&profile)
        guard let id = try databaseManager.fetchServerProfiles().first?.id else {
            return XCTFail("profile was not saved")
        }

        try databaseManager.setBackgroundBackupEnabled(false, profileID: id)
        let runDate = base.addingTimeInterval(3600)
        try databaseManager.setBackgroundBackupLastRanAt(runDate, profileID: id)

        // The enabled-only reader domain (the pre-fix bug) would miss this entirely.
        XCTAssertTrue(try databaseManager.fetchBackgroundBackupEnabledProfiles().isEmpty)

        XCTAssertEqual(
            HomeScreenStore._testLatestBackgroundRun(databaseManager),
            runDate,
            "a disabled profile's completed-run marker must remain visible to foreground pickup"
        )
    }
}

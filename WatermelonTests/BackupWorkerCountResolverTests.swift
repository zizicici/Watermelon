import XCTest
@testable import Watermelon

final class BackupWorkerCountResolverTests: XCTestCase {
    func testNodeWithoutModeInheritsManualGlobalDefault() {
        let profile = makeProfile(nodeMode: nil)

        XCTAssertEqual(
            BackupWorkerCountResolver.workerCountOverride(for: profile, globalDefault: .four),
            4
        )
    }

    func testNodeAutomaticOverridesManualGlobalDefault() {
        let profile = makeProfile(nodeMode: BackupWorkerCountMode.automatic.rawValue)

        XCTAssertNil(
            BackupWorkerCountResolver.workerCountOverride(for: profile, globalDefault: .four)
        )
    }

    func testNodeManualModeOverridesAutomaticGlobalDefault() {
        let profile = makeProfile(nodeMode: BackupWorkerCountMode.three.rawValue)

        XCTAssertEqual(
            BackupWorkerCountResolver.workerCountOverride(for: profile, globalDefault: .automatic),
            3
        )
    }

    func testInvalidPersistedNodeModeFallsBackToGlobalDefault() {
        let profile = makeProfile(nodeMode: 99)

        XCTAssertEqual(
            BackupWorkerCountResolver.workerCountOverride(for: profile, globalDefault: .two),
            2
        )
    }

    func testOneDriveNodeCanLowerAutomaticConcurrency() {
        let profile = makeProfile(
            storageType: .onedrive,
            nodeMode: BackupWorkerCountMode.one.rawValue
        )

        let override = BackupWorkerCountResolver.workerCountOverride(
            for: profile,
            globalDefault: .automatic
        )
        XCTAssertEqual(override, 1)
        XCTAssertEqual(
            BackupMonthScheduler.resolveWorkerCount(
                profile: profile,
                monthCount: 4,
                override: override
            ),
            1
        )
    }

    func testNodeCanOverrideConcurrencyUpToTwentyFour() {
        let profile = makeProfile(nodeMode: 24)
        let override = BackupWorkerCountResolver.workerCountOverride(
            for: profile,
            globalDefault: .automatic
        )

        XCTAssertEqual(override, 24)
        XCTAssertEqual(
            BackupMonthScheduler.resolveWorkerCount(
                profile: profile,
                monthCount: 30,
                override: override
            ),
            24
        )
    }

    func testHighNodeConcurrencyIsStillLimitedByMonthCount() {
        let profile = makeProfile(nodeMode: 24)

        XCTAssertEqual(
            BackupMonthScheduler.resolveWorkerCount(
                profile: profile,
                monthCount: 6,
                override: BackupWorkerCountResolver.workerCountOverride(for: profile)
            ),
            6
        )
    }

    func testHighConcurrencyAlsoAppliesToManifestDownloads() {
        let profile = makeProfile(storageType: .onedrive, nodeMode: 24)

        XCTAssertEqual(
            BackupRunPreparationService.resolveSyncDownloadConcurrency(
                profile: profile,
                override: BackupWorkerCountResolver.workerCountOverride(for: profile)
            ),
            24
        )
    }

    func testNodeSelectionRoundTripsPersistedModes() {
        for selection in NodeBackupWorkerCountSelection.allCases {
            XCTAssertEqual(
                NodeBackupWorkerCountSelection(persistedMode: selection.persistedMode),
                selection
            )
        }
    }

    func testGlobalAndNodeSelectionsExposeExpectedConcurrencyOptions() {
        XCTAssertEqual(
            BackupWorkerCountMode.allCases.map(\.rawValue),
            [0, 1, 2, 3, 4]
        )
        XCTAssertEqual(
            NodeBackupWorkerCountSelection.allCases.map(\.persistedMode),
            [nil, 0, 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24]
        )
    }

    func testDatabasePersistsModeAndConnectionEditPreservesIt() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let databaseManager = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var profile = makeProfile(id: nil, nodeMode: nil)
        try databaseManager.saveServerProfile(&profile)
        let profileID = try XCTUnwrap(profile.id)

        try databaseManager.setUploadWorkerCountMode(
            24,
            profileID: profileID
        )
        var connectionDraft = try XCTUnwrap(databaseManager.fetchServerProfile(id: profileID))
        XCTAssertEqual(connectionDraft.uploadWorkerCountMode, 24)

        connectionDraft.host = "new-server.local"
        connectionDraft.uploadWorkerCountMode = nil
        try databaseManager.saveConnectionProfile(&connectionDraft, editingProfileID: profileID)

        XCTAssertEqual(
            try databaseManager.fetchServerProfile(id: profileID)?.uploadWorkerCountMode,
            24
        )
    }

    func testDatabaseRejectsUnsupportedNodeConcurrency() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let databaseManager = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        var profile = makeProfile(id: nil, nodeMode: nil)
        try databaseManager.saveServerProfile(&profile)
        let profileID = try XCTUnwrap(profile.id)

        XCTAssertThrowsError(
            try databaseManager.setUploadWorkerCountMode(5, profileID: profileID)
        )
        XCTAssertThrowsError(
            try databaseManager.setUploadWorkerCountMode(25, profileID: profileID)
        )
    }

    private func makeProfile(
        id: Int64? = 1,
        storageType: StorageType = .smb,
        nodeMode: Int?
    ) -> ServerProfileRecord {
        var profile = ServerProfileRecord(
            id: id,
            name: "Test",
            storageType: storageType.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "server.local",
            port: 445,
            shareName: "photos",
            basePath: "",
            username: "user",
            domain: nil,
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
        profile.uploadWorkerCountMode = nodeMode
        return profile
    }
}

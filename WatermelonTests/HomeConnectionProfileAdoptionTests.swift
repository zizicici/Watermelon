import XCTest
@testable import Watermelon

@MainActor
final class HomeConnectionProfileAdoptionTests: XCTestCase {
    func testAdoptingPersistedHostKeyDisconnectsStaleRemoteIdentity() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("home-profile-adoption-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let database = try DatabaseManager(databaseURL: directory.appendingPathComponent("test.sqlite"))
        defer {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }
        let dependencies = DependencyContainer(
            databaseManager: database,
            startProfileReachability: false,
            reconcileOneDriveAccounts: false
        )
        var original = try makeSFTPProfile(hostKey: "old-key")
        try database.saveServerProfile(&original)
        try database.setActiveServerProfileID(original.id)
        dependencies.appSession.activate(profile: original, password: "secret")
        let generation = dependencies.appSession.snapshot.generation
        let controller = HomeConnectionController(dependencies: dependencies)
        var connectionChangeCount = 0
        var profileChangeCount = 0
        controller.onStateChanged = { connectionChangeCount += 1 }
        controller.onProfilesChanged = { profileChangeCount += 1 }

        var updated = try makeSFTPProfile(hostKey: "new-key")
        updated.id = original.id
        updated.createdAt = original.createdAt
        try database.saveServerProfile(&updated)
        controller.adoptPersistedConnectionProfile(updated)

        XCTAssertEqual(connectionChangeCount, 1)
        XCTAssertEqual(profileChangeCount, 1)
        XCTAssertEqual(dependencies.appSession.snapshot.generation, generation + 1)
        XCTAssertNil(dependencies.appSession.activePassword)
        XCTAssertNil(dependencies.appSession.activeProfile)
        XCTAssertNil(try database.activeServerProfileID())
        XCTAssertEqual(controller.savedProfiles.first?.sftpParams?.hostKeyFingerprintSHA256, "new-key")
    }

    private func makeSFTPProfile(hostKey: String) throws -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil,
            name: "SFTP",
            storageType: StorageType.sftp.rawValue,
            connectionParams: try ServerProfileRecord.encodedConnectionParams(
                SFTPConnectionParams(authMethod: .password, hostKeyFingerprintSHA256: hostKey)
            ),
            sortOrder: 0,
            host: "server.local",
            port: 22,
            shareName: "",
            basePath: "/Watermelon",
            username: "user",
            domain: "",
            credentialRef: "sftp|server.local|22|user",
            backgroundBackupEnabled: false,
            backgroundBackupMinIntervalMinutes: 720,
            backgroundBackupRequiresWiFi: true,
            generateRemoteThumbnails: false,
            createdAt: Date(timeIntervalSince1970: 1_700_000_000),
            updatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }
}

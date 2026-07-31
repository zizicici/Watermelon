import XCTest
@testable import WatermelonMac

final class MacSFTPProfileSaveFingerprintResolverTests:
    XCTestCase
{
    func testEditUsesLatestPersistedFingerprint() throws {
        let fixture = try makeFixture(fingerprint: "old-key")
        defer { fixture.remove() }
        let openedSnapshot = fixture.profile

        var updated = fixture.profile
        updated.connectionParams =
            try ServerProfileRecord.encodedConnectionParams(
                SFTPConnectionParams(
                    authMethod: .password,
                    hostKeyFingerprintSHA256: "new-key"
                )
            )
        try fixture.database.saveConnectionProfile(
            &updated,
            editingProfileID: fixture.profile.id
        )

        XCTAssertEqual(
            try MacSFTPProfileSaveFingerprintResolver.resolve(
                editingSnapshot: openedSnapshot,
                proposedHost: "nas.local",
                proposedPort: 22,
                testedHostKey: nil,
                databaseManager: fixture.database
            ),
            "new-key"
        )
    }

    func testMatchingConnectionTestOverridesPersistedFingerprint()
        throws
    {
        let fixture = try makeFixture(fingerprint: "persisted-key")
        defer { fixture.remove() }

        XCTAssertEqual(
            try MacSFTPProfileSaveFingerprintResolver.resolve(
                editingSnapshot: fixture.profile,
                proposedHost: "nas.local",
                proposedPort: 22,
                testedHostKey: MacSFTPTestedHostKey(
                    host: "NAS.LOCAL.",
                    port: 22,
                    fingerprint: "tested-key"
                ),
                databaseManager: fixture.database
            ),
            "tested-key"
        )
    }

    func testMissingEditingProfileFailsInsteadOfUsingSnapshot()
        throws
    {
        let fixture = try makeFixture(fingerprint: "old-key")
        defer { fixture.remove() }
        let openedSnapshot = fixture.profile
        let profileID = try XCTUnwrap(openedSnapshot.id)
        try fixture.database.deleteServerProfile(id: profileID)

        XCTAssertThrowsError(
            try MacSFTPProfileSaveFingerprintResolver.resolve(
                editingSnapshot: openedSnapshot,
                proposedHost: "nas.local",
                proposedPort: 22,
                testedHostKey: nil,
                databaseManager: fixture.database
            )
        )
    }

    private func makeFixture(
        fingerprint: String
    ) throws -> Fixture {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "WatermelonMacSFTPTests-\(UUID().uuidString)",
                isDirectory: true
            )
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        let database = try DatabaseManager(
            databaseURL: directory.appendingPathComponent("test.sqlite")
        )
        var profile = ServerProfileRecord(
            id: nil,
            name: "SFTP",
            storageType: StorageType.sftp.rawValue,
            connectionParams:
                try ServerProfileRecord.encodedConnectionParams(
                    SFTPConnectionParams(
                        authMethod: .password,
                        hostKeyFingerprintSHA256: fingerprint
                    )
                ),
            sortOrder: 0,
            host: "nas.local",
            port: 22,
            shareName: "",
            basePath: "/Watermelon",
            username: "user",
            domain: nil,
            credentialRef: "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
        try database.saveConnectionProfile(
            &profile,
            editingProfileID: nil
        )
        return Fixture(
            directory: directory,
            database: database,
            profile: profile
        )
    }

    private struct Fixture {
        let directory: URL
        let database: DatabaseManager
        let profile: ServerProfileRecord

        func remove() {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }
    }
}

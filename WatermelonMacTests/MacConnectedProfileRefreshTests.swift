import XCTest
@testable import WatermelonMac

final class MacConnectedProfileRefreshTests: XCTestCase {
    func testMetadataRefreshPreservesGenerationAndIgnoresBackgroundFlag() {
        let appSession = AppSession()
        let profile = makeProfile()
        appSession.activate(profile: profile, password: "secret")
        let generation = appSession.snapshot.generation

        var updated = profile
        updated.name = "Renamed"
        updated.backgroundBackupEnabled = true
        updated.generateRemoteThumbnails = true
        updated.uploadWorkerCountMode = 8
        updated.updatedAt = Date()

        XCTAssertEqual(
            MacConnectedProfileRefresh.apply(
                updatedProfile: updated,
                updatedCredential: "secret",
                to: appSession
            ),
            .metadata
        )
        let snapshot = appSession.snapshot
        XCTAssertEqual(snapshot.generation, generation)
        XCTAssertEqual(snapshot.activeProfile?.name, "Renamed")
        XCTAssertEqual(
            snapshot.activeProfile?.backgroundBackupEnabled,
            false
        )
        XCTAssertEqual(
            snapshot.activeProfile?.generateRemoteThumbnails,
            true
        )
        XCTAssertEqual(
            snapshot.activeProfile?.uploadWorkerCountMode,
            8
        )
    }

    func testCredentialChangeReactivatesSession() {
        let appSession = AppSession()
        var profile = makeProfile()
        profile.backgroundBackupEnabled = true
        appSession.activate(profile: profile, password: "old")
        let generation = appSession.snapshot.generation

        XCTAssertEqual(
            MacConnectedProfileRefresh.apply(
                updatedProfile: profile,
                updatedCredential: "new",
                to: appSession
            ),
            .reactivation
        )
        XCTAssertEqual(
            appSession.snapshot.generation,
            generation + 1
        )
        XCTAssertEqual(appSession.activePassword, "new")
        XCTAssertFalse(
            appSession.snapshot.activeProfile?
                .backgroundBackupEnabled ?? true
        )
    }

    func testConnectionChangeReactivatesSession() {
        let appSession = AppSession()
        let profile = makeProfile()
        appSession.activate(profile: profile, password: "secret")
        let generation = appSession.snapshot.generation
        var updated = profile
        updated.basePath = "/other"

        XCTAssertEqual(
            MacConnectedProfileRefresh.apply(
                updatedProfile: updated,
                updatedCredential: "secret",
                to: appSession
            ),
            .reactivation
        )
        XCTAssertEqual(
            appSession.snapshot.generation,
            generation + 1
        )
        XCTAssertEqual(
            appSession.activeProfile?.basePath,
            "/other"
        )
    }

    func testCredentialReferenceChangeReactivatesSession() {
        let appSession = AppSession()
        let profile = makeProfile()
        appSession.activate(profile: profile, password: "secret")
        let generation = appSession.snapshot.generation
        var updated = profile
        updated.credentialRef = "replacement"

        XCTAssertEqual(
            MacConnectedProfileRefresh.apply(
                updatedProfile: updated,
                updatedCredential: "secret",
                to: appSession
            ),
            .reactivation
        )
        XCTAssertEqual(
            appSession.snapshot.generation,
            generation + 1
        )
        XCTAssertEqual(
            appSession.activeProfile?.credentialRef,
            "replacement"
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
}

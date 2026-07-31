import XCTest
@testable import WatermelonMac

final class MacRepositoryMaintenanceSessionPolicyTests: XCTestCase {
    func testKeepsWindowForCapturedSession() {
        let profile = makeProfile(id: 7, host: "nas.local")

        XCTAssertFalse(
            MacRepositoryMaintenanceSessionPolicy.shouldClose(
                representedProfile: profile,
                representedGeneration: 3,
                current: snapshot(
                    profile: profile,
                    password: "secret",
                    generation: 3
                ),
                isBusy: false
            )
        )
    }

    func testClosesIdleWindowAfterSwitchOrDisconnect() {
        let profile = makeProfile(id: 7, host: "nas.local")

        XCTAssertTrue(
            MacRepositoryMaintenanceSessionPolicy.shouldClose(
                representedProfile: profile,
                representedGeneration: 3,
                current: snapshot(
                    profile: makeProfile(id: 8, host: "other.local"),
                    password: "secret",
                    generation: 4
                ),
                isBusy: false
            )
        )
        XCTAssertTrue(
            MacRepositoryMaintenanceSessionPolicy.shouldClose(
                representedProfile: profile,
                representedGeneration: 3,
                current: snapshot(
                    profile: nil,
                    password: nil,
                    generation: 4
                ),
                isBusy: false
            )
        )
    }

    func testClosesIdleWindowAfterSameIDDestinationChange() {
        XCTAssertTrue(
            MacRepositoryMaintenanceSessionPolicy.shouldClose(
                representedProfile: makeProfile(
                    id: 7,
                    host: "nas-a.local"
                ),
                representedGeneration: 3,
                current: snapshot(
                    profile: makeProfile(
                        id: 7,
                        host: "nas-b.local"
                    ),
                    password: "secret",
                    generation: 3
                ),
                isBusy: false
            )
        )
    }

    func testClosesIdleWindowAfterCredentialReactivation() {
        let profile = makeProfile(id: 7, host: "nas.local")

        XCTAssertTrue(
            MacRepositoryMaintenanceSessionPolicy.shouldClose(
                representedProfile: profile,
                representedGeneration: 3,
                current: snapshot(
                    profile: profile,
                    password: "new-secret",
                    generation: 4
                ),
                isBusy: false
            )
        )
    }

    func testDoesNotInterruptRunningMaintenance() {
        let profile = makeProfile(id: 7, host: "nas.local")

        XCTAssertFalse(
            MacRepositoryMaintenanceSessionPolicy.shouldClose(
                representedProfile: profile,
                representedGeneration: 3,
                current: snapshot(
                    profile: makeProfile(id: 8, host: "other.local"),
                    password: "other-secret",
                    generation: 4
                ),
                isBusy: true
            )
        )
    }

    func testContextCapturesAndRevalidatesCredential() {
        let profile = makeProfile(id: 7, host: "nas.local")
        let context = MacRepositoryMaintenanceContext.capture(
            representedProfile: profile,
            representedGeneration: 3,
            current: snapshot(
                profile: profile,
                password: "secret",
                generation: 3
            )
        )

        XCTAssertEqual(context?.credential, "secret")
        XCTAssertTrue(
            context?.isCurrent(
                snapshot(
                    profile: profile,
                    password: "secret",
                    generation: 3
                )
            ) == true
        )
        XCTAssertFalse(
            context?.isCurrent(
                snapshot(
                    profile: profile,
                    password: "changed",
                    generation: 3
                )
            ) == true
        )
    }

    func testContextRejectsMissingStoredCredential() {
        let profile = makeProfile(id: 7, host: "nas.local")

        XCTAssertNil(
            MacRepositoryMaintenanceContext.capture(
                representedProfile: profile,
                representedGeneration: 3,
                current: snapshot(
                    profile: profile,
                    password: nil,
                    generation: 3
                )
            )
        )
    }

    private func snapshot(
        profile: ServerProfileRecord?,
        password: String?,
        generation: UInt64
    ) -> AppSession.Snapshot {
        AppSession.Snapshot(
            activeProfile: profile,
            activePassword: password,
            generation: generation
        )
    }

    private func makeProfile(
        id: Int64,
        host: String
    ) -> ServerProfileRecord {
        ServerProfileRecord(
            id: id,
            name: "Archive",
            storageType: StorageType.smb.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: host,
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

import XCTest
@testable import WatermelonMac

final class MacRemoteThumbnailSettingsPolicyTests: XCTestCase {
    func testEnablingForActiveProfileOffersBackfill() {
        XCTAssertTrue(
            shouldOfferBackfill(
                wasEnabled: false,
                isEnabled: true,
                profileIsActive: true
            )
        )
    }

    func testEnablingForInactiveProfileDoesNotOfferBackfill() {
        XCTAssertFalse(
            shouldOfferBackfill(
                wasEnabled: false,
                isEnabled: true,
                profileIsActive: false
            )
        )
    }

    func testDisablingDoesNotOfferBackfill() {
        XCTAssertFalse(
            shouldOfferBackfill(
                wasEnabled: true,
                isEnabled: false,
                profileIsActive: true
            )
        )
    }

    func testUnchangedEnabledStateDoesNotOfferBackfill() {
        XCTAssertFalse(
            shouldOfferBackfill(
                wasEnabled: true,
                isEnabled: true,
                profileIsActive: true
            )
        )
    }

    func testMaintenanceRequiresEveryActivityGateToBeClear() {
        XCTAssertTrue(
            canStartMaintenance(
                taskRunning: false,
                executionActive: false,
                remoteMaintenanceActive: false,
                connectionActive: false,
                profileIsActive: true
            )
        )
        for blocked in [
            (true, false, false, false, true),
            (false, true, false, false, true),
            (false, false, true, false, true),
            (false, false, false, true, true),
            (false, false, false, false, false),
        ] {
            XCTAssertFalse(
                canStartMaintenance(
                    taskRunning: blocked.0,
                    executionActive: blocked.1,
                    remoteMaintenanceActive: blocked.2,
                    connectionActive: blocked.3,
                    profileIsActive: blocked.4
                )
            )
        }
    }

    func testMaintenanceContextCapturesActiveSession() {
        let profile = makeProfile(host: "nas.local")

        let context = MacRemoteThumbnailMaintenanceContext.capture(
            selectedProfile: profile,
            current: snapshot(
                profile: profile,
                credential: "secret",
                generation: 3
            )
        )

        XCTAssertEqual(context?.credential, "secret")
        XCTAssertEqual(context?.sessionGeneration, 3)
        XCTAssertTrue(
            context?.isCurrent(
                snapshot(
                    profile: profile,
                    credential: "secret",
                    generation: 3
                )
            ) == true
        )
    }

    func testMaintenanceContextRejectsDifferentDestination() {
        let selected = makeProfile(host: "nas-a.local")
        let active = makeProfile(host: "nas-b.local")

        XCTAssertNil(
            MacRemoteThumbnailMaintenanceContext.capture(
                selectedProfile: selected,
                current: snapshot(
                    profile: active,
                    credential: "secret",
                    generation: 3
                )
            )
        )
    }

    func testMaintenanceContextRejectsGenerationChange() {
        let profile = makeProfile(host: "nas.local")
        let context = MacRemoteThumbnailMaintenanceContext.capture(
            selectedProfile: profile,
            current: snapshot(
                profile: profile,
                credential: "secret",
                generation: 3
            )
        )

        XCTAssertFalse(
            context?.isCurrent(
                snapshot(
                    profile: profile,
                    credential: "secret",
                    generation: 4
                )
            ) == true
        )
    }

    func testMaintenanceContextRejectsCredentialChange() {
        let profile = makeProfile(host: "nas.local")
        let context = MacRemoteThumbnailMaintenanceContext.capture(
            selectedProfile: profile,
            current: snapshot(
                profile: profile,
                credential: "old",
                generation: 3
            )
        )

        XCTAssertFalse(
            context?.isCurrent(
                snapshot(
                    profile: profile,
                    credential: "new",
                    generation: 3
                )
            ) == true
        )
    }

    private func shouldOfferBackfill(
        wasEnabled: Bool,
        isEnabled: Bool,
        profileIsActive: Bool
    ) -> Bool {
        MacRemoteThumbnailSettingsPolicy.shouldOfferBackfill(
            wasEnabled: wasEnabled,
            isEnabled: isEnabled,
            profileIsActive: profileIsActive
        )
    }

    private func canStartMaintenance(
        taskRunning: Bool,
        executionActive: Bool,
        remoteMaintenanceActive: Bool,
        connectionActive: Bool,
        profileIsActive: Bool
    ) -> Bool {
        MacRemoteThumbnailMaintenanceAvailabilityPolicy.canStart(
            taskRunning: taskRunning,
            executionActive: executionActive,
            remoteMaintenanceActive: remoteMaintenanceActive,
            connectionActive: connectionActive,
            profileIsActive: profileIsActive
        )
    }

    private func snapshot(
        profile: ServerProfileRecord?,
        credential: String?,
        generation: UInt64
    ) -> AppSession.Snapshot {
        AppSession.Snapshot(
            activeProfile: profile,
            activePassword: credential,
            generation: generation
        )
    }

    private func makeProfile(host: String) -> ServerProfileRecord {
        ServerProfileRecord(
            id: 7,
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

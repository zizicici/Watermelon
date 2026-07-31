import XCTest
@testable import WatermelonMac

final class MacPhotoBrowserSessionPolicyTests: XCTestCase {
    func testMatchingCapturedSessionIsAccepted() {
        let profile = makeProfile(id: 7, host: "nas.local")

        XCTAssertTrue(
            MacPhotoBrowserSessionPolicy.matches(
                capturedProfile: profile,
                capturedGeneration: 3,
                current: snapshot(
                    profile: profile,
                    generation: 3
                )
            )
        )
    }

    func testGenerationChangeRejectsCapturedSession() {
        let profile = makeProfile(id: 7, host: "nas.local")

        XCTAssertFalse(
            MacPhotoBrowserSessionPolicy.matches(
                capturedProfile: profile,
                capturedGeneration: 3,
                current: snapshot(
                    profile: profile,
                    generation: 4
                )
            )
        )
    }

    func testDestinationChangeRejectsCapturedSession() {
        XCTAssertFalse(
            MacPhotoBrowserSessionPolicy.matches(
                capturedProfile: makeProfile(
                    id: 7,
                    host: "nas-a.local"
                ),
                capturedGeneration: 3,
                current: snapshot(
                    profile: makeProfile(
                        id: 7,
                        host: "nas-b.local"
                    ),
                    generation: 3
                )
            )
        )
    }

    func testSessionStateUsesOneSnapshot() {
        let profile = makeProfile(id: 7, host: "nas.local")
        let state = MacPhotoBrowserSessionState(
            snapshot: snapshot(
                profile: profile,
                password: "captured-secret",
                generation: 3
            )
        )

        XCTAssertEqual(state.profile?.id, profile.id)
        XCTAssertEqual(state.credential, "captured-secret")
        XCTAssertEqual(state.generation, 3)
        XCTAssertTrue(state.hasUsableRemoteSession)
    }

    func testSessionStateDoesNotAdoptProfileWhenDisconnected() {
        let state = MacPhotoBrowserSessionState(
            snapshot: snapshot(
                profile: nil,
                password: "stale-secret",
                generation: 4
            )
        )

        XCTAssertNil(state.profile)
        XCTAssertNil(state.credential)
        XCTAssertEqual(state.generation, 4)
        XCTAssertFalse(state.hasUsableRemoteSession)
    }

    func testSessionStateRequiresCredentialForRemoteBackend() {
        let state = MacPhotoBrowserSessionState(
            snapshot: snapshot(
                profile: makeProfile(id: 7, host: "nas.local"),
                password: nil,
                generation: 3
            )
        )

        XCTAssertNotNil(state.profile)
        XCTAssertNil(state.credential)
        XCTAssertFalse(state.hasUsableRemoteSession)
    }

    func testSessionStateNormalizesCredentiallessBackend() {
        let profile = makeProfile(
            id: 7,
            storageType: .externalVolume,
            host: ""
        )
        let state = MacPhotoBrowserSessionState(
            snapshot: snapshot(
                profile: profile,
                password: nil,
                generation: 3
            )
        )

        XCTAssertEqual(state.profile?.id, profile.id)
        XCTAssertEqual(state.credential, "")
        XCTAssertTrue(state.hasUsableRemoteSession)
    }

    func testRemoteReadContextRequiresDisplayedProjection() {
        let profile = makeProfile(id: 7, host: "nas.local")
        let context = MacPhotoBrowserRemoteReadContext(
            profile: profile,
            credential: "captured-secret",
            sessionGeneration: 3
        )

        XCTAssertFalse(
            context.isCurrent(
                displayedSessionGeneration: 2,
                current: snapshot(
                    profile: profile,
                    generation: 3
                )
            )
        )
    }

    func testRemoteReadContextRejectsSessionSwitch() {
        let context = MacPhotoBrowserRemoteReadContext(
            profile: makeProfile(id: 7, host: "nas-a.local"),
            credential: "captured-secret",
            sessionGeneration: 3
        )

        XCTAssertFalse(
            context.isCurrent(
                displayedSessionGeneration: 3,
                current: snapshot(
                    profile: makeProfile(
                        id: 8,
                        host: "nas-b.local"
                    ),
                    generation: 4
                )
            )
        )
    }

    func testRemoteReadContextAcceptsCapturedProjection() {
        let profile = makeProfile(id: 7, host: "nas.local")
        let context = MacPhotoBrowserRemoteReadContext(
            profile: profile,
            credential: "captured-secret",
            sessionGeneration: 3
        )

        XCTAssertTrue(
            context.isCurrent(
                displayedSessionGeneration: 3,
                current: snapshot(
                    profile: profile,
                    generation: 3
                )
            )
        )
    }

    private func snapshot(
        profile: ServerProfileRecord?,
        password: String? = "secret",
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
        storageType: StorageType = .smb,
        host: String
    ) -> ServerProfileRecord {
        ServerProfileRecord(
            id: id,
            name: "Archive",
            storageType: storageType.rawValue,
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

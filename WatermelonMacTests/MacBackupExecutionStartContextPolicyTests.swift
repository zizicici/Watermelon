import XCTest
@testable import WatermelonMac

final class MacBackupExecutionStartContextPolicyTests: XCTestCase {
    func testMatchingConnectedSessionAllowsStart() {
        XCTAssertTrue(
            allowsStart(
                profileID: 7,
                expectedGeneration: 3,
                session: snapshot(
                    profile: makeProfile(id: 7),
                    password: "secret",
                    generation: 3
                )
            )
        )
    }

    func testChangedSessionGenerationRejectsConfirmedRequest() {
        XCTAssertFalse(
            allowsStart(
                profileID: 7,
                expectedGeneration: 2,
                session: snapshot(
                    profile: makeProfile(id: 7),
                    password: "secret",
                    generation: 3
                )
            )
        )
    }

    func testDifferentOrMissingProfileRejectsStart() {
        let current = snapshot(
            profile: makeProfile(id: 8),
            password: "secret",
            generation: 3
        )

        XCTAssertFalse(
            allowsStart(
                profileID: 7,
                expectedGeneration: nil,
                session: current
            )
        )
        XCTAssertFalse(
            allowsStart(
                profileID: nil,
                expectedGeneration: nil,
                session: current
            )
        )
    }

    func testStoredCredentialRequirementIsCheckedBeforeLease() {
        XCTAssertFalse(
            allowsStart(
                profileID: 7,
                expectedGeneration: nil,
                session: snapshot(
                    profile: makeProfile(id: 7),
                    password: nil,
                    generation: 3
                )
            )
        )
        XCTAssertTrue(
            allowsStart(
                profileID: 9,
                expectedGeneration: nil,
                session: snapshot(
                    profile: makeProfile(
                        id: 9,
                        storageType: .externalVolume
                    ),
                    password: nil,
                    generation: 4
                )
            )
        )
    }

    private func allowsStart(
        profileID: Int64?,
        expectedGeneration: UInt64?,
        session: AppSession.Snapshot
    ) -> Bool {
        MacBackupExecutionStartContextPolicy.allowsStart(
            profileID: profileID,
            expectedSessionGeneration: expectedGeneration,
            session: session
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
        storageType: StorageType = .smb
    ) -> ServerProfileRecord {
        ServerProfileRecord(
            id: id,
            name: "Archive",
            storageType: storageType.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "nas.local",
            port: 445,
            shareName: "photos",
            basePath: "/Watermelon",
            username: "user",
            domain: nil,
            credentialRef: storageType == .externalVolume
                ? ""
                : "credential",
            createdAt: Date(),
            updatedAt: Date()
        )
    }
}

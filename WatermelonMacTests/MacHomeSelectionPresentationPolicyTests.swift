import XCTest
@testable import WatermelonMac

final class MacHomeSelectionPresentationPolicyTests:
    XCTestCase
{
    func testRemoteSelectionRequiresCurrentRemoteSnapshot() {
        XCTAssertFalse(
            MacHomeSelectionPresentationPolicy
                .remoteSelectionEnabled(
                    selectionEnabled: true,
                    remoteSnapshotReady: false,
                    isSpecificAlbums: false
                )
        )
        XCTAssertTrue(
            MacHomeSelectionPresentationPolicy
                .remoteSelectionEnabled(
                    selectionEnabled: true,
                    remoteSnapshotReady: true,
                    isSpecificAlbums: false
                )
        )
    }

    func testRemoteSelectionRespectsGeneralAndAlbumRestrictions() {
        XCTAssertFalse(
            MacHomeSelectionPresentationPolicy
                .remoteSelectionEnabled(
                    selectionEnabled: false,
                    remoteSnapshotReady: true,
                    isSpecificAlbums: false
                )
        )
        XCTAssertFalse(
            MacHomeSelectionPresentationPolicy
                .remoteSelectionEnabled(
                    selectionEnabled: true,
                    remoteSnapshotReady: true,
                    isSpecificAlbums: true
                )
        )
    }
}

final class MacHomeRemoteSnapshotPolicyTests: XCTestCase {
    private let currentGeneration: UInt64 = 4

    func testAcceptsSnapshotOwnedBySelectedConnection() {
        let profile = makeProfile(id: 7, host: "nas.local")
        let state = makeState(profile: profile)
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: state,
            requestedActiveConnection: true,
            sourceSessionGeneration: currentGeneration,
            currentSessionGeneration: currentGeneration,
            activeSessionProfile: profile,
            selectedProfile: profile,
            connectedProfile: profile
        )

        XCTAssertTrue(application.shouldApply)
        XCTAssertTrue(application.hasActiveConnection)
        XCTAssertEqual(
            application.state?.profileKey,
            state.profileKey
        )
    }

    func testRejectsForeignSnapshotOwner() {
        let profile = makeProfile(id: 7, host: "nas.local")
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: makeState(
                profile: makeProfile(
                    id: 8,
                    host: "other.local"
                )
            ),
            requestedActiveConnection: true,
            sourceSessionGeneration: currentGeneration,
            currentSessionGeneration: currentGeneration,
            activeSessionProfile: profile,
            selectedProfile: profile,
            connectedProfile: profile
        )

        XCTAssertTrue(application.shouldApply)
        XCTAssertFalse(application.hasActiveConnection)
        XCTAssertNil(application.state)
    }

    func testRejectsSelectedProfileDifferentFromConnection() {
        let connected = makeProfile(id: 7, host: "nas.local")
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: makeState(profile: connected),
            requestedActiveConnection: true,
            sourceSessionGeneration: currentGeneration,
            currentSessionGeneration: currentGeneration,
            activeSessionProfile: connected,
            selectedProfile: makeProfile(
                id: 8,
                host: "other.local"
            ),
            connectedProfile: connected
        )

        XCTAssertTrue(application.shouldApply)
        XCTAssertFalse(application.hasActiveConnection)
        XCTAssertNil(application.state)
    }

    func testRejectsSameIDWithDifferentDestination() {
        let connected = makeProfile(id: 7, host: "nas-a.local")
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: makeState(profile: connected),
            requestedActiveConnection: true,
            sourceSessionGeneration: currentGeneration,
            currentSessionGeneration: currentGeneration,
            activeSessionProfile: connected,
            selectedProfile: makeProfile(
                id: 7,
                host: "nas-b.local"
            ),
            connectedProfile: connected
        )

        XCTAssertTrue(application.shouldApply)
        XCTAssertFalse(application.hasActiveConnection)
        XCTAssertNil(application.state)
    }

    func testCurrentGenerationInactiveConnectionClearsSnapshot() {
        let profile = makeProfile(id: 7, host: "nas.local")
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: makeState(profile: profile),
            requestedActiveConnection: false,
            sourceSessionGeneration: currentGeneration,
            currentSessionGeneration: currentGeneration,
            activeSessionProfile: nil,
            selectedProfile: profile,
            connectedProfile: profile
        )

        XCTAssertTrue(application.shouldApply)
        XCTAssertFalse(application.hasActiveConnection)
        XCTAssertNil(application.state)
    }

    func testIgnoresStaleGenerationWithoutClearingCurrentProjection() {
        let profile = makeProfile(id: 7, host: "nas.local")
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: makeState(profile: profile),
            requestedActiveConnection: true,
            sourceSessionGeneration: currentGeneration - 1,
            currentSessionGeneration: currentGeneration,
            activeSessionProfile: profile,
            selectedProfile: profile,
            connectedProfile: profile
        )

        XCTAssertFalse(application.shouldApply)
        XCTAssertFalse(application.hasActiveConnection)
        XCTAssertNil(application.state)
    }

    func testCurrentGenerationRequiresMatchingActiveSessionProfile() {
        let connected = makeProfile(id: 7, host: "nas.local")
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: makeState(profile: connected),
            requestedActiveConnection: true,
            sourceSessionGeneration: currentGeneration,
            currentSessionGeneration: currentGeneration,
            activeSessionProfile: makeProfile(
                id: 8,
                host: "other.local"
            ),
            selectedProfile: connected,
            connectedProfile: connected
        )

        XCTAssertTrue(application.shouldApply)
        XCTAssertFalse(application.hasActiveConnection)
        XCTAssertNil(application.state)
    }

    func testRejectsSameIDActiveSessionWithDifferentDestination() {
        let connected = makeProfile(id: 7, host: "nas-a.local")
        let application = MacHomeRemoteSnapshotPolicy.resolve(
            state: makeState(profile: connected),
            requestedActiveConnection: true,
            sourceSessionGeneration: currentGeneration,
            currentSessionGeneration: currentGeneration,
            activeSessionProfile: makeProfile(
                id: 7,
                host: "nas-b.local"
            ),
            selectedProfile: connected,
            connectedProfile: connected
        )

        XCTAssertTrue(application.shouldApply)
        XCTAssertFalse(application.hasActiveConnection)
        XCTAssertNil(application.state)
    }

    private func makeState(
        profile: ServerProfileRecord
    ) -> RemoteLibrarySnapshotState {
        RemoteLibrarySnapshotState(
            revision: 1,
            isFullSnapshot: true,
            monthDeltas: [],
            profileKey:
                RemoteIndexSyncService.remoteProfileKey(profile)
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

final class MacHomeExecutionStartPolicyTests: XCTestCase {
    func testLocalOnlyBackupCanStartWhileRemoteProjectionLoads() {
        XCTAssertTrue(
            MacHomeExecutionStartPolicy.isEnabled(
                selectionEnabled: true,
                hasSelection: true,
                hasRemoteSelection: false,
                remoteSnapshotReady: false
            )
        )
        XCTAssertFalse(
            MacHomeExecutionStartPolicy.isEnabled(
                selectionEnabled: true,
                hasSelection: false,
                hasRemoteSelection: false,
                remoteSnapshotReady: false
            )
        )
    }

    func testRemoteSelectionWaitsForCurrentRemoteProjection() {
        XCTAssertFalse(
            MacHomeExecutionStartPolicy.isEnabled(
                selectionEnabled: true,
                hasSelection: true,
                hasRemoteSelection: true,
                remoteSnapshotReady: false
            )
        )
        XCTAssertTrue(
            MacHomeExecutionStartPolicy.isEnabled(
                selectionEnabled: true,
                hasSelection: true,
                hasRemoteSelection: true,
                remoteSnapshotReady: true
            )
        )
    }
}

final class MacHomeExecutionPresentationPolicyTests: XCTestCase {
    func testManualExecutionPresentationTakesPriority() {
        XCTAssertFalse(
            MacHomeExecutionPresentationPolicy
                .shouldApplyExternalPresentation(
                    manualExecutionActive: true
                )
        )
        XCTAssertTrue(
            MacHomeExecutionPresentationPolicy
                .shouldApplyExternalPresentation(
                    manualExecutionActive: false
                )
        )
    }
}

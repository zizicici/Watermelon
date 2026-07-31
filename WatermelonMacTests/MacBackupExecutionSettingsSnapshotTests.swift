import XCTest
@testable import WatermelonMac

final class MacBackupExecutionSettingsSnapshotTests:
    XCTestCase
{
    func testCaptureFreezesInheritedWorkerCountAndICloudMode() {
        var profile = makeProfile(workerCountMode: nil)
        let snapshot =
            MacBackupExecutionSettingsSnapshot.capture(
                profile: profile,
                globalWorkerCountMode: .three,
                iCloudMode: .enable
            )

        profile.uploadWorkerCountMode =
            BackupWorkerCountMode.one.rawValue

        XCTAssertEqual(snapshot.uploadWorkerCountOverride, 3)
        XCTAssertEqual(snapshot.iCloudMode, .enable)
    }

    func testNodeWorkerCountOverrideIsCaptured() {
        let snapshot =
            MacBackupExecutionSettingsSnapshot.capture(
                profile: makeProfile(workerCountMode: 12),
                globalWorkerCountMode: .two,
                iCloudMode: .disable
            )

        XCTAssertEqual(snapshot.uploadWorkerCountOverride, 12)
        XCTAssertEqual(snapshot.iCloudMode, .disable)
    }

    func testICloudPreflightCanForceSingleWorker() {
        let snapshot =
            MacBackupExecutionSettingsSnapshot.capture(
                profile: makeProfile(workerCountMode: 12),
                globalWorkerCountMode: .automatic,
                iCloudMode: .enable
            )

        XCTAssertEqual(
            snapshot.uploadWorkerCountOverride(
                requiresSingleWorker: true
            ),
            1
        )
        XCTAssertEqual(
            snapshot.uploadWorkerCountOverride(
                requiresSingleWorker: false
            ),
            12
        )
    }

    private func makeProfile(
        workerCountMode: Int?
    ) -> ServerProfileRecord {
        var profile = ServerProfileRecord(
            id: 7,
            name: "SMB",
            storageType: StorageType.smb.rawValue,
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
        profile.uploadWorkerCountMode = workerCountMode
        return profile
    }
}

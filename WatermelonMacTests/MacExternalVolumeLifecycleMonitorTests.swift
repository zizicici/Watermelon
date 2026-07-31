import AppKit
import XCTest
@testable import WatermelonMac

@MainActor
final class MacExternalVolumeLifecycleMonitorTests: XCTestCase {
    func testMatchingUnmountDisconnectsAndRefreshesReachability()
        throws
    {
        let center = NotificationCenter()
        let profile = try makeExternalProfile(
            displayPath: "/Volumes/Archive/Photos"
        )
        var disconnectCount = 0
        var refreshCount = 0
        let monitor = MacExternalVolumeLifecycleMonitor(
            notificationCenter: center,
            activeProfile: { profile },
            disconnect: { disconnectCount += 1 },
            refreshReachability: { refreshCount += 1 }
        )
        monitor.start()
        defer { monitor.stop() }

        center.post(
            name: NSWorkspace.didUnmountNotification,
            object: nil,
            userInfo: [
                NSWorkspace.volumeURLUserInfoKey:
                    URL(fileURLWithPath: "/Volumes/Archive")
            ]
        )

        XCTAssertEqual(disconnectCount, 1)
        XCTAssertEqual(refreshCount, 1)
    }

    func testUnrelatedUnmountOnlyRefreshesReachability()
        throws
    {
        let center = NotificationCenter()
        let profile = try makeExternalProfile(
            displayPath: "/Volumes/Archive/Photos"
        )
        var disconnectCount = 0
        var refreshCount = 0
        let monitor = MacExternalVolumeLifecycleMonitor(
            notificationCenter: center,
            activeProfile: { profile },
            disconnect: { disconnectCount += 1 },
            refreshReachability: { refreshCount += 1 }
        )
        monitor.start()
        defer { monitor.stop() }

        center.post(
            name: NSWorkspace.didUnmountNotification,
            object: nil,
            userInfo: [
                NSWorkspace.volumeURLUserInfoKey:
                    URL(fileURLWithPath: "/Volumes/Archive 2")
            ]
        )

        XCTAssertEqual(disconnectCount, 0)
        XCTAssertEqual(refreshCount, 1)
    }

    func testMountRefreshesReachabilityWithoutDisconnecting()
        throws
    {
        let center = NotificationCenter()
        let profile = try makeExternalProfile(
            displayPath: "/Volumes/Archive/Photos"
        )
        var disconnectCount = 0
        var refreshCount = 0
        let monitor = MacExternalVolumeLifecycleMonitor(
            notificationCenter: center,
            activeProfile: { profile },
            disconnect: { disconnectCount += 1 },
            refreshReachability: { refreshCount += 1 }
        )
        monitor.start()
        defer { monitor.stop() }

        center.post(
            name: NSWorkspace.didMountNotification,
            object: nil,
            userInfo: [
                NSWorkspace.volumeURLUserInfoKey:
                    URL(fileURLWithPath: "/Volumes/Archive")
            ]
        )

        XCTAssertEqual(disconnectCount, 0)
        XCTAssertEqual(refreshCount, 1)
    }

    func testUnmountNeverDisconnectsNetworkDestination() {
        let networkProfile = ServerProfileRecord(
            id: 8,
            name: "NAS",
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

        XCTAssertFalse(
            MacExternalVolumeLifecyclePolicy.shouldDisconnect(
                profile: networkProfile,
                unmountedVolumeURL: URL(
                    fileURLWithPath: "/Volumes/Archive"
                )
            )
        )
    }

    private func makeExternalProfile(
        displayPath: String
    ) throws -> ServerProfileRecord {
        let params = ExternalVolumeConnectionParams(
            rootBookmarkData: Data([1, 2, 3]),
            displayPath: displayPath
        )
        return ServerProfileRecord(
            id: 7,
            name: "Archive",
            storageType: StorageType.externalVolume.rawValue,
            connectionParams: try JSONEncoder().encode(params),
            sortOrder: 0,
            host: "",
            port: 0,
            shareName: "location-token",
            basePath: "/Watermelon",
            username: "",
            domain: nil,
            credentialRef: "",
            createdAt: Date(),
            updatedAt: Date()
        )
    }
}

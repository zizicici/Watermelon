import XCTest
@testable import WatermelonMac

final class MacWebDAVProbePathPolicyTests: XCTestCase {
    func testProbeListsBackupBasePathInsteadOfEndpointMountPath() {
        let profile = ServerProfileRecord(
            id: nil,
            name: "WebDAV",
            storageType: StorageType.webdav.rawValue,
            connectionParams: nil,
            sortOrder: 0,
            host: "example.com",
            port: 443,
            shareName: "/dav",
            basePath: "/Watermelon",
            username: "user",
            domain: nil,
            credentialRef: "",
            createdAt: Date(),
            updatedAt: Date()
        )

        XCTAssertEqual(
            MacWebDAVProbePathPolicy.listPath(for: profile),
            "/Watermelon"
        )
    }
}

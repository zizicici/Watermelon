import XCTest
@testable import Watermelon

final class StorageCapabilityMatrixTests: XCTestCase {
    func testLocalVolumeTriple() {
        let client = LocalVolumeClient(config: LocalVolumeClient.Config(
            rootBookmarkData: Data(),
            onBookmarkRefreshed: nil
        ))
        XCTAssertTrue(client.supportsLivenessSafeOverwriteMove)
        XCTAssertTrue(client.supportsLivenessSafeOverwriteUpload)
        XCTAssertTrue(client.supportsLivenessSafeRenewal)
    }

    func testS3Triple() {
        let client = S3Client(config: S3Client.Config(
            endpointHost: "s3.us-east-1.amazonaws.com",
            endpointPort: 0,
            scheme: "https",
            region: "us-east-1",
            bucket: "examplebucket",
            basePath: "/",
            usePathStyle: false,
            accessKeyID: "AKIAIOSFODNN7EXAMPLE",
            secretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            sessionToken: nil
        ))
        XCTAssertTrue(client.supportsLivenessSafeOverwriteMove)
        XCTAssertTrue(client.supportsLivenessSafeOverwriteUpload)
        XCTAssertTrue(client.supportsLivenessSafeRenewal)
        XCTAssertTrue(client.shouldSetModificationDate())
    }

    func testWebDAVTriple() {
        let client = WebDAVClient(config: WebDAVClient.Config(
            endpointURL: URL(string: "https://example.com/dav")!,
            username: "user",
            password: "pass"
        ))
        XCTAssertTrue(client.supportsLivenessSafeOverwriteMove)
        XCTAssertFalse(client.supportsLivenessSafeOverwriteUpload)
        XCTAssertTrue(client.supportsLivenessSafeRenewal)
    }

    func testSMBTriple() throws {
        let client = try AMSMB2Client(config: SMBServerConfig(
            host: "example.local",
            port: 445,
            shareName: "share",
            basePath: "/",
            username: "user",
            password: "pass",
            domain: nil
        ))
        XCTAssertFalse(client.supportsLivenessSafeOverwriteMove)
        XCTAssertFalse(client.supportsLivenessSafeOverwriteUpload)
        XCTAssertFalse(client.supportsLivenessSafeRenewal)
    }

    func testSFTPTriple() {
        let client = SFTPClient(config: SFTPClient.Config(
            host: "example.local",
            port: 22,
            username: "user",
            credential: .password("pass"),
            expectedHostKeyFingerprintSHA256: "deadbeef"
        ))
        XCTAssertFalse(client.supportsLivenessSafeOverwriteMove)
        XCTAssertFalse(client.supportsLivenessSafeOverwriteUpload)
        XCTAssertFalse(client.supportsLivenessSafeRenewal)
    }
}

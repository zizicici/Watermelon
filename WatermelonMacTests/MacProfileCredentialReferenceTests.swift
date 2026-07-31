import XCTest
@testable import WatermelonMac

final class MacProfileCredentialReferenceTests: XCTestCase {
    func testReferencesMatchPublishedV2IdentityContract() throws {
        let cases: [(CanonicalProfileConnection, String)] = [
            (
                .smb(
                    try CanonicalSMBConnection(
                        host: "NAS.local",
                        port: 445,
                        shareName: "Photos",
                        basePath: "/Watermelon",
                        username: "alice",
                        domain: "WORKGROUP"
                    )
                ),
                "v2|smb|438a275ce72a87924acf5054e3235ddf76509ede76946cf9b83d2c7c80321045"
            ),
            (
                .webDAV(
                    try CanonicalWebDAVConnection(
                        scheme: "https",
                        host: "dav.example.com",
                        port: 443,
                        mountPath:
                            "/remote.php/dav/files/alice",
                        basePath: "/Watermelon",
                        username: "alice"
                    )
                ),
                "v2|webdav|9caafc48cc65c59eeaa068552487318612a9f8dbc346e644fa5b936c1eac5372"
            ),
            (
                .s3(
                    try CanonicalS3Connection(
                        scheme: "https",
                        host: "s3.example.com",
                        port: 443,
                        region: "us-east-1",
                        usePathStyle: true,
                        bucket: "photos",
                        basePath: "/Watermelon",
                        accessKeyID: "ACCESS"
                    )
                ),
                "v2|s3|6a9a0ba58a5ff38b033a0f28e175852805b4b669d74eff4149df655f50de0313"
            ),
            (
                .sftp(
                    try CanonicalSFTPConnection(
                        host: "sftp.example.com",
                        port: 22,
                        basePath: "/Watermelon",
                        username: "alice",
                        authMethod: .password,
                        hostKeyFingerprintSHA256: "SHA256:test"
                    )
                ),
                "v2|sftp|af19d27831a71cc31c8ca540d424f8e929ac2bbb019970d3fa0fb25fcf175aff"
            ),
            (
                .oneDrive(
                    try CanonicalOneDriveConnection(
                        params: OneDriveConnectionParams(
                            driveID: "drive-id",
                            rootItemID: "root-id",
                            displayRootPath:
                                "/Apps/Watermelon Backup"
                        )
                    )
                ),
                "v2|onedrive|c62db30105e71863034619ea2b79f890696b4315e6c5220f72d2d5ca512b96c7"
            )
        ]

        for (connection, expected) in cases {
            XCTAssertEqual(
                MacProfileCredentialReference.make(
                    for: connection.duplicateIdentity
                ),
                expected
            )
        }
    }

    func testCanonicalVariantsShareCredentialReference() throws {
        let first = try CanonicalSMBConnection(
            host: "smb://NAS.local",
            port: 0,
            shareName: "/Photos/",
            basePath: "Watermelon/",
            username: "alice",
            domain: "WORKGROUP"
        )
        let second = try CanonicalSMBConnection(
            host: "nas.local",
            port: 445,
            shareName: "photos",
            basePath: "/Watermelon",
            username: "alice",
            domain: "workgroup"
        )

        XCTAssertEqual(
            MacProfileCredentialReference.make(
                for: CanonicalProfileConnection
                    .smb(first)
                    .duplicateIdentity
            ),
            MacProfileCredentialReference.make(
                for: CanonicalProfileConnection
                    .smb(second)
                    .duplicateIdentity
            )
        )
    }
}

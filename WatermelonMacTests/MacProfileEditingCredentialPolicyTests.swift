import XCTest
@testable import WatermelonMac

final class MacProfileEditingCredentialPolicyTests: XCTestCase {
    func testMissingPlainCredentialUsesEmptyEditorValue() {
        XCTAssertEqual(
            MacProfileEditingCredentialPolicy.plainCredential(
                storedCredential: nil
            ),
            ""
        )
    }

    func testMatchingSFTPCredentialCanBePrefilled() throws {
        let password = SFTPCredentialBlob.password("secret")
        XCTAssertEqual(
            MacProfileEditingCredentialPolicy.sftpCredential(
                storedCredential: try password.encodedJSONString(),
                authMethod: .password
            ),
            password
        )

        let privateKey = SFTPCredentialBlob.privateKey(
            pem: "private-key",
            passphrase: "passphrase"
        )
        XCTAssertEqual(
            MacProfileEditingCredentialPolicy.sftpCredential(
                storedCredential: try privateKey.encodedJSONString(),
                authMethod: .privateKey
            ),
            privateKey
        )
    }

    func testInvalidOrMismatchedSFTPCredentialIsNotPrefilled()
        throws
    {
        XCTAssertNil(
            MacProfileEditingCredentialPolicy.sftpCredential(
                storedCredential: "invalid",
                authMethod: .password
            )
        )
        XCTAssertNil(
            MacProfileEditingCredentialPolicy.sftpCredential(
                storedCredential:
                    try SFTPCredentialBlob.password("secret")
                        .encodedJSONString(),
                authMethod: .privateKey
            )
        )
    }
}

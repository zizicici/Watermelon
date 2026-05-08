import XCTest
@testable import Watermelon

final class SFTPCredentialBlobTests: XCTestCase {
    func testPasswordRoundTrip() throws {
        let blob: SFTPCredentialBlob = .password("hunter2")
        let json = try blob.encodedJSONString()
        let decoded = try SFTPCredentialBlob.decode(from: json)
        XCTAssertEqual(decoded, blob)
    }

    func testPrivateKeyWithPassphraseRoundTrip() throws {
        let pem = "-----BEGIN OPENSSH PRIVATE KEY-----\nfake\n-----END OPENSSH PRIVATE KEY-----"
        let blob: SFTPCredentialBlob = .privateKey(pem: pem, passphrase: "secret")
        let json = try blob.encodedJSONString()
        let decoded = try SFTPCredentialBlob.decode(from: json)
        XCTAssertEqual(decoded, blob)
    }

    func testPrivateKeyWithoutPassphraseRoundTrip() throws {
        let pem = "-----BEGIN OPENSSH PRIVATE KEY-----\nfake\n-----END OPENSSH PRIVATE KEY-----"
        let blob: SFTPCredentialBlob = .privateKey(pem: pem, passphrase: nil)
        let json = try blob.encodedJSONString()
        let decoded = try SFTPCredentialBlob.decode(from: json)
        XCTAssertEqual(decoded, blob)
    }

    func testDecodeRejectsNonJSONString() {
        XCTAssertThrowsError(try SFTPCredentialBlob.decode(from: "not json"))
    }

    func testConnectionParamsRoundTrip() throws {
        let params = SFTPConnectionParams(
            authMethod: .privateKey,
            hostKeyFingerprintSHA256: "SHA256:abc123"
        )
        let data = try ServerProfileRecord.encodedConnectionParams(params)
        let decoded = try JSONDecoder().decode(SFTPConnectionParams.self, from: data)
        XCTAssertEqual(decoded.authMethod, params.authMethod)
        XCTAssertEqual(decoded.hostKeyFingerprintSHA256, params.hostKeyFingerprintSHA256)
    }
}

import XCTest
@testable import Watermelon

// Citadel/NIO error types (SFTPError.connectionClosed, ChannelError.connectTimeout,
// NIOConnectionError) aren't covered here — the test target doesn't link those
// modules. This suite locks down the Foundation-level branches: the explicit
// hand-rolled errors, RemoteStorageClientError dispatch, and POSIX code matching.
final class SFTPErrorClassifierTests: XCTestCase {
    private struct Case {
        let label: String
        let error: Error
        let expected: Bool
    }

    private static func posix(_ code: Int32) -> NSError {
        NSError(domain: NSPOSIXErrorDomain, code: Int(code))
    }

    func testIsConnectionUnavailable() {
        let cases: [Case] = [
            // Hand-rolled errors
            Case(
                label: "host key mismatch is permanent, not transient",
                error: SFTPHostKeyMismatchError(actual: "SHA256:b"),
                expected: false
            ),
            Case(
                label: "unsupported key type is configuration, not connection",
                error: SFTPUnsupportedKeyTypeError(detectedType: "ecdsa"),
                expected: false
            ),

            // RemoteStorageClientError surface
            Case(
                label: "RemoteStorageClientError.notConnected",
                error: RemoteStorageClientError.notConnected,
                expected: true
            ),
            Case(
                label: "RemoteStorageClientError.unavailable",
                error: RemoteStorageClientError.unavailable,
                expected: true
            ),
            Case(
                label: "RemoteStorageClientError.invalidConfiguration",
                error: RemoteStorageClientError.invalidConfiguration,
                expected: false
            ),
            Case(
                label: "RemoteStorageClientError.externalStorageUnavailable",
                error: RemoteStorageClientError.externalStorageUnavailable,
                expected: false
            ),
            Case(
                label: "RemoteStorageClientError.unsupportedStorageType",
                error: RemoteStorageClientError.unsupportedStorageType("sftp"),
                expected: false
            ),
            Case(
                label: "underlying POSIX network code unwraps to true",
                error: RemoteStorageClientError.underlying(Self.posix(ECONNREFUSED)),
                expected: true
            ),
            Case(
                label: "underlying non-network NSError stays false",
                error: RemoteStorageClientError.underlying(NSError(domain: "Test", code: 1)),
                expected: false
            ),

            // POSIX network codes
            Case(label: "ECONNREFUSED", error: Self.posix(ECONNREFUSED), expected: true),
            Case(label: "EHOSTUNREACH", error: Self.posix(EHOSTUNREACH), expected: true),
            Case(label: "ENETUNREACH", error: Self.posix(ENETUNREACH), expected: true),
            Case(label: "ETIMEDOUT", error: Self.posix(ETIMEDOUT), expected: true),
            Case(label: "ECONNRESET", error: Self.posix(ECONNRESET), expected: true),
            Case(label: "EPERM is not a network code", error: Self.posix(EPERM), expected: false),

            // Foreign domains
            Case(
                label: "non-POSIX NSError",
                error: NSError(domain: NSCocoaErrorDomain, code: -1),
                expected: false
            )
        ]

        for c in cases {
            XCTAssertEqual(
                SFTPErrorClassifier.isConnectionUnavailable(c.error),
                c.expected,
                c.label
            )
        }
    }

    func testUnsupportedKeyTypeMessageContainsDetectedType() {
        let error = SFTPUnsupportedKeyTypeError(detectedType: "ecdsa")
        let message = SFTPErrorClassifier.describe(error)
        XCTAssertTrue(
            message.contains("ecdsa"),
            "describe() should surface the detected key type, got: \(message)"
        )
    }

    func testIsNotFoundIgnoresMessageOnlyEvidence() {
        XCTAssertTrue(SFTPErrorClassifier.isNotFound(Self.posix(ENOENT)))
        XCTAssertTrue(SFTPErrorClassifier.isNotFound(RemoteStorageClientError.underlying(Self.posix(ENOENT))))

        let wrapped = NSError(
            domain: "test.outer",
            code: 1,
            userInfo: [NSUnderlyingErrorKey: Self.posix(ENOENT)]
        )
        XCTAssertTrue(SFTPErrorClassifier.isNotFound(wrapped))

        let messageOnly = NSError(
            domain: "test.sftp",
            code: 2,
            userInfo: [NSLocalizedDescriptionKey: "No such file"]
        )
        XCTAssertFalse(SFTPErrorClassifier.isNotFound(messageOnly))
    }

    func testIsNotFoundFalseForMixedConnectionAndAbsence() {
        let wrapped = NSError(
            domain: "test.outer",
            code: 1,
            userInfo: [NSUnderlyingErrorKey: Self.posix(ENOENT)]
        )
        let connectionThenAbsence = NSError(
            domain: NSPOSIXErrorDomain,
            code: Int(ECONNRESET),
            userInfo: [NSUnderlyingErrorKey: Self.posix(ENOENT)]
        )
        let absenceThenConnection = NSError(
            domain: "test.outer",
            code: 2,
            userInfo: [NSUnderlyingErrorKey: Self.posix(ECONNRESET)]
        )
        let absenceWrappingConnection = NSError(
            domain: NSPOSIXErrorDomain,
            code: Int(ENOENT),
            userInfo: [NSUnderlyingErrorKey: Self.posix(ECONNRESET)]
        )

        XCTAssertTrue(SFTPErrorClassifier.isNotFound(wrapped))
        XCTAssertFalse(SFTPErrorClassifier.isNotFound(connectionThenAbsence))
        XCTAssertFalse(SFTPErrorClassifier.isNotFound(absenceThenConnection))
        XCTAssertFalse(SFTPErrorClassifier.isNotFound(absenceWrappingConnection))
        XCTAssertTrue(SFTPErrorClassifier.isConnectionUnavailable(absenceWrappingConnection))
        XCTAssertEqual(RemoteFaultLite.classify(connectionThenAbsence), .retryable)
        XCTAssertEqual(RemoteFaultLite.classify(absenceWrappingConnection), .retryable)
    }
}

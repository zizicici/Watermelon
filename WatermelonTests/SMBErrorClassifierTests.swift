import XCTest
@testable import Watermelon

// Raw `SMBErrorClassifier.isNotFound` is the single absence seam for `AMSMB2Client.metadata`/`exists`/
// `delete` (the client itself can't be built in tests — it needs a live SMB2Manager). These lock the
// fail-closed priority: a chain that also carries a connection-unavailable / backend / session token must
// NOT read as object absence, while clear object/path/file absence still does.
final class SMBErrorClassifierTests: XCTestCase {
    private func smb(_ token: String) -> NSError {
        NSError(domain: "AMSMB2", code: 2, userInfo: [NSLocalizedDescriptionKey: token])
    }

    private func mixed(absence: String, transient: NSError) -> NSError {
        NSError(
            domain: "AMSMB2",
            code: 2,
            userInfo: [NSLocalizedDescriptionKey: absence, NSUnderlyingErrorKey: transient]
        )
    }

    private func posix(_ code: Int32) -> NSError {
        NSError(domain: NSPOSIXErrorDomain, code: Int(code))
    }

    // MARK: - Clear absence still notFound

    func testIsNotFoundTrueForClearObjectPathFileAbsence() {
        for token in [
            "STATUS_NO_SUCH_FILE",
            "STATUS_OBJECT_NAME_NOT_FOUND",
            "STATUS_OBJECT_PATH_NOT_FOUND",
            "STATUS_NOT_FOUND"
        ] {
            XCTAssertTrue(SMBErrorClassifier.isNotFound(smb(token)), "\(token) must classify as notFound")
        }
        XCTAssertTrue(SMBErrorClassifier.isNotFound(posix(ENOENT)), "POSIX ENOENT must classify as notFound")
    }

    // MARK: - Mixed transient + absence chains fail closed (not notFound)

    func testIsNotFoundFalseForMixedAbsencePlusConnectionStatusToken() {
        for transient in ["STATUS_IO_TIMEOUT", "STATUS_BAD_NETWORK_NAME", "STATUS_REDIRECTOR_NOT_STARTED"] {
            let chain = mixed(absence: "STATUS_OBJECT_NAME_NOT_FOUND", transient: smb(transient))
            XCTAssertFalse(
                SMBErrorClassifier.isNotFound(chain),
                "a chain with absence + \(transient) must not collapse to notFound"
            )
        }
    }

    func testIsNotFoundFalseForMixedAbsencePlusConnectionPOSIXCode() {
        let chain = mixed(absence: "STATUS_OBJECT_PATH_NOT_FOUND", transient: posix(ETIMEDOUT))
        XCTAssertFalse(
            SMBErrorClassifier.isNotFound(chain),
            "a chain with a path-absence token + POSIX ETIMEDOUT must not collapse to notFound"
        )
    }

    func testIsNotFoundFalseForPureConnectionTokens() {
        XCTAssertFalse(SMBErrorClassifier.isNotFound(smb("STATUS_IO_TIMEOUT")))
        XCTAssertFalse(SMBErrorClassifier.isNotFound(smb("STATUS_BAD_NETWORK_NAME")))
    }

    // MARK: - Connection predicate unaffected (the precedence it feeds stays meaningful)

    func testIsConnectionUnavailableStillTrueForConnectionTokens() {
        XCTAssertTrue(SMBErrorClassifier.isConnectionUnavailable(smb("STATUS_IO_TIMEOUT")))
        XCTAssertTrue(SMBErrorClassifier.isConnectionUnavailable(smb("STATUS_BAD_NETWORK_NAME")))
        XCTAssertTrue(SMBErrorClassifier.isConnectionUnavailable(posix(ETIMEDOUT)))
    }

    // MARK: - Cross-seam consistency with RemoteFaultLite

    func testRawClassifierAgreesWithRemoteFaultLiteOnMixedChain() {
        let chain = mixed(absence: "STATUS_OBJECT_NAME_NOT_FOUND", transient: smb("STATUS_IO_TIMEOUT"))
        XCTAssertFalse(SMBErrorClassifier.isNotFound(chain))
        XCTAssertEqual(RemoteFaultLite.classify(chain), .retryable)
    }

    func testRawClassifierAgreesWithRemoteFaultLiteOnClearAbsence() {
        let absence = smb("STATUS_OBJECT_NAME_NOT_FOUND")
        XCTAssertTrue(SMBErrorClassifier.isNotFound(absence))
        XCTAssertEqual(RemoteFaultLite.classify(absence), .notFound)
    }

    // MARK: - SMB Date Sanitizing

    func testSafeSMBFileDateFloorsPre1970FractionalSeconds() {
        let date = Date(timeIntervalSince1970: -0.5)

        XCTAssertEqual(AMSMB2Client.safeSMBFileDate(date)?.timeIntervalSince1970, -1)
    }

    func testSafeSMBFileDateKeepsPre1970IntegerSeconds() {
        let date = Date(timeIntervalSince1970: -1)

        XCTAssertEqual(AMSMB2Client.safeSMBFileDate(date)?.timeIntervalSince1970, -1)
    }

    func testSafeSMBFileDateKeepsPost1970FractionalSeconds() {
        let date = Date(timeIntervalSince1970: 1.25)

        XCTAssertEqual(AMSMB2Client.safeSMBFileDate(date)?.timeIntervalSince1970, 1.25)
    }

    func testSafeSMBFileDateRejectsNonFiniteAndOutOfRangeValues() {
        XCTAssertNil(AMSMB2Client.safeSMBFileDate(Date(timeIntervalSince1970: .nan)))
        XCTAssertNil(AMSMB2Client.safeSMBFileDate(Date(timeIntervalSince1970: .infinity)))
        XCTAssertNil(AMSMB2Client.safeSMBFileDate(Date(timeIntervalSince1970: -11_644_473_601)))
        XCTAssertNil(AMSMB2Client.safeSMBFileDate(Date(timeIntervalSince1970: 253_402_300_800)))
    }
}

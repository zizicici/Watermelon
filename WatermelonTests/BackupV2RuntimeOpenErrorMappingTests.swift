import XCTest
@testable import Watermelon

// Bug-IX P04 R08 ClaudeReviewerA F1: transient transport errors during identity/version
// reads must propagate for retry, not classify as permanent repo damage.
final class BackupV2RuntimeOpenErrorMappingTests: XCTestCase {

    func testTranslateBootstrapIOFailure_s3Transient503_propagates() {
        let transient = NSError(
            domain: S3ErrorClassifier.errorDomain,
            code: 503,
            userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "SlowDown"]
        )
        let result = BackupV2RuntimeOpenErrorMapping.translate(
            bootstrapError: .ioFailure(transient)
        )
        XCTAssertNil(result as? BackupV2RuntimeBuildError,
                      "transient S3 503 must not map to damagedV2Repo")
    }

    func testTranslateBootstrapIOFailure_webdav503_propagates() {
        let transient = NSError(domain: WebDAVClient.errorDomain, code: 503)
        let result = BackupV2RuntimeOpenErrorMapping.translate(
            bootstrapError: .ioFailure(transient)
        )
        XCTAssertNil(result as? BackupV2RuntimeBuildError,
                      "transient WebDAV 503 must not map to damagedV2Repo")
    }

    func testTranslateBootstrapIOFailure_cancellation_propagates() {
        let result = BackupV2RuntimeOpenErrorMapping.translate(
            bootstrapError: .ioFailure(CancellationError())
        )
        XCTAssertTrue(result is CancellationError)
    }

    func testTranslateBootstrapIOFailure_permanent_mapsToDamaged() {
        let permanent = NSError(
            domain: "RepoBootstrap",
            code: 12,
            userInfo: [NSLocalizedDescriptionKey: "malformed identity marker"]
        )
        let result = BackupV2RuntimeOpenErrorMapping.translate(
            bootstrapError: .ioFailure(permanent)
        )
        guard let buildError = result as? BackupV2RuntimeBuildError else {
            XCTFail("permanent malformed error should map to BackupV2RuntimeBuildError")
            return
        }
        if case .damagedV2Repo = buildError { /* expected */ }
        else { XCTFail("expected .damagedV2Repo") }
    }

    func testTranslateVersionConflictUnreadable_s3Transient503_propagates() {
        let transient = NSError(
            domain: S3ErrorClassifier.errorDomain,
            code: 503,
            userInfo: [S3ErrorClassifier.userInfoServerCodeKey: "ServiceUnavailable"]
        )
        let result = BackupV2RuntimeOpenErrorMapping.translate(
            versionConflict: .unreadable(transient)
        )
        XCTAssertNil(result as? BackupV2RuntimeBuildError,
                      "transient S3 503 in VersionConflict.unreadable must not map to damagedV2Repo")
    }

    func testTranslateVersionConflictUnreadable_cancellation_propagates() {
        let result = BackupV2RuntimeOpenErrorMapping.translate(
            versionConflict: .unreadable(CancellationError())
        )
        XCTAssertTrue(result is CancellationError)
    }

    func testTranslateVersionConflictUnreadable_nil_mapsToDamaged() {
        let result = BackupV2RuntimeOpenErrorMapping.translate(
            versionConflict: .unreadable(nil)
        )
        guard let buildError = result as? BackupV2RuntimeBuildError else {
            XCTFail("nil underlying should map to BackupV2RuntimeBuildError")
            return
        }
        if case .damagedV2Repo = buildError { /* expected */ }
        else { XCTFail("expected .damagedV2Repo") }
    }

    // Bug-IX P04 R09: SMB share/redirector outages must propagate as transient,
    // not classify as permanent repo damage during V2 open.
    func testTranslateBootstrapIOFailure_smbShareUnavailable_propagates() {
        for token in ["STATUS_BAD_NETWORK_NAME", "STATUS_REDIRECTOR_NOT_STARTED"] {
            let error = NSError(
                domain: "SMB",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: token]
            )
            let result = BackupV2RuntimeOpenErrorMapping.translate(
                bootstrapError: .ioFailure(error)
            )
            XCTAssertNil(result as? BackupV2RuntimeBuildError,
                          "SMB \(token) must not map to damagedV2Repo")
        }
    }

    func testTranslateVersionConflictUnreadable_smbShareUnavailable_propagates() {
        for token in ["STATUS_BAD_NETWORK_NAME", "STATUS_REDIRECTOR_NOT_STARTED"] {
            let error = NSError(
                domain: "SMB",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: token]
            )
            let result = BackupV2RuntimeOpenErrorMapping.translate(
                versionConflict: .unreadable(error)
            )
            XCTAssertNil(result as? BackupV2RuntimeBuildError,
                          "SMB \(token) in VersionConflict.unreadable must not map to damagedV2Repo")
        }
    }
}

import UIKit
import XCTest
@testable import Watermelon

@MainActor
final class StorageErrorPresentationTests: XCTestCase {
    private let backends: [StorageType] = [.smb, .webdav, .s3, .sftp, .onedrive, .dropbox, .googleDrive, .externalVolume]

    private func profile(_ type: StorageType) -> ServerProfileRecord {
        ServerProfileRecord(id: 1, name: "private-name", storageType: type.rawValue, connectionParams: nil,
            sortOrder: 0, host: "private-host", port: 0, shareName: "share", basePath: "/photos",
            username: "user", domain: nil, credentialRef: "private-credential", backgroundBackupEnabled: false,
            createdAt: Date(), updatedAt: Date())
    }

    func testSavedConnectionAndEditorUseSameMessagesAcrossAllBackends() {
        let cases: [(Error, String)] = [
            (RemoteStorageClientError.connectionTimedOut, RemoteStorageClientError.connectionTimedOut.localizedDescription),
            (RemoteStorageClientError.connectionBusy, RemoteStorageClientError.connectionBusy.localizedDescription),
            (RemoteStorageClientError.buildUnavailable, RemoteStorageClientError.buildUnavailable.localizedDescription),
            (RemoteStorageClientError.verificationFailed, RemoteStorageClientError.verificationFailed.localizedDescription),
            (URLError(.timedOut), String(localized: "storage.error.requestTimedOut")),
            (URLError(.networkConnectionLost), String(localized: "storage.error.connectionLost")),
            (URLError(.serverCertificateUntrusted), String(localized: "storage.error.certificateInvalid"))
        ]
        for type in backends {
            for (error, expected) in cases {
                for wrapped in [error, RemoteStorageClientError.underlying(error)] {
                    let home = ConnectionFailurePresentation(profile: profile(type), error: wrapped)
                    let editor = ConnectionFailurePresentation(storageType: type, error: wrapped)
                    XCTAssertEqual(home.summary, expected, "\(type): \(error)")
                    XCTAssertEqual(editor.summary, home.summary, "\(type): \(error)")
                    XCTAssertTrue(editor.details.contains("backend: \(type.rawValue)"))
                }
            }
        }
    }

    func testSMBFailureReasonsAreNotFlattenedIntoDisconnected() {
        let cases: [(String, String)] = [
            ("STATUS_LOGON_FAILURE", String(localized: "storage.error.authenticationFailed")),
            ("STATUS_ACCESS_DENIED", String(localized: "storage.error.permissionDenied")),
            ("STATUS_BAD_NETWORK_NAME", String(localized: "storage.error.smbShareUnavailable")),
            ("STATUS_IO_TIMEOUT", String(localized: "storage.error.requestTimedOut")),
            ("STATUS_DISK_FULL", String(localized: "storage.error.insufficientSpace")),
            ("STATUS_MEDIA_WRITE_PROTECTED", String(localized: "storage.error.readOnly"))
        ]
        for (token, expected) in cases {
            let error = NSError(domain: "AMSMB2", code: 5, userInfo: [NSLocalizedDescriptionKey: token])
            let report = ConnectionFailurePresentation(storageType: .smb, error: RemoteStorageClientError.underlying(error))
            XCTAssertEqual(report.summary, expected)
            XCTAssertTrue(report.details.contains(token))
        }
    }

    func testExternalStorageDistinguishesPermissionsSpaceAndReadOnly() {
        for (code, key) in [(EACCES, "storage.error.filePermissionDenied"), (ENOSPC, "storage.error.fileInsufficientSpace"),
                            (EROFS, "storage.error.fileReadOnly"), (ENOENT, "storage.error.pathMissing")] {
            let error = NSError(domain: NSPOSIXErrorDomain, code: Int(code))
            let report = ConnectionFailurePresentation(storageType: .externalVolume, error: error)
            XCTAssertEqual(report.summary, String(localized: String.LocalizationValue(key)))
            XCTAssertFalse(report.summary.contains(String(localized: "storage.error.externalUnavailable")))
        }
    }

    func testFileErrorsDoNotBlameTheConfiguredDestination() {
        let localURL = FileManager.default.temporaryDirectory.appendingPathComponent(".sftp-download-test.tmp")
        let cases = [
            (NSFileWriteOutOfSpaceError, ENOSPC, "storage.error.fileInsufficientSpace"),
            (NSFileWriteVolumeReadOnlyError, EROFS, "storage.error.fileReadOnly"),
            (NSFileWriteNoPermissionError, EACCES, "storage.error.filePermissionDenied")
        ]
        for (cocoaCode, posixCode, key) in cases {
            let posix = NSError(domain: NSPOSIXErrorDomain, code: Int(posixCode))
            let cocoa = NSError(domain: NSCocoaErrorDomain, code: cocoaCode,
                userInfo: [NSFilePathErrorKey: localURL.path, NSUnderlyingErrorKey: posix])
            for type in backends {
                for error in [posix, cocoa] {
                    let report = ConnectionFailurePresentation(storageType: type,
                        error: RemoteStorageClientError.underlying(error))
                    XCTAssertEqual(report.summary, String(localized: String.LocalizationValue(key)), "\(type)")
                    XCTAssertTrue(report.details.contains("NSPOSIXErrorDomain/\(posixCode)"))
                    XCTAssertTrue(report.details.contains("original_message: \(error.localizedDescription)"))
                }
            }
        }
        let quota = NSError(domain: NSPOSIXErrorDomain, code: Int(EDQUOT))
        XCTAssertEqual(ConnectionFailurePresentation(storageType: .sftp, error: quota).summary,
                       String(localized: "storage.error.fileInsufficientSpace"))
    }

    func testServerReportedSpaceAndReadOnlyErrorsStillIdentifyTheDestination() {
        for (token, key) in [("STATUS_DISK_FULL", "storage.error.insufficientSpace"),
                             ("STATUS_MEDIA_WRITE_PROTECTED", "storage.error.readOnly")] {
            let serverError = NSError(domain: "AMSMB2", code: 5, userInfo: [NSLocalizedDescriptionKey: token])
            let report = ConnectionFailurePresentation(storageType: .smb, error: serverError)
            XCTAssertEqual(report.summary, String(localized: String.LocalizationValue(key)))
        }
        let webdav = NSError(domain: WebDAVClient.errorDomain, code: 507)
        XCTAssertEqual(ConnectionFailurePresentation(storageType: .webdav, error: webdav).summary,
                       String(localized: "storage.error.insufficientSpace"))
        XCTAssertNotEqual(String(localized: "storage.error.fileInsufficientSpace"), String(localized: "storage.error.insufficientSpace"))
        XCTAssertNotEqual(String(localized: "storage.error.fileReadOnly"), String(localized: "storage.error.readOnly"))
        XCTAssertNotEqual(String(localized: "storage.error.filePermissionDenied"), String(localized: "storage.error.permissionDenied"))
    }

    func testHTTPBackendsDistinguishTimeoutThrottlingAndServerFailure() {
        let cases: [(Int, String)] = [(408, "storage.error.requestTimedOut"), (429, "storage.error.throttled"),
                                      (503, "storage.error.serviceUnavailable")]
        for (status, key) in cases {
            let errors: [(StorageType, Error)] = [
                (.webdav, NSError(domain: WebDAVClient.errorDomain, code: status)),
                (.s3, NSError(domain: S3ErrorClassifier.errorDomain, code: status)),
                (.onedrive, OneDriveErrorClassifier.makeServiceError(statusCode: status, code: nil, message: "provider-message")),
                (.dropbox, DropboxErrorClassifier.makeServiceError(statusCode: status, summary: "provider-message")),
                (.googleDrive, NSError(domain: GoogleDriveErrorClassifier.errorDomain, code: status))
            ]
            for (type, error) in errors {
                let report = ConnectionFailurePresentation(storageType: type, error: error)
                let expected: String
                if status == 429, type == .dropbox { expected = String(localized: "dropbox.error.throttled") }
                else if status == 429, type == .googleDrive { expected = String(localized: "googledrive.error.throttled") }
                else { expected = String(localized: String.LocalizationValue(key)) }
                XCTAssertEqual(report.summary, expected, "\(type) \(status)")
                XCTAssertTrue(report.details.contains("/\(status)"))
            }
        }
    }

    func testS3ServiceCodePreservesOriginalMessageInDetails() {
        for (code, expected) in [("RequestTimeout", "storage.error.requestTimedOut"),
                                 ("ServiceUnavailable", "storage.error.serviceUnavailable"),
                                 ("InvalidPart", "storage.error.invalidRequest")] {
            let error = NSError(domain: S3ErrorClassifier.errorDomain, code: 400, userInfo: [
                S3ErrorClassifier.userInfoServerCodeKey: code,
                S3ErrorClassifier.userInfoServerMessageKey: "original server explanation"
            ])
            let report = ConnectionFailurePresentation(storageType: .s3, error: error)
            XCTAssertEqual(report.summary, String(localized: String.LocalizationValue(expected)))
            XCTAssertTrue(report.details.contains(code))
            XCTAssertTrue(report.details.contains("original server explanation"))
        }
    }

    func testS3UnknownServiceCodePreservesSpecificServerMessage() {
        let cases = [
            (507, "XMinioStorageFull", "Storage backend has reached its minimum free drive threshold. Please delete a few objects to proceed."),
            (501, "NotImplemented", "A header you provided implies functionality that is not implemented"),
            (503, "VendorMaintenance", "This bucket is undergoing maintenance until 14:00 UTC.")
        ]
        for (status, code, message) in cases {
            let error = NSError(domain: S3ErrorClassifier.errorDomain, code: status, userInfo: [
                S3ErrorClassifier.userInfoStatusCodeKey: status,
                S3ErrorClassifier.userInfoServerCodeKey: code,
                S3ErrorClassifier.userInfoServerMessageKey: message
            ])
            let report = ConnectionFailurePresentation(storageType: .s3, error: error)
            XCTAssertEqual(report.summary, message)
            XCTAssertTrue(report.details.contains(code))
            XCTAssertTrue(report.details.contains(message))
        }
    }

    func testS3MissingServerMessageUsesSpecificHTTPReason() {
        for (status, key) in [(501, "storage.error.operationUnsupported"), (507, "storage.error.insufficientSpace"),
                              (503, "storage.error.serviceUnavailable")] {
            for message in [nil, "", " \n\t"] as [String?] {
                var info: [String: Any] = [S3ErrorClassifier.userInfoStatusCodeKey: status]
                if let message { info[S3ErrorClassifier.userInfoServerMessageKey] = message }
                let error = NSError(domain: S3ErrorClassifier.errorDomain, code: status, userInfo: info)
                let report = ConnectionFailurePresentation(storageType: .s3, error: error)
                XCTAssertEqual(report.summary, String(localized: String.LocalizationValue(key)))
            }
        }
    }

    func testCloudDetailsPreserveServiceCodesReasonsAndRequestIDs() {
        let errors: [(StorageType, Error, [String])] = [
            (.onedrive, OneDriveErrorClassifier.makeServiceError(statusCode: 403, code: "accessDenied", message: "original graph reason"),
             ["accessDenied", "original graph reason"]),
            (.dropbox, DropboxErrorClassifier.makeServiceError(statusCode: 409, summary: "path/not_found", endpoint: "/files/get_metadata",
                tagPath: "path/not_found", requestID: "request-123"), ["path/not_found", "request-123", "/files/get_metadata"]),
            (.googleDrive, NSError(domain: GoogleDriveErrorClassifier.errorDomain, code: 403, userInfo: [
                GoogleDriveErrorClassifier.userInfoReasonsKey: ["backendError"],
                GoogleDriveErrorClassifier.userInfoMessageKey: "original drive reason"
            ]), ["backendError", "original drive reason"])
        ]
        for (type, error, details) in errors {
            let report = ConnectionFailurePresentation(storageType: type, error: error)
            for detail in details { XCTAssertTrue(report.details.contains(detail), "\(type): \(detail)") }
            if type == .googleDrive { XCTAssertEqual(report.summary, String(localized: "storage.error.serviceUnavailable")) }
        }
    }

    func testEveryEditorFailureCanUseExistingDetailsWithoutAnEditAction() {
        for type in backends {
            let report = ConnectionFailurePresentation(storageType: type, error: URLError(.timedOut), context: "Connection test")
            let alert = ConnectionFailureAlertFactory.make(report: report, title: "Connection test", onDetails: { _ in })
            XCTAssertEqual(alert.message, report.summary)
            XCTAssertEqual(alert.actions.map(\.title), [String(localized: "common.ok"), String(localized: "connection.failure.details")])
            XCTAssertTrue(report.details.contains("context: Connection test"))
        }
    }

    func testConnectionTestRunnerRetainsFailureStageAndOriginalError() async throws {
        let runner = ScreenBoundAsyncRunner<Void>(isScreenActive: { true }, onStateChanged: {})
        var failure: Error?
        runner.start(diagnosticStage: "connection.test", operation: { throw URLError(.timedOut) }, completion: {
            if case .failure(let error) = $0 { failure = error }
        })
        while runner.isRunning { await Task.yield() }
        let report = ConnectionFailurePresentation(storageType: .webdav, error: try XCTUnwrap(failure))
        XCTAssertTrue(report.details.contains("connection.test"))
        XCTAssertTrue(report.details.contains("NSURLErrorDomain/-1001"))
        XCTAssertTrue(report.details.contains("elapsed_seconds:"))
    }

    func testNestedTransportFailureKeepsActionableReason() {
        let error = NSError(domain: "TransportWrapper", code: 7,
            userInfo: [NSUnderlyingErrorKey: URLError(.networkConnectionLost)])
        for type in backends {
            let report = ConnectionFailurePresentation(storageType: type, error: error)
            XCTAssertEqual(report.summary, String(localized: "storage.error.connectionLost"), "\(type)")
            XCTAssertTrue(report.details.contains("TransportWrapper/7 -> NSURLErrorDomain/-1005"))
        }
    }

    func testCancellationDoesNotPresentFailureAlert() {
        let controller = UIViewController()
        let wrapped = StorageConnectionFailure(underlying: URLError(.cancelled), diagnostics: [])
        controller.presentStorageError(title: "Connection test", error: wrapped, storageType: .webdav)
        XCTAssertNil(controller.presentedViewController)
    }
}

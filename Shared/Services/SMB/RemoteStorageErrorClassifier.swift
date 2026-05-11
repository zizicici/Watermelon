import Foundation
import Citadel

/// Cross-backend "path doesn't exist" detection. Each storage backend wraps
/// not-found differently — Cocoa NSError for in-memory/local, HTTP 404 via
/// `WebDAVClient.errorDomain` for WebDAV, Windows NTSTATUS strings via
/// SMBErrorClassifier for SMB, SSH `noSuchFile` via Citadel's `SFTPError` for
/// SFTP. Inspect / bootstrap / liveness paths must distinguish "absent" from
/// "transport failure" without per-backend conditionals at every call site.
func isStorageNotFoundError(_ error: Error) -> Bool {
    let nsError = error as NSError
    if nsError.domain == NSCocoaErrorDomain
        && (nsError.code == NSFileReadNoSuchFileError || nsError.code == NSFileNoSuchFileError) {
        return true
    }
    if nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorFileDoesNotExist {
        return true
    }
    if nsError.domain == "WebDAVClient" && nsError.code == 404 {
        return true
    }
    if SMBErrorClassifier.isNotFound(error) {
        return true
    }
    if let sftpError = error as? SFTPError,
       case .errorStatus(let status) = sftpError,
       status.errorCode == .noSuchFile {
        return true
    }
    if case RemoteStorageClientError.underlying(let underlying) = error {
        return isStorageNotFoundError(underlying)
    }
    return false
}

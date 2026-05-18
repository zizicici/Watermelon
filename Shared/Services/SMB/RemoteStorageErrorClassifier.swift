import Foundation
import Citadel

/// Keeps inspect/bootstrap/liveness paths from treating transport failures as absence.
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
    if nsError.domain == S3ErrorClassifier.errorDomain {
        if nsError.code == 404 { return true }
        if let serverCode = nsError.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String,
           serverCode == "NoSuchKey" || serverCode == "NotFound" {
            return true
        }
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

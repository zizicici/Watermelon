import Foundation
import Citadel

// Shared notFound recognition must not collapse cancellation into absence.
nonisolated enum RemoteStorageErrorClassifier {
    static func isNotFound(_ error: Error) -> Bool {
        if isCancellation(error) { return false }
        if case RemoteStorageClientError.underlying(let underlying) = error {
            return isNotFound(underlying)
        }
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
            return S3ErrorClassifier.isNotFound(error)
        }
        if SMBErrorClassifier.isNotFound(error) {
            return true
        }
        if let sftpError = error as? SFTPError,
           case .errorStatus(let status) = sftpError,
           status.errorCode == .noSuchFile {
            return true
        }
        if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
            return isNotFound(underlying)
        }
        return false
    }

    private static func isCancellation(_ error: Error) -> Bool {
        if error is CancellationError { return true }
        if case RemoteStorageClientError.underlying(let underlying) = error {
            return isCancellation(underlying)
        }
        let nsError = error as NSError
        if nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled {
            return true
        }
        if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
            return isCancellation(underlying)
        }
        return false
    }
}

nonisolated func isStorageNotFoundError(_ error: Error) -> Bool {
    RemoteStorageErrorClassifier.isNotFound(error)
}

import Foundation

enum WebDAVErrorClassifier {
    static func describe(_ error: Error) -> String {
        if let remote = classifyRemote(error) {
            return remote
        }
        return (error as NSError).localizedDescription
    }

    /// Returns a formatted remote-error message only if the chain contains an error we
    /// recognize (WebDAVClient or NSURLErrorDomain). Returns nil otherwise so the caller
    /// can fall back to the outermost error's own user-facing description — e.g. a
    /// manifest wrapper's "this month is corrupt" message, which is more useful than
    /// the raw database/GRDB exception buried underneath.
    private static func classifyRemote(_ error: Error) -> String? {
        if let storage = error as? RemoteStorageClientError,
           case .underlying(let inner) = storage {
            return classifyRemote(inner)
        }

        let nsError = error as NSError
        if nsError.domain == WebDAVClient.errorDomain {
            return format(
                reason: nsError.localizedDescription,
                tail: "\(WebDAVClient.errorDomain) / \(nsError.code)"
            )
        }
        if nsError.domain == NSURLErrorDomain {
            return describeURLSessionError(nsError)
        }
        if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
            return classifyRemote(underlying)
        }
        return nil
    }

    private static func describeURLSessionError(_ nsError: NSError) -> String {
        let urlString = (nsError.userInfo[NSURLErrorFailingURLErrorKey] as? URL)?.absoluteString
        let codeTail = "NSURLErrorDomain / \(nsError.code)"
        let tail = urlString.map { "\($0) · \(codeTail)" } ?? codeTail

        let reason: String
        switch nsError.code {
        case NSURLErrorTimedOut:
            reason = String(localized: "webdav.error.reason.timeout")
        case NSURLErrorCannotFindHost, NSURLErrorDNSLookupFailed:
            reason = String(localized: "webdav.error.reason.hostNotFound")
        case NSURLErrorCannotConnectToHost:
            reason = String(localized: "webdav.error.reason.cannotConnect")
        case NSURLErrorNotConnectedToInternet, NSURLErrorNetworkConnectionLost:
            reason = String(localized: "webdav.error.reason.noNetwork")
        case NSURLErrorServerCertificateUntrusted,
             NSURLErrorServerCertificateHasBadDate,
             NSURLErrorServerCertificateHasUnknownRoot,
             NSURLErrorServerCertificateNotYetValid,
             NSURLErrorClientCertificateRejected,
             NSURLErrorClientCertificateRequired:
            reason = String(localized: "webdav.error.reason.certUntrusted")
        case NSURLErrorSecureConnectionFailed:
            reason = String(localized: "webdav.error.reason.tlsFailed")
        default:
            reason = nsError.localizedDescription
        }

        return format(reason: reason, tail: tail)
    }

    private static func format(reason: String, tail: String) -> String {
        "\(reason)\n(\(tail))"
    }
}

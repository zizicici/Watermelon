import Foundation

enum LegacyTransientErrorClassifier {
    /// Returns true if the error is likely transient (network blip, server hiccup) and worth retrying.
    /// False for definitive errors like auth failure, disk full, file too large.
    static func isTransient(_ error: Error) -> Bool {
        if SMBErrorClassifier.isConnectionUnavailable(error) {
            return true
        }
        let nsError = error as NSError
        if nsError.domain == NSURLErrorDomain {
            switch nsError.code {
            case NSURLErrorTimedOut,
                 NSURLErrorCannotConnectToHost,
                 NSURLErrorNetworkConnectionLost,
                 NSURLErrorNotConnectedToInternet,
                 NSURLErrorDNSLookupFailed,
                 NSURLErrorResourceUnavailable,
                 NSURLErrorInternationalRoamingOff,
                 NSURLErrorCallIsActive,
                 NSURLErrorDataNotAllowed,
                 NSURLErrorRequestBodyStreamExhausted:
                return true
            default:
                return false
            }
        }
        if nsError.domain == NSPOSIXErrorDomain {
            switch nsError.code {
            case Int(ETIMEDOUT), Int(ECONNRESET), Int(ECONNREFUSED),
                 Int(EHOSTDOWN), Int(EHOSTUNREACH), Int(ENETDOWN),
                 Int(ENETUNREACH), Int(EPIPE):
                return true
            default:
                return false
            }
        }
        return false
    }
}

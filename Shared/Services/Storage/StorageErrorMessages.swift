import Foundation

nonisolated enum StorageErrorMessages {
    static func unwrapped(_ error: Error) -> Error {
        var current = error
        for _ in 0..<16 {
            if let storage = current as? RemoteStorageClientError, case .underlying(let inner) = storage {
                current = inner
            } else if let probe = current as? RemoteProbeDeferredCleanupError {
                current = probe.underlyingError
            } else {
                break
            }
        }
        return current
    }

    static func transport(_ error: Error) -> String? {
        let ns = unwrapped(error) as NSError
        if ns.domain == NSURLErrorDomain {
            switch ns.code {
            case NSURLErrorTimedOut: return String(localized: "storage.error.requestTimedOut")
            case NSURLErrorCannotFindHost, NSURLErrorDNSLookupFailed:
                return String(localized: "storage.error.hostNotFound")
            case NSURLErrorCannotConnectToHost: return String(localized: "storage.error.cannotConnect")
            case NSURLErrorNotConnectedToInternet: return String(localized: "storage.error.networkUnavailable")
            case NSURLErrorNetworkConnectionLost: return String(localized: "storage.error.connectionLost")
            case NSURLErrorServerCertificateUntrusted, NSURLErrorServerCertificateHasBadDate,
                 NSURLErrorServerCertificateHasUnknownRoot, NSURLErrorServerCertificateNotYetValid,
                 NSURLErrorClientCertificateRejected, NSURLErrorClientCertificateRequired:
                return String(localized: "storage.error.certificateInvalid")
            case NSURLErrorSecureConnectionFailed: return String(localized: "storage.error.tlsFailed")
            default: break
            }
        }
        if ns.domain == NSPOSIXErrorDomain {
            switch Int32(ns.code) {
            case ETIMEDOUT: return String(localized: "storage.error.requestTimedOut")
            case ECONNREFUSED: return String(localized: "storage.error.cannotConnect")
            case ENETUNREACH, EHOSTUNREACH, ENETDOWN: return String(localized: "storage.error.networkUnavailable")
            case ECONNRESET, ECONNABORTED, ENOTCONN, EPIPE: return String(localized: "storage.error.connectionLost")
            default: break
            }
        }
        return nil
    }

    static func fileAccess(_ error: Error) -> String? {
        let ns = unwrapped(error) as NSError
        if ns.domain == NSPOSIXErrorDomain {
            switch Int32(ns.code) {
            case EACCES, EPERM: return String(localized: "storage.error.filePermissionDenied")
            case ENOSPC, EDQUOT: return String(localized: "storage.error.fileInsufficientSpace")
            case EROFS: return String(localized: "storage.error.fileReadOnly")
            case ENOENT, ENOTDIR: return String(localized: "storage.error.pathMissing")
            default: break
            }
        }
        if ns.domain == NSCocoaErrorDomain {
            switch ns.code {
            case NSFileReadNoPermissionError, NSFileWriteNoPermissionError:
                return String(localized: "storage.error.filePermissionDenied")
            case NSFileWriteOutOfSpaceError: return String(localized: "storage.error.fileInsufficientSpace")
            case NSFileWriteVolumeReadOnlyError: return String(localized: "storage.error.fileReadOnly")
            case NSFileNoSuchFileError, NSFileReadNoSuchFileError:
                return String(localized: "storage.error.pathMissing")
            default: break
            }
        }
        return nil
    }

    static func fallback(_ error: Error) -> String {
        let error = unwrapped(error)
        var current = error
        for _ in 0..<16 {
            if let message = transport(current) ?? fileAccess(current) { return message }
            if current is LocalizedError { break }
            guard let inner = (current as NSError).userInfo[NSUnderlyingErrorKey] as? Error else { break }
            current = unwrapped(inner)
        }
        return error.localizedDescription
    }

    static func httpStatus(_ status: Int) -> String? {
        switch status {
        case 408, 504: return String(localized: "storage.error.requestTimedOut")
        case 429: return String(localized: "storage.error.throttled")
        case 501: return String(localized: "storage.error.operationUnsupported")
        case 507: return String(localized: "storage.error.insufficientSpace")
        case 500...599: return String(localized: "storage.error.serviceUnavailable")
        default: return nil
        }
    }
}

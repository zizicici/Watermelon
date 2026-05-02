import Foundation

enum S3ErrorClassifier {
    static let errorDomain = "S3Client"

    static func describe(_ error: Error) -> String {
        if let message = classify(error) {
            return message
        }
        return (error as NSError).localizedDescription
    }

    static func isConnectionUnavailable(_ error: Error) -> Bool {
        for nsError in nsErrorChain(error) {
            if nsError.domain == NSURLErrorDomain,
               connectionUnavailableURLCodes.contains(nsError.code) {
                return true
            }
            if nsError.domain == errorDomain,
               let code = S3ErrorCode(rawValue: serverCode(in: nsError) ?? ""),
               code.isConnectionUnavailable {
                return true
            }
        }
        return false
    }

    static func shouldLimitUploadRetries(_ error: Error) -> Bool {
        for nsError in nsErrorChain(error) {
            if nsError.domain == errorDomain,
               let code = S3ErrorCode(rawValue: serverCode(in: nsError) ?? ""),
               code.isClientFault {
                return true
            }
        }
        return false
    }

    enum S3ErrorCode: String {
        case invalidAccessKeyID = "InvalidAccessKeyId"
        case signatureDoesNotMatch = "SignatureDoesNotMatch"
        case noSuchBucket = "NoSuchBucket"
        case noSuchKey = "NoSuchKey"
        case accessDenied = "AccessDenied"
        case requestTimeTooSkewed = "RequestTimeTooSkewed"
        case entityTooLarge = "EntityTooLarge"
        case slowDown = "SlowDown"
        case serviceUnavailable = "ServiceUnavailable"
        case internalError = "InternalError"
        case authorizationHeaderMalformed = "AuthorizationHeaderMalformed"
        case bucketRegionError = "AuthorizationHeaderMalformed_BucketRegion"

        var isClientFault: Bool {
            switch self {
            case .invalidAccessKeyID, .signatureDoesNotMatch, .accessDenied,
                 .noSuchBucket, .noSuchKey, .authorizationHeaderMalformed,
                 .entityTooLarge, .bucketRegionError:
                return true
            case .requestTimeTooSkewed, .slowDown, .serviceUnavailable, .internalError:
                return false
            }
        }

        var isConnectionUnavailable: Bool {
            self == .serviceUnavailable || self == .slowDown
        }
    }

    static let userInfoServerCodeKey = "S3ServerCode"
    static let userInfoServerMessageKey = "S3ServerMessage"
    static let userInfoStatusCodeKey = "S3StatusCode"

    private static let connectionUnavailableURLCodes: Set<Int> = [
        NSURLErrorTimedOut,
        NSURLErrorCannotFindHost,
        NSURLErrorDNSLookupFailed,
        NSURLErrorCannotConnectToHost,
        NSURLErrorNotConnectedToInternet,
        NSURLErrorNetworkConnectionLost
    ]

    private static func classify(_ error: Error) -> String? {
        if let storage = error as? RemoteStorageClientError,
           case .underlying(let inner) = storage {
            return classify(inner)
        }

        for nsError in nsErrorChain(error) {
            if nsError.domain == errorDomain {
                return formatServer(nsError)
            }
            if nsError.domain == NSURLErrorDomain {
                return formatURLSession(nsError)
            }
        }
        return nil
    }

    private static func formatServer(_ nsError: NSError) -> String {
        let status = nsError.userInfo[userInfoStatusCodeKey] as? Int
        let serverCode = serverCode(in: nsError)
        let serverMessage = nsError.userInfo[userInfoServerMessageKey] as? String

        let reason: String
        if let serverCode, let code = S3ErrorCode(rawValue: serverCode) {
            reason = localizedReason(for: code)
        } else if let serverMessage, !serverMessage.isEmpty {
            reason = serverMessage
        } else {
            reason = nsError.localizedDescription
        }

        var tail = "\(errorDomain) / \(nsError.code)"
        if let status { tail += " / HTTP \(status)" }
        if let serverCode { tail += " / \(serverCode)" }
        return "\(reason)\n(\(tail))"
    }

    private static func formatURLSession(_ nsError: NSError) -> String {
        let urlString = (nsError.userInfo[NSURLErrorFailingURLErrorKey] as? URL)?.absoluteString
        let codeTail = "NSURLErrorDomain / \(nsError.code)"
        let tail = urlString.map { "\($0) \(codeTail)" } ?? codeTail
        let reason: String
        switch nsError.code {
        case NSURLErrorTimedOut:
            reason = String(localized: "s3.error.reason.timeout")
        case NSURLErrorCannotFindHost, NSURLErrorDNSLookupFailed:
            reason = String(localized: "s3.error.reason.hostNotFound")
        case NSURLErrorCannotConnectToHost:
            reason = String(localized: "s3.error.reason.cannotConnect")
        case NSURLErrorNotConnectedToInternet, NSURLErrorNetworkConnectionLost:
            reason = String(localized: "s3.error.reason.noNetwork")
        case NSURLErrorServerCertificateUntrusted,
             NSURLErrorServerCertificateHasBadDate,
             NSURLErrorServerCertificateHasUnknownRoot,
             NSURLErrorServerCertificateNotYetValid,
             NSURLErrorClientCertificateRejected,
             NSURLErrorClientCertificateRequired:
            reason = String(localized: "s3.error.reason.certUntrusted")
        case NSURLErrorSecureConnectionFailed:
            reason = String(localized: "s3.error.reason.tlsFailed")
        default:
            reason = nsError.localizedDescription
        }
        return "\(reason)\n(\(tail))"
    }

    private static func localizedReason(for code: S3ErrorCode) -> String {
        switch code {
        case .invalidAccessKeyID:
            return String(localized: "s3.error.invalidAccessKey")
        case .signatureDoesNotMatch:
            return String(localized: "s3.error.signatureMismatch")
        case .noSuchBucket:
            return String(localized: "s3.error.noSuchBucket")
        case .noSuchKey:
            return String(localized: "s3.error.noSuchKey")
        case .accessDenied:
            return String(localized: "s3.error.accessDenied")
        case .requestTimeTooSkewed:
            return String(localized: "s3.error.clockSkew")
        case .entityTooLarge:
            return String(localized: "s3.error.entityTooLarge")
        case .slowDown, .serviceUnavailable:
            return String(localized: "s3.error.slowDown")
        case .internalError:
            return String(localized: "s3.error.internalError")
        case .authorizationHeaderMalformed, .bucketRegionError:
            return String(localized: "s3.error.bucketRegion")
        }
    }

    private static func serverCode(in nsError: NSError) -> String? {
        nsError.userInfo[userInfoServerCodeKey] as? String
    }

    private static func nsErrorChain(_ error: Error) -> [NSError] {
        var visited: Set<String> = []
        var collected: [NSError] = []
        var pending: [Error] = [error]
        while let next = pending.popLast() {
            if let storage = next as? RemoteStorageClientError,
               case .underlying(let inner) = storage {
                pending.append(inner)
                continue
            }
            let ns = next as NSError
            let key = "\(ns.domain)#\(ns.code)"
            guard visited.insert(key).inserted else { continue }
            collected.append(ns)
            if let underlying = ns.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(underlying)
            }
        }
        return collected
    }
}

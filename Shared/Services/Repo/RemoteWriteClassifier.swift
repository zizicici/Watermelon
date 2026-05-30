import Foundation

nonisolated enum RemoteVerifyFailureKind: Sendable, Equatable {
    case cancelled
    case transient
    case permanent
}

nonisolated enum RemoteWriteClassifier {
    static func cancellationCause(in error: Error) -> CancellationError? {
        var current: Error? = error
        var visited: Set<ObjectIdentifier> = []
        while let e = current {
            if e is CancellationError { return CancellationError() }
            if case RemoteStorageClientError.underlying(let underlying) = e {
                current = underlying
                continue
            }
            let nsError = e as NSError
            guard visited.insert(ObjectIdentifier(nsError)).inserted else { return nil }
            if nsError.domain == NSURLErrorDomain, nsError.code == NSURLErrorCancelled {
                return CancellationError()
            }
            current = nsError.userInfo[NSUnderlyingErrorKey] as? Error
        }
        return nil
    }

    static func isCancellation(_ error: Error) -> Bool {
        cancellationCause(in: error) != nil
    }

    static func normalizedCancellation(_ error: Error) -> Error {
        cancellationCause(in: error) ?? error
    }

    static func isMetadataGateCancellation(_ error: MetadataCreateGate.Error) -> Bool {
        switch error {
        case .stagingVerificationFailed(_, let underlying),
             .finalVerificationFailed(_, let underlying):
            guard let underlying else { return false }
            return isCancellation(underlying)
        case .nonExclusiveFinalization:
            return false
        }
    }

    static func classifyVerifyFailure(_ error: Error) -> RemoteVerifyFailureKind {
        if isCancellation(error) { return .cancelled }
        if isStorageNotFoundError(error) { return .permanent }
        if let gateError = error as? MetadataCreateGate.Error {
            switch gateError {
            case .stagingVerificationFailed(_, let underlying),
                 .finalVerificationFailed(_, let underlying):
                guard let underlying else { return .permanent }
                return classifyVerifyFailure(underlying)
            case .nonExclusiveFinalization:
                return .permanent
            }
        }
        if let storage = error as? RemoteStorageClientError {
            switch storage {
            case .notConnected, .unavailable:
                return .transient
            case .externalStorageUnavailable, .invalidConfiguration, .unsupportedStorageType(_):
                return .permanent
            case .underlying(let underlying):
                return classifyVerifyFailure(underlying)
            }
        }
        if SMBErrorClassifier.isConnectionUnavailable(error)
            || WebDAVErrorClassifier.isConnectionUnavailable(error)
            || S3ErrorClassifier.isConnectionUnavailable(error)
            || SFTPErrorClassifier.isConnectionUnavailable(error) {
            return .transient
        }
        for nsError in nsErrorChain(error) {
            if nsError.domain == WebDAVClient.errorDomain,
               (500 ... 599).contains(nsError.code) || nsError.code == 408 || nsError.code == 429 {
                return .transient
            }
            if nsError.domain == S3ErrorClassifier.errorDomain {
                if let status = nsError.userInfo[S3ErrorClassifier.userInfoStatusCodeKey] as? Int,
                   (500 ... 599).contains(status) || status == 408 || status == 429 {
                    return .transient
                }
                if let serverCode = nsError.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String,
                   serverCode == "InternalError" || serverCode == "SlowDown" || serverCode == "ServiceUnavailable" {
                    return .transient
                }
            }
        }
        return .permanent
    }

    static func isTransientVerifyFailure(_ error: Error) -> Bool {
        classifyVerifyFailure(error) == .transient
    }

    static func nsErrorChain(_ error: Error) -> [NSError] {
        BackupErrorChain.nsErrorChain(error)
    }
}

import Foundation
#if canImport(Citadel)
import Citadel
#endif

// Shared fault classifier for lock/version/month probes. Collapses a remote error into one of four
// intentions so call sites can tell "the file really isn't there" from "the network blinked" without
// re-deriving backend-specific shapes.
nonisolated enum RemoteFaultLite {
    enum Category: Equatable, Sendable {
        case notFound
        case retryable
        case cancelled
        case terminal
    }

    static func classify(_ error: Error) -> Category {
        let chain = errorChain(error)
        if chain.contains(where: isCancellationNode) {
            return .cancelled
        }
        // Retryable is tested before notFound: a chain carrying a transient backend/session/transport
        // fault must not be read as object absence just because it also carries an ambiguous not-found
        // token. A clear absence (no retryable signal anywhere in the chain) still falls through to
        // .notFound. Fail closed toward "transient" when both are present.
        if isRetryable(chain) {
            return .retryable
        }
        if isNotFound(chain) {
            return .notFound
        }
        return .terminal
    }

    // MARK: - Cancellation

    private static func isCancellationNode(_ node: Error) -> Bool {
        if node is CancellationError { return true }
        let ns = node as NSError
        return ns.domain == NSURLErrorDomain && ns.code == NSURLErrorCancelled
    }

    // MARK: - Intentional absence

    private static func isNotFound(_ chain: [Error]) -> Bool {
        if s3IsNotFound(chain) { return true }
        if sftpIsNoSuchFile(chain) { return true }
        for node in chain {
            if SMBErrorClassifier.isNotFound(node) { return true }
            let ns = node as NSError
            if ns.domain == WebDAVClient.errorDomain, ns.code == 404 { return true }
            if ns.domain == NSPOSIXErrorDomain, ns.code == Int(ENOENT) { return true }
            if ns.domain == NSCocoaErrorDomain,
               ns.code == NSFileNoSuchFileError || ns.code == NSFileReadNoSuchFileError {
                return true
            }
        }
        return false
    }

    private static func s3IsNotFound(_ chain: [Error]) -> Bool {
        for node in chain {
            let ns = node as NSError
            guard ns.domain == S3ErrorClassifier.errorDomain else { continue }
            let serverCode = ns.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String
            if serverCode == "NoSuchKey" || serverCode == "NotFound" { return true }
            // NoSuchBucket is a configuration fault, not a missing object — leave it for `.terminal`.
            if serverCode == nil {
                let status = ns.userInfo[S3ErrorClassifier.userInfoStatusCodeKey] as? Int
                if status == 404 || ns.code == 404 { return true }
            }
        }
        return false
    }

    private static func sftpIsNoSuchFile(_ chain: [Error]) -> Bool {
        #if canImport(Citadel)
        for node in chain {
            if let sftp = node as? SFTPError, case .errorStatus(let status) = sftp,
               status.errorCode == .noSuchFile {
                return true
            }
        }
        #endif
        return false
    }

    // MARK: - Transport / probe uncertainty

    private static func isRetryable(_ chain: [Error]) -> Bool {
        if s3IsRetryable(chain) { return true }
        for node in chain {
            if let storage = node as? RemoteStorageClientError {
                switch storage {
                case .notConnected, .unavailable:
                    return true
                default:
                    break
                }
            }
            if SMBErrorClassifier.isConnectionUnavailable(node) { return true }
            if SFTPErrorClassifier.isConnectionUnavailable(node) { return true }
            let ns = node as NSError
            if ns.domain == NSURLErrorDomain, S3ErrorClassifier.isConnectionUnavailableURLErrorCode(ns.code) { return true }
        }
        return false
    }

    private static let retryableHTTPStatuses: Set<Int> = [429, 500, 502, 503, 504]

    private static func s3IsRetryable(_ chain: [Error]) -> Bool {
        for node in chain {
            let ns = node as NSError
            guard ns.domain == S3ErrorClassifier.errorDomain else { continue }
            if let serverCode = ns.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String,
               let code = S3ErrorClassifier.S3ErrorCode(rawValue: serverCode) {
                // SlowDown / ServiceUnavailable / InternalError are the transient server codes.
                if !code.isClientFault { return true }
                continue
            }
            let status = (ns.userInfo[S3ErrorClassifier.userInfoStatusCodeKey] as? Int) ?? ns.code
            if retryableHTTPStatuses.contains(status) { return true }
        }
        return false
    }

    // MARK: - Error chain

    // Unwraps both RemoteStorageClientError.underlying and NSUnderlyingErrorKey so every node can be
    // inspected on its own terms. Depth-capped because a malformed userInfo could cycle.
    private static func errorChain(_ error: Error, maxDepth: Int = 32) -> [Error] {
        var result: [Error] = []
        var pending: [Error] = [error]
        while let next = pending.popLast(), result.count < maxDepth {
            result.append(next)
            if let storage = next as? RemoteStorageClientError, case .underlying(let inner) = storage {
                pending.append(inner)
                continue
            }
            let ns = next as NSError
            if let underlying = ns.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(underlying)
            }
        }
        return result
    }
}

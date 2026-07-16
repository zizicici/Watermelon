import Foundation

nonisolated enum OneDriveErrorClassifier {
    static let errorDomain = "OneDriveClient"
    static let codeKey = "OneDriveErrorCode"
    static let retryAfterKey = "OneDriveRetryAfter"
    static let claimsKey = "OneDriveClaims"

    static func makeServiceError(
        statusCode: Int,
        code: String?,
        message: String?,
        retryAfter: Date?,
        claims: String?
    ) -> NSError {
        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: message ?? String.localizedStringWithFormat(
                String(localized: "onedrive.error.graph.requestFailedHTTP"),
                statusCode
            )
        ]
        if let code { userInfo[codeKey] = code }
        if let retryAfter { userInfo[retryAfterKey] = retryAfter }
        if let claims { userInfo[claimsKey] = claims }
        return NSError(domain: errorDomain, code: statusCode, userInfo: userInfo)
    }

    static func isConnectionUnavailable(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            if node is OneDriveAuthenticationError { return false }
            let ns = node as NSError
            if ns.domain == NSURLErrorDomain {
                return S3ErrorClassifier.isConnectionUnavailableURLErrorCode(ns.code)
            }
            guard ns.domain == errorDomain else { return false }
            return isRetryableStatus(ns.code)
        }
    }

    static func isRetryableStatus(_ statusCode: Int) -> Bool {
        [408, 429, 500, 502, 503, 504, 509].contains(statusCode)
    }

    static func isNotFound(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            let ns = node as NSError
            guard ns.domain == errorDomain, ns.code == 404 else { return false }
            return (ns.userInfo[codeKey] as? String) == "itemNotFound"
        }
    }

    static func isNameCollision(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            let ns = node as NSError
            guard ns.domain == errorDomain else { return false }
            let code = ns.userInfo[codeKey] as? String
            return ns.code == 409 && code == "nameAlreadyExists"
                || ns.code == 412 && code == "preconditionFailed"
        }
    }

    static func describe(_ error: Error) -> String {
        if let auth = errorChain(error).compactMap({ $0 as? OneDriveAuthenticationError }).first {
            return auth.localizedDescription
        }
        for node in errorChain(error) {
            let ns = node as NSError
            guard ns.domain == errorDomain else { continue }
            switch ns.code {
            case 401:
                return OneDriveAuthenticationError.reauthenticationRequired.localizedDescription
            case 400 where isDriveNotReady(ns):
                return String(localized: "onedrive.error.driveNotReady")
            case 403:
                return String(localized: "onedrive.error.accessDenied")
            case 404 where (ns.userInfo[codeKey] as? String) == "itemNotFound":
                return String(localized: "onedrive.error.itemMissing")
            case 507:
                return String(localized: "onedrive.error.insufficientStorage")
            default:
                return ns.localizedDescription
            }
        }
        return error.localizedDescription
    }

    private static func isDriveNotReady(_ error: NSError) -> Bool {
        let description = error.localizedDescription.lowercased()
        return description.contains("mysite")
            || description.contains("personal site")
            || description.contains("onedrive") && description.contains("provision")
    }

    static func retryAfter(from response: HTTPURLResponse, now: Date = Date()) -> Date? {
        guard let raw = response.value(forHTTPHeaderField: "Retry-After")?.trimmingCharacters(in: .whitespacesAndNewlines),
              !raw.isEmpty else { return nil }
        if let seconds = TimeInterval(raw) {
            return now.addingTimeInterval(max(0, seconds))
        }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss zzz"
        return formatter.date(from: raw)
    }

    static func sanitizedTransportError(_ error: Error) -> Error {
        if error is CancellationError { return error }
        let ns = error as NSError
        guard ns.domain == NSURLErrorDomain else {
            return RemoteStorageClientError.unavailable
        }
        let description = URLError(URLError.Code(rawValue: ns.code)).localizedDescription
        return NSError(
            domain: NSURLErrorDomain,
            code: ns.code,
            userInfo: [NSLocalizedDescriptionKey: description]
        )
    }

    private static func errorChain(_ error: Error, maxDepth: Int = 32) -> [Error] {
        var result: [Error] = []
        var pending: [Error] = [error]
        while let next = pending.popLast(), result.count < maxDepth {
            result.append(next)
            if let storage = next as? RemoteStorageClientError, case .underlying(let inner) = storage {
                pending.append(inner)
            }
            let ns = next as NSError
            if let inner = ns.userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(inner)
            }
        }
        return result
    }
}

nonisolated struct OneDriveMutationOutcomeUnknownError: LocalizedError, Sendable {
    let operation: String

    var errorDescription: String? {
        String.localizedStringWithFormat(
            String(localized: "onedrive.error.mutationOutcomeUnknown"),
            operation
        )
    }
}

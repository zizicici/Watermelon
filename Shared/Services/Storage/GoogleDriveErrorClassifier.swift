import Foundation

nonisolated enum GoogleDriveErrorClassifier {
    static let errorDomain = "GoogleDriveClient"
    static let userInfoStatusCodeKey = "GoogleDriveStatusCode"
    static let userInfoReasonsKey = "GoogleDriveReasons"
    static let userInfoMessageKey = "GoogleDriveMessage"

    static func makeServiceError(data: Data, response: HTTPURLResponse) -> NSError {
        let payload = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        let errorPayload = payload?["error"] as? [String: Any]
        let message = errorPayload?["message"] as? String
        let reasons = (errorPayload?["errors"] as? [[String: Any]])?
            .compactMap { $0["reason"] as? String } ?? []
        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: String(localized: "googledrive.error.requestFailed"),
            userInfoStatusCodeKey: response.statusCode,
            userInfoReasonsKey: reasons
        ]
        if let message, !message.isEmpty { userInfo[userInfoMessageKey] = message }
        return NSError(domain: errorDomain, code: response.statusCode, userInfo: userInfo)
    }

    static func isNotFound(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            let ns = node as NSError
            guard ns.domain == errorDomain else { return false }
            return statusCode(in: ns) == 404 || reasons(in: ns).contains("notFound")
        }
    }

    static func isNameCollision(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            let ns = node as NSError
            return ns.domain == errorDomain && statusCode(in: ns) == 409
        }
    }

    static func isConnectionUnavailable(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            let ns = node as NSError
            if ns.domain == NSURLErrorDomain, connectionUnavailableURLCodes.contains(ns.code) {
                return true
            }
            guard ns.domain == errorDomain else { return false }
            if retryableStatuses.contains(statusCode(in: ns)) { return true }
            let reasonSet = Set(reasons(in: ns))
            return !reasonSet.isDisjoint(with: retryableReasons)
        }
    }

    static func isMutationOutcomeUnknown(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            if let authentication = node as? GoogleDriveAuthenticationError,
               case .invalidResponse = authentication {
                return true
            }
            if let storage = node as? RemoteStorageClientError,
               case .unavailable = storage {
                return true
            }
            let ns = node as NSError
            if ns.domain == NSURLErrorDomain {
                return ns.code == NSURLErrorTimedOut || ns.code == NSURLErrorNetworkConnectionLost
            }
            guard ns.domain == errorDomain else { return false }
            return ambiguousMutationStatuses.contains(statusCode(in: ns))
        }
    }

    static func describe(_ error: Error) -> String {
        for node in errorChain(error) {
            if let auth = node as? GoogleDriveAuthenticationError {
                return auth.localizedDescription
            }
            let ns = node as NSError
            guard ns.domain == errorDomain else { continue }
            let status = statusCode(in: ns)
            let reasonSet = Set(reasons(in: ns))
            let message: String
            if status == 401 || reasonSet.contains("authError") {
                message = String(localized: "googledrive.error.auth.reauthenticationRequired")
            } else if reasonSet.contains("storageQuotaExceeded") {
                message = String(localized: "googledrive.error.insufficientSpace")
            } else if status == 429 || !reasonSet.isDisjoint(with: retryableReasons) {
                message = String(localized: "googledrive.error.throttled")
            } else if status == 404 {
                message = String(localized: "googledrive.error.notFound")
            } else if reasonSet.contains("accessNotConfigured") || reasonSet.contains("serviceDisabled") {
                message = String(localized: "googledrive.error.apiNotEnabled")
            } else if status == 403 {
                message = String(localized: "googledrive.error.accessDenied")
            } else {
                message = (ns.userInfo[userInfoMessageKey] as? String)
                    ?? String(localized: "googledrive.error.requestFailed")
            }
            return "\(message)\n(Google Drive / \(status))"
        }
        return error.localizedDescription
    }

    static func retryAfter(from response: HTTPURLResponse, now: Date = Date()) -> Date? {
        guard let raw = response.value(forHTTPHeaderField: "Retry-After")?
            .trimmingCharacters(in: .whitespacesAndNewlines),
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

    private static let retryableStatuses: Set<Int> = [408, 429, 500, 502, 503, 504]
    private static let ambiguousMutationStatuses: Set<Int> = [408, 500, 502, 503, 504]
    private static let retryableReasons: Set<String> = [
        "rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded", "backendError"
    ]
    private static let connectionUnavailableURLCodes: Set<Int> = [
        NSURLErrorTimedOut,
        NSURLErrorCannotFindHost,
        NSURLErrorDNSLookupFailed,
        NSURLErrorCannotConnectToHost,
        NSURLErrorNotConnectedToInternet,
        NSURLErrorNetworkConnectionLost
    ]

    private static func statusCode(in error: NSError) -> Int {
        (error.userInfo[userInfoStatusCodeKey] as? Int) ?? error.code
    }

    private static func reasons(in error: NSError) -> [String] {
        error.userInfo[userInfoReasonsKey] as? [String] ?? []
    }

    private static func errorChain(_ error: Error, maxDepth: Int = 32) -> [Error] {
        var result: [Error] = []
        var pending: [Error] = [error]
        while let next = pending.popLast(), result.count < maxDepth {
            result.append(next)
            if let storage = next as? RemoteStorageClientError, case .underlying(let inner) = storage {
                pending.append(inner)
            }
            if let inner = (next as NSError).userInfo[NSUnderlyingErrorKey] as? Error {
                pending.append(inner)
            }
        }
        return result
    }
}

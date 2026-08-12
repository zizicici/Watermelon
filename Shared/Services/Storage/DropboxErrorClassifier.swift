import Foundation

nonisolated enum DropboxErrorClassifier {
    static let errorDomain = "DropboxClient"
    static let userInfoStatusCodeKey = "DropboxStatusCode"
    static let userInfoSummaryKey = "DropboxErrorSummary"
    static let userInfoRetryAfterKey = "DropboxRetryAfter"
    static let userInfoEndpointKey = "DropboxEndpoint"
    static let userInfoTagPathKey = "DropboxErrorTagPath"
    static let userInfoUserMessageKey = "DropboxUserMessage"
    static let userInfoRequestIDKey = "DropboxRequestID"

    static func describe(_ error: Error) -> String {
        for node in errorChain(error) {
            if let auth = node as? DropboxAuthenticationError {
                return auth.localizedDescription
            }
            let ns = node as NSError
            if ns.domain == errorDomain {
                let summary = (ns.userInfo[userInfoSummaryKey] as? String) ?? ns.localizedDescription
                let userMessage = ns.userInfo[userInfoUserMessageKey] as? String
                return "\(localizedReason(status: statusCode(in: ns), summary: summary, userMessage: userMessage))\n(Dropbox / \(ns.code))"
            }
            if ns.domain == NSURLErrorDomain {
                return ns.localizedDescription
            }
        }
        return error.localizedDescription
    }

    static func makeServiceError(
        statusCode: Int,
        summary: String?,
        retryAfter: Date? = nil,
        endpoint: String? = nil,
        tagPath: String? = nil,
        userMessage: String? = nil,
        requestID: String? = nil
    ) -> NSError {
        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: String(localized: "dropbox.error.requestFailed"),
            userInfoStatusCodeKey: statusCode
        ]
        if let summary, !summary.isEmpty { userInfo[userInfoSummaryKey] = summary }
        if let retryAfter { userInfo[userInfoRetryAfterKey] = retryAfter }
        if let endpoint, !endpoint.isEmpty { userInfo[userInfoEndpointKey] = endpoint }
        if let tagPath, !tagPath.isEmpty { userInfo[userInfoTagPathKey] = tagPath }
        if let userMessage, !userMessage.isEmpty { userInfo[userInfoUserMessageKey] = userMessage }
        if let requestID, !requestID.isEmpty { userInfo[userInfoRequestIDKey] = requestID }
        return NSError(domain: errorDomain, code: statusCode, userInfo: userInfo)
    }

    static func makeServiceError(
        data: Data,
        response: HTTPURLResponse,
        endpoint: String
    ) -> NSError {
        let payload = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        let summary = payload?["error_summary"] as? String
        let tagPath = (payload?["error"] as? [String: Any]).flatMap(tagPath(from:))
        let userMessage: String? = {
            if let value = payload?["user_message"] as? String { return value }
            return (payload?["user_message"] as? [String: Any])?["text"] as? String
        }()
        return makeServiceError(
            statusCode: response.statusCode,
            summary: summary,
            retryAfter: retryAfter(from: response),
            endpoint: endpoint,
            tagPath: tagPath,
            userMessage: userMessage,
            requestID: response.value(forHTTPHeaderField: "X-Dropbox-Request-Id")
        )
    }

    static func isNotFound(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            let ns = node as NSError
            guard ns.domain == errorDomain, statusCode(in: ns) == 409 else { return false }
            let components = errorComponents(in: ns)
            guard let notFoundIndex = components.firstIndex(of: "not_found"), notFoundIndex > 0 else {
                return false
            }
            return targetLookupTags.contains(components[notFoundIndex - 1])
        }
    }

    static func isNameCollision(_ error: Error) -> Bool {
        errorChain(error).contains { node in
            let ns = node as NSError
            guard ns.domain == errorDomain else { return false }
            guard statusCode(in: ns) == 409 else { return false }
            let components = errorComponents(in: ns)
            return components.indices.contains { index in
                guard components[index] == "conflict",
                      index > 0,
                      targetWriteTags.contains(components[index - 1]),
                      components.indices.contains(index + 1) else {
                    return false
                }
                return conflictEntryTags.contains(components[index + 1])
            }
        }
    }

    static func isConnectionUnavailable(_ error: Error) -> Bool {
        for node in errorChain(error) {
            let ns = node as NSError
            if ns.domain == NSURLErrorDomain, connectionUnavailableURLCodes.contains(ns.code) {
                return true
            }
            if ns.domain == errorDomain {
                if retryableStatuses.contains(statusCode(in: ns)) { return true }
                if errorComponents(in: ns).contains("too_many_write_operations") { return true }
            }
        }
        return false
    }

    static func retryAfter(in error: Error) -> Date? {
        errorChain(error).compactMap { node in
            let ns = node as NSError
            guard ns.domain == errorDomain else { return nil }
            return ns.userInfo[userInfoRetryAfterKey] as? Date
        }.max()
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
    private static let targetLookupTags: Set<String> = ["path", "path_lookup", "from_lookup"]
    private static let targetWriteTags: Set<String> = ["path", "to"]
    private static let conflictEntryTags: Set<String> = ["file", "folder"]
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

    private static func localizedReason(status: Int, summary: String, userMessage: String?) -> String {
        if let userMessage, !userMessage.isEmpty { return userMessage }
        let lowered = summary.lowercased()
        if status == 401 { return String(localized: "dropbox.error.auth.reauthenticationRequired") }
        if status == 403 || lowered.contains("no_write_permission") || lowered.contains("access_restricted") {
            return String(localized: "dropbox.error.accessDenied")
        }
        if lowered.contains("insufficient_space") { return String(localized: "dropbox.error.insufficientSpace") }
        if lowered.contains("not_found") { return String(localized: "dropbox.error.notFound") }
        if status == 429 || lowered.contains("too_many_write_operations") {
            return String(localized: "dropbox.error.throttled")
        }
        return summary.isEmpty ? String(localized: "dropbox.error.requestFailed") : summary
    }

    private static func errorComponents(in error: NSError) -> [String] {
        let tagComponents = ((error.userInfo[userInfoTagPathKey] as? String) ?? "")
            .lowercased().split(separator: "/").map(String.init)
        let summaryComponents = ((error.userInfo[userInfoSummaryKey] as? String) ?? "")
            .lowercased().split(separator: "/").map(String.init)
        return tagComponents.count > 1 || summaryComponents.isEmpty ? tagComponents : summaryComponents
    }

    private static func tagPath(from value: [String: Any]) -> String? {
        guard let tag = value[".tag"] as? String, !tag.isEmpty else { return nil }
        guard let child = value[tag] as? [String: Any], let childPath = tagPath(from: child) else {
            return tag
        }
        return "\(tag)/\(childPath)"
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

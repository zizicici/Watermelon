import Foundation

final actor WebDAVClient: RemoteStorageClientProtocol {
    nonisolated static let errorDomain = "WebDAVClient"

    struct Config {
        let endpointURL: URL
        let username: String
        let password: String
    }

    private struct PropfindEntry {
        var href: String = ""
        var displayName: String?
        var contentLength: Int64?
        var quotaAvailableBytes: Int64?
        var quotaUsedBytes: Int64?
        var creationDate: Date?
        var modificationDate: Date?
        var hasAnyStatus = false
        var hasSuccessStatus = false
        var statusCodes: [Int] = []
        var isDirectory = false

        var firstFailureStatusCode: Int? {
            statusCodes.first(where: { !(200 ... 299).contains($0) })
        }

        // First per-entry status that is neither success nor a genuine not-found, i.e. an unresolved
        // backend status (locked/forbidden/server error) that must fail closed rather than read as absence.
        var firstUnresolvedFailureStatusCode: Int? {
            statusCodes.first(where: { !(200 ... 299).contains($0) && !WebDAVClient.isNotFoundStatus($0) })
        }
    }

    nonisolated func shouldSetModificationDate() -> Bool {
        true
    }

    // WebDAV MOVE varies by gateway; some (123pan) alias content. Probe once per session and memoize — a
    // well-behaved server resolves to independent and keeps the fast temp→MOVE publish.
    private var moveIndependenceProbeTask: Task<Bool, Never>?

    func resolveMoveIsNonIndependent(basePath: String) async -> Bool {
        if let task = moveIndependenceProbeTask { return await task.value }
        let task = Task { await RemoteMoveIndependenceProbe.detectNonIndependentMove(client: self, basePath: basePath) }
        moveIndependenceProbeTask = task
        return await task.value
    }

    nonisolated func shouldLimitUploadRetries(for error: Error) -> Bool {
        Self.isUploadWatchdogTimeout(error)
    }

    nonisolated func cancelActiveOperationsForAbandonment() {
        metadataTasks.cancelAll()
        transferTasks.cancelAll()
    }

    private final class PropfindXMLParser: NSObject, XMLParserDelegate {
        private static let textElements: Set<String> = [
            "href",
            "displayname",
            "getcontentlength",
            "quota-available-bytes",
            "quota-used-bytes",
            "getlastmodified",
            "creationdate",
            "status"
        ]

        private(set) var entries: [PropfindEntry] = []
        private var currentEntry: PropfindEntry?
        private var textBuffer = ""
        private var elementStack: [String] = []

        func parse(_ data: Data) throws -> [PropfindEntry] {
            let parser = XMLParser(data: data)
            parser.delegate = self
            guard parser.parse() else {
                throw parser.parserError ?? NSError(
                    domain: WebDAVClient.errorDomain,
                    code: -1001,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "webdav.error.parsePropfindResponse")]
                )
            }
            return entries
        }

        func parser(_: XMLParser, didStartElement elementName: String, namespaceURI _: String?, qualifiedName qName: String?, attributes _: [String: String] = [:]) {
            let name = Self.localName(elementName: elementName, qName: qName)
            elementStack.append(name)

            if name == "response" {
                currentEntry = PropfindEntry()
                textBuffer = ""
                return
            }

            guard currentEntry != nil else { return }
            if Self.textElements.contains(name) {
                textBuffer = ""
                return
            }
            if name == "collection",
               elementStack.contains("resourcetype"),
               var entry = currentEntry {
                entry.isDirectory = true
                currentEntry = entry
            }
        }

        func parser(_: XMLParser, foundCharacters string: String) {
            guard currentEntry != nil else { return }
            textBuffer.append(string)
        }

        func parser(_: XMLParser, didEndElement elementName: String, namespaceURI _: String?, qualifiedName qName: String?) {
            let name = Self.localName(elementName: elementName, qName: qName)
            defer {
                if !elementStack.isEmpty {
                    elementStack.removeLast()
                }
                textBuffer = ""
            }

            guard var entry = currentEntry else { return }
            let value = textBuffer.trimmingCharacters(in: .whitespacesAndNewlines)

            switch name {
            case "href":
                if !value.isEmpty {
                    entry.href = value
                }
            case "displayname":
                if !value.isEmpty {
                    entry.displayName = value
                }
            case "getcontentlength":
                if let parsed = Int64(value) {
                    entry.contentLength = parsed
                }
            case "quota-available-bytes":
                if let parsed = Int64(value) {
                    entry.quotaAvailableBytes = parsed
                }
            case "quota-used-bytes":
                if let parsed = Int64(value) {
                    entry.quotaUsedBytes = parsed
                }
            case "creationdate":
                entry.creationDate = WebDAVClient.parseDate(value)
            case "getlastmodified":
                entry.modificationDate = WebDAVClient.parseDate(value)
            case "status":
                if let statusCode = WebDAVClient.parseHTTPStatusCode(value) {
                    entry.hasAnyStatus = true
                    entry.statusCodes.append(statusCode)
                    if (200 ... 299).contains(statusCode) {
                        entry.hasSuccessStatus = true
                    }
                }
            default:
                break
            }

            if name == "response" {
                if !entry.href.isEmpty {
                    entries.append(entry)
                }
                currentEntry = nil
            } else {
                currentEntry = entry
            }
        }

        private static func localName(elementName: String, qName: String?) -> String {
            let source = qName.flatMap { $0.isEmpty ? nil : $0 } ?? elementName
            if let idx = source.lastIndex(of: ":") {
                return String(source[source.index(after: idx)...]).lowercased()
            }
            return source.lowercased()
        }
    }

    private static let formatterLock = NSLock()
    private static let percentEscapeRegex = try! NSRegularExpression(pattern: "%[0-9A-Fa-f]{2}")
    private static let metadataRequestTimeout: TimeInterval = 45
    private static let metadataResourceTimeout: TimeInterval = 120
    private static let transferTimeout: TimeInterval = 7 * 24 * 60 * 60
    private static let uploadStallTimeout: TimeInterval = 3 * 60
    // Server-side post-processing can be slow on NAS/WebDAV gateways; this only starts after the body is sent.
    private static let uploadResponseTimeout: TimeInterval = 5 * 60
    private static let downloadInitialResponseTimeout: TimeInterval = 5 * 60
    private static let downloadStallTimeout: TimeInterval = 3 * 60
    // DispatchTime uptime pauses during device sleep, matching foreground URLSession transfer behavior.
    private static let uploadWatchdogInterval: TimeInterval = 5
    static let uploadStalledErrorCode = -1301          // internal: locked by WebDAVClientPartialUploadCleanupTests
    static let uploadResponseTimeoutErrorCode = -1302  // internal: locked by WebDAVClientPartialUploadCleanupTests
    private static let downloadStalledErrorCode = -1303
    private static let uploadBytesSentKey = "WebDAVUploadBytesSent"
    private static let uploadExpectedBytesKey = "WebDAVUploadExpectedBytes"
    private static let downloadBytesWrittenKey = "WebDAVDownloadBytesWritten"
    private static let downloadExpectedBytesKey = "WebDAVDownloadExpectedBytes"
    private static let lockedStatusCode = 423
    private static let createDirectoryLockedRetryJitterCapsNanos: [UInt64] = [
        200_000_000,
        500_000_000,
        1_000_000_000,
        2_000_000_000
    ]
    private static let transferStallTimeouts = URLSessionStallWatchdog.Timeouts(
        uploadBodyStall: uploadStallTimeout,
        uploadResponseStall: uploadResponseTimeout,
        downloadFirstByte: downloadInitialResponseTimeout,
        downloadStall: downloadStallTimeout,
        pollInterval: uploadWatchdogInterval
    )
    private static let rfc1123Formatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss zzz"
        return formatter
    }()

    private static let iso8601Formatter = ISO8601DateFormatter()

    private let config: Config
    private let session: URLSession
    private let transferSession: URLSession
    private let transferDelegate = URLSessionStallWatchdog.Delegate()
    nonisolated private let metadataTasks = URLSessionTaskRegistry()
    nonisolated private let transferTasks = URLSessionTaskRegistry()
    private let endpointPathPrefix: String
    private var isConnected = false
    private var pendingCancelledUploadCleanupPaths: [String] = []
    // http only: endpoint with host pre-resolved to an IPv4 literal so URLSession skips the ~5s `.local` mDNS wait.
    private var resolvedEndpointURL: URL?

    init(config: Config) {
        self.config = config
        let normalizedEndpoint = Self.normalizedEndpointURL(config.endpointURL)
        let normalizedPath = Self.normalizedPercentEncodedPath(of: normalizedEndpoint)
        let trimmed = normalizedPath == "/" ? "" : normalizedPath.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        endpointPathPrefix = trimmed.isEmpty ? "/" : "/" + trimmed

        let sessionConfig = URLSessionConfiguration.ephemeral
        sessionConfig.timeoutIntervalForRequest = Self.metadataRequestTimeout
        sessionConfig.timeoutIntervalForResource = Self.metadataResourceTimeout
        session = URLSession(configuration: sessionConfig)

        let transferSessionConfig = URLSessionConfiguration.ephemeral
        transferSessionConfig.timeoutIntervalForRequest = Self.transferTimeout
        transferSessionConfig.timeoutIntervalForResource = Self.transferTimeout
        transferSession = URLSession(
            configuration: transferSessionConfig,
            delegate: transferDelegate,
            delegateQueue: nil
        )
    }

    deinit {
        session.invalidateAndCancel()
        transferSession.invalidateAndCancel()
    }

    private var activeEndpointURL: URL { resolvedEndpointURL ?? config.endpointURL }

    // http only: connect by a pre-resolved IPv4 so URLSession skips the ~5s `.local` mDNS wait; https keeps the
    // hostname (TLS cert is bound to it). The original host stays as the Host header (makeRequest) for vhosts.
    private func resolveEndpointIfHTTP() async {
        guard resolvedEndpointURL == nil,
              config.endpointURL.scheme?.lowercased() == "http",
              let host = config.endpointURL.host,
              let ip = await HostnameResolver.resolvedIPv4(host), ip != host,
              var components = URLComponents(url: config.endpointURL, resolvingAgainstBaseURL: false) else { return }
        components.host = ip
        resolvedEndpointURL = components.url
    }

    func connect() async throws {
        if isConnected { return }
        await resolveEndpointIfHTTP()
        do {
            try await performConnectProbe()
        } catch {
            if error is CancellationError || Task.isCancelled { throw error }
            // A stale/wrong resolved IP can fail at the network OR HTTP layer (different service answering, auth,
            // not-a-WebDAV); retry once on the original hostname so the fast path can't become a regression.
            guard resolvedEndpointURL != nil else { throw error }
            resolvedEndpointURL = nil
            try await performConnectProbe()
        }
        isConnected = true
    }

    private func performConnectProbe() async throws {
        let request = makeRequest(
            url: Self.directoryURL(from: Self.normalizedEndpointURL(activeEndpointURL)),
            method: "PROPFIND",
            headers: [
                "Depth": "0",
                "Content-Type": "application/xml; charset=utf-8"
            ],
            body: Self.propfindBody
        )
        let (data, response) = try await sendData(request)
        let status = response.statusCode
        guard status == 207 || (200 ... 299).contains(status) else {
            if status == 401 || status == 403 {
                throw Self.authenticationError(status, url: response.url)
            }
            if status == 405 {
                throw RemoteStorageClientError.underlying(
                    NSError(
                        domain: WebDAVClient.errorDomain,
                        code: -1200,
                        userInfo: [
                            NSLocalizedDescriptionKey: String(localized: "webdav.error.notAService")
                        ]
                    )
                )
            }
            throw Self.statusError(status, method: "PROPFIND", url: response.url)
        }

        if status != 207 {
            let hasDAVHeader = response.value(forHTTPHeaderField: "DAV") != nil
            let parsedEntries = try? PropfindXMLParser().parse(data)
            let looksLikeWebDAV = hasDAVHeader || ((parsedEntries?.isEmpty == false))
            guard looksLikeWebDAV else {
                throw RemoteStorageClientError.underlying(
                    NSError(
                        domain: WebDAVClient.errorDomain,
                        code: -1200,
                        userInfo: [
                            NSLocalizedDescriptionKey: String(localized: "webdav.error.notAService")
                        ]
                    )
                )
            }
        }
    }

    func disconnect() async {
        await drainPendingCancelledUploadCleanup()
        isConnected = false
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()
        let targetURL = try remoteCollectionURL(forRemotePath: "/")
        let request = makeRequest(
            url: targetURL,
            method: "PROPFIND",
            headers: [
                "Depth": "0",
                "Content-Type": "application/xml; charset=utf-8"
            ],
            body: Self.quotaPropfindBody
        )
        let (data, response) = try await sendData(request)
        let status = response.statusCode
        if status == 404 {
            return nil
        }
        guard status == 207 || (200 ... 299).contains(status) else {
            throw Self.statusError(status, method: "PROPFIND", url: request.url)
        }

        let parsedEntries = try PropfindXMLParser().parse(data)
        let baseURL = Self.directoryURL(from: response.url ?? targetURL)
        let normalizedRoot = try Self.canonicalRemotePath("/")
        let targetEntry = parsedEntries.first { entry in
            guard !entry.hasAnyStatus || entry.hasSuccessStatus else { return false }
            guard let path = remotePath(fromHref: entry.href, relativeTo: baseURL) else { return false }
            return path == normalizedRoot
        } ?? parsedEntries.first(where: { !$0.hasAnyStatus || $0.hasSuccessStatus })

        guard let targetEntry else { return nil }
        let available = targetEntry.quotaAvailableBytes
        let total: Int64?
        if let available, let used = targetEntry.quotaUsedBytes {
            let (sum, overflow) = available.addingReportingOverflow(used)
            total = overflow ? nil : sum
        } else {
            total = nil
        }
        if available == nil, total == nil {
            return nil
        }
        return RemoteStorageCapacity(availableBytes: available, totalBytes: total)
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()
        let normalizedTarget = RemotePathBuilder.normalizePath(path)
        let targetURL = try remoteCollectionURL(forRemotePath: normalizedTarget)
        let request = makeRequest(
            url: targetURL,
            method: "PROPFIND",
            headers: [
                "Depth": "1",
                "Content-Type": "application/xml; charset=utf-8"
            ],
            body: Self.propfindBody
        )
        let (data, response) = try await sendData(request)
        let status = response.statusCode
        guard status == 207 || (200 ... 299).contains(status) else {
            throw Self.statusError(status, method: "PROPFIND", url: request.url)
        }

        let parsedEntries = try PropfindXMLParser().parse(data)
        let baseURL = Self.directoryURL(from: response.url ?? targetURL)
        let normalizedTargetKey = try Self.canonicalRemotePath(normalizedTarget)
        var entries: [RemoteStorageEntry] = []
        entries.reserveCapacity(parsedEntries.count)

        for parsed in parsedEntries {
            // Any unresolved per-entry status (locked/forbidden/server error) — even alongside a 2xx property
            // status, e.g. a failed `resourcetype` that hides directory-ness — makes the entry untrustworthy:
            // fail closed so callers never read a partial/misparsed listing as a complete one.
            if let failure = parsed.firstUnresolvedFailureStatusCode {
                throw Self.statusError(failure, method: "PROPFIND", url: request.url)
            }
            if parsed.hasAnyStatus, !parsed.hasSuccessStatus {
                continue   // only genuine not-found (404) statuses remain → absent member, skip
            }
            guard let remotePath = remotePath(fromHref: parsed.href, relativeTo: baseURL) else { continue }
            if remotePath == normalizedTargetKey { continue }
            let name = Self.entryName(forRemotePath: remotePath, displayName: parsed.displayName, href: parsed.href)
            entries.append(
                RemoteStorageEntry(
                    path: Self.decodedEntryPath(fromRemotePath: remotePath),
                    name: name,
                    isDirectory: parsed.isDirectory,
                    size: parsed.contentLength ?? 0,
                    creationDate: parsed.creationDate,
                    modificationDate: parsed.modificationDate
                )
            )
        }
        return entries
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        try await metadata(path: path, requestCollectionURL: false)
    }

    private func metadata(path: String, requestCollectionURL: Bool) async throws -> RemoteStorageEntry? {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()
        let normalizedTarget = RemotePathBuilder.normalizePath(path)
        let targetURL = try requestURL(forRemotePath: normalizedTarget, requestCollectionURL: requestCollectionURL)
        let request = makeRequest(
            url: targetURL,
            method: "PROPFIND",
            headers: [
                "Depth": "0",
                "Content-Type": "application/xml; charset=utf-8"
            ],
            body: Self.propfindBody
        )
        let (data, response) = try await sendData(request)
        let status = response.statusCode
        if status == 404 {
            return nil
        }
        guard status == 207 || (200 ... 299).contains(status) else {
            throw Self.statusError(status, method: "PROPFIND", url: request.url)
        }

        let parsedEntries = try PropfindXMLParser().parse(data)
        let baseURL = requestCollectionURL
            ? Self.directoryURL(from: response.url ?? targetURL)
            : (response.url ?? targetURL)
        let normalizedTargetKey = try Self.canonicalRemotePath(normalizedTarget)
        for parsed in parsedEntries {
            guard let remotePath = remotePath(fromHref: parsed.href, relativeTo: baseURL),
                  remotePath == normalizedTargetKey else {
                continue
            }
            // Any unresolved target status (locked/forbidden/server error) — even alongside a 2xx property
            // status, e.g. a failed `resourcetype` that would mis-report directory-ness — must throw so an
            // absence/type check never trusts a partial result. Only genuine not-found (404) maps to nil.
            if let failure = parsed.firstUnresolvedFailureStatusCode {
                throw Self.statusError(failure, method: "PROPFIND", url: request.url)
            }
            if parsed.hasAnyStatus, !parsed.hasSuccessStatus {
                return nil
            }
            let name = Self.entryName(forRemotePath: remotePath, displayName: parsed.displayName, href: parsed.href)
            return RemoteStorageEntry(
                path: Self.decodedEntryPath(fromRemotePath: remotePath),
                name: name,
                isDirectory: parsed.isDirectory,
                size: parsed.contentLength ?? 0,
                creationDate: parsed.creationDate,
                modificationDate: parsed.modificationDate
            )
        }
        return nil
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try await upload(
            localURL: localURL,
            remotePath: remotePath,
            mode: .replace,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func upload(
        localURL: URL,
        remotePath: String,
        mode: RemoteUploadMode,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()
        if respectTaskCancellation {
            try Task.checkCancellation()
        }

        let targetURL = try remoteURL(forRemotePath: remotePath)
        var headers = ["Content-Type": "application/octet-stream"]
        if mode == .createIfAbsent {
            headers["If-None-Match"] = "*"
        }
        let request = makeRequest(url: targetURL, method: "PUT", headers: headers)
        do {
            let (_, response) = try await sendUpload(request, fromFile: localURL, onProgress: onProgress)
            guard (200 ... 299).contains(response.statusCode) else {
                if mode == .createIfAbsent, response.statusCode == 409 || response.statusCode == 412 {
                    throw remoteStorageNameCollisionError(path: remotePath)
                }
                throw Self.statusError(response.statusCode, method: "PUT", url: request.url)
            }
        } catch {
            // Only a mid-body stall queues cleanup (shouldCleanupPartialUpload) — it proves the body was not fully
            // sent. A response-timeout or a bare cancellation is excluded: both can arrive after the body landed
            // complete, so the caller's read-back / re-upload handles any partial they leave.
            if mode == .replace, Self.shouldCleanupPartialUpload(error) {
                enqueueCancelledUploadCleanup(for: remotePath)
            }
            if Self.isCancellationError(error) {
                throw CancellationError()
            }
            throw error
        }

        // Reaching here means a 2xx landed — the object is COMPLETE on the server. A cancellation observed now must
        // NOT queue it for cleanup: deleting a complete object (most critically a direct-PUT canonical) is wrong.
        if respectTaskCancellation {
            try Task.checkCancellation()
        }
        onProgress?(1)
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()

        let targetURL = try remoteURL(forRemotePath: path)
        let davBody = Self.proppatchGetLastModifiedBody(date: date)
        let msBody = Self.proppatchWin32LastModifiedBody(date: date)
        let bodies = [davBody, msBody]
        var sawUnknownMultiStatus = false

        for body in bodies {
            let request = makeRequest(
                url: targetURL,
                method: "PROPPATCH",
                headers: ["Content-Type": "application/xml; charset=utf-8"],
                body: body
            )
            let (data, response) = try await sendData(request)
            let status = response.statusCode
            if status == 207 || (200 ... 299).contains(status) {
                if status == 207 {
                    switch evaluateMultiStatus(data, relativeTo: response.url ?? targetURL, targetPath: path) {
                    case .success:
                        return
                    case .failure(let failureStatus):
                        if failureStatus == 401 || failureStatus == 403 {
                            throw Self.authenticationError(failureStatus, url: request.url)
                        }
                        if Self.isMtimeUnsupportedStatus(failureStatus) {
                            continue
                        }
                        throw Self.statusError(failureStatus, method: "PROPPATCH", url: request.url)
                    case .unknown:
                        sawUnknownMultiStatus = true
                        continue
                    }
                }
                return
            }
            if status == 401 || status == 403 {
                throw Self.authenticationError(status, url: request.url)
            }
            if Self.isMtimeUnsupportedStatus(status) {
                continue
            }
            throw Self.statusError(status, method: "PROPPATCH", url: request.url)
        }
        if sawUnknownMultiStatus {
            throw Self.statusError(207, method: "PROPPATCH", url: targetURL)
        }
        // Best-effort metadata update: if all attempts are unsupported, keep backup success.
        return
    }

    func download(remotePath: String, localURL: URL) async throws {
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()
        try Task.checkCancellation()

        let targetURL = try remoteURL(forRemotePath: remotePath)
        let request = makeRequest(url: targetURL, method: "GET")
        let (temporaryURL, response) = try await sendDownload(request, onProgress: onProgress)
        // moveItem leaves no file behind at temporaryURL on success; on any earlier throw the leftover gets cleaned up here.
        defer { try? FileManager.default.removeItem(at: temporaryURL) }
        guard (200 ... 299).contains(response.statusCode) else {
            throw Self.statusError(response.statusCode, method: "GET", url: request.url)
        }

        let parentURL = localURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: parentURL, withIntermediateDirectories: true)
        if FileManager.default.fileExists(atPath: localURL.path) {
            try FileManager.default.removeItem(at: localURL)
        }
        try FileManager.default.moveItem(at: temporaryURL, to: localURL)
        onProgress?(1.0)
        try Task.checkCancellation()
    }

    func exists(path: String) async throws -> Bool {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()

        let targetURL = try remoteURL(forRemotePath: path)
        let headRequest = makeRequest(url: targetURL, method: "HEAD")
        let headResponse = try await sendStatus(headRequest)
        if (200 ... 299).contains(headResponse) {
            return true
        }
        if headResponse == 404 {
            return false
        }
        if [301, 302, 307, 308, 405, 501].contains(headResponse) {
            if try await metadata(path: path, requestCollectionURL: true) != nil {
                return true
            }
            return try await metadata(path: path, requestCollectionURL: false) != nil
        }
        throw Self.statusError(headResponse, method: "HEAD", url: headRequest.url)
    }

    func delete(path: String) async throws {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()

        let normalized = RemotePathBuilder.normalizePath(path)
        guard normalized != "/" else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        try await deleteWithoutPendingCleanup(path: normalized)
    }

    func createDirectory(path: String) async throws {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()

        let normalized = RemotePathBuilder.normalizePath(path)
        guard normalized != "/" else { return }

        var runningPath = ""
        let components = normalized.split(separator: "/")
        for component in components where !component.isEmpty {
            runningPath += "/\(component)"
            try await createDirectoryComponent(path: runningPath)
        }
    }

    private func createDirectoryComponent(path runningPath: String) async throws {
        var lockedRetryIndex = 0
        while true {
            let targetURL = try remoteCollectionURL(forRemotePath: runningPath)
            let request = makeRequest(url: targetURL, method: "MKCOL")
            let status = try await sendStatus(request)
            if (200 ... 299).contains(status) {
                return
            }
            if status == 405 {
                if try await confirmedDirectoryExists(path: runningPath, tolerateLocked: false) {
                    return
                }
            }
            if [301, 302, 307, 308].contains(status) {
                if try await exists(path: runningPath) { return }
            }
            if status == Self.lockedStatusCode {
                if try await confirmedDirectoryExists(path: runningPath, tolerateLocked: true) {
                    return
                }
                guard lockedRetryIndex < Self.createDirectoryLockedRetryJitterCapsNanos.count else {
                    throw Self.statusError(status, method: "MKCOL", url: request.url)
                }
                let cap = Self.createDirectoryLockedRetryJitterCapsNanos[lockedRetryIndex]
                lockedRetryIndex += 1
                try await Task.sleep(nanoseconds: UInt64.random(in: 0 ... cap))
                continue
            }
            throw Self.statusError(status, method: "MKCOL", url: request.url)
        }
    }

    private func confirmedDirectoryExists(path: String, tolerateLocked: Bool) async throws -> Bool {
        do {
            if let entry = try await metadata(path: path, requestCollectionURL: true), entry.isDirectory {
                return true
            }
        } catch {
            guard tolerateLocked, Self.containsWebDAVErrorCode(in: error, codes: [Self.lockedStatusCode]) else {
                throw error
            }
        }
        do {
            if let entry = try await metadata(path: path, requestCollectionURL: false), entry.isDirectory {
                return true
            }
        } catch {
            guard tolerateLocked, Self.containsWebDAVErrorCode(in: error, codes: [Self.lockedStatusCode]) else {
                throw error
            }
        }
        return false
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()

        let sourceURL = try remoteURL(forRemotePath: sourcePath)
        let destinationURL = try remoteURL(forRemotePath: destinationPath)
        let request = makeRequest(
            url: sourceURL,
            method: "MOVE",
            headers: [
                "Destination": destinationURL.absoluteString,
                "Overwrite": "T"
            ]
        )
        let status = try await sendStatus(request)
        guard (200 ... 299).contains(status) else {
            throw Self.statusError(status, method: "MOVE", url: request.url)
        }
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try requireConnected()
        await drainPendingCancelledUploadCleanup()

        let sourceURL = try remoteURL(forRemotePath: sourcePath)
        let destinationURL = try remoteURL(forRemotePath: destinationPath)
        let request = makeRequest(
            url: sourceURL,
            method: "COPY",
            headers: [
                "Destination": destinationURL.absoluteString,
                "Overwrite": "T"
            ]
        )
        let status = try await sendStatus(request)
        guard (200 ... 299).contains(status) else {
            throw Self.statusError(status, method: "COPY", url: request.url)
        }
    }

    private static let propfindBody = Data(
        """
        <?xml version="1.0" encoding="utf-8" ?>
        <d:propfind xmlns:d="DAV:">
          <d:prop>
            <d:resourcetype />
            <d:getcontentlength />
            <d:getlastmodified />
            <d:creationdate />
            <d:displayname />
          </d:prop>
        </d:propfind>
        """.utf8
    )

    private static let quotaPropfindBody = Data(
        """
        <?xml version="1.0" encoding="utf-8" ?>
        <d:propfind xmlns:d="DAV:">
          <d:prop>
            <d:quota-available-bytes />
            <d:quota-used-bytes />
          </d:prop>
        </d:propfind>
        """.utf8
    )

    private static func proppatchGetLastModifiedBody(date: Date) -> Data {
        formatterLock.lock()
        defer { formatterLock.unlock() }
        let value = rfc1123Formatter.string(from: date)
        return Data(
            """
            <?xml version="1.0" encoding="utf-8" ?>
            <d:propertyupdate xmlns:d="DAV:">
              <d:set>
                <d:prop>
                  <d:getlastmodified>\(value)</d:getlastmodified>
                </d:prop>
              </d:set>
            </d:propertyupdate>
            """.utf8
        )
    }

    private static func proppatchWin32LastModifiedBody(date: Date) -> Data {
        formatterLock.lock()
        defer { formatterLock.unlock() }
        let value = iso8601Formatter.string(from: date)
        return Data(
            """
            <?xml version="1.0" encoding="utf-8" ?>
            <d:propertyupdate xmlns:d="DAV:" xmlns:z="urn:schemas-microsoft-com:">
              <d:set>
                <d:prop>
                  <z:Win32LastModifiedTime>\(value)</z:Win32LastModifiedTime>
                </d:prop>
              </d:set>
            </d:propertyupdate>
            """.utf8
        )
    }

    private static func isMtimeUnsupportedStatus(_ status: Int) -> Bool {
        [400, 404, 405, 409, 415, 422, 423, 501].contains(status)
    }

    // Only HTTP 404 is read as genuine absence (matching the top-level `metadata`/`exists`/`delete` and
    // `RemoteFaultLite` 404-only not-found convention). Every other status — including 410 Gone and any
    // locked/forbidden/server error — is unresolved and must fail closed, never collapse to absence.
    private static func isNotFoundStatus(_ status: Int) -> Bool {
        status == 404
    }

    private func requireConnected() throws {
        guard isConnected else {
            throw RemoteStorageClientError.notConnected
        }
    }

    private func makeRequest(
        url: URL,
        method: String,
        headers: [String: String] = [:],
        body: Data? = nil
    ) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.httpBody = body
        if resolvedEndpointURL != nil, let host = config.endpointURL.host {
            request.setValue(config.endpointURL.port.map { "\(host):\($0)" } ?? host, forHTTPHeaderField: "Host")
        }
        request.setValue("*/*", forHTTPHeaderField: "Accept")
        request.setValue("Basic \(Self.basicAuthValue(username: config.username, password: config.password))", forHTTPHeaderField: "Authorization")
        for (key, value) in headers {
            request.setValue(value, forHTTPHeaderField: key)
        }
        return request
    }

    private func sendData(_ request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        do {
            let (data, response) = try await metadataTasks.data(for: request, in: session)
            guard let http = response as? HTTPURLResponse else {
                throw NSError(
                    domain: WebDAVClient.errorDomain,
                    code: -1100,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "webdav.error.unexpectedResponseType")]
                )
            }
            return (data, http)
        } catch {
            if Self.isCancellationError(error) {
                throw CancellationError()
            }
            throw RemoteStorageClientError.underlying(error)
        }
    }

    private func sendUpload(
        _ request: URLRequest,
        fromFile fileURL: URL,
        onProgress: ((Double) -> Void)?
    ) async throws -> (Data, HTTPURLResponse) {
        do {
            return try await URLSessionStallWatchdog.runUpload(
                session: transferSession, delegate: transferDelegate, registry: transferTasks, request: request,
                body: .file(fileURL), onProgress: onProgress, timeouts: Self.transferStallTimeouts,
                makeStallError: { Self.makeStallError($0, timeout: $1, bytes: $2, expected: $3) }
            )
        } catch {
            if Self.isCancellationError(error) {
                throw CancellationError()
            }
            throw RemoteStorageClientError.underlying(error)
        }
    }

    private func sendDownload(_ request: URLRequest, onProgress: ((Double) -> Void)? = nil) async throws -> (URL, HTTPURLResponse) {
        do {
            return try await URLSessionStallWatchdog.runDownload(
                session: transferSession, registry: transferTasks, request: request,
                onProgress: onProgress, timeouts: Self.transferStallTimeouts,
                makeStallError: { Self.makeStallError($0, timeout: $1, bytes: $2, expected: $3) }
            )
        } catch {
            if Self.isCancellationError(error) {
                throw CancellationError()
            }
            throw RemoteStorageClientError.underlying(error)
        }
    }

    private static func makeStallError(
        _ stall: URLSessionStallWatchdog.Stall,
        timeout: TimeInterval,
        bytes: Int64,
        expected: Int64?
    ) -> Error {
        let code: Int
        let formatKey: String.LocalizationValue
        let bytesKey: String
        let expectedKey: String
        switch stall {
        case .uploadBody:
            code = uploadStalledErrorCode; formatKey = "webdav.error.uploadStalled"
            bytesKey = uploadBytesSentKey; expectedKey = uploadExpectedBytesKey
        case .uploadResponse:
            code = uploadResponseTimeoutErrorCode; formatKey = "webdav.error.uploadResponseTimeout"
            bytesKey = uploadBytesSentKey; expectedKey = uploadExpectedBytesKey
        case .download:
            code = downloadStalledErrorCode; formatKey = "webdav.error.downloadStalled"
            bytesKey = downloadBytesWrittenKey; expectedKey = downloadExpectedBytesKey
        }
        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: String.localizedStringWithFormat(String(localized: formatKey), Int64(timeout.rounded())),
            bytesKey: bytes
        ]
        if let expected { userInfo[expectedKey] = expected }
        return NSError(domain: errorDomain, code: code, userInfo: userInfo)
    }

    private func sendStatus(_ request: URLRequest) async throws -> Int {
        let (_, response) = try await sendData(request)
        return response.statusCode
    }

    private enum MultiStatusOutcome {
        case success
        case failure(Int)
        case unknown
    }

    private func evaluateMultiStatus(_ data: Data, relativeTo baseURL: URL, targetPath: String) -> MultiStatusOutcome {
        guard let entries = try? PropfindXMLParser().parse(data) else {
            return .unknown
        }
        guard let targetKey = try? Self.canonicalRemotePath(targetPath) else {
            return .unknown
        }
        guard let targetEntry = entries.first(where: { entry in
            guard let entryPath = remotePath(fromHref: entry.href, relativeTo: baseURL) else { return false }
            return entryPath == targetKey
        }) else {
            return .unknown
        }
        if targetEntry.hasSuccessStatus {
            return .success
        }
        if let failureStatus = targetEntry.firstFailureStatusCode {
            return .failure(failureStatus)
        }
        return .unknown
    }

    private func requestURL(forRemotePath remotePath: String, requestCollectionURL: Bool) throws -> URL {
        if requestCollectionURL {
            return try remoteCollectionURL(forRemotePath: remotePath)
        }
        return try remoteURL(forRemotePath: remotePath)
    }

    private func remoteURL(forRemotePath remotePath: String) throws -> URL {
        let normalized = RemotePathBuilder.normalizePath(remotePath)
        let baseURL = Self.normalizedEndpointURL(activeEndpointURL)
        guard var urlComponents = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else {
            return baseURL
        }
        let basePath = endpointPathPrefix == "/" ? "" : endpointPathPrefix
        if normalized == "/" {
            urlComponents.percentEncodedPath = basePath.isEmpty ? "/" : basePath
            return urlComponents.url ?? baseURL
        }

        let components = try Self.validatedRemotePathComponents(for: normalized)

        let encodedRelative = components
            .map(Self.encodePathComponent)
            .joined(separator: "/")
        urlComponents.percentEncodedPath = basePath + "/" + encodedRelative
        return urlComponents.url ?? baseURL
    }

    private func remoteCollectionURL(forRemotePath remotePath: String) throws -> URL {
        Self.directoryURL(from: try remoteURL(forRemotePath: remotePath))
    }

    private func remotePath(fromHref href: String, relativeTo baseURL: URL) -> String? {
        let rawHref = href.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !rawHref.isEmpty else { return nil }
        let resolvedURL: URL?
        if let absolute = URL(string: rawHref), absolute.scheme != nil {
            resolvedURL = absolute.absoluteURL
        } else {
            resolvedURL = URL(string: rawHref, relativeTo: baseURL)?.absoluteURL
        }

        guard let resolvedURL else { return nil }
        let fullPath = Self.normalizedPercentEncodedPath(of: resolvedURL)
        let prefix = endpointPathPrefix

        if prefix == "/" {
            return Self.normalizePercentEncodedRemotePath(fullPath)
        }

        if fullPath == prefix || fullPath == prefix + "/" {
            return "/"
        }

        guard fullPath.hasPrefix(prefix + "/") else {
            return nil
        }

        let suffix = String(fullPath.dropFirst(prefix.count))
        return Self.normalizePercentEncodedRemotePath(suffix)
    }

    private func enqueueCancelledUploadCleanup(for remotePath: String) {
        let normalized = RemotePathBuilder.normalizePath(remotePath)
        guard normalized != "/" else { return }
        if !pendingCancelledUploadCleanupPaths.contains(normalized) {
            pendingCancelledUploadCleanupPaths.append(normalized)
        }
    }

    private func drainPendingCancelledUploadCleanup() async {
        guard !pendingCancelledUploadCleanupPaths.isEmpty else { return }
        let paths = pendingCancelledUploadCleanupPaths
        pendingCancelledUploadCleanupPaths.removeAll()
        for path in paths {
            try? await deleteWithoutPendingCleanup(path: path)
        }
    }

    private func deleteWithoutPendingCleanup(path: String) async throws {
        let targetURL = try remoteURL(forRemotePath: path)
        let request = makeRequest(url: targetURL, method: "DELETE")
        let status = try await sendStatus(request)
        if status == 404 {
            return
        }
        guard status == 207 || (200 ... 299).contains(status) else {
            throw Self.statusError(status, method: "DELETE", url: request.url)
        }
    }

    private static func basicAuthValue(username: String, password: String) -> String {
        let raw = "\(username):\(password)"
        return Data(raw.utf8).base64EncodedString()
    }

    private static func normalizedEndpointURL(_ url: URL) -> URL {
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url
        }
        components.query = nil
        components.fragment = nil
        components.user = nil
        components.password = nil

        if components.path.isEmpty {
            components.path = "/"
        } else if components.path.count > 1, components.path.hasSuffix("/") {
            components.path.removeLast()
        }
        return components.url ?? url
    }

    private static func normalizedPercentEncodedPath(of url: URL) -> String {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url.path.isEmpty ? "/" : url.path
        }
        let path = uppercasedPercentEscapes(in: components.percentEncodedPath)
        if path.isEmpty {
            return "/"
        }
        if path.count > 1, path.hasSuffix("/") {
            return String(path.dropLast())
        }
        return path
    }

    private static func directoryURL(from url: URL) -> URL {
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url
        }
        var path = components.percentEncodedPath
        if path.isEmpty {
            path = "/"
        } else if !path.hasSuffix("/") {
            path += "/"
        }
        components.percentEncodedPath = path
        return components.url ?? url
    }

    private static func normalizePercentEncodedRemotePath(_ path: String) -> String? {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        if trimmed.isEmpty {
            return "/"
        }

        let components = trimmed
            .split(separator: "/")
            .map(String.init)
            .filter { !$0.isEmpty }
        guard components.allSatisfy({ component in
            let decoded = component.removingPercentEncoding ?? component
            return decoded != "." && decoded != ".."
        }) else {
            return nil
        }
        return uppercasedPercentEscapes(in: "/" + components.joined(separator: "/"))
    }

    private static func canonicalRemotePath(_ path: String) throws -> String {
        let normalized = RemotePathBuilder.normalizePath(path)
        if normalized == "/" {
            return "/"
        }
        let encodedComponents = try validatedRemotePathComponents(for: normalized)
            .map(Self.encodePathComponent)
        return "/" + encodedComponents.joined(separator: "/")
    }

    private static func validatedRemotePathComponents(for normalizedPath: String) throws -> [String] {
        let relative = String(normalizedPath.dropFirst())
        let components = relative
            .split(separator: "/")
            .map(String.init)
            .filter { !$0.isEmpty }
        for component in components {
            let decoded = component.removingPercentEncoding ?? component
            if decoded == "." || decoded == ".." {
                throw RemoteStorageClientError.invalidConfiguration
            }
        }
        return components
    }

    private static func encodePathComponent(_ component: String) -> String {
        let allowedCharacters = CharacterSet.urlPathAllowed.subtracting(CharacterSet(charactersIn: "/"))
        let encoded = component.addingPercentEncoding(withAllowedCharacters: allowedCharacters) ?? component
        return uppercasedPercentEscapes(in: encoded)
    }

    private static func uppercasedPercentEscapes(in value: String) -> String {
        let nsRange = NSRange(value.startIndex..<value.endIndex, in: value)
        var result = value
        for match in percentEscapeRegex.matches(in: value, range: nsRange).reversed() {
            guard let range = Range(match.range, in: result) else { continue }
            result.replaceSubrange(range, with: result[range].uppercased())
        }
        return result
    }

    private static func entryName(forRemotePath remotePath: String, displayName: String?, href: String) -> String {
        let encodedName = (remotePath as NSString).lastPathComponent
        let decodedName = encodedName.removingPercentEncoding ?? encodedName
        if !decodedName.isEmpty {
            return decodedName
        }
        if let displayName, !displayName.isEmpty {
            return displayName
        }
        let decodedHref = href.removingPercentEncoding ?? href
        return decodedHref.isEmpty ? href : decodedHref
    }

    private static func decodedEntryPath(fromRemotePath remotePath: String) -> String {
        remotePath.removingPercentEncoding ?? remotePath
    }

    private static func parseDate(_ value: String) -> Date? {
        if value.isEmpty { return nil }
        formatterLock.lock()
        defer { formatterLock.unlock() }
        if let date = rfc1123Formatter.date(from: value) {
            return date
        }
        if let date = iso8601Formatter.date(from: value) {
            return date
        }
        return nil
    }

    private static func parseHTTPStatusCode(_ statusLine: String) -> Int? {
        let parts = statusLine.split(separator: " ")
        guard parts.count >= 2 else { return nil }
        return Int(parts[1])
    }

    private static func isCancellationError(_ error: Error) -> Bool {
        if error is CancellationError {
            return true
        }
        if let storageError = error as? RemoteStorageClientError {
            switch storageError {
            case .underlying(let underlying):
                return isCancellationError(underlying)
            default:
                return false
            }
        }

        let nsError = error as NSError
        if nsError.domain == NSURLErrorDomain, nsError.code == NSURLErrorCancelled {
            return true
        }
        if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
            return isCancellationError(underlying)
        }
        return false
    }

    static func isUploadWatchdogTimeout(_ error: Error) -> Bool {
        containsWebDAVErrorCode(
            in: error,
            codes: [uploadStalledErrorCode, uploadResponseTimeoutErrorCode]
        )
    }

    // A watchdog-detected stalled/timed-out transfer (upload or download) — a dead/half-open socket, which a
    // fresh reconnect recovers; RemoteFaultLite treats it as retryable so the worker reconnects rather than
    // failing the asset.
    static func isStalledTransferTimeout(_ error: Error) -> Bool {
        containsWebDAVErrorCode(
            in: error,
            codes: [uploadStalledErrorCode, uploadResponseTimeoutErrorCode, downloadStalledErrorCode]
        )
    }

    static func shouldCleanupPartialUpload(_ error: Error) -> Bool {
        // Only a mid-body STALL proves the body was not fully sent (a genuine partial worth deleting). A
        // response-timeout fires after the body was fully sent, and a bare cancellation can arrive after the body
        // is sent (even after a 2xx) — both may leave a COMPLETE object, so deleting it would wrongly remove a
        // valid landed object (most critically a direct-PUT canonical). A cancelled partial self-heals instead: a
        // canonical is repaired from its recovery scratch, a data file is re-uploaded (not recorded as backed up).
        containsWebDAVErrorCode(in: error, codes: [uploadStalledErrorCode])
    }

    private static func containsWebDAVErrorCode(in error: Error, codes: Set<Int>) -> Bool {
        if let storageError = error as? RemoteStorageClientError {
            switch storageError {
            case .underlying(let underlying):
                return containsWebDAVErrorCode(in: underlying, codes: codes)
            default:
                return false
            }
        }

        let nsError = error as NSError
        if nsError.domain == errorDomain, codes.contains(nsError.code) {
            return true
        }
        if let underlying = nsError.userInfo[NSUnderlyingErrorKey] as? Error {
            return containsWebDAVErrorCode(in: underlying, codes: codes)
        }
        return false
    }

    private static func statusError(_ statusCode: Int, method: String, url: URL?) -> RemoteStorageClientError {
        if statusCode == 401 || statusCode == 403 {
            return authenticationError(statusCode, url: url)
        }
        let target = url?.absoluteString ?? "(unknown URL)"
        return .underlying(
            NSError(
                domain: WebDAVClient.errorDomain,
                code: statusCode,
                userInfo: [
                    NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                        String(localized: "webdav.error.requestFailed"),
                        method,
                        statusCode,
                        target
                    )
                ]
            )
        )
    }

    private static func authenticationError(_ statusCode: Int, url: URL?) -> RemoteStorageClientError {
        let target = url?.absoluteString ?? "(unknown URL)"
        return .underlying(
            NSError(
                domain: WebDAVClient.errorDomain,
                code: statusCode,
                userInfo: [
                    NSLocalizedDescriptionKey: statusCode == 401
                        ? String.localizedStringWithFormat(
                            String(localized: "webdav.error.authenticationFailed"),
                            target
                        )
                        : String.localizedStringWithFormat(
                            String(localized: "webdav.error.accessDenied"),
                            target
                        )
                ]
            )
        )
    }
}

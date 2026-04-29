import Foundation

final actor WebDAVClient: RemoteStorageClientProtocol {
    static let errorDomain = "WebDAVClient"

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
    }

    nonisolated func shouldSetModificationDate() -> Bool {
        true
    }

    nonisolated func shouldLimitUploadRetries(for error: Error) -> Bool {
        Self.isUploadWatchdogTimeout(error)
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
    private static let uploadStalledErrorCode = -1301
    private static let uploadResponseTimeoutErrorCode = -1302
    private static let downloadStalledErrorCode = -1303
    private static let uploadBytesSentKey = "WebDAVUploadBytesSent"
    private static let uploadExpectedBytesKey = "WebDAVUploadExpectedBytes"
    private static let downloadBytesWrittenKey = "WebDAVDownloadBytesWritten"
    private static let downloadExpectedBytesKey = "WebDAVDownloadExpectedBytes"
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
    private let transferDelegate = TransferDelegate()
    private let endpointPathPrefix: String
    private var isConnected = false
    private var pendingCancelledUploadCleanupPaths: [String] = []

    private final class TransferDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
        private let lock = NSLock()
        private var uploadStates: [Int: UploadProgressState] = [:]

        func register(_ state: UploadProgressState, for task: URLSessionTask) {
            lock.withLock {
                uploadStates[task.taskIdentifier] = state
            }
        }

        func unregister(_ task: URLSessionTask) {
            lock.withLock {
                uploadStates[task.taskIdentifier] = nil
            }
        }

        func urlSession(
            _: URLSession,
            task: URLSessionTask,
            didSendBodyData _: Int64,
            totalBytesSent: Int64,
            totalBytesExpectedToSend: Int64
        ) {
            let state = lock.withLock {
                uploadStates[task.taskIdentifier]
            }
            state?.recordProgress(
                bytesSent: totalBytesSent,
                totalBytesExpectedToSend: totalBytesExpectedToSend
            )
        }
    }

    private final class UploadProgressState: @unchecked Sendable {
        enum Phase {
            case sendingBody
            case awaitingResponse
        }

        struct Snapshot {
            let phase: Phase
            let bytesSent: Int64
            let expectedBytes: Int64?
            let lastProgressAtNanos: UInt64
            let isFinished: Bool
        }

        private let lock = NSLock()
        private let onProgress: ((Double) -> Void)?
        private var phase: Phase = .sendingBody
        private var bytesSent: Int64 = 0
        private var expectedBytes: Int64?
        private var lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
        private var timeoutError: Error?
        private var isFinished = false

        init(onProgress: ((Double) -> Void)?) {
            self.onProgress = onProgress
        }

        func resetProgressClock() {
            lock.withLock {
                guard !isFinished else { return }
                lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
            }
        }

        func recordProgress(bytesSent incomingBytesSent: Int64, totalBytesExpectedToSend: Int64) {
            let progressToEmit: Double? = lock.withLock {
                guard !isFinished else { return nil }

                let now = DispatchTime.now().uptimeNanoseconds
                if incomingBytesSent > bytesSent {
                    bytesSent = incomingBytesSent
                    lastProgressAtNanos = now
                }
                if totalBytesExpectedToSend > 0 {
                    expectedBytes = totalBytesExpectedToSend
                }

                if let expectedBytes,
                   expectedBytes > 0,
                   incomingBytesSent >= expectedBytes,
                   phase == .sendingBody {
                    phase = .awaitingResponse
                    lastProgressAtNanos = now
                }

                guard let expectedBytes, expectedBytes > 0 else {
                    return nil
                }
                return min(max(Double(incomingBytesSent) / Double(expectedBytes), 0), 1)
            }

            if let progressToEmit {
                onProgress?(progressToEmit)
            }
        }

        func snapshot() -> Snapshot {
            lock.withLock {
                Snapshot(
                    phase: phase,
                    bytesSent: bytesSent,
                    expectedBytes: expectedBytes,
                    lastProgressAtNanos: lastProgressAtNanos,
                    isFinished: isFinished
                )
            }
        }

        func markTimedOut(_ error: Error) -> Bool {
            lock.withLock {
                guard !isFinished, timeoutError == nil else { return false }
                timeoutError = error
                return true
            }
        }

        func finish() {
            lock.withLock {
                isFinished = true
            }
        }

        func resolvedTimeoutError() -> Error? {
            lock.withLock {
                timeoutError
            }
        }
    }

    private final class DownloadProgressState: @unchecked Sendable {
        enum Phase {
            case awaitingFirstByte
            case receivingBody
        }

        struct Snapshot {
            let phase: Phase
            let bytesWritten: Int64
            let expectedBytes: Int64?
            let lastProgressAtNanos: UInt64
            let isFinished: Bool
        }

        private let lock = NSLock()
        private var phase: Phase = .awaitingFirstByte
        private var bytesWritten: Int64 = 0
        private var expectedBytes: Int64?
        private var lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
        private var timeoutError: Error?
        private var isFinished = false

        func resetProgressClock() {
            lock.withLock {
                guard !isFinished else { return }
                lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
            }
        }

        func recordProgress(bytesWritten incomingBytesWritten: Int64, totalBytesExpectedToWrite: Int64) {
            lock.withLock {
                guard !isFinished else { return }
                if totalBytesExpectedToWrite > 0 {
                    expectedBytes = totalBytesExpectedToWrite
                }
                if incomingBytesWritten > bytesWritten {
                    bytesWritten = incomingBytesWritten
                    phase = .receivingBody
                    lastProgressAtNanos = DispatchTime.now().uptimeNanoseconds
                }
            }
        }

        func snapshot() -> Snapshot {
            lock.withLock {
                Snapshot(
                    phase: phase,
                    bytesWritten: bytesWritten,
                    expectedBytes: expectedBytes,
                    lastProgressAtNanos: lastProgressAtNanos,
                    isFinished: isFinished
                )
            }
        }

        func markTimedOut(_ error: Error) -> Bool {
            lock.withLock {
                guard !isFinished, timeoutError == nil else { return false }
                timeoutError = error
                return true
            }
        }

        func finish() {
            lock.withLock {
                isFinished = true
            }
        }

        func resolvedTimeoutError() -> Error? {
            lock.withLock {
                timeoutError
            }
        }
    }

    private final class URLSessionTaskBox: @unchecked Sendable {
        private let lock = NSLock()
        private var task: URLSessionTask?

        func set(_ task: URLSessionTask) {
            lock.withLock {
                self.task = task
            }
        }

        func cancel() {
            let task = lock.withLock {
                self.task
            }
            task?.cancel()
        }

        func value() -> URLSessionTask? {
            lock.withLock {
                task
            }
        }
    }

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

    func connect() async throws {
        if isConnected { return }
        let request = makeRequest(
            url: Self.directoryURL(from: Self.normalizedEndpointURL(config.endpointURL)),
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
                throw Self.authenticationError(status, url: request.url)
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
            throw Self.statusError(status, method: "PROPFIND", url: request.url)
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
        isConnected = true
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
            if parsed.hasAnyStatus, !parsed.hasSuccessStatus {
                continue
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
            if parsed.hasAnyStatus, !parsed.hasSuccessStatus {
                continue
            }
            guard let remotePath = remotePath(fromHref: parsed.href, relativeTo: baseURL),
                  remotePath == normalizedTargetKey else {
                continue
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
        try requireConnected()
        await drainPendingCancelledUploadCleanup()
        if respectTaskCancellation {
            try Task.checkCancellation()
        }

        let targetURL = try remoteURL(forRemotePath: remotePath)
        var request = makeRequest(url: targetURL, method: "PUT")
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        do {
            let (_, response) = try await sendUpload(request, fromFile: localURL, onProgress: onProgress)
            guard (200 ... 299).contains(response.statusCode) else {
                throw Self.statusError(response.statusCode, method: "PUT", url: request.url)
            }
            if respectTaskCancellation {
                try Task.checkCancellation()
            }
            onProgress?(1)
        } catch {
            if Self.shouldCleanupPartialUpload(error) {
                enqueueCancelledUploadCleanup(for: remotePath)
            }
            if Self.isCancellationError(error) {
                throw CancellationError()
            }
            throw error
        }
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
        try requireConnected()
        await drainPendingCancelledUploadCleanup()
        try Task.checkCancellation()

        let targetURL = try remoteURL(forRemotePath: remotePath)
        let request = makeRequest(url: targetURL, method: "GET")
        let (temporaryURL, response) = try await sendDownload(request)
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
            let targetURL = try remoteCollectionURL(forRemotePath: runningPath)
            let request = makeRequest(url: targetURL, method: "MKCOL")
            let status = try await sendStatus(request)
            if (200 ... 299).contains(status) {
                continue
            }
            if status == 405 {
                if let entry = try await metadata(path: runningPath, requestCollectionURL: true), entry.isDirectory {
                    continue
                }
                if let entry = try await metadata(path: runningPath, requestCollectionURL: false), entry.isDirectory {
                    continue
                }
            }
            if [301, 302, 307, 308].contains(status) {
                if try await exists(path: runningPath) { continue }
            }
            throw Self.statusError(status, method: "MKCOL", url: request.url)
        }
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
        request.setValue("*/*", forHTTPHeaderField: "Accept")
        request.setValue("Basic \(Self.basicAuthValue(username: config.username, password: config.password))", forHTTPHeaderField: "Authorization")
        for (key, value) in headers {
            request.setValue(value, forHTTPHeaderField: key)
        }
        return request
    }

    private func sendData(_ request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        do {
            let (data, response) = try await session.data(for: request)
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
        let transferSession = self.transferSession
        let transferDelegate = self.transferDelegate
        let progressState = UploadProgressState(onProgress: onProgress)
        let taskBox = URLSessionTaskBox()
        let watchdogTask = Task {
            await Self.watchUploadProgress(progressState: progressState, taskBox: taskBox)
        }
        defer {
            watchdogTask.cancel()
            if let task = taskBox.value() {
                transferDelegate.unregister(task)
            }
        }

        do {
            return try await withTaskCancellationHandler(operation: {
                try await withCheckedThrowingContinuation { continuation in
                    let task = transferSession.uploadTask(with: request, fromFile: fileURL) { data, response, error in
                        progressState.finish()
                        if let task = taskBox.value() {
                            transferDelegate.unregister(task)
                        }

                        if let error {
                            continuation.resume(throwing: progressState.resolvedTimeoutError() ?? error)
                            return
                        }

                        guard let http = response as? HTTPURLResponse else {
                            continuation.resume(throwing: NSError(
                                domain: WebDAVClient.errorDomain,
                                code: -1101,
                                userInfo: [NSLocalizedDescriptionKey: String(localized: "webdav.error.unexpectedUploadResponseType")]
                            ))
                            return
                        }

                        continuation.resume(returning: (data ?? Data(), http))
                    }
                    taskBox.set(task)
                    transferDelegate.register(progressState, for: task)
                    task.resume()
                    progressState.resetProgressClock()
                    if Task.isCancelled {
                        task.cancel()
                    }
                }
            }, onCancel: {
                taskBox.cancel()
            })
        } catch {
            if Self.isCancellationError(error) {
                throw CancellationError()
            }
            throw RemoteStorageClientError.underlying(error)
        }
    }

    private static func watchUploadProgress(
        progressState: UploadProgressState,
        taskBox: URLSessionTaskBox
    ) async {
        while !Task.isCancelled {
            do {
                try await Task.sleep(nanoseconds: nanoseconds(for: uploadWatchdogInterval))
            } catch {
                return
            }

            let snapshot = progressState.snapshot()
            if snapshot.isFinished {
                return
            }

            let timeout: TimeInterval
            switch snapshot.phase {
            case .sendingBody:
                timeout = uploadStallTimeout
            case .awaitingResponse:
                timeout = uploadResponseTimeout
            }

            guard elapsedSeconds(since: snapshot.lastProgressAtNanos) >= timeout else {
                continue
            }

            let error = uploadTimeoutError(
                phase: snapshot.phase,
                timeout: timeout,
                snapshot: snapshot
            )
            if progressState.markTimedOut(error) {
                taskBox.cancel()
            }
            return
        }
    }

    private static func watchDownloadProgress(
        progressState: DownloadProgressState,
        taskBox: URLSessionTaskBox
    ) async {
        while !Task.isCancelled {
            do {
                try await Task.sleep(nanoseconds: nanoseconds(for: uploadWatchdogInterval))
            } catch {
                return
            }

            // Completion-handler downloadTasks suppress URLSessionDownloadDelegate callbacks,
            // so we feed the watchdog from URLSession's internal byte counters instead.
            if let task = taskBox.value() {
                progressState.recordProgress(
                    bytesWritten: task.countOfBytesReceived,
                    totalBytesExpectedToWrite: task.countOfBytesExpectedToReceive
                )
            }

            let snapshot = progressState.snapshot()
            if snapshot.isFinished {
                return
            }

            let timeout: TimeInterval
            switch snapshot.phase {
            case .awaitingFirstByte:
                timeout = downloadInitialResponseTimeout
            case .receivingBody:
                timeout = downloadStallTimeout
            }

            guard elapsedSeconds(since: snapshot.lastProgressAtNanos) >= timeout else {
                continue
            }

            let error = downloadTimeoutError(timeout: timeout, snapshot: snapshot)
            if progressState.markTimedOut(error) {
                taskBox.cancel()
            }
            return
        }
    }

    private static func uploadTimeoutError(
        phase: UploadProgressState.Phase,
        timeout: TimeInterval,
        snapshot: UploadProgressState.Snapshot
    ) -> NSError {
        let formatKey: String.LocalizationValue
        let code: Int
        switch phase {
        case .sendingBody:
            formatKey = "webdav.error.uploadStalled"
            code = uploadStalledErrorCode
        case .awaitingResponse:
            formatKey = "webdav.error.uploadResponseTimeout"
            code = uploadResponseTimeoutErrorCode
        }

        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                String(localized: formatKey),
                Int64(timeout.rounded())
            ),
            uploadBytesSentKey: snapshot.bytesSent
        ]
        if let expectedBytes = snapshot.expectedBytes {
            userInfo[uploadExpectedBytesKey] = expectedBytes
        }

        return NSError(domain: errorDomain, code: code, userInfo: userInfo)
    }

    private static func downloadTimeoutError(
        timeout: TimeInterval,
        snapshot: DownloadProgressState.Snapshot
    ) -> NSError {
        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: String.localizedStringWithFormat(
                String(localized: "webdav.error.downloadStalled"),
                Int64(timeout.rounded())
            ),
            downloadBytesWrittenKey: snapshot.bytesWritten
        ]
        if let expectedBytes = snapshot.expectedBytes {
            userInfo[downloadExpectedBytesKey] = expectedBytes
        }

        return NSError(domain: errorDomain, code: downloadStalledErrorCode, userInfo: userInfo)
    }

    private static func nanoseconds(for interval: TimeInterval) -> UInt64 {
        UInt64(max(interval, 0) * 1_000_000_000)
    }

    private static func elapsedSeconds(since startNanos: UInt64) -> TimeInterval {
        let now = DispatchTime.now().uptimeNanoseconds
        guard now >= startNanos else { return 0 }
        return TimeInterval(now - startNanos) / 1_000_000_000
    }

    private func sendDownload(_ request: URLRequest) async throws -> (URL, HTTPURLResponse) {
        let transferSession = self.transferSession
        let progressState = DownloadProgressState()
        let taskBox = URLSessionTaskBox()
        let watchdogTask = Task {
            await Self.watchDownloadProgress(progressState: progressState, taskBox: taskBox)
        }
        defer {
            watchdogTask.cancel()
        }

        do {
            return try await withTaskCancellationHandler(operation: {
                try await withCheckedThrowingContinuation { continuation in
                    let task = transferSession.downloadTask(with: request) { temporaryURL, response, error in
                        progressState.finish()

                        if let error {
                            continuation.resume(throwing: progressState.resolvedTimeoutError() ?? error)
                            return
                        }

                        guard let temporaryURL, let http = response as? HTTPURLResponse else {
                            continuation.resume(throwing: NSError(
                                domain: WebDAVClient.errorDomain,
                                code: -1102,
                                userInfo: [NSLocalizedDescriptionKey: String(localized: "webdav.error.unexpectedDownloadResponseType")]
                            ))
                            return
                        }

                        do {
                            let stableTemporaryURL = FileManager.default.temporaryDirectory
                                .appendingPathComponent("Watermelon-WebDAV-\(UUID().uuidString)", isDirectory: false)
                            try FileManager.default.moveItem(at: temporaryURL, to: stableTemporaryURL)
                            continuation.resume(returning: (stableTemporaryURL, http))
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }
                    taskBox.set(task)
                    task.resume()
                    progressState.resetProgressClock()
                    if Task.isCancelled {
                        task.cancel()
                    }
                }
            }, onCancel: {
                taskBox.cancel()
            })
        } catch {
            if Self.isCancellationError(error) {
                throw CancellationError()
            }
            throw RemoteStorageClientError.underlying(error)
        }
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
        let baseURL = Self.normalizedEndpointURL(config.endpointURL)
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

    private static func shouldCleanupPartialUpload(_ error: Error) -> Bool {
        isCancellationError(error) || isUploadWatchdogTimeout(error)
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

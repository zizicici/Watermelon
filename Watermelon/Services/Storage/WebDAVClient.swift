import Foundation

final actor WebDAVClient: RemoteStorageClientProtocol {
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
                    domain: "WebDAVClient",
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
    private let endpointPathPrefix: String
    private var isConnected = false
    private var pendingCancelledUploadCleanupPaths: [String] = []

    init(config: Config) {
        self.config = config
        let normalizedEndpoint = Self.normalizedEndpointURL(config.endpointURL)
        let normalizedPath = Self.normalizedPercentEncodedPath(of: normalizedEndpoint)
        let trimmed = normalizedPath == "/" ? "" : normalizedPath.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        endpointPathPrefix = trimmed.isEmpty ? "/" : "/" + trimmed

        let sessionConfig = URLSessionConfiguration.ephemeral
        sessionConfig.timeoutIntervalForRequest = 45
        sessionConfig.timeoutIntervalForResource = 120
        session = URLSession(configuration: sessionConfig)
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
                        domain: "WebDAVClient",
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
                        domain: "WebDAVClient",
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
            let (_, response) = try await sendUpload(request, fromFile: localURL)
            guard (200 ... 299).contains(response.statusCode) else {
                throw Self.statusError(response.statusCode, method: "PUT", url: request.url)
            }
            if respectTaskCancellation {
                try Task.checkCancellation()
            }
            onProgress?(1)
        } catch {
            if Self.isCancellationError(error) {
                enqueueCancelledUploadCleanup(for: remotePath)
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
                    domain: "WebDAVClient",
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

    private func sendUpload(_ request: URLRequest, fromFile fileURL: URL) async throws -> (Data, HTTPURLResponse) {
        do {
            let (data, response) = try await session.upload(for: request, fromFile: fileURL)
            guard let http = response as? HTTPURLResponse else {
                throw NSError(
                    domain: "WebDAVClient",
                    code: -1101,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "webdav.error.unexpectedUploadResponseType")]
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

    private func sendDownload(_ request: URLRequest) async throws -> (URL, HTTPURLResponse) {
        do {
            let (fileURL, response) = try await session.download(for: request)
            guard let http = response as? HTTPURLResponse else {
                throw NSError(
                    domain: "WebDAVClient",
                    code: -1102,
                    userInfo: [NSLocalizedDescriptionKey: String(localized: "webdav.error.unexpectedDownloadResponseType")]
                )
            }
            return (fileURL, http)
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

    private static func statusError(_ statusCode: Int, method: String, url: URL?) -> RemoteStorageClientError {
        if statusCode == 401 || statusCode == 403 {
            return authenticationError(statusCode, url: url)
        }
        let target = url?.absoluteString ?? "(unknown URL)"
        return .underlying(
            NSError(
                domain: "WebDAVClient",
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
                domain: "WebDAVClient",
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

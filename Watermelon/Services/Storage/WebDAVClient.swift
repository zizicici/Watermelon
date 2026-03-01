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
        var isDirectory = false
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
                    userInfo: [NSLocalizedDescriptionKey: "Failed to parse WebDAV PROPFIND response."]
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
            let source = (qName?.isEmpty == false) ? qName! : elementName
            if let idx = source.lastIndex(of: ":") {
                return String(source[source.index(after: idx)...]).lowercased()
            }
            return source.lowercased()
        }
    }

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

    init(config: Config) {
        self.config = config
        let normalizedEndpoint = Self.normalizedEndpointURL(config.endpointURL)
        let normalizedPath = normalizedEndpoint.path
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
            url: Self.normalizedEndpointURL(config.endpointURL),
            method: "PROPFIND",
            headers: [
                "Depth": "0",
                "Content-Type": "application/xml; charset=utf-8"
            ],
            body: Self.propfindBody
        )
        let (_, response) = try await sendData(request)
        let status = response.statusCode
        guard status == 207 || (200 ... 299).contains(status) else {
            if status == 401 || status == 403 {
                throw Self.authenticationError(status, url: request.url)
            }
            throw Self.statusError(status, method: "PROPFIND", url: request.url)
        }
        isConnected = true
    }

    func disconnect() async {
        isConnected = false
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        try requireConnected()
        let targetURL = try remoteURL(forRemotePath: "/")
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
        let targetEntry = parsedEntries.first { entry in
            guard !entry.hasAnyStatus || entry.hasSuccessStatus else { return false }
            guard let path = remotePath(fromHref: entry.href) else { return false }
            return path == "/"
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
        let normalizedTarget = RemotePathBuilder.normalizePath(path)
        let targetURL = try remoteURL(forRemotePath: normalizedTarget)
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
        var entries: [RemoteStorageEntry] = []
        entries.reserveCapacity(parsedEntries.count)

        for parsed in parsedEntries {
            if parsed.hasAnyStatus, !parsed.hasSuccessStatus {
                continue
            }
            guard let remotePath = remotePath(fromHref: parsed.href) else { continue }
            if remotePath == normalizedTarget { continue }
            let name = parsed.displayName ?? (remotePath as NSString).lastPathComponent
            entries.append(
                RemoteStorageEntry(
                    path: remotePath,
                    name: name.isEmpty ? parsed.href : name,
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
        try requireConnected()
        let normalizedTarget = RemotePathBuilder.normalizePath(path)
        let targetURL = try remoteURL(forRemotePath: normalizedTarget)
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
        for parsed in parsedEntries {
            if parsed.hasAnyStatus, !parsed.hasSuccessStatus {
                continue
            }
            guard let remotePath = remotePath(fromHref: parsed.href),
                  remotePath == normalizedTarget else {
                continue
            }
            let name = parsed.displayName ?? (remotePath as NSString).lastPathComponent
            return RemoteStorageEntry(
                path: remotePath,
                name: name.isEmpty ? parsed.href : name,
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
        if respectTaskCancellation {
            try Task.checkCancellation()
        }

        let targetURL = try remoteURL(forRemotePath: remotePath)
        var request = makeRequest(url: targetURL, method: "PUT")
        request.setValue("application/octet-stream", forHTTPHeaderField: "Content-Type")
        let (_, response) = try await sendUpload(request, fromFile: localURL)
        guard (200 ... 299).contains(response.statusCode) else {
            throw Self.statusError(response.statusCode, method: "PUT", url: request.url)
        }

        if respectTaskCancellation {
            try Task.checkCancellation()
        }
        onProgress?(1)
    }

    func setModificationDate(_: Date, forPath _: String) async throws {
        // WebDAV servers vary widely in writable mtime support; backup pipeline treats this as best-effort.
    }

    func download(remotePath: String, localURL: URL) async throws {
        try requireConnected()
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

        let targetURL = try remoteURL(forRemotePath: path)
        let headRequest = makeRequest(url: targetURL, method: "HEAD")
        let headResponse = try await sendStatus(headRequest)
        if (200 ... 299).contains(headResponse) {
            return true
        }
        if headResponse == 404 {
            return false
        }
        if headResponse != 405 {
            throw Self.statusError(headResponse, method: "HEAD", url: headRequest.url)
        }
        return try await metadata(path: path) != nil
    }

    func delete(path: String) async throws {
        try requireConnected()

        let normalized = RemotePathBuilder.normalizePath(path)
        guard normalized != "/" else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        let targetURL = try remoteURL(forRemotePath: normalized)
        let request = makeRequest(url: targetURL, method: "DELETE")
        let status = try await sendStatus(request)
        if status == 404 {
            return
        }
        guard status == 207 || (200 ... 299).contains(status) else {
            throw Self.statusError(status, method: "DELETE", url: request.url)
        }
    }

    func createDirectory(path: String) async throws {
        try requireConnected()

        let normalized = RemotePathBuilder.normalizePath(path)
        guard normalized != "/" else { return }

        var runningPath = ""
        let components = normalized.split(separator: "/")
        for component in components where !component.isEmpty {
            runningPath += "/\(component)"
            let targetURL = try remoteURL(forRemotePath: runningPath)
            let request = makeRequest(url: targetURL, method: "MKCOL")
            let status = try await sendStatus(request)
            if (200 ... 299).contains(status) || status == 405 {
                continue
            }
            if status == 301 || status == 302 {
                if try await exists(path: runningPath) { continue }
            }
            throw Self.statusError(status, method: "MKCOL", url: request.url)
        }
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        try requireConnected()

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
        guard status == 201 || status == 204 else {
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
                    userInfo: [NSLocalizedDescriptionKey: "Unexpected WebDAV response type."]
                )
            }
            return (data, http)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
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
                    userInfo: [NSLocalizedDescriptionKey: "Unexpected WebDAV upload response type."]
                )
            }
            return (data, http)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
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
                    userInfo: [NSLocalizedDescriptionKey: "Unexpected WebDAV download response type."]
                )
            }
            return (fileURL, http)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            throw RemoteStorageClientError.underlying(error)
        }
    }

    private func sendStatus(_ request: URLRequest) async throws -> Int {
        let (_, response) = try await sendData(request)
        return response.statusCode
    }

    private func remoteURL(forRemotePath remotePath: String) throws -> URL {
        let normalized = RemotePathBuilder.normalizePath(remotePath)
        if normalized == "/" {
            return Self.normalizedEndpointURL(config.endpointURL)
        }

        let relative = String(normalized.dropFirst())
        let components = relative.split(separator: "/")
        guard !components.contains(where: { $0 == ".." }) else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        var url = Self.normalizedEndpointURL(config.endpointURL)
        for component in components where !component.isEmpty {
            url.appendPathComponent(String(component), isDirectory: false)
        }
        return url
    }

    private func remotePath(fromHref href: String) -> String? {
        let decodedHref = href.removingPercentEncoding ?? href
        let resolvedURL: URL?
        if let absolute = URL(string: decodedHref), absolute.scheme != nil {
            resolvedURL = absolute
        } else {
            resolvedURL = URL(string: decodedHref, relativeTo: Self.normalizedEndpointURL(config.endpointURL))?.absoluteURL
        }

        guard let resolvedURL else { return nil }
        let fullPath = resolvedURL.path
        let prefix = endpointPathPrefix

        if prefix == "/" {
            return RemotePathBuilder.normalizePath(fullPath)
        }

        if fullPath == prefix || fullPath == prefix + "/" {
            return "/"
        }

        guard fullPath.hasPrefix(prefix + "/") else {
            return nil
        }

        let suffix = String(fullPath.dropFirst(prefix.count))
        return RemotePathBuilder.normalizePath(suffix)
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

    private static func parseDate(_ value: String) -> Date? {
        if value.isEmpty { return nil }
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

    private static func statusError(_ statusCode: Int, method: String, url: URL?) -> RemoteStorageClientError {
        let target = url?.absoluteString ?? "(unknown URL)"
        return .underlying(
            NSError(
                domain: "WebDAVClient",
                code: statusCode,
                userInfo: [
                    NSLocalizedDescriptionKey: "WebDAV \(method) failed with status \(statusCode): \(target)"
                ]
            )
        )
    }

    private static func authenticationError(_ statusCode: Int, url: URL?) -> RemoteStorageClientError {
        let target = url?.absoluteString ?? "(unknown URL)"
        let message = statusCode == 401
            ? "WebDAV authentication failed. Check username and password, then try again."
            : "WebDAV access was denied (403). Check account permissions and endpoint path, then try again."
        return .underlying(
            NSError(
                domain: "WebDAVClient",
                code: statusCode,
                userInfo: [
                    NSLocalizedDescriptionKey: "\(message) Endpoint: \(target)"
                ]
            )
        )
    }
}

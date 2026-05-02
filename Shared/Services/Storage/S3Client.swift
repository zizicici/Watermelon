import Foundation

final actor S3Client: RemoteStorageClientProtocol {
    static let errorDomain = S3ErrorClassifier.errorDomain

    struct Config: Sendable {
        let endpointHost: String
        let endpointPort: Int
        let scheme: String
        let region: String
        let bucket: String
        let usePathStyle: Bool
        let accessKeyID: String
        let secretAccessKey: String
        let sessionToken: String?
    }

    private static let metadataRequestTimeout: TimeInterval = 45
    private static let metadataResourceTimeout: TimeInterval = 120
    private static let transferTimeout: TimeInterval = 7 * 24 * 60 * 60
    private static let listPageSize = 1000
    // AWS PutObject and CopyObject hard cap.
    private static let singlePartMaxSize: Int64 = 5 * 1024 * 1024 * 1024
    // AWS requires parts >= 5 MiB (except the final part).
    static let multipartPartSize: Int64 = 8 * 1024 * 1024
    static let multipartThreshold: Int64 = multipartPartSize
    private static let multipartConcurrency = 4
    private static let multipartMaxParts = 10_000
    // Buffer below the 10000-part hard ceiling.
    private static let multipartTargetParts: Int64 = 9_000

    static func partSize(forFileSize size: Int64) -> Int64 {
        let baseline = multipartPartSize
        let needed = (size + multipartTargetParts - 1) / multipartTargetParts
        if needed <= baseline { return baseline }
        return ((needed + baseline - 1) / baseline) * baseline
    }

    private static let rfc1123Formatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "EEE, dd MMM yyyy HH:mm:ss zzz"
        return formatter
    }()

    private struct MultipartUploadHandle: Hashable {
        let key: String
        let uploadId: String
    }

    struct UploadedPart: Sendable {
        let partNumber: Int
        let etag: String
        let size: Int64
    }

    private let config: Config
    private let session: URLSession
    private let transferSession: URLSession
    private var activeMultipartUploads: Set<MultipartUploadHandle> = []

    init(config: Config) {
        self.config = config

        let metadataConfig = URLSessionConfiguration.ephemeral
        metadataConfig.timeoutIntervalForRequest = Self.metadataRequestTimeout
        metadataConfig.timeoutIntervalForResource = Self.metadataResourceTimeout
        metadataConfig.urlCache = nil
        metadataConfig.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        self.session = URLSession(configuration: metadataConfig)

        let transferConfig = URLSessionConfiguration.ephemeral
        transferConfig.timeoutIntervalForRequest = Self.transferTimeout
        transferConfig.timeoutIntervalForResource = Self.transferTimeout
        transferConfig.urlCache = nil
        transferConfig.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        transferConfig.httpMaximumConnectionsPerHost = max(8, Self.multipartConcurrency * 2)
        self.transferSession = URLSession(configuration: transferConfig)
    }

    deinit {
        session.invalidateAndCancel()
        transferSession.invalidateAndCancel()
    }

    // MARK: - RemoteStorageClientProtocol

    nonisolated func shouldSetModificationDate() -> Bool {
        false
    }

    nonisolated func shouldLimitUploadRetries(for error: Error) -> Bool {
        S3ErrorClassifier.shouldLimitUploadRetries(error)
    }

    func connect() async throws {
        let url = try makeURL(key: "", query: [
            ("list-type", "2"),
            ("max-keys", "0")
        ])
        let request = signedRequest(method: "GET", url: url, bodyHash: .empty)
        _ = try await performMetadata(request)
    }

    func disconnect() async {
        let toAbort = activeMultipartUploads
        activeMultipartUploads.removeAll()
        for handle in toAbort {
            // Orphaned parts are billed; tolerate abort failure (lifecycle policy is the safety net).
            try? await abortMultipartUpload(key: handle.key, uploadId: handle.uploadId)
        }
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        nil
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let prefix = keyPrefix(forListPath: path)
        var entries: [RemoteStorageEntry] = []
        var token: String?
        repeat {
            try Task.checkCancellation()
            var query: [(String, String)] = [
                ("list-type", "2"),
                ("delimiter", "/"),
                ("max-keys", String(Self.listPageSize))
            ]
            if !prefix.isEmpty {
                query.append(("prefix", prefix))
            }
            if let nextToken = token {
                query.append(("continuation-token", nextToken))
            }
            let url = try makeURL(key: "", query: query)
            let request = signedRequest(method: "GET", url: url, bodyHash: .empty)
            let data = try await performMetadata(request)
            let parsed = try S3ListXMLParser().parse(data: data)

            for content in parsed.contents {
                if content.key == prefix { continue }
                entries.append(makeContentEntry(key: content.key, size: content.size, lastModified: content.lastModified, prefix: prefix))
            }
            for commonPrefix in parsed.commonPrefixes {
                entries.append(makePrefixEntry(commonPrefix: commonPrefix, prefix: prefix))
            }
            token = parsed.nextContinuationToken
        } while token != nil
        return entries
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let key = key(forPath: path)
        if key.isEmpty { return nil }
        let url = try makeURL(key: key, query: [])
        let request = signedRequest(method: "HEAD", url: url, bodyHash: .empty)
        do {
            let (_, http) = try await performMetadataResponse(request)
            return parseHeadEntry(http: http, key: key)
        } catch {
            if Self.isNotFoundError(error) { return nil }
            throw error
        }
    }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        let key = key(forPath: remotePath)
        if key.isEmpty {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let size = try fileSize(at: localURL)
        if respectTaskCancellation {
            try Task.checkCancellation()
        }
        onProgress?(0)

        if size > Self.multipartThreshold {
            try await multipartUpload(localURL: localURL, key: key, size: size, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
        } else {
            try await singlePartUpload(localURL: localURL, key: key, size: size)
            onProgress?(1.0)
        }
    }

    private func singlePartUpload(localURL: URL, key: String, size: Int64) async throws {
        if size > Self.singlePartMaxSize {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "File exceeds 5 GiB single-part limit"]
            ))
        }
        let url = try makeURL(key: key, query: [])
        let request = signedRequest(
            method: "PUT",
            url: url,
            additionalHeaders: ["Content-Type": "application/octet-stream"],
            bodyHash: .unsigned
        )
        let (data, response) = try await transferSession.upload(for: request, fromFile: localURL)
        guard let http = response as? HTTPURLResponse else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Unexpected response type"]
            ))
        }
        if !(200 ..< 300).contains(http.statusCode) {
            throw makeServerError(method: "PUT", url: url, statusCode: http.statusCode, body: data)
        }
    }

    private func multipartUpload(localURL: URL, key: String, size: Int64, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        let uploadId = try await createMultipartUpload(key: key)
        let handle = MultipartUploadHandle(key: key, uploadId: uploadId)
        activeMultipartUploads.insert(handle)
        do {
            let parts = try await uploadAllParts(
                localURL: localURL,
                key: key,
                uploadId: uploadId,
                size: size,
                respectTaskCancellation: respectTaskCancellation,
                onProgress: onProgress
            )
            try await completeMultipartUpload(key: key, uploadId: uploadId, parts: parts)
            activeMultipartUploads.remove(handle)
            onProgress?(1.0)
        } catch {
            activeMultipartUploads.remove(handle)
            try? await abortMultipartUpload(key: key, uploadId: uploadId)
            throw error
        }
    }

    private func createMultipartUpload(key: String) async throws -> String {
        let url = try makeURL(key: key, query: [("uploads", "")])
        let request = signedRequest(
            method: "POST",
            url: url,
            additionalHeaders: ["Content-Type": "application/octet-stream"],
            bodyHash: .empty
        )
        let data = try await performMetadata(request)
        guard let uploadId = S3SimpleXMLValueParser(target: "UploadId").parse(data: data),
              !uploadId.isEmpty else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Missing UploadId in CreateMultipartUpload response"]
            ))
        }
        return uploadId
    }

    private func uploadAllParts(
        localURL: URL,
        key: String,
        uploadId: String,
        size: Int64,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> [UploadedPart] {
        let partSize = Self.partSize(forFileSize: size)
        let totalParts = Int((size + partSize - 1) / partSize)
        if totalParts > Self.multipartMaxParts {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "File exceeds maximum upload size"]
            ))
        }

        var collected: [UploadedPart] = []
        var nextPartNumber = 1
        var bytesUploaded: Int64 = 0

        while nextPartNumber <= totalParts {
            if respectTaskCancellation {
                try Task.checkCancellation()
            }
            let batchEnd = min(nextPartNumber + Self.multipartConcurrency - 1, totalParts)
            let batch = try await withThrowingTaskGroup(of: UploadedPart.self) { group in
                for partNumber in nextPartNumber...batchEnd {
                    let offset = Int64(partNumber - 1) * partSize
                    let length = min(partSize, size - offset)
                    group.addTask { [self] in
                        try await uploadOnePart(
                            localURL: localURL,
                            key: key,
                            uploadId: uploadId,
                            partNumber: partNumber,
                            offset: offset,
                            length: length
                        )
                    }
                }
                var results: [UploadedPart] = []
                for try await part in group {
                    results.append(part)
                }
                return results
            }
            collected.append(contentsOf: batch)
            bytesUploaded += batch.reduce(0) { $0 + $1.size }
            if size > 0 {
                onProgress?(Double(bytesUploaded) / Double(size))
            }
            nextPartNumber = batchEnd + 1
        }

        collected.sort { $0.partNumber < $1.partNumber }
        return collected
    }

    private func uploadOnePart(
        localURL: URL,
        key: String,
        uploadId: String,
        partNumber: Int,
        offset: Int64,
        length: Int64
    ) async throws -> UploadedPart {
        let partData = try readFileSlice(at: localURL, offset: offset, length: length)
        let url = try makeURL(key: key, query: [
            ("partNumber", String(partNumber)),
            ("uploadId", uploadId)
        ])
        let request = signedRequest(
            method: "PUT",
            url: url,
            bodyHash: .unsigned
        )
        let (responseBody, response) = try await transferSession.upload(for: request, from: partData)
        guard let http = response as? HTTPURLResponse else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Unexpected response type for UploadPart"]
            ))
        }
        if !(200 ..< 300).contains(http.statusCode) {
            throw makeServerError(method: "PUT", url: url, statusCode: http.statusCode, body: responseBody)
        }
        let etag = http.value(forHTTPHeaderField: "ETag") ?? ""
        if etag.isEmpty {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Missing ETag header on UploadPart response (part \(partNumber))"]
            ))
        }
        return UploadedPart(partNumber: partNumber, etag: etag, size: Int64(partData.count))
    }

    private func completeMultipartUpload(key: String, uploadId: String, parts: [UploadedPart]) async throws {
        let url = try makeURL(key: key, query: [("uploadId", uploadId)])
        let body = Data(Self.buildCompleteMultipartXML(parts: parts).utf8)
        let request = signedRequest(
            method: "POST",
            url: url,
            additionalHeaders: ["Content-Type": "application/xml"],
            bodyHash: .data(body)
        )
        // Large-upload assembly can exceed metadata session's 120s timeout.
        let (data, response) = try await transferSession.upload(for: request, from: body)
        guard let http = response as? HTTPURLResponse else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Unexpected response type for CompleteMultipartUpload"]
            ))
        }
        if !(200 ..< 300).contains(http.statusCode) {
            throw makeServerError(method: "POST", url: url, statusCode: http.statusCode, body: data)
        }
        try throwIfEmbeddedError(method: "POST", url: url, body: data)
    }

    private func abortMultipartUpload(key: String, uploadId: String) async throws {
        let url = try makeURL(key: key, query: [("uploadId", uploadId)])
        let request = signedRequest(method: "DELETE", url: url, bodyHash: .empty)
        _ = try await performMetadata(request)
    }

    nonisolated static func buildCompleteMultipartXML(parts: [UploadedPart]) -> String {
        var xml = "<CompleteMultipartUpload>"
        for part in parts {
            xml += "<Part><PartNumber>\(part.partNumber)</PartNumber><ETag>\(part.etag)</ETag></Part>"
        }
        xml += "</CompleteMultipartUpload>"
        return xml
    }

    nonisolated private func readFileSlice(at url: URL, offset: Int64, length: Int64) throws -> Data {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        try handle.seek(toOffset: UInt64(offset))
        return try handle.read(upToCount: Int(length)) ?? Data()
    }

    func setModificationDate(_: Date, forPath _: String) async throws {}

    func download(remotePath: String, localURL: URL) async throws {
        let key = key(forPath: remotePath)
        if key.isEmpty {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let url = try makeURL(key: key, query: [])
        let request = signedRequest(method: "GET", url: url, bodyHash: .empty)
        let (tempURL, response) = try await transferSession.download(for: request)
        defer { try? FileManager.default.removeItem(at: tempURL) }
        guard let http = response as? HTTPURLResponse else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Unexpected response type"]
            ))
        }
        if !(200 ..< 300).contains(http.statusCode) {
            let body = (try? Data(contentsOf: tempURL)) ?? Data()
            throw makeServerError(method: "GET", url: url, statusCode: http.statusCode, body: body)
        }
        let parent = localURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
        if FileManager.default.fileExists(atPath: localURL.path) {
            try FileManager.default.removeItem(at: localURL)
        }
        try FileManager.default.moveItem(at: tempURL, to: localURL)
    }

    func exists(path: String) async throws -> Bool {
        try await metadata(path: path) != nil
    }

    func delete(path: String) async throws {
        let key = key(forPath: path)
        if key.isEmpty {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let url = try makeURL(key: key, query: [])
        let request = signedRequest(method: "DELETE", url: url, bodyHash: .empty)
        _ = try await performMetadata(request)
    }

    func createDirectory(path _: String) async throws {}

    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await copy(from: sourcePath, to: destinationPath)
        try await delete(path: sourcePath)
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        let sourceKey = key(forPath: sourcePath)
        let destinationKey = key(forPath: destinationPath)
        if sourceKey.isEmpty || destinationKey.isEmpty {
            throw RemoteStorageClientError.invalidConfiguration
        }

        // Single CopyObject is capped at 5 GiB; larger sources must use UploadPartCopy.
        if let sourceSize = try await metadata(path: sourcePath)?.size,
           sourceSize > Self.singlePartMaxSize {
            try await multipartCopy(sourceKey: sourceKey, destinationKey: destinationKey, sourceSize: sourceSize)
            return
        }

        let destinationURL = try makeURL(key: destinationKey, query: [])
        let request = signedRequest(
            method: "PUT",
            url: destinationURL,
            additionalHeaders: ["x-amz-copy-source": Self.copySourceHeader(bucket: config.bucket, key: sourceKey)],
            bodyHash: .empty
        )
        let data = try await performMetadata(request)
        try throwIfEmbeddedError(method: "PUT", url: destinationURL, body: data)
    }

    private func multipartCopy(sourceKey: String, destinationKey: String, sourceSize: Int64) async throws {
        let uploadId = try await createMultipartUpload(key: destinationKey)
        let handle = MultipartUploadHandle(key: destinationKey, uploadId: uploadId)
        activeMultipartUploads.insert(handle)
        do {
            let parts = try await uploadAllCopyParts(
                sourceKey: sourceKey,
                destinationKey: destinationKey,
                uploadId: uploadId,
                sourceSize: sourceSize
            )
            try await completeMultipartUpload(key: destinationKey, uploadId: uploadId, parts: parts)
            activeMultipartUploads.remove(handle)
        } catch {
            activeMultipartUploads.remove(handle)
            try? await abortMultipartUpload(key: destinationKey, uploadId: uploadId)
            throw error
        }
    }

    private func uploadAllCopyParts(
        sourceKey: String,
        destinationKey: String,
        uploadId: String,
        sourceSize: Int64
    ) async throws -> [UploadedPart] {
        let partSize = Self.partSize(forFileSize: sourceSize)
        let totalParts = Int((sourceSize + partSize - 1) / partSize)
        if totalParts > Self.multipartMaxParts {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Source object exceeds maximum copy size"]
            ))
        }

        var collected: [UploadedPart] = []
        var nextPartNumber = 1
        while nextPartNumber <= totalParts {
            try Task.checkCancellation()
            let batchEnd = min(nextPartNumber + Self.multipartConcurrency - 1, totalParts)
            let batch = try await withThrowingTaskGroup(of: UploadedPart.self) { group in
                for partNumber in nextPartNumber...batchEnd {
                    let offset = Int64(partNumber - 1) * partSize
                    let endByte = min(offset + partSize - 1, sourceSize - 1)
                    group.addTask { [self] in
                        try await uploadOneCopyPart(
                            sourceKey: sourceKey,
                            destinationKey: destinationKey,
                            uploadId: uploadId,
                            partNumber: partNumber,
                            rangeStart: offset,
                            rangeEnd: endByte
                        )
                    }
                }
                var results: [UploadedPart] = []
                for try await part in group {
                    results.append(part)
                }
                return results
            }
            collected.append(contentsOf: batch)
            nextPartNumber = batchEnd + 1
        }
        collected.sort { $0.partNumber < $1.partNumber }
        return collected
    }

    private func uploadOneCopyPart(
        sourceKey: String,
        destinationKey: String,
        uploadId: String,
        partNumber: Int,
        rangeStart: Int64,
        rangeEnd: Int64
    ) async throws -> UploadedPart {
        let url = try makeURL(key: destinationKey, query: [
            ("partNumber", String(partNumber)),
            ("uploadId", uploadId)
        ])
        let request = signedRequest(
            method: "PUT",
            url: url,
            additionalHeaders: [
                "x-amz-copy-source": Self.copySourceHeader(bucket: config.bucket, key: sourceKey),
                "x-amz-copy-source-range": "bytes=\(rangeStart)-\(rangeEnd)"
            ],
            bodyHash: .empty
        )
        // Large server-side part copy can exceed metadata session's 120s timeout.
        let (data, response) = try await transferSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Unexpected response type for UploadPartCopy"]
            ))
        }
        if !(200 ..< 300).contains(http.statusCode) {
            throw makeServerError(method: "PUT", url: url, statusCode: http.statusCode, body: data)
        }
        try throwIfEmbeddedError(method: "PUT", url: url, body: data)
        guard let etag = S3SimpleXMLValueParser(target: "ETag").parse(data: data),
              !etag.isEmpty else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Missing ETag in UploadPartCopy response (part \(partNumber))"]
            ))
        }
        return UploadedPart(partNumber: partNumber, etag: etag, size: rangeEnd - rangeStart + 1)
    }

    // MARK: - URL building

    nonisolated func makeURL(key: String, query: [(String, String)]) throws -> URL {
        var components = URLComponents()
        components.scheme = effectiveScheme

        if config.usePathStyle {
            components.host = config.endpointHost
            let bucketSegment = "/" + Self.percentEncodeURIComponent(config.bucket)
            if key.isEmpty {
                components.percentEncodedPath = bucketSegment
            } else {
                components.percentEncodedPath = bucketSegment + "/" + Self.percentEncodePath(key)
            }
        } else {
            components.host = "\(config.bucket).\(config.endpointHost)"
            if key.isEmpty {
                components.percentEncodedPath = "/"
            } else {
                components.percentEncodedPath = "/" + Self.percentEncodePath(key)
            }
        }

        if let port = effectivePort {
            components.port = port
        }

        if !query.isEmpty {
            components.percentEncodedQuery = query
                .map { "\(Self.percentEncodeURIComponent($0.0))=\(Self.percentEncodeURIComponent($0.1))" }
                .joined(separator: "&")
        }

        guard let url = components.url else {
            throw RemoteStorageClientError.invalidConfiguration
        }
        return url
    }

    nonisolated private var effectiveScheme: String {
        let scheme = config.scheme.lowercased()
        return scheme == "http" ? "http" : "https"
    }

    nonisolated private var effectivePort: Int? {
        let port = config.endpointPort
        if port == 0 { return nil }
        let defaultPort = effectiveScheme == "https" ? 443 : 80
        return port == defaultPort ? nil : port
    }

    nonisolated private var effectiveRegion: String {
        config.region.isEmpty ? "us-east-1" : config.region
    }

    nonisolated private static let uriUnreserved: CharacterSet = {
        var allowed = CharacterSet()
        allowed.insert(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~")
        return allowed
    }()

    nonisolated static func percentEncodeURIComponent(_ value: String) -> String {
        value.addingPercentEncoding(withAllowedCharacters: uriUnreserved) ?? ""
    }

    nonisolated static func percentEncodePath(_ key: String) -> String {
        key.split(separator: "/", omittingEmptySubsequences: false)
            .map(String.init)
            .map { percentEncodeURIComponent($0) }
            .joined(separator: "/")
    }

    nonisolated static func copySourceHeader(bucket: String, key: String) -> String {
        "/" + percentEncodeURIComponent(bucket) + "/" + percentEncodePath(key)
    }

    nonisolated static func parseEndpoint(_ raw: String) -> (scheme: String, host: String, port: Int)? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { return nil }
        let normalized = trimmed.contains("://") ? trimmed : "https://" + trimmed
        guard let url = URL(string: normalized),
              let host = url.host?.trimmingCharacters(in: .whitespacesAndNewlines),
              !host.isEmpty else { return nil }
        let scheme = (url.scheme ?? "https").lowercased()
        if scheme != "http", scheme != "https" { return nil }
        let port = url.port ?? (scheme == "http" ? 80 : 443)
        return (scheme, host, port)
    }

    nonisolated static func defaultPathStyle(forHost host: String) -> Bool {
        let lower = host.lowercased()
        if lower.hasSuffix(".amazonaws.com") { return false }
        if lower.hasSuffix(".cloudflarestorage.com") { return false }
        if lower.hasSuffix(".backblazeb2.com") { return false }
        return true
    }

    nonisolated private func fileSize(at url: URL) throws -> Int64 {
        let attrs = try FileManager.default.attributesOfItem(atPath: url.path)
        return (attrs[.size] as? NSNumber)?.int64Value ?? 0
    }

    // MARK: - Path / key helpers

    nonisolated private func key(forPath path: String) -> String {
        let normalized = RemotePathBuilder.normalizePath(path)
        return normalized == "/" ? "" : String(normalized.dropFirst())
    }

    nonisolated private func keyPrefix(forListPath path: String) -> String {
        let key = key(forPath: path)
        return key.isEmpty ? "" : key + "/"
    }

    // MARK: - Signing

    nonisolated private func signedRequest(
        method: String,
        url: URL,
        additionalHeaders: [String: String] = [:],
        bodyHash: S3SigV4Signer.BodyHash
    ) -> URLRequest {
        let signed = S3SigV4Signer.sign(
            method: method,
            url: url,
            additionalHeaders: additionalHeaders,
            bodyHash: bodyHash,
            accessKeyID: config.accessKeyID,
            secretAccessKey: config.secretAccessKey,
            sessionToken: config.sessionToken,
            region: effectiveRegion,
            date: Date()
        )
        var request = URLRequest(url: url)
        request.httpMethod = method
        // host and content-length are set automatically by URLSession.
        for (key, value) in signed.headers where key != "host" && key != "content-length" {
            request.setValue(value, forHTTPHeaderField: key)
        }
        return request
    }

    // MARK: - Request execution

    private func performMetadata(_ request: URLRequest) async throws -> Data {
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Unexpected response type"]
            ))
        }
        if !(200 ..< 300).contains(http.statusCode) {
            throw makeServerError(method: request.httpMethod ?? "?", url: request.url, statusCode: http.statusCode, body: data)
        }
        return data
    }

    private func performMetadataResponse(_ request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw RemoteStorageClientError.underlying(NSError(
                domain: Self.errorDomain,
                code: -1,
                userInfo: [NSLocalizedDescriptionKey: "Unexpected response type"]
            ))
        }
        if !(200 ..< 300).contains(http.statusCode) {
            throw makeServerError(method: request.httpMethod ?? "?", url: request.url, statusCode: http.statusCode, body: data)
        }
        return (data, http)
    }

    // MARK: - Error construction

    // S3 returns HTTP 200 with embedded <Error> on partial server-side failures of
    // CopyObject, CompleteMultipartUpload, and UploadPartCopy.
    nonisolated private func throwIfEmbeddedError(method: String, url: URL, body: Data) throws {
        if let parsed = S3ErrorXMLParser().parse(data: body), parsed.code != nil {
            throw makeEmbeddedError(method: method, url: url, payload: parsed)
        }
    }

    nonisolated private func makeEmbeddedError(method: String, url: URL?, payload: S3ErrorPayload) -> RemoteStorageClientError {
        let target = url?.absoluteString ?? "(unknown URL)"
        let baseDescription = String.localizedStringWithFormat(
            String(localized: "s3.error.requestFailed"),
            method,
            200,
            target
        )
        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: baseDescription,
            S3ErrorClassifier.userInfoStatusCodeKey: 200
        ]
        if let code = payload.code { userInfo[S3ErrorClassifier.userInfoServerCodeKey] = code }
        if let message = payload.message { userInfo[S3ErrorClassifier.userInfoServerMessageKey] = message }
        return .underlying(NSError(domain: Self.errorDomain, code: 200, userInfo: userInfo))
    }

    nonisolated private func makeServerError(method: String, url: URL?, statusCode: Int, body: Data) -> RemoteStorageClientError {
        var serverCode: String?
        var serverMessage: String?
        if !body.isEmpty, let parsed = S3ErrorXMLParser().parse(data: body) {
            serverCode = parsed.code
            serverMessage = parsed.message
        }
        let target = url?.absoluteString ?? "(unknown URL)"
        let baseDescription = String.localizedStringWithFormat(
            String(localized: "s3.error.requestFailed"),
            method,
            statusCode,
            target
        )
        var userInfo: [String: Any] = [
            NSLocalizedDescriptionKey: baseDescription,
            S3ErrorClassifier.userInfoStatusCodeKey: statusCode
        ]
        if let serverCode { userInfo[S3ErrorClassifier.userInfoServerCodeKey] = serverCode }
        if let serverMessage { userInfo[S3ErrorClassifier.userInfoServerMessageKey] = serverMessage }
        return .underlying(NSError(domain: Self.errorDomain, code: statusCode, userInfo: userInfo))
    }

    nonisolated private static func isNotFoundError(_ error: Error) -> Bool {
        if let storage = error as? RemoteStorageClientError, case .underlying(let inner) = storage {
            return isNotFoundError(inner)
        }
        let ns = error as NSError
        if ns.domain == errorDomain {
            if ns.code == 404 { return true }
            if let serverCode = ns.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String,
               serverCode == "NoSuchKey" || serverCode == "NotFound" {
                return true
            }
        }
        return false
    }

    // MARK: - Entry construction

    nonisolated private func makeContentEntry(key: String, size: Int64, lastModified: Date?, prefix: String) -> RemoteStorageEntry {
        let name: String
        if !prefix.isEmpty, key.hasPrefix(prefix) {
            name = String(key.dropFirst(prefix.count))
        } else {
            name = key.split(separator: "/").last.map(String.init) ?? key
        }
        return RemoteStorageEntry(
            path: "/" + key,
            name: name,
            isDirectory: false,
            size: size,
            creationDate: nil,
            modificationDate: lastModified
        )
    }

    nonisolated private func makePrefixEntry(commonPrefix: String, prefix: String) -> RemoteStorageEntry {
        var trimmed = commonPrefix
        if trimmed.hasSuffix("/") { trimmed.removeLast() }
        let name: String
        if !prefix.isEmpty, trimmed.hasPrefix(prefix) {
            name = String(trimmed.dropFirst(prefix.count))
        } else {
            name = trimmed.split(separator: "/").last.map(String.init) ?? trimmed
        }
        return RemoteStorageEntry(
            path: "/" + trimmed,
            name: name,
            isDirectory: true,
            size: 0,
            creationDate: nil,
            modificationDate: nil
        )
    }

    nonisolated private func parseHeadEntry(http: HTTPURLResponse, key: String) -> RemoteStorageEntry {
        let size = (http.value(forHTTPHeaderField: "Content-Length")).flatMap(Int64.init) ?? 0
        let lastModified = (http.value(forHTTPHeaderField: "Last-Modified")).flatMap { Self.rfc1123Formatter.date(from: $0) }
        let name = key.split(separator: "/").last.map(String.init) ?? key
        return RemoteStorageEntry(
            path: "/" + key,
            name: name,
            isDirectory: false,
            size: size,
            creationDate: nil,
            modificationDate: lastModified
        )
    }
}

// MARK: - XML parsers

struct S3ListContent {
    var key: String
    var size: Int64
    var lastModified: Date?
}

struct S3ListResult {
    var contents: [S3ListContent] = []
    var commonPrefixes: [String] = []
    var nextContinuationToken: String?
    var isTruncated = false
}

final class S3ListXMLParser: NSObject, XMLParserDelegate {
    private struct PartialContent {
        var key: String = ""
        var size: Int64 = 0
        var lastModified: Date?
    }

    private var result = S3ListResult()
    private var currentContent: PartialContent?
    private var elementStack: [String] = []
    private var textBuffer = ""

    func parse(data: Data) throws -> S3ListResult {
        let parser = XMLParser(data: data)
        parser.delegate = self
        guard parser.parse() else {
            throw RemoteStorageClientError.underlying(parser.parserError ?? NSError(
                domain: S3Client.errorDomain,
                code: -1001,
                userInfo: [NSLocalizedDescriptionKey: "Failed to parse S3 list response"]
            ))
        }
        return result
    }

    func parser(_: XMLParser, didStartElement elementName: String, namespaceURI _: String?, qualifiedName _: String?, attributes _: [String: String] = [:]) {
        elementStack.append(elementName)
        textBuffer = ""
        if elementName == "Contents" {
            currentContent = PartialContent()
        }
    }

    func parser(_: XMLParser, foundCharacters string: String) {
        textBuffer.append(string)
    }

    func parser(_: XMLParser, didEndElement elementName: String, namespaceURI _: String?, qualifiedName _: String?) {
        defer {
            if !elementStack.isEmpty { elementStack.removeLast() }
            textBuffer = ""
        }
        let value = textBuffer.trimmingCharacters(in: .whitespacesAndNewlines)
        let parent = elementStack.dropLast().last ?? ""

        switch (elementName, parent) {
        case ("Contents", _):
            if let c = currentContent {
                result.contents.append(S3ListContent(key: c.key, size: c.size, lastModified: c.lastModified))
            }
            currentContent = nil
        case ("Key", "Contents"):
            currentContent?.key = value
        case ("Size", "Contents"):
            currentContent?.size = Int64(value) ?? 0
        case ("LastModified", "Contents"):
            currentContent?.lastModified = Self.parseDate(value)
        case ("Prefix", "CommonPrefixes"):
            if !value.isEmpty { result.commonPrefixes.append(value) }
        case ("NextContinuationToken", _):
            if !value.isEmpty { result.nextContinuationToken = value }
        case ("IsTruncated", _):
            result.isTruncated = (value.lowercased() == "true")
        default:
            break
        }
    }

    static func parseDate(_ value: String) -> Date? {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let d = formatter.date(from: value) { return d }
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: value)
    }
}

struct S3ErrorPayload {
    var code: String?
    var message: String?
}

final class S3SimpleXMLValueParser: NSObject, XMLParserDelegate {
    private let target: String
    private var capturing = false
    private var captured: String?
    private var textBuffer = ""

    init(target: String) {
        self.target = target
    }

    func parse(data: Data) -> String? {
        let parser = XMLParser(data: data)
        parser.delegate = self
        guard parser.parse() else { return nil }
        return captured
    }

    func parser(_: XMLParser, didStartElement elementName: String, namespaceURI _: String?, qualifiedName _: String?, attributes _: [String: String] = [:]) {
        if elementName == target, captured == nil {
            capturing = true
            textBuffer = ""
        }
    }

    func parser(_: XMLParser, foundCharacters string: String) {
        if capturing {
            textBuffer.append(string)
        }
    }

    func parser(_: XMLParser, didEndElement elementName: String, namespaceURI _: String?, qualifiedName _: String?) {
        if elementName == target, capturing {
            captured = textBuffer.trimmingCharacters(in: .whitespacesAndNewlines)
            capturing = false
            textBuffer = ""
        }
    }
}

final class S3ErrorXMLParser: NSObject, XMLParserDelegate {
    private var result = S3ErrorPayload()
    private var currentElement: String?
    private var textBuffer = ""

    func parse(data: Data) -> S3ErrorPayload? {
        let parser = XMLParser(data: data)
        parser.delegate = self
        guard parser.parse() else { return nil }
        if result.code != nil || result.message != nil {
            return result
        }
        return nil
    }

    func parser(_: XMLParser, didStartElement elementName: String, namespaceURI _: String?, qualifiedName _: String?, attributes _: [String: String] = [:]) {
        currentElement = elementName
        textBuffer = ""
    }

    func parser(_: XMLParser, foundCharacters string: String) {
        textBuffer.append(string)
    }

    func parser(_: XMLParser, didEndElement elementName: String, namespaceURI _: String?, qualifiedName _: String?) {
        let value = textBuffer.trimmingCharacters(in: .whitespacesAndNewlines)
        switch elementName {
        case "Code":
            if !value.isEmpty { result.code = value }
        case "Message":
            if !value.isEmpty { result.message = value }
        default:
            break
        }
        currentElement = nil
        textBuffer = ""
    }
}

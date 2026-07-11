import Foundation

final actor S3Client: RemoteStorageClientProtocol {
    static let errorDomain = S3ErrorClassifier.errorDomain

    struct Config: Sendable {
        let endpointHost: String
        let endpointPort: Int
        let scheme: String
        let region: String
        let bucket: String
        let basePath: String
        let usePathStyle: Bool
        let accessKeyID: String
        let secretAccessKey: String
        let sessionToken: String?
    }

    private static let metadataRequestTimeout: TimeInterval = 45
    private static let metadataResourceTimeout: TimeInterval = 120
    private static let transferTimeout: TimeInterval = 7 * 24 * 60 * 60
    // The transfer session's timeout is effectively unbounded; a no-progress watchdog bounds a half-open transfer.
    private static let uploadBodyStallTimeout: TimeInterval = 3 * 60
    private static let uploadResponseTimeout: TimeInterval = 5 * 60
    private static let downloadInitialResponseTimeout: TimeInterval = 5 * 60
    private static let downloadStallTimeout: TimeInterval = 3 * 60
    private static let watchdogPollInterval: TimeInterval = 5
    static let uploadStalledErrorCode = -1301
    static let uploadResponseTimeoutErrorCode = -1302
    static let downloadStalledErrorCode = -1303
    private static let transferStallTimeouts = URLSessionStallWatchdog.Timeouts(
        uploadBodyStall: uploadBodyStallTimeout,
        uploadResponseStall: uploadResponseTimeout,
        downloadFirstByte: downloadInitialResponseTimeout,
        downloadStall: downloadStallTimeout,
        pollInterval: watchdogPollInterval
    )
    // CompleteMultipartUpload / UploadPartCopy do server-side work with no byte progress to feed the watchdog
    // (these intentionally use the long-lived transfer session). The body-stall still bounds a stuck request, but
    // the response / first-byte wait is generous so slow server-side assembly/copy isn't false-timed-out.
    private static let serverProcessingResponseTimeout: TimeInterval = 30 * 60
    private static let serverProcessingStallTimeouts = URLSessionStallWatchdog.Timeouts(
        uploadBodyStall: uploadBodyStallTimeout,
        uploadResponseStall: serverProcessingResponseTimeout,
        downloadFirstByte: serverProcessingResponseTimeout,
        downloadStall: downloadStallTimeout,
        pollInterval: watchdogPollInterval
    )
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
    private static let probeKeyPrefix = ".watermelon_probe_"

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
    private let transferDelegate = URLSessionStallWatchdog.Delegate()
    nonisolated private let metadataTasks = URLSessionTaskRegistry()
    nonisolated private let transferTasks = URLSessionTaskRegistry()
    nonisolated private let verificationTemporaryFiles = VerificationTemporaryFileRegistry()
    nonisolated private let verificationCleanupRegistry = S3VerificationCleanupRegistry()
    private var activeMultipartUploads: Set<MultipartUploadHandle> = []
    private var abandonedVerificationCleanupURLs: Set<URL> = []

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
        self.transferSession = URLSession(configuration: transferConfig, delegate: transferDelegate, delegateQueue: nil)
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

    nonisolated func cancelActiveOperationsForAbandonment() {
        verificationCleanupRegistry.scheduleAllDelayedConfirmations()
        verificationTemporaryFiles.abandon()
        metadataTasks.cancelAll()
        transferTasks.cancelAll()
    }

    func reapAbandonedOperations() async {
        let urls = abandonedVerificationCleanupURLs
        abandonedVerificationCleanupURLs.removeAll()
        for url in urls {
            try? await deleteObject(at: url)
        }
        await disconnect()
    }

    func connect() async throws {
        var query: [(String, String)] = [
            ("list-type", "2"),
            ("max-keys", "1")
        ]
        let prefix = keyPrefix(forListPath: config.basePath)
        if !prefix.isEmpty {
            query.append(("prefix", prefix))
        }
        let url = try makeURL(key: "", query: query)
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

    func verifyWriteAccess() async throws {
        try Task.checkCancellation()
        let pathA = makeProbePath()
        let pathB = makeProbePath()
        let keyA = key(forPath: pathA)
        let keyB = key(forPath: pathB)
        let urlA = try makeURL(key: keyA, query: [])
        let urlB = try makeURL(key: keyB, query: [])
        let firstProbeData = Data("watermelon-write-probe-a".utf8)
        let secondProbeData = Data("watermelon-write-probe-b".utf8)
        let probeName = UUID().uuidString.lowercased()
        let firstLocalURL = FileManager.default.temporaryDirectory.appendingPathComponent("s3-probe-\(probeName)-a")
        let secondLocalURL = FileManager.default.temporaryDirectory.appendingPathComponent("s3-probe-\(probeName)-b")
        let downloadedURL = FileManager.default.temporaryDirectory.appendingPathComponent("s3-probe-\(probeName)-download")
        let temporaryFiles = VerificationTemporaryFileLease(
            urls: [firstLocalURL, secondLocalURL, downloadedURL]
        )
        let cleanupConfig = config
        let cleanupCoordinator = RemoteProbeCleanupCoordinator(
            makeClient: { S3Client(config: cleanupConfig) },
            probePaths: [pathA, pathB],
            shouldConnect: false
        )
        guard verificationTemporaryFiles.register(temporaryFiles) else { throw CancellationError() }
        defer { verificationTemporaryFiles.unregister(temporaryFiles) }
        try temporaryFiles.write([
            (firstProbeData, firstLocalURL),
            (secondProbeData, secondLocalURL)
        ])
        let cleanupRegistration = verificationCleanupRegistry.register(cleanupCoordinator)
        defer { verificationCleanupRegistry.unregister(cleanupRegistration) }

        do {
            try await upload(
                localURL: firstLocalURL,
                remotePath: pathA,
                mode: .createIfAbsent,
                respectTaskCancellation: true,
                onProgress: nil
            )
            try Task.checkCancellation()
            var collisionProven = false
            do {
                try await upload(
                    localURL: secondLocalURL,
                    remotePath: pathA,
                    mode: .createIfAbsent,
                    respectTaskCancellation: true,
                    onProgress: nil
                )
            } catch {
                guard remoteStorageIsNameCollision(error) else { throw error }
                collisionProven = true
            }
            guard collisionProven else {
                throw RemoteStorageClientError.unsafeConditionalCreateUnsupported
            }
            try Task.checkCancellation()
            try await download(remotePath: pathA, localURL: downloadedURL)
            guard try Data(contentsOf: downloadedURL) == firstProbeData else {
                throw RemoteStorageClientError.unavailable
            }
            try Task.checkCancellation()

            try await serverSideCopy(sourceKey: keyA, destinationURL: urlB)
            try Task.checkCancellation()
            let getRequest = signedRequest(method: "GET", url: urlB, bodyHash: .empty)
            let (copiedData, _) = try await performMetadata(getRequest)
            guard copiedData == firstProbeData else {
                throw RemoteStorageClientError.unavailable
            }

            async let delA: Void = deleteObject(at: urlA)
            async let delB: Void = deleteObject(at: urlB)
            try await delA
            try await delB
        } catch {
            if Task.isCancelled || error is CancellationError {
                cleanupCoordinator.schedule(.delayedConfirmation)
                abandonedVerificationCleanupURLs.formUnion([urlA, urlB])
                throw CancellationError()
            }
            cleanupCoordinator.schedule(.delayedConfirmation)
            throw error
        }
    }

    nonisolated private func makeProbePath() -> String {
        RemotePathBuilder.absolutePath(
            basePath: config.basePath,
            remoteRelativePath: Self.probeKeyPrefix + UUID().uuidString.lowercased()
        )
    }

    private func serverSideCopy(sourceKey: String, destinationURL: URL) async throws {
        let req = signedRequest(
            method: "PUT",
            url: destinationURL,
            additionalHeaders: ["x-amz-copy-source": Self.copySourceHeader(bucket: config.bucket, key: sourceKey)],
            bodyHash: .empty
        )
        let (body, _) = try await performMetadata(req)
        try throwIfEmbeddedError(method: "PUT", url: destinationURL, body: body)
    }

    private func deleteObject(at url: URL) async throws {
        let req = signedRequest(method: "DELETE", url: url, bodyHash: .empty)
        _ = try await performMetadata(req)
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
            let (data, _) = try await performMetadata(request)
            let parsed = try S3ListXMLParser().parse(data: data)

            for content in parsed.contents {
                if content.key == prefix { continue }
                entries.append(makeContentEntry(key: content.key, size: content.size, lastModified: content.lastModified, prefix: prefix))
            }
            for commonPrefix in parsed.commonPrefixes {
                entries.append(makePrefixEntry(commonPrefix: commonPrefix, prefix: prefix))
            }
            token = try Self.nextListContinuationToken(
                isTruncated: parsed.isTruncated,
                nextContinuationToken: parsed.nextContinuationToken
            )
        } while token != nil
        return entries
    }

    // A truncated page with no usable continuation token is an unresolved listing we cannot continue;
    // returning it as complete would let Lite reconcile read the omitted objects as deletions.
    nonisolated static func nextListContinuationToken(
        isTruncated: Bool,
        nextContinuationToken: String?
    ) throws -> String? {
        guard isTruncated else { return nil }
        guard let token = nextContinuationToken, !token.isEmpty else {
            throw internalError("S3 ListObjectsV2 reported truncation without a continuation token")
        }
        return token
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        let key = key(forPath: path)
        if key.isEmpty { return nil }
        let url = try makeURL(key: key, query: [])
        let request = signedRequest(method: "HEAD", url: url, bodyHash: .empty)
        do {
            let (_, http) = try await performMetadata(request)
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
        let key = key(forPath: remotePath)
        if key.isEmpty {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let size = try fileSize(at: localURL)
        if respectTaskCancellation {
            try Task.checkCancellation()
        }
        onProgress?(0)

        if mode == .createIfAbsent {
            try await singlePartUpload(localURL: localURL, key: key, size: size, mode: mode)
            onProgress?(1.0)
        } else if size > Self.multipartThreshold {
            try await multipartUpload(localURL: localURL, key: key, size: size, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
        } else {
            try await singlePartUpload(localURL: localURL, key: key, size: size, mode: mode)
            onProgress?(1.0)
        }
    }

    private func singlePartUpload(localURL: URL, key: String, size: Int64, mode: RemoteUploadMode) async throws {
        if size > Self.singlePartMaxSize {
            throw Self.internalError("File exceeds 5 GiB single-part limit")
        }
        let url = try makeURL(key: key, query: [])
        var headers = ["Content-Type": "application/octet-stream"]
        if mode == .createIfAbsent {
            headers["If-None-Match"] = "*"
        }
        let request = signedRequest(
            method: "PUT",
            url: url,
            additionalHeaders: headers,
            bodyHash: .unsigned
        )
        do {
            _ = try await performTransfer(request, fromFile: localURL)
        } catch {
            if mode == .createIfAbsent, Self.isConditionalCreateCollision(error) {
                throw remoteStorageNameCollisionError(path: key)
            }
            throw error
        }
    }

    typealias PartUploader = @Sendable (_ uploadId: String, _ partNumber: Int, _ offset: Int64, _ length: Int64) async throws -> UploadedPart

    private func runMultipartTransfer(
        key: String,
        totalSize: Int64,
        respectCancellation: Bool,
        onProgress: ((Double) -> Void)?,
        uploadPart: @escaping PartUploader
    ) async throws {
        let partSize = Self.partSize(forFileSize: totalSize)
        let totalParts = Int((totalSize + partSize - 1) / partSize)
        if totalParts > Self.multipartMaxParts {
            throw Self.internalError("Object exceeds maximum part count of \(Self.multipartMaxParts)")
        }

        let uploadId = try await createMultipartUpload(key: key)
        let handle = MultipartUploadHandle(key: key, uploadId: uploadId)
        activeMultipartUploads.insert(handle)

        do {
            var collected: [UploadedPart] = []
            var nextPartNumber = 1
            var bytesUploaded: Int64 = 0

            while nextPartNumber <= totalParts {
                if respectCancellation {
                    try Task.checkCancellation()
                }
                let batchEnd = min(nextPartNumber + Self.multipartConcurrency - 1, totalParts)
                let batch = try await withThrowingTaskGroup(of: UploadedPart.self) { group in
                    for partNumber in nextPartNumber...batchEnd {
                        let offset = Int64(partNumber - 1) * partSize
                        let length = min(partSize, totalSize - offset)
                        group.addTask {
                            try await uploadPart(uploadId, partNumber, offset, length)
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
                if totalSize > 0 {
                    onProgress?(Double(bytesUploaded) / Double(totalSize))
                }
                nextPartNumber = batchEnd + 1
            }

            collected.sort { $0.partNumber < $1.partNumber }
            try await completeMultipartUpload(key: key, uploadId: uploadId, parts: collected)
            activeMultipartUploads.remove(handle)
            onProgress?(1.0)
        } catch {
            activeMultipartUploads.remove(handle)
            try? await abortMultipartUpload(key: key, uploadId: uploadId)
            throw error
        }
    }

    private func multipartUpload(localURL: URL, key: String, size: Int64, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await runMultipartTransfer(
            key: key,
            totalSize: size,
            respectCancellation: respectTaskCancellation,
            onProgress: onProgress
        ) { [self] uploadId, partNumber, offset, length in
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

    private func createMultipartUpload(key: String) async throws -> String {
        let url = try makeURL(key: key, query: [("uploads", "")])
        let request = signedRequest(
            method: "POST",
            url: url,
            additionalHeaders: ["Content-Type": "application/octet-stream"],
            bodyHash: .empty
        )
        let (data, _) = try await performMetadata(request)
        guard let uploadId = S3SimpleXMLValueParser(target: "UploadId").parse(data: data),
              !uploadId.isEmpty else {
            throw Self.internalError("Missing UploadId in CreateMultipartUpload response")
        }
        return uploadId
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
        let request = signedRequest(method: "PUT", url: url, bodyHash: .unsigned)
        let (_, http) = try await performTransfer(request, from: partData)
        let etag = http.value(forHTTPHeaderField: "ETag") ?? ""
        if etag.isEmpty {
            throw Self.internalError("Missing ETag header on UploadPart response (part \(partNumber))")
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
        // Large-upload assembly can be slow with no byte progress; use the generous server-processing window.
        let (data, _) = try await performTransfer(request, from: body, timeouts: Self.serverProcessingStallTimeouts)
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
        try await download(remotePath: remotePath, localURL: localURL, onProgress: nil)
    }

    func download(remotePath: String, localURL: URL, onProgress: ((Double) -> Void)?) async throws {
        let key = key(forPath: remotePath)
        if key.isEmpty {
            throw RemoteStorageClientError.invalidConfiguration
        }
        let url = try makeURL(key: key, query: [])
        let request = signedRequest(method: "GET", url: url, bodyHash: .empty)
        let (tempURL, _) = try await performTransferDownload(request, onProgress: onProgress)
        defer { try? FileManager.default.removeItem(at: tempURL) }
        let parent = localURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
        if FileManager.default.fileExists(atPath: localURL.path) {
            try FileManager.default.removeItem(at: localURL)
        }
        try FileManager.default.moveItem(at: tempURL, to: localURL)
        onProgress?(1.0)
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
        let (data, _) = try await performMetadata(request)
        try throwIfEmbeddedError(method: "PUT", url: destinationURL, body: data)
    }

    private func multipartCopy(sourceKey: String, destinationKey: String, sourceSize: Int64) async throws {
        try await runMultipartTransfer(
            key: destinationKey,
            totalSize: sourceSize,
            respectCancellation: true,
            onProgress: nil
        ) { [self] uploadId, partNumber, offset, length in
            try await uploadOneCopyPart(
                sourceKey: sourceKey,
                destinationKey: destinationKey,
                uploadId: uploadId,
                partNumber: partNumber,
                rangeStart: offset,
                rangeEnd: offset + length - 1
            )
        }
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
        // Large server-side part copy can be slow with no byte progress; use the generous server-processing window.
        let (data, _) = try await performTransferData(request, timeouts: Self.serverProcessingStallTimeouts)
        try throwIfEmbeddedError(method: "PUT", url: url, body: data)
        guard let etag = S3SimpleXMLValueParser(target: "ETag").parse(data: data),
              !etag.isEmpty else {
            throw Self.internalError("Missing ETag in UploadPartCopy response (part \(partNumber))")
        }
        return UploadedPart(partNumber: partNumber, etag: etag, size: rangeEnd - rangeStart + 1)
    }

    // MARK: - URL building

    nonisolated func makeURL(key: String, query: [(String, String)]) throws -> URL {
        var components = URLComponents()
        components.scheme = effectiveScheme
        guard let endpoint = RemoteHostEndpoint.representation(config.endpointHost) else {
            throw RemoteStorageClientError.invalidConfiguration
        }

        if config.usePathStyle {
            components.percentEncodedHost = endpoint.urlAuthority
            let bucketSegment = "/" + Self.percentEncodeURIComponent(config.bucket)
            if key.isEmpty {
                components.percentEncodedPath = bucketSegment
            } else {
                components.percentEncodedPath = bucketSegment + "/" + Self.percentEncodePath(key)
            }
        } else {
            guard !endpoint.isIPLiteral,
                  let virtualHost = RemoteHostEndpoint.representation("\(config.bucket).\(endpoint.socketHost)") else {
                throw RemoteStorageClientError.invalidConfiguration
            }
            components.percentEncodedHost = virtualHost.urlAuthority
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
        Self.effectiveSigningRegion(userInput: config.region, host: config.endpointHost)
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
        if !trimmed.contains("://"),
           let endpoint = RemoteHostEndpoint.representation(trimmed),
           endpoint.socketHost.contains(":") {
            return ("https", endpoint.socketHost, 443)
        }
        let normalized = trimmed.contains("://") ? trimmed : "https://" + trimmed
        guard let url = URL(string: normalized),
              let parsedHost = url.host,
              let host = RemoteHostEndpoint.socketHost(parsedHost) else { return nil }
        let scheme = (url.scheme ?? "https").lowercased()
        if scheme != "http", scheme != "https" { return nil }
        let port = url.port ?? (scheme == "http" ? 80 : 443)
        return (scheme, host, port)
    }

    nonisolated static func defaultPathStyle(forHost host: String) -> Bool {
        let canonicalHost = RemoteHostIdentity.canonical(host)
        if canonicalHost.hasSuffix(".amazonaws.com") { return false }
        if canonicalHost.hasSuffix(".cloudflarestorage.com") { return false }
        if canonicalHost.hasSuffix(".backblazeb2.com") { return false }
        if canonicalHost.hasSuffix(".digitaloceanspaces.com") { return false }
        if canonicalHost.hasSuffix(".wasabisys.com") { return false }
        return true
    }

    nonisolated static func resolveRegion(userInput: String, host: String) -> String {
        let trimmed = userInput.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmed.isEmpty { return trimmed }
        return defaultRegion(forHost: host) ?? ""
    }

    nonisolated static func effectiveSigningRegion(userInput: String, host: String) -> String {
        let resolved = resolveRegion(userInput: userInput, host: host)
        return resolved.isEmpty ? "us-east-1" : resolved
    }

    nonisolated static func defaultRegion(forHost host: String) -> String? {
        let canonicalHost = RemoteHostIdentity.canonical(host)
        if canonicalHost.hasSuffix(".r2.cloudflarestorage.com") { return "auto" }
        if let region = extractMiddleSegment(host: canonicalHost, prefix: "s3.", suffix: ".amazonaws.com") {
            return region
        }
        if let region = extractMiddleSegment(host: canonicalHost, prefix: "s3.", suffix: ".backblazeb2.com") {
            return region
        }
        if let region = extractMiddleSegment(host: canonicalHost, prefix: "s3.", suffix: ".wasabisys.com") {
            return region
        }
        if canonicalHost.hasSuffix(".digitaloceanspaces.com") {
            let trimmed = String(canonicalHost.dropLast(".digitaloceanspaces.com".count))
            if !trimmed.isEmpty, !trimmed.contains(".") {
                return trimmed
            }
        }
        return nil
    }

    nonisolated private static func extractMiddleSegment(host: String, prefix: String, suffix: String) -> String? {
        guard host.hasPrefix(prefix), host.hasSuffix(suffix), host.count > prefix.count + suffix.count else {
            return nil
        }
        let middle = host.dropFirst(prefix.count).dropLast(suffix.count)
        if middle.isEmpty || middle.contains(".") { return nil }
        return String(middle)
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
        // URLSession derives host from URL; setting it on the request would conflict.
        for (key, value) in signed.headers where key != "host" {
            request.setValue(value, forHTTPHeaderField: key)
        }
        return request
    }

    // MARK: - Request execution

    private func performMetadata(_ request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        let (data, response) = try await metadataTasks.data(for: request, in: session)
        return try validateResponse(request: request, data: data, response: response)
    }

    private func performMetadata(_ request: URLRequest, from body: Data) async throws -> (Data, HTTPURLResponse) {
        let (data, response) = try await metadataTasks.upload(for: request, from: body, in: session)
        return try validateResponse(request: request, data: data, response: response)
    }

    private func performTransfer(_ request: URLRequest, from body: Data, timeouts: URLSessionStallWatchdog.Timeouts? = nil) async throws -> (Data, HTTPURLResponse) {
        let (data, response) = try await URLSessionStallWatchdog.runUpload(
            session: transferSession, delegate: transferDelegate, registry: transferTasks, request: request,
            body: .data(body), onProgress: nil, timeouts: timeouts ?? Self.transferStallTimeouts,
            makeStallError: { stall, timeout, _, _ in Self.makeStallError(stall, timeout: timeout) }
        )
        return try validateResponse(request: request, data: data, response: response)
    }

    private func performTransfer(_ request: URLRequest, fromFile fileURL: URL) async throws -> (Data, HTTPURLResponse) {
        let (data, response) = try await URLSessionStallWatchdog.runUpload(
            session: transferSession, delegate: transferDelegate, registry: transferTasks, request: request,
            body: .file(fileURL), onProgress: nil, timeouts: Self.transferStallTimeouts,
            makeStallError: { stall, timeout, _, _ in Self.makeStallError(stall, timeout: timeout) }
        )
        return try validateResponse(request: request, data: data, response: response)
    }

    private func performTransferData(_ request: URLRequest, timeouts: URLSessionStallWatchdog.Timeouts? = nil) async throws -> (Data, HTTPURLResponse) {
        let (data, response) = try await URLSessionStallWatchdog.runData(
            session: transferSession, registry: transferTasks, request: request,
            timeouts: timeouts ?? Self.transferStallTimeouts,
            makeStallError: { stall, timeout, _, _ in Self.makeStallError(stall, timeout: timeout) }
        )
        return try validateResponse(request: request, data: data, response: response)
    }

    private func performTransferDownload(_ request: URLRequest, onProgress: ((Double) -> Void)? = nil) async throws -> (URL, HTTPURLResponse) {
        let (tempURL, http) = try await URLSessionStallWatchdog.runDownload(
            session: transferSession, registry: transferTasks, request: request,
            onProgress: onProgress, timeouts: Self.transferStallTimeouts,
            makeStallError: { stall, timeout, _, _ in Self.makeStallError(stall, timeout: timeout) }
        )
        if !(200 ..< 300).contains(http.statusCode) {
            let body = (try? Data(contentsOf: tempURL)) ?? Data()
            try? FileManager.default.removeItem(at: tempURL)
            throw makeServerError(method: request.httpMethod ?? "?", url: request.url, statusCode: http.statusCode, body: body)
        }
        return (tempURL, http)
    }

    nonisolated private static func makeStallError(_ stall: URLSessionStallWatchdog.Stall, timeout _: TimeInterval) -> Error {
        let code: Int
        switch stall {
        case .uploadBody: code = uploadStalledErrorCode
        case .uploadResponse: code = uploadResponseTimeoutErrorCode
        case .download: code = downloadStalledErrorCode
        }
        return NSError(domain: errorDomain, code: code, userInfo: [
            NSLocalizedDescriptionKey: String(localized: "s3.error.reason.timeout")
        ])
    }

    // A watchdog-detected stalled transfer (dead/half-open socket); RemoteFaultLite treats it as retryable so the
    // worker/restore reconnects rather than failing the asset.
    nonisolated static func isStalledTransferTimeout(_ error: Error) -> Bool {
        containsErrorCode(in: error, codes: [uploadStalledErrorCode, uploadResponseTimeoutErrorCode, downloadStalledErrorCode])
    }

    nonisolated private static func containsErrorCode(in error: Error, codes: Set<Int>) -> Bool {
        if let storage = error as? RemoteStorageClientError, case .underlying(let inner) = storage {
            return containsErrorCode(in: inner, codes: codes)
        }
        let ns = error as NSError
        if ns.domain == errorDomain, codes.contains(ns.code) { return true }
        if let underlying = ns.userInfo[NSUnderlyingErrorKey] as? Error {
            return containsErrorCode(in: underlying, codes: codes)
        }
        return false
    }

    nonisolated private func validateResponse(request: URLRequest, data: Data, response: URLResponse) throws -> (Data, HTTPURLResponse) {
        guard let http = response as? HTTPURLResponse else {
            throw Self.internalError("Unexpected response type")
        }
        if !(200 ..< 300).contains(http.statusCode) {
            throw makeServerError(method: request.httpMethod ?? "?", url: request.url, statusCode: http.statusCode, body: data)
        }
        return (data, http)
    }

    nonisolated private static func internalError(_ message: String) -> RemoteStorageClientError {
        .underlying(NSError(
            domain: errorDomain,
            code: -1,
            userInfo: [NSLocalizedDescriptionKey: message]
        ))
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

    nonisolated private static func isConditionalCreateCollision(_ error: Error) -> Bool {
        if let storage = error as? RemoteStorageClientError, case .underlying(let inner) = storage {
            return isConditionalCreateCollision(inner)
        }
        let ns = error as NSError
        guard ns.domain == errorDomain else { return false }
        if ns.code == 409 || ns.code == 412 { return true }
        if let serverCode = ns.userInfo[S3ErrorClassifier.userInfoServerCodeKey] as? String {
            return serverCode == "ConditionalRequestConflict" || serverCode == "PreconditionFailed"
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

private final class S3VerificationCleanupRegistry: @unchecked Sendable {
    struct Registration: Hashable, Sendable {
        fileprivate let id: UUID
    }

    private let lock = NSLock()
    private var coordinators: [UUID: RemoteProbeCleanupCoordinator] = [:]

    func register(_ coordinator: RemoteProbeCleanupCoordinator) -> Registration {
        let registration = Registration(id: UUID())
        lock.withLock { coordinators[registration.id] = coordinator }
        return registration
    }

    func unregister(_ registration: Registration) {
        _ = lock.withLock { coordinators.removeValue(forKey: registration.id) }
    }

    func scheduleAllDelayedConfirmations() {
        let active = lock.withLock { Array(coordinators.values) }
        active.forEach { $0.schedule(.delayedConfirmation) }
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

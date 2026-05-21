import Foundation

struct RetentionManifestWriteResult: Sendable, Equatable {
    enum Outcome: Sendable, Equatable {
        case wroteVerified
        case alreadyExistedSameBytes
    }

    let outcome: Outcome
    let filename: String
    let path: String
    let manifest: RetentionManifest
}

struct RetentionManifestLoadResult: Sendable, Equatable {
    let valid: [RetentionManifest]
    let invalid: [InvalidRetentionManifestEntry]
    let ignoredFilenameCount: Int
}

struct RetentionManifestBarrierLoadResult: Sendable, Equatable {
    let valid: [RetentionManifest]
    let invalid: [InvalidRetentionManifestEntry]
    let barrierSet: RetentionBarrierSet
    let isComplete: Bool
}

struct InvalidRetentionManifestEntry: Sendable, Equatable {
    let filename: String
    let reason: InvalidRetentionManifestReason
}

enum InvalidRetentionManifestReason: Sendable, Equatable, Hashable {
    case filenameMalformed
    case bodyDecodeFailed
    case filenameBodyMismatch
    case foreignRepoID(String)
    case monthMismatch
    case vanishedDuringRead
}

enum RetentionManifestStoreError: Error, Equatable {
    case collisionDifferentBytes(filename: String)
    case readbackMismatch(filename: String)
    case decodeRoundtripMismatch(filename: String)
    case filenameBodyMismatch(filename: String)
}

struct RetentionManifestRemoteStore: Sendable {
    let client: any RemoteStorageClientProtocol
    let basePath: String

    init(client: any RemoteStorageClientProtocol, basePath: String) {
        self.client = wrapIfSerial(client)
        self.basePath = basePath
    }

    func writeVerified(
        _ manifest: RetentionManifest,
        respectTaskCancellation: Bool
    ) async throws -> RetentionManifestWriteResult {
        let dir = RepoLayout.retentionDirectoryPath(base: basePath)
        let filename = RetentionManifestStore.filename(for: manifest.ref)
        let path = RepoLayout.retentionManifestPath(base: basePath, ref: manifest.ref)
        do {
            try await client.createDirectory(path: dir)
            let existedBefore = try await metadataExists(path: path)
            let localURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("retention-manifest-\(UUID().uuidString).json")
            defer { try? FileManager.default.removeItem(at: localURL) }
            try RetentionManifestStore.encode(manifest).write(to: localURL, options: .atomic)

            let outcome = try await MetadataCreateGate.createWithStagingFallbackOutcome(
                client: client,
                localURL: localURL,
                remotePath: path,
                respectTaskCancellation: respectTaskCancellation,
                finalizationPolicy: .allowBestEffort
            )
            if case .alreadyExists = outcome.result {
                guard try await verifyMatchesLocal(remotePath: path, localURL: localURL) else {
                    throw RetentionManifestStoreError.collisionDifferentBytes(filename: filename)
                }
                try await verifyDecodeRoundtrip(remotePath: path, filename: filename, manifest: manifest)
                return RetentionManifestWriteResult(
                    outcome: .alreadyExistedSameBytes,
                    filename: filename,
                    path: path,
                    manifest: manifest
                )
            }
            if outcome.verification != .verifiedLocalBytes {
                guard try await verifyMatchesLocal(remotePath: path, localURL: localURL) else {
                    throw RetentionManifestStoreError.readbackMismatch(filename: filename)
                }
            }
            try await verifyDecodeRoundtrip(remotePath: path, filename: filename, manifest: manifest)
            return RetentionManifestWriteResult(
                outcome: existedBefore ? .alreadyExistedSameBytes : .wroteVerified,
                filename: filename,
                path: path,
                manifest: manifest
            )
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    func loadManifests(
        expectedRepoID: String?,
        month: LibraryMonthKey?
    ) async throws -> RetentionManifestLoadResult {
        let canonicalExpectedRepoID = expectedRepoID.flatMap {
            UUID(uuidString: $0)?.uuidString.lowercased()
        } ?? expectedRepoID?.lowercased()
        let dir = RepoLayout.retentionDirectoryPath(base: basePath)
        let entries: [RemoteStorageEntry]
        do {
            entries = try await client.list(path: dir)
        } catch {
            if isStorageNotFoundError(error) {
                return RetentionManifestLoadResult(valid: [], invalid: [], ignoredFilenameCount: 0)
            }
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }

        var valid: [RetentionManifest] = []
        var invalid: [InvalidRetentionManifestEntry] = []
        var ignoredFilenameCount = 0

        for entry in entries {
            if entry.isDirectory {
                if let parsedRef = RetentionManifestStore.parseFilename(entry.name) {
                    let matchesMonth = month.map { parsedRef.month == $0 } ?? true
                    if matchesMonth {
                        invalid.append(InvalidRetentionManifestEntry(filename: entry.name, reason: .bodyDecodeFailed))
                    }
                } else if entry.name.hasSuffix(".json") {
                    invalid.append(InvalidRetentionManifestEntry(filename: entry.name, reason: .filenameMalformed))
                }
                continue
            }
            guard let parsedRef = RetentionManifestStore.parseFilename(entry.name) else {
                if entry.name.hasSuffix(".json") {
                    invalid.append(InvalidRetentionManifestEntry(filename: entry.name, reason: .filenameMalformed))
                } else {
                    ignoredFilenameCount += 1
                }
                continue
            }
            if let month, parsedRef.month != month {
                continue
            }

            let remotePath = RepoLayout.normalize(joining: [dir, entry.name])
            let temp = FileManager.default.temporaryDirectory
                .appendingPathComponent("retention-manifest-load-\(UUID().uuidString).json")
            defer { try? FileManager.default.removeItem(at: temp) }
            let data: Data
            do {
                try await client.download(remotePath: remotePath, localURL: temp)
                data = try Data(contentsOf: temp)
            } catch {
                if isStorageNotFoundError(error) {
                    invalid.append(InvalidRetentionManifestEntry(filename: entry.name, reason: .vanishedDuringRead))
                    continue
                }
                if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
                throw error
            }

            let manifest: RetentionManifest
            do {
                manifest = try RetentionManifestStore.decode(data)
            } catch {
                invalid.append(InvalidRetentionManifestEntry(filename: entry.name, reason: .bodyDecodeFailed))
                continue
            }
            if let month, manifest.month != month {
                invalid.append(InvalidRetentionManifestEntry(filename: entry.name, reason: .monthMismatch))
            } else if parsedRef != manifest.ref {
                invalid.append(InvalidRetentionManifestEntry(filename: entry.name, reason: .filenameBodyMismatch))
            } else if let canonicalExpectedRepoID, manifest.repoID != canonicalExpectedRepoID {
                invalid.append(InvalidRetentionManifestEntry(filename: entry.name, reason: .foreignRepoID(manifest.repoID)))
            } else {
                valid.append(manifest)
            }
        }

        return RetentionManifestLoadResult(
            valid: valid,
            invalid: invalid,
            ignoredFilenameCount: ignoredFilenameCount
        )
    }

    func loadBarrierSet(
        expectedRepoID: String,
        month: LibraryMonthKey
    ) async throws -> RetentionManifestBarrierLoadResult {
        let result = try await loadManifests(expectedRepoID: expectedRepoID, month: month)
        return RetentionManifestBarrierLoadResult(
            valid: result.valid,
            invalid: result.invalid,
            barrierSet: RetentionBarrierSet.unsuperseded(manifests: result.valid),
            isComplete: result.invalid.isEmpty
        )
    }

    private func metadataExists(path: String) async throws -> Bool {
        do {
            return try await client.metadata(path: path) != nil
        } catch {
            if isStorageNotFoundError(error) { return false }
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    private func verifyMatchesLocal(remotePath: String, localURL: URL) async throws -> Bool {
        do {
            return try await MetadataCreateGate.verifyMatchesLocalWithRetries(
                client: client,
                remotePath: remotePath,
                localURL: localURL
            )
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }

    private func verifyDecodeRoundtrip(
        remotePath: String,
        filename: String,
        manifest: RetentionManifest
    ) async throws {
        let temp = FileManager.default.temporaryDirectory
            .appendingPathComponent("retention-manifest-readback-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: temp) }
        do {
            try await client.download(remotePath: remotePath, localURL: temp)
            let data = try Data(contentsOf: temp)
            let decoded: RetentionManifest
            do {
                decoded = try RetentionManifestStore.decode(data)
            } catch {
                throw RetentionManifestStoreError.decodeRoundtripMismatch(filename: filename)
            }
            guard decoded == manifest else {
                throw RetentionManifestStoreError.decodeRoundtripMismatch(filename: filename)
            }
            guard RetentionManifestStore.parseFilename(filename) == decoded.ref else {
                throw RetentionManifestStoreError.filenameBodyMismatch(filename: filename)
            }
        } catch {
            if RemoteWriteClassifier.isCancellation(error) { throw CancellationError() }
            throw error
        }
    }
}

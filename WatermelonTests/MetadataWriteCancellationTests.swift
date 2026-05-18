import XCTest
@testable import Watermelon

final class MetadataWriteCancellationTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "11111111-1111-1111-1111-111111111111"
    private let repoID = "repo-A"
    private let month = LibraryMonthKey(year: 2026, month: 5)

    func testMetadataCreateGate_publicWriteSurfaceNormalizesCancellationShapes() async throws {
        for error in cancellationShapes() {
            let client = OperationFailureClient(error: error)
            let payload = try makeTempFile("payload")
            defer { try? FileManager.default.removeItem(at: payload) }

            await assertThrowsCancellation {
                _ = try await MetadataCreateGate.createWithStagingFallbackOutcome(
                    client: client,
                    localURL: payload,
                    remotePath: "\(self.basePath)/.watermelon/test.json",
                    respectTaskCancellation: false
                )
            }
        }
    }

    func testCommitLogWriter_publicWriteSurfaceNormalizesCancellationShapes() async throws {
        for error in cancellationShapes() {
            let writer = CommitLogWriter(client: OperationFailureClient(error: error), basePath: basePath)
            await assertThrowsCancellation {
                _ = try await writer.write(
                    header: self.commitHeader(),
                    ops: [self.sampleOp()],
                    month: self.month,
                    respectTaskCancellation: false
                )
            }
        }
    }

    func testSnapshotWriter_publicWriteSurfaceNormalizesCancellationShapes() async throws {
        for error in cancellationShapes() {
            let writer = SnapshotWriter(client: OperationFailureClient(error: error), basePath: basePath)
            await assertThrowsCancellation {
                _ = try await writer.write(
                    header: self.snapshotHeader(),
                    assets: [],
                    resources: [],
                    assetResources: [],
                    deletedKeys: [],
                    month: self.month,
                    lamport: 1,
                    runID: "run",
                    respectTaskCancellation: false
                )
            }
        }
    }

    func testMigrationMarkerStore_publicWriteSurfaceNormalizesCancellationShapes() async throws {
        for error in cancellationShapes() {
            let store = MigrationMarkerStore(client: OperationFailureClient(error: error), basePath: basePath)
            await assertThrowsCancellation {
                try await store.writePhase(writerID: self.writerID, phase: .phase1, runID: "run")
            }
        }
    }

    func testVersionManifestStore_publicWriteSurfaceNormalizesCancellationShapes() async throws {
        for error in cancellationShapes() {
            let store = VersionManifestStore(client: OperationFailureClient(error: error), basePath: basePath)
            await assertThrowsCancellation {
                try await store.writeIfAbsent(writerID: self.writerID)
            }
        }
    }

    func testRepoBootstrapIdentityFinalization_publicWriteSurfaceNormalizesCancellationShapes() async throws {
        for error in cancellationShapes() {
            let bootstrap = RepoBootstrap(client: OperationFailureClient(error: error), basePath: basePath)
            await assertThrowsCancellation {
                _ = try await bootstrap.ensureIdentityFinalization(repoID: self.repoID, writerID: self.writerID)
            }
        }
    }

    func testRepoBootstrapEnsureRepoJSON_publicWriteSurfaceNormalizesCancellationShapes() async throws {
        for error in cancellationShapes() {
            let client = OperationFailureClient(
                error: error,
                failingOperation: .atomicCreate,
                failingRemotePath: RepoLayout.repoFilePath(base: basePath)
            )
            let bootstrap = RepoBootstrap(client: client, basePath: basePath)
            await assertThrowsCancellation {
                _ = try await bootstrap.ensureRepoJSON(repoID: self.repoID, writerID: self.writerID)
            }
        }
    }

    func testIdentityClaimStore_publicWriteSurfaceNormalizesCancellationShapes() async throws {
        for error in cancellationShapes() {
            let store = IdentityClaimStore(client: OperationFailureClient(error: error), basePath: basePath)
            await assertThrowsCancellation {
                try await store.writeOwnClaim(repoID: self.repoID, writerID: self.writerID, createdAtMs: 1)
            }
        }
    }

    func testVersionManifestStore_preflightCancellationShapesNormalize() async throws {
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        for error in cancellationShapes() {
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .createDirectory)
                try await VersionManifestStore(client: client, basePath: self.basePath).writeIfAbsent(writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .metadata, failingRemotePath: versionPath)
                try await VersionManifestStore(client: client, basePath: self.basePath).writeIfAbsent(writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .download, failingRemotePath: versionPath)
                await client.injectFile(
                    path: versionPath,
                    data: try VersionManifestWire(
                        formatVersion: RepoLayout.formatVersion,
                        minAppVersion: RepoLayout.minAppVersionPlaceholder,
                        createdAtMs: 1,
                        createdByWriter: self.writerID
                    ).encode()
                )
                try await VersionManifestStore(client: client, basePath: self.basePath).writeIfAbsent(writerID: self.writerID)
            }
        }
    }

    func testIdentityClaimStore_preflightCancellationShapesNormalize() async throws {
        let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
        for error in cancellationShapes() {
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .metadata, failingRemotePath: claimPath)
                try await IdentityClaimStore(client: client, basePath: self.basePath)
                    .writeOwnClaim(repoID: self.repoID, writerID: self.writerID, createdAtMs: 1)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .download, failingRemotePath: claimPath)
                await client.injectFile(
                    path: claimPath,
                    data: try IdentityClaimWire(repoID: self.repoID, createdAtMs: 1, writerID: self.writerID).encode()
                )
                try await IdentityClaimStore(client: client, basePath: self.basePath)
                    .writeOwnClaim(repoID: self.repoID, writerID: self.writerID, createdAtMs: 1)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .delete, failingRemotePath: claimPath)
                await client.injectFile(path: claimPath, data: Data("not-json".utf8))
                try await IdentityClaimStore(client: client, basePath: self.basePath)
                    .writeOwnClaim(repoID: self.repoID, writerID: self.writerID, createdAtMs: 1)
            }
        }
    }

    func testMigrationMarkerStore_preflightCancellationShapesNormalize() async throws {
        let migrationsDir = RepoLayout.migrationsDirectoryPath(base: basePath)
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        for error in cancellationShapes() {
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .createDirectory, failingRemotePath: migrationsDir)
                try await MigrationMarkerStore(client: client, basePath: self.basePath)
                    .writePhase(writerID: self.writerID, phase: .phase1, runID: "run")
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .list, failingRemotePath: migrationsDir)
                try await MigrationMarkerStore(client: client, basePath: self.basePath)
                    .writePhase(writerID: self.writerID, phase: .phase1, runID: "run")
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .metadata, failingRemotePath: canonical)
                try await MigrationMarkerStore(client: client, basePath: self.basePath)
                    .writePhase(writerID: self.writerID, phase: .phase1, runID: "run")
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .download, failingRemotePath: canonical)
                await client.injectFile(path: canonical, data: self.migrationMarkerBytes(phase: 1))
                try await MigrationMarkerStore(client: client, basePath: self.basePath)
                    .writePhase(writerID: self.writerID, phase: .phase1, runID: "run")
            }
        }
    }

    func testRepoBootstrap_publicPreflightCancellationShapesNormalize() async throws {
        let watermelonDir = RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory])
        let identityDir = RepoLayout.identityDirectoryPath(base: basePath)
        let finalizationPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        let repoPath = RepoLayout.repoFilePath(base: basePath)
        let claimPath = RepoLayout.identityClaimPath(base: basePath, writerID: writerID)
        for error in cancellationShapes() {
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .createDirectory, failingRemotePath: watermelonDir)
                _ = try await RepoBootstrap(client: client, basePath: self.basePath)
                    .ensureIdentityFinalization(repoID: self.repoID, writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .metadata, failingRemotePath: finalizationPath)
                _ = try await RepoBootstrap(client: client, basePath: self.basePath)
                    .ensureIdentityFinalization(repoID: self.repoID, writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .createDirectory, failingRemotePath: watermelonDir)
                _ = try await RepoBootstrap(client: client, basePath: self.basePath)
                    .ensureRepoJSON(repoID: self.repoID, writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .metadata, failingRemotePath: finalizationPath)
                _ = try await RepoBootstrap(client: client, basePath: self.basePath)
                    .ensureRepoJSON(repoID: self.repoID, writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .list, failingRemotePath: identityDir)
                _ = try await RepoBootstrap(client: client, basePath: self.basePath)
                    .ensureRepoJSON(repoID: self.repoID, writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .download, failingRemotePath: repoPath)
                await client.injectFile(
                    path: repoPath,
                    data: try RepoCacheWire(repoID: self.repoID, createdAtMs: 1, createdByWriter: self.writerID).encode()
                )
                _ = try await RepoBootstrap(client: client, basePath: self.basePath)
                    .ensureRepoJSON(repoID: self.repoID, writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .download, failingRemotePath: claimPath)
                await client.injectFile(path: claimPath, data: Data())
                _ = try await RepoBootstrap(client: client, basePath: self.basePath)
                    .ensureRepoJSON(repoID: self.repoID, writerID: self.writerID)
            }
            await assertThrowsCancellation {
                let client = OperationFailureClient(error: error, failingOperation: .delete, failingRemotePath: claimPath)
                await client.injectFile(path: claimPath, data: Data())
                _ = try await RepoBootstrap(client: client, basePath: self.basePath)
                    .ensureRepoJSON(repoID: self.repoID, writerID: self.writerID)
            }
        }
    }

    private func cancellationShapes() -> [Error] {
        let url = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        return [
            CancellationError(),
            url,
            RemoteStorageClientError.underlying(url),
            NSError(domain: "outer", code: 1, userInfo: [NSUnderlyingErrorKey: url])
        ]
    }

    private func assertThrowsCancellation(
        _ body: () async throws -> Void,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        do {
            try await body()
            XCTFail("expected CancellationError", file: file, line: line)
        } catch is CancellationError {
        } catch {
            XCTFail("expected CancellationError, got \(error)", file: file, line: line)
        }
    }

    private func makeTempFile(_ text: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(text.utf8).write(to: url, options: .atomic)
        return url
    }

    private func commitHeader() -> CommitHeader {
        CommitHeader(
            version: CommitHeader.currentVersion,
            repoID: repoID,
            writerID: writerID,
            seq: 1,
            runID: "run",
            scope: CommitHeader.monthScope(month),
            clockMin: 1,
            clockMax: 1,
            bodyKind: CommitHeader.bodyKindPlain
        )
    }

    private func sampleOp() -> CommitOp {
        CommitOp(
            opSeq: 0,
            clock: 1,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.fingerprint(0xAA),
                reason: .userDeleted
            ))
        )
    }

    private func snapshotHeader() -> SnapshotHeader {
        SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: CoveredRanges()
        )
    }

    private func migrationMarkerBytes(phase: Int) -> Data {
        let dict: [String: Any] = [
            "v": 2,
            "writer_id": writerID,
            "phase": phase,
            "started_at_ms": 1,
            "run_id": "run"
        ]
        return try! JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])
    }
}

private enum FailingOperation: Sendable {
    case atomicCreate
    case createDirectory
    case metadata
    case download
    case list
    case delete
}

private actor OperationFailureClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }

    private let inner = InMemoryRemoteStorageClient()
    private let error: Error
    private let failingRemotePath: String?

    private let failingOperation: FailingOperation

    init(error: Error, failingOperation: FailingOperation = .atomicCreate, failingRemotePath: String? = nil) {
        self.error = error
        self.failingOperation = failingOperation
        self.failingRemotePath = failingRemotePath
    }

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        .exclusive
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func injectFile(path: String, data: Data) async {
        await inner.injectFile(path: path, data: data)
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        if shouldFail(.list, path: path) { throw error }
        return try await inner.list(path: path)
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        if shouldFail(.metadata, path: path) { throw error }
        return try await inner.metadata(path: path)
    }
    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try await inner.upload(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        if shouldFail(.atomicCreate, path: remotePath) {
            throw error
        }
        return try await inner.atomicCreate(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }

    func download(remotePath: String, localURL: URL) async throws {
        if shouldFail(.download, path: remotePath) { throw error }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }

    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        if shouldFail(.delete, path: path) { throw error }
        try await inner.delete(path: path)
    }

    func createDirectory(path: String) async throws {
        if shouldFail(.createDirectory, path: path) { throw error }
        try await inner.createDirectory(path: path)
    }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private func shouldFail(_ operation: FailingOperation, path: String) -> Bool {
        failingOperation == operation && (failingRemotePath == nil || failingRemotePath == path)
    }
}

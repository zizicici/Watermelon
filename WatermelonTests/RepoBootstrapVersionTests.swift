import XCTest
@testable import Watermelon

/// Version manifest collision recovery verifies the remote version is
/// compatible. Higher format → back off (`unsupportedRemoteFormat`); read failure
/// → unreadable (never silently treated as compatible).
final class RepoBootstrapVersionTests: XCTestCase {
    private let basePath = "/repo"

    func testFreshRepo_writesVersionAndIdentityFiles() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)

        let resolvedID = try await bootstrap.initializeFreshRepo(writerID: "writer-A")
        XCTAssertFalse(resolvedID.isEmpty)
        let versionExists = await client.hasFile(RepoLayout.versionFilePath(base: basePath))
        let identityExists = await client.hasFile(RepoLayout.identityFinalizationFilePath(base: basePath))
        XCTAssertTrue(versionExists)
        XCTAssertTrue(identityExists)
    }

    func testEnsureVersion_alreadyExistsHigherFormat_throwsHigherFormatVersion() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectVersionJSON(
            client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9", writerID: "future"
        )

        let store = VersionManifestStore(client: client, basePath: basePath)
        do {
            try await store.writeIfAbsent(writerID: "us")
            XCTFail("expected higherFormatVersion")
        } catch RepoBootstrap.VersionConflict.higherFormatVersion(let remote, let local, let minApp) {
            XCTAssertEqual(remote, 99)
            XCTAssertEqual(local, RepoLayout.formatVersion)
            XCTAssertEqual(minApp, "9.9.9")
        }
    }

    func testEnsureVersion_alreadyExistsUnreadable_throwsUnreadable() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: Data("not json at all".utf8))

        let store = VersionManifestStore(client: client, basePath: basePath)
        do {
            try await store.writeIfAbsent(writerID: "us")
            XCTFail("expected unreadable")
        } catch RepoBootstrap.VersionConflict.unreadable {
            // expected
        }
    }

    func testLoadRepoIDStrict_absent_returnsAbsent() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let result = try await bootstrap.loadRepoIDStrict()
        guard case .absent = result else {
            XCTFail("expected .absent, got \(result)")
            return
        }
    }

    /// A just-published finalized marker can be metadata-visible while its download still 404s on a
    /// grace backend; identity resolution must spend the grace budget, not refuse the repo as damaged.
    func testLoadRepoID_finalizedMarkerDownloadVisibilityLag_resolvesAfterGrace() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setReadAfterWriteGrace(30)
        let repoID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
        let wire = RepoIdentityFinalizationWire(
            repoID: repoID,
            formatVersion: RepoLayout.formatVersion,
            createdAtMs: 1_000,
            createdByWriter: "writer-A"
        )
        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        await client.injectFile(path: markerPath, data: try wire.encode())
        await client.injectDownloadError(.notFound, for: markerPath)

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let resolved = try await bootstrap.loadRepoID()
        XCTAssertEqual(resolved, repoID,
                       "metadata-visible finalized marker with a download visibility lag must resolve, not throw damaged")
    }

    /// Past the grace budget, an unreadable finalized marker is genuinely damaged: keep the strict
    /// ioFailure mapping so the open fails closed instead of inventing an identity.
    func testLoadRepoID_finalizedMarkerPersistentDownloadNotFound_throwsIOFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setReadAfterWriteGrace(0.2)
        let wire = RepoIdentityFinalizationWire(
            repoID: "dddddddd-dddd-dddd-dddd-dddddddddddd",
            formatVersion: RepoLayout.formatVersion,
            createdAtMs: 1_000,
            createdByWriter: "writer-A"
        )
        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        await client.injectFile(path: markerPath, data: try wire.encode())
        await client.injectPersistentDownloadError(.notFound, for: markerPath)

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        do {
            _ = try await bootstrap.loadRepoID()
            XCTFail("expected ioFailure after grace exhausts")
        } catch RepoBootstrap.BootstrapError.ioFailure {
            // expected — unrecoverable finalized marker after grace
        }
    }

    /// Mixed metadata/download lag: the first read proves the finalized marker exists, the data-path
    /// GET 404s, and a retry's metadata read then flaps to not-found mid-grace. The tolerant loader
    /// must keep spending the grace budget rather than demote the authoritative finalized identity to
    /// absence (which would fall back to claims / `.absent` and risk a false repoIdentityMismatch).
    func testLoadRepoID_finalizedMarkerMetadataFlapsToAbsentMidGrace_resolves() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let repoID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
        let wire = RepoIdentityFinalizationWire(
            repoID: repoID,
            formatVersion: RepoLayout.formatVersion,
            createdAtMs: 1_000,
            createdByWriter: "writer-A"
        )
        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        await inner.injectFile(path: markerPath, data: try wire.encode())
        // metadata: present, not-found (flap), present; download: 404 once, then served.
        let client = FinalizedIdentityFlapClient(
            inner: inner,
            markerPath: markerPath,
            metadataNilCallIndices: [2],
            downloadNotFoundCallIndices: [1],
            graceSeconds: 3
        )

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        let resolved = try await bootstrap.loadRepoID()
        XCTAssertEqual(resolved, repoID,
                       "a finalized marker whose metadata flaps to absent mid-grace must not demote to absence")
    }

    /// Once the marker is proven and stays unreadable past the grace budget, the loader must fail
    /// closed, never return a bare nil that would silently demote the finalized identity to absence.
    func testLoadRepoID_finalizedMarkerMetadataFlapsAbsentPastGrace_failsClosed() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let wire = RepoIdentityFinalizationWire(
            repoID: "dddddddd-dddd-dddd-dddd-dddddddddddd",
            formatVersion: RepoLayout.formatVersion,
            createdAtMs: 1_000,
            createdByWriter: "writer-A"
        )
        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        await inner.injectFile(path: markerPath, data: try wire.encode())
        let client = FinalizedIdentityFlapClient(
            inner: inner,
            markerPath: markerPath,
            metadataNilFromIndex: 2,
            downloadNotFoundCallIndices: [1],
            graceSeconds: 0.3
        )

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        do {
            _ = try await bootstrap.loadRepoID()
            XCTFail("expected fail-closed throw once the proven finalized marker stays unreadable past grace")
        } catch RepoBootstrap.BootstrapError.ioFailure {
            // expected
        }
    }

    func testEnsureSubdirectories_createDirectoryURLCancel_propagatesAsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectCreateDirectoryURLErrorCancelled(for: RepoLayout.commitsDirectoryPath(base: basePath))

        let bootstrap = RepoBootstrap(client: client, basePath: basePath)
        do {
            try await bootstrap.ensureSubdirectories()
            XCTFail("expected CancellationError from URL-shaped createDirectory cancellation")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }
}

/// Flaps `metadata`/`download` for one marker path to model an eventually-consistent grace backend:
/// metadata can report not-found on chosen call indices and `download` can 404 on chosen call
/// indices, all against a marker whose bytes are present in the inner store.
private actor FinalizedIdentityFlapClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated let graceSeconds: TimeInterval
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { graceSeconds }

    private let inner: InMemoryRemoteStorageClient
    private let markerPath: String
    private let metadataNilCallIndices: Set<Int>
    private let metadataNilFromIndex: Int?
    private let downloadNotFoundCallIndices: Set<Int>
    private var metadataCalls = 0
    private var downloadCalls = 0

    init(
        inner: InMemoryRemoteStorageClient,
        markerPath: String,
        metadataNilCallIndices: Set<Int> = [],
        metadataNilFromIndex: Int? = nil,
        downloadNotFoundCallIndices: Set<Int> = [],
        graceSeconds: TimeInterval
    ) {
        self.inner = inner
        self.markerPath = markerPath
        self.metadataNilCallIndices = metadataNilCallIndices
        self.metadataNilFromIndex = metadataNilFromIndex
        self.downloadNotFoundCallIndices = downloadNotFoundCallIndices
        self.graceSeconds = graceSeconds
    }

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        guard normalize(path) == normalize(markerPath) else { return try await inner.metadata(path: path) }
        metadataCalls += 1
        if metadataNilCallIndices.contains(metadataCalls) { return nil }
        if let from = metadataNilFromIndex, metadataCalls >= from { return nil }
        return try await inner.metadata(path: path)
    }

    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        guard normalize(remotePath) == normalize(markerPath) else {
            try await inner.download(remotePath: remotePath, localURL: localURL)
            return
        }
        downloadCalls += 1
        if downloadNotFoundCallIndices.contains(downloadCalls) {
            throw RemoteStorageClientError.underlying(NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError))
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    nonisolated private func normalize(_ p: String) -> String {
        let trimmed = p.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        let collapsed = trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
        return "/" + collapsed
    }
}

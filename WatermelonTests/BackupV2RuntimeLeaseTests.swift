import XCTest
@testable import Watermelon

final class BackupV2RuntimeLeaseTests: XCTestCase {
    private let basePath = "/repo"
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!
    private var activeLease: BackupV2RuntimeLease?

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDown() async throws {
        if let lease = activeLease {
            await lease.shutdown()
            activeLease = nil
        }
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    // MARK: - Foreground

    func testForegroundRun_success_ownsMetadataClient_andShutdownDisconnectsIt() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        let profile = try insertProfile()
        let stream = BackupEventStream()
        defer { stream.finish() }

        let lease = try await BackupV2RuntimeLease.forForegroundRun(
            client: dataClient,
            profile: profile,
            databaseManager: databaseManager,
            format: RemoteFormatCompatibilityService(),
            eventStream: stream,
            makeMetadataClient: {
                try await metadataClient.connect()
                return metadataClient
            }
        )
        activeLease = lease

        XCTAssertFalse(lease.services.repoID.isEmpty)
        let beforeShutdown = await metadataClient.disconnectCount
        XCTAssertEqual(beforeShutdown, 0)
        await lease.shutdown()
        activeLease = nil
        let afterShutdown = await metadataClient.disconnectCount
        XCTAssertEqual(afterShutdown, 1, "FG lease owns metadata client; shutdown must disconnect")
    }

    func testForegroundRun_buildFailure_compatibilityMapped_disconnectsMetadata() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        try await TestFixtures.injectVersionJSON(dataClient, basePath: basePath)
        await dataClient.injectFile(path: RepoLayout.identityFinalizationFilePath(base: basePath), contents: "{not-json")
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        let profile = try insertProfile()
        let stream = BackupEventStream()
        defer { stream.finish() }

        do {
            _ = try await BackupV2RuntimeLease.forForegroundRun(
                client: dataClient,
                profile: profile,
                databaseManager: databaseManager,
                format: RemoteFormatCompatibilityService(),
                eventStream: stream,
                makeMetadataClient: {
                    try await metadataClient.connect()
                    return metadataClient
                }
            )
            XCTFail("expected damagedV2Repo via BackupCompatibilityError")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected — FG factory maps build failure to BackupCompatibilityError
        }
        let disconnects = await metadataClient.disconnectCount
        XCTAssertGreaterThanOrEqual(disconnects, 1, "FG build failure must disconnect owned metadata client")
    }

    func testForegroundRun_makeMetadataClientFailure_propagatesRawError_noCompatibilityMapping() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        try await dataClient.connect()
        let profile = try insertProfile()
        let stream = BackupEventStream()
        defer { stream.finish() }
        struct ProbeError: Error, Equatable {}

        do {
            _ = try await BackupV2RuntimeLease.forForegroundRun(
                client: dataClient,
                profile: profile,
                databaseManager: databaseManager,
                format: RemoteFormatCompatibilityService(),
                eventStream: stream,
                makeMetadataClient: { throw ProbeError() }
            )
            XCTFail("expected raw ProbeError")
        } catch is ProbeError {
            // expected — raw error propagation (matches current FG pre-mapping behavior)
        } catch {
            XCTFail("expected raw ProbeError, got \(error)")
        }
    }

    func testForegroundRun_makeMetadataClientCancellation_propagatesRawCancellation() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        try await dataClient.connect()
        let profile = try insertProfile()
        let stream = BackupEventStream()
        defer { stream.finish() }

        do {
            _ = try await BackupV2RuntimeLease.forForegroundRun(
                client: dataClient,
                profile: profile,
                databaseManager: databaseManager,
                format: RemoteFormatCompatibilityService(),
                eventStream: stream,
                makeMetadataClient: { throw CancellationError() }
            )
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // expected — cancellation propagates raw, not wrapped in BackupCompatibilityError
        }
    }

    func testForegroundRun_buildCancellation_throwsCancellation_andDisconnectsMetadata() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        try await TestFixtures.injectVersionJSON(dataClient, basePath: basePath)
        try await TestFixtures.injectIdentityFinalization(
            dataClient,
            basePath: basePath,
            repoID: "cccccccc-1111-2222-3333-444444444444"
        )
        await dataClient.injectDownloadCancellation(for: RepoLayout.identityFinalizationFilePath(base: basePath))
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        let profile = try insertProfile()
        let stream = BackupEventStream()
        defer { stream.finish() }

        do {
            _ = try await BackupV2RuntimeLease.forForegroundRun(
                client: dataClient,
                profile: profile,
                databaseManager: databaseManager,
                format: RemoteFormatCompatibilityService(),
                eventStream: stream,
                makeMetadataClient: {
                    try await metadataClient.connect()
                    return metadataClient
                }
            )
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // expected — withCompatibilityMapping translates cancellation to CancellationError()
        }
        let disconnects = await metadataClient.disconnectCount
        XCTAssertGreaterThanOrEqual(disconnects, 1, "FG build cancellation must disconnect owned metadata client")
    }

    // MARK: - Background

    func testBackgroundRun_success_ownsMetadataClient_andShutdownDisconnectsIt() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        let profile = try insertProfile()

        let result = await BackupV2RuntimeLease.forBackgroundRun(
            client: dataClient,
            profile: profile,
            databaseManager: databaseManager,
            makeMetadataClient: {
                try await metadataClient.connect()
                return metadataClient
            }
        )
        guard case .success(let lease) = result else {
            XCTFail("expected success, got \(result)")
            return
        }
        activeLease = lease
        XCTAssertFalse(lease.services.repoID.isEmpty)
        let beforeShutdown = await metadataClient.disconnectCount
        XCTAssertEqual(beforeShutdown, 0)
        await lease.shutdown()
        activeLease = nil
        let afterShutdown = await metadataClient.disconnectCount
        XCTAssertEqual(afterShutdown, 1, "BG lease owns metadata client; shutdown must disconnect")
    }

    func testBackgroundRun_makeMetadataClientFailure_returnsTypedMetadataConnectNonCancellation() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        try await dataClient.connect()
        let profile = try insertProfile()
        struct ProbeError: Error {}

        let result = await BackupV2RuntimeLease.forBackgroundRun(
            client: dataClient,
            profile: profile,
            databaseManager: databaseManager,
            makeMetadataClient: { throw ProbeError() }
        )
        guard case .failure(.metadataConnect(let error)) = result else {
            XCTFail("expected .metadataConnect, got \(result)")
            return
        }
        XCTAssertTrue(error is ProbeError, "metadata-connect error must propagate raw, not wrapped")
        XCTAssertFalse(RemoteWriteClassifier.isCancellation(error))
    }

    func testBackgroundRun_makeMetadataClientCancellation_returnsTypedMetadataConnect() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        try await dataClient.connect()
        let profile = try insertProfile()

        let result = await BackupV2RuntimeLease.forBackgroundRun(
            client: dataClient,
            profile: profile,
            databaseManager: databaseManager,
            makeMetadataClient: { throw CancellationError() }
        )
        guard case .failure(.metadataConnect(let error)) = result else {
            XCTFail("expected .metadataConnect, got \(result)")
            return
        }
        XCTAssertTrue(RemoteWriteClassifier.isCancellation(error),
                      "cancellation must be detectable so caller can map to .cancelled")
    }

    func testBackgroundRun_builderFailure_returnsBuilderOpenWithExpectedKind() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        // V1 sentinel without migration permission → builder throws requiresForegroundMigration.
        await TestFixtures.injectV1ManifestSentinel(dataClient, basePath: basePath, year: 2025, month: 6)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        let profile = try insertProfile()

        let result = await BackupV2RuntimeLease.forBackgroundRun(
            client: dataClient,
            profile: profile,
            databaseManager: databaseManager,
            makeMetadataClient: {
                try await metadataClient.connect()
                return metadataClient
            }
        )
        guard case .failure(.builderOpen(let error)) = result else {
            XCTFail("expected .builderOpen, got \(result)")
            return
        }
        guard case BackupV2RuntimeBuildError.requiresForegroundMigration = error else {
            return XCTFail("expected requiresForegroundMigration, got \(error)")
        }
        let disconnects = await metadataClient.disconnectCount
        XCTAssertGreaterThanOrEqual(disconnects, 1, "BG builder failure must disconnect owned metadata client")
    }

    func testBackgroundRun_builderCancellation_returnsBuilderOpenCancellation_andDisconnectsMetadata() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        try await TestFixtures.injectVersionJSON(dataClient, basePath: basePath)
        try await TestFixtures.injectIdentityFinalization(
            dataClient,
            basePath: basePath,
            repoID: "cccccccc-1111-2222-3333-444444444444"
        )
        await dataClient.injectDownloadCancellation(for: RepoLayout.identityFinalizationFilePath(base: basePath))
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        let profile = try insertProfile()

        let result = await BackupV2RuntimeLease.forBackgroundRun(
            client: dataClient,
            profile: profile,
            databaseManager: databaseManager,
            makeMetadataClient: {
                try await metadataClient.connect()
                return metadataClient
            }
        )
        guard case .failure(.builderOpen(let error)) = result else {
            XCTFail("expected .builderOpen, got \(result)")
            return
        }
        XCTAssertTrue(RemoteWriteClassifier.isCancellation(error), "expected cancellation, got \(error)")
        let disconnects = await metadataClient.disconnectCount
        XCTAssertGreaterThanOrEqual(disconnects, 1, "BG builder cancellation must disconnect owned metadata client")
    }

    func testBackgroundRun_builderUnknownError_returnsBuilderOpenOther_andDisconnectsMetadata() async throws {
        // A raw metadata error injected at the version-file read path escapes
        // BackupV2RuntimeBuilder.build without being translated to a BuildError /
        // BootstrapError / VersionConflict / BackupCompatibilityError, so the runner's
        // disposition mapping falls through to .skippedOther.
        struct ProbeError: Error {}
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        // Marker directory present so format inspection actually reads version.json.
        try await dataClient.createDirectory(
            path: RepoLayout.normalize(joining: [basePath, RepoLayout.watermelonDirectory])
        )
        await dataClient.injectRawMetadataError(ProbeError(), for: RepoLayout.versionFilePath(base: basePath))
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        let profile = try insertProfile()

        let result = await BackupV2RuntimeLease.forBackgroundRun(
            client: dataClient,
            profile: profile,
            databaseManager: databaseManager,
            makeMetadataClient: {
                try await metadataClient.connect()
                return metadataClient
            }
        )
        guard case .failure(.builderOpen(let error)) = result else {
            XCTFail("expected .builderOpen, got \(result)")
            return
        }
        XCTAssertEqual(
            BackgroundBackupRunner.runtimeOpenFailureDisposition(error),
            .skippedOther,
            "non-translated builder error must map to .skippedOther"
        )
        let disconnects = await metadataClient.disconnectCount
        XCTAssertGreaterThanOrEqual(disconnects, 1, "BG builder .other failure must disconnect owned metadata client")
    }

    // MARK: - Verify

    func testVerifyMonth_success_borrowsMetadataClient_andShutdownDoesNotDisconnect() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        // Seed an existing V2 repo with matching local state so .openExistingV2 fires.
        let canonicalRepoID = "bbbbbbbb-1111-2222-3333-444444444444"
        try await TestFixtures.injectIdentityFinalization(dataClient, basePath: basePath, repoID: canonicalRepoID)
        try await TestFixtures.injectVersionJSON(dataClient, basePath: basePath)
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()  // caller owns the connect for borrowed client
        let profile = try insertProfile()
        let identity = RepoIdentity(database: databaseManager)
        let writerID = try await identity.lazyEnsureWriterID(profileID: profile.id!)
        _ = try await identity.lazyEnsureRepoState(profileID: profile.id!, repoID: canonicalRepoID, writerID: writerID)

        let lease = try await BackupV2RuntimeLease.forVerifyMonth(
            client: dataClient,
            borrowedMetadataClient: metadataClient,
            profile: profile,
            databaseManager: databaseManager,
            format: RemoteFormatCompatibilityService()
        )
        activeLease = lease

        XCTAssertEqual(lease.services.repoID, canonicalRepoID)
        let beforeShutdown = await metadataClient.disconnectCount
        XCTAssertEqual(beforeShutdown, 0)
        await lease.shutdown()
        activeLease = nil
        let afterShutdown = await metadataClient.disconnectCount
        XCTAssertEqual(afterShutdown, 0,
                       "verify lease borrows metadata client; shutdown must NOT disconnect")
    }

    func testVerifyMonth_buildFailure_compatibilityMapped_doesNotDisconnectBorrowedClient() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        try await TestFixtures.injectVersionJSON(dataClient, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9")
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        do {
            _ = try await BackupV2RuntimeLease.forVerifyMonth(
                client: dataClient,
                borrowedMetadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected BackupCompatibilityError.remoteFormatUnsupported")
        } catch BackupCompatibilityError.remoteFormatUnsupported(let minApp) {
            XCTAssertEqual(minApp, "9.9.9")
        }
        let disconnects = await metadataClient.disconnectCount
        XCTAssertEqual(disconnects, 0,
                       "verify build failure must NOT disconnect borrowed metadata client")
    }

    func testVerifyMonth_buildCancellation_doesNotDisconnectBorrowedClient() async throws {
        let dataClient = InMemoryRemoteStorageClient()
        dataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await dataClient.connect()
        try await TestFixtures.injectVersionJSON(dataClient, basePath: basePath)
        try await TestFixtures.injectIdentityFinalization(
            dataClient,
            basePath: basePath,
            repoID: "cccccccc-1111-2222-3333-444444444444"
        )
        await dataClient.injectDownloadCancellation(for: RepoLayout.identityFinalizationFilePath(base: basePath))
        let metadataClient = InMemoryRemoteStorageClient()
        metadataClient.setMoveIfAbsentGuarantee(.exclusive)
        try await metadataClient.connect()
        let profile = try insertProfile()

        do {
            _ = try await BackupV2RuntimeLease.forVerifyMonth(
                client: dataClient,
                borrowedMetadataClient: metadataClient,
                profile: profile,
                databaseManager: databaseManager,
                format: RemoteFormatCompatibilityService()
            )
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // expected
        }
        let disconnects = await metadataClient.disconnectCount
        XCTAssertEqual(disconnects, 0,
                       "verify build cancellation must NOT disconnect borrowed metadata client")
    }

    // MARK: - Helpers

    private func insertProfile() throws -> ServerProfileRecord {
        let id = try TestFixtures.insertServerProfile(in: databaseManager, basePath: basePath, storageType: .webdav)
        return TestFixtures.makeServerProfile(id: id, storageType: .webdav, basePath: basePath)
    }
}

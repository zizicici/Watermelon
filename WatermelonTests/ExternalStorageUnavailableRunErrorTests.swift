import XCTest
@testable import Watermelon

final class ExternalStorageUnavailableRunErrorTests: XCTestCase {

    // MARK: - Repo-open / repo-ID read mapping preserves external cause

    func testNormalizeOpenErrorPreservesExternalCauseFromBootstrapIOFailure() {
        // Canonical identity reads wrap download failures
        // as BootstrapError.ioFailure(error). For a local external-volume profile, that
        // error is RemoteStorageClientError.externalStorageUnavailable. The classifier
        // doesn't walk BootstrapError, so the mapping site must surface the underlying
        // cause instead of collapsing to damagedV2Repo.
        let bootstrap = RepoBootstrap.BootstrapError.ioFailure(
            RemoteStorageClientError.externalStorageUnavailable
        )
        let normalized = BackupV2RuntimeOpenErrorMapping.normalizeOpenError(bootstrap)
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(normalized))
    }

    func testNormalizeOpenErrorStillCollapsesNonExternalBootstrapIOFailure() {
        let malformed = NSError(
            domain: "RepoBootstrap",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "repo.json malformed"]
        )
        let bootstrap = RepoBootstrap.BootstrapError.ioFailure(malformed)
        let normalized = BackupV2RuntimeOpenErrorMapping.normalizeOpenError(bootstrap)
        if case BackupV2RuntimeBuildError.damagedV2Repo = normalized {
            return
        }
        XCTFail("expected damagedV2Repo build error, got \(normalized)")
    }

    func testNormalizeOpenErrorPreservesExternalCauseFromVersionConflictUnreadable() {
        // VersionManifestStore.verifyCompatible wraps download failures as
        // VersionConflict.unreadable(error). External-volume loss must be preserved.
        let conflict = RepoBootstrap.VersionConflict.unreadable(
            RemoteStorageClientError.externalStorageUnavailable
        )
        let normalized = BackupV2RuntimeOpenErrorMapping.normalizeOpenError(conflict)
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(normalized))
    }

    func testNormalizeOpenErrorVersionConflictUnreadableWithNilUnderlyingCollapses() {
        let conflict = RepoBootstrap.VersionConflict.unreadable(nil)
        let normalized = BackupV2RuntimeOpenErrorMapping.normalizeOpenError(conflict)
        if case BackupV2RuntimeBuildError.damagedV2Repo = normalized {
            return
        }
        XCTFail("expected damagedV2Repo build error, got \(normalized)")
    }

    func testNormalizeOpenErrorPreservesV2WrappedExternalCauseInsideBootstrapIOFailure() {
        // Defense-in-depth: a snapshot-write failure may itself surface through
        // BootstrapError.ioFailure if a future producer wraps it there.
        let inner = V2MonthSession.FlushError.postCommitFailed(underlying: SnapshotWriter.WriteError.finalizationFailed(
                RemoteStorageClientError.externalStorageUnavailable
            ))
        let bootstrap = RepoBootstrap.BootstrapError.ioFailure(inner)
        let normalized = BackupV2RuntimeOpenErrorMapping.normalizeOpenError(bootstrap)
        XCTAssertTrue(RemoteStorageClientError.isLikelyExternalStorageUnavailable(normalized))
    }

    // MARK: - HomeExecutionSession.failForMissingConnection respects override

    func testFailForMissingConnectionUsesMessageOverrideForExternalStorageUnavailable() {
        // When BackupSessionController stamps the run as externally unavailable,
        // HomeExecutionCoordinator passes the BSC statusText as a message override so
        // the AppSession.clear cascade doesn't surface the generic "No node connected".
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(
            backup: [month],
            download: [],
            complement: [],
            localAssetIDs: { _ in ["asset1"] }
        )

        let externalMessage = "External storage unavailable"
        let alert = session.failForMissingConnection(messageOverride: externalMessage)

        XCTAssertEqual(alert.message, externalMessage)
        XCTAssertEqual(
            session.monthPlans[month]?.failureFacts.terminalFailure?.message,
            externalMessage
        )
        if case .failed(let phaseMessage) = session.phase {
            XCTAssertEqual(phaseMessage, externalMessage)
        } else {
            XCTFail("expected phase .failed, got \(String(describing: session.phase))")
        }
    }

    func testFailForMissingConnectionFallsBackToDefaultMessageWithoutOverride() {
        let month = LibraryMonthKey(year: 2024, month: 6)
        var session = HomeExecutionSession()
        session.enter(
            backup: [month],
            download: [],
            complement: [],
            localAssetIDs: { _ in ["asset1"] }
        )
        let alert = session.failForMissingConnection()
        XCTAssertEqual(alert.message, String(localized: "home.execution.notConnected"))
        XCTAssertEqual(
            session.monthPlans[month]?.failureFacts.terminalFailure?.message,
            String(localized: "home.execution.notConnected")
        )
    }

    // MARK: - Reducer data invariant for the cascade

    func testApplyRunErrorWithExternalUnavailableStampsFailedStateAndExternalMessage() {
        // BackupSessionController.handleRunError relies on this state stamp running
        // before AppSession.clear's synchronous cascade reaches Home — the reducer
        // must produce state=.failed + the externalUnavailable status text so
        // HomeExecutionCoordinator.pendingRunFailureMessageOverride() can surface
        // the real reason instead of "No node connected".
        var state = BackupSessionState()
        state.state = .running
        state.controlPhase = .idle
        state.applyRunError(
            RemoteStorageClientError.externalStorageUnavailable,
            runMode: .full,
            displayMode: .full,
            externalUnavailable: true,
            intent: .none,
            phaseBeforeFailure: .idle
        )
        XCTAssertEqual(state.state, .failed)
        XCTAssertEqual(state.statusText, String(localized: "backup.session.externalUnavailable"))
        XCTAssertEqual(state.controlPhase, .idle)
        XCTAssertFalse(state.isStartCommandInFlight)
    }

    func testApplyRunErrorWithoutExternalUnavailableUsesGenericFailureMessage() {
        var state = BackupSessionState()
        state.state = .running
        state.applyRunError(
            NSError(domain: "test", code: 1),
            runMode: .full,
            displayMode: .full,
            externalUnavailable: false,
            intent: .none,
            phaseBeforeFailure: .idle
        )
        XCTAssertEqual(state.state, .failed)
        XCTAssertEqual(state.statusText, String(localized: "backup.session.failed"))
    }

    // MARK: - Direct tests for the three uncovered Round-8 mapping sites

    private static let mappingTestBasePath = "/repo"

    private func makeBackupRunPreparationService(_ databaseManager: DatabaseManager) -> BackupRunPreparationService {
        BackupRunPreparationService(
            photoLibraryService: PhotoLibraryService(),
            storageClientFactory: StorageClientFactory(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: RemoteIndexSyncService(),
            databaseManager: databaseManager
        )
    }

    private func makeTempDatabaseManager() throws -> (DatabaseManager, URL) {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let url = dir.appendingPathComponent("test.sqlite")
        return (try DatabaseManager(databaseURL: url), url)
    }

    /// Round 8 added this catch to RemoteIndexV2SyncEngine.loadExpectedRepoIDReadOnly so an
    /// external-volume drop during the read-only repo-ID load surfaces as the leaf cause
    /// instead of collapsing to `damagedV2Repo`.
    func testLoadExpectedRepoIDReadOnlyPreservesExternalCauseFromBootstrapIOFailure() async throws {
        let basePath = Self.mappingTestBasePath
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        await client.injectFile(path: markerPath, data: Data([0x01]))
        await client.injectRawDownloadError(
            RemoteStorageClientError.externalStorageUnavailable,
            for: markerPath
        )

        do {
            _ = try await RemoteIndexV2SyncEngine().loadExpectedRepoIDReadOnly(
                client: client,
                basePath: basePath
            )
            XCTFail("expected external-storage error to propagate")
        } catch {
            XCTAssertTrue(
                RemoteStorageClientError.isLikelyExternalStorageUnavailable(error),
                "expected external-storage propagation, got \(error)"
            )
            if case BackupCompatibilityError.damagedV2Repo = error {
                XCTFail("external cause must not collapse to damagedV2Repo")
            }
        }
    }

    /// verifyMonthV2's catch peels external cause out of BootstrapError.ioFailure before
    /// `damagedV2Repo` normalization. Uses profile=nil so the identity guard is skipped and
    /// only the loadRepoIDStrict catch is exercised.
    func testVerifyMonthV2PreservesExternalCauseFromBootstrapIOFailure() async throws {
        let (databaseManager, dbURL) = try makeTempDatabaseManager()
        // addTeardownBlock fires after the test method returns and its locals are
        // released, so the GRDB DatabaseQueue closes before we unlink the file.
        addTeardownBlock { try? FileManager.default.removeItem(at: dbURL.deletingLastPathComponent()) }
        let basePath = Self.mappingTestBasePath

        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        // Need a valid v2 version.json so the inspect step before verifyMonthV2 returns .v2.
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        // Finalized marker must exist (so metadataFileIfPresent returns non-nil) but its
        // download must fail with externalStorageUnavailable to trigger BootstrapError.ioFailure.
        let markerPath = RepoLayout.identityFinalizationFilePath(base: basePath)
        await client.injectFile(path: markerPath, data: Data([0x01]))
        await client.injectRawDownloadError(
            RemoteStorageClientError.externalStorageUnavailable,
            for: markerPath
        )

        let service = makeBackupRunPreparationService(databaseManager)

        do {
            _ = try await service.verifyMonthV2(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5)
            )
            XCTFail("expected external-storage error to propagate")
        } catch {
            XCTAssertTrue(
                RemoteStorageClientError.isLikelyExternalStorageUnavailable(error),
                "expected external-storage propagation, got \(error)"
            )
            if case BackupCompatibilityError.damagedV2Repo = error {
                XCTFail("external cause must not collapse to damagedV2Repo")
            }
        }
    }

    func testProfileLessInspectVersionConflictUnreadableWithNilUnderlyingCollapsesToDamagedV2Repo() async throws {
        let (databaseManager, dbURL) = try makeTempDatabaseManager()
        addTeardownBlock { try? FileManager.default.removeItem(at: dbURL.deletingLastPathComponent()) }
        let basePath = Self.mappingTestBasePath

        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        await client.injectFile(path: versionPath, data: Data([0x01]))

        let service = makeBackupRunPreparationService(databaseManager)

        do {
            _ = try await service.verifyMonth(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5),
                profile: nil,
                password: nil
            )
            XCTFail("expected damagedV2Repo to be thrown")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        } catch {
            XCTFail("expected BackupCompatibilityError.damagedV2Repo, got \(error)")
        }
    }

    func testProfileLessInspectPreservesExternalCauseFromVersionManifestDownload() async throws {
        let (databaseManager, dbURL) = try makeTempDatabaseManager()
        addTeardownBlock { try? FileManager.default.removeItem(at: dbURL.deletingLastPathComponent()) }
        let basePath = Self.mappingTestBasePath

        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // version.json metadata present + within the size cap; download throws external.
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        await client.injectFile(path: versionPath, data: Data([0x01]))
        await client.injectRawDownloadError(
            RemoteStorageClientError.externalStorageUnavailable,
            for: versionPath
        )

        let service = makeBackupRunPreparationService(databaseManager)

        do {
            _ = try await service.verifyMonth(
                client: client,
                basePath: basePath,
                month: LibraryMonthKey(year: 2026, month: 5),
                profile: nil,
                password: nil
            )
            XCTFail("expected external-storage error to propagate")
        } catch {
            XCTAssertTrue(
                RemoteStorageClientError.isLikelyExternalStorageUnavailable(error),
                "expected external-storage propagation, got \(error)"
            )
            if case BackupCompatibilityError.damagedV2Repo = error {
                XCTFail("external cause must not collapse to damagedV2Repo")
            }
        }
    }
}

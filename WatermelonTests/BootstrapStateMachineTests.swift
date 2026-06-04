import XCTest
@testable import Watermelon

final class BootstrapStateMachineTests: XCTestCase {
    private let basePath = "/repo"
    private let format = RemoteFormatCompatibilityService()

    func testEmptyBasePath_returnsFresh() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: basePath)
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh)
    }

    func testWatermelonAbsent_noV1Manifests_returnsFresh() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/somethingelse")
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh)
    }

    func testWatermelonAbsent_withV1Manifests_returnsV1() async throws {
        let (client, profile) = await makeFixture()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v1)
    }

    func testWatermelonPresent_repoAbsent_versionAbsent_noV1_returnsFresh() async throws {
        // Half-bootstrap: createDirectory(.watermelon/) succeeded but no files written
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh, "empty .watermelon dir → idempotent re-bootstrap")
    }

    func testWatermelonPresent_repoAbsent_versionAbsent_withV1_returnsV1() async throws {
        // V1 phase1 wrote commits + snapshots into .watermelon/, then crashed before phase2 wrote version.json
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v1, "V1 manifests present + version.json absent → resume V1 migration")
    }

    func testWatermelonPresent_repoPresent_versionAbsent_returnsFreshOrV1() async throws {
        // .watermelon/ present but version.json absent
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")

        // Without V1 manifests → idempotent re-bootstrap
        var outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh)

        // With V1 manifests → resume V1 migration
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v1)
    }

    func testWatermelonPresent_repoPresent_versionMatches_returnsV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion))
    }

    func testWatermelonPresent_versionLagHiddenOnGraceBackend_returnsV2NotFresh() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        // Backend advertises metadata read-after-write lag; the first version.json metadata read
        // 404s (just-written manifest still propagating) while the file is genuinely present.
        client.setReadAfterWriteGrace(2)
        await client.injectMetadataError(.notFound, for: RepoLayout.versionFilePath(base: basePath))

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "marker-present + version lag-hidden on a grace backend must reconfirm to .v2, not route .fresh")
    }

    func testWatermelonPresent_versionDownloadLagHiddenOnGraceBackend_returnsV2NotError() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        // version.json metadata/list is visible, but the first data-path GET 404s (data-path GET
        // lagging behind metadata on a grace backend); it becomes readable on retry.
        client.setReadAfterWriteGrace(2)
        await client.injectDownloadError(.notFound, for: RepoLayout.versionFilePath(base: basePath))

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "metadata-visible version.json whose download lag-404s within grace must reconfirm to .v2, not abort with a raw error")
    }

    func testWatermelonPresent_versionStablyAbsentOnGraceBackend_returnsFresh() async throws {
        // Same grace backend, but version.json is genuinely absent (empty .watermelon). The
        // reconfirm loop must still settle on .fresh rather than hang or misclassify.
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        client.setReadAfterWriteGrace(1)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh)
    }

    /// A grace backend's base LIST can transiently omit a just-written `.watermelon/` while version.json
    /// is already readable (direct object reads lead the parent-prefix listing). Inspection must reconfirm
    /// the marker within grace and route .v2, not demote a live V2 repo to .fresh — a V2-bound syncIndex
    /// treats .fresh/.v1 as a deterministic format regression and clears the committed view.
    func testWatermelonMarkerLagOmittedFromBaseListOnGraceBackend_reconfirmsV2NotFresh() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        try await TestFixtures.injectVersionJSON(inner, basePath: basePath)
        let client = BaseListMarkerOmitWrapper(inner: inner, grace: 2)
        let profile = TestFixtures.makeServerProfile(
            id: 1, name: "Test", storageType: .webdav,
            host: "host", port: 0, shareName: "", basePath: basePath, username: ""
        )
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "marker lag-omitted from one base LIST on a grace backend must reconfirm to .v2, not route .fresh")
    }

    /// Same wrapper but zero grace: a marker-absent base LIST is authoritative and must route .fresh
    /// without spending any reconfirm budget.
    func testWatermelonMarkerOmittedFromBaseListOnZeroGraceBackend_routesFresh() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        try await TestFixtures.injectVersionJSON(inner, basePath: basePath)
        let client = BaseListMarkerOmitWrapper(inner: inner, grace: 0)
        let profile = TestFixtures.makeServerProfile(
            id: 1, name: "Test", storageType: .webdav,
            host: "host", port: 0, shareName: "", basePath: basePath, username: ""
        )
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh,
                       "zero-grace backend: a marker-absent base LIST is authoritative")
    }

    /// Mixed-lag marker omission: the base LIST omits `.watermelon/` AND the first version.json data-path
    /// GET 404s behind already-visible metadata. The reconfirm must use the download-lag-tolerant loader so
    /// it reconfirms `.v2` within grace instead of aborting inspection with the raw download not-found.
    func testWatermelonMarkerLagOmitted_versionDownloadLag404OnGraceBackend_reconfirmsV2() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        try await TestFixtures.injectVersionJSON(inner, basePath: basePath)
        // version.json metadata is visible; the first data-path GET 404s within grace, then resolves.
        await inner.injectDownloadError(.notFound, for: RepoLayout.versionFilePath(base: basePath))
        let client = BaseListMarkerOmitWrapper(inner: inner, grace: 2)
        let profile = TestFixtures.makeServerProfile(
            id: 1, name: "Test", storageType: .webdav,
            host: "host", port: 0, shareName: "", basePath: basePath, username: ""
        )
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "marker omitted + version.json download lag-404 within grace must reconfirm to .v2, not abort with the raw download error")
    }

    /// A genuinely-fresh empty remote on a high-grace backend has no `.watermelon/` and no version.json,
    /// so the marker-absent reconfirm must settle on `.fresh` after only a few bounded reads — not poll the
    /// full read-after-write ceiling (which would stall every empty/legacy open by ~grace seconds).
    func testFreshEmptyRemoteOnHighGraceBackend_routesFreshWithBoundedReads() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        try await inner.createDirectory(path: basePath)
        let client = MetadataCountingClient(inner: inner, grace: 30)
        let profile = TestFixtures.makeServerProfile(
            id: 1, name: "Test", storageType: .webdav,
            host: "host", port: 0, shareName: "", basePath: basePath, username: ""
        )
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh)
        let reads = await client.metadataCalls()
        XCTAssertLessThanOrEqual(reads, 8,
                                 "marker-absent reconfirm on a fresh remote must be bounded, not a full-grace-ceiling poll (got \(reads) reads)")
    }

    func testWatermelonPresent_versionHigher_returnsUnsupported() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9")
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        if case .unsupported(let minApp) = outcome {
            XCTAssertEqual(minApp, "9.9.9")
        } else {
            XCTFail("expected .unsupported, got \(outcome)")
        }
    }

    func testFutureVersion_doesNotListMigrationDirectory() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9")
        await client.injectListError(.transport, for: RepoLayout.migrationsDirectoryPath(base: basePath))

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .unsupported(minAppVersion: "9.9.9"))
    }

    func testFormatVersionBelowTwo_doesNotParseMarkersOrDetectV1Manifests() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 1, minAppVersion: "1.0.0")
        let writerID = "12121212-1212-1212-1212-121212121212"
        try await injectMigrationMarker(client: client, writerID: writerID, phase: 3)
        await client.injectPersistentDownloadError(.transport, for: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID))
        try await client.createDirectory(path: "\(basePath)/2025")
        await client.injectListError(.transport, for: "\(basePath)/2025")

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .unsupported(minAppVersion: "1.0.0"))
    }

    func testWatermelonPresent_dataDirsHaveContent_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        // Plant a leftover commit in commits/ — identity files absent.
        let commitsPath = RepoLayout.commitsDirectoryPath(base: basePath)
        try await client.createDirectory(path: commitsPath)
        await client.injectFile(path: "\(commitsPath)/some-leftover-commit.jsonl", contents: "stale")

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testWatermelonPresent_emptyDataDirs_returnsFreshNotDamaged() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        // Empty commits/ + snapshots/ = aborted bootstrap mid-mkdir, NOT damage.
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh)
    }

    func testWatermelonPresent_onlyStagingOrphan_returnsFreshNotDamaged() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        // Interrupted first write left only `<commit>.jsonl.staging-<uuid>` — recoverable, not damage.
        let writerID = "11111111-1111-1111-1111-111111111111"
        let month = LibraryMonthKey(year: 2026, month: 6)
        let stagingName = "\(RepoLayout.commitFileName(month: month, writerID: writerID, seq: 1)).staging-\(UUID().uuidString)"
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        await client.injectFile(path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/\(stagingName)", contents: "partial")
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh)
    }

    func testMigrationInProgressMarker_withV1Manifests_forcesV1() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        // Plant a migration marker.
        let writerID = "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        await client.injectFile(path: markerPath, contents: #"{"v":2,"writer_id":"\#(writerID)","phase":3,"started_at_ms":0}"#)
        // Phase3 cleanup has not finished.
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2WithV1Manifests(formatVersion: RepoLayout.formatVersion),
                       "marker + V1 manifests after V2 sentinel → V2-with-V1-residue forces foreground migration")
    }

    // ClaudeReviewerC P17 R01: detectV1Manifests flagged out-of-range two-digit month dirs (e.g. 2023/13,
    // 2023/00) that scanV1Months/verifyFinalState skip, so a V2 repo carrying such a stray manifest routed
    // to .v2WithV1Manifests on every open while migration reported success — a permanent foreground-migration
    // loop with no resolution path. The admission predicate must share scanV1Months' 01-12 month domain.
    func testV2_withOutOfRangeMonthManifest_returnsV2NotV1Manifests() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2023, month: 13)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2023, month: 0)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "out-of-range month dirs migration cannot process must not flag admission as .v2WithV1Manifests")
    }

    func testMarkerAbsent_onlyOutOfRangeMonthManifest_returnsFreshNotV1() async throws {
        let (client, profile) = await makeFixture()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2023, month: 13)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh,
                       "an out-of-range month manifest is not a migratable V1 month; admission must not route .v1")
    }

    func testStaleMigrationMarker_noV1Manifests_returnsV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let writerID = "b1b1b1b1-b1b1-b1b1-b1b1-b1b1b1b1b1b1"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        await client.injectFile(path: markerPath, contents: #"{"v":2,"writer_id":"\#(writerID)","phase":3,"started_at_ms":0}"#)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2WithPendingMigrationCleanup(formatVersion: RepoLayout.formatVersion, ownerWriterID: writerID), "stale marker without V1 manifests → V2 cleanup")
    }

    func testPhase1ResidueMarker_noV1Manifests_routesToCleanup() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let writerID = "23232323-2323-2323-2323-232323232323"
        try await injectMigrationMarker(client: client, writerID: writerID, phase: 1)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(
            outcome,
            .v2WithPendingMigrationCleanup(formatVersion: RepoLayout.formatVersion, ownerWriterID: writerID)
        )
    }

    func testSupportedVersion_phaseAndWriterSort_selectsHighestPhaseLowestWriterID() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let phase2WriterID = "11111111-1111-1111-1111-111111111111"
        let phase3LowWriterID = "22222222-2222-2222-2222-222222222222"
        let phase3HighWriterID = "33333333-3333-3333-3333-333333333333"
        try await injectMigrationMarker(client: client, writerID: phase2WriterID, phase: 2)
        try await injectMigrationMarker(client: client, writerID: phase3HighWriterID, phase: 3)
        try await injectMigrationMarker(client: client, writerID: phase3LowWriterID, phase: 3)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(
            outcome,
            .v2WithPendingMigrationCleanup(formatVersion: RepoLayout.formatVersion, ownerWriterID: phase3LowWriterID)
        )
    }

    func testSupportedVersion_markerDownloadFailureWithV1Manifests_propagatesMarkerError() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let writerID = "45454545-4545-4545-4545-454545454545"
        try await injectMigrationMarker(client: client, writerID: writerID, phase: 3)
        await client.injectPersistentDownloadError(.transport, for: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID))
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected marker transport error")
        } catch RemoteStorageClientError.underlying(let underlying) {
            let nsError = underlying as NSError
            XCTAssertEqual(nsError.domain, NSURLErrorDomain)
            XCTAssertEqual(nsError.code, NSURLErrorNotConnectedToInternet)
        } catch {
            XCTFail("expected marker transport error, got \(error)")
        }
    }

    /// Transport errors while downloading marker files must propagate as storage
    /// errors, not be collapsed into damagedV2Repo.
    func testVersionAbsentMarkerTransportError_noV2Data_propagatesStorageError() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        let writerID = "34343434-3434-3434-3434-343434343434"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        await client.injectFile(path: markerPath, contents: "not-json")
        await client.injectPersistentDownloadError(.transport, for: markerPath)

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("transport error during marker download should propagate")
        } catch is BackupCompatibilityError {
            XCTFail("transient transport error must not be classified as damagedV2Repo")
        } catch {
            // expected: raw storage error propagates
        }
    }

    func testWatermelonPresent_versionJsonGarbage_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), contents: "{not-json")
        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testWatermelonPresent_versionJsonBooleanFormatVersion_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        let body: [String: Any] = [
            "format_version": true,
            "min_app_version": "2.0.0"
        ]
        let data = try JSONSerialization.data(withJSONObject: body)
        await client.injectFile(path: RepoLayout.versionFilePath(base: basePath), data: data)
        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testWatermelonPresent_versionJsonIsDirectory_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        // Directory squatting on the version.json path — code 18 in VersionManifestStore.
        try await client.createDirectory(path: RepoLayout.versionFilePath(base: basePath) + "/child")
        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testVersionLoadCancellation_propagatesCancellation() async throws {
        try await assertVersionLoadCancellation { client, path in
            await client.injectDownloadCancellation(for: path)
        }
    }

    func testVersionLoadURLErrorCancelled_propagatesCancellation() async throws {
        try await assertVersionLoadCancellation { client, path in
            await client.injectDownloadURLErrorCancelled(for: path)
        }
    }

    func testVersionLoadWrappedURLCancellation_propagatesCancellation() async throws {
        try await assertVersionLoadCancellation { client, path in
            await client.injectDownloadWrappedURLCancellation(for: path)
        }
    }

    private func assertVersionLoadCancellation(
        inject: (InMemoryRemoteStorageClient, String) async -> Void,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        await inject(client, RepoLayout.versionFilePath(base: basePath))

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected cancellation", file: file, line: line)
        } catch is CancellationError {
        } catch {
            XCTFail("expected CancellationError, got \(error)", file: file, line: line)
        }
    }

    func testTransientListErrorPropagates() async throws {
        // version.json transient errors should NOT be misclassified as unsupported.
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        await client.injectDownloadError(.transport, for: RepoLayout.versionFilePath(base: basePath))
        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected transport error to propagate")
        } catch {
            // expected — caller can retry rather than locking the repo as unsupported
        }
    }

    func testHijackedMigrationMarker_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let filenameWriterID = "11111111-1111-1111-1111-111111111111"
        let hijackJSONWriterID = "22222222-2222-2222-2222-222222222222"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: filenameWriterID)
        await client.injectFile(
            path: markerPath,
            contents: #"{"v":2,"writer_id":"\#(hijackJSONWriterID)","phase":2}"#
        )

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testMigrationMarkerParse_acceptsLegacyVoneMarker() throws {
        let writerID = "33333333-3333-3333-3333-333333333333"
        let parsed = try MigrationMarker.parse(
            filename: "\(writerID).json",
            bytes: Data("{}".utf8)
        )
        XCTAssertEqual(parsed.writerID, writerID)
        XCTAssertEqual(parsed.phase, .phase1, "legacy markers without `phase` field map to phase1")
        XCTAssertNil(parsed.runID)
        XCTAssertNil(parsed.startedAtMs)
    }

    func testMigrationMarkerParse_rejectsUnknownPhase() {
        let writerID = "44444444-4444-4444-4444-444444444444"
        let bytes = Data(#"{"v":2,"writer_id":"\#(writerID)","phase":4}"#.utf8)
        XCTAssertThrowsError(try MigrationMarker.parse(filename: "\(writerID).json", bytes: bytes)) { error in
            guard case MigrationMarkerError.unknownPhase(let raw) = error else {
                XCTFail("expected .unknownPhase, got \(error)")
                return
            }
            XCTAssertEqual(raw, 4)
        }
    }

    func testMigrationMarkerParse_rejectsHijackedWriterID() {
        let filenameWriterID = "55555555-5555-5555-5555-555555555555"
        let hijackJSONWriterID = "66666666-6666-6666-6666-666666666666"
        let bytes = Data(#"{"v":2,"writer_id":"\#(hijackJSONWriterID)","phase":2}"#.utf8)
        let parsedFilename = "\(filenameWriterID).json"
        XCTAssertThrowsError(try MigrationMarker.parse(filename: parsedFilename, bytes: bytes)) { error in
            guard case MigrationMarkerError.writerIDMismatch(let filename, let jsonWriter) = error else {
                XCTFail("expected .writerIDMismatch, got \(error)")
                return
            }
            XCTAssertEqual(filename, parsedFilename)
            XCTAssertEqual(jsonWriter, hijackJSONWriterID)
        }
    }

    func testMigrationMarkerParse_rejectsWrongTypeWriterID() {
        let writerID = "77777777-7777-7777-7777-777777777777"
        // Present-but-wrong-type writer_id must not silently fall back to filename writerID.
        let bytes = Data(#"{"v":2,"writer_id":123,"phase":2}"#.utf8)
        XCTAssertThrowsError(try MigrationMarker.parse(filename: "\(writerID).json", bytes: bytes)) { error in
            guard case MigrationMarkerError.writerIDWrongType = error else {
                XCTFail("expected .writerIDWrongType, got \(error)")
                return
            }
        }
    }

    func testMigrationMarkerParse_rejectsWrongTypePhase() {
        let writerID = "88888888-8888-8888-8888-888888888888"
        // Present-but-wrong-type phase must not silently default to phase1.
        let bytes = Data(#"{"v":2,"writer_id":"\#(writerID)","phase":"2"}"#.utf8)
        XCTAssertThrowsError(try MigrationMarker.parse(filename: "\(writerID).json", bytes: bytes)) { error in
            guard case MigrationMarkerError.phaseWrongType = error else {
                XCTFail("expected .phaseWrongType, got \(error)")
                return
            }
        }
    }

    func testMigrationMarkerParse_rejectsBooleanPhase() {
        let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        // JSON booleans bridge through `as? Int` (true → 1); reject before phase mapping.
        let bytes = Data(#"{"v":2,"writer_id":"\#(writerID)","phase":true}"#.utf8)
        XCTAssertThrowsError(try MigrationMarker.parse(filename: "\(writerID).json", bytes: bytes)) { error in
            guard case MigrationMarkerError.phaseWrongType = error else {
                XCTFail("expected .phaseWrongType, got \(error)")
                return
            }
        }
    }

    func testMigrationMarkerParse_rejectsFalseBooleanPhase() {
        let writerID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        let bytes = Data(#"{"v":2,"writer_id":"\#(writerID)","phase":false}"#.utf8)
        XCTAssertThrowsError(try MigrationMarker.parse(filename: "\(writerID).json", bytes: bytes)) { error in
            guard case MigrationMarkerError.phaseWrongType = error else {
                XCTFail("expected .phaseWrongType, got \(error)")
                return
            }
        }
    }

    func testMigrationMarkerParse_rejectsZeroPhase() {
        let writerID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
        let bytes = Data(#"{"v":2,"writer_id":"\#(writerID)","phase":0}"#.utf8)
        XCTAssertThrowsError(try MigrationMarker.parse(filename: "\(writerID).json", bytes: bytes)) { error in
            guard case MigrationMarkerError.unknownPhase(let raw) = error else {
                XCTFail("expected .unknownPhase(0), got \(error)")
                return
            }
            XCTAssertEqual(raw, 0)
        }
    }

    func testMigrationMarkerEncodeParse_roundTripsAllPhases() throws {
        let writerID = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
        for phase in [MigrationMarkerPhase.phase1, .phase2, .phase3] {
            let marker = ParsedMigrationMarker(
                writerID: writerID,
                phase: phase,
                runID: "run-\(phase.rawValue)",
                startedAtMs: 1_700_000_000_000,
                lastStepMs: 1_700_000_000_500
            )
            let bytes = try MigrationMarker.encode(marker)
            let parsed = try MigrationMarker.parse(filename: "\(writerID).json", bytes: bytes)
            XCTAssertEqual(parsed.writerID, writerID)
            XCTAssertEqual(parsed.phase, phase, "round-trip must preserve phase \(phase.rawValue)")
            XCTAssertEqual(parsed.runID, "run-\(phase.rawValue)")
            XCTAssertEqual(parsed.startedAtMs, 1_700_000_000_000)
            XCTAssertEqual(parsed.lastStepMs, 1_700_000_000_500)
        }
    }

    func testWrongTypePhaseMarker_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let writerID = "99999999-9999-9999-9999-999999999999"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        await client.injectFile(
            path: markerPath,
            contents: #"{"v":2,"writer_id":"\#(writerID)","phase":"2"}"#
        )

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testBooleanPhaseMarker_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let writerID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        await client.injectFile(
            path: markerPath,
            contents: #"{"v":2,"writer_id":"\#(writerID)","phase":true}"#
        )

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testMalformedParseableMigrationMarker_versionAbsentWithV2Data_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        await client.injectFile(path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/leftover.jsonl", contents: "data")
        let writerID = "abababab-abab-abab-abab-abababababab"
        await client.injectFile(
            path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID),
            contents: #"{"v":2,"writer_id":"\#(writerID)","phase":true}"#
        )

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testDirectoryShapedMigrationMarker_withVersionJson_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        // Create a directory at the canonical migration marker path.
        let writerID = "12121212-1212-1212-1212-121212121212"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        try await client.createDirectory(path: markerPath)

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("directory-shaped migration marker with version.json should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testDirectoryShapedPhaseSuffixedMigrationMarker_withVersionJson_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let writerID = "12121212-1212-1212-1212-121212121212"
        let markerPath = RepoLayout.migrationPhaseMarkerPath(
            base: basePath,
            writerID: writerID,
            phase: 3,
            markerID: "abcd1111-abcd-1111-abcd-111111111111"
        )
        try await client.createDirectory(path: markerPath)

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("directory-shaped phase-suffixed migration marker should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testDirectoryShapedMigrationMarker_alongsideValidMarker_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        // Valid phase-3 file marker for writer B.
        let fileWriterID = "55555555-5555-5555-5555-555555555555"
        try await injectMigrationMarker(client: client, writerID: fileWriterID, phase: 3)
        // Directory-shaped marker for writer A alongside it.
        let dirWriterID = "66666666-6666-6666-6666-666666666666"
        try await client.createDirectory(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: dirWriterID))

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("mixed directory + file migration markers should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testDirectoryShapedMigrationMarker_versionAbsent_alongsideValidMarker_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        await client.injectFile(path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/leftover.jsonl", contents: "data")
        // Valid phase-3 file marker for writer B.
        let fileWriterID = "77777777-7777-7777-7777-777777777777"
        try await injectMigrationMarker(client: client, writerID: fileWriterID, phase: 3)
        // Directory-shaped marker for writer A alongside it.
        let dirWriterID = "88888888-8888-8888-8888-888888888888"
        try await client.createDirectory(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: dirWriterID))

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("mixed directory + file migration markers in version-absent path should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testDirectoryShapedMigrationMarker_versionAbsent_withV2Data_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        await client.injectFile(path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/leftover.jsonl", contents: "data")
        let writerID = "34343434-3434-3434-3434-343434343434"
        try await client.createDirectory(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID))

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("directory-shaped migration marker with V2 data should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testDirectoryShapedMigrationMarker_versionAbsent_withV2DataAndV1Manifests_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        await client.injectFile(path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/leftover.jsonl", contents: "data")
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        let writerID = "34343434-3434-3434-3434-343434343434"
        try await client.createDirectory(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID))

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("directory-shaped migration marker with V2 data + V1 manifests should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testDirectoryShapedMigrationMarker_versionAbsent_noV2Data_withV1Manifests_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        let writerID = "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1"
        try await client.createDirectory(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID))

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("directory-shaped migration marker with V1 manifests and no V2 data should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    func testDirectoryShapedMigrationMarker_versionAbsent_noV2Data_noV1Manifests_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        let writerID = "b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2"
        try await client.createDirectory(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID))

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("directory-shaped migration marker with no V2 data and no V1 manifests should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    /// A malformed marker file alongside V2 data + V1 manifests must fail damaged.
    func testMalformedMarker_versionAbsent_withV2DataAndV1Manifests_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        await client.injectFile(path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/leftover.jsonl", contents: "data")
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        let writerID = "c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3"
        // Write a marker file with malformed body (not valid JSON for MigrationMarkerWire)
        await client.injectFile(
            path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID),
            contents: "not valid marker json"
        )

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("malformed marker with V2 data + V1 manifests should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    /// A malformed marker file without V2 data must still fail damaged.
    func testMalformedMarker_versionAbsent_noV2Data_noV1Manifests_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        let writerID = "d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4"
        await client.injectFile(
            path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID),
            contents: "not valid marker json"
        )

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("malformed marker with no V2 data and no V1 manifests should throw damagedV2Repo")
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        }
    }

    /// Transport errors while reading valid-marker files must propagate as storage
    /// errors, not be collapsed into damagedV2Repo.
    func testValidMarkerTransportError_versionAbsent_noV2Data_propagatesStorageError() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        let writerID = "e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        // Write a parseable marker so the listing passes filename validation.
        try await injectMigrationMarker(client: client, writerID: writerID, phase: 2)
        // Make the download always fail with a transport error.
        await client.injectPersistentDownloadError(.transport, for: markerPath)

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("transport error during valid-marker download should propagate")
        } catch is BackupCompatibilityError {
            XCTFail("transient transport error must not be classified as damagedV2Repo")
        } catch {
            // expected: raw storage error propagates
        }
    }

    /// A year directory listed in the base enumeration but 404ing on the follow-up list
    /// (concurrent removal / eventual consistency) must be treated as absence, not a hard
    /// inspection failure that fails the whole V2 open.
    func testStaleV1YearDirNotFound_markerAbsent_returnsFreshNotThrow() async throws {
        let (client, profile) = await makeFixture()
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        // Year dir is present in the base listing but vanishes before detectV1Manifests reads it.
        await client.injectListError(.notFound, for: "\(basePath)/2025")
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh, "vanished V1 year dir contributes no manifest; must not throw")
    }

    /// On a healthy V2 repo carrying residual V1 manifests, a month directory that 404s on
    /// the follow-up list must not collapse the `.v2WithV1Manifests`/`.v2` decision into an
    /// open failure.
    func testStaleV1MonthDirNotFound_versionPresent_returnsV2NotThrow() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)
        // Year lists fine (returns the 06 dir) but the month dir 404s on the follow-up list.
        await client.injectListError(.notFound, for: "\(basePath)/2025/06")
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "vanished V1 month dir contributes no manifest; must resolve .v2, not throw")
    }

    private func makeFixture() async -> (InMemoryRemoteStorageClient, ServerProfileRecord) {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        let profile = TestFixtures.makeServerProfile(
            id: 1, name: "Test", storageType: .webdav,
            host: "host", port: 0, shareName: "", basePath: basePath, username: ""
        )
        return (client, profile)
    }

    private func injectMigrationMarker(
        client: InMemoryRemoteStorageClient,
        writerID: String,
        phase: Int
    ) async throws {
        let marker: [String: Any] = [
            "v": 2,
            "writer_id": writerID,
            "run_id": "run-\(phase)",
            "phase": phase,
            "started_at_ms": Int64(0),
            "last_step_at_ms": Int64(1)
        ]
        let data = try JSONSerialization.data(withJSONObject: marker)
        await client.injectFile(path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID), data: data)
    }

    func testInspect_basePathListURLCancellation_propagatesAsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        let profile = TestFixtures.makeServerProfile(
            id: 1, name: "Test", storageType: .webdav,
            host: "host", port: 0, shareName: "", basePath: basePath, username: ""
        )
        await client.injectListURLErrorCancelled(for: basePath)

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected CancellationError from URL-shaped list cancellation")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    func testInspect_basePathListWrappedURLCancellation_propagatesAsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        let profile = TestFixtures.makeServerProfile(
            id: 1, name: "Test", storageType: .webdav,
            host: "host", port: 0, shareName: "", basePath: basePath, username: ""
        )
        await client.injectListWrappedURLCancellation(for: basePath)

        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected CancellationError from wrapped URL-shaped list cancellation")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    // MARK: - Profileless inspection

    func testProfileless_versionFormatVersionBelowTwo_returnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 1, minAppVersion: "1.0.0")

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .unsupported(minAppVersion: "1.0.0"),
                       "format_version < 2 is impossible for V1 and corrupt for V2; must not return .v1")
    }

    func testProfileless_versionFormatVersionZero_returnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 0, minAppVersion: "0.0.1")

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .unsupported(minAppVersion: "0.0.1"),
                       "format_version 0 is corrupt; must not return .v1")
    }

    func testProfileless_versionAbsent_returnsV1() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .v1,
                       "absent version.json should still return .v1 for profileless path")
    }

    func testProfileless_versionMatchesCurrent_returnsV2() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion))
    }

    func testProfileless_versionDownloadLagHiddenOnGraceBackend_returnsV2() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        // Metadata precheck sees version.json; the first download 404s within grace, then resolves.
        client.setReadAfterWriteGrace(2)
        await client.injectDownloadError(.notFound, for: RepoLayout.versionFilePath(base: basePath))

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "profileless verify must reconfirm a lag-404'd version download to .v2, not abort")
    }

    func testProfileless_versionHigher_returnsUnsupported() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9")

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .unsupported(minAppVersion: "9.9.9"))
    }

    /// Mixed lag: precheck proves version.json metadata, the data-path GET 404s, and a retry's metadata
    /// read then flaps to not-found mid-grace before resolving. The tolerant proven-metadata loader must
    /// keep spending the grace budget and reconfirm `.v2`, never demote the proven marker to `.v1`.
    func testProfileless_versionDownloadLagThenMetadataFlapWithinGrace_returnsV2() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        try await TestFixtures.injectVersionJSON(inner, basePath: basePath)
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        // metadata: present (precheck #1), present (first load #2), absent (retry flap #3), present (#4).
        // download: 404 once (first load), then served.
        let client = VersionPathFlapClient(
            inner: inner,
            targetPath: versionPath,
            metadataNilCallIndices: [3],
            downloadNotFoundCallIndices: [1],
            graceSeconds: 3
        )

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "download-lag then metadata flap within grace must reconfirm .v2, not demote to .v1")
    }

    /// The precheck proves version.json metadata, but the very first tolerant `load()` metadata read
    /// flaps to not-found before any download. With the proven precheck, that absence is visibility lag,
    /// not a fresh endpoint — the loader must reconfirm `.v2` within grace, not return `.v1`.
    func testProfileless_metadataFlapsAbsentOnFirstLoadWithinGrace_returnsV2() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        try await TestFixtures.injectVersionJSON(inner, basePath: basePath)
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        // metadata: present (precheck #1), absent (first load #2 flap), present (#3).
        let client = VersionPathFlapClient(
            inner: inner,
            targetPath: versionPath,
            metadataNilCallIndices: [2],
            downloadNotFoundCallIndices: [],
            graceSeconds: 3
        )

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "a metadata flap after a proven precheck must not demote a V2 marker to .v1")
    }

    /// The very first metadata precheck for version.json returns nil on a grace backend (visibility lag
    /// of the object's metadata itself, not just its download). `inspectRemoteFormatProfileless` must
    /// retry the precheck within the grace budget and resolve `.v2`, not immediately return `.v1`.
    func testProfileless_firstMetadataPrecheckHiddenOnGraceBackend_thenVisible_returnsV2() async throws {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        client.setReadAfterWriteGrace(3)
        await client.injectMetadataError(.notFound, for: RepoLayout.versionFilePath(base: basePath))

        let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "first metadata precheck hidden by grace lag must retry, not return .v1")
    }

    /// Once the precheck proved version.json metadata and the marker then stays unreadable past grace,
    /// the proven-metadata loader must fail closed (throw) rather than returning `.absent`/routing `.v1`.
    func testProfileless_metadataFlapsAbsentPastGrace_failsClosedNotV1() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        try await TestFixtures.injectVersionJSON(inner, basePath: basePath)
        let versionPath = RepoLayout.versionFilePath(base: basePath)
        // metadata: present (precheck #1), then absent from #2 onward; grace too small to recover.
        let client = VersionPathFlapClient(
            inner: inner,
            targetPath: versionPath,
            metadataNilFromIndex: 2,
            downloadNotFoundCallIndices: [],
            graceSeconds: 0.3
        )

        do {
            let outcome = try await format.inspectRemoteFormatProfileless(client: client, basePath: basePath)
            XCTFail("expected fail-closed throw, got \(outcome)")
        } catch is BackupCompatibilityError {
            // acceptable fail-closed mapping
        } catch {
            // raw fail-closed propagation is also acceptable; the only forbidden outcome is `.v1`.
        }
    }
}

/// Flaps `metadata`/`download` for one target path to model an eventually-consistent grace backend:
/// metadata can report not-found on chosen call indices (or from a given index onward) and `download`
/// can 404 on chosen call indices, all against a file whose bytes are present in the inner store.
private actor VersionPathFlapClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated let graceSeconds: TimeInterval
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { graceSeconds }

    private let inner: InMemoryRemoteStorageClient
    private let targetPath: String
    private let metadataNilCallIndices: Set<Int>
    private let metadataNilFromIndex: Int?
    private let downloadNotFoundCallIndices: Set<Int>
    private var metadataCalls = 0
    private var downloadCalls = 0

    init(
        inner: InMemoryRemoteStorageClient,
        targetPath: String,
        metadataNilCallIndices: Set<Int> = [],
        metadataNilFromIndex: Int? = nil,
        downloadNotFoundCallIndices: Set<Int> = [],
        graceSeconds: TimeInterval
    ) {
        self.inner = inner
        self.targetPath = targetPath
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
        guard normalize(path) == normalize(targetPath) else { return try await inner.metadata(path: path) }
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
        guard normalize(remotePath) == normalize(targetPath) else {
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

/// Models a grace backend whose parent-prefix LIST omits the just-written `.watermelon/` directory while
/// the underlying objects (version.json, repo-identity.json) stay metadata/download-readable. Used to prove that
/// inspection reconfirms the marker against the more-consistent object reads instead of demoting to .fresh.
private struct BaseListMarkerOmitWrapper: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let grace: TimeInterval

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    var readAfterWriteGraceSeconds: TimeInterval { grace }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await inner.list(path: path).filter { !($0.isDirectory && $0.name == RepoLayout.watermelonDirectory) }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Counts `metadata` reads so a test can prove a marker-absent reconfirm on a high-grace backend issues
/// only a small bounded number of version.json probes rather than polling the full grace ceiling.
private actor MetadataCountingClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated let graceSeconds: TimeInterval
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { graceSeconds }

    private let inner: InMemoryRemoteStorageClient
    private var metadataCallCount = 0

    init(inner: InMemoryRemoteStorageClient, grace: TimeInterval) {
        self.inner = inner
        self.graceSeconds = grace
    }

    func metadataCalls() -> Int { metadataCallCount }

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? {
        metadataCallCount += 1
        return try await inner.metadata(path: path)
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
}

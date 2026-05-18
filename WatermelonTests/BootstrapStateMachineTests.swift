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
        // .watermelon/repo.json written but version.json failed
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "abcd")

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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "abcd")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion))
    }

    func testWatermelonPresent_versionHigher_returnsUnsupported() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "abcd")
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "future-id")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, formatVersion: 99, minAppVersion: "9.9.9")
        await client.injectListError(.transport, for: RepoLayout.migrationsDirectoryPath(base: basePath))

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .unsupported(minAppVersion: "9.9.9"))
    }

    func testFormatVersionBelowTwo_doesNotParseMarkersOrDetectV1Manifests() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "legacy-format")
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

    func testMigrationInProgressMarker_withV1Manifests_forcesV1() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        // Plant a migration marker.
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: "A")
        await client.injectFile(path: markerPath, contents: #"{"v":1,"writer_id":"A","started_at_ms":0}"#)
        // Phase3 cleanup has not finished.
        await TestFixtures.injectV1ManifestSentinel(client, basePath: basePath, year: 2025, month: 6)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2WithV1Manifests(formatVersion: RepoLayout.formatVersion),
                       "marker + V1 manifests after V2 sentinel → V2-with-V1-residue forces foreground migration")
    }

    func testStaleMigrationMarker_noV1Manifests_returnsV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: "A")
        await client.injectFile(path: markerPath, contents: #"{"v":1,"writer_id":"A","started_at_ms":0}"#)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion), "stale marker without V1 manifests → V2")
    }

    func testPhase1ResidueMarker_noV1Manifests_routesToCleanup() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
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

    func testVersionAbsentRawMarker_noV2Data_returnsFresh() async throws {
        let (client, profile) = await makeFixture()
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        let writerID = "34343434-3434-3434-3434-343434343434"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        await client.injectFile(path: markerPath, contents: "not-json")
        await client.injectPersistentDownloadError(.transport, for: markerPath)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .fresh)
    }

    func testWatermelonPresent_versionJsonGarbage_throwsDamagedV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "abcd")
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "abcd")
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "abcd")
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "abcd")
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
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "abcd")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        await client.injectDownloadError(.transport, for: RepoLayout.versionFilePath(base: basePath))
        do {
            _ = try await format.inspectRemoteFormat(client: client, profile: profile)
            XCTFail("expected transport error to propagate")
        } catch {
            // expected — caller can retry rather than locking the repo as unsupported
        }
    }

    func testHijackedMigrationMarker_isSkippedAndRoutesToV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let filenameWriterID = "11111111-1111-1111-1111-111111111111"
        let hijackJSONWriterID = "22222222-2222-2222-2222-222222222222"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: filenameWriterID)
        await client.injectFile(
            path: markerPath,
            contents: #"{"v":2,"writer_id":"\#(hijackJSONWriterID)","phase":2}"#
        )

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "hijacked marker must be skipped, never adopted as cleanup ownerWriterID")
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

    func testWrongTypePhaseMarker_isSkippedAndRoutesToV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let writerID = "99999999-9999-9999-9999-999999999999"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        await client.injectFile(
            path: markerPath,
            contents: #"{"v":2,"writer_id":"\#(writerID)","phase":"2"}"#
        )

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "wrong-type phase must be skipped, never routed to cleanup")
    }

    func testBooleanPhaseMarker_isSkippedAndRoutesToV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let writerID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: writerID)
        await client.injectFile(
            path: markerPath,
            contents: #"{"v":2,"writer_id":"\#(writerID)","phase":true}"#
        )

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion),
                       "boolean phase must be skipped, never routed to cleanup")
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
}

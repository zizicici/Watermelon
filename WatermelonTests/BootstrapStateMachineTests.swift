import XCTest
@testable import Watermelon

/// Decision-table tests for `inspectRemoteFormat`. Each scenario stages a specific
/// half-bootstrapped / half-migrated remote state and asserts the routing.
///
/// The bootstrap state machine has been the source of multiple data-loss / data-stuck
/// bugs across review iterations (V1 mid-migration → fresh; V2 missing repo.json →
/// per-session UUID; etc.). These tests lock in the recovery decisions so future
/// refactors can't silently regress them.
final class BootstrapStateMachineTests: XCTestCase {
    private let basePath = "/repo"
    private let format = RemoteFormatCompatibilityService()

    // Decision table:
    // | basePath dir? | .watermelon/ | repo.json | version.json | V1 manifests | → outcome
    // |---|---|---|---|---|---|
    // | absent (empty list) | -            | -          | -          | -          | → .fresh (basePath empty)
    // | exists | absent       | -          | -          | -          | → .fresh
    // | exists | absent       | -          | -          | present    | → .v1
    // | exists | present      | absent     | absent     | absent     | → .fresh (idempotent re-bootstrap)
    // | exists | present      | absent     | absent     | present    | → .v1 (interrupted V1 phase1)
    // | exists | present      | present    | absent     | absent     | → .fresh (re-bootstrap completes version)
    // | exists | present      | present    | absent     | present    | → .v1 (interrupted V1 phase2)
    // | exists | present      | present    | present (v=2) | -       | → .v2(2)
    // | exists | present      | present    | present (v=99) | -      | → .unsupported

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

    /// Marker present + identity files (repo.json/version.json) gone but commits/
    /// or snapshots/ still hold data = damaged V2. Treating it as fresh would
    /// mint a new repoID and orphan all existing commits.
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

    /// Marker with V1 data means phase3 crashed before migration cleanup completed.
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

    /// Marker without V1 data must not strand a healthy V2 repo in migration mode.
    func testStaleMigrationMarker_noV1Manifests_returnsV2() async throws {
        let (client, profile) = await makeFixture()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: "id-A")
        try await TestFixtures.injectVersionJSON(client, basePath: basePath)
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: "A")
        await client.injectFile(path: markerPath, contents: #"{"v":1,"writer_id":"A","started_at_ms":0}"#)

        let outcome = try await format.inspectRemoteFormat(client: client, profile: profile)
        XCTAssertEqual(outcome, .v2(formatVersion: RepoLayout.formatVersion), "stale marker without V1 manifests → V2")
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

    private func makeFixture() async -> (InMemoryRemoteStorageClient, ServerProfileRecord) {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        let profile = TestFixtures.makeServerProfile(
            id: 1, name: "Test", storageType: .webdav,
            host: "host", port: 0, shareName: "", basePath: basePath, username: ""
        )
        return (client, profile)
    }
}

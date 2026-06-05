import XCTest
@testable import Watermelon

/// Pure decision-table coverage for `RepoFormatAnalyzer`: every `RemoteFormatInspection` route and
/// fail-closed boundary is exercised against canned evidence, with no remote client. Mirrors the routes
/// the end-to-end `BootstrapStateMachineTests` exercise through the live evidence implementation, but
/// isolates the deterministic mapping so a logic regression surfaces here without remote-read noise.
final class RepoFormatAnalyzerTests: XCTestCase {
    private let analyzer = RepoFormatAnalyzer()

    // MARK: - Marker-absent routes

    func testMarkerAbsent_noV1_returnsFresh() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onMarkerPresent = { false }
        evidence.onHasV1Manifests = { false }
        evidence.onLoadVersion = { XCTFail("marker-absent must not load version"); return .absent }
        evidence.onMigrationDirectoryEntries = { XCTFail("marker-absent must not list migrations"); return [] }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .fresh)
    }

    func testMarkerAbsent_withV1_returnsV1() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onMarkerPresent = { false }
        evidence.onHasV1Manifests = { true }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v1)
    }

    // MARK: - Version-found routes

    func testVersionFound_clean_returnsV2() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(2) }
        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v2(formatVersion: 2))
    }

    func testVersionFound_higherThanSupported_returnsUnsupported_withoutListingMigrations() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(99, minApp: "9.9.9") }
        evidence.onMigrationDirectoryEntries = { XCTFail("future version must short-circuit before listing migrations"); return [] }
        evidence.onHasV1Manifests = { XCTFail("future version must not detect V1"); return false }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .unsupported(minAppVersion: "9.9.9"))
    }

    func testVersionFound_belowTwo_returnsUnsupported() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(1, minApp: "1.0.0") }
        // fv=1 is not > supported, so it reaches analyzeVersionFound; markers/V1 must not be probed.
        evidence.onParseMigrationMarkers = { _ in XCTFail("fv<2 must not parse markers"); return [] }
        evidence.onHasV1Manifests = { XCTFail("fv<2 must not detect V1"); return false }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .unsupported(minAppVersion: "1.0.0"))
    }

    func testVersionFound_withV1Manifests_returnsV2WithV1Manifests() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(2) }
        evidence.onMigrationDirectoryEntries = { [Self.fileMarkerEntry(writerID: "w0")] }
        evidence.onParseMigrationMarkers = { _ in [Self.marker("w0", .phase3)] }
        evidence.onHasV1Manifests = { true }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v2WithV1Manifests(formatVersion: 2))
    }

    func testVersionFound_cleanupSafeMarker_routesToCleanup() async throws {
        let writerID = "11111111-1111-1111-1111-111111111111"
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(2) }
        evidence.onMigrationDirectoryEntries = { [Self.fileMarkerEntry(writerID: writerID)] }
        evidence.onParseMigrationMarkers = { _ in [Self.marker(writerID, .phase3)] }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v2WithPendingMigrationCleanup(formatVersion: 2, ownerWriterID: writerID))
    }

    func testVersionFound_phase1ResidueMarker_routesToCleanup() async throws {
        let writerID = "23232323-2323-2323-2323-232323232323"
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(2) }
        evidence.onMigrationDirectoryEntries = { [Self.fileMarkerEntry(writerID: writerID)] }
        // phase1 is NOT cleanup-safe; the analyzer falls through to the residue branch (still cleanup).
        evidence.onParseMigrationMarkers = { _ in [Self.marker(writerID, .phase1)] }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v2WithPendingMigrationCleanup(formatVersion: 2, ownerWriterID: writerID))
    }

    func testVersionFound_markerSort_selectsHighestPhaseLowestWriterID() async throws {
        let phase2Writer = "11111111-1111-1111-1111-111111111111"
        let phase3LowWriter = "22222222-2222-2222-2222-222222222222"
        let phase3HighWriter = "33333333-3333-3333-3333-333333333333"
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(2) }
        evidence.onMigrationDirectoryEntries = {
            [
                Self.fileMarkerEntry(writerID: phase2Writer),
                Self.fileMarkerEntry(writerID: phase3LowWriter),
                Self.fileMarkerEntry(writerID: phase3HighWriter)
            ]
        }
        evidence.onParseMigrationMarkers = { _ in
            [
                Self.marker(phase2Writer, .phase2),
                Self.marker(phase3HighWriter, .phase3),
                Self.marker(phase3LowWriter, .phase3)
            ]
        }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v2WithPendingMigrationCleanup(formatVersion: 2, ownerWriterID: phase3LowWriter))
    }

    func testVersionFound_directoryShapedMarker_throwsDamaged() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(2) }
        evidence.onMigrationDirectoryEntries = { [Self.directoryMarkerEntry(writerID: "44444444-4444-4444-4444-444444444444")] }
        evidence.onParseMigrationMarkers = { _ in [] }

        await assertDamaged { try await self.analyzer.analyze(evidence: evidence) }
    }

    func testVersionFound_invalidMarkerParse_throwsDamaged() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { Self.version(2) }
        evidence.onMigrationDirectoryEntries = { [Self.fileMarkerEntry(writerID: "w0")] }
        evidence.onParseMigrationMarkers = { _ in throw MigrationMarkerStore.InvalidMarker(path: "/x", reason: "bad") }

        await assertDamaged { try await self.analyzer.analyze(evidence: evidence) }
    }

    // MARK: - Version-absent routes

    func testVersionAbsent_v2DataWithV1_returnsV2WithV1Manifests() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { .absent }
        evidence.onHasV1Manifests = { true }
        evidence.onHasV2DataDirectories = { true }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v2WithV1Manifests(formatVersion: RepoLayout.formatVersion))
    }

    func testVersionAbsent_v2DataNoV1_withMigrationMarker_routesToCleanup() async throws {
        let writerID = "55555555-5555-5555-5555-555555555555"
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { .absent }
        evidence.onHasV1Manifests = { false }
        evidence.onHasV2DataDirectories = { true }
        evidence.onMigrationDirectoryEntries = { [Self.fileMarkerEntry(writerID: writerID)] }
        evidence.onParseMigrationMarkers = { _ in [Self.marker(writerID, .phase3)] }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v2WithPendingMigrationCleanup(formatVersion: RepoLayout.formatVersion, ownerWriterID: writerID))
    }

    func testVersionAbsent_v2DataNoV1_noMigration_throwsDamaged() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { .absent }
        evidence.onHasV1Manifests = { false }
        evidence.onHasV2DataDirectories = { true }
        evidence.onMigrationDirectoryEntries = { [] }

        await assertDamaged { try await self.analyzer.analyze(evidence: evidence) }
    }

    func testVersionAbsent_noV2Data_noV1_noMarkers_returnsFresh() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { .absent }
        evidence.onHasV1Manifests = { false }
        evidence.onHasV2DataDirectories = { false }
        evidence.onMigrationDirectoryEntries = { [] }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .fresh)
    }

    func testVersionAbsent_noV2Data_withV1_returnsV1() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { .absent }
        evidence.onHasV1Manifests = { true }
        evidence.onHasV2DataDirectories = { false }
        evidence.onMigrationDirectoryEntries = { [] }

        let outcome = try await analyzer.analyze(evidence: evidence)
        XCTAssertEqual(outcome, .v1)
    }

    func testVersionAbsent_noV2Data_directoryShapedMarker_throwsDamaged() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { .absent }
        evidence.onHasV1Manifests = { false }
        evidence.onHasV2DataDirectories = { false }
        evidence.onMigrationDirectoryEntries = { [Self.directoryMarkerEntry(writerID: "66666666-6666-6666-6666-666666666666")] }

        await assertDamaged { try await self.analyzer.analyze(evidence: evidence) }
    }

    func testVersionAbsent_noV2Data_invalidFileMarker_throwsDamaged() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { .absent }
        evidence.onHasV1Manifests = { false }
        evidence.onHasV2DataDirectories = { false }
        evidence.onMigrationDirectoryEntries = { [Self.fileMarkerEntry(writerID: "w0")] }
        evidence.onParseMigrationMarkers = { _ in throw MigrationMarkerStore.InvalidMarker(path: "/x", reason: "bad") }

        await assertDamaged { try await self.analyzer.analyze(evidence: evidence) }
    }

    // MARK: - Version-load failures map to damaged

    func testLoadVersionConflict_throwsDamaged() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { throw RepoBootstrap.VersionConflict.unreadable(nil) }

        await assertDamaged { try await self.analyzer.analyze(evidence: evidence) }
    }

    func testLoadVersionIOFailure_throwsDamaged() async throws {
        let evidence = FakeFormatEvidence()
        evidence.onLoadVersion = { throw RepoBootstrap.BootstrapError.ioFailure(NSError(domain: "x", code: 1)) }

        await assertDamaged { try await self.analyzer.analyze(evidence: evidence) }
    }

    // MARK: - Helpers

    private func assertDamaged(
        _ body: @escaping () async throws -> RemoteFormatInspection,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        do {
            let outcome = try await body()
            XCTFail("expected damagedV2Repo, got \(outcome)", file: file, line: line)
        } catch BackupCompatibilityError.damagedV2Repo {
            // expected
        } catch {
            XCTFail("expected damagedV2Repo, got \(error)", file: file, line: line)
        }
    }

    private static func version(_ formatVersion: Int, minApp: String? = nil) -> VersionManifestStore.Load {
        .found(VersionManifest(
            formatVersion: formatVersion,
            minAppVersion: minApp,
            createdAtMs: nil,
            createdByWriter: nil
        ))
    }

    private static func marker(_ writerID: String, _ phase: MigrationMarkerPhase) -> ParsedMigrationMarker {
        ParsedMigrationMarker(writerID: writerID, phase: phase, runID: "run", startedAtMs: 0, lastStepMs: 0)
    }

    private static func fileMarkerEntry(writerID: String) -> RemoteStorageEntry {
        RemoteStorageEntry(
            path: "/repo/.watermelon/migrations/\(writerID).json",
            name: "\(writerID).json",
            isDirectory: false,
            size: 0,
            creationDate: nil,
            modificationDate: nil
        )
    }

    private static func directoryMarkerEntry(writerID: String) -> RemoteStorageEntry {
        RemoteStorageEntry(
            path: "/repo/.watermelon/migrations/\(writerID).json",
            name: "\(writerID).json",
            isDirectory: true,
            size: 0,
            creationDate: nil,
            modificationDate: nil
        )
    }
}

/// Canned `RepoFormatEvidenceProviding` whose per-query closures default to the "clean V2" shape and can
/// be overridden per test. Closures that must not be reached are set to `XCTFail`, pinning the analyzer's
/// lazy read ordering (e.g. future-version short-circuit, marker-absent skipping the version load).
private final class FakeFormatEvidence: RepoFormatEvidenceProviding, @unchecked Sendable {
    var onMarkerPresent: () async throws -> Bool = { true }
    var onLoadVersion: () async throws -> VersionManifestStore.Load = { .absent }
    var onMigrationDirectoryEntries: () async throws -> [RemoteStorageEntry] = { [] }
    var onParseMigrationMarkers: ([RemoteStorageEntry]) async throws -> [ParsedMigrationMarker] = { _ in [] }
    var onHasV1Manifests: () async throws -> Bool = { false }
    var onHasV2DataDirectories: () async throws -> Bool = { false }

    func markerPresent() async throws -> Bool { try await onMarkerPresent() }
    func loadVersion() async throws -> VersionManifestStore.Load { try await onLoadVersion() }
    func migrationDirectoryEntries() async throws -> [RemoteStorageEntry] { try await onMigrationDirectoryEntries() }
    func parseMigrationMarkers(_ entries: [RemoteStorageEntry]) async throws -> [ParsedMigrationMarker] {
        try await onParseMigrationMarkers(entries)
    }
    func hasV1Manifests() async throws -> Bool { try await onHasV1Manifests() }
    func hasV2DataDirectories() async throws -> Bool { try await onHasV2DataDirectories() }
}

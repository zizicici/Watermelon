import XCTest
@testable import Watermelon

final class V1MigrationResidueQuarantineTests: XCTestCase {
    private let basePath = "/repo"


    func testQuarantine_destinationAbsent_movesToCanonicalResidue() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let payload = Data("src-bytes".utf8)
        await client.injectFile(path: sourcePath, data: payload)

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let sourceGone = await client.hasFile(sourcePath) == false
        let residueExists = await client.hasFile(residuePath)
        XCTAssertTrue(sourceGone, "source must be deleted after quarantine")
        XCTAssertTrue(residueExists, "canonical residue must be created at \(residuePath)")
        let residueBytes = await client.snapshotFiles()[residuePath]
        XCTAssertEqual(residueBytes, payload)
    }

    func testQuarantine_existingResidueByteEqual_deletesSource() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let shared = Data("identical".utf8)
        await client.injectFile(path: residuePath, data: shared)
        await client.injectFile(path: sourcePath, data: shared)

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)

        let sourceGone = await client.hasFile(sourcePath) == false
        let residueIntact = await client.snapshotFiles()[residuePath]
        XCTAssertTrue(sourceGone)
        XCTAssertEqual(residueIntact, shared)
    }

    func testQuarantine_existingResidueDivergent_writesUniqueResidue() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let existingBytes = Data("existing".utf8)
        let sourceBytes = Data("divergent".utf8)
        await client.injectFile(path: residuePath, data: existingBytes)
        await client.injectFile(path: sourcePath, data: sourceBytes)

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)

        let sourceGone = await client.hasFile(sourcePath) == false
        XCTAssertTrue(sourceGone)
        let snapshot = await client.snapshotFiles()
        XCTAssertEqual(snapshot[residuePath], existingBytes, "existing residue must be preserved")
        let uniquePrefix = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)."
        let uniqueEntries = snapshot.filter { $0.key.hasPrefix(uniquePrefix) }
        XCTAssertEqual(uniqueEntries.count, 1, "exactly one unique residue must be created")
        XCTAssertEqual(uniqueEntries.first?.value, sourceBytes)
    }

    func testQuarantine_nonExclusiveMove_copyVerifyDeleteFallback() async throws {
        let client = InMemoryRemoteStorageClient()
        // Default is `.overwritePossible`, but make it explicit for the test record.
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        try await client.connect()

        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let payload = Data("copy-fallback".utf8)
        await client.injectFile(path: sourcePath, data: payload)

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)

        let sourceGone = await client.hasFile(sourcePath) == false
        XCTAssertTrue(sourceGone)
        let snapshot = await client.snapshotFiles()
        let canonical = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        XCTAssertNil(snapshot[canonical], "non-exclusive path must skip the canonical residue and write a `.uuid` variant")
        let uniquePrefix = "\(canonical)."
        let uniqueEntries = snapshot.filter { $0.key.hasPrefix(uniquePrefix) }
        XCTAssertEqual(uniqueEntries.count, 1)
        XCTAssertEqual(uniqueEntries.first?.value, payload)
    }

    func testQuarantine_peerDeletedSourceMidMove_returnsSilently() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        await client.injectFile(path: sourcePath, data: Data("victim".utf8))

        await client.setPreMoveSourceMutation { src in
            try? await client.delete(path: src)
        }

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)

        let sourceGone = await client.hasFile(sourcePath) == false
        XCTAssertTrue(sourceGone)
        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let residueExists = await client.hasFile(residuePath)
        XCTAssertFalse(residueExists, "no orphan must be produced when source vanished mid-operation")
    }

    func testQuarantine_bestEffortRetry_postWriteVerifyDeletesSource() async throws {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()

        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let payload = Data("best-effort-bytes".utf8)
        await client.injectFile(path: sourcePath, data: payload)

        await client.setMoveIfAbsentOutcomeOverride(.bestEffortRetry)

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)

        let sourceGone = await client.hasFile(sourcePath) == false
        XCTAssertTrue(sourceGone, "finishBestEffortResidueMove must delete source after equality verification")
        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let residueBytes = await client.snapshotFiles()[residuePath]
        XCTAssertEqual(residueBytes, payload)
    }


    func testSweep_partialMigrationMarkerPresent_preservesResidue() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await client.injectFile(path: residuePath, data: Data("residue".utf8))
        await client.injectFile(path: markerPath, data: Data("{}".utf8))

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests()

        let residueSurvived = await client.hasFile(residuePath)
        let markerSurvived = await client.hasFile(markerPath)
        XCTAssertTrue(residueSurvived, "partial-migration marker must gate retention")
        XCTAssertTrue(markerSurvived)
    }

    // Bug-IX P01 R05 Codex A / Checker Finding 2: a directory squatting at the reserved
    // partial-migration marker path must gate residue deletion. This matches the R04 (F10)
    // fail-closed handling in RepoRetentionDeletePreflightService and
    // RepoSnapshotDeletePreflightService — a damaged marker is not proof of marker absence.
    func testSweep_partialMigrationMarkerAsDirectory_preservesResidue() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await client.injectFile(path: residuePath, data: Data("residue".utf8))
        try await client.createDirectory(path: markerPath)

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests()

        let residueSurvived = await client.hasFile(residuePath)
        XCTAssertTrue(
            residueSurvived,
            "a directory-shaped partial-migration marker must preserve residue; deleting it would lose evidence the later destructive preflights need"
        )
    }

    func testSweep_noPartialMigrationMarker_deletesResidue() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        await client.injectFile(path: residuePath, data: Data("residue".utf8))

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests()

        let residueGone = await client.hasFile(residuePath) == false
        XCTAssertTrue(residueGone)
    }

    func testSweep_missingBase_doesNotThrow() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests()
    }
}

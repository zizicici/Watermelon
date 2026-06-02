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

    // Bug-IX P04 R04 CodexReviewerA F1: stale listing omits partial-migration marker while
    // residue files are visible → must not delete residue. Metadata probe confirms marker.
    func testSweep_staleListingOmitsMarker_preservesResidue() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        let client = StaleListingClient(inner: inner, hiddenNames: [V1MigrationResidueFileNames.partialMigrationMarkerFileName])

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests()

        let residueSurvived = await inner.hasFile(residuePath)
        let markerSurvived = await inner.hasFile(markerPath)
        XCTAssertTrue(residueSurvived, "stale listing must not cause residue deletion when marker is still present")
        XCTAssertTrue(markerSurvived)
    }

    // Bug-IX P04 R05 CodexReviewerA F1: stale listing omits marker, metadata probe throws
    // non-not-found error → sweep must not delete residue when marker status is uncertain.
    func testSweep_staleListingOmitsMarker_metadataFault_preservesResidue() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        let client = StaleListingClient(
            inner: inner,
            hiddenNames: [V1MigrationResidueFileNames.partialMigrationMarkerFileName],
            metadataFaultPaths: [markerPath]
        )

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests()

        let residueSurvived = await inner.hasFile(residuePath)
        XCTAssertTrue(residueSurvived, "inconclusive metadata probe must not cause residue deletion")
    }

    // Bug-X P07 R01 CodexChecker F1: on a grace backend a partial-migration marker written
    // earlier in this run can lag BOTH the listing and the metadata probe; the single
    // non-grace probe then reads a stale not-found and the sweep deletes the residue. Months
    // this run wrote a marker for are passed in `preserveMonthRelPaths` and must be preserved.
    func testSweep_markerLagsListingAndMetadata_writtenThisRun_preservesResidue() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        let client = StaleListingClient(
            inner: inner,
            hiddenNames: [V1MigrationResidueFileNames.partialMigrationMarkerFileName],
            metadataNotFoundPaths: [markerPath]
        )

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests(preserveMonthRelPaths: ["2024/03"])

        let residueSurvived = await inner.hasFile(residuePath)
        XCTAssertTrue(
            residueSurvived,
            "a marker written this run must gate retention even when listing and metadata both lag within read-after-write grace"
        )
    }

    // Contrast: with the same listing+metadata lag but the month NOT recorded as written this
    // run (peer/cross-run cleanup path), behavior is unchanged — the sweep relies on the marker
    // being reliably visible, so a genuinely-absent marker still permits residue deletion.
    func testSweep_markerLagsListingAndMetadata_notWrittenThisRun_deletesResidue() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        let client = StaleListingClient(
            inner: inner,
            hiddenNames: [V1MigrationResidueFileNames.partialMigrationMarkerFileName],
            metadataNotFoundPaths: [markerPath]
        )

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests(preserveMonthRelPaths: [])

        let residueGone = await inner.hasFile(residuePath) == false
        XCTAssertTrue(residueGone, "default sweep behavior must be unchanged when no marker was written this run")
    }

    // Bug-X P07 R02 CodexChecker F1: cross-run cleanup-only resume runs a fresh service with an
    // empty `preserveMonthRelPaths` set, so the R01 same-run fix can't protect it. A partial marker
    // written just before the interrupt can lag both LIST and metadata within read-after-write
    // grace. With the cross-run deadline set, the sweep must poll until the marker surfaces and
    // preserve the residue rather than deleting it on the first stale not-found.
    func testSweep_crossRunMarkerLagsThenAppears_preservesResidue() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setReadAfterWriteGrace(30)
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        // Marker hidden from LIST always; metadata 404s on the first probe, then reveals.
        let client = LaggyMarkerMetadataClient(
            inner: inner,
            markerPath: markerPath,
            markerName: V1MigrationResidueFileNames.partialMigrationMarkerFileName,
            notFoundProbeCount: 1
        )

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests(
            preserveMonthRelPaths: [],
            crossRunMarkerVisibilityDeadline: Date().addingTimeInterval(5)
        )

        let residueSurvived = await inner.hasFile(residuePath)
        XCTAssertTrue(
            residueSurvived,
            "cross-run cleanup must poll within grace and preserve residue once the lagging marker surfaces"
        )
    }

    // Contrast: a fully-migrated month has residue but never had a partial marker. After the shared
    // grace deadline confirms genuine absence, the cross-run sweep must still delete the residue —
    // the fix must not turn into a permanent residue leak.
    func testSweep_crossRunMarkerGenuinelyAbsent_deletesResidueAfterDeadline() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setReadAfterWriteGrace(30)
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))

        let quarantine = V1MigrationResidueQuarantine(client: inner, basePath: basePath)
        try await quarantine.sweepResidueManifests(
            preserveMonthRelPaths: [],
            crossRunMarkerVisibilityDeadline: Date().addingTimeInterval(0.4)
        )

        let residueGone = await inner.hasFile(residuePath) == false
        XCTAssertTrue(
            residueGone,
            "cross-run cleanup must still sweep fully-migrated residue once the grace window confirms genuine marker absence"
        )
    }

    // Bug-X P07 R03 ClaudeReviewerC F1: an interrupted multi-month migration resumes via
    // runFullMigration (a later month still has a live manifest), so the sweep sees BOTH a
    // prior-run partial-marker residue month (NOT scanned this run) and this run's freshly
    // migrated clean residue. The cross-run deadline must protect the prior-run residue under
    // marker lag, while the same-run set lets this run's clean residue sweep without paying the
    // grace window — otherwise every healthy migration would stall one grace window.
    func testSweep_crossRun_priorRunMarkerLags_preserved_sameRunCleanResidueDeleted() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setReadAfterWriteGrace(30)
        try await inner.connect()

        // Prior-run partial-marker month: residue + marker, marker lags LIST + first metadata probe.
        let priorResidue = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let priorMarker = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: priorResidue, data: Data("prior-residue".utf8))
        await inner.injectFile(path: priorMarker, data: Data("{}".utf8))
        // This-run cleanly-migrated month: residue only, no marker ever written.
        let sameRunResidue = "\(basePath)/2024/04/\(V1MigrationResidueFileNames.residueManifestFileName)"
        await inner.injectFile(path: sameRunResidue, data: Data("same-run-residue".utf8))

        let client = LaggyMarkerMetadataClient(
            inner: inner,
            markerPath: priorMarker,
            markerName: V1MigrationResidueFileNames.partialMigrationMarkerFileName,
            notFoundProbeCount: 1
        )

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests(
            preserveMonthRelPaths: [],
            sameRunProcessedMonthRelPaths: ["2024/04"],
            crossRunMarkerVisibilityDeadline: Date().addingTimeInterval(5)
        )

        let priorSurvived = await inner.hasFile(priorResidue)
        XCTAssertTrue(
            priorSurvived,
            "prior-run residue (not scanned this run) must be preserved once its lagging partial marker surfaces"
        )
        let sameRunGone = await inner.hasFile(sameRunResidue) == false
        XCTAssertTrue(
            sameRunGone,
            "a month migrated cleanly this run has no marker by authoritative in-memory knowledge — its residue sweeps without the grace probe"
        )
    }

    // Same as above but proves the same-run clean month skips the probe even when a (lagging)
    // marker physically exists: a month recorded as processed-this-run is trusted from the
    // in-memory set, so `partialMarkerVisible` is never invoked for it (probeCount stays 0).
    func testSweep_sameRunProcessedMonth_skipsCrossRunGraceProbe() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setReadAfterWriteGrace(30)
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        // Marker reveals only after a not-found probe — if the sweep probed, it would preserve.
        let client = LaggyMarkerMetadataClient(
            inner: inner,
            markerPath: markerPath,
            markerName: V1MigrationResidueFileNames.partialMigrationMarkerFileName,
            notFoundProbeCount: 1
        )

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.sweepResidueManifests(
            preserveMonthRelPaths: [],
            sameRunProcessedMonthRelPaths: ["2024/03"],
            crossRunMarkerVisibilityDeadline: Date().addingTimeInterval(5)
        )

        let residueGone = await inner.hasFile(residuePath) == false
        XCTAssertTrue(residueGone, "same-run residue is swept on in-memory authority alone")
        XCTAssertEqual(client.probeCount, 0, "a same-run month must not pay the cross-run marker grace probe")
    }

    // Bug-IX P04 R06 CodexChecker F2: metadata probe must propagate CancellationError
    // instead of swallowing it and continuing.
    func testSweep_staleListingOmitsMarker_metadataCancellation_propagates() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let markerPath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.partialMigrationMarkerFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        let client = StaleListingClient(
            inner: inner,
            hiddenNames: [V1MigrationResidueFileNames.partialMigrationMarkerFileName],
            metadataCancellationPaths: [markerPath]
        )

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        do {
            try await quarantine.sweepResidueManifests()
            XCTFail("expected CancellationError to propagate")
        } catch is CancellationError {
            // expected
        }
        let residueSurvived = await inner.hasFile(residuePath)
        XCTAssertTrue(residueSurvived, "cancellation must not cause residue deletion")
    }

    // Bug-IX P04 R15 CodexReviewerA F1: sweep must not consider cleanup complete when
    // a backend reports delete success but the residue file remains visible.
    func testSweep_noPartialMigrationMarker_noOpDelete_throwsIncomplete() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        await inner.injectFile(path: residuePath, data: Data("residue".utf8))

        let client = NoOpDeleteClient(inner: inner, noOpDeletePaths: [residuePath])

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        do {
            try await quarantine.sweepResidueManifests()
            XCTFail("sweep must throw when residue survives a no-op delete")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "V1MigrationService")
            XCTAssertEqual(error.code, -33)
        }

        let residueSurvived = await inner.hasFile(residuePath)
        XCTAssertTrue(residueSurvived, "residue must still be present after no-op delete")
    }

    // Bug-IX P04 R07 ClaudeReviewerA F1 / CodexReviewerB F1: equal-residue fast path must
    // verify source deletion when the remote reports success but the file remains visible.
    func testQuarantine_existingResidueEqual_noOpDelete_throwsIncomplete() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let shared = Data("identical".utf8)
        await inner.injectFile(path: residuePath, data: shared)
        await inner.injectFile(path: sourcePath, data: shared)

        let client = NoOpDeleteClient(inner: inner, noOpDeletePaths: [sourcePath])

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        do {
            try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)
            XCTFail("quarantine must throw when source survives a no-op delete")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "V1MigrationService")
            XCTAssertEqual(error.code, -33)
        }

        let sourceSurvived = await inner.hasFile(sourcePath)
        XCTAssertTrue(sourceSurvived, "source must still be present after no-op delete")
        let residueIntact = await inner.snapshotFiles()[residuePath]
        XCTAssertEqual(residueIntact, shared, "residue must be untouched")
    }

    func testQuarantine_noExistingResidue_moveCreated_sourceSurvives_throwsIncomplete() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setMoveIfAbsentGuarantee(.exclusive)
        try await inner.connect()

        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let payload = Data("move-no-delete".utf8)
        await inner.injectFile(path: sourcePath, data: payload)

        let client = CopyOnlyMoveClient(inner: inner, leakySourcePaths: [sourcePath])

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)

        let sourceSurvived = await inner.hasFile(sourcePath)
        XCTAssertFalse(sourceSurvived, "source must be gone after the catch block repairs by deleting source")

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let residueBytes = await inner.snapshotFiles()[residuePath]
        XCTAssertEqual(residueBytes, payload, "destination residue must have been created")

        XCTAssertTrue(client.moveIfAbsentCalled, "moveIfAbsent must have been called")
    }

    func testQuarantine_copyFallback_deleteNotFound_sourceSurvives_throwsIncomplete() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setMoveIfAbsentGuarantee(.overwritePossible)
        try await inner.connect()

        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let payload = Data("copy-delete-notfound".utf8)
        await inner.injectFile(path: sourcePath, data: payload)

        let client = DeleteNotFoundClient(inner: inner, throwNotFoundFor: [sourcePath])

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        do {
            try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)
            XCTFail("quarantine must throw when source survives a delete-not-found in copy path")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "V1MigrationService")
            XCTAssertEqual(error.code, -33)
        }

        let sourceSurvived = await inner.hasFile(sourcePath)
        XCTAssertTrue(sourceSurvived, "source must still be present")
    }

    func testQuarantine_bestEffortRetry_deleteNotFound_sourceSurvives_throwsIncomplete() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setMoveIfAbsentGuarantee(.exclusive)
        try await inner.connect()

        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let payload = Data("besteffort-delete-notfound".utf8)
        await inner.injectFile(path: sourcePath, data: payload)

        await inner.setMoveIfAbsentOutcomeOverride(.bestEffortRetry)

        let client = DeleteNotFoundClient(inner: inner, throwNotFoundFor: [sourcePath])

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        do {
            try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)
            XCTFail("quarantine must throw when source survives a delete-not-found in best-effort path")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "V1MigrationService")
            XCTAssertEqual(error.code, -33)
        }

        let sourceSurvived = await inner.hasFile(sourcePath)
        XCTAssertTrue(sourceSurvived, "source must still be present")
    }

    func testQuarantine_existingDivergentResidue_moveCreated_sourceSurvives_throwsIncomplete() async throws {
        let inner = InMemoryRemoteStorageClient()
        inner.setMoveIfAbsentGuarantee(.exclusive)
        try await inner.connect()

        let residuePath = "\(basePath)/2024/03/\(V1MigrationResidueFileNames.residueManifestFileName)"
        let sourcePath = "\(basePath)/2024/03/.watermelon_manifest.sqlite"
        let existingBytes = Data("existing-residue".utf8)
        let sourceBytes = Data("new-source".utf8)
        await inner.injectFile(path: residuePath, data: existingBytes)
        await inner.injectFile(path: sourcePath, data: sourceBytes)

        let client = CopyOnlyMoveClient(inner: inner, leakySourcePaths: [sourcePath])

        let quarantine = V1MigrationResidueQuarantine(client: client, basePath: basePath)
        do {
            try await quarantine.quarantine(year: 2024, month: 3, sourcePath: sourcePath)
            XCTFail("quarantine must throw when source survives moveIfAbsent .created in unique residue path")
        } catch let error as NSError {
            XCTAssertEqual(error.domain, "V1MigrationService")
            XCTAssertEqual(error.code, -33)
        }

        let sourceSurvived = await inner.hasFile(sourcePath)
        XCTAssertTrue(sourceSurvived, "source must still be present")
        let residueIntact = await inner.snapshotFiles()[residuePath]
        XCTAssertEqual(residueIntact, existingBytes, "existing residue must be untouched")
    }
}

/// Client that hides specific file names from `list` results but preserves them for `metadata`.
/// Optionally injects non-not-found errors for specific metadata paths.
private final class StaleListingClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let hiddenNames: Set<String>
    private let metadataFaultPaths: Set<String>
    private let metadataCancellationPaths: Set<String>
    private let metadataNotFoundPaths: Set<String>

    init(inner: InMemoryRemoteStorageClient, hiddenNames: [String], metadataFaultPaths: [String] = [], metadataCancellationPaths: [String] = [], metadataNotFoundPaths: [String] = []) {
        self.inner = inner
        self.hiddenNames = Set(hiddenNames)
        self.metadataFaultPaths = Set(metadataFaultPaths)
        self.metadataCancellationPaths = Set(metadataCancellationPaths)
        self.metadataNotFoundPaths = Set(metadataNotFoundPaths)
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await inner.list(path: path).filter { !hiddenNames.contains($0.name) }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? {
        if metadataCancellationPaths.contains(path) {
            throw CancellationError()
        }
        if metadataFaultPaths.contains(path) {
            throw RemoteStorageClientError.unavailable
        }
        if metadataNotFoundPaths.contains(path) {
            throw NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError)
        }
        return try await inner.metadata(path: path)
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
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

/// Client that hides one marker path from `list` and returns not-found from `metadata` for its
/// first `notFoundProbeCount` probes, then reveals it — models a partial-migration marker whose
/// read-after-write visibility lags into a cross-run cleanup sweep before surfacing.
private final class LaggyMarkerMetadataClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let markerPath: String
    private let markerName: String
    private let notFoundProbeCount: Int
    private let lock = NSLock()
    private var probes = 0

    init(inner: InMemoryRemoteStorageClient, markerPath: String, markerName: String, notFoundProbeCount: Int) {
        self.inner = inner
        self.markerPath = markerPath
        self.markerName = markerName
        self.notFoundProbeCount = notFoundProbeCount
    }

    /// Number of metadata probes observed for the marker path — lets a test assert the sweep
    /// skipped the cross-run grace probe entirely for a same-run-processed month.
    var probeCount: Int { lock.withLock { probes } }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { inner.readAfterWriteGraceSeconds }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await inner.list(path: path).filter { $0.name != markerName }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? {
        if path == markerPath {
            let reveal = lock.withLock { () -> Bool in
                probes += 1
                return probes > notFoundProbeCount
            }
            if !reveal { return nil }
        }
        return try await inner.metadata(path: path)
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
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

/// Client that wraps InMemoryRemoteStorageClient but makes delete a no-op for
/// specific paths — simulates a remote backend that reports success without removing the file.
private final class NoOpDeleteClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let noOpDeletePaths: Set<String>

    init(inner: InMemoryRemoteStorageClient, noOpDeletePaths: [String]) {
        self.inner = inner
        self.noOpDeletePaths = Set(noOpDeletePaths)
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        if noOpDeletePaths.contains(path) { return }
        try await inner.delete(path: path)
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
}

private final class CopyOnlyMoveClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let leakySourcePaths: Set<String>
    private let _lock = NSLock()
    private var _moveIfAbsentCalled = false
    private var _deleteCalled = false

    var moveIfAbsentCalled: Bool { _lock.withLock { _moveIfAbsentCalled } }
    var deleteCalled: Bool { _lock.withLock { _deleteCalled } }

    init(inner: InMemoryRemoteStorageClient, leakySourcePaths: [String]) {
        self.inner = inner
        self.leakySourcePaths = Set(leakySourcePaths)
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        _lock.withLock { _deleteCalled = true }
        try await inner.delete(path: path)
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        _lock.withLock { _moveIfAbsentCalled = true }
        if leakySourcePaths.contains(sourcePath) {
            let snapshot = await inner.snapshotFiles()
            let normKey = "/" + sourcePath
                .trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
                .split(separator: "/", omittingEmptySubsequences: true)
                .joined(separator: "/")
            if let data = snapshot[normKey] {
                await inner.injectFile(path: destinationPath, data: data)
            }
            return .created
        }
        return try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
}

private final class DeleteNotFoundClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let throwNotFoundFor: Set<String>

    init(inner: InMemoryRemoteStorageClient, throwNotFoundFor: [String]) {
        self.inner = inner
        self.throwNotFoundFor = Set(throwNotFoundFor)
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { inner.moveIfAbsentGuarantee }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool {
        try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: destinationPath)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        if throwNotFoundFor.contains(path) {
            throw NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError)
        }
        try await inner.delete(path: path)
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
}

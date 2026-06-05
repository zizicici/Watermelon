import XCTest
@testable import Watermelon

/// P07-A2-AggressiveGC: aggressive checkpoint thresholds plus the additive accepted-baseline
/// coverage-attestation signal that lets a legacy month obtain a fresh attested baseline before the
/// aggressive commit-delete path opens. The conservative deletion safeguards (contiguous seq-1 prefix,
/// accepted/planner prefix intersection, body retention, clean gates, post-delete verification) are
/// unchanged and re-asserted here.
final class RepoAggressiveGCA2Tests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let otherWriterID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-a2-test"
    private let year = 2026
    private let monthValue = 5
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: monthValue) }
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    // MARK: - Coverage-attestation signal (materializer trust)

    func testFullReadCoverageAttestedAcceptsValidAttestedHeader() {
        let covered = covered([(1, 5)])
        let header = attestedHeader(covered: covered)
        let runIDPrefix = RepoLayout.runIDPrefix(runID)
        let digest = SnapshotCoverageDigest.filenameDigest(
            forHeader: header, month: monthKey, lamport: 7, runIDPrefix: runIDPrefix
        )
        let valid = try! XCTUnwrap(digest)
        let parsed = RepoLayout.ParsedSnapshotFilename(
            month: monthKey, lamport: 7, writerID: writerID, runIDPrefix: runIDPrefix, digest: valid
        )
        XCTAssertTrue(SnapshotCoverageDigest.fullReadCoverageAttested(header: header, parsed: parsed))
    }

    func testFullReadCoverageAttestedRejectsLegacyMissingAndMismatched() {
        let covered = covered([(1, 5)])
        let header = attestedHeader(covered: covered)
        let runIDPrefix = RepoLayout.runIDPrefix(runID)
        let valid = try! XCTUnwrap(SnapshotCoverageDigest.filenameDigest(
            forHeader: header, month: monthKey, lamport: 7, runIDPrefix: runIDPrefix
        ))

        // Legacy 4-segment filename (no digest) → not attested.
        let legacyParsed = RepoLayout.ParsedSnapshotFilename(
            month: monthKey, lamport: 7, writerID: writerID, runIDPrefix: runIDPrefix, digest: nil
        )
        XCTAssertFalse(SnapshotCoverageDigest.fullReadCoverageAttested(header: header, parsed: legacyParsed))

        // Digest mismatch → not attested.
        let wrongParsed = RepoLayout.ParsedSnapshotFilename(
            month: monthKey, lamport: 7, writerID: writerID, runIDPrefix: runIDPrefix,
            digest: String(repeating: "a", count: 64)
        )
        XCTAssertFalse(SnapshotCoverageDigest.fullReadCoverageAttested(header: header, parsed: wrongParsed))

        // Missing attestation on the header → not attested, even with a (now stale) valid-shaped digest.
        let legacyHeader = SnapshotHeader(
            version: SnapshotHeader.checkpointVersion, scope: CommitHeader.monthScope(monthKey),
            writerID: writerID, repoID: repoID, covered: covered, createdAtMs: nil, coverageAttestation: nil
        )
        let validParsed = RepoLayout.ParsedSnapshotFilename(
            month: monthKey, lamport: 7, writerID: writerID, runIDPrefix: runIDPrefix, digest: valid
        )
        XCTAssertFalse(SnapshotCoverageDigest.fullReadCoverageAttested(header: legacyHeader, parsed: validParsed))

        // Unsupported attestation version → not attested (defensive; the decoder also fails closed earlier).
        let unsupportedHeader = SnapshotHeader(
            version: SnapshotHeader.checkpointVersion, scope: CommitHeader.monthScope(monthKey),
            writerID: writerID, repoID: repoID, covered: covered, createdAtMs: nil,
            coverageAttestation: SnapshotCoverageAttestation(version: SnapshotCoverageAttestation.currentVersion + 1)
        )
        XCTAssertFalse(SnapshotCoverageDigest.fullReadCoverageAttested(header: unsupportedHeader, parsed: validParsed))
    }

    func testAttestedCheckpointBaselineMaterializesCoverageAttested() async throws {
        // The compaction checkpoint service writes a new-format attested baseline.
        let (client, _) = try await attestedBaselineRepoWithResidualCommits(seqs: 1...1)

        let output = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        let accepted = try XCTUnwrap(output.acceptedSnapshotBaselinesByMonth[monthKey])
        XCTAssertTrue(accepted.coverageAttested,
            "a new-format checkpoint authenticated by its filename digest must be coverage-attested")
    }

    func testLegacyBaselineMaterializesNotCoverageAttested() async throws {
        let client = try await makeConnectedClient()
        // A legacy (4-segment, no attestation) baseline written via SnapshotWriter directly.
        try await writeLegacyBaseline(client: client, low: 1, high: 1, lamport: 11, assetByte: 0xA1)

        let output = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        let accepted = try XCTUnwrap(output.acceptedSnapshotBaselinesByMonth[monthKey])
        XCTAssertFalse(accepted.coverageAttested,
            "a legacy 4-segment baseline must never be marked coverage-attested")
    }

    // MARK: - Service-level: legacy upgrade before aggressive delete

    func testLegacyMonthObtainsAttestedBaselineBeforeAggressiveDeleteBeyondLegacyProof() async throws {
        let client = try await makeConnectedClient()
        // Legacy faithful baseline covering [1,1] carrying fp(0x01); commit seq 1 is covered (legacy proof),
        // commit seq 2 is replayed beyond the baseline.
        try await writeLegacyBaseline(client: client, low: 1, high: 1, lamport: 11, assetByte: 0x01)
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0x01)
        try await writeAddCommit(client: client, seq: 2, clock: 2, assetByte: 0x02)

        let before = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(before.outcomeByMonth[monthKey], .clean)
        XCTAssertEqual(before.acceptedSnapshotBaselinesByMonth[monthKey]?.coverageAttested, false,
            "precondition: the only baseline is legacy/unattested")

        // A2 policy is the default (aggressive 1_000/4 MiB); replayed=1 is far below it, so ONLY the
        // legacy-upgrade signal can drive this checkpoint.
        let services = try await makeServices(client: client, policy: .default)
        let pass1 = try await RepoCompactionService(services: services).compactMonth(monthKey)

        XCTAssertEqual(pass1.outcome, .checkpointWritten,
            "a legacy baseline with a deletable covered commit must checkpoint to obtain an attested baseline")
        let afterPass1 = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(afterPass1.acceptedSnapshotBaselinesByMonth[monthKey]?.coverageAttested, true,
            "the freshly written baseline is new-format and coverage-attested")

        // Deletion this pass is bounded by legacy proof (the pre-checkpoint planner prefix [1,1]): seq 1 may
        // go, seq 2 (beyond legacy proof) must remain until a later pass runs against the attested baseline.
        let seq1Path = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 1)
        let seq2Path = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 2)
        let seq1Survived = await client.hasFile(seq1Path)
        let seq2Survived = await client.hasFile(seq2Path)
        XCTAssertFalse(seq1Survived, "the legacy-proof commit prefix is deletable this pass")
        XCTAssertTrue(seq2Survived,
            "the commit beyond legacy proof must not be deleted before an attested baseline covers it")

        // Pass 2: the attested baseline now covers [1,2]; no legacy upgrade fires, and the aggressive delete
        // path now reclaims seq 2 against the attested baseline.
        let pass2 = try await RepoCompactionService(services: services).compactMonth(monthKey)
        XCTAssertNotEqual(pass2.outcome, .checkpointWritten,
            "an already-attested baseline must not trigger a redundant legacy-upgrade checkpoint")
        let seq2SurvivedPass2 = await client.hasFile(seq2Path)
        XCTAssertFalse(seq2SurvivedPass2,
            "with an attested baseline in place, the residual commit beyond legacy proof is deletable")
    }

    func testAttestedBaselineDoesNotTriggerLegacyUpgradeCheckpoint() async throws {
        let client = try await makeConnectedClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0x01)
        let services = try await makeServices(client: client, policy: policy(checkpointCommitThreshold: 1))
        // Pass 1 writes the attested baseline covering [1,1] (no pre-existing baseline ⇒ no deletion yet).
        _ = try await RepoCompactionService(services: services).compactMonth(monthKey)

        // Pass 2: baseline is attested and fully covers the (single) residual commit. No fresh checkpoint
        // should be written; commit GC reclaims the residual prefix against the attested baseline.
        let services2 = try await makeServices(client: client, policy: policy(checkpointCommitThreshold: 1_000))
        let pass2 = try await RepoCompactionService(services: services2).compactMonth(monthKey)
        XCTAssertNotEqual(pass2.outcome, .checkpointWritten)
        let seq1Path = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: 1)
        let seq1Survived = await client.hasFile(seq1Path)
        XCTAssertFalse(seq1Survived, "covered commit is reclaimed against the attested baseline")
    }

    // MARK: - Service-level: aggressive delete equivalence + quantified reduction

    func testAggressiveDeleteThenMaterializeEqualsUndeletedBaseline() async throws {
        let (client, services) = try await attestedBaselineRepoWithResidualCommits(seqs: 1...3)

        // Snapshot the pre-delete authority: attested baseline + residual commits still present.
        let preDelete = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(preDelete.outcomeByMonth[monthKey], .clean)
        XCTAssertEqual(preDelete.state.months[monthKey]?.assets.count, 3)

        // Aggressive delete pass: the attested baseline now drives commit GC over the covered prefix.
        let result = try await RepoCompactionService(services: services).compactMonth(monthKey)
        guard case .completed(let summary, _, let verification) = try XCTUnwrap(result.commitCleanup) else {
            return XCTFail("expected commit GC to complete the aggressive delete, got \(String(describing: result.commitCleanup))")
        }
        XCTAssertFalse(summary.deleted.isEmpty, "the covered commit prefix must actually be deleted")
        if case .passed = verification {} else {
            XCTFail("post-delete verification must pass, got \(verification)")
        }

        let postDelete = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(postDelete.outcomeByMonth[monthKey], .clean,
            "the month stays clean after the covered prefix is deleted")
        XCTAssertEqual(postDelete.state.months[monthKey], preDelete.state.months[monthKey],
            "post-delete materialized state must equal the undeleted baseline state")
        XCTAssertEqual(postDelete.coveredByMonth[monthKey], preDelete.coveredByMonth[monthKey],
            "post-delete coverage must equal the undeleted baseline coverage")
    }

    func testAggressiveDeleteQuantifiesCommitFileCountReduction() async throws {
        let (client, services) = try await attestedBaselineRepoWithResidualCommits(seqs: 1...4)
        let before = await commitFileCount(client)
        XCTAssertEqual(before, 4, "precondition: four residual commit files remain after the baseline write")

        _ = try await RepoCompactionService(services: services).compactMonth(monthKey)

        let after = await commitFileCount(client)
        XCTAssertEqual(after, 0, "the entire covered commit prefix is reclaimed")
        XCTAssertLessThan(after, before, "aggressive GC must measurably reduce the commit metadata file count")
    }

    // MARK: - Service-level: corrupt sibling safety

    func testCorruptSnapshotSiblingDoesNotFalseCleanOrAllowUnsafeDelete() async throws {
        let (client, services) = try await attestedBaselineRepoWithResidualCommits(seqs: 1...3)
        // A single damaged (unparseable, unattested) sibling snapshot from another writer.
        let corruptPath = RepoLayout.snapshotFilePath(
            base: basePath, month: monthKey, lamport: 9_000, writerID: otherWriterID, runID: "bad-sibling"
        )
        await client.injectFile(path: corruptPath, data: Data("not-jsonl\n".utf8))

        let baselineState = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID).state.months[monthKey]

        let result = try await RepoCompactionService(services: services).compactMonth(monthKey)
        // The trusted attested baseline still carries the month; the damaged sibling is ignored for
        // authority, so commit GC proceeds bounded by the baseline rather than false-cleaning or over-deleting.
        guard case .completed(let summary, _, let verification) = try XCTUnwrap(result.commitCleanup) else {
            return XCTFail("commit GC must complete around the corrupt sibling, got \(String(describing: result.commitCleanup))")
        }
        XCTAssertFalse(summary.deleted.isEmpty)
        if case .passed = verification {} else { XCTFail("verification must pass, got \(verification)") }

        let corruptSurvived = await client.hasFile(corruptPath)
        XCTAssertTrue(corruptSurvived,
            "a corrupt sibling is ignored for authority but must not be deleted by commit GC")

        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(monthKey, expectedRepoID: repoID)
        XCTAssertEqual(after.outcomeByMonth[monthKey], .clean,
            "the month is genuinely clean from the trusted baseline, not false-cleaned by the corrupt sibling")
        XCTAssertEqual(after.state.months[monthKey], baselineState,
            "no asset is lost: post-delete state equals the baseline the corrupt sibling could not corrupt")
    }

    // MARK: - Helpers

    private func attestedBaselineRepoWithResidualCommits(
        seqs: ClosedRange<UInt64>
    ) async throws -> (InMemoryRemoteStorageClient, BackupV2RuntimeServices) {
        let client = try await makeConnectedClient()
        for seq in seqs {
            try await writeAddCommit(client: client, seq: seq, clock: seq, assetByte: UInt8(seq & 0xFF))
        }
        // First pass: no pre-existing baseline ⇒ the planner prefix is empty, so this writes the attested
        // baseline without deleting any commit (commits remain for a subsequent aggressive-delete pass).
        let services = try await makeServices(client: client, policy: policy(checkpointCommitThreshold: 1))
        let pass1 = try await RepoCompactionService(services: services).compactMonth(monthKey)
        XCTAssertEqual(pass1.outcome, .checkpointWritten, "first pass must write the attested baseline")
        // No pre-existing baseline ⇒ the planner delete prefix is empty, so commit GC finds nothing to delete.
        if case .preflightBlocked = try XCTUnwrap(pass1.commitCleanup) {} else {
            XCTFail("first pass must not delete commits, got \(String(describing: pass1.commitCleanup))")
        }
        for seq in seqs {
            let path = RepoLayout.commitFilePath(base: basePath, month: monthKey, writerID: writerID, seq: seq)
            let survived = await client.hasFile(path)
            XCTAssertTrue(survived, "commit seq \(seq) must survive the baseline-writing pass")
        }
        // Re-make services so the aggressive-delete pass re-materializes against the now-persisted baseline.
        let deleteServices = try await makeServices(client: client, policy: policy(checkpointCommitThreshold: 1_000))
        return (client, deleteServices)
    }

    private func makeConnectedClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        return client
    }

    private func attestedHeader(covered: CoveredRanges) -> SnapshotHeader {
        SnapshotHeader(
            version: SnapshotHeader.checkpointVersion,
            scope: CommitHeader.monthScope(monthKey),
            writerID: writerID,
            repoID: repoID,
            covered: covered,
            createdAtMs: nil,
            coverageAttestation: SnapshotCoverageAttestation()
        )
    }

    /// Writes a faithful legacy (4-segment, unattested) baseline covering [low,high] carrying the single
    /// asset that `writeAddCommit(assetByte:)` materializes at seq `low`, so commit GC body retention passes.
    private func writeLegacyBaseline(
        client: InMemoryRemoteStorageClient,
        low: UInt64,
        high: UInt64,
        lamport: UInt64,
        assetByte: UInt8
    ) async throws {
        let fp = TestFixtures.assetFingerprint(assetByte)
        let hash = TestFixtures.fingerprint(assetByte &+ 1)
        let path = String(format: "%04d/%02d/asset-%02x.jpg", year, monthValue, assetByte)
        let stamp = OpStamp(writerID: writerID, seq: low, clock: low)
        var state = RepoMonthState.empty
        state.assets[fp] = SnapshotAssetRow(
            assetFingerprint: fp, creationDateMs: nil, backedUpAtMs: Int64(low),
            resourceCount: 1, totalFileSizeBytes: 100, stamp: stamp
        )
        state.resources[RemotePhysicalPathKey(path)] = SnapshotResourceRow(
            physicalRemotePath: path, contentHash: hash, fileSize: 100,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: Int64(low),
            crypto: nil, stamp: stamp
        )
        state.assetResources[AssetResourceKey(assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0)] =
            SnapshotAssetResourceRow(
                assetFingerprint: fp, role: ResourceTypeCode.photo, slot: 0, resourceHash: hash, logicalName: "asset.jpg"
            )
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion, scope: CommitHeader.monthScope(monthKey),
            writerID: writerID, repoID: repoID, covered: covered([(low, high)]), createdAtMs: nil
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: state)
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header, assets: parts.assets, resources: parts.resources,
            assetResources: parts.assetResources, deletedKeys: parts.deletedKeys,
            month: monthKey, lamport: lamport, runID: runID, respectTaskCancellation: true
        )
    }

    private func writeAddCommit(
        client: InMemoryRemoteStorageClient,
        seq: UInt64,
        clock: UInt64,
        assetByte: UInt8
    ) async throws {
        let assetFP = TestFixtures.assetFingerprint(assetByte)
        let resources = [CommitResourceEntry(
            physicalRemotePath: String(format: "%04d/%02d/asset-%02x.jpg", year, monthValue, assetByte),
            logicalName: "asset.jpg",
            contentHash: TestFixtures.fingerprint(assetByte &+ 1),
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            role: ResourceTypeCode.photo,
            slot: 0,
            crypto: nil
        )]
        let op = CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: assetFP, creationDateMs: nil, backedUpAtMs: Int64(clock), resources: resources
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerID, seq: seq, runID: runID,
                month: monthKey, clockMin: clock, clockMax: clock
            ),
            ops: [op], month: monthKey, respectTaskCancellation: true
        )
    }

    private func commitFileCount(_ client: InMemoryRemoteStorageClient) async -> Int {
        await client.snapshotFiles().keys.filter { $0.contains("/.watermelon/commits/") }.count
    }

    private func covered(_ ranges: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [writerID: ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }])
    }

    private func policy(checkpointCommitThreshold: Int) -> RepoCompactionPolicy {
        RepoCompactionPolicy(
            checkpointCommitThreshold: checkpointCommitThreshold,
            checkpointByteThreshold: Int64.max,
            snapshotFallbackKeepCount: 2,
            snapshotGCMarginFileCount: 2
        )
    }

    private func makeServices(
        client: InMemoryRemoteStorageClient,
        policy: RepoCompactionPolicy
    ) async throws -> BackupV2RuntimeServices {
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav
        )
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: repoID,
            runID: runID,
            basePath: basePath,
            postOpenSyncInspection: .v2(formatVersion: RepoLayout.currentSupportedFormatVersion),
            database: databaseManager,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: commitWriter,
            snapshotWriter: snapshotWriter,
            compactionPolicy: policy,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil)
        )
    }
}

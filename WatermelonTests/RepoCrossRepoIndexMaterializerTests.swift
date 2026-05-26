import XCTest
@testable import Watermelon

@MainActor
final class RepoCrossRepoIndexMaterializerTests: XCTestCase {
    private let month1 = LibraryMonthKey(year: 2026, month: 1)
    private let month2 = LibraryMonthKey(year: 2026, month: 2)

    // MARK: - Cold / hot start

    func testColdStart_NoIndex_MaterializeBehavesAsToday() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x11)
        let hash = TestFixtures.fingerprint(0x12)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: hash)
        let output = try await builder.materialize()

        XCTAssertNotNil(output.state.months[month1])
        XCTAssertTrue(output.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
    }

    func testHotStart_ValidIndex_CoveredCommitsAreNotReplayedDespiteCorruption() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x21)
        let hash = TestFixtures.fingerprint(0x22)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: hash)

        let firstOutput = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: firstOutput, lamport: 100)

        // Corrupt the commit file. The cross-repo index covers it (covered ranges include seq=1).
        // Today's materializer would normally re-read it; with cross-repo, it's covered and skipped.
        let commitDirEntries = try await builder.client.list(path: "\(builder.basePath)/.watermelon/commits")
        for entry in commitDirEntries where !entry.isDirectory && entry.name.hasSuffix(".jsonl") {
            await builder.client.corrupt(path: entry.path, with: Data("CORRUPT".utf8))
        }

        let secondOutput = try await builder.materialize()
        XCTAssertEqual(secondOutput.acceptedCrossRepoIndexBaselineByMonth.count, 1)
        XCTAssertEqual(secondOutput.state.months[month1]?.assets[fp]?.assetFingerprint, fp)
    }

    func testHotStart_PublishesIndexFile_OnFirstMaterialize() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x31)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0x32))
        let output = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: output, lamport: 50)
        let indexFiles = try await builder.client.list(path: "\(builder.basePath)/.watermelon/index")
        let jsonl = indexFiles.filter { !$0.isDirectory && $0.name.hasSuffix(".jsonl") }
        XCTAssertEqual(jsonl.count, 1)
        // Filename format is `<lamport16hex>--<writerID>--<runIDPrefix>.jsonl` (no "index--" prefix);
        // the dir itself is the artifact discriminator.
        let lamport16Hex = String(format: "%016x", 50)
        XCTAssertTrue(jsonl[0].name.hasPrefix("\(lamport16Hex)--"))
    }

    // MARK: - Trust predicate failures

    func testCrossRepoIndex_Sha256Mismatch_CandidateSkipped() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x41)
        let hash = TestFixtures.fingerprint(0x42)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: hash)
        let output = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: output, lamport: 50)

        let indexEntries = try await builder.client.list(path: "\(builder.basePath)/.watermelon/index")
        for entry in indexEntries where !entry.isDirectory {
            await builder.client.corrupt(path: entry.path, with: Data("totally not a valid jsonl".utf8))
        }

        let nextOutput = try await builder.materialize()
        XCTAssertTrue(nextOutput.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
        // State is still correct because the commit log is still readable.
        XCTAssertNotNil(nextOutput.state.months[month1]?.assets[fp])
    }

    func testCrossRepoIndex_RepoIDMismatch_CandidateSkipped() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x51)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0x52))
        let output = try await builder.materialize()

        // Build a parallel "foreign" repo writer pointing at the SAME basePath, then publish
        // an index file with a different repoID. The materializer must skip it.
        let foreignBuilder = try await RepoTestBuilder.freshRepo(
            basePath: builder.basePath,
            repoID: "ffffffff-ffff-ffff-ffff-ffffffffffff",
            writerID: "ffffffff-1111-2222-3333-444444444444",
            runID: "run-foreign"
        )
        try await foreignBuilder.publishCrossRepoIndex(from: output, lamport: 99)
        let next = try await builder.materialize()
        XCTAssertTrue(next.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
    }

    func testCrossRepoIndex_FilenameHeaderDisagreement_CandidateSkipped() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x61)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0x62))
        let output = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: output, lamport: 200)

        let indexDir = "\(builder.basePath)/.watermelon/index"
        let entries = try await builder.client.list(path: indexDir)
        let jsonl = try XCTUnwrap(entries.first { !$0.isDirectory && $0.name.hasSuffix(".jsonl") })

        // Read the published file's bytes, rename to a different lamport-named file, and delete the original.
        let downloadTemp = FileManager.default.temporaryDirectory.appendingPathComponent("idx-dl-\(UUID()).jsonl")
        defer { try? FileManager.default.removeItem(at: downloadTemp) }
        try await builder.client.download(remotePath: jsonl.path, localURL: downloadTemp)
        let renamedBytes = try Data(contentsOf: downloadTemp)
        // Body header lamport=200 but filename lamport=0x123 — mismatch.
        let renamed = "0000000000000123--\(builder.writerID)--\(RepoLayout.runIDPrefix(builder.runID)).jsonl"
        await builder.client.injectFile(path: "\(indexDir)/\(renamed)", data: renamedBytes)
        try await builder.client.delete(path: jsonl.path)

        let next = try await builder.materialize()
        XCTAssertTrue(next.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
    }

    func testCrossRepoIndex_MultiWriter_LexMaxValidPicked() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x71)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0x72))
        let output = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: output, lamport: 100, runID: "run-low")
        try await builder.publishCrossRepoIndex(from: output, lamport: 500, runID: "run-high")

        let next = try await builder.materialize()
        let baseline = try XCTUnwrap(next.acceptedCrossRepoIndexBaselineByMonth[month1])
        XCTAssertEqual(baseline.lamport, 500)
    }

    func testCrossRepoIndex_HigherLamportCorrupt_FallsBackToNextCandidate() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x81)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0x82))
        let output = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: output, lamport: 100, runID: "run-low")
        try await builder.publishCrossRepoIndex(from: output, lamport: 500, runID: "run-high")

        // Corrupt the higher-lamport candidate; materializer falls back to lamport 100.
        let indexDir = "\(builder.basePath)/.watermelon/index"
        let entries = try await builder.client.list(path: indexDir)
        let highLamport = String(format: "%016x", 500)
        let highEntry = try XCTUnwrap(entries.first { $0.name.hasPrefix("\(highLamport)--") })
        await builder.client.corrupt(path: highEntry.path, with: Data("trash".utf8))

        let next = try await builder.materialize()
        XCTAssertEqual(next.acceptedCrossRepoIndexBaselineByMonth[month1]?.lamport, 100)
    }

    // MARK: - Publish: observe-before-tick / header round trip

    func testPublish_HeaderRoundTripPreservesObservedClockAndLamport() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0x91)
        try await builder.addAsset(month: month1, seq: 1, clock: 500, fingerprint: fp, contentHash: TestFixtures.fingerprint(0x92))
        let output = try await builder.materialize()
        XCTAssertEqual(output.state.observedClock, 500)

        let published = try await builder.publishCrossRepoIndex(from: output, lamport: 501)
        XCTAssertEqual(published.header.lamport, 501)
        XCTAssertEqual(published.header.observedClock, 500)
    }

    func testWriter_CreatesIndexDirectory_EvenIfBootstrapDidNot() async throws {
        // Skip the freshRepo bootstrap path (which creates the index dir via P-1-A) and use a
        // minimal client that never had .watermelon/index created. The writer's defense-in-depth
        // createDirectory (P-1-B) must establish it.
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let basePath = "/raw-repo"
        try await client.createDirectory(path: basePath)
        try await client.createDirectory(path: "\(basePath)/.watermelon")
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")

        let writer = RepoCrossRepoIndexWriter(client: client, basePath: basePath)
        let output = RepoMaterializer.MaterializeOutput(
            state: RepoSnapshotState(months: [month1: .empty], observedClock: 0),
            observedSeqByWriter: [:],
            coveredByMonth: [month1: .empty],
            acceptedSnapshotBaselinesByMonth: [:],
            corruptedSnapshotMonths: [],
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
        _ = try await writer.write(
            materialized: output,
            expectedRepoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "run-test",
            lamport: 1,
            respectTaskCancellation: false
        )

        let listed = try await client.list(path: "\(basePath)/.watermelon/index")
        let jsonl = listed.filter { !$0.isDirectory && $0.name.hasSuffix(".jsonl") }
        XCTAssertEqual(jsonl.count, 1)
    }

    // MARK: - Cross-repo vs per-month winner

    func testCrossRepoIndex_VsPerMonthSnapshot_WhenOnlyCrossRepo_CrossRepoWins() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xA1)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xA2))
        let output = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: output, lamport: 50)

        let next = try await builder.materialize()
        XCTAssertEqual(next.acceptedCrossRepoIndexBaselineByMonth[month1]?.lamport, 50)
        XCTAssertNil(next.acceptedSnapshotBaselinesByMonth[month1])
    }

    func testCrossRepoIndex_VsPerMonthSnapshot_OlderCrossRepo_NewerPerMonthSnapshotWins() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xB1)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xB2))

        // Publish cross-repo at lamport 50 (older).
        let output = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: output, lamport: 50)

        // Publish a per-month snapshot at lamport 300 (newer).
        try await Self.publishPerMonthSnapshot(builder: builder, month: month1, lamport: 300, materialized: output)

        let next = try await builder.materialize()
        let perMonth = try XCTUnwrap(next.acceptedSnapshotBaselinesByMonth[month1])
        XCTAssertEqual(perMonth.lamport, 300)
        XCTAssertNil(next.acceptedCrossRepoIndexBaselineByMonth[month1])
    }

    // MARK: - P-2: validateRetry covers cross-repo baseline

    func testValidateRetry_SnapshotVanished_CrossRepoBaselineSameOrNewer_AcceptsRecovery() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xC1)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xC2))

        let materialized = try await builder.materialize()
        try await Self.publishPerMonthSnapshot(builder: builder, month: month1, lamport: 50, materialized: materialized)
        let withSnapshot = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: withSnapshot, lamport: 100)

        let perMonthName = RepoLayout.snapshotFileName(
            month: month1,
            lamport: 50,
            writerID: builder.writerID,
            runID: builder.runID
        )
        let perMonthPath = "\(builder.basePath)/.watermelon/snapshots/\(perMonthName)"

        // Race: first download attempt deletes the file AND throws notFound. Retry sees the
        // file is gone from LIST, materializes via cross-repo, and validateRetry accepts the
        // recovery because the cross-repo baseline's (lamport, writerID, runIDPrefix) is
        // same-or-newer than the vanished per-month snapshot's.
        let race = MaterializerVanishRaceClient(inner: builder.client)
        await race.setVanishHook(
            path: perMonthPath,
            inner: builder.client
        )

        let materializer = RepoMaterializer(client: race, basePath: builder.basePath)
        let recovered = try await materializer.materialize(expectedRepoID: builder.repoID)
        XCTAssertNotNil(recovered.acceptedCrossRepoIndexBaselineByMonth[month1])
        XCTAssertNotNil(recovered.state.months[month1]?.assets[fp])
    }

    func testValidateRetry_SnapshotVanished_NoCrossRepo_StillThrows() async throws {
        // Pin that the today's behavior survives when cross-repo cannot recover: a per-month
        // snapshot vanishes mid-read with no cross-repo baseline and no fallback per-month
        // candidate → materialize throws.
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xD1)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xD2))
        let materialized = try await builder.materialize()
        try await Self.publishPerMonthSnapshot(builder: builder, month: month1, lamport: 50, materialized: materialized)

        let perMonthName = RepoLayout.snapshotFileName(
            month: month1,
            lamport: 50,
            writerID: builder.writerID,
            runID: builder.runID
        )
        let perMonthPath = "\(builder.basePath)/.watermelon/snapshots/\(perMonthName)"
        // Persistent download error: each materializeOnce attempt sees the file in LIST but
        // its read throws notFound. Without recovery from another baseline, the second
        // attempt throws InternalMetadataReadRace again → metadataChangedAgainAfterRetry.
        await builder.client.injectPersistentDownloadError(.notFound, for: perMonthPath)

        do {
            _ = try await builder.materialize()
            XCTFail("expected materialize to throw without recovery")
        } catch let error as RepoMaterializer.MetadataReadRaceError {
            switch error {
            case .snapshotVanishedWithoutRecovery, .metadataChangedAgainAfterRetry:
                return
            case .requiredCommitVanished:
                XCTFail("unexpected commit-vanished error: \(error)")
            }
        } catch {
            // Acceptable: other materializer paths may map this differently across runs.
            return
        }
    }

    // MARK: - R03 Checker finding: optional cross-repo failures fall back

    func testCrossRepoIndex_ListTransportFailure_FallsBackToSnapshotsAndCommits() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xF1)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xF2))

        // Create the index directory so the listFilenames path actually issues a `list` call
        // (an absent directory is the easy path; we want the LIST itself to fail).
        let indexDir = "\(builder.basePath)/.watermelon/index"
        try await builder.client.createDirectory(path: indexDir)
        // Make the index file actually present so `metadata(indexDir) == nil` doesn't short
        // the listFilenames fallback. We need the LIST to throw on a populated directory.
        await builder.client.injectFile(path: "\(indexDir)/sentinel.jsonl", data: Data("placeholder".utf8))
        await builder.client.injectListError(.transport, for: indexDir)

        // Per-month + commit replay must still recover the asset. The cross-repo LIST failure
        // is non-fatal under the R03 fix.
        let next = try await builder.materialize()
        XCTAssertTrue(next.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
        XCTAssertNotNil(next.state.months[month1]?.assets[fp])
    }

    func testCrossRepoIndex_DownloadTransportFailure_FallsBackToSnapshotsAndCommits() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xF3)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xF4))
        let materialized = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: materialized, lamport: 100)

        // Find the published index file and inject a PERSISTENT transport error on its
        // download. The LIST still returns the entry, but download fails with a
        // non-cancellation error every time. The R03 fix must treat this candidate as
        // unavailable rather than aborting materialize.
        let indexDir = "\(builder.basePath)/.watermelon/index"
        let entries = try await builder.client.list(path: indexDir)
        let jsonl = try XCTUnwrap(entries.first { !$0.isDirectory && $0.name.hasSuffix(".jsonl") })
        await builder.client.injectPersistentDownloadError(.transport, for: jsonl.path)

        let next = try await builder.materialize()
        XCTAssertTrue(next.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
        XCTAssertNotNil(next.state.months[month1]?.assets[fp])
    }

    // MARK: - R02 Reviewer A/B finding 1: strict header/body / deleted-key validation

    func testCrossRepoIndex_HeaderCoversMonthWithoutMatchingBodySection_CandidateSkipped() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xE1)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xE2))
        let materialized = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: materialized, lamport: 100)

        // Patch the published index: strip the per-month section body lines but keep
        // header.coveredByMonth claiming the month is covered. Recompute sha + rowCount so
        // wire integrity passes — the candidate must still be rejected by the post-parse
        // section-set-vs-header check.
        let indexDir = "\(builder.basePath)/.watermelon/index"
        let entries = try await builder.client.list(path: indexDir)
        let jsonl = try XCTUnwrap(entries.first { !$0.isDirectory && $0.name.hasSuffix(".jsonl") })
        let downloadTemp = FileManager.default.temporaryDirectory.appendingPathComponent("idx-dl-\(UUID()).jsonl")
        defer { try? FileManager.default.removeItem(at: downloadTemp) }
        try await builder.client.download(remotePath: jsonl.path, localURL: downloadTemp)
        let raw = try String(contentsOf: downloadTemp, encoding: .utf8)
        let mutated = Self.stripMonthSection(from: raw, monthText: month1.text)
        await builder.client.injectFile(path: jsonl.path, data: Data(mutated.utf8))

        let next = try await builder.materialize()
        XCTAssertTrue(next.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
        // Per-month + commit replay must still recover the asset (fail-closed fallback).
        XCTAssertNotNil(next.state.months[month1]?.assets[fp])
    }

    func testCrossRepoIndex_DuplicateMonthSection_CandidateSkipped() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xE3)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xE4))
        let materialized = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: materialized, lamport: 100)

        let indexDir = "\(builder.basePath)/.watermelon/index"
        let entries = try await builder.client.list(path: indexDir)
        let jsonl = try XCTUnwrap(entries.first { !$0.isDirectory && $0.name.hasSuffix(".jsonl") })
        let downloadTemp = FileManager.default.temporaryDirectory.appendingPathComponent("idx-dl-\(UUID()).jsonl")
        defer { try? FileManager.default.removeItem(at: downloadTemp) }
        try await builder.client.download(remotePath: jsonl.path, localURL: downloadTemp)
        let raw = try String(contentsOf: downloadTemp, encoding: .utf8)
        // Duplicate the month1 section so it appears twice in the body.
        let mutated = Self.duplicateMonthSection(in: raw, monthText: month1.text)
        await builder.client.injectFile(path: jsonl.path, data: Data(mutated.utf8))

        let next = try await builder.materialize()
        XCTAssertTrue(next.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
        XCTAssertNotNil(next.state.months[month1]?.assets[fp])
    }

    func testCrossRepoIndex_BodyHasUnsupportedDeletedKeyType_CandidateSkipped() async throws {
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xE5)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xE6))
        let materialized = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: materialized, lamport: 100)

        // Inject a `keyType="resource"` deletedKey row into the month section (writer-side
        // wire format allows it, but per-month snapshot trust pipeline rejects, and so must
        // cross-repo trust to keep the contract identical).
        let indexDir = "\(builder.basePath)/.watermelon/index"
        let entries = try await builder.client.list(path: indexDir)
        let jsonl = try XCTUnwrap(entries.first { !$0.isDirectory && $0.name.hasSuffix(".jsonl") })
        let downloadTemp = FileManager.default.temporaryDirectory.appendingPathComponent("idx-dl-\(UUID()).jsonl")
        defer { try? FileManager.default.removeItem(at: downloadTemp) }
        try await builder.client.download(remotePath: jsonl.path, localURL: downloadTemp)
        let raw = try String(contentsOf: downloadTemp, encoding: .utf8)
        let mutated = Self.injectResourceDeletedKey(
            in: raw,
            monthText: month1.text,
            writerID: builder.writerID
        )
        await builder.client.injectFile(path: jsonl.path, data: Data(mutated.utf8))

        let next = try await builder.materialize()
        XCTAssertTrue(next.acceptedCrossRepoIndexBaselineByMonth.isEmpty)
        XCTAssertNotNil(next.state.months[month1]?.assets[fp])
    }

    // MARK: - R02 Checker finding 3: per-month snapshot reads are SKIPPED when cross-repo wins

    func testHotStart_ValidCrossRepoIndex_PerMonthSnapshotReadIsElided() async throws {
        // Build a repo with both a per-month snapshot AND a cross-repo index that wins the
        // tiebreak. Configure the in-memory client to PERSISTENTLY return notFound for the
        // per-month snapshot file. If the materializer reads it, materialize fails. If the
        // cross-repo's filter elides the read, materialize succeeds.
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xE7)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xE8))

        let materialized = try await builder.materialize()
        try await Self.publishPerMonthSnapshot(builder: builder, month: month1, lamport: 50, materialized: materialized)
        let withSnapshot = try await builder.materialize()
        try await builder.publishCrossRepoIndex(from: withSnapshot, lamport: 100)

        // Persistent error: any download of the per-month snapshot fails with notFound.
        // If the materializer downloads it, the test fails. If the filter elides it, the
        // test passes — proving the U02 read-elision invariant.
        let perMonthName = RepoLayout.snapshotFileName(
            month: month1,
            lamport: 50,
            writerID: builder.writerID,
            runID: builder.runID
        )
        let perMonthPath = "\(builder.basePath)/.watermelon/snapshots/\(perMonthName)"
        await builder.client.injectPersistentDownloadError(.notFound, for: perMonthPath)

        let next = try await builder.materialize()
        XCTAssertEqual(next.acceptedCrossRepoIndexBaselineByMonth[month1]?.lamport, 100)
        XCTAssertNotNil(next.state.months[month1]?.assets[fp])
        XCTAssertNil(next.acceptedSnapshotBaselinesByMonth[month1])
    }

    func testHotStart_PerMonthSnapshotNewerThanCrossRepo_PerMonthReadStillHappens() async throws {
        // Inverse of the elision test: the per-month snapshot has a HIGHER lex tiebreak than
        // the cross-repo baseline, so the per-month read must NOT be elided. Verify that
        // injecting a persistent download error on the per-month file causes materialize to
        // fail — confirming the per-month read is attempted (and the cross-repo filter
        // correctly doesn't elide it).
        let builder = try await RepoTestBuilder.freshRepo(
            writerID: "11111111-2222-3333-4444-555555555555",
            runID: "abcdef12-3456-7890-abcd-ef1234567890"
        )
        let fp = TestFixtures.fingerprint(0xE9)
        try await builder.addAsset(month: month1, seq: 1, fingerprint: fp, contentHash: TestFixtures.fingerprint(0xEA))

        let materialized = try await builder.materialize()
        // Cross-repo at lamport 50 (lower).
        try await builder.publishCrossRepoIndex(from: materialized, lamport: 50)
        // Per-month snapshot at lamport 300 (higher → must be read).
        try await Self.publishPerMonthSnapshot(builder: builder, month: month1, lamport: 300, materialized: materialized)

        let perMonthName = RepoLayout.snapshotFileName(
            month: month1,
            lamport: 300,
            writerID: builder.writerID,
            runID: builder.runID
        )
        let perMonthPath = "\(builder.basePath)/.watermelon/snapshots/\(perMonthName)"
        await builder.client.injectPersistentDownloadError(.notFound, for: perMonthPath)

        do {
            _ = try await builder.materialize()
            XCTFail("expected materialize to throw — per-month snapshot must be attempted because its lex tiebreak beats cross-repo")
        } catch {
            // Acceptable: the per-month read was attempted and failed. Confirms no elision.
            return
        }
    }

    // MARK: - Helpers

    /// Rewrites a JSONL body by removing the rows of `monthText`'s monthBegin→monthEnd section
    /// (inclusive) while leaving the header's `coveredByMonth` claim intact, then recomputing
    /// `sha256` and `rowCount` so wire integrity validates. Used to manufacture the "header
    /// covers month M but body has no section for M" trust gap.
    private static func stripMonthSection(from raw: String, monthText: String) -> String {
        let lines = raw.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
        var output: [String] = []
        var dropping = false
        for line in lines {
            if line.contains("\"crossRepoMonthBegin\"") && line.contains("\"\(monthText)\"") {
                dropping = true
                continue
            }
            if dropping && line.contains("\"crossRepoMonthEnd\"") && line.contains("\"\(monthText)\"") {
                dropping = false
                continue
            }
            if dropping { continue }
            output.append(line)
        }
        return Self.recomputeIntegrity(lines: output)
    }

    /// Duplicates the `monthText` section so the body has two `monthBegin/monthEnd(monthText)`
    /// pairs. Recomputes sha256 + rowCount.
    private static func duplicateMonthSection(in raw: String, monthText: String) -> String {
        let lines = raw.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
        var sectionLines: [String] = []
        var inSection = false
        for line in lines {
            if line.contains("\"crossRepoMonthBegin\"") && line.contains("\"\(monthText)\"") {
                inSection = true
            }
            if inSection {
                sectionLines.append(line)
            }
            if inSection && line.contains("\"crossRepoMonthEnd\"") && line.contains("\"\(monthText)\"") {
                inSection = false
            }
        }
        // Insert a duplicate copy immediately after the original section's monthEnd.
        var output: [String] = []
        var copiedDuplicate = false
        for line in lines {
            output.append(line)
            if !copiedDuplicate && line.contains("\"crossRepoMonthEnd\"") && line.contains("\"\(monthText)\"") {
                output.append(contentsOf: sectionLines)
                copiedDuplicate = true
            }
        }
        return Self.recomputeIntegrity(lines: output)
    }

    /// Injects a `keyType="resource"` deletedKey row into the body of `monthText`'s section.
    /// Per-month and cross-repo trust pipelines both reject non-`.asset` keyType baselines;
    /// this exercises that rejection on the cross-repo side.
    private static func injectResourceDeletedKey(in raw: String, monthText: String, writerID: String) -> String {
        let lines = raw.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
        let injectedRow = "{\"r\":{\"keyType\":\"resource\",\"keyValue\":\"2026/01/fake-resource\",\"lastClock\":1,\"lastSeq\":1,\"lastWriterID\":\"\(writerID)\"},\"t\":\"deleted_key\"}"
        var output: [String] = []
        for line in lines {
            output.append(line)
            if line.contains("\"crossRepoMonthBegin\"") && line.contains("\"\(monthText)\"") {
                output.append(injectedRow)
            }
        }
        return Self.recomputeIntegrity(lines: output)
    }

    /// Re-computes `sha256`/`rowCount` on the end row so the JSONL integrity trailer matches
    /// the (mutated) body. The end row is always the last non-empty line; everything before
    /// it counts toward `IntegrityAccumulator`.
    private static func recomputeIntegrity(lines linesIn: [String]) -> String {
        var lines = linesIn
        while let last = lines.last, last.isEmpty { lines.removeLast() }
        guard !lines.isEmpty else { return lines.joined(separator: "\n") + "\n" }
        let endIndex = lines.count - 1
        var integrity = IntegrityAccumulator()
        for i in 0..<endIndex {
            integrity.absorbLine(lines[i])
        }
        let sha = integrity.finalize()
        let rowCount = integrity.rowCount
        let newEnd = "{\"rowCount\":\(rowCount),\"sha256\":\"\(sha)\",\"t\":\"end\"}"
        lines[endIndex] = newEnd
        return lines.joined(separator: "\n") + "\n"
    }

    private static func publishPerMonthSnapshot(
        builder: RepoTestBuilder,
        month: LibraryMonthKey,
        lamport: UInt64,
        materialized: RepoMaterializer.MaterializeOutput
    ) async throws {
        let perMonthState = materialized.state.months[month] ?? .empty
        let snapshotWriter = SnapshotWriter(client: builder.client, basePath: builder.basePath)
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: builder.writerID,
            repoID: builder.repoID,
            covered: materialized.coveredByMonth[month] ?? .empty
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: perMonthState)
        _ = try await snapshotWriter.write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: month,
            lamport: lamport,
            runID: builder.runID,
            respectTaskCancellation: false
        )
    }
}

/// Minimal race-injecting wrapper that mirrors the surface of
/// `RepoMaterializerReadRaceTests.ReadRaceClient` (which is private to that file). On the
/// first hooked-path download it deletes the file on the inner client and throws
/// `RepoJSONLReadError.notFound` — letting a test simulate "file in LIST at first attempt,
/// gone from LIST at retry attempt".
private final class MaterializerVanishRaceClient: @unchecked Sendable, RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    private let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var hookedPath: String?
    private var hookedInner: InMemoryRemoteStorageClient?

    init(inner: InMemoryRemoteStorageClient) { self.inner = inner }

    func setVanishHook(path: String, inner: InMemoryRemoteStorageClient) async {
        lock.withLock {
            hookedPath = Self.normalize(path)
            hookedInner = inner
        }
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }

    func download(remotePath: String, localURL: URL) async throws {
        let key = Self.normalize(remotePath)
        let pair: (path: String, inner: InMemoryRemoteStorageClient)? = lock.withLock {
            guard let p = hookedPath, p == key, let i = hookedInner else { return nil }
            hookedPath = nil
            hookedInner = nil
            return (p, i)
        }
        if let pair {
            try? await pair.inner.delete(path: pair.path)
            throw RepoJSONLReadError.notFound(filename: (key as NSString).lastPathComponent)
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
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

    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }
}

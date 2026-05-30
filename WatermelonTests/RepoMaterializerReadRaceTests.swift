import Foundation
import XCTest
@testable import Watermelon

final class RepoMaterializerReadRaceTests: XCTestCase {
    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let writerB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    private let runID = "race-run"
    private let month = LibraryMonthKey(year: 2026, month: 1)

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

    func testCommitVanishedThenReappearsAfterRestartSucceeds() async throws {
        let client = try await makeClient()
        let fp = TestFixtures.assetFingerprint(0x10)
        try await writeAddCommit(client: client, seq: 1, fingerprint: fp)
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: commitPath) { _ in throw Self.notFoundError() }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[fp])
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1))
        XCTAssertEqual(race.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 2)
    }

    func testCommitVanishedThenNewSnapshotCoversAfterRestartSucceeds() async throws {
        let client = try await makeClient()
        let fp = TestFixtures.assetFingerprint(0x11)
        try await writeAddCommit(client: client, seq: 1, fingerprint: fp)
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: commitPath) { _ in
            try await self.writeSnapshot(client: client, lamport: 10, writerID: self.writerA, coveredSeqs: [1], fingerprints: [fp])
            try await client.delete(path: commitPath)
            throw Self.notFoundError()
        }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[fp])
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1))
        XCTAssertEqual(output.observedSeqByWriter[writerA], 1)
        XCTAssertEqual(race.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 2)
    }

    func testCommitStillMissingAfterRestartFailsClosed() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x12))
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: commitPath) { _ in
            try await client.delete(path: commitPath)
            throw Self.notFoundError()
        }

        do {
            _ = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected requiredCommitVanished")
        } catch RepoMaterializer.MetadataReadRaceError.requiredCommitVanished(let filename, let month, let writerID, let seq) {
            XCTAssertEqual(filename, RepoLayout.commitFileName(month: self.month, writerID: writerA, seq: 1))
            XCTAssertEqual(month, self.month)
            XCTAssertEqual(writerID, writerA)
            XCTAssertEqual(seq, 1)
            XCTAssertEqual(race.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 2)
        }
    }

    func testSecondCommitDisappearanceIsBounded() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x13))
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: commitPath, once: false) { _ in throw Self.notFoundError() }

        do {
            _ = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected metadataChangedAgainAfterRetry")
        } catch RepoMaterializer.MetadataReadRaceError.metadataChangedAgainAfterRetry {
            XCTAssertEqual(race.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 2)
        }
    }

    func testCommitListedButLaggingWithinGraceRetriesUntilReadable() async throws {
        let client = try await makeClient()
        let fp = TestFixtures.assetFingerprint(0x50)
        try await writeAddCommit(client: client, seq: 1, fingerprint: fp)
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let race = ReadRaceClient(inner: client)
        race.setReadAfterWriteGrace(3)
        // 404 on the initial pass AND the first immediate retry; only the in-grace retry reads it.
        // Zero-grace code (single immediate retry) would fail closed here.
        let calls = LockedCounter()
        race.setDownloadHook(path: commitPath, once: false) { _ in
            if calls.increment() <= 2 { throw Self.notFoundError() }
        }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[fp])
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1))
        XCTAssertGreaterThanOrEqual(race.downloadCount(path: commitPath), 3)
    }

    func testSnapshotListedButLaggingWithinGraceRetriesUntilReadable() async throws {
        let client = try await makeClient()
        let fp = TestFixtures.assetFingerprint(0x51)
        try await writeSnapshot(client: client, lamport: 100, writerID: writerA, coveredSeqs: [], fingerprints: [fp])
        let snapshotPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 100, writerID: writerA, runID: runID)
        let race = ReadRaceClient(inner: client)
        race.setReadAfterWriteGrace(3)
        let calls = LockedCounter()
        race.setDownloadHook(path: snapshotPath, once: false) { _ in
            if calls.increment() <= 2 { throw Self.notFoundError() }
        }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[fp])
        XCTAssertEqual(output.acceptedSnapshotBaselinesByMonth[month]?.lamport, 100)
        XCTAssertGreaterThanOrEqual(race.downloadCount(path: snapshotPath), 3)
    }

    func testCommitListedThenRetryListingOmitsThenReappearsWithinGraceSucceeds() async throws {
        let client = try await makeClient()
        let fp = TestFixtures.assetFingerprint(0x60)
        try await writeAddCommit(client: client, seq: 1, fingerprint: fp)
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let commitFilename = RepoLayout.commitFileName(month: month, writerID: writerA, seq: 1)
        let race = ReadRaceClient(inner: client)
        race.setReadAfterWriteGrace(3)
        // Pass 1: listed + GET 404. Pass 2 retry: listing omits the same file, so validateRetry
        // reports the original commit's coverage missing (MetadataReadRaceError, not the internal
        // race). The grace loop must keep retrying; pass 3 lists it again and the GET succeeds.
        race.setListHook(path: RepoLayout.commitsDirectoryPath(base: basePath)) { callIndex, entries in
            callIndex == 2 ? entries.filter { $0.name != commitFilename } : entries
        }
        race.setDownloadHook(path: commitPath, once: true) { _ in throw Self.notFoundError() }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[fp])
        XCTAssertTrue((output.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1))
        XCTAssertGreaterThanOrEqual(race.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 3)
    }

    func testSnapshotListedThenRetryListingOmitsThenReappearsWithinGraceSucceeds() async throws {
        let client = try await makeClient()
        let fp = TestFixtures.assetFingerprint(0x61)
        try await writeSnapshot(client: client, lamport: 100, writerID: writerA, coveredSeqs: [], fingerprints: [fp])
        let snapshotPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 100, writerID: writerA, runID: runID)
        let snapshotFilename = RepoLayout.snapshotFileName(month: month, lamport: 100, writerID: writerA, runID: runID)
        let race = ReadRaceClient(inner: client)
        race.setReadAfterWriteGrace(3)
        race.setListHook(path: RepoLayout.snapshotsDirectoryPath(base: basePath)) { callIndex, entries in
            callIndex == 2 ? entries.filter { $0.name != snapshotFilename } : entries
        }
        race.setDownloadHook(path: snapshotPath, once: true) { _ in throw Self.notFoundError() }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[fp])
        XCTAssertEqual(output.acceptedSnapshotBaselinesByMonth[month]?.lamport, 100)
        XCTAssertGreaterThanOrEqual(race.listCount(path: RepoLayout.snapshotsDirectoryPath(base: basePath)), 3)
    }

    func testCommitPersistentNotFoundPastGraceFailsClosed() async throws {
        let client = try await makeClient()
        client.setReadAfterWriteGrace(1)
        try await writeAddCommit(client: client, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x52))
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await client.injectPersistentDownloadError(.notFound, for: commitPath)

        do {
            _ = try await RepoMaterializer(client: client, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected metadataChangedAgainAfterRetry after grace budget is spent")
        } catch RepoMaterializer.MetadataReadRaceError.metadataChangedAgainAfterRetry {
        }
    }

    func testUnclassifiedCommitReadErrorFailsWithoutRestart() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x14))
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: commitPath) { _ in
            throw NSError(domain: NSURLErrorDomain, code: NSURLErrorTimedOut)
        }

        do {
            _ = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected transport failure")
        } catch {
            XCTAssertFalse(error is RepoMaterializer.MetadataReadRaceError)
            XCTAssertEqual(race.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 1)
        }
    }

    func testCancellationPropagatesFromPassOneAndPassTwo() async throws {
        let passOneClient = try await makeClient()
        try await writeAddCommit(client: passOneClient, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x15))
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let passOneRace = ReadRaceClient(inner: passOneClient)
        passOneRace.setDownloadHook(path: commitPath) { _ in throw CancellationError() }

        do {
            _ = try await RepoMaterializer(client: passOneRace, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected cancellation")
        } catch is CancellationError {
            XCTAssertEqual(passOneRace.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 1)
        }

        let passTwoClient = try await makeClient()
        try await writeAddCommit(client: passTwoClient, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x16))
        let passTwoRace = ReadRaceClient(inner: passTwoClient)
        let calls = LockedCounter()
        passTwoRace.setDownloadHook(path: commitPath, once: false) { _ in
            if calls.increment() == 1 { throw Self.notFoundError() }
            throw CancellationError()
        }

        do {
            _ = try await RepoMaterializer(client: passTwoRace, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected cancellation")
        } catch is CancellationError {
            XCTAssertEqual(passTwoRace.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 2)
        }
    }

    func testCorruptCommitAndHeaderMismatchDoNotRestart() async throws {
        let corruptClient = try await makeClient()
        try await writeAddCommit(client: corruptClient, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x17))
        let corruptPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await corruptClient.corrupt(path: corruptPath, with: Data("not-jsonl".utf8))
        let corruptRace = ReadRaceClient(inner: corruptClient)

        let corruptOutput = try await RepoMaterializer(client: corruptRace, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(corruptOutput.observedSeqByWriter[writerA], 1)
        XCTAssertFalse((corruptOutput.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1))
        XCTAssertEqual(corruptRace.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 1)

        let mismatchClient = try await makeClient()
        try await writeAddCommit(client: mismatchClient, seq: 2, fingerprint: TestFixtures.assetFingerprint(0x18))
        let source = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        let target = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let files = await mismatchClient.snapshotFiles()
        let bytes = try XCTUnwrap(files[source])
        await mismatchClient.injectFile(path: target, data: bytes)
        try await mismatchClient.delete(path: source)
        let mismatchRace = ReadRaceClient(inner: mismatchClient)

        let mismatchOutput = try await RepoMaterializer(client: mismatchRace, basePath: basePath).materialize(expectedRepoID: repoID)
        XCTAssertEqual(mismatchOutput.observedSeqByWriter[writerA], 1)
        XCTAssertFalse((mismatchOutput.coveredByMonth[month] ?? .empty).contains(writerID: writerA, seq: 1))
        XCTAssertEqual(mismatchRace.listCount(path: RepoLayout.commitsDirectoryPath(base: basePath)), 1)
    }

    func testSnapshotVanishedThenReappearsSucceeds() async throws {
        let client = try await makeClient()
        let fp = TestFixtures.assetFingerprint(0x20)
        try await writeSnapshot(client: client, lamport: 100, writerID: writerA, coveredSeqs: [], fingerprints: [fp])
        let snapshotPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 100, writerID: writerA, runID: runID)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: snapshotPath) { _ in throw Self.notFoundError() }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[fp])
        XCTAssertEqual(output.acceptedSnapshotBaselinesByMonth[month]?.lamport, 100)
        XCTAssertEqual(race.listCount(path: RepoLayout.snapshotsDirectoryPath(base: basePath)), 2)
    }

    func testSnapshotVanishedDowngradeFailsClosed() async throws {
        let client = try await makeClient()
        try await writeSnapshot(client: client, lamport: 100, writerID: writerA, coveredSeqs: [], fingerprints: [TestFixtures.assetFingerprint(0x21)])
        try await writeSnapshot(client: client, lamport: 200, writerID: writerA, coveredSeqs: [], fingerprints: [TestFixtures.assetFingerprint(0x22)])
        let highPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 200, writerID: writerA, runID: runID)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: highPath) { _ in
            try await client.delete(path: highPath)
            throw Self.notFoundError()
        }

        do {
            _ = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected snapshotVanishedWithoutRecovery")
        } catch RepoMaterializer.MetadataReadRaceError.snapshotVanishedWithoutRecovery(let filename, let month, let lamport, let writerID, _) {
            XCTAssertEqual(filename, RepoLayout.snapshotFileName(month: self.month, lamport: 200, writerID: writerA, runID: runID))
            XCTAssertEqual(month, self.month)
            XCTAssertEqual(lamport, 200)
            XCTAssertEqual(writerID, writerA)
            XCTAssertEqual(race.listCount(path: RepoLayout.snapshotsDirectoryPath(base: basePath)), 2)
        }
    }

    func testSnapshotVanishedRecoveredBySameLamportHigherWriterSucceeds() async throws {
        let client = try await makeClient()
        try await writeSnapshot(client: client, lamport: 200, writerID: writerA, coveredSeqs: [], fingerprints: [TestFixtures.assetFingerprint(0x23)])
        let highPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 200, writerID: writerA, runID: runID)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: highPath) { _ in
            try await client.delete(path: highPath)
            try await self.writeSnapshot(client: client, lamport: 200, writerID: self.writerB, coveredSeqs: [], fingerprints: [TestFixtures.assetFingerprint(0x24)])
            throw Self.notFoundError()
        }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertEqual(output.acceptedSnapshotBaselinesByMonth[month]?.writerID, writerB)
        XCTAssertEqual(race.listCount(path: RepoLayout.snapshotsDirectoryPath(base: basePath)), 2)
    }

    func testRepeatedSnapshotDisappearanceIsBounded() async throws {
        let client = try await makeClient()
        try await writeSnapshot(client: client, lamport: 100, writerID: writerA, coveredSeqs: [], fingerprints: [TestFixtures.assetFingerprint(0x25)])
        let snapshotPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 100, writerID: writerA, runID: runID)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: snapshotPath, once: false) { _ in throw Self.notFoundError() }

        do {
            _ = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected metadataChangedAgainAfterRetry")
        } catch RepoMaterializer.MetadataReadRaceError.metadataChangedAgainAfterRetry {
            XCTAssertEqual(race.listCount(path: RepoLayout.snapshotsDirectoryPath(base: basePath)), 2)
        }
    }

    func testReadableCorruptSnapshotStillFallsBackWithoutRestart() async throws {
        let client = try await makeClient()
        let olderFP = TestFixtures.assetFingerprint(0x26)
        try await writeSnapshot(client: client, lamport: 100, writerID: writerA, coveredSeqs: [], fingerprints: [olderFP])
        try await writeSnapshot(client: client, lamport: 200, writerID: writerA, coveredSeqs: [], fingerprints: [TestFixtures.assetFingerprint(0x27)])
        let highPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 200, writerID: writerA, runID: runID)
        await client.corrupt(path: highPath, with: Data("corrupt".utf8))
        let race = ReadRaceClient(inner: client)

        let output = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)

        XCTAssertNotNil(output.state.months[month]?.assets[olderFP])
        XCTAssertEqual(output.acceptedSnapshotBaselinesByMonth[month]?.lamport, 100)
        XCTAssertEqual(race.listCount(path: RepoLayout.snapshotsDirectoryPath(base: basePath)), 1)
    }

    func testMonthFilterDoesNotReadRacedMetadataFromOtherMonth() async throws {
        let client = try await makeClient()
        let otherMonth = LibraryMonthKey(year: 2026, month: 2)
        try await writeSnapshot(client: client, month: otherMonth, lamport: 100, writerID: writerA, coveredSeqs: [], fingerprints: [TestFixtures.assetFingerprint(0x28)])
        let otherPath = RepoLayout.snapshotFilePath(base: basePath, month: otherMonth, lamport: 100, writerID: writerA, runID: runID)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: otherPath) { _ in throw Self.notFoundError() }

        let output = try await RepoMaterializer(client: race, basePath: basePath).materializeMonth(month, expectedRepoID: repoID)

        XCTAssertNil(output.acceptedSnapshotBaselinesByMonth[otherMonth])
        XCTAssertEqual(race.downloadCount(path: otherPath), 0)
        XCTAssertEqual(race.listCount(path: RepoLayout.snapshotsDirectoryPath(base: basePath)), 1)
    }

    func testReadersMapNotFoundAndPropagateCancellation() async throws {
        let client = try await makeClient()
        let commitFilename = RepoLayout.commitFileName(month: month, writerID: writerA, seq: 1)
        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await client.injectFile(path: commitPath, contents: "corrupt")
        let commitReader = CommitLogReader(client: client, basePath: basePath)
        do {
            _ = try await commitReader.read(filename: commitFilename)
            XCTFail("expected corrupt commit")
        } catch RepoJSONLReadError.notFound {
            XCTFail("corrupt bytes must not map to notFound")
        } catch RepoJSONLReadError.missingEnd, RepoJSONLReadError.missingHeader, RepoJSONLReadError.decodeFailure {
        }
        await client.injectPersistentDownloadError(.notFound, for: commitPath)
        do {
            _ = try await commitReader.read(filename: commitFilename)
            XCTFail("expected notFound")
        } catch RepoJSONLReadError.notFound(let filename) {
            XCTAssertEqual(filename, commitFilename)
        }
        await client.clearPersistentDownloadError(for: commitPath)
        await client.injectDownloadURLErrorCancelled(for: commitPath)
        do {
            _ = try await commitReader.read(filename: commitFilename)
            XCTFail("expected cancellation")
        } catch is CancellationError {
        }

        let snapshotFilename = RepoLayout.snapshotFileName(month: month, lamport: 1, writerID: writerA, runID: runID)
        let snapshotPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 1, writerID: writerA, runID: runID)
        await client.injectFile(path: snapshotPath, contents: "corrupt")
        let snapshotReader = SnapshotReader(client: client, basePath: basePath)
        do {
            _ = try await snapshotReader.read(filename: snapshotFilename)
            XCTFail("expected corrupt snapshot")
        } catch RepoJSONLReadError.notFound {
            XCTFail("corrupt bytes must not map to notFound")
        } catch RepoJSONLReadError.missingEnd, RepoJSONLReadError.missingHeader, RepoJSONLReadError.decodeFailure {
        }
        await client.injectPersistentDownloadError(.notFound, for: snapshotPath)
        do {
            _ = try await snapshotReader.read(filename: snapshotFilename)
            XCTFail("expected notFound")
        } catch RepoJSONLReadError.notFound(let filename) {
            XCTAssertEqual(filename, snapshotFilename)
        }
        await client.clearPersistentDownloadError(for: snapshotPath)
        await client.injectDownloadWrappedURLCancellation(for: snapshotPath)
        do {
            _ = try await snapshotReader.read(filename: snapshotFilename)
            XCTFail("expected cancellation")
        } catch is CancellationError {
        }
    }

    func testIdentityScanIgnoresListedThenVanishedMetadata() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x30))
        try await writeAddCommit(client: client, seq: 2, fingerprint: TestFixtures.assetFingerprint(0x31))
        let vanishedPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await client.injectDownloadError(.notFound, for: vanishedPath)
        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager,
            writerID: writerA,
            basePath: basePath,
            storageType: .webdav
        )

        let resolution = try await RepoIdentityAuthority(
            context: RepoIdentityAuthorityContext(
                profileID: profileID,
                writerID: writerA,
                basePath: basePath,
                dataClient: client,
                identity: identity,
                format: RemoteFormatCompatibilityService()
            )
        ).resolve()

        XCTAssertEqual(resolution.data, repoID)
        XCTAssertEqual(resolution.suggested, repoID)
    }

    func testIdentityScanSpendsGraceOnSoleLaggingListedMetadata() async throws {
        let client = try await makeClient()
        client.setReadAfterWriteGrace(2)
        // The only V2 data file is listed but its GET 404s once inside grace. Skipping it outright
        // would scan to empty while the directory is non-empty, routing the repo to damagedV2Repo.
        try await writeAddCommit(client: client, seq: 1, fingerprint: TestFixtures.assetFingerprint(0x33))
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        await client.injectDownloadError(.notFound, for: path)
        let identity = RepoIdentity(database: databaseManager)
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager,
            writerID: writerA,
            basePath: basePath,
            storageType: .webdav
        )

        let resolution = try await RepoIdentityAuthority(
            context: RepoIdentityAuthorityContext(
                profileID: profileID,
                writerID: writerA,
                basePath: basePath,
                dataClient: client,
                identity: identity,
                format: RemoteFormatCompatibilityService()
            )
        ).resolve()

        XCTAssertEqual(resolution.data, repoID)
        XCTAssertEqual(resolution.suggested, repoID)
    }

    func testCrossMonthObservedSeqDoesNotRecoverVanishedCommit() async throws {
        let otherMonth = LibraryMonthKey(year: 2026, month: 2)
        let client = try await makeClient()
        let fp1 = TestFixtures.assetFingerprint(0x40)
        try await writeAddCommit(client: client, seq: 1, fingerprint: fp1)

        let fpOther = TestFixtures.assetFingerprint(0x41)
        let otherWriter = CommitLogWriter(client: client, basePath: basePath)
        let otherOp = CommitOp(opSeq: 0, clock: 10, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fpOther,
            creationDateMs: nil,
            backedUpAtMs: 10,
            resources: []
        )))
        _ = try await otherWriter.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID, writerID: writerA, seq: 10, runID: runID, month: otherMonth
            ),
            ops: [otherOp],
            month: otherMonth,
            respectTaskCancellation: false
        )

        let commitPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let race = ReadRaceClient(inner: client)
        race.setDownloadHook(path: commitPath) { _ in
            try await client.delete(path: commitPath)
            throw Self.notFoundError()
        }

        do {
            _ = try await RepoMaterializer(client: race, basePath: basePath).materialize(expectedRepoID: repoID)
            XCTFail("expected requiredCommitVanished — cross-month observedSeq must not recover")
        } catch RepoMaterializer.MetadataReadRaceError.requiredCommitVanished(let filename, let m, let w, let seq) {
            XCTAssertEqual(seq, 1)
            XCTAssertEqual(m, month)
            XCTAssertEqual(w, writerA)
        }
    }

    func testListCancellationPropagatesFromCommitAndSnapshotReaders() async throws {
        let commitsDir = RepoLayout.commitsDirectoryPath(base: basePath)
        let snapshotsDir = RepoLayout.snapshotsDirectoryPath(base: basePath)

        let commitClient = try await makeClient()
        await commitClient.injectListWrappedURLCancellation(for: commitsDir)
        let commitReader = CommitLogReader(client: commitClient, basePath: basePath)
        do {
            _ = try await commitReader.listCommitFilenames()
            XCTFail("expected cancellation from list error")
        } catch is CancellationError {
        }

        let snapshotClient = try await makeClient()
        await snapshotClient.injectListWrappedURLCancellation(for: snapshotsDir)
        let snapshotReader = SnapshotReader(client: snapshotClient, basePath: basePath)
        do {
            _ = try await snapshotReader.listSnapshotFilenames()
            XCTFail("expected cancellation from list error")
        } catch is CancellationError {
        }
    }

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerA)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        return client
    }

    private func writeAddCommit(
        client: InMemoryRemoteStorageClient,
        seq: UInt64,
        fingerprint: AssetFingerprint
    ) async throws {
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let op = CommitOp(opSeq: 0, clock: seq, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: Int64(seq),
            resources: []
        )))
        _ = try await writer.write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerA,
                seq: seq,
                runID: runID,
                month: month
            ),
            ops: [op],
            month: month,
            respectTaskCancellation: false
        )
    }

    private func writeSnapshot(
        client: InMemoryRemoteStorageClient,
        month: LibraryMonthKey? = nil,
        lamport: UInt64,
        writerID: String,
        coveredSeqs: [UInt64],
        fingerprints: [AssetFingerprint]
    ) async throws {
        let month = month ?? self.month
        var covered = CoveredRanges.empty
        for seq in coveredSeqs {
            covered.add(writerID: writerA, seq: seq)
        }
        covered.add(writerID: writerID, seq: 1)
        let assets = fingerprints.map { fp in
            SnapshotAssetRow(
                assetFingerprint: fp,
                creationDateMs: nil,
                backedUpAtMs: 1,
                resourceCount: 0,
                totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerID, seq: 1, clock: lamport)
            )
        }
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: SnapshotHeader(
                version: SnapshotHeader.currentVersion,
                scope: CommitHeader.monthScope(month),
                writerID: writerID,
                repoID: repoID,
                covered: covered
            ),
            assets: assets,
            resources: [],
            assetResources: [],
            deletedKeys: [],
            month: month,
            lamport: lamport,
            runID: runID,
            respectTaskCancellation: false
        )
    }

    private static func notFoundError() -> Error {
        RemoteStorageClientError.underlying(NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError))
    }
}

private enum ReadRaceMutationError: Error {
    case mutation(String)
}

private final class LockedCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    func increment() -> Int {
        lock.lock()
        defer { lock.unlock() }
        value += 1
        return value
    }
}

private final class ReadRaceClient: @unchecked Sendable, RemoteStorageClientProtocol {
    typealias DownloadHook = @Sendable (_ path: String) async throws -> Void

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { lock.withLock { graceSeconds } }
    private var graceSeconds: TimeInterval = 0
    func setReadAfterWriteGrace(_ seconds: TimeInterval) { lock.withLock { graceSeconds = seconds } }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        .exclusive
    }

    typealias ListHook = @Sendable (_ callIndex: Int, _ entries: [RemoteStorageEntry]) -> [RemoteStorageEntry]

    private let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var listCounts: [String: Int] = [:]
    private var downloadCounts: [String: Int] = [:]
    private var hooks: [String: (once: Bool, hook: DownloadHook)] = [:]
    private var listHooks: [String: ListHook] = [:]

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    func setDownloadHook(path: String, once: Bool = true, hook: @escaping DownloadHook) {
        lock.withLock {
            hooks[Self.normalize(path)] = (once, hook)
        }
    }

    func setListHook(path: String, hook: @escaping ListHook) {
        lock.withLock {
            listHooks[Self.normalize(path)] = hook
        }
    }

    func listCount(path: String) -> Int {
        lock.withLock { listCounts[Self.normalize(path)] ?? 0 }
    }

    func downloadCount(path: String) -> Int {
        lock.withLock { downloadCounts[Self.normalize(path)] ?? 0 }
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        let key = Self.normalize(path)
        let (callIndex, hook): (Int, ListHook?) = lock.withLock {
            listCounts[key, default: 0] += 1
            return (listCounts[key] ?? 0, listHooks[key])
        }
        let entries = try await inner.list(path: path)
        guard let hook else { return entries }
        return hook(callIndex, entries)
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        try await inner.metadata(path: path)
    }

    func download(remotePath: String, localURL: URL) async throws {
        let key = Self.normalize(remotePath)
        let hook: DownloadHook? = lock.withLock {
            downloadCounts[key, default: 0] += 1
            guard let entry = hooks[key] else { return nil }
            if entry.once { hooks.removeValue(forKey: key) }
            return entry.hook
        }
        if let hook {
            try await hook(key)
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }

    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        XCTFail("materializer must not upload")
        throw ReadRaceMutationError.mutation("upload")
    }

    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        XCTFail("materializer must not atomicCreate")
        throw ReadRaceMutationError.mutation("atomicCreate")
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        XCTFail("materializer must not setModificationDate")
        throw ReadRaceMutationError.mutation("setModificationDate")
    }

    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }

    func delete(path: String) async throws {
        XCTFail("materializer must not delete")
        throw ReadRaceMutationError.mutation("delete")
    }

    func createDirectory(path: String) async throws {
        XCTFail("materializer must not createDirectory")
        throw ReadRaceMutationError.mutation("createDirectory")
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        XCTFail("materializer must not move")
        throw ReadRaceMutationError.mutation("move")
    }

    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        XCTFail("materializer must not moveIfAbsent")
        throw ReadRaceMutationError.mutation("moveIfAbsent")
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        XCTFail("materializer must not copy")
        throw ReadRaceMutationError.mutation("copy")
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

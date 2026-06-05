import XCTest
@testable import Watermelon

/// Focused coverage for SnapshotReader.readAuthenticated / recoverAuthenticatedCoverage — the A1a
/// authenticated header recovery that turns a body-corrupt-but-attested snapshot into repair-only
/// coverage evidence, and fails closed everywhere else.
final class SnapshotReaderRecoveryTests: XCTestCase {
    private let basePath = "/repo"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let foreignRepoID = "99999999-9999-9999-9999-999999999999"
    private let runID = "run-001"
    private let month = LibraryMonthKey(year: 2026, month: 1)
    private let otherMonth = LibraryMonthKey(year: 2026, month: 2)

    private func attestedHeader(
        writerID: String? = nil,
        repoID: String? = nil,
        month monthOverride: LibraryMonthKey? = nil,
        covered: CoveredRanges
    ) -> SnapshotHeader {
        SnapshotHeader(
            version: SnapshotHeader.checkpointVersion,
            scope: CommitHeader.monthScope(monthOverride ?? month),
            writerID: writerID ?? writerA,
            repoID: repoID ?? self.repoID,
            covered: covered,
            createdAtMs: nil,
            coverageAttestation: SnapshotCoverageAttestation()
        )
    }

    private func covered(_ low: UInt64, _ high: UInt64, writer: String? = nil) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [(writer ?? writerA): [ClosedSeqRange(low: low, high: high)]])
    }

    // MARK: - End-to-end via readAuthenticated

    func testFullValidAttestedBodyReadsAsFull() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let header = attestedHeader(covered: covered(1, 3))
        let lamport: UInt64 = 9
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header, assets: [], resources: [], assetResources: [], deletedKeys: [],
            month: month, lamport: lamport, runID: runID, respectTaskCancellation: false
        )
        let path = TestFixtures.attestedSnapshotPath(basePath: basePath, header: header, month: month, lamport: lamport, runID: runID)
        let filename = (path as NSString).lastPathComponent
        let parsed = try XCTUnwrap(RepoLayout.parseSnapshotFilename(filename))
        XCTAssertNotNil(parsed.digest, "a valid attested write must carry a filename digest")

        let result = try await SnapshotReader(client: client, basePath: basePath)
            .readAuthenticated(parsed: parsed, filename: filename, expectedRepoID: repoID)
        guard case .full(let file) = result else { return XCTFail("expected .full for a valid attested body") }
        XCTAssertEqual(file.header.covered.rangesByWriter, covered(1, 3).rangesByWriter)
        XCTAssertNotNil(file.header.coverageAttestation)
    }

    func testCorruptTailAttestedHeaderRecoversAuthenticatedCoverage() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let filename = try await TestFixtures.injectAttestedCorruptSnapshot(
            client, basePath: basePath, month: month, writerID: writerA, repoID: repoID,
            lamport: 9, runID: runID, covered: covered(1, 5)
        )
        let parsed = try XCTUnwrap(RepoLayout.parseSnapshotFilename(filename))

        let result = try await SnapshotReader(client: client, basePath: basePath)
            .readAuthenticated(parsed: parsed, filename: filename, expectedRepoID: repoID)
        guard case .corruptBody(let authenticated) = result else {
            return XCTFail("a body-corrupt attested snapshot must read as .corruptBody")
        }
        XCTAssertEqual(authenticated?.rangesByWriter, covered(1, 5).rangesByWriter,
            "the authenticated covered must be recovered from the intact attested header")
    }

    // MARK: - Fail-closed via recoverAuthenticatedCoverage

    /// Builds the raw body for a hand-crafted corrupt snapshot: intact header line + a broken body line.
    private func corruptRaw(for header: SnapshotHeader) throws -> String {
        try SnapshotRowMapper.encodeHeaderLine(header) + "\ncorrupt-body-not-jsonl\n"
    }

    private func parsedFilename(
        header: SnapshotHeader,
        filenameMonth: LibraryMonthKey? = nil,
        filenameWriterID: String? = nil,
        lamport: UInt64 = 9,
        digestCoveredOverride: CoveredRanges? = nil,
        digestMonthOverride: LibraryMonthKey? = nil,
        digestWriterOverride: String? = nil
    ) -> RepoLayout.ParsedSnapshotFilename {
        let fnMonth = filenameMonth ?? month
        let fnWriter = filenameWriterID ?? header.writerID
        let runIDPrefix = RepoLayout.runIDPrefix(runID)
        let digest = SnapshotCoverageDigest.digest(
            version: SnapshotCoverageAttestation.currentVersion,
            repoID: header.repoID,
            month: digestMonthOverride ?? fnMonth,
            writerID: digestWriterOverride ?? fnWriter,
            filenameLamport: lamport,
            filenameRunIDPrefix: runIDPrefix,
            covered: digestCoveredOverride ?? header.covered
        )
        return RepoLayout.ParsedSnapshotFilename(
            month: fnMonth, lamport: lamport, writerID: fnWriter, runIDPrefix: runIDPrefix, digest: digest
        )
    }

    func testRecoverySucceedsWhenDigestAndIdentityMatch() throws {
        let header = attestedHeader(covered: covered(2, 7))
        let parsed = parsedFilename(header: header)
        let recovered = SnapshotReader.recoverAuthenticatedCoverage(
            rawText: try corruptRaw(for: header), parsed: parsed, expectedRepoID: repoID
        )
        XCTAssertEqual(recovered?.rangesByWriter, covered(2, 7).rangesByWriter)
    }

    func testDigestMismatchFailsClosed() throws {
        let header = attestedHeader(covered: covered(1, 3))
        // Filename digest was computed over a DIFFERENT covered, so the recomputed digest won't match.
        let parsed = parsedFilename(header: header, digestCoveredOverride: covered(1, 9))
        let recovered = SnapshotReader.recoverAuthenticatedCoverage(
            rawText: try corruptRaw(for: header), parsed: parsed, expectedRepoID: repoID
        )
        XCTAssertNil(recovered, "a covered/digest mismatch must fail closed to unknown coverage")
    }

    func testRepoIDMismatchFailsClosed() throws {
        // Header (and its digest) belong to a foreign repo; the expected repoID differs.
        let header = attestedHeader(repoID: foreignRepoID, covered: covered(1, 3))
        let parsed = parsedFilename(header: header)
        let recovered = SnapshotReader.recoverAuthenticatedCoverage(
            rawText: try corruptRaw(for: header), parsed: parsed, expectedRepoID: repoID
        )
        XCTAssertNil(recovered, "a foreign-repo attested header must not authenticate against our repoID")
    }

    func testScopeMonthMismatchFailsClosed() throws {
        // Header scope is otherMonth, but the filename binds `month`; digest matches the filename month.
        let header = attestedHeader(month: otherMonth, covered: covered(1, 3))
        let parsed = parsedFilename(header: header, filenameMonth: month, digestMonthOverride: month)
        let recovered = SnapshotReader.recoverAuthenticatedCoverage(
            rawText: try corruptRaw(for: header), parsed: parsed, expectedRepoID: repoID
        )
        XCTAssertNil(recovered, "a header whose scope month disagrees with the filename month must fail closed")
    }

    func testWriterIDMismatchFailsClosed() throws {
        // Header writerID is writerB; the filename binds writerA; digest matches the filename writerID.
        let header = attestedHeader(writerID: writerB, covered: covered(1, 3))
        let parsed = parsedFilename(header: header, filenameWriterID: writerA, digestWriterOverride: writerA)
        let recovered = SnapshotReader.recoverAuthenticatedCoverage(
            rawText: try corruptRaw(for: header), parsed: parsed, expectedRepoID: repoID
        )
        XCTAssertNil(recovered, "a header whose writerID disagrees with the filename writerID must fail closed")
    }

    func testUnreadableHeaderFailsClosed() {
        let parsed = RepoLayout.ParsedSnapshotFilename(
            month: month, lamport: 9, writerID: writerA, runIDPrefix: RepoLayout.runIDPrefix(runID),
            digest: String(repeating: "a", count: 64)
        )
        let recovered = SnapshotReader.recoverAuthenticatedCoverage(
            rawText: "garbage-not-json\nmore-garbage\n", parsed: parsed, expectedRepoID: repoID
        )
        XCTAssertNil(recovered, "an unparseable first line must fail closed")
    }

    func testLegacyCorruptWithoutFilenameDigestFailsClosed() throws {
        // No filename digest at all (legacy filename) ⇒ coverage unknown regardless of header contents.
        let header = attestedHeader(covered: covered(1, 3))
        let parsed = RepoLayout.ParsedSnapshotFilename(
            month: month, lamport: 9, writerID: writerA, runIDPrefix: RepoLayout.runIDPrefix(runID), digest: nil
        )
        let recovered = SnapshotReader.recoverAuthenticatedCoverage(
            rawText: try corruptRaw(for: header), parsed: parsed, expectedRepoID: repoID
        )
        XCTAssertNil(recovered, "a legacy filename carries no digest, so corrupt coverage stays unknown")
    }

    func testUnattestedHeaderFailsClosed() throws {
        // A digested filename but a header with no coverage attestation (e.g. legacy header smuggled under
        // an attested name) must not authenticate.
        let legacyHeader = SnapshotHeader(
            version: SnapshotHeader.currentVersion, scope: CommitHeader.monthScope(month),
            writerID: writerA, repoID: repoID, covered: covered(1, 3), createdAtMs: nil
        )
        let parsed = parsedFilename(header: legacyHeader)
        let recovered = SnapshotReader.recoverAuthenticatedCoverage(
            rawText: try corruptRaw(for: legacyHeader), parsed: parsed, expectedRepoID: repoID
        )
        XCTAssertNil(recovered, "a header without a coverage attestation must not authenticate")
    }
}

import XCTest
@testable import Watermelon

/// On a non-atomic backend (SMB exists+upload TOCTOU), `CommitLogWriter` returns
/// `.bestEffortRetry` and verifies the remote bytes via SHA roundtrip. SHA mismatch
/// → throw `.alreadyExists` so the upper layer re-allocates seq and retries.
final class CommitLogWriterBestEffortTests: XCTestCase {
    private let basePath = "/repo"
    private let month = LibraryMonthKey(year: 2026, month: 1)
    private let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let repoID = "test-repo"

    func testBestEffortVerify_succeedsWhenBytesMatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        // No race injection — InMemoryRemoteStorageClient writes our own bytes,
        // so verify-on-remote round-trips successfully.
        let file = try await writer.write(
            header: header, ops: [op], month: month, respectTaskCancellation: false
        )
        XCTAssertEqual(file.rowCount, 2)
    }

    func testBestEffortVerify_throwsAlreadyExistsWhenRemoteBytesDiffer() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        // Stage a self-consistent peer commit at the seq=1 path. atomicCreate will
        // return `.bestEffortRetry` but remote will contain the peer's bytes (which
        // parse cleanly but have a different SHA than what the local writer produced)
        // → verifyCommitOnRemote's SHA check must fail and throw `.alreadyExists`.
        let peerOp = sampleOp(opSeq: 0, clock: 1)
        let peerHeader = makeHeader(seq: 1, clock: 1)
        let peerBytes = try Self.encodeCommit(header: peerHeader, ops: [peerOp])
        let ourOp = CommitOp(
            opSeq: 0, clock: 1,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.fingerprint(0xBB),  // different fp → different SHA
                reason: .userDeleted
            ))
        )
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.stageBestEffortRace(at: path, with: peerBytes)

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [ourOp],
                month: month,
                respectTaskCancellation: false
            )
            XCTFail("expected .alreadyExists due to verify SHA mismatch")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected — caller will re-allocate seq and retry
        }
    }

    /// Build a self-consistent commit jsonl outside of `CommitLogWriter.write` so we
    /// can stage it as a "peer's bytes". Mirrors the writer's encoding.
    private static func encodeCommit(header: CommitHeader, ops: [CommitOp]) throws -> Data {
        var integrity = IntegrityAccumulator()
        var lines: [String] = []
        let h = try CommitOpMapper.encodeHeaderLine(header)
        integrity.absorbLine(h)
        lines.append(h)
        for op in ops {
            let line = try CommitOpMapper.encodeOpLine(op)
            integrity.absorbLine(line)
            lines.append(line)
        }
        let sha = integrity.finalize()
        let endLine = try CommitOpMapper.encodeEndLine(sha256Hex: sha, rowCount: integrity.rowCount)
        lines.append(endLine)
        return Data((lines.joined(separator: "\n") + "\n").utf8)
    }

    /// Contract: verify-on-remote treats ANY mismatch as a race signal. Unparseable
    /// remote bytes (truncated peer write, garbage at our seq slot) get the same
    /// `.alreadyExists` classification as SHA mismatches — flush layer re-allocates
    /// the seq and retries.
    func testBestEffortVerify_unparseableRemoteBytes_shouldClassifyAsAlreadyExists() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        // Bytes that fail to parse as a commit jsonl — simulating a peer whose
        // upload was truncated mid-commit on a non-atomic backend.
        await client.stageBestEffortRace(at: path, with: Data("garbage that doesn't parse\n".utf8))

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [sampleOp(opSeq: 0, clock: 1)],
                month: month,
                respectTaskCancellation: false
            )
            XCTFail("expected the writer to throw on remote-bytes mismatch")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // success — the desired contract holds
        } catch {
            XCTFail("contract violated: expected .alreadyExists, got \(error)")
        }
    }

    private func makeHeader(seq: UInt64, clock: UInt64) -> CommitHeader {
        CommitHeader(
            version: CommitHeader.currentVersion,
            repoID: repoID,
            writerID: writerID,
            seq: seq,
            runID: "run-001",
            scope: CommitHeader.monthScope(month),
            clockMin: clock,
            clockMax: clock,
            bodyKind: CommitHeader.bodyKindPlain
        )
    }

    private func sampleOp(opSeq: Int, clock: UInt64) -> CommitOp {
        CommitOp(
            opSeq: opSeq, clock: clock,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.fingerprint(0xAA),
                reason: .userDeleted
            ))
        )
    }
}

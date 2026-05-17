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
        // AMSMB2 production parity: atomicCreate is overwritePossible but moveIfAbsent
        // is exclusive (libsmb2's rename refuses on collision).
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        // No peer collision — gate stages, moveIfAbsent succeeds, post-verify matches.
        let file = try await writer.write(
            header: header, ops: [op], month: month, respectTaskCancellation: false
        )
        XCTAssertEqual(file.rowCount, 2)
    }

    func testBestEffortVerify_throwsAlreadyExistsWhenRemoteBytesDiffer() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        // Pre-populate the seq=1 path with a self-consistent peer commit. The gate's
        // `moveIfAbsent` from staging→final will see the destination occupied → return
        // `.alreadyExists`; the post-readback SHA confirms peer bytes ≠ local bytes →
        // surfaces `.alreadyExists` so the flusher re-allocates seq.
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
        await client.injectFile(path: path, data: peerBytes)

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [ourOp],
                month: month,
                respectTaskCancellation: false
            )
            XCTFail("expected .alreadyExists due to peer collision at the final path")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected — caller will re-allocate seq and retry
        }
        // Peer commit must still be on remote.
        let afterWrite = await client.snapshotFiles()
        XCTAssertEqual(afterWrite[path], peerBytes)
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

    /// Guards against regression to a direct `client.atomicCreate(remotePath:)` publish.
    /// On `.overwritePossible` backends (AMSMB2 `uploadItem(toPath:)`), `exists+upload`
    /// TOCTOU would silently replace a peer's commit bytes with ours, and the writer's
    /// self-SHA verify would report success because the remote bytes (ours) match our
    /// local SHA — peer's seq lost from the commit log.
    ///
    /// With the fix, the writer routes overwrite-prone backends through
    /// `MetadataCreateGate.createWithStagingFallback(.requireExclusiveMove)` so a
    /// same-`(writer, seq)` collision surfaces as `.alreadyExists` and the flusher
    /// re-allocates seq. Peer commit bytes at the final path must survive.
    func testOverwritePossibleBackend_silentlyOverwritingPeerCommit_surfacesAsAlreadyExists() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Production parity with AMSMB2: atomicCreate is overwritePossible but
        // moveIfAbsent is exclusive (libsmb2's rename refuses on collision).
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let peerOp = sampleOp(opSeq: 0, clock: 1)
        let peerHeader = makeHeader(seq: 1, clock: 1)
        let peerBytes = try Self.encodeCommit(header: peerHeader, ops: [peerOp])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: peerBytes)

        let ourOp = CommitOp(
            opSeq: 0, clock: 1,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.fingerprint(0xCC),
                reason: .userDeleted
            ))
        )
        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [ourOp],
                month: month,
                respectTaskCancellation: false
            )
            XCTFail("expected .alreadyExists — peer commit must not be silently overwritten")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected — flusher would re-allocate seq and retry
        }

        // Peer's commit must still be on remote — the writer must not have replaced it.
        let afterWrite = await client.snapshotFiles()
        XCTAssertEqual(afterWrite[path], peerBytes,
                       "peer commit bytes must survive — losing them silently drops backed-up ops from the commit log")
    }

    /// Direct-shape coverage: if a future regression routes commit publish back through
    /// `client.atomicCreate(remotePath:)` on an overwrite-prone backend, peer bytes get
    /// silently replaced. This test arms the in-memory client's overwrite hook on the
    /// final commit path and asserts the hook never fires — `CommitLogWriter.write` must
    /// stage to a UUID side path, not write the final path directly.
    func testOverwritePossibleBackend_writerMustNotAtomicCreateAtFinalPath() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.stageBestEffortOverwriteOfExistingPath(at: path)
        // No peer bytes pre-injected — exercising the happy path.
        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        _ = try await writer.write(
            header: header, ops: [op], month: month, respectTaskCancellation: false
        )

        // If the writer ever called `atomicCreate(remotePath: path)` directly, the hook
        // would have been consumed (removed from the set). Hook still present ⇒ writer
        // correctly bypassed the final-path overwrite codepath.
        let hookStillArmed = await client.isBestEffortOverwriteStaged(at: path)
        XCTAssertTrue(hookStillArmed,
                      "writer must not invoke atomicCreate on the final commit path; staging hook should remain unfired")
    }

    /// Contract: verify-on-remote treats ANY mismatch as a race signal. Unparseable
    /// remote bytes (truncated peer write, garbage at our seq slot) get the same
    /// `.alreadyExists` classification as SHA mismatches — flush layer re-allocates
    /// the seq and retries.
    func testBestEffortVerify_unparseableRemoteBytes_shouldClassifyAsAlreadyExists() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        // Bytes that fail to parse as a commit jsonl — simulating a peer whose
        // upload was truncated mid-commit on a non-atomic backend.
        await client.injectFile(path: path, data: Data("garbage that doesn't parse\n".utf8))

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

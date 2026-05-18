import XCTest
@testable import Watermelon

/// Commit publication contract regardless of backend shape.
/// - `.overwritePossible` (SMB exists+upload): writer must stage via the gate and
///   surface same-`(writer, seq)` collisions as `.alreadyExists` instead of
///   silently overwriting peer bytes.
/// - `.exclusive` (S3 If-None-Match / POSIX O_EXCL): writer must SHA-verify a
///   `.alreadyExists` outcome so an S3 single-part PUT phantom (bytes landed,
///   response lost) is absorbed as success while mismatched peer bytes still
///   surface as `.alreadyExists` for seq re-allocation.
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

    /// `.exclusive` backend (S3 If-None-Match): when prior bytes at the final path
    /// match the bytes we're trying to write — e.g. S3 single-part PUT phantom where
    /// the upload landed but the response was lost and the retry hits 412 — the
    /// writer must absorb `.alreadyExists` rather than allocate a new seq + write a
    /// duplicate commit row that LWW will reconcile to the same state.
    func testExclusiveBackend_alreadyExistsWithMatchingBytes_succeeds() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Switch the in-memory client to .exclusive so the writer's `.exclusive`
        // arm is exercised (default is .overwritePossible → staging fallback).
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        // Pre-populate the final path with bytes that match what the writer would
        // produce; the next atomicCreate returns `.alreadyExists` (in-memory's
        // .strictlyAtomic semantics) and the writer must SHA-verify and accept.
        let ourBytes = try Self.encodeCommit(header: header, ops: [op])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: ourBytes)

        let file = try await writer.write(
            header: header, ops: [op], month: month, respectTaskCancellation: false
        )
        XCTAssertEqual(file.rowCount, 2)
        // Remote bytes must remain the matched payload — writer must NOT have rewritten.
        let afterWrite = await client.snapshotFiles()
        XCTAssertEqual(afterWrite[path], ourBytes)
    }

    /// `.exclusive` backend, mismatched remote bytes (peer wrote a different commit
    /// at the same `(writer, seq)` slot): writer must still throw `.alreadyExists`
    /// so the flusher re-allocates seq. Pins that the matching-bytes shortcut does
    /// not weaken the foreign-bytes guard.
    func testExclusiveBackend_alreadyExistsWithMismatchedBytes_throwsAlreadyExists() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let peerOp = CommitOp(
            opSeq: 0, clock: 1,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.fingerprint(0xDD),
                reason: .userDeleted
            ))
        )
        let peerHeader = makeHeader(seq: 1, clock: 1)
        let peerBytes = try Self.encodeCommit(header: peerHeader, ops: [peerOp])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: peerBytes)

        let ourOp = sampleOp(opSeq: 0, clock: 1)
        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [ourOp],
                month: month,
                respectTaskCancellation: false
            )
            XCTFail("expected .alreadyExists — peer bytes must not be accepted as our own")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected
        }
        // Peer bytes must survive untouched.
        let afterWrite = await client.snapshotFiles()
        XCTAssertEqual(afterWrite[path], peerBytes)
    }

    /// `.exclusive` backend, `.alreadyExists` outcome, and `download` during the
    /// SHA self-verify is cancelled (user stop / task cancel): the writer must
    /// surface `CancellationError`, not wrap it as `WriteError.ioFailure`. Callers
    /// up the stack treat cancellation specially (pause vs. abort) — wrapping it
    /// makes the run look like a transport failure and produces the wrong UI.
    func testExclusiveBackend_alreadyExistsThenDownloadCancelled_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        // Pre-populate the final path so atomicCreate returns .alreadyExists, which
        // routes through verifyCommitOnRemote → client.download.
        let bytes = try Self.encodeCommit(header: header, ops: [op])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: bytes)
        await client.injectDownloadCancellation(for: path)

        do {
            _ = try await writer.write(
                header: header, ops: [op], month: month, respectTaskCancellation: false
            )
            XCTFail("expected CancellationError to propagate from the verify download")
        } catch is CancellationError {
            // expected — cancellation preserved
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    /// S3 / URLSession-backed clients surface task cancellation as
    /// `NSURLErrorCancelled`, not literal `CancellationError`. `verifyCommitOnRemote`
    /// must normalize that so user stop/cancellation is distinguishable from
    /// `WriteError.ioFailure` up-stack (pause vs. abort UI).
    func testExclusiveBackend_alreadyExistsThenDownloadURLErrorCancelled_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        let bytes = try Self.encodeCommit(header: header, ops: [op])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: bytes)
        await client.injectDownloadURLErrorCancelled(for: path)

        do {
            _ = try await writer.write(
                header: header, ops: [op], month: month, respectTaskCancellation: false
            )
            XCTFail("expected URLSession-shaped cancellation to surface as CancellationError, not .ioFailure")
        } catch is CancellationError {
            // expected — cancellation normalized from NSURLErrorCancelled
        } catch CommitLogWriter.WriteError.ioFailure(let underlying) {
            XCTFail("URLErrorCancelled must NOT wrap as .ioFailure (got: \(underlying))")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    /// `.exclusive` backend, cancellation during the primary `atomicCreate` call
    /// itself (not during the SHA verify after `.alreadyExists`). S3 URLSession
    /// surfaces task cancellation as raw `NSURLErrorCancelled`; without
    /// normalization the writer would wrap it as `.ioFailure` and
    /// `BackupParallelExecutor` would treat a user stop as a transport failure.
    func testExclusiveBackend_atomicCreateURLErrorCancelled_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectAtomicCreateURLErrorCancelled(for: path)

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [sampleOp(opSeq: 0, clock: 1)],
                month: month, respectTaskCancellation: false
            )
            XCTFail("expected CancellationError from a URLSession-shaped cancel on the primary write")
        } catch is CancellationError {
            // expected
        } catch CommitLogWriter.WriteError.ioFailure(let underlying) {
            XCTFail("URLErrorCancelled at atomicCreate must NOT wrap as .ioFailure (got: \(underlying))")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    /// Pins `CommitLogWriter.isCancellationError`'s recursion arm:
    /// `RemoteStorageClientError.underlying(NSURLErrorCancelled)` must still
    /// normalize to `CancellationError`. Models a future adapter that wraps
    /// URLSession errors in `.underlying` — without the recursion arm the
    /// cancellation would silently demote to `WriteError.ioFailure`.
    func testExclusiveBackend_alreadyExistsThenDownloadWrappedURLCancellation_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        let bytes = try Self.encodeCommit(header: header, ops: [op])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: bytes)
        await client.injectDownloadWrappedURLCancellation(for: path)

        do {
            _ = try await writer.write(
                header: header, ops: [op], month: month, respectTaskCancellation: false
            )
            XCTFail("expected wrapped URLSession cancellation to surface as CancellationError")
        } catch is CancellationError {
            // expected — recursion arm of isCancellationError unwrapped the `.underlying` payload
        } catch CommitLogWriter.WriteError.ioFailure(let underlying) {
            XCTFail("wrapped URLErrorCancelled must NOT wrap as .ioFailure (got: \(underlying))")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    /// `.exclusive` + `.alreadyExists` + transient transport error on the SHA
    /// verify download must collapse back to `.alreadyExists` so the flusher
    /// re-allocates seq and retries. Before this fix, the verify download's
    /// transient ioFailure aborted the flush and surfaced as a user-visible
    /// failure — a regression compared to the pre-SHA-verify retry behaviour.
    func testExclusiveBackend_alreadyExistsThenVerifyTransientFailure_classifiesAsAlreadyExists() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        // Pre-populate the final path so atomicCreate returns .alreadyExists, then
        // arm the next verify download to fail transiently.
        let bytes = try Self.encodeCommit(header: header, ops: [op])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: bytes)
        await client.injectDownloadError(.transport, for: path)

        do {
            _ = try await writer.write(
                header: header, ops: [op], month: month, respectTaskCancellation: false
            )
            XCTFail("expected .alreadyExists after transient verify failure (so flusher can retry)")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected — flusher re-allocates seq + retries; materializer tolerates
            // the duplicate-content commit if the original .alreadyExists was a real
            // self-write phantom.
        } catch {
            XCTFail("expected .alreadyExists, got \(error)")
        }
    }

    /// `.overwritePossible` backend (AMSMB2 production parity): user-stop fires
    /// while the gate is running its staging-verify download. `URLSession` surfaces
    /// task cancellation as `NSURLErrorCancelled`; without normalization inside
    /// the gate's `verifyMatchesLocalWithRetries`, the retry loop swallows the
    /// cancel-shape as `lastError`, hits the read-after-write deadline, then the
    /// `MetadataCreateGate.Error.stagingVerificationFailed(underlying:)` wrap reaches
    /// the writer as a generic gate error. The writer's `isMetadataGateCancellation`
    /// is the second line of defence — this test pins both layers.
    func testOverwritePossibleBackend_gateStagingVerifyURLCancellation_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        // Gate stages at a UUID side-path, so per-path download injection can't reach
        // it; use the path-agnostic one-shot.
        await client.injectNextDownloadURLErrorCancelled()

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [sampleOp(opSeq: 0, clock: 1)],
                month: month, respectTaskCancellation: false
            )
            XCTFail("expected CancellationError from the gate's staging-verify URL cancellation")
        } catch is CancellationError {
            // expected
        } catch CommitLogWriter.WriteError.ioFailure(let underlying) {
            XCTFail("URL-cancel through the gate must NOT surface as .ioFailure (got: \(underlying))")
        } catch CommitLogWriter.WriteError.alreadyExists {
            XCTFail("URL-cancel through the gate must NOT silently demote to .alreadyExists")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    /// `.overwritePossible` backend + same-(writer, seq) collision: the gate's
    /// `moveIfAbsent` returns `.alreadyExists`, and the gate's verify of the
    /// occupied final path runs `client.download(remotePath:finalPath)`. If that
    /// download fails with URLSession-shaped cancellation, the gate must surface
    /// it as `CancellationError` rather than treating the verify as "inconclusive"
    /// and returning `.alreadyExists` (which would silently demote a user stop to
    /// a seq-collision retry up at the flusher).
    func testOverwritePossibleBackend_gateFinalVerifyURLCancellation_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        // Pre-occupy the final path so the gate's moveIfAbsent (staging → final)
        // returns .alreadyExists and routes into the post-move verify branch.
        let peerOp = sampleOp(opSeq: 0, clock: 1)
        let peerHeader = makeHeader(seq: 1, clock: 1)
        let peerBytes = try Self.encodeCommit(header: peerHeader, ops: [peerOp])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: peerBytes)
        // First download is the gate's staging-verify (succeeds). Second is the
        // post-move final-verify — arm THAT one with URL cancellation by injecting
        // per-path so the staging-verify pass-through is undisturbed.
        await client.injectDownloadURLErrorCancelled(for: path)

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [sampleOp(opSeq: 0, clock: 1)],
                month: month, respectTaskCancellation: false
            )
            XCTFail("expected CancellationError from the gate's post-move final-verify")
        } catch is CancellationError {
            // expected — gate normalizes URL-cancel, writer's generic catch passes it through
        } catch CommitLogWriter.WriteError.alreadyExists {
            XCTFail("URL-cancel during final-verify must NOT demote to .alreadyExists")
        } catch CommitLogWriter.WriteError.ioFailure(let underlying) {
            XCTFail("URL-cancel during final-verify must NOT wrap as .ioFailure (got: \(underlying))")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    /// Negative side of the gate-cancellation helper: a real
    /// `MetadataCreateGate.Error.nonExclusiveFinalization` (the backend doesn't
    /// support exclusive move and we required it) must NOT normalize to
    /// `CancellationError` — it's a real configuration failure and should surface
    /// as `WriteError.ioFailure` so the operator sees the actionable cause.
    func testOverwritePossibleBackend_nonExclusiveFinalization_surfacesAsIOFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // overwrite-possible moveIfAbsent + requireExclusiveMove policy ⇒ nonExclusiveFinalization
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [sampleOp(opSeq: 0, clock: 1)],
                month: month, respectTaskCancellation: false
            )
            XCTFail("expected .ioFailure — nonExclusiveFinalization is a real config error, not cancellation")
        } catch is CancellationError {
            XCTFail("nonExclusiveFinalization must NOT normalize to CancellationError")
        } catch CommitLogWriter.WriteError.ioFailure {
            // expected
        } catch {
            XCTFail("expected .ioFailure, got \(error)")
        }
    }

    /// `.exclusive` + `.alreadyExists` + PERMANENT (non-transient) failure on the
    /// SHA-verify download (e.g. permission denied, AccessDenied, NoSuchKey).
    /// Must surface as `WriteError.ioFailure` so the operator sees the actionable
    /// cause — not get demoted to `.alreadyExists` (which the flusher would retry
    /// up to 4 times and ultimately report as a seq collision rather than the
    /// underlying permission/auth issue).
    func testExclusiveBackend_alreadyExistsThenVerifyPermanentFailure_surfacesAsIOFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        let bytes = try Self.encodeCommit(header: header, ops: [op])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: bytes)
        // Permission denied — NOT transient. Must propagate as .ioFailure.
        await client.injectDownloadError(.permission, for: path)

        do {
            _ = try await writer.write(
                header: header, ops: [op], month: month, respectTaskCancellation: false
            )
            XCTFail("expected .ioFailure — permanent verify failure must NOT demote to .alreadyExists")
        } catch CommitLogWriter.WriteError.ioFailure {
            // expected — the underlying permission error is preserved
        } catch CommitLogWriter.WriteError.alreadyExists {
            XCTFail("permanent verify failure must NOT silently demote to .alreadyExists")
        } catch {
            XCTFail("expected .ioFailure, got \(error)")
        }
    }

    /// Sibling positive case: a not-found during verify (the verify download says
    /// the file isn't there, even though atomicCreate said `.alreadyExists`) is a
    /// genuine anomaly — propagate as `.ioFailure` so the operator can investigate
    /// instead of silently retrying with a fresh seq.
    func testExclusiveBackend_alreadyExistsThenVerifyNotFound_surfacesAsIOFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let header = makeHeader(seq: 1, clock: 1)
        let op = sampleOp(opSeq: 0, clock: 1)
        let bytes = try Self.encodeCommit(header: header, ops: [op])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: bytes)
        await client.injectDownloadError(.notFound, for: path)

        do {
            _ = try await writer.write(
                header: header, ops: [op], month: month, respectTaskCancellation: false
            )
            XCTFail("expected .ioFailure — not-found during verify is a genuine anomaly, not a seq collision")
        } catch CommitLogWriter.WriteError.ioFailure {
            // expected
        } catch CommitLogWriter.WriteError.alreadyExists {
            XCTFail("verify-not-found must NOT demote to .alreadyExists")
        } catch {
            XCTFail("expected .ioFailure, got \(error)")
        }
    }

    /// `.overwritePossible` backend, no peer at final path, gate's
    /// post-success final-verify download fails TRANSIENTLY (the bytes ARE at the
    /// destination — move succeeded — but readback was inconclusive). Gate must
    /// surface `.bestEffortRetry`; writer's `.requireExclusiveMove` arm then runs
    /// `verifyAfterAlreadyExists`, which collapses the same transient on its own
    /// download into `WriteError.alreadyExists` so the flusher re-allocates seq.
    /// Without the gate's transient classification at the outer post-success
    /// verify, a recoverable transport hiccup would throw
    /// `MetadataCreateGate.Error.finalVerificationFailed` → wrapped as
    /// `WriteError.ioFailure`, turning a retryable backend blip into a fatal
    /// finalization failure even though the bytes did land.
    func testOverwritePossibleBackend_outerFinalVerifyTransientFailure_demotesToBestEffortRetry() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        // Final path NOT pre-occupied — moveIfAbsent returns .created. The gate's
        // staging-verify download (against the UUID staging path) is unaffected by
        // a per-final-path persistent injection; only the gate's outer post-move
        // verify and the writer's verifyAfterAlreadyExists download hit this hook.
        await client.injectPersistentDownloadError(.transport, for: path)

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [sampleOp(opSeq: 0, clock: 1)],
                month: month, respectTaskCancellation: false
            )
            XCTFail("expected .alreadyExists — gate demotes transient outer-verify to .bestEffortRetry, writer's verifyAfterAlreadyExists collapses to .alreadyExists")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected — flusher would re-allocate seq and retry
        } catch CommitLogWriter.WriteError.ioFailure(let underlying) {
            XCTFail("transient outer-verify must NOT throw .finalVerificationFailed → .ioFailure (got: \(underlying))")
        } catch {
            XCTFail("expected .alreadyExists, got \(error)")
        }

        // Bytes ARE at the final path because moveIfAbsent ran successfully before
        // the post-move verify failed — pins that `.bestEffortRetry` semantics
        // (recoverable readback after the bytes already landed) is the right
        // mental model, not a "move failed" interpretation.
        let afterWrite = await client.snapshotFiles()
        XCTAssertNotNil(afterWrite[path], "moveIfAbsent succeeded — final path must contain bytes")
    }

    /// `.overwritePossible` backend, no peer at the final path, gate's
    /// post-success final-verify download fails PERMANENTLY (e.g. permission
    /// denied / AccessDenied). The transient case is covered by
    /// `testOverwritePossibleBackend_outerFinalVerifyTransientFailure_demotesToBestEffortRetry`;
    /// the analogous-but-different `.alreadyExists` (peer-occupied) path's
    /// permanent case is covered by
    /// `testOverwritePossibleBackend_finalVerifyPermanentFailure_surfacesAsIOFailure`.
    /// This pins the move-succeeded path's permanent branch (gate's
    /// `.finalVerificationFailed` → writer's `.ioFailure`) so a regression that
    /// widens the transient catch into a non-exhaustive default would surface.
    func testOverwritePossibleBackend_outerFinalVerifyPermanentFailure_surfacesAsIOFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        // Final path NOT pre-occupied — moveIfAbsent returns .created. Permission
        // denied is NOT transient, so the gate must throw .finalVerificationFailed
        // (not demote to .bestEffortRetry).
        await client.injectPersistentDownloadError(.permission, for: path)

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [sampleOp(opSeq: 0, clock: 1)],
                month: month, respectTaskCancellation: false
            )
            XCTFail("expected .ioFailure — permanent outer-verify failure must NOT demote to .alreadyExists or .bestEffortRetry")
        } catch CommitLogWriter.WriteError.ioFailure(let underlying) {
            // The gate's outer post-move classification must throw
            // .finalVerificationFailed for this remote path so the actionable
            // cause survives the writer's .ioFailure wrap. A bare
            // RemoteStorageClientError underlying would mean the gate widened
            // its transient catch into a default and the writer's
            // verifyAfterAlreadyExists surfaced the persistent permission
            // error — same outer WriteError, wrong production path.
            guard case MetadataCreateGate.Error.finalVerificationFailed(let failedPath, let cause) = underlying else {
                XCTFail("expected MetadataCreateGate.Error.finalVerificationFailed, got \(underlying)")
                return
            }
            XCTAssertEqual(failedPath, path, "finalVerificationFailed must name the final commit path")
            XCTAssertNotNil(cause, "permanent permission denial must be preserved as the underlying cause")
        } catch CommitLogWriter.WriteError.alreadyExists {
            XCTFail("permanent outer-verify failure must NOT silently demote to .alreadyExists")
        } catch is CancellationError {
            XCTFail("permission denial must NOT normalize to CancellationError")
        } catch {
            XCTFail("expected .ioFailure, got \(error)")
        }

        // Bytes are at the final path — moveIfAbsent succeeded before verify
        // failed. The failure is purely a readback/permission issue.
        let afterWrite = await client.snapshotFiles()
        XCTAssertNotNil(afterWrite[path], "moveIfAbsent succeeded — final path must contain bytes")
    }

    /// `.overwritePossible` backend + peer-occupied final path + PERMANENT failure
    /// on the gate's post-move verify download (e.g. permission denied, AccessDenied,
    /// NoSuchKey). Must surface as `WriteError.ioFailure` so the operator sees the
    /// actionable cause; without classification the gate would swallow the permanent
    /// error as "verify inconclusive" and return `.alreadyExists`, which the writer
    /// would re-allocate seq for — burning retries before the next collision finally
    /// hits the same wall and reports as a generic seq-exhaustion failure.
    func testOverwritePossibleBackend_finalVerifyPermanentFailure_surfacesAsIOFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        // Pre-occupy the final path so the gate's moveIfAbsent returns .alreadyExists
        // and routes into the post-move verify-download branch.
        let peerOp = sampleOp(opSeq: 0, clock: 1)
        let peerHeader = makeHeader(seq: 1, clock: 1)
        let peerBytes = try Self.encodeCommit(header: peerHeader, ops: [peerOp])
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        await client.injectFile(path: path, data: peerBytes)
        // Persistent permission denial — the retry loop hits the deadline and throws
        // the permanent error rather than the cancellation arm.
        await client.injectPersistentDownloadError(.permission, for: path)

        do {
            _ = try await writer.write(
                header: makeHeader(seq: 1, clock: 1),
                ops: [sampleOp(opSeq: 0, clock: 1)],
                month: month, respectTaskCancellation: false
            )
            XCTFail("expected .ioFailure — permanent verify failure must NOT demote to .alreadyExists at the gate boundary")
        } catch CommitLogWriter.WriteError.ioFailure {
            // expected — the gate throws .finalVerificationFailed(underlying:), the
            // writer wraps as .ioFailure, the actionable cause is preserved.
        } catch CommitLogWriter.WriteError.alreadyExists {
            XCTFail("permanent post-move verify failure must NOT silently demote to .alreadyExists")
        } catch is CancellationError {
            XCTFail("permission denial must NOT normalize to CancellationError")
        } catch {
            XCTFail("expected .ioFailure, got \(error)")
        }
    }

    // MARK: - isMetadataGateCancellation direct coverage

    /// `MetadataCreateGate.Error.stagingVerificationFailed(underlying:
    /// NSURLErrorCancelled)` must classify as cancellation so the writer surfaces it
    /// as `CancellationError` rather than `WriteError.ioFailure`. Mirrors the
    /// defense-in-depth path for any future regression that drops the gate's own
    /// cancellation normalization.
    func testIsMetadataGateCancellation_stagingVerificationFailed_withURLErrorCancelled_isCancellation() {
        let underlying = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled, userInfo: nil)
        let gateError = MetadataCreateGate.Error.stagingVerificationFailed(remotePath: "/x", underlying: underlying)
        XCTAssertTrue(CommitLogWriter.isMetadataGateCancellation(gateError))
    }

    /// Wrapped form: `RemoteStorageClientError.underlying(NSURLErrorCancelled)`
    /// recurses through `isCancellationError` and must still classify as cancellation.
    func testIsMetadataGateCancellation_finalVerificationFailed_withWrappedURLCancellation_isCancellation() {
        let url = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled, userInfo: nil)
        let wrapped = RemoteStorageClientError.underlying(url)
        let gateError = MetadataCreateGate.Error.finalVerificationFailed(remotePath: "/x", underlying: wrapped)
        XCTAssertTrue(CommitLogWriter.isMetadataGateCancellation(gateError))
    }

    /// `.nonExclusiveFinalization` is a real configuration failure — never a
    /// cancellation. Pinning prevents a future widening of the helper that would
    /// swallow this distinctive error.
    func testIsMetadataGateCancellation_nonExclusiveFinalization_isNotCancellation() {
        let gateError = MetadataCreateGate.Error.nonExclusiveFinalization(remotePath: "/x")
        XCTAssertFalse(CommitLogWriter.isMetadataGateCancellation(gateError))
    }

    /// Non-cancellation underlying (permission denied) on a verification-failed
    /// wrapper must NOT classify as cancellation; that would demote a real IO
    /// failure to a user-stop signal.
    func testIsMetadataGateCancellation_stagingVerificationFailed_withPermissionDenied_isNotCancellation() {
        let underlying = NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoPermissionError, userInfo: nil)
        let gateError = MetadataCreateGate.Error.stagingVerificationFailed(remotePath: "/x", underlying: underlying)
        XCTAssertFalse(CommitLogWriter.isMetadataGateCancellation(gateError))
    }

    /// `nil` underlying (the gate threw because verify returned `false` cleanly, not
    /// because it errored out) is not cancellation — the caller should treat it as a
    /// content divergence, not a user stop.
    func testIsMetadataGateCancellation_finalVerificationFailed_nilUnderlying_isNotCancellation() {
        let gateError = MetadataCreateGate.Error.finalVerificationFailed(remotePath: "/x", underlying: nil)
        XCTAssertFalse(CommitLogWriter.isMetadataGateCancellation(gateError))
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

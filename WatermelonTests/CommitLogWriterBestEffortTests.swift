import XCTest
@testable import Watermelon

final class CommitLogWriterBestEffortTests: XCTestCase {
    private let basePath = "/repo"
    private let month = LibraryMonthKey(year: 2026, month: 1)
    private let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

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
                assetFingerprint: TestFixtures.assetFingerprint(0xBB),  // different fp → different SHA
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
                assetFingerprint: TestFixtures.assetFingerprint(0xCC),
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

    func testExclusiveBackend_alreadyExistsWithMismatchedBytes_throwsAlreadyExists() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = CommitLogWriter(client: client, basePath: basePath)

        let peerOp = CommitOp(
            opSeq: 0, clock: 1,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.assetFingerprint(0xDD),
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


    func testRemoteWriteClassifierMetadataGateCancellation_stagingVerificationFailedWithURLErrorCancelled_isCancellation() {
        let underlying = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled, userInfo: nil)
        let gateError = MetadataCreateGate.Error.stagingVerificationFailed(remotePath: "/x", underlying: underlying)
        XCTAssertTrue(RemoteWriteClassifier.isMetadataGateCancellation(gateError))
    }

    func testRemoteWriteClassifierMetadataGateCancellation_finalVerificationFailedWithWrappedURLCancellation_isCancellation() {
        let url = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled, userInfo: nil)
        let wrapped = RemoteStorageClientError.underlying(url)
        let gateError = MetadataCreateGate.Error.finalVerificationFailed(remotePath: "/x", underlying: wrapped)
        XCTAssertTrue(RemoteWriteClassifier.isMetadataGateCancellation(gateError))
    }

    func testRemoteWriteClassifierMetadataGateCancellation_nonExclusiveFinalization_isNotCancellation() {
        let gateError = MetadataCreateGate.Error.nonExclusiveFinalization(remotePath: "/x")
        XCTAssertFalse(RemoteWriteClassifier.isMetadataGateCancellation(gateError))
    }

    func testRemoteWriteClassifierMetadataGateCancellation_stagingVerificationFailedWithPermissionDenied_isNotCancellation() {
        let underlying = NSError(domain: NSCocoaErrorDomain, code: NSFileReadNoPermissionError, userInfo: nil)
        let gateError = MetadataCreateGate.Error.stagingVerificationFailed(remotePath: "/x", underlying: underlying)
        XCTAssertFalse(RemoteWriteClassifier.isMetadataGateCancellation(gateError))
    }

    func testRemoteWriteClassifierMetadataGateCancellation_finalVerificationFailedNilUnderlying_isNotCancellation() {
        let gateError = MetadataCreateGate.Error.finalVerificationFailed(remotePath: "/x", underlying: nil)
        XCTAssertFalse(RemoteWriteClassifier.isMetadataGateCancellation(gateError))
    }

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
                assetFingerprint: TestFixtures.assetFingerprint(0xAA),
                reason: .userDeleted
            ))
        )
    }
}

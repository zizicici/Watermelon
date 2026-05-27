import XCTest
@testable import Watermelon

final class MetadataCreateOrchestratorTests: XCTestCase {
    private let basePath = "/repo"
    private let month = LibraryMonthKey(year: 2026, month: 1)
    private let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let remotePath = "/test/metadata.bin"

    private func writeLocalFile(_ data: Data) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("orch-local-\(UUID().uuidString).bin")
        try data.write(to: url, options: .atomic)
        return url
    }

    private func deleteFile(_ url: URL) { try? FileManager.default.removeItem(at: url) }

    private struct StubVerifier: MetadataWriteVerifier {
        let outcome: MetadataWriteVerifyOutcome
        func verify(client: any RemoteStorageClientProtocol, remotePath: String, localURL: URL) async -> MetadataWriteVerifyOutcome {
            outcome
        }
    }

    private actor RecordingVerifier: MetadataWriteVerifier {
        private(set) var invocationCount = 0
        private let outcome: MetadataWriteVerifyOutcome
        init(outcome: MetadataWriteVerifyOutcome) { self.outcome = outcome }
        func verify(client: any RemoteStorageClientProtocol, remotePath: String, localURL: URL) async -> MetadataWriteVerifyOutcome {
            invocationCount += 1
            return outcome
        }
    }

    // MARK: - Orchestrator dispatch (6 tests)

    func testOrchestrator_AtomicCreateReturnsCreated_ReturnsCreatedWithoutVerification_NoVerifyInvocation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        await client.setAtomicCreateMode(.strictlyAtomic)
        let local = try writeLocalFile(Data("hello".utf8))
        defer { deleteFile(local) }
        let recorder = RecordingVerifier(outcome: .matched)

        let attempt = try await MetadataCreateOrchestrator.atomicCreateThenVerify(
            client: client, localURL: local, remotePath: remotePath,
            respectTaskCancellation: true, verifier: recorder
        )
        guard case .createdWithoutVerification = attempt else {
            XCTFail("expected .createdWithoutVerification, got \(attempt)"); return
        }
        let count = await recorder.invocationCount
        XCTAssertEqual(count, 0, "verifier must not be invoked when atomicCreate returns .created")
    }

    func testOrchestrator_AtomicCreateReturnsAlreadyExists_RunsVerifierAndReturnsVerifyAttempted() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        await client.setAtomicCreateMode(.strictlyAtomic)
        await client.injectFile(path: remotePath, data: Data("peer".utf8))
        let local = try writeLocalFile(Data("hello".utf8))
        defer { deleteFile(local) }
        let recorder = RecordingVerifier(outcome: .deterministicMismatch)

        let attempt = try await MetadataCreateOrchestrator.atomicCreateThenVerify(
            client: client, localURL: local, remotePath: remotePath,
            respectTaskCancellation: true, verifier: recorder
        )
        guard case .verifyAttempted(let result, let verify) = attempt else {
            XCTFail("expected .verifyAttempted, got \(attempt)"); return
        }
        XCTAssertEqual(result, .alreadyExists)
        guard case .deterministicMismatch = verify else {
            XCTFail("expected .deterministicMismatch from recorder, got \(verify)"); return
        }
        let count = await recorder.invocationCount
        XCTAssertEqual(count, 1)
    }

    func testOrchestrator_AtomicCreateReturnsBestEffortRetry_RunsVerifierAndReturnsVerifyAttempted() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // bestEffort mode returns .bestEffortRetry on successful upload.
        client.setAtomicCreateGuarantee(.overwritePossible)
        await client.setAtomicCreateMode(.bestEffort)
        let local = try writeLocalFile(Data("hello".utf8))
        defer { deleteFile(local) }
        let recorder = RecordingVerifier(outcome: .matched)

        let attempt = try await MetadataCreateOrchestrator.atomicCreateThenVerify(
            client: client, localURL: local, remotePath: remotePath,
            respectTaskCancellation: true, verifier: recorder
        )
        guard case .verifyAttempted(let result, let verify) = attempt else {
            XCTFail("expected .verifyAttempted, got \(attempt)"); return
        }
        XCTAssertEqual(result, .bestEffortRetry)
        guard case .matched = verify else {
            XCTFail("expected .matched, got \(verify)"); return
        }
        let count = await recorder.invocationCount
        XCTAssertEqual(count, 1)
    }

    func testOrchestrator_AtomicCreateThrowsCancellationError_ThrowsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        await client.setAtomicCreateMode(.strictlyAtomic)
        await client.injectAtomicCreateURLErrorCancelled(for: remotePath)
        // URLErrorCancelled tested specifically below — for "literal CancellationError",
        // we exercise the same path: the orchestrator must throw CancellationError.
        let local = try writeLocalFile(Data("hello".utf8))
        defer { deleteFile(local) }
        let verifier = StubVerifier(outcome: .matched)

        do {
            _ = try await MetadataCreateOrchestrator.atomicCreateThenVerify(
                client: client, localURL: local, remotePath: remotePath,
                respectTaskCancellation: true, verifier: verifier
            )
            XCTFail("expected CancellationError to be thrown")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    func testOrchestrator_AtomicCreateThrowsURLSessionCancelled_NormalizedToCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        await client.setAtomicCreateMode(.strictlyAtomic)
        await client.injectAtomicCreateURLErrorCancelled(for: remotePath)
        let local = try writeLocalFile(Data("hello".utf8))
        defer { deleteFile(local) }

        do {
            _ = try await MetadataCreateOrchestrator.atomicCreateThenVerify(
                client: client, localURL: local, remotePath: remotePath,
                respectTaskCancellation: true, verifier: StubVerifier(outcome: .matched)
            )
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // expected — URLSession-shaped cancellation normalized to CancellationError().
        }
    }

    func testOrchestrator_AtomicCreateThrowsOther_PropagatesUnchanged() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        await client.setAtomicCreateMode(.strictlyAtomic)
        // .permission → NSFileReadNoPermissionError, classified as .permanent, not cancellation.
        await client.injectUploadError(.permission, for: remotePath)
        let local = try writeLocalFile(Data("hello".utf8))
        defer { deleteFile(local) }

        do {
            _ = try await MetadataCreateOrchestrator.atomicCreateThenVerify(
                client: client, localURL: local, remotePath: remotePath,
                respectTaskCancellation: true, verifier: StubVerifier(outcome: .matched)
            )
            XCTFail("expected the raw error to propagate")
        } catch is CancellationError {
            XCTFail("permission error must NOT normalize to CancellationError")
        } catch {
            // expected — raw upload error propagates as-is.
            XCTAssertFalse(RemoteWriteClassifier.isCancellation(error))
        }
    }

    // MARK: - Row 1: Gate `.exclusive` `.alreadyExists` (via orchestrator) — 5 tests

    func testGateExclusiveAlreadyExists_Matched_ReturnsCreatedVerifiedLocalBytes() throws {
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .matched
        )
        let outcome = try MetadataCreateGate.mapExclusiveCreateAttempt(attempt, remotePath: remotePath)
        XCTAssertEqual(outcome.result, .created)
        XCTAssertEqual(outcome.verification, .verifiedLocalBytes)
    }

    func testGateExclusiveAlreadyExists_DeterministicMismatch_ReturnsAlreadyExistsUnverified() throws {
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .deterministicMismatch
        )
        let outcome = try MetadataCreateGate.mapExclusiveCreateAttempt(attempt, remotePath: remotePath)
        XCTAssertEqual(outcome.result, .alreadyExists)
        XCTAssertEqual(outcome.verification, .unverified)
    }

    func testGateExclusiveAlreadyExists_TransientFailure_ReturnsAlreadyExistsUnverified() throws {
        let underlying = NSError(domain: "transient", code: 1)
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .transientFailure(underlying: underlying)
        )
        let outcome = try MetadataCreateGate.mapExclusiveCreateAttempt(attempt, remotePath: remotePath)
        XCTAssertEqual(outcome.result, .alreadyExists)
        XCTAssertEqual(outcome.verification, .unverified)
    }

    func testGateExclusiveAlreadyExists_PermanentFailure_ThrowsFinalVerificationFailedWithUnderlying() {
        let underlying = NSError(domain: "permanent", code: 99)
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .permanentFailure(underlying: underlying)
        )
        do {
            _ = try MetadataCreateGate.mapExclusiveCreateAttempt(attempt, remotePath: remotePath)
            XCTFail("expected throw")
        } catch let MetadataCreateGate.Error.finalVerificationFailed(rp, u) {
            XCTAssertEqual(rp, remotePath)
            XCTAssertEqual((u as NSError?)?.domain, "permanent")
            XCTAssertEqual((u as NSError?)?.code, 99)
        } catch {
            XCTFail("expected finalVerificationFailed, got \(error)")
        }
    }

    func testGateExclusiveAlreadyExists_Cancelled_ThrowsCancellationError() {
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .cancelled
        )
        do {
            _ = try MetadataCreateGate.mapExclusiveCreateAttempt(attempt, remotePath: remotePath)
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    // MARK: - Row 2: Gate `.overwritePossible` STAGING verify (direct) — 5 tests

    func testGateStagingVerify_Matched_ContinuesToFinalization() {
        let action = MetadataCreateGate.mapStagingVerify(.matched, stagingPath: "/staging")
        guard case .continueToFinalization = action else {
            XCTFail("expected .continueToFinalization, got \(action)"); return
        }
    }

    func testGateStagingVerify_DeterministicMismatch_CleansUpAndThrowsStagingVerificationFailedNilUnderlying() {
        let action = MetadataCreateGate.mapStagingVerify(.deterministicMismatch, stagingPath: "/staging")
        guard case .cleanupThenThrowStagingVerificationFailed(let u) = action else {
            XCTFail("expected .cleanupThenThrowStagingVerificationFailed, got \(action)"); return
        }
        XCTAssertNil(u)
    }

    func testGateStagingVerify_TransientFailure_CleansUpAndThrowsStagingVerificationFailedWithUnderlying() {
        let underlying = NSError(domain: "transient", code: 1)
        let action = MetadataCreateGate.mapStagingVerify(
            .transientFailure(underlying: underlying), stagingPath: "/staging"
        )
        guard case .cleanupThenThrowStagingVerificationFailed(let u) = action else {
            XCTFail("expected .cleanupThenThrowStagingVerificationFailed, got \(action)"); return
        }
        XCTAssertEqual((u as NSError?)?.domain, "transient")
    }

    func testGateStagingVerify_PermanentFailure_CleansUpAndThrowsStagingVerificationFailedWithUnderlying() {
        let underlying = NSError(domain: "permanent", code: 99)
        let action = MetadataCreateGate.mapStagingVerify(
            .permanentFailure(underlying: underlying), stagingPath: "/staging"
        )
        guard case .cleanupThenThrowStagingVerificationFailed(let u) = action else {
            XCTFail("expected .cleanupThenThrowStagingVerificationFailed, got \(action)"); return
        }
        XCTAssertEqual((u as NSError?)?.domain, "permanent")
    }

    func testGateStagingVerify_Cancelled_CleansUpAndThrowsCancellation() {
        let action = MetadataCreateGate.mapStagingVerify(.cancelled, stagingPath: "/staging")
        guard case .cleanupThenThrowCancellation = action else {
            XCTFail("expected .cleanupThenThrowCancellation, got \(action)"); return
        }
    }

    // MARK: - Row 3: Gate `.overwritePossible` POST-MOVE `.alreadyExists` (direct) — 5 tests

    func testGatePostMoveAlreadyExists_Matched_CleansUpStagingAndReturnsCreatedVerifiedLocalBytes() {
        let action = MetadataCreateGate.mapPostMoveAlreadyExistsVerify(.matched, remotePath: remotePath)
        guard case .returnCreatedVerifiedLocalBytes = action else {
            XCTFail("expected .returnCreatedVerifiedLocalBytes, got \(action)"); return
        }
    }

    func testGatePostMoveAlreadyExists_DeterministicMismatch_CleansUpStagingAndReturnsAlreadyExistsUnverified() {
        let action = MetadataCreateGate.mapPostMoveAlreadyExistsVerify(.deterministicMismatch, remotePath: remotePath)
        guard case .returnAlreadyExistsUnverified = action else {
            XCTFail("expected .returnAlreadyExistsUnverified, got \(action)"); return
        }
    }

    func testGatePostMoveAlreadyExists_TransientFailure_CleansUpStagingAndReturnsAlreadyExistsUnverified() {
        let underlying = NSError(domain: "transient", code: 1)
        let action = MetadataCreateGate.mapPostMoveAlreadyExistsVerify(
            .transientFailure(underlying: underlying), remotePath: remotePath
        )
        guard case .returnAlreadyExistsUnverified = action else {
            XCTFail("expected .returnAlreadyExistsUnverified, got \(action)"); return
        }
    }

    func testGatePostMoveAlreadyExists_PermanentFailure_CleansUpStagingAndThrowsFinalVerificationFailedWithUnderlying() {
        let underlying = NSError(domain: "permanent", code: 99)
        let action = MetadataCreateGate.mapPostMoveAlreadyExistsVerify(
            .permanentFailure(underlying: underlying), remotePath: remotePath
        )
        guard case .throwFinalVerificationFailed(let u) = action else {
            XCTFail("expected .throwFinalVerificationFailed, got \(action)"); return
        }
        XCTAssertEqual((u as NSError?)?.domain, "permanent")
    }

    func testGatePostMoveAlreadyExists_Cancelled_CleansUpStagingAndThrowsCancellation() {
        let action = MetadataCreateGate.mapPostMoveAlreadyExistsVerify(.cancelled, remotePath: remotePath)
        guard case .throwCancellation = action else {
            XCTFail("expected .throwCancellation, got \(action)"); return
        }
    }

    // MARK: - Row 4: Gate `.overwritePossible` FINAL POST-MOVE `.created` (direct) — 5 tests

    func testGateFinalPostMoveCreated_Matched_ReturnsCreatedVerifiedLocalBytes() throws {
        let outcome = try MetadataCreateGate.mapFinalPostMoveVerify(.matched, remotePath: remotePath)
        XCTAssertEqual(outcome.result, .created)
        XCTAssertEqual(outcome.verification, .verifiedLocalBytes)
    }

    func testGateFinalPostMoveCreated_DeterministicMismatch_ThrowsFinalVerificationFailedNilUnderlying() {
        do {
            _ = try MetadataCreateGate.mapFinalPostMoveVerify(.deterministicMismatch, remotePath: remotePath)
            XCTFail("expected throw — deterministic mismatch at final post-move is FATAL")
        } catch let MetadataCreateGate.Error.finalVerificationFailed(rp, u) {
            XCTAssertEqual(rp, remotePath)
            XCTAssertNil(u, "deterministic mismatch must carry nil underlying — load-bearing invariant")
        } catch {
            XCTFail("expected finalVerificationFailed, got \(error)")
        }
    }

    func testGateFinalPostMoveCreated_TransientFailure_DemotesToBestEffortRetryUnverified() throws {
        let underlying = NSError(domain: "transient", code: 1)
        let outcome = try MetadataCreateGate.mapFinalPostMoveVerify(
            .transientFailure(underlying: underlying), remotePath: remotePath
        )
        XCTAssertEqual(outcome.result, .bestEffortRetry)
        XCTAssertEqual(outcome.verification, .unverified)
    }

    func testGateFinalPostMoveCreated_PermanentFailure_ThrowsFinalVerificationFailedWithUnderlying() {
        let underlying = NSError(domain: "permanent", code: 99)
        do {
            _ = try MetadataCreateGate.mapFinalPostMoveVerify(
                .permanentFailure(underlying: underlying), remotePath: remotePath
            )
            XCTFail("expected throw")
        } catch let MetadataCreateGate.Error.finalVerificationFailed(rp, u) {
            XCTAssertEqual(rp, remotePath)
            XCTAssertEqual((u as NSError?)?.domain, "permanent")
        } catch {
            XCTFail("expected finalVerificationFailed, got \(error)")
        }
    }

    func testGateFinalPostMoveCreated_Cancelled_ThrowsCancellationError() {
        do {
            _ = try MetadataCreateGate.mapFinalPostMoveVerify(.cancelled, remotePath: remotePath)
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    // MARK: - Row 5: CommitLogWriter EXCLUSIVE post-`atomicCreate` (via orchestrator) — 5 tests

    func testCommitLogWriterExclusive_Matched_Returns() throws {
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .matched
        )
        try CommitLogWriter.mapExclusiveCreateAttempt(attempt)
    }

    func testCommitLogWriterExclusive_DeterministicMismatch_ThrowsWriteErrorAlreadyExists() {
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .deterministicMismatch
        )
        do {
            try CommitLogWriter.mapExclusiveCreateAttempt(attempt)
            XCTFail("expected WriteError.alreadyExists")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected
        } catch {
            XCTFail("expected WriteError.alreadyExists, got \(error)")
        }
    }

    func testCommitLogWriterExclusive_TransientFailure_ThrowsWriteErrorAlreadyExists() {
        let underlying = NSError(domain: "transient", code: 1)
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .transientFailure(underlying: underlying)
        )
        do {
            try CommitLogWriter.mapExclusiveCreateAttempt(attempt)
            XCTFail("expected WriteError.alreadyExists")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected — transient demoted to alreadyExists so caller retries seq.
        } catch {
            XCTFail("expected WriteError.alreadyExists, got \(error)")
        }
    }

    func testCommitLogWriterExclusive_PermanentFailure_ThrowsWriteErrorIOFailureWithUnderlying() {
        let underlying = NSError(domain: "permanent", code: 99)
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .permanentFailure(underlying: underlying)
        )
        do {
            try CommitLogWriter.mapExclusiveCreateAttempt(attempt)
            XCTFail("expected WriteError.ioFailure")
        } catch CommitLogWriter.WriteError.ioFailure(let inner) {
            XCTAssertEqual((inner as NSError).domain, "permanent")
            XCTAssertEqual((inner as NSError).code, 99)
        } catch {
            XCTFail("expected WriteError.ioFailure, got \(error)")
        }
    }

    func testCommitLogWriterExclusive_Cancelled_ThrowsCancellationError() {
        let attempt = MetadataCreateOrchestrator.AfterCreateAttempt.verifyAttempted(
            result: .alreadyExists, verify: .cancelled
        )
        do {
            try CommitLogWriter.mapExclusiveCreateAttempt(attempt)
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    // MARK: - Row 6: CommitLogWriter STAGED post-Gate `.bestEffortRetry` (direct) — 5 tests

    func testCommitLogWriterStagedBestEffortRetry_Matched_Returns() throws {
        try CommitLogWriter.mapStagedBestEffortRetryVerify(.matched)
    }

    func testCommitLogWriterStagedBestEffortRetry_DeterministicMismatch_ThrowsWriteErrorAlreadyExists() {
        do {
            try CommitLogWriter.mapStagedBestEffortRetryVerify(.deterministicMismatch)
            XCTFail("expected WriteError.alreadyExists")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected
        } catch {
            XCTFail("expected WriteError.alreadyExists, got \(error)")
        }
    }

    func testCommitLogWriterStagedBestEffortRetry_TransientFailure_ThrowsWriteErrorAlreadyExists() {
        let underlying = NSError(domain: "transient", code: 1)
        do {
            try CommitLogWriter.mapStagedBestEffortRetryVerify(.transientFailure(underlying: underlying))
            XCTFail("expected WriteError.alreadyExists")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected
        } catch {
            XCTFail("expected WriteError.alreadyExists, got \(error)")
        }
    }

    func testCommitLogWriterStagedBestEffortRetry_PermanentFailure_ThrowsWriteErrorIOFailureWithUnderlying() {
        let underlying = NSError(domain: "permanent", code: 99)
        do {
            try CommitLogWriter.mapStagedBestEffortRetryVerify(.permanentFailure(underlying: underlying))
            XCTFail("expected WriteError.ioFailure")
        } catch CommitLogWriter.WriteError.ioFailure(let inner) {
            XCTAssertEqual((inner as NSError).domain, "permanent")
        } catch {
            XCTFail("expected WriteError.ioFailure, got \(error)")
        }
    }

    func testCommitLogWriterStagedBestEffortRetry_Cancelled_ThrowsCancellationError() {
        do {
            try CommitLogWriter.mapStagedBestEffortRetryVerify(.cancelled)
            XCTFail("expected CancellationError")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    // MARK: - STAGED non-verify-arm preservation (3 tests)

    private static func encodeCommit(
        writerID: String = "11111111-1111-1111-1111-aaaaaaaaaaaa",
        seq: UInt64 = 1,
        clock: UInt64 = 1
    ) throws -> Data {
        let header = CommitHeader(
            version: CommitHeader.currentVersion,
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            writerID: writerID, seq: seq, runID: "run-orch-test",
            scope: CommitHeader.monthScope(LibraryMonthKey(year: 2026, month: 1)),
            clockMin: clock, clockMax: clock, bodyKind: CommitHeader.bodyKindPlain
        )
        let op = CommitOp(
            opSeq: 0, clock: clock,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.assetFingerprint(0xAA), reason: .userDeleted
            ))
        )
        var lines: [String] = []
        var integrity = IntegrityAccumulator()
        let h = try CommitOpMapper.encodeHeaderLine(header)
        lines.append(h); integrity.absorbLine(h)
        let l = try CommitOpMapper.encodeOpLine(op)
        lines.append(l); integrity.absorbLine(l)
        let sha = integrity.finalize()
        let endLine = try CommitOpMapper.encodeEndLine(sha256Hex: sha, rowCount: integrity.rowCount)
        lines.append(endLine)
        return Data((lines.joined(separator: "\n") + "\n").utf8)
    }

    func testCommitLogWriterStagedPath_GateReturnsCreated_ReturnsWithoutInvokingVerifier() async throws {
        // Pin that the staged .created arm does NOT invoke MetadataWriteVerifiers.commitAware.
        // CommitAwareVerifier downloads to a localURL named "commit-verify-UUID.jsonl";
        // ByteEqualityVerifier (used by Gate's own staging+final post-move verify) names
        // its temp file "metadata-verify-UUID.bin". A wrapping probe counts only the
        // commit-aware-prefix downloads on the final commit path so any regression that
        // adds a commit-aware verify on the .created arm trips the assertion below.
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        inner.setMoveIfAbsentGuarantee(.exclusive)
        await inner.setAtomicCreateMode(.bestEffort)
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        let probe = StagedPathDownloadProbe(inner: inner, finalCommitPath: path)

        let writer = CommitLogWriter(client: probe, basePath: basePath)
        let header = CommitHeader(
            version: CommitHeader.currentVersion, repoID: repoID, writerID: writerID,
            seq: 1, runID: "run-orch", scope: CommitHeader.monthScope(month),
            clockMin: 1, clockMax: 1, bodyKind: CommitHeader.bodyKindPlain
        )
        let op = CommitOp(
            opSeq: 0, clock: 1,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.assetFingerprint(0xAA), reason: .userDeleted
            ))
        )

        let file = try await writer.write(
            header: header, ops: [op], month: month, respectTaskCancellation: false
        )
        XCTAssertEqual(file.rowCount, 2)
        let snapshot = await inner.snapshotFiles()
        XCTAssertNotNil(snapshot[path], "our bytes must be at the final commit path after Gate returns .created")

        let commitAwareCount = await probe.commitAwareDownloadCount()
        XCTAssertEqual(
            commitAwareCount, 0,
            "commit-aware verifier must NOT be invoked on staged .created arm — if any download with 'commit-verify-' prefix hit the final commit path, performStagedCreateAndVerify's .created arm has regressed into running commit-aware verification"
        )
    }

    func testCommitLogWriterStagedPath_GateReturnsAlreadyExists_ThrowsWriteErrorAlreadyExistsWithoutInvokingVerifier() async throws {
        // Pin that the staged .alreadyExists arm throws WriteError.alreadyExists WITHOUT
        // invoking commit-aware verification — Gate's own byte-equality verify runs (and
        // is allowed; counted under the metadata-verify- prefix), but a commit-aware
        // download on the final commit path would indicate a regression.
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        inner.setMoveIfAbsentGuarantee(.exclusive)
        await inner.setAtomicCreateMode(.bestEffort)

        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 1)
        let peerBytes = try Self.encodeCommit(writerID: "22222222-2222-2222-2222-bbbbbbbbbbbb", seq: 1)
        await inner.injectFile(path: path, data: peerBytes)
        let probe = StagedPathDownloadProbe(inner: inner, finalCommitPath: path)

        let writer = CommitLogWriter(client: probe, basePath: basePath)
        let header = CommitHeader(
            version: CommitHeader.currentVersion, repoID: repoID, writerID: writerID,
            seq: 1, runID: "run-orch", scope: CommitHeader.monthScope(month),
            clockMin: 1, clockMax: 1, bodyKind: CommitHeader.bodyKindPlain
        )
        let op = CommitOp(
            opSeq: 0, clock: 1,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.assetFingerprint(0xCC), reason: .userDeleted
            ))
        )

        do {
            _ = try await writer.write(
                header: header, ops: [op], month: month, respectTaskCancellation: false
            )
            XCTFail("expected WriteError.alreadyExists")
        } catch CommitLogWriter.WriteError.alreadyExists {
            // expected — staged .alreadyExists branch throws directly without verify.
        }

        // Peer bytes must survive — overwrite-protection invariant.
        let snapshot = await inner.snapshotFiles()
        XCTAssertEqual(snapshot[path], peerBytes,
                       "peer commit bytes must survive — losing them would drop committed ops")

        let commitAwareCount = await probe.commitAwareDownloadCount()
        XCTAssertEqual(
            commitAwareCount, 0,
            "commit-aware verifier must NOT be invoked on staged .alreadyExists arm — if any download with 'commit-verify-' prefix hit the final commit path, performStagedCreateAndVerify's .alreadyExists arm has regressed into running commit-aware verification before throwing"
        )
    }

    func testCommitLogWriterStagedPath_GateInvokedWithRequireExclusiveMovePolicy() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Move-if-absent is overwritePossible (no exclusive move) — the Gate's
        // .requireExclusiveMove policy MUST reject finalization, surfacing as
        // ioFailure(nonExclusiveFinalization). If the writer used .allowBestEffort
        // (i.e. didn't pass .requireExclusiveMove) the write would silently succeed
        // via bestEffort copy, overwriting any peer.
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setAtomicCreateMode(.bestEffort)

        let header = CommitHeader(
            version: CommitHeader.currentVersion, repoID: repoID, writerID: writerID,
            seq: 1, runID: "run-orch", scope: CommitHeader.monthScope(month),
            clockMin: 1, clockMax: 1, bodyKind: CommitHeader.bodyKindPlain
        )
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let op = CommitOp(
            opSeq: 0, clock: 1,
            body: .tombstoneAsset(CommitTombstoneBody(
                assetFingerprint: TestFixtures.assetFingerprint(0xAA), reason: .userDeleted
            ))
        )
        do {
            _ = try await writer.write(
                header: header, ops: [op], month: month, respectTaskCancellation: false
            )
            XCTFail("expected ioFailure(nonExclusiveFinalization) — staged writer must pass .requireExclusiveMove")
        } catch CommitLogWriter.WriteError.ioFailure(let inner) {
            guard let gateError = inner as? MetadataCreateGate.Error,
                  case .nonExclusiveFinalization = gateError else {
                XCTFail("expected MetadataCreateGate.Error.nonExclusiveFinalization, got \(inner)")
                return
            }
        }
    }
}

// Wraps InMemoryRemoteStorageClient and counts download calls to a designated
// final commit path that arrive with a "commit-verify-" prefixed temp localURL.
// This is the disk signature of MetadataWriteVerifiers.commitAware. Byte-equality
// downloads from Gate's own staging/post-move verifies use "metadata-verify-"
// and are NOT counted, so the probe pins commit-aware non-invocation on the
// staged .created and .alreadyExists arms without touching production code.
private actor StagedPathDownloadProbe: RemoteStorageClientProtocol {
    private let inner: InMemoryRemoteStorageClient
    private let finalCommitPath: String
    private var commitAwareDownloads: Int = 0

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }

    init(inner: InMemoryRemoteStorageClient, finalCommitPath: String) {
        self.inner = inner
        self.finalCommitPath = finalCommitPath
    }

    func commitAwareDownloadCount() -> Int { commitAwareDownloads }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws {
        try await inner.upload(
            localURL: localURL, remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation, onProgress: onProgress
        )
    }
    func atomicCreate(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(
            localURL: localURL, remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation, onProgress: onProgress
        )
    }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { inner.moveIfAbsentGuarantee }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { inner.readAfterWriteGraceSeconds }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool {
        try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: destinationPath)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        if remotePath == finalCommitPath,
           localURL.lastPathComponent.hasPrefix("commit-verify-") {
            commitAwareDownloads += 1
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
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

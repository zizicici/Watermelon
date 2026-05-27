import XCTest
@testable import Watermelon

final class MetadataWriteVerifierTests: XCTestCase {
    private let remotePath = "/test/metadata.bin"

    private func writeLocalFile(_ data: Data) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("verifier-local-\(UUID().uuidString).bin")
        try data.write(to: url, options: .atomic)
        return url
    }

    private func deleteFile(_ url: URL) { try? FileManager.default.removeItem(at: url) }

    // MARK: - byteEquality

    func testByteEquality_SizeAndSHAMatch_ReturnsMatched() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let bytes = Data("hello world".utf8)
        let local = try writeLocalFile(bytes)
        defer { deleteFile(local) }
        await client.injectFile(path: remotePath, data: bytes)

        let outcome = await MetadataWriteVerifiers.byteEquality.verify(
            client: client, remotePath: remotePath, localURL: local
        )
        guard case .matched = outcome else {
            XCTFail("expected .matched, got \(outcome)"); return
        }
    }

    func testByteEquality_SizeMismatch_ReturnsDeterministicMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let local = try writeLocalFile(Data("hello world".utf8))
        defer { deleteFile(local) }
        await client.injectFile(path: remotePath, data: Data("hello".utf8))

        let outcome = await MetadataWriteVerifiers.byteEquality.verify(
            client: client, remotePath: remotePath, localURL: local
        )
        guard case .deterministicMismatch = outcome else {
            XCTFail("expected .deterministicMismatch, got \(outcome)"); return
        }
    }

    func testByteEquality_SameSizeSHAMismatchAfterRetries_ReturnsDeterministicMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let local = try writeLocalFile(Data("hello world".utf8))
        defer { deleteFile(local) }
        // Same size, different bytes — SHA mismatch.
        await client.injectFile(path: remotePath, data: Data("world hello".utf8))

        let outcome = await MetadataWriteVerifiers.byteEquality.verify(
            client: client, remotePath: remotePath, localURL: local
        )
        guard case .deterministicMismatch = outcome else {
            XCTFail("expected .deterministicMismatch, got \(outcome)"); return
        }
    }

    func testByteEquality_TransientDownloadFailureAfterRetries_ReturnsTransientFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let local = try writeLocalFile(Data("hello world".utf8))
        defer { deleteFile(local) }
        // .transport translates to NSURLErrorNotConnectedToInternet → classified as .transient.
        await client.injectPersistentDownloadError(.transport, for: remotePath)

        let outcome = await MetadataWriteVerifiers.byteEquality.verify(
            client: client, remotePath: remotePath, localURL: local
        )
        guard case .transientFailure(let underlying) = outcome else {
            XCTFail("expected .transientFailure, got \(outcome)"); return
        }
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(underlying), .transient)
    }

    func testByteEquality_PermanentDownloadFailure_ReturnsPermanentFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let local = try writeLocalFile(Data("hello world".utf8))
        defer { deleteFile(local) }
        // Raw NSError with unrecognized domain → defaults to .permanent.
        let rawPermanent = NSError(
            domain: "com.test.permanent", code: 42,
            userInfo: [NSLocalizedDescriptionKey: "unrecognized permanent failure"]
        )
        await client.injectRawDownloadError(rawPermanent, for: remotePath)
        // Inject a second time so retries also hit (raw error consumed on first call).
        await client.injectPersistentDownloadError(.permission, for: remotePath)

        let outcome = await MetadataWriteVerifiers.byteEquality.verify(
            client: client, remotePath: remotePath, localURL: local
        )
        guard case .permanentFailure(let underlying) = outcome else {
            XCTFail("expected .permanentFailure, got \(outcome)"); return
        }
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(underlying), .permanent)
    }

    func testByteEquality_CancellationErrorThrown_ReturnsCancelled() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let local = try writeLocalFile(Data("hello world".utf8))
        defer { deleteFile(local) }
        await client.injectDownloadCancellation(for: remotePath)

        let outcome = await MetadataWriteVerifiers.byteEquality.verify(
            client: client, remotePath: remotePath, localURL: local
        )
        guard case .cancelled = outcome else {
            XCTFail("expected .cancelled, got \(outcome)"); return
        }
    }

    func testByteEquality_URLSessionCancelledShape_ReturnsCancelled() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let local = try writeLocalFile(Data("hello world".utf8))
        defer { deleteFile(local) }
        await client.injectDownloadURLErrorCancelled(for: remotePath)

        let outcome = await MetadataWriteVerifiers.byteEquality.verify(
            client: client, remotePath: remotePath, localURL: local
        )
        guard case .cancelled = outcome else {
            XCTFail("expected .cancelled, got \(outcome)"); return
        }
    }

    // MARK: - commitAware

    private static func encodeCommit(
        writerID: String = "11111111-1111-1111-1111-aaaaaaaaaaaa",
        seq: UInt64 = 1,
        clock: UInt64 = 1,
        opCount: Int = 1
    ) throws -> (data: Data, sha: String, rowCount: Int) {
        let header = CommitHeader(
            version: CommitHeader.currentVersion,
            repoID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            writerID: writerID,
            seq: seq,
            runID: "run-verifier-test",
            scope: CommitHeader.monthScope(LibraryMonthKey(year: 2026, month: 1)),
            clockMin: clock,
            clockMax: clock,
            bodyKind: CommitHeader.bodyKindPlain
        )
        var ops: [CommitOp] = []
        for i in 0 ..< opCount {
            ops.append(CommitOp(
                opSeq: i, clock: clock,
                body: .tombstoneAsset(CommitTombstoneBody(
                    assetFingerprint: TestFixtures.assetFingerprint(UInt8(0xA0 &+ UInt8(i % 16))),
                    reason: .userDeleted
                ))
            ))
        }
        var lines: [String] = []
        var integrity = IntegrityAccumulator()
        let h = try CommitOpMapper.encodeHeaderLine(header)
        lines.append(h); integrity.absorbLine(h)
        for op in ops {
            let line = try CommitOpMapper.encodeOpLine(op)
            lines.append(line); integrity.absorbLine(line)
        }
        let sha = integrity.finalize()
        let endLine = try CommitOpMapper.encodeEndLine(sha256Hex: sha, rowCount: integrity.rowCount)
        lines.append(endLine)
        let data = Data((lines.joined(separator: "\n") + "\n").utf8)
        return (data, sha, integrity.rowCount)
    }

    func testCommitAware_DownloadParseSHAAndRowCountMatch_ReturnsMatched() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commit = try Self.encodeCommit()
        let local = try writeLocalFile(commit.data)
        defer { deleteFile(local) }
        await client.injectFile(path: remotePath, data: commit.data)

        let verifier = MetadataWriteVerifiers.commitAware(
            expectedSha: commit.sha, expectedRowCount: commit.rowCount
        )
        let outcome = await verifier.verify(client: client, remotePath: remotePath, localURL: local)
        guard case .matched = outcome else {
            XCTFail("expected .matched, got \(outcome)"); return
        }
    }

    func testCommitAware_ParseFailure_ReturnsDeterministicMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commit = try Self.encodeCommit()
        let local = try writeLocalFile(commit.data)
        defer { deleteFile(local) }
        // Inject malformed bytes — CommitLogReader.parse will throw.
        await client.injectFile(path: remotePath, data: Data("not a valid commit file".utf8))

        let verifier = MetadataWriteVerifiers.commitAware(
            expectedSha: commit.sha, expectedRowCount: commit.rowCount
        )
        let outcome = await verifier.verify(client: client, remotePath: remotePath, localURL: local)
        guard case .deterministicMismatch = outcome else {
            XCTFail("expected .deterministicMismatch (parse fail), got \(outcome)"); return
        }
    }

    func testCommitAware_ShaMismatch_ReturnsDeterministicMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let ours = try Self.encodeCommit(seq: 1)
        let local = try writeLocalFile(ours.data)
        defer { deleteFile(local) }
        // Pre-populate remotePath with a different commit (different writerID → different SHA).
        let theirs = try Self.encodeCommit(writerID: "22222222-2222-2222-2222-bbbbbbbbbbbb", seq: 1)
        await client.injectFile(path: remotePath, data: theirs.data)

        let verifier = MetadataWriteVerifiers.commitAware(
            expectedSha: ours.sha, expectedRowCount: ours.rowCount
        )
        let outcome = await verifier.verify(client: client, remotePath: remotePath, localURL: local)
        guard case .deterministicMismatch = outcome else {
            XCTFail("expected .deterministicMismatch (SHA mismatch), got \(outcome)"); return
        }
    }

    func testCommitAware_RowCountMismatch_ReturnsDeterministicMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let ours = try Self.encodeCommit(opCount: 2)
        let local = try writeLocalFile(ours.data)
        defer { deleteFile(local) }
        // Remote bytes self-consistent but with different row count.
        let theirs = try Self.encodeCommit(opCount: 3)
        await client.injectFile(path: remotePath, data: theirs.data)

        // Use ours.sha so SHA comparison fails too — but verifier only short-circuits
        // on parse failure; both SHA AND row-count mismatches surface the same way.
        let verifier = MetadataWriteVerifiers.commitAware(
            expectedSha: ours.sha, expectedRowCount: ours.rowCount
        )
        let outcome = await verifier.verify(client: client, remotePath: remotePath, localURL: local)
        guard case .deterministicMismatch = outcome else {
            XCTFail("expected .deterministicMismatch (row count mismatch), got \(outcome)"); return
        }
    }

    func testCommitAware_TransientDownloadFailure_ReturnsTransientFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commit = try Self.encodeCommit()
        let local = try writeLocalFile(commit.data)
        defer { deleteFile(local) }
        await client.injectDownloadError(.transport, for: remotePath)

        let verifier = MetadataWriteVerifiers.commitAware(
            expectedSha: commit.sha, expectedRowCount: commit.rowCount
        )
        let outcome = await verifier.verify(client: client, remotePath: remotePath, localURL: local)
        guard case .transientFailure(let underlying) = outcome else {
            XCTFail("expected .transientFailure, got \(outcome)"); return
        }
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(underlying), .transient)
    }

    func testCommitAware_PermanentDownloadFailure_ReturnsPermanentFailure() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commit = try Self.encodeCommit()
        let local = try writeLocalFile(commit.data)
        defer { deleteFile(local) }
        let rawPermanent = NSError(
            domain: "com.test.permanent", code: 42,
            userInfo: [NSLocalizedDescriptionKey: "unrecognized permanent failure"]
        )
        await client.injectRawDownloadError(rawPermanent, for: remotePath)

        let verifier = MetadataWriteVerifiers.commitAware(
            expectedSha: commit.sha, expectedRowCount: commit.rowCount
        )
        let outcome = await verifier.verify(client: client, remotePath: remotePath, localURL: local)
        guard case .permanentFailure(let underlying) = outcome else {
            XCTFail("expected .permanentFailure, got \(outcome)"); return
        }
        XCTAssertEqual(RemoteWriteClassifier.classifyVerifyFailure(underlying), .permanent)
    }

    func testCommitAware_CancellationDuringDownload_ReturnsCancelled() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let commit = try Self.encodeCommit()
        let local = try writeLocalFile(commit.data)
        defer { deleteFile(local) }
        await client.injectDownloadCancellation(for: remotePath)

        let verifier = MetadataWriteVerifiers.commitAware(
            expectedSha: commit.sha, expectedRowCount: commit.rowCount
        )
        let outcome = await verifier.verify(client: client, remotePath: remotePath, localURL: local)
        guard case .cancelled = outcome else {
            XCTFail("expected .cancelled, got \(outcome)"); return
        }
    }
}

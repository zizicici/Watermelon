import XCTest
@testable import Watermelon

/// SnapshotWriter cancellation contract — direct callers (V1MigrationService,
/// V2MonthSnapshotFlusher) classify CancellationError as user-stop and a
/// wrapped finalization failure as a generic migration/snapshot bug. These
/// tests pin the writer's gate-boundary catch so cancellation arriving via
/// the metadata gate keeps its identity end-to-end.
final class SnapshotWriterCancellationTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let month = LibraryMonthKey(year: 2026, month: 4)
    private let runID = "snap-cancel-run"

    /// `.exclusive` backend, primary atomicCreate surfaces URLSession-shape
    /// cancellation (S3). Without the gate's atomicCreate-boundary normalization
    /// and the writer's `catch is CancellationError` arm, the raw NSError reaches
    /// the writer's generic catch and is wrapped as `.finalizationFailed` —
    /// V1MigrationService then misreports user-stop as a generic migration failure.
    func testExclusiveBackend_atomicCreateURLErrorCancelled_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = SnapshotWriter(client: client, basePath: basePath)

        let lamport: UInt64 = 1
        let path = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: lamport,
            writerID: writerID, runID: runID
        )
        await client.injectAtomicCreateURLErrorCancelled(for: path)

        do {
            _ = try await writer.write(
                header: emptySnapshotHeader(),
                assets: [], resources: [], assetResources: [], deletedKeys: [],
                month: month, lamport: lamport, runID: runID,
                respectTaskCancellation: false
            )
            XCTFail("expected CancellationError from URLSession-shape cancel on primary write")
        } catch is CancellationError {
            // expected
        } catch SnapshotWriter.WriteError.finalizationFailed(let underlying) {
            XCTFail("URL-cancel at atomicCreate must NOT wrap as .finalizationFailed (got: \(underlying))")
        } catch SnapshotWriter.WriteError.ioFailure(let underlying) {
            XCTFail("URL-cancel must NOT wrap as .ioFailure (got: \(underlying))")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    /// `.overwritePossible` backend (AMSMB2 parity): user-stop fires while the
    /// gate is running its staging-verify download. The gate normalizes
    /// URL-cancel to CancellationError; the writer's first catch arm must
    /// preserve that identity rather than wrap as finalizationFailed.
    func testOverwritePossibleBackend_gateStagingVerifyURLCancellation_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let writer = SnapshotWriter(client: client, basePath: basePath)

        // Gate stages at a UUID side-path — use the path-agnostic one-shot to
        // hit the first download (the gate's staging-verify).
        await client.injectNextDownloadURLErrorCancelled()

        do {
            _ = try await writer.write(
                header: emptySnapshotHeader(),
                assets: [], resources: [], assetResources: [], deletedKeys: [],
                month: month, lamport: 1, runID: runID,
                respectTaskCancellation: false
            )
            XCTFail("expected CancellationError from gate's staging-verify URL cancellation")
        } catch is CancellationError {
            // expected
        } catch SnapshotWriter.WriteError.finalizationFailed(let underlying) {
            XCTFail("URL-cancel through the gate must NOT surface as .finalizationFailed (got: \(underlying))")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    /// `.exclusive` backend, `.alreadyExists` from atomicCreate, then the SHA
    /// verify download surfaces URL-cancel. The gate's verify retry loop
    /// normalizes this to CancellationError; the writer's `catch is
    /// CancellationError` must preserve it rather than treating the
    /// gate-domain error as a finalization wrap.
    func testExclusiveBackend_alreadyExistsThenVerifyURLCancellation_propagatesCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let writer = SnapshotWriter(client: client, basePath: basePath)

        let lamport: UInt64 = 1
        let path = RepoLayout.snapshotFilePath(
            base: basePath, month: month, lamport: lamport,
            writerID: writerID, runID: runID
        )
        // Pre-occupy so atomicCreate returns .alreadyExists, then inject URL-cancel
        // on the verify download path.
        await client.injectFile(path: path, data: Data("placeholder peer bytes".utf8))
        await client.injectDownloadURLErrorCancelled(for: path)

        do {
            _ = try await writer.write(
                header: emptySnapshotHeader(),
                assets: [], resources: [], assetResources: [], deletedKeys: [],
                month: month, lamport: lamport, runID: runID,
                respectTaskCancellation: false
            )
            XCTFail("expected CancellationError from gate's post-alreadyExists verify URL cancellation")
        } catch is CancellationError {
            // expected
        } catch SnapshotWriter.WriteError.finalizationFailed(let underlying) {
            XCTFail("URL-cancel during verify must NOT wrap as .finalizationFailed (got: \(underlying))")
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
    }

    private func emptySnapshotHeader() -> SnapshotHeader {
        var covered = CoveredRanges()
        covered.add(writerID: writerID, range: ClosedSeqRange(low: 1, high: 1))
        return SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: covered
        )
    }
}

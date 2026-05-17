import Foundation
@testable import Watermelon

/// Single in-memory entry point for V2 repo test state. Every write goes through
/// the real production writers (`CommitLogWriter`, `SnapshotWriter`) so the
/// in-memory state mirrors what a real backup run would produce. This is the
/// only sanctioned way for tests to construct repo state — direct mutation of
/// `RemoteLibrarySnapshotCache` or `RepoCommittedView` is reserved for
/// production code paths.
///
/// The contract this enforces:
/// - Wire-format encoding/decoding gets exercised on every test setup, so a
///   new `RepoWireValidator` rule that breaks the encoder is caught here, not
///   silently bypassed by tests.
/// - Cache state shape always reflects what `materialize` produces, so tests
///   can't assert against artificial states that are unreachable in production.
@MainActor
struct RepoTestBuilder {
    let client: InMemoryRemoteStorageClient
    let basePath: String
    let repoID: String
    let writerID: String
    let runID: String

    /// Bootstrap a fresh V2 repo with default IDs and an empty commit log.
    static func freshRepo(
        basePath: String = "/repo",
        repoID: String = "repo-test-uuid",
        writerID: String = "writer-test-uuid-aaaaaaaaaaaaaaaa",
        runID: String = "run-test"
    ) async throws -> RepoTestBuilder {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // AMSMB2 production parity: atomicCreate is overwritePossible but moveIfAbsent
        // is exclusive — the gate's staging→moveIfAbsent path closes the silent
        // peer-commit overwrite hazard.
        client.setMoveIfAbsentGuarantee(.exclusive)
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        try await client.createDirectory(path: "\(basePath)/.watermelon/commits")
        try await client.createDirectory(path: "\(basePath)/.watermelon/snapshots")
        return RepoTestBuilder(
            client: client,
            basePath: basePath,
            repoID: repoID,
            writerID: writerID,
            runID: runID
        )
    }

    /// Write an addAsset commit through the real `CommitLogWriter`.
    @discardableResult
    func addAsset(
        month: LibraryMonthKey,
        seq: UInt64 = 1,
        clock: UInt64? = nil,
        fingerprint: Data,
        contentHash: Data,
        physicalRemotePath: String? = nil,
        role: Int = ResourceTypeCode.photo,
        slot: Int = 0,
        fileSize: Int64 = 100,
        injectFile: Bool = true
    ) async throws -> Self {
        let path = physicalRemotePath ?? String(
            format: "%04d/%02d/%@.jpg", month.year, month.month, contentHash.hexString
        )
        let body = CommitAddAssetBody(
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: path,
                    logicalName: (path as NSString).lastPathComponent,
                    contentHash: contentHash,
                    fileSize: fileSize,
                    resourceType: role,
                    role: role,
                    slot: slot,
                    crypto: nil
                )
            ]
        )
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let header = TestFixtures.makeCommitHeader(
            repoID: repoID, writerID: writerID, seq: seq, runID: runID, month: month,
            clockMin: clock ?? seq, clockMax: clock ?? seq
        )
        _ = try await writer.write(
            header: header,
            ops: [CommitOp(opSeq: 0, clock: clock ?? seq, body: .addAsset(body))],
            month: month, respectTaskCancellation: false
        )
        if injectFile {
            await client.injectFile(
                path: "\(basePath)/\(path)",
                data: Data(repeating: 0, count: Int(fileSize))
            )
        }
        return self
    }

    /// Write a tombstone commit through the real `CommitLogWriter`.
    @discardableResult
    func tombstoneAsset(
        month: LibraryMonthKey,
        seq: UInt64 = 1,
        clock: UInt64? = nil,
        fingerprint: Data,
        reason: CommitTombstoneBody.Reason = .verifyFailed,
        observedBasis: TombstoneObservationBasis? = nil
    ) async throws -> Self {
        let writer = CommitLogWriter(client: client, basePath: basePath)
        let header = TestFixtures.makeCommitHeader(
            repoID: repoID, writerID: writerID, seq: seq, runID: runID, month: month,
            clockMin: clock ?? seq, clockMax: clock ?? seq
        )
        _ = try await writer.write(
            header: header,
            ops: [CommitOp(opSeq: 0, clock: clock ?? seq, body: .tombstoneAsset(
                CommitTombstoneBody(assetFingerprint: fingerprint, reason: reason, observedBasis: observedBasis)
            ))],
            month: month, respectTaskCancellation: false
        )
        return self
    }

    /// Run a real `RepoMaterializer.materialize()` — what production sees.
    func materialize() async throws -> RepoMaterializer.MaterializeOutput {
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        return try await materializer.materialize(expectedRepoID: repoID)
    }

    /// Run a real materialize for a single month.
    func materialize(month: LibraryMonthKey) async throws -> RepoMaterializer.MaterializeOutput {
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        return try await materializer.materializeMonth(month, expectedRepoID: repoID)
    }
}

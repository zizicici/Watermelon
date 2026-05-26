import Foundation
@testable import Watermelon

@MainActor
struct RepoTestBuilder {
    let client: InMemoryRemoteStorageClient
    let basePath: String
    let repoID: String
    let writerID: String
    let runID: String

    static func freshRepo(
        basePath: String = "/repo",
        repoID: String = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
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
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerID)
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
                CommitTombstoneBody(
                    assetFingerprint: fingerprint,
                    reason: reason,
                    observedBasis: observedBasis ?? TombstoneObservationBasis(perWriterMaxSeq: [:], lamportWatermark: clock ?? seq)
                )
            ))],
            month: month, respectTaskCancellation: false
        )
        return self
    }

    func materialize() async throws -> RepoMaterializer.MaterializeOutput {
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        return try await materializer.materialize(expectedRepoID: repoID)
    }

    func materialize(month: LibraryMonthKey) async throws -> RepoMaterializer.MaterializeOutput {
        let materializer = RepoMaterializer(client: client, basePath: basePath)
        return try await materializer.materializeMonth(month, expectedRepoID: repoID)
    }

    /// Publishes a cross-repo index from the supplied materialize output. Test setup helper
    /// that drives the same publish path the open service uses, with a configurable lamport.
    @discardableResult
    func publishCrossRepoIndex(
        from output: RepoMaterializer.MaterializeOutput,
        lamport: UInt64,
        writerID overrideWriter: String? = nil,
        runID overrideRunID: String? = nil,
        respectTaskCancellation: Bool = false
    ) async throws -> RepoCrossRepoIndexFile {
        try await client.createDirectory(path: "\(basePath)/.watermelon/index")
        let writer = RepoCrossRepoIndexWriter(client: client, basePath: basePath)
        return try await writer.write(
            materialized: output,
            expectedRepoID: repoID,
            writerID: overrideWriter ?? writerID,
            runID: overrideRunID ?? runID,
            lamport: lamport,
            respectTaskCancellation: respectTaskCancellation
        )
    }
}

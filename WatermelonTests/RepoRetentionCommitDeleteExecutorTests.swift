import XCTest
@testable import Watermelon

final class RepoRetentionCommitDeleteExecutorTests: XCTestCase {
    func testDisabledModePerformsZeroIO() async throws {
        let inner = InMemoryRemoteStorageClient()
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)

        let result = try await executor(client: spy, mode: .disabled).execute(
            month: month,
            expectedRepoID: repoID.uppercased(),
            nowMs: nowMs
        )

        XCTAssertEqual(result, .disabled)
        XCTAssertEqual(spy.totalCallCount, 0)
    }

    func testBlockedPreflightPerformsNoDelete() async throws {
        let inner = try await makeClient()
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
                month: month,
                expectedRepoID: repoID,
                nowMs: nowMs
            )

        if case .preflightBlocked(let blockers, _) = result {
            XCTAssertTrue(blockers.contains(.emptyBarrierSet))
        } else {
            XCTFail("expected blocked preflight")
        }
        XCTAssertEqual(spy.deletePaths, [])
    }

    func testHappyPathDeletesOnlyExactCommitPrefixAndLeavesProtectedFiles() async throws {
        let inner = try await makeClient()
        try await writeCommits(client: inner, seqs: 0...4)
        try await writeCommits(client: inner, month: otherMonth, seqs: 1...1)
        let covered = coveredRanges([(1, 3)])
        let snapshot = try await writeSnapshot(client: inner, covered: covered)
        try await writeBarrier(client: inner, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)
        let assetPath = "\(basePath)/2026/05/asset.jpg"
        await inner.injectFile(path: assetPath, contents: "asset")
        await inner.injectFile(path: RepoLayout.livenessFilePath(base: basePath, writerID: writerA), contents: "{}")
        let filesBefore = await inner.snapshotFiles()
        let protectedBefore = filesBefore.filter { path, _ in
            path.contains("/.watermelon/snapshots/") ||
                path.contains("/.watermelon/retention/") ||
                path.contains("/.watermelon/liveness/") ||
                path == RepoLayout.versionFilePath(base: basePath) ||
                path == RepoLayout.repoFilePath(base: basePath) ||
                path == assetPath
        }
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .completed(let summary, _, .passed(_)) = result else {
            XCTFail("expected completed, got \(result)")
            return
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1, 2, 3])
        XCTAssertEqual(summary.alreadyMissing, [])
        XCTAssertEqual(spy.deletePaths, (1...3).map {
            RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: UInt64($0))
        })
        XCTAssertTrue(spy.deletePaths.allSatisfy { $0.hasPrefix(RepoLayout.commitsDirectoryPath(base: basePath) + "/") })
        XCTAssertEqual(spy.forbiddenMutationCount, 0)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 0), exists: true)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 4), exists: true)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: otherMonth, writerID: writerA, seq: 1), exists: true)
        let filesAfter = await inner.snapshotFiles()
        let protectedAfter = filesAfter.filter { protectedBefore.keys.contains($0.key) }
        XCTAssertEqual(protectedAfter, protectedBefore)
    }

    func testNotFoundDeleteIsAlreadyAbsentAndStillVerifies() async throws {
        let inner = try await makeReadyClient(seqs: 1...1, covered: coveredRanges([(1, 1)]))
        let path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDeleteHook { deletePath in
            if deletePath == path {
                try await inner.delete(path: deletePath)
                throw Self.notFoundError()
            }
            try await inner.delete(path: deletePath)
        }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .completed(let summary, _, .passed(_)) = result else {
            XCTFail("expected completed, got \(result)")
            return
        }
        XCTAssertEqual(summary.attempted.map(\.seq), [1])
        XCTAssertEqual(summary.deleted, [])
        XCTAssertEqual(summary.alreadyMissing.map(\.seq), [1])
    }

    func testCandidateChangedAfterFreshPreflightFailsBeforeDelete() async throws {
        let inner = try await makeClient()
        try await writeCommits(client: inner, seqs: 1...2)
        let files = await inner.snapshotFiles()
        let seq2Bytes = try XCTUnwrap(files[RepoLayout.commitFilePath(
            base: basePath,
            month: month,
            writerID: writerA,
            seq: 2
        )])
        let covered = coveredRanges([(1, 1)])
        let snapshot = try await writeSnapshot(client: inner, covered: covered)
        try await writeBarrier(client: inner, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)
        let seq1Path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDownloadHook { path in
            if path == seq1Path, spy.downloadCount(path: path) == 2 {
                await inner.injectFile(path: seq1Path, data: seq2Bytes)
            }
        }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .stopped(let summary, let reason, _, nil) = result else {
            XCTFail("expected pre-delete revalidation failure, got \(result)")
            return
        }
        XCTAssertEqual(summary.attempted, [])
        XCTAssertEqual(spy.deletePaths, [])
        if case .preDeleteRevalidationFailed(_, .headerMismatch(.seq(expected: 1, actual: 2))) = reason {
        } else {
            XCTFail("unexpected stop reason \(reason)")
        }
        await assertFile(inner, path: seq1Path, exists: true)
    }

    func testRevalidationTransportReadFailureStopsBeforeDelete() async throws {
        let inner = try await makeReadyClient(seqs: 1...1, covered: coveredRanges([(1, 1)]))
        let seq1Path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDownloadHook { path in
            if path == seq1Path, spy.downloadCount(path: path) == 2 {
                throw Self.transportError()
            }
        }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .stopped(let summary, let reason, _, nil) = result else {
            XCTFail("expected read-failed revalidation stop, got \(result)")
            return
        }
        XCTAssertEqual(summary.attempted, [])
        XCTAssertEqual(spy.deletePaths, [])
        guard case .preDeleteRevalidationFailed(let candidate, .readFailed) = reason else {
            XCTFail("unexpected stop reason \(reason)")
            return
        }
        XCTAssertEqual(candidate.seq, 1)
        await assertFile(inner, path: seq1Path, exists: true)
    }

    func testFirstCandidateHardFailureStillRunsVerification() async throws {
        let inner = try await makeReadyClient(seqs: 1...1, covered: coveredRanges([(1, 1)]))
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDeleteHook { _ in throw Self.transportError() }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .stopped(let summary, let reason, _, .passed(_)) = result else {
            XCTFail("expected failed with passed verification, got \(result)")
            return
        }
        XCTAssertEqual(summary.attempted.map(\.seq), [1])
        XCTAssertEqual(summary.deleted, [])
        if case .deleteFailed(_, let failure) = reason {
            if case .cancelled = failure {
                XCTFail("expected non-cancellation delete failure")
            }
        } else {
            XCTFail("unexpected stop reason \(reason)")
        }
    }

    func testHardFailureAfterPartialSuccessRecordsMutationEvidence() async throws {
        let inner = try await makeReadyClient(seqs: 1...2, covered: coveredRanges([(1, 2)]))
        let seq2Path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDeleteHook { path in
            if path == seq2Path {
                throw Self.transportError()
            }
            try await inner.delete(path: path)
        }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .stopped(let summary, let reason, _, .passed(_)) = result else {
            XCTFail("expected partial failure, got \(result)")
            return
        }
        XCTAssertEqual(summary.attempted.map(\.seq), [1, 2])
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        if case .deleteFailed(let candidate, let failure) = reason {
            XCTAssertEqual(candidate.seq, 2)
            if case .cancelled = failure {
                XCTFail("expected non-cancellation delete failure")
            }
        } else {
            XCTFail("unexpected stop reason \(reason)")
        }
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1), exists: false)
        await assertFile(inner, path: seq2Path, exists: true)
    }

    func testVerificationInconclusiveDominatesHardDeleteFailure() async throws {
        let inner = try await makeReadyClient(seqs: 1...2, covered: coveredRanges([(1, 2)]))
        let snapshotPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: snapshotLamport, writerID: writerA, runID: runID)
        let seq2Path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDeleteHook { path in
            if path == seq2Path {
                throw Self.transportError()
            }
            try await inner.delete(path: path)
        }
        spy.setDownloadHook { path in
            if path == snapshotPath, spy.deletePaths.contains(seq2Path) {
                throw Self.notFoundError()
            }
        }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .verificationInconclusive(
            let summary,
            let stopReason?,
            _,
            .inconclusive(reason: .materializerReadRace)
        ) = result else {
            XCTFail("expected inconclusive verification to dominate, got \(result)")
            return
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        guard case .deleteFailed(let candidate, .other) = stopReason else {
            XCTFail("unexpected stop reason \(stopReason)")
            return
        }
        XCTAssertEqual(candidate.seq, 2)
    }

    func testVerificationFailureDominatesHardDeleteFailure() async throws {
        let inner = try await makeReadyClient(seqs: 1...2, covered: coveredRanges([(1, 2)]))
        let snapshotPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: snapshotLamport, writerID: writerA, runID: runID)
        let seq2Path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDeleteHook { path in
            if path == seq2Path {
                try await inner.delete(path: snapshotPath)
                throw Self.transportError()
            }
            try await inner.delete(path: path)
        }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .verificationFailed(
            let summary,
            let stopReason?,
            _,
            .failed(reason: .missingAcceptedSnapshot(month), evidence: _)
        ) = result else {
            XCTFail("expected verification failure to dominate, got \(result)")
            return
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        if case .deleteFailed = stopReason {
        } else {
            XCTFail("unexpected stop reason \(stopReason)")
        }
    }

    func testCancellationBeforeFirstDeleteIsCleanAndZeroMutation() async throws {
        let inner = try await makeReadyClient(seqs: 1...1, covered: coveredRanges([(1, 1)]))
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        let service = executor(client: spy, mode: .commitPrefixDeletionEnabled)

        let task = Task {
            try await service.execute(month: month, expectedRepoID: repoID, nowMs: nowMs)
        }
        task.cancel()

        do {
            _ = try await task.value
            XCTFail("expected cancellation")
        } catch is CancellationError {
        }
        XCTAssertEqual(spy.totalCallCount, 0)
    }

    func testCancellationInsideDeleteIsHardFailureThenVerification() async throws {
        let inner = try await makeReadyClient(seqs: 1...1, covered: coveredRanges([(1, 1)]))
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDeleteHook { _ in throw CancellationError() }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .stopped(let summary, let reason, _, .passed(_)) = result else {
            XCTFail("expected typed delete cancellation failure, got \(result)")
            return
        }
        XCTAssertEqual(summary.attempted.map(\.seq), [1])
        if case .deleteFailed(_, .cancelled) = reason {
        } else {
            XCTFail("unexpected stop reason \(reason)")
        }
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1), exists: true)
    }

    func testPostDeleteMaterializerReadRaceIsVerificationInconclusive() async throws {
        let inner = try await makeClient()
        try await writeCommits(client: inner, seqs: 1...2)
        let covered = coveredRanges([(1, 1)])
        let snapshot = try await writeSnapshot(client: inner, covered: covered)
        try await writeBarrier(client: inner, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)
        let seq2Path = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDownloadHook { path in
            if path == seq2Path, spy.deletePaths.count > 0 {
                throw Self.notFoundError()
            }
        }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .verificationInconclusive(
            let summary,
            nil,
            _,
            .inconclusive(reason: .materializerReadRace)
        ) = result else {
            XCTFail("expected read-race verification inconclusive, got \(result)")
            return
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
    }

    func testConcurrentAdditiveCommitIsAllowedByRetentionSupersetVerification() async throws {
        let inner = try await makeReadyClient(seqs: 1...1, covered: coveredRanges([(1, 1)]))
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)
        spy.setDeleteHook { path in
            try await inner.delete(path: path)
            try await self.writeCommit(
                client: inner,
                writerID: self.writerA,
                seq: 2,
                ops: [self.addAssetOp(fingerprint: TestFixtures.fingerprint(0xA1), clock: 2)]
            )
        }

        let result = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .completed(let summary, _, .passed(_)) = result else {
            XCTFail("expected completed, got \(result)")
            return
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2), exists: true)
    }

    func testSpyClientAllowsOnlyCommitDirectoryDeletesAndNoOtherMutations() async throws {
        let inner = try await makeReadyClient(seqs: 1...1, covered: coveredRanges([(1, 1)]))
        let spy = ExecutorSpyClient(inner: inner, basePath: basePath)

        _ = try await executor(client: spy, mode: .commitPrefixDeletionEnabled).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        XCTAssertEqual(spy.forbiddenMutationCount, 0)
        XCTAssertEqual(spy.deletePaths, [
            RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        ])
        XCTAssertTrue(spy.deletePaths.allSatisfy {
            $0.hasPrefix(RepoLayout.commitsDirectoryPath(base: basePath) + "/")
        })
    }

    func testNoProductionCallerOrRuntimeWiring() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let executorPath = root
            .appendingPathComponent("Shared/Services/Repo/RepoRetentionDeleteExecutor.swift")
            .path
        let executorSource = try String(contentsOf: URL(fileURLWithPath: executorPath), encoding: .utf8)
        XCTAssertFalse(executorSource.contains("PreflightProvider"))
        XCTAssertFalse(executorSource.contains("testingPreflightProvider"))
        let productionFiles = try swiftSources(root: root, under: "Shared") +
            swiftSources(root: root, under: "Watermelon") +
            swiftSources(root: root, under: "WatermelonMac")
        let callSites = try productionFiles.filter { $0.path != executorPath }.compactMap { url -> String? in
            let text = try String(contentsOf: url, encoding: .utf8)
            let constructsExecutor = text.contains("RepoRetentionCommitDeleteExecutor(")
            let enablesMode = text.contains("commitPrefixDeletionEnabled")
            let constructsPreflight = text.contains("RepoRetentionDeletePreflightService(")
            return constructsExecutor || enablesMode || constructsPreflight
                ? relativePath(root: root, url: url)
                : nil
        }.sorted()
        XCTAssertEqual(callSites, [])
    }

    private func makeReadyClient(seqs: ClosedRange<UInt64>, covered: CoveredRanges) async throws -> InMemoryRemoteStorageClient {
        let client = try await makeClient()
        try await writeCommits(client: client, seqs: seqs)
        let snapshot = try await writeSnapshot(client: client, covered: covered)
        try await writeBarrier(client: client, covered: covered, checkpointSHA256Hex: snapshot.sha256Hex)
        return client
    }

    private func assertFile(
        _ client: InMemoryRemoteStorageClient,
        path: String,
        exists expected: Bool,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        let actual = await client.hasFile(path)
        XCTAssertEqual(actual, expected, file: file, line: line)
    }

    private func makeClient(injectVersion: Bool = true) async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        client.setAtomicCreateGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerA)
        if injectVersion {
            try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerA)
        }
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.retentionDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        return client
    }

    private func executor(
        client: any RemoteStorageClientProtocol,
        mode: RepoRetentionDeleteExecutionMode
    ) -> RepoRetentionCommitDeleteExecutor {
        RepoRetentionCommitDeleteExecutor(
            client: client,
            basePath: basePath,
            mode: mode,
            policy: policy,
            isLocalVolume: false,
            peerStatusProvider: { .empty }
        )
    }

    private func writeCommits(
        client: any RemoteStorageClientProtocol,
        month: LibraryMonthKey? = nil,
        writerID: String? = nil,
        seqs: ClosedRange<UInt64>
    ) async throws {
        for seq in seqs {
            try await writeCommit(
                client: client,
                month: month ?? self.month,
                writerID: writerID ?? writerA,
                seq: seq,
                ops: []
            )
        }
    }

    private func writeCommit(
        client: any RemoteStorageClientProtocol,
        month: LibraryMonthKey? = nil,
        writerID: String,
        seq: UInt64,
        ops: [CommitOp]
    ) async throws {
        let targetMonth = month ?? self.month
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
                seq: seq,
                runID: runID,
                month: targetMonth
            ),
            ops: ops,
            month: targetMonth,
            respectTaskCancellation: false
        )
    }

    @discardableResult
    private func writeSnapshot(
        client: any RemoteStorageClientProtocol,
        covered: CoveredRanges,
        state: RepoMonthState = .empty
    ) async throws -> SnapshotFile {
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA,
            repoID: repoID,
            covered: covered
        )
        let rows = RepoSnapshotBuilder.build(header: header, state: state)
        return try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: rows.assets,
            resources: rows.resources,
            assetResources: rows.assetResources,
            deletedKeys: rows.deletedKeys,
            month: month,
            lamport: snapshotLamport,
            runID: runID,
            respectTaskCancellation: false
        )
    }

    private func writeBarrier(
        client: InMemoryRemoteStorageClient,
        covered: CoveredRanges,
        deletePrefixByWriter: [String: UInt64]? = nil,
        checkpointSHA256Hex: String
    ) async throws {
        let manifest = RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: repoID,
            month: month,
            createdByWriterID: writerA,
            runID: UUID(uuidString: runID)!,
            createdAtMs: oldBarrierCreatedAtMs,
            barrierLamport: snapshotLamport,
            checkpointSnapshotName: RepoLayout.snapshotFileName(
                month: month,
                lamport: snapshotLamport,
                writerID: writerA,
                runID: runID
            ),
            checkpointSHA256Hex: checkpointSHA256Hex,
            coveredRanges: covered,
            deletePrefixByWriter: deletePrefixByWriter ?? policy.conservativeDeletePrefixByWriter(covered: covered),
            observedSeqHighByWriter: covered.rangesByWriter.mapValues { ranges in
                ranges.map(\.high).max() ?? 0
            },
            policy: RetentionManifestPolicy(
                keepUncoveredCommits: true,
                keepCorruptOrUntrustedCommits: true,
                keepTombstones: true,
                snapshotKeepCount: policy.snapshotFallbackKeepCount
            ),
            livenessGate: RetentionLivenessGate(
                requiredCompleteView: true,
                requiredNoActiveNonSelfWriters: true,
                legacyClientGraceMs: Int64(policy.legacyClientGraceSeconds) * 1000
            )
        )
        await client.injectFile(
            path: RepoLayout.retentionManifestPath(base: basePath, ref: manifest.ref),
            data: try RetentionManifestStore.encode(manifest)
        )
    }

    private func coveredRanges(_ ranges: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [
            writerA: ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        ])
    }

    private func coveredRanges(_ rangesByWriter: [String: [(UInt64, UInt64)]]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: rangesByWriter.mapValues { ranges in
            ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        })
    }

    private func addAssetOp(fingerprint: Data, clock: UInt64) -> CommitOp {
        CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: []
        )))
    }

    private func swiftSources(root: URL, under relativePath: String) throws -> [URL] {
        let directory = root.appendingPathComponent(relativePath)
        guard let enumerator = FileManager.default.enumerator(
            at: directory,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return []
        }
        return try enumerator.compactMap { item in
            guard let url = item as? URL, url.pathExtension == "swift" else { return nil }
            let values = try url.resourceValues(forKeys: [.isRegularFileKey])
            return values.isRegularFile == true ? url : nil
        }
    }

    private func relativePath(root: URL, url: URL) -> String {
        let rootPath = root.path.hasSuffix("/") ? root.path : root.path + "/"
        return url.path.hasPrefix(rootPath) ? String(url.path.dropFirst(rootPath.count)) : url.path
    }

    private static func notFoundError() -> Error {
        RemoteStorageClientError.underlying(NSError(domain: NSCocoaErrorDomain, code: NSFileNoSuchFileError))
    }

    private static func transportError() -> Error {
        RemoteStorageClientError.underlying(NSError(
            domain: NSURLErrorDomain,
            code: NSURLErrorTimedOut,
            userInfo: [NSLocalizedDescriptionKey: "transport failure"]
        ))
    }

    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerA = "11111111-1111-1111-1111-111111111111"
    private let writerB = "22222222-2222-2222-2222-222222222222"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let month = LibraryMonthKey(year: 2026, month: 5)
    private let otherMonth = LibraryMonthKey(year: 2026, month: 6)
    private let snapshotLamport: UInt64 = 10
    private let nowMs: Int64 = 1_800_000_000_000
    private var oldBarrierCreatedAtMs: Int64 { nowMs - Int64(policy.retentionStalenessThresholdSeconds + 1) * 1_000 }
    private let policy = RepoCompactionPolicy(
        checkpointCommitThreshold: 1,
        checkpointByteThreshold: 1,
        minimumCheckpointIntervalSeconds: 0,
        retentionStalenessThresholdSeconds: 60,
        legacyClientGraceSeconds: 120,
        snapshotFallbackKeepCount: 2
    )
}

private final class ExecutorSpyClient: @unchecked Sendable, RemoteStorageClientProtocol {
    typealias DeleteHook = @Sendable (_ path: String) async throws -> Void
    typealias DownloadHook = @Sendable (_ path: String) async throws -> Void

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    nonisolated var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var isSerialized: Bool { false }

    private let inner: InMemoryRemoteStorageClient
    private let commitPrefix: String
    private let lock = NSLock()
    private var callsByName: [String: Int] = [:]
    private var recordedDeletePaths: [String] = []
    private var forbiddenMutations = 0
    private var deleteHook: DeleteHook?
    private var downloadHook: DownloadHook?
    private var downloadCountsByPath: [String: Int] = [:]

    init(inner: InMemoryRemoteStorageClient, basePath: String) {
        self.inner = inner
        self.commitPrefix = Self.normalize(RepoLayout.commitsDirectoryPath(base: basePath)) + "/"
    }

    var totalCallCount: Int {
        lock.withLock { callsByName.values.reduce(0, +) }
    }

    var forbiddenMutationCount: Int {
        lock.withLock { forbiddenMutations }
    }

    var deletePaths: [String] {
        lock.withLock { recordedDeletePaths }
    }

    func downloadCount(path: String) -> Int {
        lock.withLock { downloadCountsByPath[Self.normalize(path)] ?? 0 }
    }

    func setDeleteHook(_ hook: @escaping DeleteHook) {
        lock.withLock { deleteHook = hook }
    }

    func setDownloadHook(_ hook: @escaping DownloadHook) {
        lock.withLock { downloadHook = hook }
    }

    func shouldSetModificationDate() -> Bool { true }
    func shouldLimitUploadRetries(for error: Error) -> Bool { false }
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }

    func connect() async throws {
        record("connect")
        try await inner.connect()
    }

    func disconnect() async {
        record("disconnect")
        await inner.disconnect()
    }

    func verifyWriteAccess() async throws {
        record("verifyWriteAccess")
    }

    func storageCapacity() async throws -> RemoteStorageCapacity? {
        record("storageCapacity")
        return try await inner.storageCapacity()
    }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        record("list")
        return try await inner.list(path: path)
    }

    func metadata(path: String) async throws -> RemoteStorageEntry? {
        record("metadata")
        return try await inner.metadata(path: path)
    }

    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try failForbiddenMutation("upload")
    }

    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try failForbiddenMutation("atomicCreate")
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try failForbiddenMutation("setModificationDate")
    }

    func download(remotePath: String, localURL: URL) async throws {
        let key = Self.normalize(remotePath)
        let hook: DownloadHook? = lock.withLock {
            callsByName["download", default: 0] += 1
            downloadCountsByPath[key, default: 0] += 1
            return downloadHook
        }
        if let hook {
            try await hook(key)
        }
        try await inner.download(remotePath: key, localURL: localURL)
    }

    func exists(path: String) async throws -> Bool {
        record("exists")
        return try await inner.exists(path: path)
    }

    func delete(path: String) async throws {
        let key = Self.normalize(path)
        let hook: DeleteHook? = lock.withLock {
            callsByName["delete", default: 0] += 1
            recordedDeletePaths.append(key)
            return deleteHook
        }
        guard key.hasPrefix(commitPrefix) else {
            XCTFail("delete outside commits directory: \(key)")
            throw NSError(domain: "ExecutorSpyClient", code: 1)
        }
        if let hook {
            try await hook(key)
        } else {
            try await inner.delete(path: key)
        }
    }

    func createDirectory(path: String) async throws {
        try failForbiddenMutation("createDirectory")
    }

    func move(from sourcePath: String, to destinationPath: String) async throws {
        try failForbiddenMutation("move")
    }

    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try failForbiddenMutation("moveIfAbsent")
    }

    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try failForbiddenMutation("copy")
    }

    private func record(_ name: String) {
        lock.withLock { callsByName[name, default: 0] += 1 }
    }

    private func failForbiddenMutation(_ name: String) throws -> Never {
        lock.withLock {
            callsByName[name, default: 0] += 1
            forbiddenMutations += 1
        }
        XCTFail("executor called forbidden mutation \(name)")
        throw NSError(domain: "ExecutorSpyClient", code: 2)
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

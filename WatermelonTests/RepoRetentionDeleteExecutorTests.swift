import XCTest
@testable import Watermelon

final class RepoRetentionDeleteExecutorTests: XCTestCase {
    func testSuccessDeletesOnlyFreshAuthorizedCommitCandidatesAndVerifies() async throws {
        let inner = try await makeReadyClient()
        try await writeCommit(client: inner, writerID: writerA, seq: 0, ops: [])
        try await writeCommits(client: inner, month: otherMonth, writerID: writerA, seqs: 1...1)
        await inner.injectFile(
            path: "\(RepoLayout.commitsDirectoryPath(base: basePath))/\(month.text)--not-a-writer--0000000000000005.jsonl",
            contents: "not trusted"
        )
        await inner.injectFile(
            path: RepoLayout.livenessFilePath(base: basePath, writerID: writerB),
            contents: "{}"
        )
        let spy = StorageCallSpyClient(inner: inner)

        let result = try await executor(client: spy).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )
        let summary = try requireCompleted(result)

        XCTAssertEqual(summary.deleted.map(\.seq), [1, 2, 3])
        XCTAssertEqual(summary.alreadyMissingCount, 0)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1), exists: false)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2), exists: false)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 3), exists: false)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 0), exists: true)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 4), exists: true)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: otherMonth, writerID: writerA, seq: 1), exists: true)
        await assertFile(inner, path: RepoLayout.snapshotFilePath(
            base: basePath,
            month: month,
            lamport: snapshotLamport,
            writerID: writerA,
            runID: runID
        ), exists: true)
        await assertFile(inner, path: RepoLayout.livenessFilePath(base: basePath, writerID: writerB), exists: true)

        let deletePaths = spy.deletePaths()
        XCTAssertEqual(deletePaths, summary.deleted.map(\.path))
        XCTAssertTrue(deletePaths.allSatisfy { path in
            summary.deleted.map(\.path).contains(path)
                && RepoLayout.parseCommitFilename((path as NSString).lastPathComponent) != nil
                && path.hasPrefix(RepoLayout.commitsDirectoryPath(base: basePath) + "/")
        })
        XCTAssertEqual(spy.mutatingCalls().map(\.operation), Array(repeating: "delete", count: 3))
    }

    func testFreshPreflightIgnoresStalePlanAfterRemoteChanges() async throws {
        let inner = try await makeReadyClient()
        let stale = try await RepoRetentionDeletePreflightService(
            client: inner,
            basePath: basePath,
            policy: policy,
            isLocalVolume: false,
            peerStatusProvider: { .empty }
        ).makePlan(month: month, expectedRepoID: repoID, mode: .dryRun, nowMs: nowMs)
        XCTAssertEqual(try requirePlan(stale).commitFiles.map(\.seq), [1, 2, 3])
        await inner.injectFile(
            path: RepoLayout.migrationMarkerPath(base: basePath, writerID: writerA),
            contents: "{}"
        )

        let result = try await executor(client: inner).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .preflightBlocked(let blockers, _) = result else {
            return XCTFail("expected blocked fresh preflight, got \(result)")
        }
        XCTAssertTrue(blockers.contains(.migrationInProgress))
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1), exists: true)
    }

    func testRevalidationRereadsCandidateAndRejectsChangedContentBeforeDelete() async throws {
        let inner = try await makeReadyClient()
        let target = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let changedBytes = try encodedCommitData(
            header: TestFixtures.makeCommitHeader(repoID: repoID, writerID: writerA, seq: 1, runID: runID, month: month),
            ops: [addAssetOp(fingerprint: TestFixtures.assetFingerprint(0xF1), clock: 10)]
        )
        let racing = PostDownloadMutationClient(inner: inner, targetPath: target) { client in
            await client.injectFile(path: target, data: changedBytes)
        }

        let result = try await executor(client: racing).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .stopped(let summary, let reason, _, nil) = result else {
            return XCTFail("expected revalidation stop without verification, got \(result)")
        }
        XCTAssertEqual(summary.deletedCount, 0)
        XCTAssertEqual(racing.downloadCount(path: target), 2)
        await assertFile(inner, path: target, exists: true)
        guard case .preDeleteRevalidationFailed(let candidate, let failure) = reason else {
            return XCTFail("expected revalidation failure, got \(reason)")
        }
        XCTAssertEqual(candidate.seq, 1)
        guard case .contentHashMismatch = failure else {
            return XCTFail("expected content hash mismatch, got \(failure)")
        }
    }

    func testCandidateMissingDuringRevalidationIsIdempotent() async throws {
        let inner = try await makeReadyClient()
        let target = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 1)
        let racing = PostDownloadMutationClient(inner: inner, targetPath: target) { client in
            try? await client.delete(path: target)
        }

        let result = try await executor(client: racing).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )
        let summary = try requireCompleted(result)

        XCTAssertEqual(summary.alreadyMissing.map(\.seq), [1])
        XCTAssertEqual(summary.deleted.map(\.seq), [2, 3])
        XCTAssertEqual(summary.attempted.map(\.seq), [2, 3])
        await assertFile(inner, path: target, exists: false)
    }

    func testDeleteFailureStopsWholeBatchInSeqOrderAndRunsVerification() async throws {
        let inner = try await makeReadyClient(includeWriterB: true)
        let failingPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        let failing = FailingDeleteClient(
            inner: inner,
            failingPath: failingPath,
            failure: InMemoryRemoteStorageClient.InjectedError.transport
        )

        let result = try await executor(client: failing).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .stopped(let summary, let reason, _, let verification?) = result else {
            return XCTFail("expected stopped partial failure with verification, got \(result)")
        }
        XCTAssertEqual(summary.attempted.map { "\($0.writerID):\($0.seq)" }, ["\(writerA):1", "\(writerA):2"])
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        await assertFile(inner, path: failingPath, exists: true)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 3), exists: true)
        await assertFile(inner, path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerB, seq: 1), exists: true)
        guard case .deleteFailed(let candidate, .other) = reason else {
            return XCTFail("expected delete failure, got \(reason)")
        }
        XCTAssertEqual(candidate.seq, 2)
        guard case .passed = verification else {
            return XCTFail("expected verification pass, got \(verification)")
        }
    }

    func testVerificationFailureDominatesDeleteFailure() async throws {
        let inner = try await makeReadyClient()
        let failingPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2)
        let extraPath = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 4)
        let failing = FailingDeleteClient(
            inner: inner,
            failingPath: failingPath,
            failure: InMemoryRemoteStorageClient.InjectedError.transport
        ) { client in
            try? await client.delete(path: extraPath)
        }

        let result = try await executor(client: failing).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .verificationFailed(let summary, let stopReason?, _, let verification) = result else {
            return XCTFail("expected verification failure precedence, got \(result)")
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        guard case .deleteFailed(let candidate, .other) = stopReason else {
            return XCTFail("expected retained delete failure stop reason, got \(String(describing: stopReason))")
        }
        XCTAssertEqual(candidate.seq, 2)
        guard case .failed(let reason, _) = verification else {
            return XCTFail("expected failed verification, got \(verification)")
        }
        XCTAssertEqual(reason, .coveredRangeRegression)
    }

    func testCancellationBeforeDeletePropagatesAndAfterPartialDeleteReturnsTypedResult() async throws {
        let preflightCancelClient = try await makeReadyClient()
        do {
            _ = try await executor(
                client: preflightCancelClient,
                peerStatusProvider: { throw CancellationError() }
            ).execute(month: month, expectedRepoID: repoID, nowMs: nowMs)
            XCTFail("expected cancellation before delete")
        } catch is CancellationError {
        }

        let inner = try await makeReadyClient()
        let cancelling = FailingDeleteClient(
            inner: inner,
            failingPath: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 2),
            failure: CancellationError()
        )
        let result = try await executor(client: cancelling).execute(
            month: month,
            expectedRepoID: repoID,
            nowMs: nowMs
        )

        guard case .stopped(let summary, let reason, _, let verification?) = result else {
            return XCTFail("expected typed cancellation after partial delete, got \(result)")
        }
        XCTAssertEqual(summary.deleted.map(\.seq), [1])
        XCTAssertEqual(reason, .deleteFailed(
            candidate: summary.attempted[1],
            failure: .cancelled
        ))
        guard case .passed = verification else {
            return XCTFail("expected verification pass, got \(verification)")
        }
    }

    func testVerifierSurfacesMaterializerRaceAsInconclusive() async throws {
        let inner = try await makeReadyClient()
        let plan = try await readyPlan(client: inner)
        let target = RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerA, seq: 4)
        let race = DownloadHookClient(inner: inner)
        race.setDownloadHook(path: target, once: false) { _ in
            throw Self.notFoundError()
        }

        let verification = await RepoRetentionPostDeleteVerifier(client: race, basePath: basePath).verify(
            month: month,
            expectedRepoID: repoID,
            contract: plan.preDeleteEvidence.postDeleteEquivalenceContract
        )

        XCTAssertEqual(verification, .inconclusive(reason: .materializerReadRace))
    }

    func testVerifierReadsAuthoritativeRepoIdentityAndFailsOnSwap() async throws {
        let inner = try await makeReadyClient()
        let plan = try await readyPlan(client: inner)
        let swappedRepoID = "ffffffff-0000-1111-2222-333333333333"
        try await TestFixtures.injectRepoJSON(inner, basePath: basePath, repoID: swappedRepoID, writerID: writerA)
        try await TestFixtures.injectIdentityFinalization(inner, basePath: basePath, repoID: swappedRepoID, writerID: writerA)

        let verification = await RepoRetentionPostDeleteVerifier(client: inner, basePath: basePath).verify(
            month: month,
            expectedRepoID: repoID,
            contract: plan.preDeleteEvidence.postDeleteEquivalenceContract
        )

        XCTAssertEqual(verification, .failed(
            reason: .repoIdentityMismatch(
                expected: RepoCanonicalIdentity.normalizeLossy(repoID),
                observed: RepoCanonicalIdentity.normalizeLossy(swappedRepoID)
            ),
            evidence: nil
        ))
    }

    func testVerifierFailsClosedWhenRepoIdentityIsMissing() async throws {
        let inner = try await makeReadyClient()
        let plan = try await readyPlan(client: inner)
        try await inner.delete(path: RepoLayout.identityFinalizationFilePath(base: basePath))

        let verification = await RepoRetentionPostDeleteVerifier(client: inner, basePath: basePath).verify(
            month: month,
            expectedRepoID: repoID,
            contract: plan.preDeleteEvidence.postDeleteEquivalenceContract
        )

        XCTAssertEqual(verification, .failed(
            reason: .missingRepoIdentity(expected: RepoCanonicalIdentity.normalizeLossy(repoID)),
            evidence: nil
        ))
    }

    func testVerifierIsInconclusiveWhenRepoIdentityIsUnreadable() async throws {
        let inner = try await makeReadyClient()
        let plan = try await readyPlan(client: inner)
        await inner.injectFile(path: RepoLayout.identityFinalizationFilePath(base: basePath), contents: "{not-json")

        let verification = await RepoRetentionPostDeleteVerifier(client: inner, basePath: basePath).verify(
            month: month,
            expectedRepoID: repoID,
            contract: plan.preDeleteEvidence.postDeleteEquivalenceContract
        )

        XCTAssertEqual(verification, .inconclusive(reason: .repoIdentityReadFailed))
    }

    func testRuntimeWiringOnlyUsesRetentionMaintenanceOrchestratorAndExecutorDeletes() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let executorPath = "Shared/Services/Repo/RepoRetentionDeleteExecutor.swift"
        let verifierPath = "Shared/Services/Repo/RepoRetentionPostDeleteVerifier.swift"
        let executorSource = try source(root, executorPath)
        XCTAssertTrue(executorSource.contains("client.delete(path: candidate.path)"))
        XCTAssertTrue(executorSource.contains("guard candidate.seq > 0 else"))
        XCTAssertTrue(executorSource.contains("return .failed(.seqZero)"))
        XCTAssertFalse(executorSource.contains("PreflightProvider"))
        XCTAssertFalse(executorSource.contains("testingPreflightProvider"))
        for forbidden in ["upload(", "atomicCreate(", "createDirectory(", "move(", "moveIfAbsent(", "copy("] {
            XCTAssertFalse(executorSource.contains(forbidden), "executor unexpectedly references \(forbidden)")
        }
        XCTAssertFalse(try source(root, "Shared/Services/Repo/RepoRetentionRuntimeMode.swift").contains("RepoRetentionDeleteExecutionMode"))
        XCTAssertFalse(try source(root, verifierPath).contains("RepoCompactionPlanner"))
        XCTAssertFalse(try source(root, verifierPath).contains("RetentionManifestRemoteStore"))

        let productionFiles = try swiftSources(root: root, under: "Shared")
            + swiftSources(root: root, under: "Watermelon")
            + swiftSources(root: root, under: "WatermelonMac")
        let executorURL = root.appendingPathComponent(executorPath)
        let callSites = try productionFiles.filter { $0.path != executorURL.path }.compactMap { url -> String? in
            let text = try String(contentsOf: url, encoding: .utf8)
            return text.contains("RepoRetentionCommitDeleteExecutor")
                ? relativePath(root: root, url: url)
                : nil
        }.sorted()
        XCTAssertEqual(callSites, [
            "Shared/Services/Repo/RepoMaintenanceCoordinator.swift",
            "Shared/Services/Repo/RetentionMaintenanceOrchestrator.swift"
        ])

        for path in [
            "Shared/Services/Repo/BackupV2RuntimeBuilder.swift",
            "Shared/Services/Backup/V2MonthSession.swift",
            "Shared/Services/Backup/V2RetentionBarrierRefresh.swift",
            "Watermelon/Home/HomeScreenStore.swift",
            "Watermelon/Home/HomeExecutionCoordinator.swift"
        ] {
            let text = try source(root, path)
            XCTAssertFalse(text.contains("RepoRetentionCommitDeleteExecutor"), "executor unexpectedly wired in \(path)")
        }

        let orchestrator = try source(root, "Shared/Services/Repo/RetentionMaintenanceOrchestrator.swift")
        XCTAssertTrue(orchestrator.contains("RepoRetentionCommitDeleteExecutor("))
    }

    private func makeReadyClient(includeWriterB: Bool = false) async throws -> InMemoryRemoteStorageClient {
        let client = try await makeClient()
        try await writeCommits(client: client, writerID: writerA, seqs: 1...4)
        if includeWriterB {
            try await writeCommits(client: client, writerID: writerB, seqs: 1...2)
        }
        let covered = includeWriterB
            ? coveredRanges([writerA: [(1, 3)], writerB: [(1, 2)]])
            : coveredRanges([writerA: [(1, 3)]])
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

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        client.setMoveIfAbsentGuarantee(.exclusive)
        client.setAtomicCreateGuarantee(.exclusive)
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerA)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.retentionDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.migrationsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.livenessDirectoryPath(base: basePath))
        return client
    }

    private func executor(
        client: any RemoteStorageClientProtocol,
        peerStatusProvider: @escaping RepoRetentionCommitDeleteExecutor.PeerStatusProvider = { .empty }
    ) -> RepoRetentionCommitDeleteExecutor {
        RepoRetentionCommitDeleteExecutor(
            client: client,
            basePath: basePath,
            policy: policy,
            isLocalVolume: false,
            peerStatusProvider: peerStatusProvider
        )
    }

    private func readyPlan(client: any RemoteStorageClientProtocol) async throws -> RepoRetentionDeletePreflightPlan {
        try requirePlan(try await RepoRetentionDeletePreflightService(
            client: client,
            basePath: basePath,
            policy: policy,
            isLocalVolume: false,
            peerStatusProvider: { .empty }
        ).makePlan(month: month, expectedRepoID: repoID, mode: .dryRun, nowMs: nowMs))
    }

    private func writeCommits(
        client: any RemoteStorageClientProtocol,
        month: LibraryMonthKey? = nil,
        writerID: String,
        seqs: ClosedRange<UInt64>
    ) async throws {
        for seq in seqs {
            try await writeCommit(client: client, month: month, writerID: writerID, seq: seq, ops: [])
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
        lamport: UInt64? = nil,
        state: RepoMonthState = .empty
    ) async throws -> SnapshotFile {
        let targetLamport = lamport ?? snapshotLamport
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
            lamport: targetLamport,
            runID: runID,
            respectTaskCancellation: false
        )
    }

    private func writeBarrier(
        client: InMemoryRemoteStorageClient,
        covered: CoveredRanges,
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
            deletePrefixByWriter: policy.conservativeDeletePrefixByWriter(covered: covered),
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
                legacyClientGraceMs: 0
            )
        )
        await client.injectFile(
            path: RepoLayout.retentionManifestPath(base: basePath, ref: manifest.ref),
            data: try RetentionManifestStore.encode(manifest)
        )
    }

    private func addAssetOp(fingerprint: AssetFingerprint, clock: UInt64) -> CommitOp {
        CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: fingerprint,
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: [
                CommitResourceEntry(
                    physicalRemotePath: "\(month.year)/\(String(format: "%02d", month.month))/changed.jpg",
                    logicalName: "changed.jpg",
                    contentHash: TestFixtures.fingerprint(0xE1),
                    fileSize: 1,
                    resourceType: ResourceTypeCode.photo,
                    role: ResourceTypeCode.photo,
                    slot: 0,
                    crypto: nil
                )
            ]
        )))
    }

    private func encodedCommitData(header: CommitHeader, ops: [CommitOp]) throws -> Data {
        var lines: [String] = []
        var integrity = IntegrityAccumulator()
        let headerLine = try CommitOpMapper.encodeHeaderLine(header)
        lines.append(headerLine)
        integrity.absorbLine(headerLine)
        for op in ops {
            let line = try CommitOpMapper.encodeOpLine(op)
            lines.append(line)
            integrity.absorbLine(line)
        }
        lines.append(try CommitOpMapper.encodeEndLine(
            sha256Hex: integrity.finalize(),
            rowCount: integrity.rowCount
        ))
        return Data((lines.joined(separator: "\n") + "\n").utf8)
    }

    private func coveredRanges(_ rangesByWriter: [String: [(UInt64, UInt64)]]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: rangesByWriter.mapValues { ranges in
            ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        })
    }

    private func requireCompleted(_ result: RepoRetentionCommitDeleteResult) throws -> RepoRetentionCommitDeleteSummary {
        guard case .completed(let summary, _, let verification) = result else {
            XCTFail("expected completed, got \(result)")
            throw NSError(domain: "RepoRetentionDeleteExecutorTests", code: 1)
        }
        guard case .passed = verification else {
            XCTFail("expected passed verification, got \(verification)")
            throw NSError(domain: "RepoRetentionDeleteExecutorTests", code: 2)
        }
        return summary
    }

    private func requirePlan(_ result: RepoRetentionDeletePreflightResult) throws -> RepoRetentionDeletePreflightPlan {
        switch result {
        case .planned(let plan, _):
            return plan
        case .blocked(let blockers, _):
            XCTFail("expected plan, got blockers \(blockers)")
            throw NSError(domain: "RepoRetentionDeleteExecutorTests", code: 3)
        }
    }

    private func source(_ root: URL, _ path: String) throws -> String {
        try String(contentsOf: root.appendingPathComponent(path), encoding: .utf8)
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
        RemoteStorageClientError.underlying(NSError(
            domain: NSCocoaErrorDomain,
            code: NSFileNoSuchFileError,
            userInfo: [NSLocalizedDescriptionKey: "no such file"]
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
        retentionStalenessThresholdSeconds: 60,
        snapshotFallbackKeepCount: 1
    )
}

private struct SpyCall: Equatable {
    let operation: String
    let path: String?
}

private final class StorageCallSpyClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var calls: [SpyCall] = []

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func allCalls() -> [SpyCall] { lock.withLock { calls } }
    func deletePaths() -> [String] { allCalls().filter { $0.operation == "delete" }.compactMap(\.path) }
    func mutatingCalls() -> [SpyCall] {
        allCalls().filter { ["upload", "atomicCreate", "setModificationDate", "delete", "createDirectory", "move", "moveIfAbsent", "copy"].contains($0.operation) }
    }

    func connect() async throws { record("connect"); try await inner.connect() }
    func disconnect() async { record("disconnect"); await inner.disconnect() }
    func verifyWriteAccess() async throws { record("verifyWriteAccess") }
    func storageCapacity() async throws -> RemoteStorageCapacity? { record("storageCapacity"); return try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { record("list", path); return try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { record("metadata", path); return try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        record("upload", remotePath)
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        record("atomicCreate", remotePath)
        return try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool {
        record("supportsExclusiveMoveIfAbsent", destinationPath)
        return true
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        record("setModificationDate", path)
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        record("download", remotePath)
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { record("exists", path); return try await inner.exists(path: path) }
    func delete(path: String) async throws { record("delete", path); try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { record("createDirectory", path); try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        record("move", "\(sourcePath)->\(destinationPath)")
        try await inner.move(from: sourcePath, to: destinationPath)
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        record("moveIfAbsent", "\(sourcePath)->\(destinationPath)")
        return try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        record("copy", "\(sourcePath)->\(destinationPath)")
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private func record(_ operation: String, _ path: String? = nil) {
        lock.withLock { calls.append(SpyCall(operation: operation, path: path.map(Self.normalize))) }
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

private final class PostDownloadMutationClient: @unchecked Sendable, RemoteStorageClientProtocol {
    typealias Mutation = @Sendable (_ client: InMemoryRemoteStorageClient) async -> Void

    let inner: InMemoryRemoteStorageClient
    private let targetPath: String
    private let mutation: Mutation
    private let lock = NSLock()
    private var downloadCounts: [String: Int] = [:]
    private var didMutate = false

    init(inner: InMemoryRemoteStorageClient, targetPath: String, mutation: @escaping Mutation) {
        self.inner = inner
        self.targetPath = Self.normalize(targetPath)
        self.mutation = mutation
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func downloadCount(path: String) -> Int { lock.withLock { downloadCounts[Self.normalize(path)] ?? 0 } }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws {
        let key = Self.normalize(remotePath)
        try await inner.download(remotePath: remotePath, localURL: localURL)
        let shouldMutate = lock.withLock { () -> Bool in
            downloadCounts[key, default: 0] += 1
            guard key == targetPath, !didMutate else { return false }
            didMutate = true
            return true
        }
        if shouldMutate {
            await mutation(inner)
        }
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

private final class FailingDeleteClient: @unchecked Sendable, RemoteStorageClientProtocol {
    typealias ExtraMutation = @Sendable (_ client: InMemoryRemoteStorageClient) async -> Void

    let inner: InMemoryRemoteStorageClient
    private let failingPath: String
    private let failure: Error
    private let extraMutation: ExtraMutation?

    init(
        inner: InMemoryRemoteStorageClient,
        failingPath: String,
        failure: Error,
        extraMutation: ExtraMutation? = nil
    ) {
        self.inner = inner
        self.failingPath = Self.normalize(failingPath)
        self.failure = failure
        self.extraMutation = extraMutation
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws {
        if Self.normalize(path) == failingPath {
            if let extraMutation {
                await extraMutation(inner)
            }
            throw failure
        }
        try await inner.delete(path: path)
    }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

private final class DownloadHookClient: @unchecked Sendable, RemoteStorageClientProtocol {
    typealias DownloadHook = @Sendable (_ path: String) async throws -> Void

    let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var hooks: [String: (once: Bool, hook: DownloadHook)] = [:]

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var supportsLivenessSafeOverwriteUpload: Bool { true }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }

    func setDownloadHook(path: String, once: Bool = true, hook: @escaping DownloadHook) {
        lock.withLock { hooks[Self.normalize(path)] = (once, hook) }
    }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func download(remotePath: String, localURL: URL) async throws {
        let key = Self.normalize(remotePath)
        let hook = lock.withLock { () -> DownloadHook? in
            guard let entry = hooks[key] else { return nil }
            if entry.once { hooks.removeValue(forKey: key) }
            return entry.hook
        }
        if let hook {
            try await hook(key)
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

import Foundation
import XCTest
@testable import Watermelon

final class RepoRetentionBarrierServiceTests: XCTestCase {
    func testPublisherBuildsManifestFromFreshCheckpointState() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xA1)
        let checkpoint = try await writeAcceptedCheckpoint(client: client)
        let snapshotName = try XCTUnwrap(checkpoint.snapshotName)
        let beforeProtected = await protectedKeyspace(client)

        let result = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
        let snapshotFile = try await SnapshotReader(client: client, basePath: basePath).read(filename: snapshotName)
        let materialized = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(month, expectedRepoID: repoID)

        XCTAssertEqual(result.writeOutcome, .wroteVerified)
        XCTAssertEqual(result.filename, RetentionManifestStore.filename(for: result.manifest.ref))
        XCTAssertEqual(result.manifest.repoID, repoID)
        XCTAssertEqual(result.manifest.month, month)
        XCTAssertEqual(result.manifest.createdByWriterID, writerA)
        XCTAssertEqual(result.manifest.runID, UUID(uuidString: runID))
        XCTAssertEqual(result.manifest.createdAtMs, 1_800_000_000_000)
        XCTAssertEqual(result.manifest.barrierLamport, checkpoint.lamport)
        XCTAssertEqual(result.manifest.checkpointSnapshotName, snapshotName)
        XCTAssertEqual(result.manifest.checkpointSHA256Hex, snapshotFile.sha256Hex)
        XCTAssertEqual(result.manifest.coveredRanges, checkpoint.covered)
        XCTAssertEqual(result.manifest.deletePrefixByWriter, policy.conservativeDeletePrefixByWriter(covered: checkpoint.covered))
        XCTAssertEqual(result.manifest.observedSeqHighByWriter, materialized.observedSeqByWriter)
        XCTAssertEqual(result.manifest.policy, RetentionManifestPolicy(
            keepUncoveredCommits: true,
            keepCorruptOrUntrustedCommits: true,
            keepTombstones: true,
            snapshotKeepCount: policy.snapshotFallbackKeepCount
        ))
        XCTAssertEqual(result.manifest.livenessGate, RetentionLivenessGate(
            requiredCompleteView: true,
            requiredNoActiveNonSelfWriters: true,
            legacyClientGraceMs: Int64(BackupV2Constants.unknownRetentionCapabilityGraceSeconds) * 1000
        ))
        XCTAssertTrue(result.barrierSet.unionCovered.superset(of: result.manifest.coveredRanges))
        XCTAssertEqual(result.loadInvalidEntries, [])
        let afterProtected = await protectedKeyspace(client)
        XCTAssertEqual(afterProtected, beforeProtected)
    }

    func testPublisherRejectsStaleCheckpointAndDoesNotWriteRetention() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xB1)
        let checkpoint = try await writeAcceptedCheckpoint(client: client)
        try await writeSnapshot(client: client, lamport: 999, writerID: writerB, runID: peerRunID, covered: checkpoint.covered)
        let beforeRetention = await retentionKeyspace(client)

        do {
            _ = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            XCTFail("expected checkpointNotAccepted")
        } catch RepoRetentionBarrierError.checkpointNotAccepted(let snapshotName) {
            XCTAssertEqual(snapshotName, checkpoint.snapshotName)
        }
        let afterRetention = await retentionKeyspace(client)
        XCTAssertEqual(afterRetention, beforeRetention)
    }

    func testPublisherRejectsSkippedCheckpointAndInvalidRunIDBeforeWriting() async throws {
        let client = try await makeClient()
        let skipped = RepoCheckpointResult(
            outcome: .skippedEmptyFold,
            month: month,
            snapshotName: nil,
            lamport: nil,
            covered: .empty,
            beforeReport: nil,
            afterReport: nil,
            acceptedSnapshot: nil
        )

        do {
            _ = try await service(client: client).publishBarrier(for: skipped, respectTaskCancellation: true)
            XCTFail("expected checkpointNotWritten")
        } catch RepoRetentionBarrierError.checkpointNotWritten {
        }

        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xC1)
        let checkpoint = try await writeAcceptedCheckpoint(client: client)
        do {
            _ = try await service(client: client, runID: "not-a-uuid").publishBarrier(for: checkpoint, respectTaskCancellation: true)
            XCTFail("expected invalidRunID")
        } catch RepoRetentionBarrierError.invalidRunID(let value) {
            XCTAssertEqual(value, "not-a-uuid")
        }
    }

    func testPublisherFailsClosedWhenSameMonthInvalidSiblingExists() async throws {
        let client = try await makeClient()
        let foreign = makeManifest(lamport: 77, repoID: foreignRepoID, writerID: writerB, runID: peerRunID)
        await client.injectFile(
            path: RepoLayout.retentionManifestPath(base: basePath, ref: foreign.ref),
            data: try RetentionManifestStore.encode(foreign)
        )
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xD1)
        let checkpoint = try await writeAcceptedCheckpoint(client: client)

        do {
            _ = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            XCTFail("expected invalidBarrierSet")
        } catch RepoRetentionBarrierError.invalidBarrierSet(let invalid) {
            XCTAssertEqual(invalid.map(\.reason), [.foreignRepoID(foreignRepoID)])
        }
    }

    func testPublisherPropagatesCancellationUnwrappedFromMaterialize() async throws {
        let inner = try await makeClient()
        try await writeAddCommit(client: inner, seq: 1, clock: 1, assetByte: 0xE1)
        let checkpoint = try await writeAcceptedCheckpoint(client: inner)
        let client = BarrierHookClient(inner: inner)
        client.cancelNextList(path: RepoLayout.snapshotsDirectoryPath(base: basePath))

        do {
            _ = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            XCTFail("expected CancellationError")
        } catch is CancellationError {
        }
    }

    func testPublisherPropagatesCancellationUnwrappedFromWriteAndPostWriteSteps() async throws {
        for hook in CancellationHook.allCases {
            let inner = try await makeClient()
            try await writeAddCommit(client: inner, seq: 1, clock: 1, assetByte: 0xE2)
            let checkpoint = try await writeAcceptedCheckpoint(client: inner)
            let client = BarrierHookClient(inner: inner)
            let manifestPath = RepoLayout.retentionManifestPath(
                base: basePath,
                ref: RetentionManifestRef(
                    month: month,
                    lamport: try XCTUnwrap(checkpoint.lamport),
                    writerID: writerA,
                    runIDPrefix: RepoLayout.runIDPrefix(runID)
                )
            )

            switch hook {
            case .createDirectory:
                client.cancelNextCreateDirectory(path: RepoLayout.retentionDirectoryPath(base: basePath))
            case .metadataGateCreate:
                client.cancelNextAtomicCreate(containing: RepoLayout.retentionDirectoryPath(base: basePath))
            case .verifyMatchesLocal:
                client.cancelDownload(containing: ".staging-", occurrence: 1)
            case .decodeRoundtripDownload:
                client.cancelDownload(containing: manifestPath, occurrence: 2)
            case .postWriteLoadBarrierSet:
                client.cancelNextList(path: RepoLayout.retentionDirectoryPath(base: basePath))
            }

            do {
                _ = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
                XCTFail("expected CancellationError for \(hook)")
            } catch is CancellationError {
            }
        }
    }

    func testPublisherPropagatesCancellationUnwrappedFromCheckpointRead() async throws {
        let inner = try await makeClient()
        try await writeAddCommit(client: inner, seq: 1, clock: 1, assetByte: 0xE3)
        let checkpoint = try await writeAcceptedCheckpoint(client: inner)
        let client = BarrierHookClient(inner: inner)
        client.cancelDownload(containing: try XCTUnwrap(checkpoint.snapshotName), occurrence: 2)

        do {
            _ = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            XCTFail("expected CancellationError")
        } catch is CancellationError {
        }
    }

    func testPublisherRejectsCheckpointCoverageMismatch() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xE4)
        let checkpoint = try await writeAcceptedCheckpoint(client: client)
        let mismatched = RepoCheckpointResult(
            outcome: checkpoint.outcome,
            month: checkpoint.month,
            snapshotName: checkpoint.snapshotName,
            lamport: checkpoint.lamport,
            covered: .empty,
            beforeReport: checkpoint.beforeReport,
            afterReport: checkpoint.afterReport,
            acceptedSnapshot: checkpoint.acceptedSnapshot
        )

        do {
            _ = try await service(client: client).publishBarrier(for: mismatched, respectTaskCancellation: true)
            XCTFail("expected checkpointCoverageMismatch")
        } catch RepoRetentionBarrierError.checkpointCoverageMismatch(let snapshotName) {
            XCTAssertEqual(snapshotName, checkpoint.snapshotName)
        }
    }

    func testPublisherRejectsCheckpointReadFailure() async throws {
        let inner = try await makeClient()
        try await writeAddCommit(client: inner, seq: 1, clock: 1, assetByte: 0xE5)
        let checkpoint = try await writeAcceptedCheckpoint(client: inner)
        let client = BarrierHookClient(inner: inner)
        client.failDownloadWithTransport(containing: try XCTUnwrap(checkpoint.snapshotName), occurrence: 2)

        do {
            _ = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            XCTFail("expected checkpointReadFailed")
        } catch RepoRetentionBarrierError.checkpointReadFailed(let snapshotName) {
            XCTAssertEqual(snapshotName, checkpoint.snapshotName)
        }
    }

    func testPublisherRejectsCheckpointHeaderMismatch() async throws {
        let inner = try await makeClient()
        try await writeAddCommit(client: inner, seq: 1, clock: 1, assetByte: 0xE6)
        let checkpoint = try await writeAcceptedCheckpoint(client: inner)
        let replacement = try await makeSnapshotBytes(writerID: writerB, runID: peerRunID, covered: checkpoint.covered)
        let client = BarrierHookClient(inner: inner)
        client.replaceDownload(containing: try XCTUnwrap(checkpoint.snapshotName), occurrence: 2, data: replacement)

        do {
            _ = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            XCTFail("expected checkpointSHAMismatch")
        } catch RepoRetentionBarrierError.checkpointSHAMismatch(let snapshotName) {
            XCTAssertEqual(snapshotName, checkpoint.snapshotName)
        }
    }

    func testPublisherRejectsInvalidBarrierLamportBeforeWriting() async throws {
        let client = try await makeClient()
        let snapshotName = RepoLayout.snapshotFileName(
            month: month,
            lamport: LamportClock.maxAdoptableValue,
            writerID: writerA,
            runID: runID
        )
        let checkpoint = RepoCheckpointResult(
            outcome: .writtenAccepted,
            month: month,
            snapshotName: snapshotName,
            lamport: LamportClock.maxAdoptableValue,
            covered: .empty,
            beforeReport: nil,
            afterReport: nil,
            acceptedSnapshot: nil
        )

        do {
            _ = try await service(client: client).publishBarrier(for: checkpoint, respectTaskCancellation: true)
            XCTFail("expected invalidBarrierLamport")
        } catch RepoRetentionBarrierError.invalidBarrierLamport(let lamport) {
            XCTAssertEqual(lamport, LamportClock.maxAdoptableValue)
        }
        let retention = await retentionKeyspace(client)
        XCTAssertEqual(retention, [:])
    }

    func testRetentionSweepIntegrationLivesInCorrectModules() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let runtimeBuilder = root.appendingPathComponent("Shared/Services/Repo/BackupV2RuntimeBuilder.swift")
        let runtimeSource = try String(contentsOf: runtimeBuilder, encoding: .utf8)
        XCTAssertFalse(runtimeSource.contains("RepoRetentionBarrierService"))
        XCTAssertFalse(runtimeSource.contains("RetentionManifestRemoteStore"))
        XCTAssertFalse(runtimeSource.contains("OrphanMetadataCleanup"))
        XCTAssertFalse(runtimeSource.contains("RepoRetentionStartupMaintenance"))
        XCTAssertFalse(runtimeSource.contains("snapshotPeerStatuses"))
        XCTAssertFalse(runtimeSource.contains("liveness.start"))

        let openService = root.appendingPathComponent("Shared/Services/Repo/BackupV2RepoOpenService.swift")
        let openSource = try String(contentsOf: openService, encoding: .utf8)
        XCTAssertFalse(openSource.contains("OrphanMetadataCleanup"))
        XCTAssertFalse(openSource.contains("RepoRetentionStartupMaintenance"))
        XCTAssertFalse(openSource.contains("Task(priority: .utility)"))

        let maintenanceRuntime = root.appendingPathComponent("Shared/Services/Repo/RepoMaintenanceRuntime.swift")
        let maintenanceSource = try String(contentsOf: maintenanceRuntime, encoding: .utf8)
        XCTAssertTrue(maintenanceSource.contains("OrphanMetadataCleanup"))
        XCTAssertTrue(maintenanceSource.contains("RepoRetentionStartupMaintenance"))
        XCTAssertTrue(maintenanceSource.contains("liveness.start"))

        let cleanup = root.appendingPathComponent("Shared/Services/Repo/OrphanMetadataCleanup.swift")
        let cleanupSource = try String(contentsOf: cleanup, encoding: .utf8)
        XCTAssertTrue(cleanupSource.contains("retentionDirectory"))
    }

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectIdentityFinalization(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerA)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        return client
    }

    private func service(
        client: any RemoteStorageClientProtocol,
        runID: String = "33333333-3333-3333-3333-333333333333"
    ) -> RepoRetentionBarrierService {
        RepoRetentionBarrierService(
            client: client,
            basePath: basePath,
            repoID: repoID,
            writerID: writerA,
            runID: runID,
            policy: policy,
            nowMs: { 1_800_000_000_000 }
        )
    }

    private func writeAcceptedCheckpoint(client: any RemoteStorageClientProtocol) async throws -> RepoCheckpointResult {
        let materialized = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(month, expectedRepoID: repoID)
        let covered = materialized.coveredByMonth[month, default: .empty]
        let state = materialized.state.months[month] ?? .empty
        let lamport = max(materialized.state.observedClock, 1) + 1
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerA,
            repoID: repoID,
            covered: covered
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: state)
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: month,
            lamport: lamport,
            runID: runID,
            respectTaskCancellation: true
        )
        let snapshotName = RepoLayout.snapshotFileName(month: month, lamport: lamport, writerID: writerA, runID: runID)
        let after = try await RepoMaterializer(client: client, basePath: basePath)
            .materializeMonth(month, expectedRepoID: repoID)
        let acceptedAfter = after.acceptedSnapshotBaselinesByMonth[month]
        return RepoCheckpointResult(
            outcome: .writtenAccepted,
            month: month,
            snapshotName: snapshotName,
            lamport: lamport,
            covered: covered,
            beforeReport: nil,
            afterReport: nil,
            acceptedSnapshot: acceptedAfter
        )
    }

    private func writeAddCommit(
        client: any RemoteStorageClientProtocol,
        seq: UInt64,
        clock: UInt64,
        assetByte: UInt8
    ) async throws {
        let op = CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: TestFixtures.fingerprint(assetByte),
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: []
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerA,
                seq: seq,
                runID: runID,
                month: month,
                clockMin: clock,
                clockMax: clock
            ),
            ops: [op],
            month: month,
            respectTaskCancellation: true
        )
    }

    private func writeSnapshot(
        client: any RemoteStorageClientProtocol,
        lamport: UInt64,
        writerID: String,
        runID: String,
        covered: CoveredRanges
    ) async throws {
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: covered
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: monthState())
        _ = try await SnapshotWriter(client: client, basePath: basePath).write(
            header: header,
            assets: parts.assets,
            resources: parts.resources,
            assetResources: parts.assetResources,
            deletedKeys: parts.deletedKeys,
            month: month,
            lamport: lamport,
            runID: runID,
            respectTaskCancellation: true
        )
    }

    private func makeSnapshotBytes(
        writerID: String,
        runID: String,
        covered: CoveredRanges
    ) async throws -> Data {
        let tempClient = InMemoryRemoteStorageClient()
        try await tempClient.connect()
        try await writeSnapshot(
            client: tempClient,
            lamport: 123,
            writerID: writerID,
            runID: runID,
            covered: covered
        )
        let path = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 123, writerID: writerID, runID: runID)
        let files = await tempClient.snapshotFiles()
        return try XCTUnwrap(files[path])
    }

    private func monthState() -> RepoMonthState {
        let fp = TestFixtures.fingerprint(0xB1)
        var state = RepoMonthState.empty
        state.assets[fp] = SnapshotAssetRow(
            assetFingerprint: fp,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 0,
            totalFileSizeBytes: 0,
            stamp: OpStamp(writerID: writerA, seq: 1, clock: 1)
        )
        return state
    }

    private func makeManifest(
        lamport: UInt64,
        repoID: String,
        writerID: String,
        runID: String
    ) -> RetentionManifest {
        let covered = CoveredRanges(rangesByWriter: [writerID: [ClosedSeqRange(low: 1, high: 1)]])
        return RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: repoID,
            month: month,
            createdByWriterID: writerID,
            runID: UUID(uuidString: runID)!,
            createdAtMs: 1,
            barrierLamport: lamport,
            checkpointSnapshotName: RepoLayout.snapshotFileName(month: month, lamport: lamport, writerID: writerID, runID: runID),
            checkpointSHA256Hex: String(repeating: "b", count: 64),
            coveredRanges: covered,
            deletePrefixByWriter: covered.conservativeContiguousPrefixByWriter(),
            observedSeqHighByWriter: [writerID: 1],
            policy: RetentionManifestPolicy(
                keepUncoveredCommits: true,
                keepCorruptOrUntrustedCommits: true,
                keepTombstones: true,
                snapshotKeepCount: 2
            ),
            livenessGate: RetentionLivenessGate(
                requiredCompleteView: true,
                requiredNoActiveNonSelfWriters: true,
                legacyClientGraceMs: 604_800_000
            )
        )
    }

    private func protectedKeyspace(_ client: InMemoryRemoteStorageClient) async -> [String: Data] {
        await client.snapshotFiles().filter {
            $0.key.contains("/.watermelon/commits/")
                || $0.key.contains("/.watermelon/snapshots/")
                || $0.key.contains("/.watermelon/liveness/")
                || $0.key.contains("/.watermelon/identity/")
                || $0.key.contains("/.watermelon/migrations/")
        }
    }

    private func retentionKeyspace(_ client: InMemoryRemoteStorageClient) async -> [String: Data] {
        await client.snapshotFiles().filter { $0.key.contains("/.watermelon/retention/") }
    }

    private let basePath = "/unit4-publisher"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let foreignRepoID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let peerRunID = "44444444-4444-4444-4444-444444444444"
    private let month = LibraryMonthKey(year: 2026, month: 5)
    private let policy = RepoCompactionPolicy(
        checkpointCommitThreshold: 1,
        checkpointByteThreshold: Int64.max,
        minimumCheckpointIntervalSeconds: 0,
        retentionStalenessThresholdSeconds: 86_400,
        snapshotFallbackKeepCount: 2
    )

    private enum CancellationHook: CaseIterable {
        case createDirectory
        case metadataGateCreate
        case verifyMatchesLocal
        case decodeRoundtripDownload
        case postWriteLoadBarrierSet
    }
}

private final class BarrierHookClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var cancelledListPaths: Set<String> = []
    private var cancelledCreateDirectoryPaths: Set<String> = []
    private var cancelledAtomicCreateSubstrings: [String] = []
    private var downloadHooks: [DownloadHook] = []
    private var downloadCounts: [String: Int] = [:]

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    func cancelNextList(path: String) {
        lock.withLock {
            _ = cancelledListPaths.insert(Self.normalize(path))
        }
    }

    func cancelNextCreateDirectory(path: String) {
        lock.withLock {
            _ = cancelledCreateDirectoryPaths.insert(Self.normalize(path))
        }
    }

    func cancelNextAtomicCreate(containing substring: String) {
        lock.withLock {
            cancelledAtomicCreateSubstrings.append(Self.normalize(substring))
        }
    }

    func cancelDownload(containing substring: String, occurrence: Int) {
        addDownloadHook(containing: substring, occurrence: occurrence, action: .cancel)
    }

    func failDownloadWithTransport(containing substring: String, occurrence: Int) {
        addDownloadHook(containing: substring, occurrence: occurrence, action: .transportFailure)
    }

    func replaceDownload(containing substring: String, occurrence: Int, data: Data) {
        addDownloadHook(containing: substring, occurrence: occurrence, action: .replace(data))
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    var supportsLivenessSafeOverwriteUpload: Bool { false }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var isSerialized: Bool { false }

    func shouldSetModificationDate() -> Bool { true }
    func shouldLimitUploadRetries(for error: Error) -> Bool { false }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] {
        let normalized = Self.normalize(path)
        let shouldCancel = lock.withLock { cancelledListPaths.remove(normalized) != nil }
        if shouldCancel { throw CancellationError() }
        return try await inner.list(path: path)
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool {
        try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: destinationPath)
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        let normalized = Self.normalize(remotePath)
        let shouldCancel = lock.withLock { () -> Bool in
            guard let index = cancelledAtomicCreateSubstrings.firstIndex(where: { normalized.contains($0) }) else {
                return false
            }
            cancelledAtomicCreateSubstrings.remove(at: index)
            return true
        }
        if shouldCancel {
            throw NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
        }
        return try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        let normalized = Self.normalize(remotePath)
        let action = lock.withLock { () -> DownloadAction? in
            for index in downloadHooks.indices {
                let hook = downloadHooks[index]
                guard normalized.contains(hook.substring) else { continue }
                let count = (downloadCounts[hook.substring] ?? 0) + 1
                downloadCounts[hook.substring] = count
                guard count == hook.occurrence else { return nil }
                downloadHooks.remove(at: index)
                return hook.action
            }
            return nil
        }
        switch action {
        case .cancel:
            throw CancellationError()
        case .transportFailure:
            throw RemoteStorageClientError.underlying(NSError(
                domain: NSURLErrorDomain,
                code: NSURLErrorNotConnectedToInternet,
                userInfo: [NSLocalizedDescriptionKey: "transport failure"]
            ))
        case .replace(let data):
            try data.write(to: localURL, options: .atomic)
            return
        case nil:
            break
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws {
        let normalized = Self.normalize(path)
        let shouldCancel = lock.withLock {
            cancelledCreateDirectoryPaths.remove(normalized) != nil
        }
        if shouldCancel { throw CancellationError() }
        try await inner.createDirectory(path: path)
    }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }

    private func addDownloadHook(containing substring: String, occurrence: Int, action: DownloadAction) {
        lock.withLock {
            downloadHooks.append(DownloadHook(
                substring: Self.normalizeForContains(substring),
                occurrence: occurrence,
                action: action
            ))
        }
    }

    private static func normalizeForContains(_ value: String) -> String {
        value.contains("/") ? normalize(value) : value
    }

    private struct DownloadHook {
        let substring: String
        let occurrence: Int
        let action: DownloadAction
    }

    private enum DownloadAction {
        case cancel
        case transportFailure
        case replace(Data)
    }
}

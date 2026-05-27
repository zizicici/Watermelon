import Foundation
import XCTest
@testable import Watermelon

final class RepoCheckpointServiceTests: XCTestCase {
    func testFreshMaterializeWritesAcceptedCheckpointAndCoversReplayedCommits() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0xA1)
        try await writeAddCommit(client: client, seq: 2, clock: 2, assetByte: 0xA2)
        let beforeFiles = await client.snapshotFiles()

        let result = try await service(
            client: client,
            clock: LamportClock(initial: 0),
            policy: policy(checkpointCommitThreshold: 1)
        ).checkpointMonth(month, mode: .whenRecommended, respectTaskCancellation: true)

        XCTAssertEqual(result.outcome, .writtenAccepted)
        XCTAssertEqual(result.covered, covered([(1, 2)]))
        let name = try XCTUnwrap(result.snapshotName)
        let accepted = try XCTUnwrap(result.acceptedSnapshot)
        XCTAssertEqual(accepted.filename, name)
        XCTAssertEqual(accepted.covered, covered([(1, 2)]))
        XCTAssertEqual(result.afterReport?.replayedSinceCheckpointCommitCount, 0)
        let afterCommits = await commitFiles(client)
        let afterRetention = await retentionFiles(client)
        XCTAssertEqual(afterCommits, beforeFiles.filter { $0.key.contains("/.watermelon/commits/") })
        XCTAssertEqual(afterRetention.count, 0)
    }

    func testPriorAcceptedSnapshotIsExtendedWithReplayedCommitCoverage() async throws {
        let client = try await makeClient()
        try await writeSnapshot(client: client, lamport: 10, covered: covered([(1, 5)]), assetBytes: [])
        try await writeAddCommit(client: client, seq: 6, clock: 6, assetByte: 0xB1)

        let result = try await service(client: client, clock: LamportClock(initial: 0))
            .checkpointMonth(month, mode: .force, respectTaskCancellation: true)

        XCTAssertEqual(result.outcome, .writtenAccepted)
        XCTAssertEqual(result.lamport, 11)
        XCTAssertEqual(result.covered, covered([(1, 6)]))
        XCTAssertEqual(result.acceptedSnapshot?.covered, covered([(1, 6)]))
        XCTAssertEqual(result.afterReport?.replayedSinceCheckpointCommitCount, 0)
    }

    func testClockObservesMaterializedClockBeforeTicking() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 100, assetByte: 0xC1)
        let clock = RecordingCheckpointClock(initial: 1)

        let result = try await service(client: client, clock: clock)
            .checkpointMonth(month, mode: .force, respectTaskCancellation: true)

        let events = await clock.events
        XCTAssertEqual(events, [.observe(100), .tick(1)])
        XCTAssertEqual(result.lamport, 101)
        XCTAssertEqual(result.acceptedSnapshot?.lamport, 101)
    }

    func testReadbackVerificationFailureDoesNotReportAcceptedCheckpoint() async throws {
        let inner = try await makeClient()
        try await writeAddCommit(client: inner, seq: 1, clock: 1, assetByte: 0xD1)
        let finalPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 2, writerID: writerID, runID: runID)
        let client = CheckpointHookClient(inner: inner)
        client.corruptFinalSnapshotDownload(path: finalPath, afterSuccessfulDownloads: 0)
        let beforeCommits = await commitFiles(inner)

        do {
            _ = try await service(client: client, clock: LamportClock(initial: 0))
                .checkpointMonth(month, mode: .force, respectTaskCancellation: true)
            XCTFail("expected readback mismatch")
        } catch RepoCheckpointError.readbackMismatch(let name, _) {
            XCTAssertEqual(name, RepoLayout.snapshotFileName(month: month, lamport: 2, writerID: writerID, runID: runID))
        }

        let afterCommits = await commitFiles(inner)
        XCTAssertEqual(afterCommits, beforeCommits)
        let output = try await RepoMaterializer(client: inner, basePath: basePath).materializeMonth(month, expectedRepoID: repoID)
        XCTAssertNotNil(output.state.months[month]?.assets[TestFixtures.assetFingerprint(0xD1)])
    }

    func testPostWriteHigherSnapshotCausesNotAcceptedAfterWrite() async throws {
        let inner = try await makeClient()
        try await writeAddCommit(client: inner, seq: 1, clock: 1, assetByte: 0xE1)
        let finalPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 2, writerID: writerID, runID: runID)
        let peerName = RepoLayout.snapshotFileName(month: month, lamport: 999, writerID: writerB, runID: peerRunID)
        let peerPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 999, writerID: writerB, runID: peerRunID)
        let peerBytes = makeSnapshotBytes(
            writerID: writerB,
            covered: covered([(1, UInt64(0xE1))]),
            assetBytes: [0xE1]
        )
        let client = CheckpointHookClient(inner: inner)
        client.afterFinalSnapshotReadback(path: finalPath, afterSuccessfulDownloads: 0) {
            await inner.injectFile(path: peerPath, data: peerBytes)
        }

        do {
            _ = try await service(client: client, clock: LamportClock(initial: 0))
                .checkpointMonth(month, mode: .force, respectTaskCancellation: true)
            XCTFail("expected notAcceptedAfterWrite")
        } catch RepoCheckpointError.notAcceptedAfterWrite(let name) {
            XCTAssertEqual(name, RepoLayout.snapshotFileName(month: month, lamport: 2, writerID: writerID, runID: runID))
        }

        let output = try await RepoMaterializer(client: inner, basePath: basePath).materializeMonth(month, expectedRepoID: repoID)
        XCTAssertEqual(output.acceptedSnapshotBaselinesByMonth[month]?.filename, peerName)
        let retention = await retentionFiles(inner)
        XCTAssertEqual(retention.count, 0)
    }

    func testSkipForceAndRepairCorruptBaselinePaths() async throws {
        let belowClient = try await makeClient()
        try await writeAddCommit(client: belowClient, seq: 1, clock: 1, assetByte: 0xF1)
        let beforeBelow = await belowClient.snapshotFiles()
        let skipped = try await service(
            client: belowClient,
            clock: LamportClock(initial: 0),
            policy: policy(checkpointCommitThreshold: 10)
        ).checkpointMonth(month, mode: .whenRecommended, respectTaskCancellation: true)
        XCTAssertEqual(skipped.outcome, .skippedBelowThreshold)
        let afterBelow = await belowClient.snapshotFiles()
        XCTAssertEqual(afterBelow, beforeBelow)

        let forced = try await service(
            client: belowClient,
            clock: LamportClock(initial: 0),
            policy: policy(checkpointCommitThreshold: 10)
        ).checkpointMonth(month, mode: .force, respectTaskCancellation: true)
        XCTAssertEqual(forced.outcome, .writtenAccepted)

        let emptyClient = try await makeClient()
        let empty = try await service(client: emptyClient, clock: LamportClock(initial: 0))
            .checkpointMonth(month, mode: .force, respectTaskCancellation: true)
        XCTAssertEqual(empty.outcome, .skippedEmptyFold)
        let emptyFiles = await emptyClient.snapshotFiles()
        XCTAssertEqual(emptyFiles.filter { $0.key.contains("/.watermelon/snapshots/") }, [:])

        let repairClient = try await makeClient()
        await repairClient.injectFile(
            path: RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 5, writerID: writerB, runID: peerRunID),
            data: Data("not-jsonl\n".utf8)
        )
        try await writeAddCommit(client: repairClient, seq: 1, clock: 1, assetByte: 0xF2)
        let repair = try await service(client: repairClient, clock: LamportClock(initial: 0))
            .checkpointMonth(month, mode: .repairCorruptBaseline, respectTaskCancellation: true)
        XCTAssertEqual(repair.outcome, .writtenAccepted)
    }

    func testCheckpointOnlyMaterializePreservesSemanticRows() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 10, assetByte: 0x61, includeResource: true)
        try await writeTombstoneCommit(client: client, seq: 2, clock: 20, assetByte: 0x62)
        let before = try await RepoMaterializer(client: client, basePath: basePath).materializeMonth(month, expectedRepoID: repoID)

        let result = try await service(client: client, clock: LamportClock(initial: 0))
            .checkpointMonth(month, mode: .force, respectTaskCancellation: true)
        XCTAssertEqual(result.outcome, .writtenAccepted)

        for path in await commitFiles(client).keys {
            try await client.delete(path: path)
        }
        let after = try await RepoMaterializer(client: client, basePath: basePath).materializeMonth(month, expectedRepoID: repoID)
        XCTAssertTrue(RepoRetentionEquivalence.matches(before, after, month: month, mode: .retentionSuperset))
        XCTAssertNotNil(after.state.months[month]?.resources["2026/05/asset-61.jpg"])
        XCTAssertTrue(after.state.months[month]?.deletedAssetStamps.keys.contains(TestFixtures.assetFingerprint(0x62)) == true)
    }

    func testCancellationPropagatesUnwrappedFromWriterAndReaderSurfaces() async throws {
        let writerInner = try await makeClient()
        try await writeAddCommit(client: writerInner, seq: 1, clock: 1, assetByte: 0x71)
        let writerClient = CheckpointHookClient(inner: writerInner)
        writerClient.cancelNextAtomicCreate()

        do {
            _ = try await service(client: writerClient, clock: LamportClock(initial: 0))
                .checkpointMonth(month, mode: .force, respectTaskCancellation: true)
            XCTFail("expected CancellationError")
        } catch is CancellationError {
        }

        let readerInner = try await makeClient()
        try await writeAddCommit(client: readerInner, seq: 1, clock: 1, assetByte: 0x72)
        let finalPath = RepoLayout.snapshotFilePath(base: basePath, month: month, lamport: 2, writerID: writerID, runID: runID)
        let readerClient = CheckpointHookClient(inner: readerInner)
        readerClient.cancelFinalSnapshotDownload(path: finalPath, afterSuccessfulDownloads: 0)

        do {
            _ = try await service(client: readerClient, clock: LamportClock(initial: 0))
                .checkpointMonth(month, mode: .force, respectTaskCancellation: true)
            XCTFail("expected CancellationError")
        } catch is CancellationError {
        }
    }

    func testForeignAndCorruptMetadataRemainOutsideCheckpointCoverage() async throws {
        let client = try await makeClient()
        try await writeAddCommit(client: client, seq: 1, clock: 1, assetByte: 0x81)
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: "99999999-9999-9999-9999-999999999999",
                writerID: writerID,
                seq: 9,
                runID: runID,
                month: month,
                clockMin: 9,
                clockMax: 9
            ),
            ops: [],
            month: month,
            respectTaskCancellation: true
        )
        await client.injectFile(
            path: RepoLayout.commitFilePath(base: basePath, month: month, writerID: writerID, seq: 99),
            data: Data("bad\n".utf8)
        )

        let result = try await service(client: client, clock: LamportClock(initial: 0))
            .checkpointMonth(month, mode: .force, respectTaskCancellation: true)

        XCTAssertEqual(result.covered, covered([(1, 1)]))
        XCTAssertEqual(result.afterReport?.notCheckpointCoveredCommitCount, 2)
        XCTAssertEqual(result.afterReport?.protectedUnparseableFilenameCount, 0)
    }

    func testCheckpointServiceHasNoInlineRetentionOrDeletePrimitive() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let serviceURL = root.appendingPathComponent("Shared/Services/Repo/RepoCheckpointService.swift")
        let serviceSource = try String(contentsOf: serviceURL)
        XCTAssertFalse(serviceSource.contains("RetentionManifest"))
        XCTAssertFalse(serviceSource.contains(".watermelon/retention"))
        XCTAssertFalse(serviceSource.contains("delete("))

        let sourceFiles = try FileManager.default.subpathsOfDirectory(atPath: root.path)
            .filter { $0.hasSuffix(".swift") }
            .filter { path in
                !path.hasSuffix("RepoCheckpointService.swift")
                    && !path.hasSuffix("RepoCheckpointServiceTests.swift")
                    && !path.hasSuffix("RepoCheckpointBarrierHook.swift")
                    && !path.hasSuffix("RepoCheckpointBarrierHookTests.swift")
                    && !path.hasSuffix("RetentionMaintenanceOrchestratorTests.swift")
                    && !path.hasSuffix("RepoMaintenanceCoordinator.swift")
                    && !path.hasSuffix("RepoMaintenanceCoordinatorTests.swift")
            }
        for path in sourceFiles {
            let text = try String(contentsOf: root.appendingPathComponent(path))
            XCTAssertFalse(text.contains("RepoCheckpointService"), "unexpected runtime reference in \(path)")
        }
    }

    private func service(
        client: any RemoteStorageClientProtocol,
        clock: any RepoCheckpointClock,
        policy: RepoCompactionPolicy = RepoCheckpointServiceTests.policy()
    ) -> RepoCheckpointService {
        RepoCheckpointService(
            client: client,
            basePath: basePath,
            repoID: repoID,
            writerID: writerID,
            runID: runID,
            clock: clock,
            policy: policy
        )
    }

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerID)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerID)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        return client
    }

    private func writeAddCommit(
        client: any RemoteStorageClientProtocol,
        seq: UInt64,
        clock: UInt64,
        assetByte: UInt8,
        includeResource: Bool = false
    ) async throws {
        let assetFP = TestFixtures.assetFingerprint(assetByte)
        let resources: [CommitResourceEntry]
        if includeResource {
            let hash = TestFixtures.fingerprint(assetByte &+ 1)
            resources = [CommitResourceEntry(
                physicalRemotePath: String(format: "2026/05/asset-%02x.jpg", assetByte),
                logicalName: "asset.jpg",
                contentHash: hash,
                fileSize: 100,
                resourceType: ResourceTypeCode.photo,
                role: ResourceTypeCode.photo,
                slot: 0,
                crypto: nil
            )]
        } else {
            resources = []
        }
        let op = CommitOp(opSeq: 0, clock: clock, body: .addAsset(CommitAddAssetBody(
            assetFingerprint: assetFP,
            creationDateMs: nil,
            backedUpAtMs: Int64(clock),
            resources: resources
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
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

    private func writeTombstoneCommit(
        client: any RemoteStorageClientProtocol,
        seq: UInt64,
        clock: UInt64,
        assetByte: UInt8
    ) async throws {
        let op = CommitOp(opSeq: 0, clock: clock, body: .tombstoneAsset(CommitTombstoneBody(
            assetFingerprint: TestFixtures.assetFingerprint(assetByte),
            reason: .verifyFailed
        )))
        _ = try await CommitLogWriter(client: client, basePath: basePath).write(
            header: TestFixtures.makeCommitHeader(
                repoID: repoID,
                writerID: writerID,
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
        covered: CoveredRanges,
        assetBytes: [UInt8]
    ) async throws {
        let state = monthState(assetBytes: assetBytes)
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
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
    }

    private func makeSnapshotBytes(writerID: String, covered: CoveredRanges, assetBytes: [UInt8]) -> Data {
        let header = SnapshotHeader(
            version: SnapshotHeader.currentVersion,
            scope: CommitHeader.monthScope(month),
            writerID: writerID,
            repoID: repoID,
            covered: covered
        )
        let parts = RepoSnapshotBuilder.build(header: header, state: monthState(assetBytes: assetBytes))
        var integrity = IntegrityAccumulator()
        var lines: [String] = []
        let headerLine = try! SnapshotRowMapper.encodeHeaderLine(header)
        lines.append(headerLine)
        integrity.absorbLine(headerLine)
        for row in parts.assets.sorted(by: { $0.assetFingerprint.rawValue.lexicographicallyPrecedes($1.assetFingerprint.rawValue) }) {
            let line = try! SnapshotRowMapper.encodeAssetLine(row)
            lines.append(line)
            integrity.absorbLine(line)
        }
        for row in parts.resources.sorted(by: { $0.physicalRemotePath < $1.physicalRemotePath }) {
            let line = try! SnapshotRowMapper.encodeResourceLine(row)
            lines.append(line)
            integrity.absorbLine(line)
        }
        for row in parts.assetResources.sorted(by: { lhs, rhs in
            if lhs.assetFingerprint != rhs.assetFingerprint {
                return lhs.assetFingerprint.rawValue.lexicographicallyPrecedes(rhs.assetFingerprint.rawValue)
            }
            if lhs.role != rhs.role { return lhs.role < rhs.role }
            return lhs.slot < rhs.slot
        }) {
            let line = try! SnapshotRowMapper.encodeAssetResourceLine(row)
            lines.append(line)
            integrity.absorbLine(line)
        }
        for row in parts.deletedKeys.sorted(by: { $0.keyValue < $1.keyValue }) {
            let line = try! SnapshotRowMapper.encodeDeletedKeyLine(row)
            lines.append(line)
            integrity.absorbLine(line)
        }
        lines.append(try! SnapshotRowMapper.encodeEndLine(sha256Hex: integrity.finalize(), rowCount: integrity.rowCount))
        return Data((lines.joined(separator: "\n") + "\n").utf8)
    }

    private func monthState(assetBytes: [UInt8]) -> RepoMonthState {
        var state = RepoMonthState.empty
        for byte in assetBytes {
            let fp = TestFixtures.assetFingerprint(byte)
            state.assets[fp] = SnapshotAssetRow(
                assetFingerprint: fp,
                creationDateMs: nil,
                backedUpAtMs: Int64(byte),
                resourceCount: 0,
                totalFileSizeBytes: 0,
                stamp: OpStamp(writerID: writerID, seq: UInt64(byte), clock: UInt64(byte))
            )
        }
        return state
    }

    private func covered(_ ranges: [(UInt64, UInt64)]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: [
            writerID: ranges.map { ClosedSeqRange(low: $0.0, high: $0.1) }
        ])
    }

    private func commitFiles(_ client: InMemoryRemoteStorageClient) async -> [String: Data] {
        await client.snapshotFiles().filter { $0.key.contains("/.watermelon/commits/") }
    }

    private func retentionFiles(_ client: InMemoryRemoteStorageClient) async -> [String: Data] {
        await client.snapshotFiles().filter { $0.key.contains("/.watermelon/retention/") }
    }

    private static func policy(checkpointCommitThreshold: Int = 1) -> RepoCompactionPolicy {
        RepoCompactionPolicy(
            checkpointCommitThreshold: checkpointCommitThreshold,
            checkpointByteThreshold: Int64.max,
            retentionStalenessThresholdSeconds: 86_400,
            snapshotFallbackKeepCount: 2
        )
    }

    private func policy(checkpointCommitThreshold: Int = 1) -> RepoCompactionPolicy {
        Self.policy(checkpointCommitThreshold: checkpointCommitThreshold)
    }

    private let basePath = "/repo"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let writerID = "11111111-1111-1111-1111-111111111111"
    private let writerB = "22222222-2222-2222-2222-222222222222"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let peerRunID = "44444444-4444-4444-4444-444444444444"
    private let month = LibraryMonthKey(year: 2026, month: 5)
}

private actor RecordingCheckpointClock: RepoCheckpointClock {
    enum Event: Equatable {
        case observe(UInt64)
        case tick(Int)
    }

    private var current: UInt64
    private(set) var events: [Event] = []

    init(initial: UInt64) {
        self.current = initial
    }

    func observeForCheckpoint(_ external: UInt64) async throws {
        events.append(.observe(external))
        current = max(current, external)
    }

    func tickRangeForCheckpoint(count: Int) async throws -> LamportClock.Range {
        events.append(.tick(count))
        let low = current + 1
        current += UInt64(count)
        return LamportClock.Range(low: low, high: current)
    }
}

private final class CheckpointHookClient: @unchecked Sendable, RemoteStorageClientProtocol {
    private struct DownloadHook {
        var path: String
        var afterSuccessfulDownloads: Int
        var kind: Kind
        var count: Int = 0
        var fired: Bool = false

        enum Kind {
            case corrupt
            case cancel
            case callback(@Sendable () async -> Void)
        }
    }

    private let inner: InMemoryRemoteStorageClient
    private let lock = NSLock()
    private var hooks: [DownloadHook] = []
    private var cancelAtomicCreate = false

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }

    func corruptFinalSnapshotDownload(path: String, afterSuccessfulDownloads: Int) {
        appendHook(path: path, afterSuccessfulDownloads: afterSuccessfulDownloads, kind: .corrupt)
    }

    func cancelFinalSnapshotDownload(path: String, afterSuccessfulDownloads: Int) {
        appendHook(path: path, afterSuccessfulDownloads: afterSuccessfulDownloads, kind: .cancel)
    }

    func afterFinalSnapshotReadback(
        path: String,
        afterSuccessfulDownloads: Int,
        _ callback: @escaping @Sendable () async -> Void
    ) {
        appendHook(path: path, afterSuccessfulDownloads: afterSuccessfulDownloads, kind: .callback(callback))
    }

    func cancelNextAtomicCreate() {
        lock.lock()
        cancelAtomicCreate = true
        lock.unlock()
    }

    private func appendHook(path: String, afterSuccessfulDownloads: Int, kind: DownloadHook.Kind) {
        lock.lock()
        hooks.append(DownloadHook(path: normalize(path), afterSuccessfulDownloads: afterSuccessfulDownloads, kind: kind))
        lock.unlock()
    }

    func shouldSetModificationDate() -> Bool { true }
    func shouldLimitUploadRetries(for error: Error) -> Bool { false }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .exclusive }
    var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    var supportsLivenessSafeOverwriteUpload: Bool { false }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var isSerialized: Bool { false }

    func upload(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws {
        try await inner.upload(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func atomicCreate(
        localURL: URL,
        remotePath: String,
        respectTaskCancellation: Bool,
        onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        lock.lock()
        let shouldCancel = cancelAtomicCreate
        if cancelAtomicCreate { cancelAtomicCreate = false }
        lock.unlock()
        if shouldCancel { throw CancellationError() }
        return try await inner.atomicCreate(
            localURL: localURL,
            remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation,
            onProgress: onProgress
        )
    }

    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }

    func download(remotePath: String, localURL: URL) async throws {
        let path = normalize(remotePath)
        let action = nextDownloadAction(path: path)
        if case .cancel = action { throw CancellationError() }
        try await inner.download(remotePath: remotePath, localURL: localURL)
        switch action {
        case .none:
            return
        case .corrupt:
            try Data("corrupt snapshot readback\n".utf8).write(to: localURL, options: .atomic)
        case .callback(let callback):
            await callback()
        case .cancel:
            return
        }
    }

    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool { true }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private enum DownloadAction {
        case none
        case corrupt
        case cancel
        case callback(@Sendable () async -> Void)
    }

    private func nextDownloadAction(path: String) -> DownloadAction {
        lock.lock()
        defer { lock.unlock() }
        for index in hooks.indices where hooks[index].path == path && !hooks[index].fired {
            hooks[index].count += 1
            if hooks[index].count > hooks[index].afterSuccessfulDownloads {
                switch hooks[index].kind {
                case .corrupt:
                    return .corrupt
                case .cancel:
                    hooks[index].fired = true
                    return .cancel
                case .callback(let callback):
                    hooks[index].fired = true
                    return .callback(callback)
                }
            }
        }
        return .none
    }

    private func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

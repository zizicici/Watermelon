import XCTest
import GRDB
@testable import Watermelon

// Integration coverage for the extracted finalizeMonth seam — the report's top-priority "final flush" path —
// driven with real methods + a fake remote (no photo library). Covers: a transient flush fault → reconnect →
// commit (no orphans); a sustained outage → exhaustion (month left uncommitted, sentinel-fatal so the worker
// settles a resumable pause); and a preset month fatal → skip the flush entirely.
final class MonthFlushRecoveryTests: XCTestCase {
    private let basePath = "/photos"
    private let year = 2024
    private let month = 3
    private var dbDir: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        dbDir = FileManager.default.temporaryDirectory.appendingPathComponent("WM-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dbDir, withIntermediateDirectories: true)
        databaseManager = try DatabaseManager(databaseURL: dbDir.appendingPathComponent("t.sqlite"))
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let dbDir { try? FileManager.default.removeItem(at: dbDir) }
    }

    private func makeExecutor(remoteIndex: RemoteIndexSyncService = RemoteIndexSyncService()) -> BackupParallelExecutor {
        let hashRepo = ContentHashIndexRepository(databaseManager: databaseManager)
        let assetProcessor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: hashRepo,
            remoteIndexService: remoteIndex
        )
        return BackupParallelExecutor(
            hashIndexRepository: hashRepo,
            assetProcessor: assetProcessor,
            remoteIndexService: remoteIndex
        )
    }

    private func makeStore(client: RemoteStorageClientProtocol) throws -> MonthManifestStore {
        let localURL = MonthManifestStore.makeLocalManifestURL(year: year, month: month)
        try? FileManager.default.removeItem(at: localURL)
        let queue = try DatabaseQueue(path: localURL.path)
        try MonthManifestStore.migrate(queue)
        return MonthManifestStore(
            client: client, basePath: basePath, year: year, month: month,
            localManifestURL: localURL, dbQueue: queue, remoteFilesByName: [:], dirty: false,
            layout: .lite, liteWriteOwnership: {}
        )
    }

    private func makeWriteMode() -> RepoWriteMode {
        let lock = WriteLockService(basePath: basePath, writerID: UUID().uuidString.lowercased(), client: InMemoryRemoteStorageClient())!
        return RepoWriteMode.lite(RepoLeaseSession(lock: lock), nil)
    }

    private func profile() -> ServerProfileRecord {
        ServerProfileRecord(
            id: nil, name: "p", storageType: StorageType.s3.rawValue, connectionParams: nil, sortOrder: 0,
            host: "h", port: 0, shareName: "s", basePath: basePath, username: "u", domain: nil,
            credentialRef: "r", backgroundBackupEnabled: false, createdAt: Date(), updatedAt: Date(), writerID: nil
        )
    }

    private var networkLost: Error { NSError(domain: NSURLErrorDomain, code: NSURLErrorNetworkConnectionLost) }

    private func finalize(
        store: MonthManifestStore,
        pool: StorageClientPool,
        state: BackupParallelExecutor.MonthFinalizeState,
        recoveryWindow: TimeInterval,
        loadedSnapshot: (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink])? = nil,
        eventStream: BackupEventStream = BackupEventStream(),
        remoteIndex: RemoteIndexSyncService = RemoteIndexSyncService()
    ) async -> BackupParallelExecutor.MonthFinalizeDisposition {
        await makeExecutor(remoteIndex: remoteIndex).finalizeMonth(
            monthStore: store,
            monthKey: LibraryMonthKey(year: year, month: month),
            loadedSnapshot: loadedSnapshot ?? store.unsortedSnapshot(),
            monthDirtyAssetIDs: ["asset-a"],
            monthProgressCounts: BackupMonthProgressCounts(),
            incrementalFlushInterval: nil,
            recoveryWindow: recoveryWindow,
            writeMode: makeWriteMode(),
            clientPool: pool,
            monthQueue: MonthWorkQueue(months: []),
            profile: profile(),
            eventStream: eventStream,
            aggregator: ParallelBackupProgressAggregator(total: 1),
            onMonthUploaded: nil,
            workerID: 0,
            state: state
        )
    }

    // Drains the (finished) event stream and returns the un-mark event's payload, if any.
    private func resumeUnmark(in eventStream: BackupEventStream) async -> (ids: Set<String>, failedCount: Int)? {
        eventStream.finish()
        for await event in eventStream.stream {
            if case .monthChanged(let change) = event,
               case .uploadFailed(let ids, let count) = change.action {
                return (ids, count)
            }
        }
        return nil
    }

    private func state(client: RemoteStorageClientProtocol, monthFatalError: Error? = nil) -> BackupParallelExecutor.MonthFinalizeState {
        BackupParallelExecutor.MonthFinalizeState(
            client: client, clientReusable: true, recoveryDeadline: nil, run: WorkerRunState(), monthFatalError: monthFatalError
        )
    }

    func testTransientFlushFaultRecoversAndCommits() async throws {
        let broken = InMemoryRemoteStorageClient()
        await broken.enqueueUploadError(networkLost)   // first manifest upload trips
        let store = try makeStore(client: broken)
        try store.upsertResource(TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg"))
        XCTAssertTrue(store.dirty)

        let healthy = InMemoryRemoteStorageClient()
        let pool = StorageClientPool(maxConnections: 1) { healthy }
        let s = state(client: broken)

        let disposition = await finalize(store: store, pool: pool, state: s, recoveryWindow: 10)

        guard case .proceed = disposition else { return XCTFail("expected .proceed, got \(disposition)") }
        XCTAssertNil(s.monthFatalError, "a recovered flush is not fatal")
        XCTAssertTrue(s.clientReusable)
        XCTAssertFalse(store.dirty, "the month manifest must commit after recovery — no orphans")
    }

    func testSustainedOutageExhaustsAndLeavesMonthUncommitted() async throws {
        let broken = InMemoryRemoteStorageClient()
        await broken.enqueueUploadError(networkLost)
        let store = try makeStore(client: broken)
        try store.upsertResource(TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg"))

        // Reconnect never succeeds → recovery exhausts within a tight window.
        let pool = StorageClientPool(maxConnections: 1) { throw RemoteStorageClientError.unavailable }
        let s = state(client: broken)

        let disposition = await finalize(store: store, pool: pool, state: s, recoveryWindow: 0.25)

        guard case .proceed = disposition else { return XCTFail("expected .proceed (worker rethrows the fatal), got \(disposition)") }
        XCTAssertTrue(s.monthFatalError is BackupNetworkRecoveryExhausted, "exhaustion must set the resumable-pause sentinel")
        XCTAssertFalse(s.clientReusable)
        XCTAssertTrue(store.dirty, "an exhausted flush must leave the month uncommitted (no data dropped)")
    }

    // A reconnect that surfaces a cancellation-shaped fault (server RST → NSURLErrorCancelled) while the worker
    // task is NOT cancelled returns .cancelled. On a dirty final flush this must pause cleanly and un-mark the
    // month's uncommitted assets — NOT throw the raw network error (which the reducer would settle .failed,
    // leaving the assets resume-complete so resume skips them → orphans).
    func testCancellationShapedReconnectPausesUncommittedNotFailed() async throws {
        let broken = InMemoryRemoteStorageClient()
        await broken.enqueueUploadError(networkLost)   // first manifest flush trips
        let store = try makeStore(client: broken)
        let committedBaseline = store.unsortedSnapshot()   // last committed state — empty, before the dirty upsert
        let dirtyResource = TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0x77]), fileName: "d.jpg")
        try store.upsertResource(dirtyResource)
        XCTAssertTrue(store.dirty)

        // Mirror production's optimistic cache upsert during upload, so the rollback has something to revert.
        let remoteIndex = RemoteIndexSyncService()
        remoteIndex.upsertCachedResource(dirtyResource)

        // Every reconnect surfaces a cancellation-shaped fault → recoverWorkerConnection returns .cancelled.
        let pool = StorageClientPool(maxConnections: 1) { throw CancellationError() }
        let s = state(client: broken)
        let eventStream = BackupEventStream()

        // 30s window: the .cancelled verdict short-circuits on the first reconnect attempt, so a large window
        // costs only one backoff; a tight one risks a slow-CONNECT box flipping the outcome to .exhausted.
        let disposition = await finalize(
            store: store, pool: pool, state: s, recoveryWindow: 30,
            loadedSnapshot: committedBaseline, eventStream: eventStream, remoteIndex: remoteIndex
        )

        guard case .breakMonthLoop = disposition else {
            return XCTFail("a cancellation-shaped reconnect on a dirty flush must pause cleanly, got \(disposition)")
        }
        XCTAssertTrue(s.run.paused, "must pause, not fail")
        XCTAssertNil(s.monthFatalError, "a clean pause is not a fatal — the raw network error must not be thrown")
        XCTAssertFalse(s.clientReusable)
        XCTAssertTrue(store.dirty, "the month stays uncommitted (its assets are un-marked for resume)")
        // The uncommitted assets must be un-marked for resume (count 0 = pause, not a reported failure).
        let unmark = await resumeUnmark(in: eventStream)
        XCTAssertEqual(unmark?.ids, ["asset-a"], "must un-mark the month's uncommitted assets for resume")
        XCTAssertEqual(unmark?.failedCount, 0, "a clean pause un-marks without reporting failures")
        // The cache must roll back to the committed baseline (empty) — no orphaned uncommitted resource left.
        let cachedMonth = remoteIndex.remoteMonthRawData(for: LibraryMonthKey(year: year, month: month))
        XCTAssertEqual(cachedMonth?.resources.count ?? 0, 0, "a failed flush must roll the cache back to committed baseline")
    }

    func testPresetMonthFatalSkipsFlush() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client)
        try store.upsertResource(TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg"))
        XCTAssertTrue(store.dirty)

        // A month already doomed by a prior fatal must skip the flush entirely.
        let s = state(client: client, monthFatalError: BackupNetworkRecoveryExhausted(underlying: networkLost))
        let pool = StorageClientPool(maxConnections: 1) { client }

        let disposition = await finalize(store: store, pool: pool, state: s, recoveryWindow: 10)

        guard case .proceed = disposition else { return XCTFail("expected .proceed, got \(disposition)") }
        XCTAssertTrue(store.dirty, "a preset-fatal month must skip the flush")
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "no manifest upload may be attempted for a doomed month")
    }
}

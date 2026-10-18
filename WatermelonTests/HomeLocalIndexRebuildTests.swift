import XCTest
import Photos
import SQLite3
@testable import Watermelon

final class HomeLocalIndexRebuildTests: XCTestCase {
    func testFailedFullReadDoesNotConfirmRevisionAndNextReadRecoversWithoutAnotherCommit() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let worker = fixture.worker(input: input)
        let revision = worker._testIndexState(assetIDs: []).revision
        LocalIndexReadFailureInjection.interruptNextStatement(in: fixture.database)

        let failed = await worker.reloadLocalIndex(scope: .device(.all))
        XCTAssertTrue(failed.readFailed)
        XCTAssertEqual(worker._testIndexState(assetIDs: []).revision, revision)
        let failedSeed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertNil(failedSeed)
        let sourceRevision = fixture.repository.changes.currentRevision

        let retry = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        guard case .reloaded(let loaded) = retry else { return XCTFail("Expected a complete retry") }
        XCTAssertFalse(loaded.readFailed)
        XCTAssertEqual(fixture.repository.changes.currentRevision, sourceRevision)
        XCTAssertEqual(worker._testIndexState(assetIDs: []).revision, sourceRevision)
        let seed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertEqual(seed?.localIDByFingerprint[Fixture.currentFingerprint], "current")
    }

    func testFailedIncrementalReadPreservesFingerprintAndDoesNotConsumeCommit() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let worker = fixture.worker(input: input)
        _ = await worker.reloadLocalIndex(scope: .device(.all))
        let before = worker._testIndexState(assetIDs: ["current"])
        try fixture.database.write { db in
            try db.execute(sql: "UPDATE local_assets SET updatedAt = ? WHERE assetLocalIdentifier = 'current'", arguments: [Date()])
        }
        LocalIndexReadFailureInjection.interruptNextStatement(in: fixture.database)
        let result = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        guard case .failed = result else { return XCTFail("A read failure must remain distinguishable from empty data") }
        XCTAssertEqual(worker._testIndexState(assetIDs: ["current"]).revision, before.revision)
        XCTAssertEqual(worker._testIndexState(assetIDs: ["current"]).fingerprints, before.fingerprints)
        XCTAssertEqual(worker.allMonthRows().values.first?.local?.assetCount, 4, "Same-scope presentation can keep its last snapshot")
        XCTAssertEqual(worker.localAssetIDs(for: .init(year: 2024, month: 1), expectedScope: .device(.all)),
            ["edited", "invalidated", "current", "unindexed"], "Preflight must retain known members while a retry is pending")
        let seed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertNil(seed)
        do {
            _ = try await worker.remoteOnlyItems(for: .init(year: 2024, month: 1), expectedScope: .device(.all))
            XCTFail("An invalid local snapshot must not mean there are no downloads")
        } catch is HomeLocalIndexReadError { }
        let deferred = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all), allowFullReload: false)
        guard case .requiresReload = deferred else { return XCTFail("The failed commit must remain pending") }
        let recovered = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        guard case .reloaded(let loaded) = recovered else { return XCTFail("Expected full recovery") }
        XCTAssertFalse(loaded.readFailed)
        let recoveredSeed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertEqual(recoveredSeed?.localIDByFingerprint[Fixture.currentFingerprint], "current")
    }

    func testPhotoChangeReadFailureDoesNotPartiallyApplyMembership() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let worker = fixture.worker(input: input)
        _ = await worker.reloadLocalIndex(scope: .device(.all))
        let before = worker._testIndexState(assetIDs: ["current"])
        let added = TestFixtures.snapshot(id: "added", year: 2024, month: 2)
        LocalIndexReadFailureInjection.interruptNextStatement(in: fixture.database)
        let result = await worker._testApplyPhotoChange(
            TestFixtures.changePayload([TestFixtures.incrementalChange(at: 0, removed: ["current"], inserted: [added])]),
            snapshots: [added])
        XCTAssertTrue(result.readFailed)
        XCTAssertEqual(worker._testIndexState(assetIDs: ["current"]).fingerprints, before.fingerprints)
        XCTAssertEqual(worker.allMonthRows().values.first?.local?.assetCount, 4)
        let seed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertNil(seed)
        input.setPayload(TestFixtures.initialPayload([[added]]))
        _ = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        let recovered = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertEqual(recovered?.assets.map(\.localIdentifier), ["added"])
    }

    func testFailedScopeSwitchCannotExposePreviousScopeAndAuthorizationLossStillClearsIt() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let worker = fixture.worker(input: input)
        _ = await worker.reloadLocalIndex(scope: .device(.all))
        LocalIndexReadFailureInjection.interruptNextStatement(in: fixture.database)
        let failed = await worker.reloadLocalIndex(scope: .device(.videos))
        XCTAssertTrue(failed.readFailed)
        XCTAssertEqual(failed.changedMonths, [.init(year: 2024, month: 1)])
        XCTAssertTrue(worker.allMonthRows().isEmpty)
        XCTAssertTrue(worker.localAssetIDs(for: .init(year: 2024, month: 1), expectedScope: .device(.videos)).isEmpty)
        input.setPayload(nil)
        let denied = await worker.reloadLocalIndex(scope: .device(.videos))
        XCTAssertFalse(denied.readFailed)
        XCTAssertFalse(denied.isAuthorized)
        XCTAssertTrue(worker.allMonthRows().isEmpty)
    }

    @MainActor
    func testManagerFailureDefersRetryAndOperationQueryThrowsUntilRecovery() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let manager = HomeIncrementalDataManager(photoLibraryService: PhotoLibraryService(),
            contentHashIndexRepository: fixture.repository,
            hooks: .init(remoteMonthSnapshot: { _ in nil }, currentScope: { .device(.all) }),
            fetchLocalLibrary: { input.fetch($0) })
        manager.setAppActive(false)
        await manager.reloadLocalIndex()
        XCTAssertFalse(manager.needsLocalIndexReload)
        var reloadRequests = 0
        manager.onReloadNeeded = { reloadRequests += 1 }
        LocalIndexReadFailureInjection.interruptNextStatement(in: fixture.database)
        await manager.reloadLocalIndex()
        XCTAssertTrue(manager.needsLocalIndexReload)
        XCTAssertEqual(reloadRequests, 0)
        let reads = input.queries.count
        await Task.yield()
        XCTAssertEqual(input.queries.count, reads, "A failed read must not schedule its own retry")

        LocalIndexReadFailureInjection.interruptNextStatement(in: fixture.database)
        do {
            _ = try await manager.remoteOnlyItems(for: .init(year: 2024, month: 1))
            XCTFail("A failed retry cannot become an empty successful download plan")
        } catch is HomeLocalIndexReadError { }
        XCTAssertTrue(manager.needsLocalIndexReload)
        XCTAssertEqual(reloadRequests, 0)
        _ = try await manager.remoteOnlyItems(for: .init(year: 2024, month: 1))
        XCTAssertFalse(manager.needsLocalIndexReload)
    }

    func testExpiredHistoryUsesFullReloadValidationAndRevisionBarrier() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let worker = fixture.worker(input: input)
        let initial = await worker.reloadLocalIndex(scope: .device(.all))
        XCTAssertEqual(initial.fingerprintValidationAssetIDs, ["invalidated"])

        input.setPayload(fixture.payload(edited: true))
        try fixture.expireHistory(prefix: "first")
        let result = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        guard case .reloaded(let rebuilt) = result else {
            return XCTFail("Expired history must produce the complete reload result")
        }
        XCTAssertTrue(rebuilt.isAuthorized)
        XCTAssertEqual(rebuilt.fingerprintValidationAssetIDs, ["edited", "invalidated"])
        XCTAssertEqual(rebuilt.monthGroupingTimeZone, .frozenCurrent())
        let currentSeed = await worker.browserLocalSeed(expectedScope: .device(.all))
        let seed = try XCTUnwrap(currentSeed)
        XCTAssertNil(seed.assets.first { $0.localIdentifier == "edited" }?.fingerprint)
        XCTAssertEqual(seed.localIDByFingerprint[Fixture.currentFingerprint], "current")
        XCTAssertEqual(input.queries, [.library(.all), .library(.all)])

        let explicit = await worker.reloadLocalIndex(scope: .device(.all))
        XCTAssertEqual(explicit.fingerprintValidationAssetIDs, rebuilt.fingerprintValidationAssetIDs)
        XCTAssertEqual(explicit.changedMonths, rebuilt.changedMonths)
        XCTAssertEqual(explicit.monthGroupingTimeZone, rebuilt.monthGroupingTimeZone)
    }

    func testCommitDuringBulkFetchRemainsUnconsumed() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let repository = fixture.repository
        let worker = HomeDataProcessingWorker(photoLibraryService: PhotoLibraryService(),
            contentHashIndexRepository: repository, remoteMonthSnapshot: { _ in nil },
            fetchLocalLibrary: { query in
                let result = input.fetch(query)
                if input.queries.count == 2 {
                    do {
                        try repository.upsertAssetFingerprint(assetLocalIdentifier: "late-commit",
                            assetFingerprint: Data([9]), resourceCount: 1, totalFileSizeBytes: 1, modificationDateMs: nil)
                    } catch {
                        XCTFail("Late commit failed: \(error)")
                    }
                }
                return result
            })
        _ = await worker.reloadLocalIndex(scope: .device(.all))
        try fixture.expireHistory(prefix: "bulk")
        let result = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        guard case .reloaded = result else { return XCTFail("Expected bulk reload") }
        let seed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertNil(seed, "A commit after the reload's captured revision must remain pending")
    }

    @MainActor
    func testBulkResultReplaysValidationTimeZoneSizesSeedAndAuthorizationEffects() async throws {
        let savedTimeZone = UserDefaults.standard.string(forKey: MonthGroupingTimeZonePreference.storageKey)
        defer {
            if let savedTimeZone {
                UserDefaults.standard.set(savedTimeZone, forKey: MonthGroupingTimeZonePreference.storageKey)
            } else {
                UserDefaults.standard.removeObject(forKey: MonthGroupingTimeZonePreference.storageKey)
            }
        }
        MonthGroupingTimeZonePreference.setCurrent(.fixedUTC())
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let manager = HomeIncrementalDataManager(photoLibraryService: PhotoLibraryService(),
            contentHashIndexRepository: fixture.repository,
            hooks: .init(remoteMonthSnapshot: { _ in nil }, currentScope: { .device(.all) }),
            fetchLocalLibrary: { input.fetch($0) })
        defer { manager.fileSizeCoordinator.reset() }
        var validations: [Set<String>] = []
        manager.onFingerprintValidationNeeded = { validations.append($0) }
        let seeds = Counter()
        let token = manager.browserLocalSeedChanges.addObserver { seeds.increment() }
        defer { manager.browserLocalSeedChanges.removeObserver(token) }
        var sizeUpdates: [Set<LibraryMonthKey>] = []
        var sizeCompletion: XCTestExpectation? = expectation(description: "Initial full size scan")
        let initialSizes = try XCTUnwrap(sizeCompletion)
        manager.fileSizeCoordinator.onFileSizesUpdated = {
            sizeUpdates.append($0)
            sizeCompletion?.fulfill()
            sizeCompletion = nil
        }
        await manager.reloadLocalIndex()
        await fulfillment(of: [initialSizes], timeout: 3)
        XCTAssertTrue(manager._testIsObservingPhotoLibrary)
        XCTAssertEqual(validations, [["invalidated"]])
        XCTAssertEqual(seeds.value, 1)
        XCTAssertEqual(manager.monthGroupingTimeZoneForLocalIndex(), .fixedUTC())

        let shiftedZone = MonthGroupingTimeZonePreference(mode: .fixedOffset, offsetSeconds: -8 * 3_600)
        MonthGroupingTimeZonePreference.setCurrent(shiftedZone)
        input.setPayload(fixture.payload(edited: true))
        try fixture.expireHistory(prefix: "first")
        let rebuiltSizes = expectation(description: "Bulk full size scan")
        sizeCompletion = rebuiltSizes
        let months = await manager.refreshLocalIndex(forAssetIDs: [])
        await fulfillment(of: [rebuiltSizes], timeout: 3)
        let january = LibraryMonthKey(year: 2024, month: 1)
        let december = LibraryMonthKey(year: 2023, month: 12)
        XCTAssertEqual(months, [january, december])
        XCTAssertEqual(validations.last, ["edited", "invalidated"])
        XCTAssertEqual(seeds.value, 2)
        XCTAssertEqual(manager.monthGroupingTimeZoneForLocalIndex(), shiftedZone)
        XCTAssertEqual(sizeUpdates, [[january], [december]], "A bulk rebuild must restart the full scan over current months")
        XCTAssertEqual(manager.monthRow(for: december).local?.totalSizeBytes, 0)
        XCTAssertNil(manager.monthRow(for: january).local)
        let seed = await manager.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertEqual(seed?.monthGroupingTimeZone, shiftedZone)

        input.setPayload(nil)
        try fixture.expireHistory(prefix: "denied")
        let removed = await manager.refreshLocalIndex(forAssetIDs: [])
        XCTAssertEqual(removed, [december])
        XCTAssertFalse(manager._testIsObservingPhotoLibrary)
        XCTAssertTrue(manager.allMonthRows().isEmpty)
        XCTAssertEqual(seeds.value, 3)
        XCTAssertEqual(validations.count, 2)
    }

    func testBackgroundPhotoChangesInvalidateSeedAndCoalesceUntilExplicitRead() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let worker = fixture.worker(input: input)
        _ = await worker.reloadLocalIndex(scope: .device(.all))
        let month = LibraryMonthKey(year: 2024, month: 1)
        let sample = await worker.sampleFileSizeScan(for: month)
        worker.setAppActive(false)
        input.setPayload(fixture.payload(edited: true))
        for _ in 0..<3 {
            let result = await photoChange(worker)
            XCTAssertTrue(result.requiresReload)
        }
        XCTAssertEqual(input.queries.count, 1)
        let seed = await worker.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertNil(seed)
        let deferred = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all), allowFullReload: false)
        guard case .requiresReload = deferred else { return XCTFail("Blocked refresh must retain deferred changes") }
        XCTAssertEqual(input.queries.count, 1)
        let rebuilt = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        guard case .reloaded(let load) = rebuilt else { return XCTFail("Explicit operation reads must catch up in the background") }
        XCTAssertEqual(load.fingerprintValidationAssetIDs, ["edited", "invalidated"])
        XCTAssertEqual(input.queries.count, 2)
        let wroteOldScan = await worker.writeFileSizeIfIndexStable(123, for: month, sampledScope: sample.scope,
            sampledGeneration: sample.generation, sampledAssetIDs: sample.ids)
        XCTAssertFalse(wroteOldScan, "A full rebuild invalidates scans even with identical scope and membership")
        worker.setAppActive(true)
        _ = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.all))
        XCTAssertEqual(input.queries.count, 2)
    }

    func testUnrelatedPhotoChangeDoesNotInvalidateFileSizeSample() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let worker = fixture.worker(input: input)
        _ = await worker.reloadLocalIndex(scope: .device(.all))
        let month = LibraryMonthKey(year: 2024, month: 1)
        let sample = await worker.sampleFileSizeScan(for: month)
        worker.handlePhotoLibraryChange(PHChange()) { _ in }
        let written = await worker.writeFileSizeIfIndexStable(123, for: month, sampledScope: sample.scope,
            sampledGeneration: sample.generation, sampledAssetIDs: sample.ids)
        XCTAssertTrue(written)
    }

    func testBulkInvalidationWaitsForFullReloadPermissionAndPreservesScope() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let worker = fixture.worker(input: input)
        _ = await worker.reloadLocalIndex(scope: .device(.photos))
        try fixture.expireHistory(prefix: "blocked")
        let result = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.photos), allowFullReload: false)
        guard case .requiresReload = result else { return XCTFail("Bulk invalidation must respect the full reload gate") }
        XCTAssertEqual(input.queries.count, 1)
        _ = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.videos))
        XCTAssertEqual(input.queries.count, 1, "A stale scope must not rebuild a different projection")
        _ = await worker.refreshLocalIndex(forAssetIDs: [], expectedScope: .device(.photos))
        XCTAssertEqual(input.queries, [.library(.photos), .library(.photos)])
    }

    @MainActor
    func testBrowserSeedPublishesChangesConsumedBeforeScheduledHomeRefresh() async throws {
        let fixture = try Fixture()
        defer { fixture.close() }
        let input = Input(payload: fixture.payload(edited: false))
        let manager = HomeIncrementalDataManager(photoLibraryService: PhotoLibraryService(),
            contentHashIndexRepository: fixture.repository,
            hooks: .init(remoteMonthSnapshot: { _ in nil }, currentScope: { .device(.all) }),
            fetchLocalLibrary: { input.fetch($0) })
        manager.fileSizeCoordinator.setActive(false)
        await manager.reloadLocalIndex()
        var changed: [Set<LibraryMonthKey>] = []
        manager.onMonthsChanged = { changed.append($0) }
        try fixture.database.write { db in
            try db.execute(sql: "DELETE FROM local_assets WHERE assetLocalIdentifier = 'current'")
        }
        _ = await manager.browserLocalSeed(expectedScope: .device(.all))
        XCTAssertEqual(changed.count, 1)
        XCTAssertFalse(changed[0].isEmpty)
        let laterChanges = await manager.refreshLocalIndex(forAssetIDs: [])
        XCTAssertTrue(laterChanges.isEmpty)
    }

    private func photoChange(_ worker: HomeDataProcessingWorker) async -> HomeDataChangeResult {
        await withCheckedContinuation { continuation in
            worker.handlePhotoLibraryChange(PHChange()) { continuation.resume(returning: $0) }
        }
    }

    private final class Input: @unchecked Sendable {
        private let lock = NSLock()
        private var payload: LibraryInitialPayload?
        private var requested: [PhotoLibraryQuery] = []

        init(payload: LibraryInitialPayload?) { self.payload = payload }
        var queries: [PhotoLibraryQuery] { lock.withLock { requested } }
        func setPayload(_ payload: LibraryInitialPayload?) { lock.withLock { self.payload = payload } }

        func fetch(_ query: PhotoLibraryQuery) -> HomeDataProcessingWorker.LocalLibraryInput? {
            XCTAssertFalse(Thread.isMainThread)
            return lock.withLock {
                requested.append(query)
                return payload.map { .init(collections: [], payload: $0) }
            }
        }
    }

    private final class Counter: @unchecked Sendable {
        private let lock = NSLock()
        private var count = 0
        var value: Int { lock.withLock { count } }
        func increment() { lock.withLock { count += 1 } }
    }

    private final class Fixture {
        static let currentFingerprint = Data([3])
        let directory: URL
        let database: DatabaseManager
        let repository: ContentHashIndexRepository
        private let indexedAt = Date(timeIntervalSince1970: 1_000)

        init() throws {
            directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            database = try DatabaseManager(databaseURL: directory.appendingPathComponent("index.sqlite"))
            repository = ContentHashIndexRepository(databaseManager: database)
            try database.write { db in
                for (id, fingerprint) in [("edited", Data([1])), ("invalidated", Data([2])), ("current", Self.currentFingerprint)] {
                    try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                        arguments: [id, fingerprint, indexedAt])
                }
                try db.execute(sql: "INSERT INTO local_asset_resources (assetLocalIdentifier, role, slot, contentHash, fileSize) VALUES (?, 1, 0, ?, 1)",
                    arguments: ["invalidated", Data([2])])
                try db.execute(sql: "UPDATE local_assets SET assetFingerprint = NULL WHERE assetLocalIdentifier = ?", arguments: ["invalidated"])
            }
        }

        func payload(edited: Bool) -> LibraryInitialPayload {
            let created = Date(timeIntervalSince1970: 1_704_069_000)
            let assets = ["edited", "invalidated", "current", "unindexed"].map { id in
                LibraryAssetSnapshot(localIdentifier: id, creationDate: created,
                    modificationDate: indexedAt.addingTimeInterval(edited && id == "edited" ? 1 : 0), mediaKind: .photo)
            }
            return LibraryInitialPayload(collections: [assets])
        }

        func worker(input: Input) -> HomeDataProcessingWorker {
            HomeDataProcessingWorker(photoLibraryService: PhotoLibraryService(), contentHashIndexRepository: repository,
                remoteMonthSnapshot: { _ in nil }, fetchLocalLibrary: { input.fetch($0) })
        }

        func expireHistory(prefix: String) throws {
            try database.write { db in
                for index in 0...8_192 {
                    try db.execute(sql: "INSERT INTO local_assets (assetLocalIdentifier, assetFingerprint, updatedAt) VALUES (?, ?, ?)",
                        arguments: ["\(prefix)-\(index)", Data([8]), indexedAt])
                }
            }
        }

        func close() {
            try? database.dbQueue.close()
            try? FileManager.default.removeItem(at: directory)
        }
    }
}

enum LocalIndexReadFailureInjection {
    static func interruptNextStatement(in database: DatabaseManager) {
        database.dbQueue.writeWithoutTransaction { db in
            guard let connection = db.sqliteConnection else { return XCTFail("Expected an open database") }
            sqlite3_progress_handler(connection, 1, { context in
                guard let context else { return 1 }
                sqlite3_progress_handler(OpaquePointer(context), 0, nil, nil)
                return 1
            }, UnsafeMutableRawPointer(connection))
        }
    }
}

import XCTest
@testable import Watermelon

final class WriteLockServiceTests: XCTestCase {
    private let basePath = "/photos"
    // Fixed reference instant; all freshness is computed against an explicit `now` passed per call.
    private let base = Date(timeIntervalSince1970: 1_700_000_000)

    private func fresh(_ now: Date) -> Date { now.addingTimeInterval(-60) }       // age 60s  <= 300s expiry
    private func stale(_ now: Date) -> Date { now.addingTimeInterval(-600) }       // age 600s >  300s expiry

    private func newWriterID() -> String { UUID().uuidString.lowercased() }

    private func makeService(
        writerID: String,
        client: InMemoryRemoteStorageClient
    ) -> WriteLockService {
        guard let service = WriteLockService(basePath: basePath, writerID: writerID, client: client) else {
            preconditionFailure("canonical writer ID must build a service")
        }
        return service
    }

    private func lockPath(_ writerID: String) -> String {
        RepoLayoutLite.lockPath(basePath: basePath, writerID: writerID)!
    }

    private var locksDirectory: String {
        RepoLayoutLite.locksDirectoryPath(basePath: basePath)
    }

    // MARK: - Acquire: no contention

    func testAcquireWithNoLocksWritesOwnLock() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let created = await client.createdDirectories
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertFalse(created.contains(locksDirectory))
        XCTAssertTrue(holds)
        XCTAssertTrue(confident)
    }

    func testAcquireReclaimsOwnExistingLock() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertTrue(holds)
    }

    // MARK: - Acquire: other fresh/unknown contention

    func testOtherFreshForegroundFailsClosed() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: fresh(base))
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .blocked)
        XCTAssertTrue(uploaded.isEmpty)
        XCTAssertFalse(holds)
    }

    func testOtherFreshBackgroundSkips() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: fresh(base))
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .skipped)
        XCTAssertTrue(uploaded.isEmpty)
    }

    func testOwnLockDoesNotHideOtherFresh() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: base)
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: fresh(base))
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .blocked)
        XCTAssertTrue(uploaded.isEmpty)
    }

    // MARK: - Acquire: stale other contention

    func testForegroundTakesOverStaleOtherAndDeletesIt() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(other)))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
    }

    func testBackgroundSkipsStaleOtherWithoutDeleting() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .skipped)
        XCTAssertTrue(deleted.isEmpty)
        XCTAssertTrue(uploaded.isEmpty)
    }

    // MARK: - Acquire: post-write re-LIST conflict

    func testPostWriteConflictDeletesOwnLockAndStops() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        // Pre-write LIST is empty; post-write LIST surfaces a fresh other writer (eventual consistency).
        await client.enqueueListResult([])
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base),
            makeLockEntry(basePath: basePath, writerID: other, modificationDate: fresh(base))
        ])
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let deleted = await client.deletedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .blocked)
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertTrue(deleted.contains(lockPath(me)))
        XCTAssertFalse(holds)
    }

    func testTwoContendersBothStopOnceConflictIsVisible() async {
        let a = newWriterID()
        let b = newWriterID()
        let clientA = InMemoryRemoteStorageClient()
        let clientB = InMemoryRemoteStorageClient()
        await clientA.enqueueListResult([])
        await clientB.enqueueListResult([])
        await clientA.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: a, modificationDate: base),
            makeLockEntry(basePath: basePath, writerID: b, modificationDate: fresh(base))
        ])
        await clientB.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: b, modificationDate: base),
            makeLockEntry(basePath: basePath, writerID: a, modificationDate: fresh(base))
        ])
        let serviceA = makeService(writerID: a, client: clientA)
        let serviceB = makeService(writerID: b, client: clientB)

        let resultA = await serviceA.acquire(mode: .foreground, now: base)
        let resultB = await serviceB.acquire(mode: .foreground, now: base)
        let holdsA = await serviceA.holdsLease
        let holdsB = await serviceB.holdsLease
        let deletedA = await clientA.deletedPaths
        let deletedB = await clientB.deletedPaths

        XCTAssertEqual(resultA, .blocked)
        XCTAssertEqual(resultB, .blocked)
        XCTAssertFalse(holdsA)
        XCTAssertFalse(holdsB)
        XCTAssertTrue(deletedA.contains(lockPath(a)))
        XCTAssertTrue(deletedB.contains(lockPath(b)))
    }

    // MARK: - Suspend/kill same-writer recovery

    func testSuspendKillSameWriterReclaimsImmediately() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: base)
        let later = base.addingTimeInterval(1000)   // own lock is now stale
        await client.setPendingUploadModificationDate(later)
        let service = makeService(writerID: me, client: client)
        await service.noteConfidenceLoss(.appLifecycleKillRecovery)

        let result = await service.acquire(mode: .background, now: later)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: later)

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(holds)
        XCTAssertTrue(confident)
    }

    // MARK: - Refresh

    func testRefreshSuccessUpdatesModificationDate() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)
        let mtime0 = await client.lockModificationDate(basePath: basePath, writerID: me)

        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refresh = await service.refresh(now: later)
        let mtime1 = await client.lockModificationDate(basePath: basePath, writerID: me)

        XCTAssertEqual(refresh, .refreshed)
        XCTAssertEqual(mtime0, base)
        XCTAssertEqual(mtime1, later)
        XCTAssertGreaterThan(mtime1!, mtime0!)
    }

    func testRefreshTransportFailureDegradesConfidenceButKeepsOwnership() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let later = base.addingTimeInterval(60)
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        let refresh = await service.refresh(now: later)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: later)

        XCTAssertEqual(refresh, .degraded(.retryable))
        XCTAssertTrue(holds, "a transient refresh failure must not abort ownership")
        XCTAssertFalse(confident)
    }

    // MARK: - assertStillOwned

    func testAssertStopsOnOtherFreshLock() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.seedLock(basePath: basePath, writerID: other, modificationDate: fresh(base))
        let assertion = await service.assertStillOwned(mode: .foreground, now: base)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)

        XCTAssertEqual(assertion, .lost(.otherWriter))
        XCTAssertFalse(holds)
        XCTAssertFalse(confident)
    }

    func testAssertStopsOnOtherUnknownLock() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil)
        let assertion = await service.assertStillOwned(mode: .foreground, now: base)
        let holds = await service.holdsLease

        XCTAssertEqual(assertion, .lost(.otherWriter))
        XCTAssertFalse(holds)
    }

    func testAssertStopsWhenOwnLockDeleted() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.removeLock(basePath: basePath, writerID: me)
        let assertion = await service.assertStillOwned(mode: .foreground, now: base)
        let holds = await service.holdsLease

        XCTAssertEqual(assertion, .lost(.ownLockDeleted))
        XCTAssertFalse(holds)
    }

    func testAssertReclaimsOwnStaleLockWhenNoUnsafeOther() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let later = base.addingTimeInterval(1000)   // own lock is now stale
        await client.setPendingUploadModificationDate(later)
        let assertion = await service.assertStillOwned(mode: .foreground, now: later)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: later)
        let mtime = await client.lockModificationDate(basePath: basePath, writerID: me)

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
        XCTAssertTrue(confident)
        XCTAssertEqual(mtime, later)
    }

    func testAssertReclaimsOwnUnknownLockWhenNoUnsafeOther() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(nil)   // own lock uploaded with unknown mtime
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.setPendingUploadModificationDate(base)
        let assertion = await service.assertStillOwned(mode: .foreground, now: base)
        let holds = await service.holdsLease

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
    }

    func testAssertListFailureFaultsAndDropsConfidence() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.enqueueListError(RemoteErrorFixtures.retryable)
        let assertion = await service.assertStillOwned(mode: .foreground, now: base)
        let confident = await service.hasLeaseConfidence(now: base)

        XCTAssertEqual(assertion, .faulted(.retryable))
        XCTAssertFalse(confident)
    }

    // MARK: - Lease-confidence gate

    func testLeaseConfidenceGateForEveryTrigger() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        for trigger in WriteLockService.ConfidenceLossTrigger.allCases {
            let refreshed = await service.refresh(now: base)
            XCTAssertEqual(refreshed, .refreshed)
            let before = await service.hasLeaseConfidence(now: base)
            XCTAssertTrue(before, "\(trigger) precondition")
            await service.noteConfidenceLoss(trigger)
            let after = await service.hasLeaseConfidence(now: base)
            XCTAssertFalse(after, "\(trigger) must drop confidence")
        }

        // Elapsed-since-refresh trigger: confidence expires after refreshInterval x2.
        let refreshed = await service.refresh(now: base)
        XCTAssertEqual(refreshed, .refreshed)
        let edge = base.addingTimeInterval(WriteLockService.confidenceMaxAge)
        let atEdge = await service.hasLeaseConfidence(now: edge)
        let pastEdge = await service.hasLeaseConfidence(now: edge.addingTimeInterval(1))
        XCTAssertTrue(atEdge)
        XCTAssertFalse(pastEdge)
    }

    // MARK: - Nil mtime fallback

    func testUnknownOtherLockBlocksAcquisition() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil)
        let foreground = makeService(writerID: me, client: client)
        let background = makeService(writerID: me, client: client)

        let foregroundResult = await foreground.acquire(mode: .foreground, now: base)
        let backgroundResult = await background.acquire(mode: .background, now: base)
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(foregroundResult, .blocked)
        XCTAssertEqual(backgroundResult, .skipped)
        XCTAssertTrue(uploaded.isEmpty)
    }

    func testUnknownOwnLockReclaimsSafely() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: nil)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(uploaded.contains(lockPath(me)))
    }

    // MARK: - Missing directory / fault classification

    func testMissingLocksDirectoryIsCreatedThenAcquires() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()   // no seeded locks directory
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let created = await client.createdDirectories
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(created.contains(locksDirectory))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
    }

    func testRetryableListErrorFaults() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let created = await client.createdDirectories

        XCTAssertEqual(result, .faulted(.retryable))
        XCTAssertTrue(uploaded.isEmpty)
        XCTAssertTrue(created.isEmpty)
    }

    func testTerminalListErrorFaults() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.terminal)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .faulted(.terminal))
        XCTAssertTrue(uploaded.isEmpty)
    }
}

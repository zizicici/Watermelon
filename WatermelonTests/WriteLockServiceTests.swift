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

    func testRefreshDoesNotRestoreConfidenceAfterGapExceedingMaxAge() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Advance past confidenceMaxAge — the gap between refreshes exceeds the window
        // where the lock could have been reclaimed by another foreground writer.
        let tLate = base.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        await client.setPendingUploadModificationDate(tLate)
        let refresh = await service.refresh(now: tLate)
        let confident = await service.hasLeaseConfidence(now: tLate)

        XCTAssertEqual(refresh, .degraded(.retryable),
                       "refresh must not upload when gap since last refresh exceeds confidenceMaxAge")
        XCTAssertFalse(confident, "confidence must not be restored when gap exceeds confidenceMaxAge")
    }

    func testRefreshAfterExpiryDoesNotRestoreConfidenceWithForeignWriter() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let serviceA = makeService(writerID: me, client: client)
        let acquired = await serviceA.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Advance past expiry so A's lock is stale.
        let tLate = base.addingTimeInterval(WriteLockService.expiry + 1)

        // Writer B acquires (foreground, deletes A's stale lock, writes B's lock).
        let serviceB = makeService(writerID: other, client: client)
        await client.setPendingUploadModificationDate(tLate)
        let bResult = await serviceB.acquire(mode: .foreground, now: tLate)
        XCTAssertEqual(bResult, .acquired, "B must acquire after A's lock expires")

        // Writer A resumes and refreshes. The gap exceeds confidenceMaxAge so A must not upload.
        await client.setPendingUploadModificationDate(tLate)
        let aRefresh = await serviceA.refresh(now: tLate)
        let aConfident = await serviceA.hasLeaseConfidence(now: tLate)

        XCTAssertEqual(aRefresh, .degraded(.retryable),
                       "A must not upload when gap exceeds confidenceMaxAge")
        XCTAssertFalse(aConfident, "A must not restore confidence after expiry gap without reassertion")

        // A must not have recreated its lock — B is the only valid writer.
        let aLockExists = await client.lockExists(basePath: basePath, writerID: me)
        XCTAssertFalse(aLockExists,
                       "expired-confidence refresh must not recreate A's stale lock")

        // B must retain ownership without seeing an unsafe other writer.
        let bAssertion = await serviceB.assertStillOwned(mode: .foreground, now: tLate)
        XCTAssertEqual(bAssertion, .stillOwned,
                       "B must remain owned — A's expired refresh must not evict B")
    }

    func testRefreshRestoresConfidenceWithinMaxAgeGap() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // First refresh at normal interval — within confidenceMaxAge.
        let t1 = base.addingTimeInterval(WriteLockService.refreshInterval)
        await client.setPendingUploadModificationDate(t1)
        let refresh1 = await service.refresh(now: t1)
        let confident1 = await service.hasLeaseConfidence(now: t1)
        XCTAssertEqual(refresh1, .refreshed)
        XCTAssertTrue(confident1, "normal-interval refresh must restore confidence")

        // Second refresh at normal interval — cumulative gap from base is 2*interval = confidenceMaxAge,
        // but the gap from the PREVIOUS refresh is only one interval, which is within the window.
        let t2 = t1.addingTimeInterval(WriteLockService.refreshInterval)
        await client.setPendingUploadModificationDate(t2)
        let refresh2 = await service.refresh(now: t2)
        let confident2 = await service.hasLeaseConfidence(now: t2)
        XCTAssertEqual(refresh2, .refreshed)
        XCTAssertTrue(confident2, "consecutive normal-interval refreshes must keep confidence")
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

    func testAssertPostWriteConflictDeletesOwnLockAndReturnsLost() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Initial LIST sees only our lock (safe). Confirmation LIST surfaces a fresh other writer.
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base),
            makeLockEntry(basePath: basePath, writerID: other, modificationDate: fresh(base))
        ])

        let assertion = await service.assertStillOwned(mode: .foreground, now: base)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)
        let deleted = await client.deletedPaths

        XCTAssertEqual(assertion, .lost(.otherWriter))
        XCTAssertFalse(holds)
        XCTAssertFalse(confident)
        XCTAssertTrue(deleted.contains(lockPath(me)))
    }

    func testAssertConfirmationListFailureDeletesOwnLockAndDropsLease() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Initial LIST sees only our lock. writeOwnLock succeeds. Confirmation LIST fails.
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueListError(RemoteErrorFixtures.retryable)

        let assertion = await service.assertStillOwned(mode: .foreground, now: base)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)
        let deleted = await client.deletedPaths

        XCTAssertEqual(assertion, .faulted(.retryable))
        XCTAssertFalse(holds)
        XCTAssertFalse(confident)
        XCTAssertTrue(deleted.contains(lockPath(me)))
    }

    func testRefreshDoesNotRestoreConfidenceAfterNoteConfidenceLoss() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await service.noteConfidenceLoss(.appLifecycleSuspend)
        let afterLoss = await service.hasLeaseConfidence(now: base)
        XCTAssertFalse(afterLoss)

        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refreshed = await service.refresh(now: later)
        let confident = await service.hasLeaseConfidence(now: later)

        XCTAssertEqual(refreshed, .refreshed)
        XCTAssertFalse(confident, "refresh must not restore confidence after noteConfidenceLoss")
    }

    func testRefreshIsNoOpAfterRelease() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let uploadedBefore = await client.uploadedPaths.count
        await service.release()

        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refreshed = await service.refresh(now: later)
        let uploadedAfter = await client.uploadedPaths.count
        let holds = await service.holdsLease

        XCTAssertEqual(refreshed, .degraded(.retryable))
        XCTAssertEqual(uploadedAfter, uploadedBefore, "refresh must not upload after release")
        XCTAssertFalse(holds)
    }

    func testReleaseClearsLeaseBeforeSuspension() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // After release, holdsLeaseValue must be false immediately (before the
        // best-effort delete completes), so a racing refresh() sees the cleared state.
        await service.release()
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)

        XCTAssertFalse(holds)
        XCTAssertFalse(confident)
    }

    func testRefreshDoesNotRestoreConfidenceAfterInternalFault() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Simulate a refresh transport failure that degrades confidence.
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        let degraded = await service.refresh(now: base)
        XCTAssertEqual(degraded, .degraded(.retryable))
        let afterFault = await service.hasLeaseConfidence(now: base)
        XCTAssertFalse(afterFault)

        // Next successful refresh must NOT restore confidence without a full assertion.
        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refreshed = await service.refresh(now: later)
        let confident = await service.hasLeaseConfidence(now: later)

        XCTAssertEqual(refreshed, .refreshed)
        XCTAssertFalse(confident, "refresh must not restore confidence after internal fault")
    }

    func testAssertStillOwnedWriteFailureBlocksConfidenceRestoration() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Initial LIST sees own lock. writeOwnLock fails. Cannot proceed without refreshed lease.
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)

        let assertion = await service.assertStillOwned(mode: .foreground, now: base)
        XCTAssertEqual(assertion, .faulted(.retryable),
                       "failed refresh must not return .stillOwned even with a fresh own lock")
        let afterAssert = await service.hasLeaseConfidence(now: base)
        XCTAssertFalse(afterAssert, "confidence must be degraded after writeOwnLock failure")

        // Next successful refresh must NOT restore confidence without a full assertion.
        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refreshed = await service.refresh(now: later)
        let confident = await service.hasLeaseConfidence(now: later)

        XCTAssertEqual(refreshed, .refreshed)
        XCTAssertFalse(confident, "refresh must not restore confidence after assertStillOwned write failure")
    }

    func testAssertStillOwnedStaleOwnLockWriteFailureReturnsLost() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Advance past expiry so the own lock appears stale.
        let expired = base.addingTimeInterval(WriteLockService.expiry + 1)

        // Initial LIST sees own lock with stale mtime. writeOwnLock fails.
        // A stale unrefreshed lock is reclaimable by another writer, so must return .lost.
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)

        let assertion = await service.assertStillOwned(mode: .foreground, now: expired)
        XCTAssertEqual(assertion, .lost(.ownLockDeleted),
                       "stale own lock + failed refresh must return .lost, not .stillOwned")
    }

    func testAssertFailsClosedAfterForeignWriterLoss() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Seed a fresh foreign lock; first assertion detects competing writer.
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: fresh(base))
        let first = await service.assertStillOwned(mode: .foreground, now: base)
        XCTAssertEqual(first, .lost(.otherWriter))

        // Remove the foreign lock (competing writer finished and released).
        await client.removeLock(basePath: basePath, writerID: other)

        // Second assertion must still fail closed: ownership was already lost.
        let second = await service.assertStillOwned(mode: .foreground, now: base)
        XCTAssertEqual(second, .lost(.ownLockDeleted),
                       "assertion must stay lost after detecting a competing writer")
        let holds = await service.holdsLease
        XCTAssertFalse(holds)
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
            // Re-acquire to clear confidenceLossPending between iterations.
            _ = await service.acquire(mode: .foreground, now: base)
            let before = await service.hasLeaseConfidence(now: base)
            XCTAssertTrue(before, "\(trigger) precondition")
            await service.noteConfidenceLoss(trigger)
            let after = await service.hasLeaseConfidence(now: base)
            XCTAssertFalse(after, "\(trigger) must drop confidence")
        }

        // Elapsed-since-refresh trigger: confidence expires after refreshInterval x2.
        _ = await service.acquire(mode: .foreground, now: base)
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

    // MARK: - Backward wall-clock (A1 regression)

    func testBackwardClockDoesNotPreserveLeaseConfidence() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let backward = base.addingTimeInterval(-60)
        let confident = await service.hasLeaseConfidence(now: backward)
        XCTAssertFalse(confident, "negative elapsed must not pass the confidence gate")
    }

    func testRefreshWithBackwardClockDoesNotRestoreConfidence() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)
        let preConfident = await service.hasLeaseConfidence(now: base)
        XCTAssertTrue(preConfident)

        // Refresh with backward clock. Negative elapsed must skip upload and lose confidence.
        let backward = base.addingTimeInterval(-60)
        await client.setPendingUploadModificationDate(backward)
        let refreshed = await service.refresh(now: backward)
        XCTAssertEqual(refreshed, .degraded(.retryable),
                       "backward-clock refresh must not upload")
        let confident = await service.hasLeaseConfidence(now: backward)
        XCTAssertFalse(confident, "backward-clock refresh must lose confidence")
    }

    func testBackwardClockRefreshSetsConfidenceLossPending() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Refresh with backward clock sets confidenceLossPending without uploading.
        let backward = base.addingTimeInterval(-60)
        await client.setPendingUploadModificationDate(backward)
        let refresh1 = await service.refresh(now: backward)
        XCTAssertEqual(refresh1, .degraded(.retryable),
                       "backward-clock refresh must not upload")
        let confident1 = await service.hasLeaseConfidence(now: backward)
        XCTAssertFalse(confident1)
        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refresh2 = await service.refresh(now: later)
        let confident = await service.hasLeaseConfidence(now: later)
        XCTAssertEqual(refresh2, .refreshed)
        XCTAssertFalse(confident, "confidence must stay lost after backward-clock set confidenceLossPending")
    }

    func testBackwardClockOwnLockTreatedAsUnknownDuringAssertion() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Clock moves backward. LIST shows own lock with mtime `base` but now = base - 60.
        // Freshness: elapsed = -60 → .unknown → ownFresh = false.
        // writeOwnLock fails → !ownFresh → .lost(.ownLockDeleted).
        let backward = base.addingTimeInterval(-60)
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)

        let assertion = await service.assertStillOwned(mode: .foreground, now: backward)
        XCTAssertEqual(assertion, .lost(.ownLockDeleted),
                       "backward clock must treat own lock as unknown freshness")
    }

    // MARK: - Assert ownership with write failure (A2 regression)

    func testAssertStillOwnedReturnsFaultedWhenOwnLockRefreshFailsEvenIfFresh() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Near-expiry: own lock still fresh but close to expiry.
        let nearExpiry = base.addingTimeInterval(WriteLockService.expiry - 1)
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: nearExpiry)
        ])
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)

        let assertion = await service.assertStillOwned(mode: .foreground, now: nearExpiry)
        XCTAssertEqual(assertion, .faulted(.retryable),
                       "near-expiry lock with failed refresh must not return .stillOwned")
        let confident = await service.hasLeaseConfidence(now: nearExpiry)
        XCTAssertFalse(confident)
        let holds = await service.holdsLease
        XCTAssertTrue(holds, "lease ownership is retained for retry when the lock is still present")
    }

    // MARK: - Release during in-flight upload

    func testRefreshCleansUpLockAfterReleaseDuringUpload() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        _ = await service.acquire(mode: .foreground, now: base)

        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)

        // Coordinate: upload signals started, then waits for resume.
        let (startedStream, startedCont) = AsyncStream.makeStream(of: Void.self)
        let (resumeStream, resumeCont) = AsyncStream.makeStream(of: Void.self)

        await client.setOnUpload {
            startedCont.yield(())
            for await _ in resumeStream { break }
        }

        let refreshTask = Task { await service.refresh(now: later) }

        // Wait for upload to start, then release while it's suspended.
        for await _ in startedStream { break }
        await service.release()

        // Resume the upload — it recreates the lock file on the fake remote.
        resumeCont.yield(())
        resumeCont.finish()

        let result = await refreshTask.value
        XCTAssertEqual(result, .degraded(.retryable))

        let exists = await client.lockExists(basePath: basePath, writerID: me)
        XCTAssertFalse(exists,
                       "refresh must clean up the lock recreated by an upload that completed after release")
    }

    func testAssertStillOwnedCleansUpLockAfterReleaseDuringUpload() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        _ = await service.acquire(mode: .foreground, now: base)

        let (startedStream, startedCont) = AsyncStream.makeStream(of: Void.self)
        let (resumeStream, resumeCont) = AsyncStream.makeStream(of: Void.self)

        await client.setOnUpload {
            startedCont.yield(())
            for await _ in resumeStream { break }
        }

        let assertTask = Task { await service.assertStillOwned(mode: .foreground, now: base) }

        for await _ in startedStream { break }
        await service.release()

        resumeCont.yield(())
        resumeCont.finish()

        let assertion = await assertTask.value
        XCTAssertEqual(assertion, .lost(.ownLockDeleted),
                       "assertStillOwned must return .lost when release happened during upload")

        let exists = await client.lockExists(basePath: basePath, writerID: me)
        XCTAssertFalse(exists,
                       "assertStillOwned must clean up the lock recreated by an upload after release")
    }

    // MARK: - Same-writer reacquire race (R13 regression)

    // Demonstrates the race: releasing without awaiting an in-flight refresh lets the old cleanup
    // delete a newly acquired same-writer lock.  stopAndRelease now awaits the refresh task before
    // calling release, closing this window.
    func testReleaseWithoutAwaitingRefreshAllowsSameWriterLockDeletion() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let oldLock = makeService(writerID: me, client: client)
        _ = await oldLock.acquire(mode: .foreground, now: base)

        let (uploadStarted, uploadStartedCont) = AsyncStream.makeStream(of: Void.self)
        let (resumeUpload, resumeUploadCont) = AsyncStream.makeStream(of: Void.self)
        await client.setOnUpload {
            uploadStartedCont.yield(())
            for await _ in resumeUpload { break }
        }

        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)

        let oldRefreshTask = Task { _ = await oldLock.refresh(now: later) }
        for await _ in uploadStarted { break }

        // Release WITHOUT awaiting the refresh — the buggy pattern.
        await oldLock.release()

        // A new session acquires the same writer ID (same lock path).
        await client.setPendingUploadModificationDate(base)
        let newLock = makeService(writerID: me, client: client)
        let result = await newLock.acquire(mode: .foreground, now: base)
        XCTAssertEqual(result, .acquired)

        // Old upload completes → old refresh sees holdsLeaseValue == false → deletes ownLockPath.
        resumeUploadCont.yield(())
        resumeUploadCont.finish()
        _ = await oldRefreshTask.value

        let exists = await client.lockExists(basePath: basePath, writerID: me)
        XCTAssertFalse(exists,
                       "old session's stale cleanup must delete the new session's same-writer lock")
    }

    // The fix pattern: awaiting the in-flight refresh before release prevents the race.
    func testAwaitingRefreshBeforeReleasePreventsSameWriterLockDeletion() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let oldLock = makeService(writerID: me, client: client)
        _ = await oldLock.acquire(mode: .foreground, now: base)

        let (uploadStarted, uploadStartedCont) = AsyncStream.makeStream(of: Void.self)
        let (resumeUpload, resumeUploadCont) = AsyncStream.makeStream(of: Void.self)
        await client.setOnUpload {
            uploadStartedCont.yield(())
            for await _ in resumeUpload { break }
        }

        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)

        let oldRefreshTask = Task { _ = await oldLock.refresh(now: later) }
        for await _ in uploadStarted { break }

        // FIX: resume the upload and await the refresh BEFORE releasing.
        resumeUploadCont.yield(())
        resumeUploadCont.finish()
        _ = await oldRefreshTask.value

        await oldLock.release()

        // New session acquires the same writer ID.
        await client.setPendingUploadModificationDate(base)
        let newLock = makeService(writerID: me, client: client)
        let result = await newLock.acquire(mode: .foreground, now: base)
        XCTAssertEqual(result, .acquired)

        let exists = await client.lockExists(basePath: basePath, writerID: me)
        XCTAssertTrue(exists,
                      "new session's lock must survive — no stale cleanup after awaiting refresh before release")
    }
}

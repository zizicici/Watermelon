import XCTest
@testable import Watermelon

final class WriteLockServiceTests: XCTestCase {
    private let basePath = "/photos"
    // Fixed reference instant; all freshness is computed against an explicit `now` passed per call.
    private let base = Date(timeIntervalSince1970: 1_700_000_000)

    private func fresh(_ now: Date) -> Date { now.addingTimeInterval(-60) }       // age 60s  <= 300s expiry
    private func stale(_ now: Date) -> Date { now.addingTimeInterval(-600) }       // age 600s >  300s expiry
    private func retryAfter(_ timestamp: Date) -> Date {
        timestamp.addingTimeInterval(WriteLockService.expiry + WriteLockService.clockSkewTolerance)
    }

    private func ownBlock(
        _ reason: WriteLockService.OwnLockBlock.Reason,
        retryAfter: Date?
    ) -> WriteLockService.OwnLockBlock {
        WriteLockService.OwnLockBlock(reason: reason, retryAfter: retryAfter)
    }

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

    // MARK: - Remote lock body reader

    func testRemoteLockReaderDownloadBodyDecodesWithoutMetadataOrList() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: writerID,
            sessionToken: "session",
            lockToken: "token",
            generation: 3,
            writtenAt: base
        )
        await client.seedLock(basePath: basePath, writerID: writerID, modificationDate: base, body: body)
        let path = lockPath(writerID)

        let decoded = try await RemoteLockReader.downloadBody(client: client, path: path)
        let listed = await client.listedPaths
        let metadataAttempts = await client.metadataAttemptPaths
        let downloads = await client.downloadAttemptPaths

        XCTAssertEqual(decoded, body)
        XCTAssertTrue(listed.isEmpty)
        XCTAssertTrue(metadataAttempts.isEmpty)
        XCTAssertEqual(downloads, [path])
    }

    func testRemoteLockReaderDownloadBodyReturnsNilForUndecodableBytes() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedUndecodableLock(basePath: basePath, writerID: writerID, modificationDate: base)
        let path = lockPath(writerID)

        let decoded = try await RemoteLockReader.downloadBody(client: client, path: path)
        let metadataAttempts = await client.metadataAttemptPaths

        XCTAssertNil(decoded)
        XCTAssertTrue(metadataAttempts.isEmpty)
    }

    func testRemoteLockReaderDownloadBodyPropagatesDownloadErrorsWithoutMetadata() async {
        let cases: [(String, Error)] = [
            ("notFound", RemoteErrorFixtures.notFound),
            ("retryable", RemoteErrorFixtures.retryable)
        ]

        for (label, expectedError) in cases {
            let client = InMemoryRemoteStorageClient()
            let path = lockPath(newWriterID())
            await client.enqueueDownloadError(expectedError)

            do {
                _ = try await RemoteLockReader.downloadBody(client: client, path: path)
                XCTFail("Expected \(label) download error")
            } catch let caughtError {
                let actual = caughtError as NSError
                let expected = expectedError as NSError
                XCTAssertEqual(actual.domain, expected.domain, label)
                XCTAssertEqual(actual.code, expected.code, label)
            }

            let listed = await client.listedPaths
            let metadataAttempts = await client.metadataAttemptPaths
            XCTAssertTrue(listed.isEmpty, label)
            XCTAssertTrue(metadataAttempts.isEmpty, label)
        }
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

    func testAcquireDeletesStableStaleOwnLockBeforeUploadingOnNoOverwriteBackend() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: stale(base))
        await client.setRejectUploadOntoExistingDestination(true)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(me)), "stable stale own locks must be deleted before no-overwrite upload")
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertTrue(holds)
    }

    func testAcquireReclaimsStableStaleUndecodableOwnLock() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedUndecodableLock(basePath: basePath, writerID: me, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(uploaded.contains(lockPath(me)), "stable expired legacy own locks remain recoverable")
        XCTAssertTrue(holds)
    }

    func testAcquireReclaimsOwnLockWithNilMtimeAndStaleBodyTimestamp() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: me,
            sessionToken: "old-session",
            lockToken: "old-token",
            generation: 1,
            writtenAt: stale(base)
        )
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: nil, body: body)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertTrue(holds)
    }

    func testAcquireBlocksOwnLockWithNilMtimeAndFreshBodyTimestamp() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: me,
            sessionToken: "live-session",
            lockToken: "live-token",
            generation: 1,
            writtenAt: fresh(base)
        )
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: nil, body: body)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .blockedByOwnLock(ownBlock(.stillFresh, retryAfter: retryAfter(fresh(base)))))
        XCTAssertTrue(uploaded.isEmpty)
        XCTAssertFalse(holds)
    }

    func testAcquireBlocksFreshSameWriterExistingLockWithoutOverwrite() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: fresh(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .blockedByOwnLock(ownBlock(.stillFresh, retryAfter: retryAfter(fresh(base)))))
        XCTAssertTrue(uploaded.isEmpty, "a fresh same-writer lock must not be overwritten")
        XCTAssertFalse(holds)
    }

    func testBackgroundAcquireSkipsFreshSameWriterExistingLockWithoutOverwrite() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: fresh(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .skippedByOwnLock(ownBlock(.stillFresh, retryAfter: retryAfter(fresh(base)))))
        XCTAssertTrue(uploaded.isEmpty, "background must not overwrite a live same-writer lock")
        XCTAssertFalse(holds)
    }

    func testAcquireDoesNotOverwriteSameWriterSuccessorCreatedAfterInitialScan() async throws {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let successor = LockFileBody(
            writerID: me,
            sessionToken: "successor-session",
            lockToken: "successor-lock",
            generation: 1,
            writtenAt: fresh(base)
        )
        let path = lockPath(me)
        let testBasePath = basePath
        let successorDate = fresh(base)
        await client.setOnUpload {
            await client.seedLock(basePath: testBasePath, writerID: me, modificationDate: successorDate, body: successor)
        }
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease
        let decoded = try await RemoteLockReader.downloadBody(client: client, path: path)

        XCTAssertEqual(result, .blockedByOwnLock(ownBlock(.ownershipUnverified, retryAfter: nil)))
        XCTAssertTrue(uploaded.isEmpty, "create-if-absent must not overwrite a same-writer successor")
        XCTAssertEqual(decoded, successor)
        XCTAssertFalse(holds)
    }

    func testAcquireBlocksSameWriterStaleLockThatRefreshesDuringConfirmation() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let lockPath = lockPath(me)
        let freshDate = fresh(base)
        let testBasePath = basePath
        await client.setOnDownload { path in
            if path == lockPath {
                await client.setLockModificationDate(basePath: testBasePath, writerID: me, to: freshDate)
            }
        }
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .blockedByOwnLock(ownBlock(.stillFresh, retryAfter: retryAfter(freshDate))))
        XCTAssertTrue(uploaded.isEmpty, "a same-writer lock refreshed during proof must not be overwritten")
        XCTAssertFalse(holds)
    }

    func testAcquireDoesNotReturnPastRetryAfterWhenOwnLockChangesButRemainsStale() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let lockPath = lockPath(me)
        let stillStaleDate = base.addingTimeInterval(-500)
        let testBasePath = basePath
        await client.setOnDownload { path in
            if path == lockPath {
                await client.setLockModificationDate(basePath: testBasePath, writerID: me, to: stillStaleDate)
            }
        }
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .blockedByOwnLock(ownBlock(.changedDuringConfirmation, retryAfter: nil)))
        XCTAssertTrue(uploaded.isEmpty, "a changing same-writer lock must not be overwritten")
        XCTAssertFalse(holds)
    }

    func testAcquireFailsWhenOwnLockBodyNoLongerMatchesSession() async throws {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let successorBody = LockFileBody(
            writerID: me,
            sessionToken: "successor-session",
            lockToken: "successor-lock",
            generation: 1
        )
        await client.enqueueDownloadData(try LockFileCodec.encode(successorBody))
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let holds = await service.holdsLease

        XCTAssertEqual(result, .blocked)
        XCTAssertFalse(holds)
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

    func testBackgroundTakesOverStaleOtherAndDeletesIt() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(other)))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
    }

    func testBackgroundTakesOverStaleForeignLockEvenWithStaleOwnLock() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: stale(base))      // stale own
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))   // stale foreign
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let uploaded = await client.uploadedPaths
        let deleted = await client.deletedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        let holds = await service.holdsLease

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertTrue(deleted.contains(lockPath(me)))
        XCTAssertTrue(deleted.contains(lockPath(other)))
        XCTAssertFalse(foreignExists)
        XCTAssertTrue(holds)
    }

    func testBackgroundTakesOverStaleForeignLockAppearingAtConfirmation() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.setPendingUploadModificationDate(base)
        await client.enqueueListResult([])   // initial LIST misses the foreign lock
        await client.enqueueListResult([     // confirmation LIST surfaces the foreign lock as stale
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base),
            makeLockEntry(basePath: basePath, writerID: other, modificationDate: stale(base))
        ])
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let uploaded = await client.uploadedPaths
        let deleted = await client.deletedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(uploaded.contains(lockPath(me)), "the own lock was written before confirmation")
        XCTAssertFalse(deleted.contains(lockPath(me)))
        XCTAssertTrue(holds)
    }

    // Foreground uses the same takeover rule as background.
    func testForegroundTakesOverStaleForeignLockEvenWithStaleOwnLock() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: stale(base))      // stale own
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))   // stale foreign
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .acquired,
                       "foreground takes over a stale foreign lock")
        XCTAssertTrue(deleted.contains(lockPath(other)), "foreground deletes the confirmed-stale foreign lock")
        XCTAssertTrue(uploaded.contains(lockPath(me)))
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

    // Regression (R05 Cluster D-Minor3): if writeOwnLock's upload lands but the acquire task is then
    // cancelled, the proof+delete cleanup must run outside the cancelled task — otherwise the just-landed
    // own lock leaks until lease expiry. Here the upload lands, cancellation then faults the proveOwnLock
    // read, routing acquire to deleteOwnLockBestEffort; that cleanup's own download must not be cancelled out.
    func testAcquireDeletesLandedOwnLockWhenCancelledAfterWrite() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.setRespectTaskCancellation(true)
        // Cancel exactly when writeOwnLock's upload lands — after the upload's own cancellation check has
        // passed, so the lock body lands but every later cancel-sensitive op (proveOwnLock's download) faults.
        final class CancelHandle { var cancel: (() -> Void)? }
        let handle = CancelHandle()
        await client.setOnUpload { handle.cancel?() }

        let service = makeService(writerID: me, client: client)
        let task = Task { await service.acquire(mode: .foreground, now: base) }
        handle.cancel = { task.cancel() }
        let result = await task.value

        if case .faulted = result {
            // expected: the cancelled proof read faults the acquisition
        } else {
            XCTFail("a cancelled acquire must surface .faulted, got \(result)")
        }
        let leaked = await client.lockExists(basePath: basePath, writerID: me)
        XCTAssertFalse(leaked, "a landed own lock must be deleted even when the acquire task is cancelled")
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.contains(lockPath(me)), "the shielded cleanup must delete the just-landed own lock")
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

    func testSuspendKillSameWriterReclaimsAfterExpiry() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: base)
        let later = base.addingTimeInterval(1000)   // own lock is now stale
        await client.setPendingUploadModificationDate(later)
        let service = makeService(writerID: me, client: client)
        await service.noteConfidenceLoss()

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

    func testRefreshRestoresConfidenceAfterGapWhenOwnLockStillProvable() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Advance past confidenceMaxAge. Refresh must re-prove ownership instead of blindly uploading.
        let tLate = base.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)
        await client.setPendingUploadModificationDate(tLate)
        let refresh = await service.refresh(now: tLate)
        let confident = await service.hasLeaseConfidence(now: tLate)

        XCTAssertEqual(refresh, .refreshed,
                       "refresh can recover after the gap when the own lock body still proves this session")
        XCTAssertTrue(confident, "confidence is restored after the ownership proof refreshes the lock")
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

        // Advance past expiry + skew so A's lock is stale (beyond the skew band).
        let tLate = base.addingTimeInterval(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 1)

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
        let bAssertion = await serviceB.assertStillOwned(now: tLate)
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

        // Second refresh at normal interval — the gap from the PREVIOUS refresh is one interval, which
        // remains within the shortened confidence window.
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
        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)

        XCTAssertEqual(assertion, .lost(.otherWriter))
        XCTAssertFalse(holds)
        XCTAssertFalse(confident)
    }

    func testAssertClearsOtherInvalidUnknownLock() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil)
        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
        XCTAssertFalse(foreignExists)
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
        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease

        XCTAssertEqual(assertion, .lost(.ownLockDeleted))
        XCTAssertFalse(holds)
    }

    func testAssertMissingOwnLockDoesNotClearStaleForeignLock() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.removeLock(basePath: basePath, writerID: me)
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))

        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        let deleted = await client.deletedPaths

        XCTAssertEqual(assertion, .lost(.ownLockDeleted))
        XCTAssertFalse(holds)
        XCTAssertTrue(foreignExists)
        XCTAssertFalse(deleted.contains(lockPath(other)))
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
        let assertion = await service.assertStillOwned(now: later)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: later)
        let mtime = await client.lockModificationDate(basePath: basePath, writerID: me)

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
        XCTAssertTrue(confident)
        XCTAssertEqual(mtime, later)
    }

    func testAssertRefreshesOwnLockMtimeWhenBackendRefusesOverwriteUpload() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setRejectUploadOntoExistingDestination(true)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let later = base.addingTimeInterval(60)
        await client.setPendingUploadModificationDate(later)
        let assertion = await service.assertStillOwned(now: later)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: later)
        let mtime = await client.lockModificationDate(basePath: basePath, writerID: me)
        let deleted = await client.deletedPaths

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
        XCTAssertTrue(confident)
        XCTAssertEqual(mtime, later)
        XCTAssertFalse(deleted.contains(lockPath(me)), "mtime fallback must not delete the live own lock")
    }

    func testAssertFaultsWhenNameCollisionTouchDoesNotProveCurrentWrite() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setRejectUploadOntoExistingDestination(true)
        await client.setIgnoreSetModificationDate(true)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let later = base.addingTimeInterval(60)
        let assertion = await service.assertStillOwned(now: later)
        let confident = await service.hasLeaseConfidence(now: later)
        let holds = await service.holdsLease
        let mtime = await client.lockModificationDate(basePath: basePath, writerID: me)
        let deleted = await client.deletedPaths

        XCTAssertEqual(assertion, .faulted(.retryable))
        XCTAssertFalse(confident)
        XCTAssertTrue(holds)
        XCTAssertEqual(mtime, base)
        XCTAssertFalse(deleted.contains(lockPath(me)), "a failed touch proof must not delete the own lock")
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
        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
    }

    func testBackgroundAssertClearsStaleForeignLockAfterAcquire() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .background, now: base)
        XCTAssertEqual(acquired, .acquired)

        // A stale foreign lock the acquire LISTs missed surfaces before the next assertion.
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
        XCTAssertFalse(foreignExists)
    }

    // A foreground lease uses the same stale foreign cleanup rule during a live assertion.
    func testForegroundAssertReclaimsAlongsideStaleForeignLockAfterAcquire() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(assertion, .stillOwned,
                       "foreground clears a stale foreign lock during a live run")
        XCTAssertTrue(holds)
        XCTAssertFalse(foreignExists)
    }

    func testBackgroundForeignAbsentCheckClearsStaleForeignWithinConfidenceWindow() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .background, now: base)
        XCTAssertEqual(acquired, .acquired)

        let confidentBefore = await service.hasLeaseConfidence(now: base)
        XCTAssertTrue(confidentBefore, "the lease is still locally confident within the window")
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))

        let assertion = await service.assertForeignAbsentForBackgroundWrite(now: base)
        let holds = await service.holdsLease
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
        XCTAssertFalse(foreignExists)
    }

    func testBackgroundForeignAbsentCheckMissingOwnLockDoesNotClearStaleForeign() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .background, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.removeLock(basePath: basePath, writerID: me)
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))

        let assertion = await service.assertForeignAbsentForBackgroundWrite(now: base)
        let holds = await service.holdsLease
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        let deleted = await client.deletedPaths

        XCTAssertEqual(assertion, .lost(.ownLockDeleted))
        XCTAssertFalse(holds)
        XCTAssertTrue(foreignExists)
        XCTAssertFalse(deleted.contains(lockPath(other)))
    }

    // The lightweight background data-byte gate passes on a clean lease and does not rewrite the own lock
    // (it is only a foreign-evidence probe, kept cheap for the per-upload hot path).
    func testBackgroundForeignAbsentCheckPassesAndDoesNotRewriteOwnLock() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .background, now: base)
        XCTAssertEqual(acquired, .acquired)

        let uploadsBefore = await client.uploadedPaths.count
        let assertion = await service.assertForeignAbsentForBackgroundWrite(now: base)
        let uploadsAfter = await client.uploadedPaths.count
        let holds = await service.holdsLease

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
        XCTAssertEqual(uploadsAfter, uploadsBefore, "the lightweight gate must not rewrite the own lock")
    }

    func testAssertUsesStableClientSnapshotWhenReplacementInterleaves() async throws {
        let me = newWriterID()
        let clientA = InMemoryRemoteStorageClient()
        let clientB = InMemoryRemoteStorageClient()
        await clientA.seedDirectory(locksDirectory)
        await clientA.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: clientA)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let successor = LockFileBody(
            writerID: me,
            sessionToken: UUID().uuidString,
            lockToken: UUID().uuidString,
            generation: 99
        )
        await clientB.seedLock(basePath: basePath, writerID: me, modificationDate: base, body: successor)
        let replacementBeforeData = await clientB.fileData(path: lockPath(me))
        let replacementBefore = try XCTUnwrap(replacementBeforeData)

        await clientA.setOnDownload { _ in
            await service.replaceClient(clientB)
        }

        let assertion = await service.assertStillOwned(now: base)
        let replacementAfter = await clientB.fileData(path: self.lockPath(me))
        let replacementAfterData = try XCTUnwrap(replacementAfter)
        let replacementUploads = await clientB.uploadedPaths

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertEqual(replacementAfterData, replacementBefore)
        XCTAssertFalse(replacementUploads.contains(lockPath(me)))
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

        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)
        let deleted = await client.deletedPaths

        XCTAssertEqual(assertion, .lost(.otherWriter))
        XCTAssertFalse(holds)
        XCTAssertFalse(confident)
        XCTAssertTrue(deleted.contains(lockPath(me)))
    }

    func testAssertConfirmationClearsStaleForeignLockAndStaysOwned() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base),
            makeLockEntry(basePath: basePath, writerID: other, modificationDate: stale(base))
        ])

        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(assertion, .stillOwned)
        XCTAssertTrue(holds)
        XCTAssertTrue(confident)
        XCTAssertFalse(foreignExists)
    }

    func testAssertConfirmationListFaultRetainsLeaseAndRecovers() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Initial LIST sees only our lock. writeOwnLock succeeds. Confirmation LIST faults transiently.
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueListError(RemoteErrorFixtures.retryable)

        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)
        let deleted = await client.deletedPaths
        let lockStillThere = await client.lockExists(basePath: basePath, writerID: me)

        XCTAssertEqual(assertion, .faulted(.retryable),
                       "a transient confirmation LIST fault must not abandon ownership")
        XCTAssertTrue(holds, "the lease is retained through a transient confirmation fault")
        XCTAssertFalse(confident, "confidence drops for the moment")
        XCTAssertFalse(deleted.contains(lockPath(me)), "own lock must not be deleted on a transient fault")
        XCTAssertTrue(lockStillThere, "own lock survives a transient confirmation fault")

        // Recovery: a later successful refresh within the window re-proves ownership.
        let later = base.addingTimeInterval(60)
        await client.setPendingUploadModificationDate(later)
        let refresh = await service.refresh(now: later)
        let recovered = await service.hasLeaseConfidence(now: later)
        XCTAssertEqual(refresh, .refreshed)
        XCTAssertTrue(recovered, "a successful in-window refresh recovers confidence after a transient fault")
    }

    func testRefreshRestoresConfidenceAfterNoteConfidenceLossWithinWindow() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        await service.noteConfidenceLoss()
        let afterLoss = await service.hasLeaseConfidence(now: base)
        XCTAssertFalse(afterLoss, "noteConfidenceLoss drops confidence until re-proven")

        // A successful in-window refresh re-proves ownership (lock could not have been reclaimed yet).
        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refreshed = await service.refresh(now: later)
        let confident = await service.hasLeaseConfidence(now: later)

        XCTAssertEqual(refreshed, .refreshed)
        XCTAssertTrue(confident, "an in-window refresh recovers confidence after noteConfidenceLoss")
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

    func testRefreshRestoresConfidenceAfterTransientRefreshFault() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // One transient refresh transport failure degrades confidence but keeps ownership.
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)
        let degraded = await service.refresh(now: base)
        XCTAssertEqual(degraded, .degraded(.retryable))
        let afterFault = await service.hasLeaseConfidence(now: base)
        XCTAssertFalse(afterFault)

        // The next successful refresh within the window re-proves ownership and recovers confidence.
        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refreshed = await service.refresh(now: later)
        let confident = await service.hasLeaseConfidence(now: later)

        XCTAssertEqual(refreshed, .refreshed)
        XCTAssertTrue(confident, "an in-window refresh recovers confidence after one transient fault")
    }

    func testAssertStillOwnedWriteFaultDropsConfidenceThenRecovers() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Initial LIST sees a fresh own lock. writeOwnLock faults transiently: lease retained, faulted.
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)

        let assertion = await service.assertStillOwned(now: base)
        XCTAssertEqual(assertion, .faulted(.retryable),
                       "failed refresh must not return .stillOwned even with a fresh own lock")
        let afterAssert = await service.hasLeaseConfidence(now: base)
        XCTAssertFalse(afterAssert, "confidence must be degraded after writeOwnLock failure")
        let holds = await service.holdsLease
        XCTAssertTrue(holds, "a fresh own lock + transient write fault retains ownership")

        // The next successful refresh within the window recovers confidence (token/session still ours).
        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refreshed = await service.refresh(now: later)
        let confident = await service.hasLeaseConfidence(now: later)

        XCTAssertEqual(refreshed, .refreshed)
        XCTAssertTrue(confident, "an in-window refresh recovers confidence after a transient assert write fault")
    }

    func testAssertStillOwnedStaleOwnLockWriteFailureReturnsLost() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Advance past expiry + skew so the own lock appears stale (beyond the skew band).
        let expired = base.addingTimeInterval(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 1)

        // Initial LIST sees own lock with stale mtime. writeOwnLock fails.
        // A stale unrefreshed lock is reclaimable by another writer, so must return .lost.
        await client.enqueueListResult([
            makeLockEntry(basePath: basePath, writerID: me, modificationDate: base)
        ])
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)

        let assertion = await service.assertStillOwned(now: expired)
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
        let first = await service.assertStillOwned(now: base)
        XCTAssertEqual(first, .lost(.otherWriter))

        // Remove the foreign lock (competing writer finished and released).
        await client.removeLock(basePath: basePath, writerID: other)

        // Second assertion must still fail closed: ownership was already lost.
        let second = await service.assertStillOwned(now: base)
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
        let assertion = await service.assertStillOwned(now: base)
        let confident = await service.hasLeaseConfidence(now: base)

        XCTAssertEqual(assertion, .faulted(.retryable))
        XCTAssertFalse(confident)
    }

    // MARK: - Lease-confidence gate

    func testLeaseConfidenceGate() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let before = await service.hasLeaseConfidence(now: base)
        XCTAssertTrue(before)
        await service.noteConfidenceLoss()
        let after = await service.hasLeaseConfidence(now: base)
        XCTAssertFalse(after, "noteConfidenceLoss must drop confidence")

        // Elapsed-since-refresh: confidence expires after refreshInterval x2.
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

    func testForeignLockWithNilMtimeAndOldBodyWithoutTimestampForegroundTakesOver() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(other)))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertFalse(foreignExists)
    }

    func testForeignLockWithNilMtimeAndStaleBodyTimestampForegroundTakesOver() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: other,
            sessionToken: "stale-session",
            lockToken: "stale-token",
            generation: 1,
            writtenAt: stale(base)
        )
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil, body: body)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(other)))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertFalse(foreignExists)
    }

    func testForeignLockWithNilMtimeAndFreshBodyTimestampBlocksAcquisition() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: other,
            sessionToken: "fresh-session",
            lockToken: "fresh-token",
            generation: 1,
            writtenAt: fresh(base)
        )
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil, body: body)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .blocked)
        XCTAssertTrue(deleted.isEmpty)
        XCTAssertTrue(uploaded.isEmpty)
    }

    func testForeignLockWithNilMtimeAndFutureBodyTimestampBlocksAcquisition() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: other,
            sessionToken: "future-session",
            lockToken: "future-token",
            generation: 1,
            writtenAt: base.addingTimeInterval(60)
        )
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil, body: body)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(result, .blocked)
        XCTAssertTrue(deleted.isEmpty)
        XCTAssertTrue(uploaded.isEmpty)
        XCTAssertTrue(foreignExists)
    }

    func testForeignLockWithStaleMtimeAndFreshBodyTimestampBlocksTakeover() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: other,
            sessionToken: "fresh-session",
            lockToken: "fresh-token",
            generation: 1,
            writtenAt: fresh(base)
        )
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base), body: body)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(result, .blocked)
        XCTAssertTrue(deleted.isEmpty)
        XCTAssertTrue(uploaded.isEmpty)
        XCTAssertTrue(foreignExists)
    }

    func testForeignLockWithNilMtimeAndStaleBodyTimestampBackgroundTakesOver() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: other,
            sessionToken: "stale-session",
            lockToken: "stale-token",
            generation: 1,
            writtenAt: stale(base)
        )
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil, body: body)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(other)))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertFalse(foreignExists)
    }

    func testForeignLockWithNilMtimeAndFutureBodyTimestampBackgroundSkips() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: other,
            sessionToken: "future-session",
            lockToken: "future-token",
            generation: 1,
            writtenAt: base.addingTimeInterval(60)
        )
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil, body: body)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(result, .skipped)
        XCTAssertTrue(deleted.isEmpty)
        XCTAssertTrue(uploaded.isEmpty)
        XCTAssertTrue(foreignExists)
    }

    func testForeignLockWithStaleMtimeAndFreshBodyTimestampBackgroundSkips() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: other,
            sessionToken: "fresh-session",
            lockToken: "fresh-token",
            generation: 1,
            writtenAt: fresh(base)
        )
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base), body: body)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(result, .skipped)
        XCTAssertTrue(deleted.isEmpty)
        XCTAssertTrue(uploaded.isEmpty)
        XCTAssertTrue(foreignExists)
    }

    func testForeignInvalidLockWithNilMtimeBackgroundTakesOver() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: nil)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .background, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(other)))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertFalse(foreignExists)
    }

    func testOwnLockWithNilMtimeAndOldBodyWithoutTimestampReclaims() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: nil)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths
        let holds = await service.holdsLease

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(me)))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
        XCTAssertTrue(holds)
    }

    func testOwnUndecodableLockWithNilMtimeReclaims() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedUndecodableLock(basePath: basePath, writerID: me, modificationDate: nil)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .acquired)
        XCTAssertTrue(deleted.contains(lockPath(me)))
        XCTAssertTrue(uploaded.contains(lockPath(me)))
    }

    func testOwnLockWithNilMtimeAndFutureBodyTimestampBlocksAcquisition() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let body = LockFileBody(
            writerID: me,
            sessionToken: "future-session",
            lockToken: "future-token",
            generation: 1,
            writtenAt: base.addingTimeInterval(60)
        )
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: nil, body: body)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .blockedByOwnLock(ownBlock(.ownershipUnverified, retryAfter: nil)))
        XCTAssertTrue(deleted.isEmpty)
        XCTAssertTrue(uploaded.isEmpty)
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

    func testBackwardClockRefreshFailsClosedThenRecoversForward() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // Backward clock: refresh must fail closed (no upload, no confidence).
        let backward = base.addingTimeInterval(-60)
        await client.setPendingUploadModificationDate(backward)
        let refresh1 = await service.refresh(now: backward)
        XCTAssertEqual(refresh1, .degraded(.retryable),
                       "backward-clock refresh must not upload")
        let confident1 = await service.hasLeaseConfidence(now: backward)
        XCTAssertFalse(confident1)

        // A later forward refresh within the window re-proves ownership and recovers confidence.
        let later = base.addingTimeInterval(120)
        await client.setPendingUploadModificationDate(later)
        let refresh2 = await service.refresh(now: later)
        let confident = await service.hasLeaseConfidence(now: later)
        XCTAssertEqual(refresh2, .refreshed)
        XCTAssertTrue(confident, "a forward in-window refresh recovers confidence after a backward-clock blip")
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

        let assertion = await service.assertStillOwned(now: backward)
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

        let assertion = await service.assertStillOwned(now: nearExpiry)
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

        let assertTask = Task { await service.assertStillOwned(now: base) }

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
    // delete a newly acquired same-writer lock. stopAndRelease now awaits the refresh task before
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

    // MARK: - Tokenized lock body (Phase 2)

    func testLockFileCodecDecodesOldBodyWithoutWrittenAt() throws {
        let data = Data("""
        {"writerID":"writer","sessionToken":"session","lockToken":"token","generation":7}
        """.utf8)

        let body = try XCTUnwrap(LockFileCodec.decode(data))

        XCTAssertEqual(body.writerID, "writer")
        XCTAssertEqual(body.sessionToken, "session")
        XCTAssertEqual(body.lockToken, "token")
        XCTAssertEqual(body.generation, 7)
        XCTAssertNil(body.writtenAt)
    }

    func testLockFileCodecRoundTripsWrittenAt() throws {
        let body = LockFileBody(
            writerID: "writer",
            sessionToken: "session",
            lockToken: "token",
            generation: 7,
            writtenAt: base
        )

        let decoded = try XCTUnwrap(LockFileCodec.decode(try LockFileCodec.encode(body)))

        XCTAssertEqual(decoded, body)
        XCTAssertEqual(decoded.writtenAt, base)
    }

    func testOwnLockBodyCarriesWriterSessionTokenAndGeneration() async throws {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let data = await client.fileData(path: lockPath(me))
        let body = try XCTUnwrap(LockFileCodec.decode(try XCTUnwrap(data)))
        XCTAssertEqual(body.writerID, me, "lock body carries the writer ID")
        XCTAssertFalse(body.sessionToken.isEmpty, "lock body carries a session token")
        XCTAssertFalse(body.lockToken.isEmpty, "lock body carries a lock token")
        XCTAssertGreaterThanOrEqual(body.generation, 1, "lock body carries a write generation")
        XCTAssertEqual(body.writtenAt, base, "lock body carries the operation timestamp")
    }

    func testForegroundStaleTakeoverDeletesWhenSameTokenStillStale() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let foreignBody = LockFileBody(writerID: other, sessionToken: "sess-other", lockToken: "tok-other", generation: 3)
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base), body: foreignBody)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let ownExists = await client.lockExists(basePath: basePath, writerID: me)

        XCTAssertEqual(result, .acquired, "an unchanged stale foreign lock is safe to take over")
        XCTAssertTrue(deleted.contains(lockPath(other)), "the confirmed-stale foreign lock is deleted")
        XCTAssertTrue(ownExists)
    }

    func testForegroundStaleTakeoverFaultsWhenDownloadedLockCannotBeReadLocally() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        await client.enqueueDownloadWithoutLocalFile()
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let uploaded = await client.uploadedPaths

        XCTAssertEqual(result, .faulted(.retryable))
        XCTAssertFalse(deleted.contains(lockPath(other)))
        XCTAssertTrue(uploaded.isEmpty)
    }

    func testForegroundStaleTakeoverSkipsForeignLockFreshenedOnSecondConfirmation() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        // The foreign writer "refreshes" between the two confirmation reads: its mtime is bumped to fresh
        // after the first body read is served, so the second confirmation observes a fresh contender.
        let basePath = self.basePath
        let freshDate = fresh(base)
        await client.setOnDownload { path in
            if path == RepoLayoutLite.lockPath(basePath: basePath, writerID: other) {
                await client.setLockModificationDate(basePath: basePath, writerID: other, to: freshDate)
            }
        }

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        let ownExists = await client.lockExists(basePath: basePath, writerID: me)

        XCTAssertEqual(result, .blocked,
                       "a foreign lock that freshened during confirmation must block, not be taken over")
        XCTAssertFalse(deleted.contains(lockPath(other)), "a freshened foreign lock must not be deleted")
        XCTAssertTrue(foreignExists)
        XCTAssertFalse(ownExists, "no own lock is written when takeover is refused")
    }

    func testForegroundStaleTakeoverSkipsForeignLockFreshenedBeforeFinalDelete() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let readCounter = IntCounter()
        let basePath = self.basePath
        let freshDate = fresh(base)
        await client.setOnDownload { path in
            guard path == RepoLayoutLite.lockPath(basePath: basePath, writerID: other) else { return }
            if await readCounter.increment() == 2 {
                await client.setLockModificationDate(basePath: basePath, writerID: other, to: freshDate)
            }
        }

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        let ownExists = await client.lockExists(basePath: basePath, writerID: me)

        XCTAssertEqual(result, .blocked,
                       "a foreign lock freshened before final delete must block takeover")
        XCTAssertFalse(deleted.contains(lockPath(other)))
        XCTAssertTrue(foreignExists)
        XCTAssertFalse(ownExists)
    }

    func testReleaseDoesNotDeleteSameWriterSuccessorLock() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // A same-writer successor session overwrites the lock with a different session/token.
        let successor = LockFileBody(
            writerID: me, sessionToken: "successor-session", lockToken: "successor-token", generation: 1
        )
        await client.seedLock(basePath: basePath, writerID: me, modificationDate: base, body: successor)

        await service.release()

        let exists = await client.lockExists(basePath: basePath, writerID: me)
        let deleted = await client.deletedPaths
        XCTAssertTrue(exists, "release must not delete a same-writer successor's lock")
        XCTAssertFalse(deleted.contains(lockPath(me)), "no delete is issued for a foreign-session body")
    }

    // MARK: - Own-lock body proof on refresh/assert (R02 Fix A)

    func testOldSessionRefreshLosesLeaseAfterSuccessorOwnsLock() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)

        let oldSession = makeService(writerID: me, client: client)
        let oldAcquired = await oldSession.acquire(mode: .foreground, now: base)
        XCTAssertEqual(oldAcquired, .acquired)

        // A successor session (same writer ID, new tokens) acquires after the old lock expires.
        let takeoverTime = base.addingTimeInterval(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 1)
        let newSession = makeService(writerID: me, client: client)
        await client.setPendingUploadModificationDate(takeoverTime)
        let newAcquired = await newSession.acquire(mode: .foreground, now: takeoverTime)
        XCTAssertEqual(newAcquired, .acquired)

        // The old session refreshes within the confidence window. Filename matches, but the remote body
        // now proves the successor — the old session must lose the lease and must not overwrite it.
        let later = takeoverTime.addingTimeInterval(60)
        await client.setPendingUploadModificationDate(later)
        let refresh = await oldSession.refresh(now: later)
        let oldHolds = await oldSession.holdsLease
        let oldConfident = await oldSession.hasLeaseConfidence(now: later)

        XCTAssertEqual(refresh, .degraded(.retryable))
        XCTAssertFalse(oldHolds, "the old session must lose the lease once a successor owns the lock")
        XCTAssertFalse(oldConfident, "the old session must not regain confidence on a successor's lock")

        // The successor's lock body was not clobbered: it can still re-assert ownership.
        let successorStillOwned = await newSession.assertStillOwned(now: later)
        XCTAssertEqual(successorStillOwned, .stillOwned,
                       "the old session's refresh must not have overwritten the successor's lock")
    }

    func testOldSessionAssertLosesAfterSuccessorOwnsLock() async {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)

        let oldSession = makeService(writerID: me, client: client)
        let oldAcquired = await oldSession.acquire(mode: .foreground, now: base)
        XCTAssertEqual(oldAcquired, .acquired)

        let takeoverTime = base.addingTimeInterval(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 1)
        let newSession = makeService(writerID: me, client: client)
        await client.setPendingUploadModificationDate(takeoverTime)
        let newAcquired = await newSession.acquire(mode: .foreground, now: takeoverTime)
        XCTAssertEqual(newAcquired, .acquired)

        // Old session re-asserts: filename scan sees an own-path lock, but the body is the successor's.
        let later = takeoverTime.addingTimeInterval(60)
        await client.setPendingUploadModificationDate(later)
        let assertion = await oldSession.assertStillOwned(now: later)
        let oldHolds = await oldSession.holdsLease

        XCTAssertEqual(assertion, .lost(.ownLockDeleted),
                       "a successor's body must make the old session's assertion fail closed")
        XCTAssertFalse(oldHolds)

        // The successor's lock was not reclaimed/overwritten by the old session's assertion.
        let successorStillOwned = await newSession.assertStillOwned(now: later)
        XCTAssertEqual(successorStillOwned, .stillOwned)
    }

    // A same-writer successor that reclaims the lock body *after* our writeOwnLock but *before* the
    // post-confirmation proof (the write→confirm window) is invisible to the filename-only confirmation
    // LIST. The final body re-proof must catch it and fail closed, so two same-writer sessions never both
    // believe they hold authority (FG suspended + BG launch un-suspends both with one shared writerID).
    func testAssertLosesWhenSuccessorReclaimsBodyInWriteConfirmWindow() async throws {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // The body our own session actually wrote, replayed for the pre-write proof (so it still reads ours).
        let ownBodyDataValue = await client.fileData(path: lockPath(me))
        let ownBodyData = try XCTUnwrap(ownBodyDataValue)
        let successor = LockFileBody(
            writerID: me, sessionToken: "successor-session", lockToken: "successor-token", generation: 99
        )
        let successorData = try LockFileCodec.encode(successor)

        // assertStillOwned proves the body twice (download): pre-write (ours) then post-confirmation. Script
        // the second read to observe a successor body that landed during the write→confirm window.
        await client.enqueueDownloadData(ownBodyData)
        await client.enqueueDownloadData(successorData)

        let assertion = await service.assertStillOwned(now: base)
        let holds = await service.holdsLease
        let confident = await service.hasLeaseConfidence(now: base)

        XCTAssertEqual(assertion, .lost(.ownLockDeleted),
                       "a same-writer successor reclaim in the write→confirm window must fail the assertion closed")
        XCTAssertFalse(holds, "the older session must drop the lease once a successor owns the lock body")
        XCTAssertFalse(confident, "confidence must not be restored on a successor's lock body")
    }

    // MARK: - Undecodable foreign lock recovery

    func testForegroundStaleTakeoverDeletesStableUndecodableForeignLock() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedUndecodableLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        let ownExists = await client.lockExists(basePath: basePath, writerID: me)

        XCTAssertEqual(result, .acquired,
                       "an unchanged expired undecodable foreign lock is recoverable by raw-byte proof")
        XCTAssertTrue(deleted.contains(lockPath(other)), "the stable undecodable foreign lock is deleted")
        XCTAssertFalse(foreignExists)
        XCTAssertTrue(ownExists)
    }

    func testForegroundStaleTakeoverBlocksUndecodableForeignLockWhenBytesChange() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let otherPath = lockPath(other)
        let staleDate = stale(base)
        await client.seedUndecodableLock(basePath: basePath, writerID: other, modificationDate: staleDate)
        await client.setPendingUploadModificationDate(base)
        let readCounter = IntCounter()
        await client.setOnDownload { path in
            guard path == otherPath else { return }
            if await readCounter.increment() == 1 {
                await client.seedFile(path: otherPath, data: Data("changed-lock-body".utf8), modificationDate: staleDate)
            }
        }
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        let ownExists = await client.lockExists(basePath: basePath, writerID: me)

        XCTAssertEqual(result, .blocked,
                       "changed undecodable bytes invalidate stale-lock proof")
        XCTAssertFalse(deleted.contains(otherPath))
        XCTAssertTrue(foreignExists)
        XCTAssertFalse(ownExists)
    }

    func testForegroundStaleTakeoverBlocksUndecodableForeignLockWhenMtimeFreshens() async {
        let me = newWriterID()
        let other = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let otherPath = lockPath(other)
        await client.seedUndecodableLock(basePath: basePath, writerID: other, modificationDate: stale(base))
        await client.setPendingUploadModificationDate(base)
        let basePath = self.basePath
        let freshDate = fresh(base)
        await client.setOnDownload { path in
            if path == otherPath {
                await client.setLockModificationDate(basePath: basePath, writerID: other, to: freshDate)
            }
        }
        let service = makeService(writerID: me, client: client)

        let result = await service.acquire(mode: .foreground, now: base)
        let deleted = await client.deletedPaths
        let foreignExists = await client.lockExists(basePath: basePath, writerID: other)
        let ownExists = await client.lockExists(basePath: basePath, writerID: me)

        XCTAssertEqual(result, .blocked,
                       "a freshened undecodable foreign lock must block takeover")
        XCTAssertFalse(deleted.contains(otherPath))
        XCTAssertTrue(foreignExists)
        XCTAssertFalse(ownExists)
    }

    // MARK: - Ownership assertion cancellation classification

    // A cancelled ownership LIST (run torn down mid-assert) must surface as cancellation, never be
    // relabeled LiteRepoError.leaseConfidenceLost — otherwise a user pause becomes a lease-fail-fast
    // run-fatal that stops the whole month queue instead of a clean pause.
    func testCancelledOwnershipAssertionSurfacesAsCancellation() async throws {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let session = RepoLeaseSession(lock: service)
        await client.enqueueListError(CancellationError())   // the next ownership LIST is cancelled

        do {
            try await session.assertStillOwnedForWrite(now: base)
            XCTFail("a cancelled ownership LIST must throw")
        } catch {
            XCTAssertTrue(error is CancellationError, "cancellation must surface as cancellation")
            XCTAssertNotEqual(error as? LiteRepoError, .leaseConfidenceLost,
                              "the cancellation must not be relabeled as a confidence loss")
        }
    }

    // A genuine non-cancellation transport fault (unrecoverable here: no reconnect provider) still maps
    // to leaseConfidenceLost — the cancellation special-case must not swallow real faults.
    func testRetryableOwnershipAssertionStillMapsToConfidenceLoss() async throws {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        let session = RepoLeaseSession(lock: service)
        await client.enqueueListError(RemoteErrorFixtures.retryable)

        do {
            try await session.assertStillOwnedForWrite(now: base)
            XCTFail("an unrecoverable transient fault must throw")
        } catch {
            XCTAssertEqual(error as? LiteRepoError, .leaseConfidenceLost,
                           "a non-cancellation fault is still surfaced as confidence loss")
        }
    }

    // Sibling of the direct-LIST case: a pause/stop during the lock-client reconnect (after a retryable
    // ownership LIST) is swallowed by recoverLockClient's `catch { return false }`, leaving the stale
    // `.faulted(.retryable)` to be mapped. The torn-down run must still surface cancellation, not a
    // leaseConfidenceLost lease-fail-fast.
    func testCancelledReconnectDuringRetryableAssertSurfacesAsCancellation() async throws {
        let me = newWriterID()
        let client = InMemoryRemoteStorageClient()
        await client.seedDirectory(locksDirectory)
        await client.setPendingUploadModificationDate(base)
        let service = makeService(writerID: me, client: client)
        let acquired = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquired, .acquired)

        // A reconnect that throws models a pause interrupting the multi-round-trip connect().
        let session = RepoLeaseSession(lock: service, reconnectLockClient: { throw CancellationError() })
        await client.enqueueListError(RemoteErrorFixtures.retryable)   // first ownership LIST faults retryably

        // Cancel before the body runs (Task{} schedules; cancel() lands first), so Task.isCancelled is
        // true when the stale .faulted(.retryable) reaches the mapper.
        let task = Task { try await session.assertStillOwnedForWrite(now: base) }
        task.cancel()
        let result = await task.result

        switch result {
        case .success:
            XCTFail("a cancelled, recovery-failed ownership assertion must throw")
        case .failure(let error):
            XCTAssertTrue(error is CancellationError,
                          "a pause swallowed during reconnect must surface as cancellation, not leaseConfidenceLost")
            XCTAssertNotEqual(error as? LiteRepoError, .leaseConfidenceLost)
        }
    }

    // MARK: - Lease gates (read tier + write tier) must never write the lock

    // Regression for the concurrent-worker lock corruption: per-month read-tier checks AND per-flush
    // write-tier proofs must be lock-write-free. Many concurrent gates under a confident lease perform
    // zero lock uploads/deletes (the refresh task is the sole writer).
    func testConcurrentLeaseGatesNeverWriteLock() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let service = makeService(writerID: writerID, client: client)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)

        let session = RepoLeaseSession(lock: service)
        let path = lockPath(writerID)
        let now = base
        let uploadsBefore = await client.uploadedPaths.filter { $0 == path }.count
        let deletesBefore = await client.deletedPaths.filter { $0 == path }.count

        await withTaskGroup(of: Bool.self) { group in
            for _ in 0 ..< 12 {
                group.addTask { (try? await session.assertLeaseConfidence(now: now)) != nil }       // read tier
                group.addTask { (try? await session.assertLeaseProvenForWrite(now: now)) != nil }   // write tier
            }
            var allSucceeded = true
            for await ok in group where !ok { allSucceeded = false }
            XCTAssertTrue(allSucceeded, "every gate under a confident, owned lease must succeed")
        }

        let uploadsAfter = await client.uploadedPaths.filter { $0 == path }.count
        let deletesAfter = await client.deletedPaths.filter { $0 == path }.count
        XCTAssertEqual(uploadsAfter, uploadsBefore, "lease gates must never write the lock")
        XCTAssertEqual(deletesAfter, deletesBefore, "lease gates must never delete the lock")
    }

    // The write-tier gate proves ownership against the backend (read-only) and fails closed when our lock
    // is gone — but still never writes the lock.
    func testWriteGateProvesOwnershipAndFailsClosedWhenLockGone() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let service = makeService(writerID: writerID, client: client)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)

        let session = RepoLeaseSession(lock: service)
        let path = lockPath(writerID)

        // Owned lock present → write gate passes, no lock write.
        try await session.assertLeaseProvenForWrite(now: base)
        let uploadsAfter = await client.uploadedPaths.filter { $0 == path }.count
        let acquireUploads = 1
        XCTAssertEqual(uploadsAfter, acquireUploads, "write gate proves ownership read-only, never re-writes the lock")

        // Simulate a takeover: our lock vanishes from the backend.
        try await client.delete(path: path)
        let deletesAfterSetup = await client.deletedPaths.filter { $0 == path }.count

        do {
            try await session.assertLeaseProvenForWrite(now: base)
            XCTFail("write gate must fail closed when the own lock is gone")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let uploadsFinal = await client.uploadedPaths.filter { $0 == path }.count
        let deletesFinal = await client.deletedPaths.filter { $0 == path }.count
        XCTAssertEqual(uploadsFinal, acquireUploads, "a lost write gate must not re-create the lock")
        XCTAssertEqual(deletesFinal, deletesAfterSetup, "the gate itself must not delete the lock")
    }

    // On a confidence lapse the read-tier gate recovers by READING (read-only ownership proof) when the
    // lock is still ours — never rewriting it; and fails closed when the lock is gone — still never writing.
    func testReadGateRecoversByProofOnLapseAndFailsClosedWhenLockGone() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let service = makeService(writerID: writerID, client: client)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)

        let session = RepoLeaseSession(lock: service)
        let path = lockPath(writerID)
        let uploadsBefore = await client.uploadedPaths.filter { $0 == path }.count
        let lapsed = base.addingTimeInterval(WriteLockService.confidenceMaxAge + 1)

        // Lapsed but still owned → read-only proof recovers (no throw, no lock write).
        try await session.assertLeaseConfidence(now: lapsed)
        let uploadsAfterRecover = await client.uploadedPaths.filter { $0 == path }.count
        XCTAssertEqual(uploadsAfterRecover, uploadsBefore, "read-only recovery must not write the lock")

        // Lock gone + lapsed → fails closed, still no write.
        try await client.delete(path: path)
        let deletesBaseline = await client.deletedPaths.filter { $0 == path }.count
        do {
            try await session.assertLeaseConfidence(now: lapsed)
            XCTFail("a lapsed lease whose lock is gone must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
        let uploadsFinal = await client.uploadedPaths.filter { $0 == path }.count
        let deletesFinal = await client.deletedPaths.filter { $0 == path }.count
        XCTAssertEqual(uploadsFinal, uploadsBefore, "a lost read gate must not re-create the lock")
        XCTAssertEqual(deletesFinal, deletesBaseline, "the gate must not delete the lock")
    }

    // Write tier fails closed when a same-writer SUCCESSOR session has replaced our lock body — and must
    // not rewrite the successor's body (read-only).
    func testWriteGateFailsClosedOnSameWriterSuccessorBody() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let service = makeService(writerID: writerID, client: client)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)

        let session = RepoLeaseSession(lock: service)
        let path = lockPath(writerID)
        // A successor (same writerID, different session/lock tokens) overwrites our lock body, fresh.
        let successor = LockFileBody(
            writerID: writerID, sessionToken: UUID().uuidString, lockToken: UUID().uuidString,
            generation: 9, writtenAt: base
        )
        await client.seedLock(basePath: basePath, writerID: writerID, modificationDate: base, body: successor)
        let successorData = await client.fileData(path: path)
        let uploadsBefore = await client.uploadedPaths.filter { $0 == path }.count

        do {
            try await session.assertLeaseProvenForWrite(now: base)
            XCTFail("must fail closed on a successor body")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
        let uploadsAfter = await client.uploadedPaths.filter { $0 == path }.count
        let dataAfter = await client.fileData(path: path)
        XCTAssertEqual(uploadsAfter, uploadsBefore, "must not rewrite the successor's lock")
        XCTAssertEqual(dataAfter, successorData, "successor body must be untouched")
    }

    // Write tier fails closed on a FOREIGN writer whose LIST mtime looks stale/missing but whose body is
    // fresh (backend LIST mtime can lag the body — S3/Ceph) — the read-only path must body-confirm, not pass
    // it — and must neither write our own lock nor delete the foreign one.
    func testWriteGateFailsClosedOnForeignLockWithStaleMtimeButFreshBody() async throws {
        let staleListMtime = base.addingTimeInterval(-(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 60))
        try await assertWriteGateBlocksBodyFreshForeign(listMtime: staleListMtime)
    }

    func testWriteGateFailsClosedOnForeignLockWithNilMtimeButFreshBody() async throws {
        try await assertWriteGateBlocksBodyFreshForeign(listMtime: nil)
    }

    private func assertWriteGateBlocksBodyFreshForeign(listMtime: Date?) async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let service = makeService(writerID: writerID, client: client)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)

        let session = RepoLeaseSession(lock: service)
        let ownPath = lockPath(writerID)
        let foreignWriter = newWriterID()
        let foreignPath = lockPath(foreignWriter)
        let foreignBody = LockFileBody(
            writerID: foreignWriter, sessionToken: UUID().uuidString, lockToken: UUID().uuidString,
            generation: 1, writtenAt: base   // body is fresh even though the LIST mtime is stale/missing
        )
        await client.seedLock(basePath: basePath, writerID: foreignWriter, modificationDate: listMtime, body: foreignBody)
        let ownUploadsBefore = await client.uploadedPaths.filter { $0 == ownPath }.count

        do {
            try await session.assertLeaseProvenForWrite(now: base)
            XCTFail("must fail closed on a body-fresh foreign writer")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let ownUploadsAfter = await client.uploadedPaths.filter { $0 == ownPath }.count
        let foreignDeletes = await client.deletedPaths.filter { $0 == foreignPath }.count
        let foreignStillExists = await client.lockExists(basePath: basePath, writerID: foreignWriter)
        XCTAssertEqual(ownUploadsAfter, ownUploadsBefore, "must not write our own lock")
        XCTAssertEqual(foreignDeletes, 0, "read-only gate must not delete the foreign lock")
        XCTAssertTrue(foreignStillExists, "the foreign lock must remain (acquire/refresh own cleanup)")
    }

    // Write tier fails closed when our own lock is still present + ours but STALE (refresh dead past
    // expiry+skew) — a read-only gate can't refresh it, so it must not let a write proceed, and must not write.
    func testWriteGateFailsClosedOnStaleOwnLockWithoutWriting() async throws {
        let writerID = newWriterID()
        let client = InMemoryRemoteStorageClient()
        let service = makeService(writerID: writerID, client: client)
        let acquisition = await service.acquire(mode: .foreground, now: base)
        XCTAssertEqual(acquisition, .acquired)

        let session = RepoLeaseSession(lock: service)
        let path = lockPath(writerID)
        let uploadsBefore = await client.uploadedPaths.filter { $0 == path }.count
        let stale = base.addingTimeInterval(WriteLockService.expiry + WriteLockService.clockSkewTolerance + 1)

        do {
            try await session.assertLeaseProvenForWrite(now: stale)
            XCTFail("must fail closed on a stale own lock")
        } catch let error as LiteRepoError {
            // Stale-but-ours is a confidence loss (refresh task can reclaim), not an ownership loss.
            XCTAssertEqual(error, .leaseConfidenceLost)
        }
        let uploadsAfter = await client.uploadedPaths.filter { $0 == path }.count
        let leaseStillHeld = await service.holdsLease
        XCTAssertEqual(uploadsAfter, uploadsBefore, "a stale-own-lock gate must not write the lock")
        XCTAssertTrue(leaseStillHeld, "a stale-but-ours lock must not be torn down — the refresh task can reclaim it")
    }
}

private actor IntCounter {
    private var value = 0

    func increment() -> Int {
        value += 1
        return value
    }
}

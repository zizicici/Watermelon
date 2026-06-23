import XCTest
@testable import Watermelon

final class RemoteMoveReplaceTests: XCTestCase {
    private let finalPath = "/repo/item"
    private let tempPath = "/repo/item.tmp"
    private let backupPath = "/repo/item.bak"

    private let oldBytes = Data([0xAA, 0xBB])

    func testLostOwnershipAtRestoreProbeBlocksBackupOverFinal() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: finalPath, data: oldBytes)

        let ownership = PassThenLostOwnership(passCount: 3)
        do {
            try await RemoteMoveReplace.moveReplacing(
                client: client,
                tempPath: tempPath,
                finalPath: finalPath,
                backupPath: backupPath,
                ignoreCancellation: false,
                assertOwnership: { try ownership.assertWhileOwned() }
            )
            XCTFail("moveReplacing should have thrown once ownership was lost at the restore re-prove")
        } catch {
            // Expected: the post-restore assert propagates the lost-ownership error.
        }

        let finalData = await client.fileData(path: finalPath)
        let backupData = await client.fileData(path: backupPath)
        XCTAssertNil(finalData, "a lease lost before the restore move must not promote the backup to the canonical final")
        XCTAssertEqual(backupData, oldBytes, "the stranded backup must remain for a later owned recovery")
    }

    func testHeldOwnershipRestoresBackupToFinal() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: finalPath, data: oldBytes)

        do {
            try await RemoteMoveReplace.moveReplacing(
                client: client,
                tempPath: tempPath,
                finalPath: finalPath,
                backupPath: backupPath,
                ignoreCancellation: false,
                assertOwnership: {}
            )
            XCTFail("moveReplacing should still throw the temp→final move fault even on a successful restore")
        } catch {
            // Expected: the temp→final retry fault propagates after the owned restore.
        }

        let finalData = await client.fileData(path: finalPath)
        XCTAssertEqual(finalData, oldBytes, "an owned restore must promote the backup back to the canonical final")
    }

    // The fallback replace (direct overwrite refused → back up the prior final, then move temp onto final)
    // must NOT delete the backup before the caller validates the replacement: a later read-back mismatch
    // would otherwise discard the only recovery copy. The backup is retained as recovery scratch.
    func testFallbackReplaceRetainsBackupUntilCallerValidates() async throws {
        let client = InMemoryRemoteStorageClient()
        let newBytes = Data([0xCC, 0xDD])
        await client.seedFile(path: finalPath, data: oldBytes)   // existing canonical
        await client.seedFile(path: tempPath, data: newBytes)    // staged replacement
        // Force the fallback: the direct temp→final overwrite move fails (backend refuses overwrite).
        await client.enqueueMoveError(RemoteErrorFixtures.terminal)

        try await RemoteMoveReplace.moveReplacing(
            client: client,
            tempPath: tempPath,
            finalPath: finalPath,
            backupPath: backupPath,
            ignoreCancellation: false,
            assertOwnership: {}
        )

        let finalData = await client.fileData(path: finalPath)
        let backupData = await client.fileData(path: backupPath)
        XCTAssertEqual(finalData, newBytes, "the replacement is published to the canonical final")
        XCTAssertEqual(
            backupData, oldBytes,
            "the prior good final is retained as recovery scratch until the caller validates the replacement"
        )
    }

    // Finding 1 (R04): with backupExistingFinal, an existing canonical must be backed up BEFORE it is
    // overwritten, even on a backend that permits overwrite-by-move (where the direct branch would otherwise
    // replace it with no retained backup), so a failed caller read-back can recover the prior good copy.
    func testBackupExistingFinalBacksUpBeforeOverwriteOnPermittingBackend() async throws {
        let client = InMemoryRemoteStorageClient()
        let newBytes = Data([0xCC, 0xDD])
        await client.seedFile(path: finalPath, data: oldBytes)   // existing canonical
        await client.seedFile(path: tempPath, data: newBytes)    // staged replacement
        // No move error enqueued: the InMemory client permits overwrite-by-move, so without the flag the
        // direct branch would succeed and leave no backup.

        try await RemoteMoveReplace.moveReplacing(
            client: client,
            tempPath: tempPath,
            finalPath: finalPath,
            backupPath: backupPath,
            ignoreCancellation: false,
            assertOwnership: {},
            backupExistingFinal: true
        )

        let finalData = await client.fileData(path: finalPath)
        let backupData = await client.fileData(path: backupPath)
        XCTAssertEqual(finalData, newBytes, "the replacement is published to the canonical final")
        XCTAssertEqual(
            backupData, oldBytes,
            "an existing canonical is backed up before overwrite so a failed read-back can recover the prior good copy"
        )
    }

    // A create (no existing final) still uses the direct move and writes no backup, even with the flag set.
    func testBackupExistingFinalSkipsBackupWhenNoExistingFinal() async throws {
        let client = InMemoryRemoteStorageClient()
        let newBytes = Data([0xCC, 0xDD])
        await client.seedFile(path: tempPath, data: newBytes)   // no existing final

        try await RemoteMoveReplace.moveReplacing(
            client: client,
            tempPath: tempPath,
            finalPath: finalPath,
            backupPath: backupPath,
            ignoreCancellation: false,
            assertOwnership: {},
            backupExistingFinal: true
        )

        let finalData = await client.fileData(path: finalPath)
        let backupData = await client.fileData(path: backupPath)
        XCTAssertEqual(finalData, newBytes)
        XCTAssertNil(backupData, "a create (no existing final) needs no backup")
    }

    // Finding 1 (R05): with backupExistingFinal, a transient existence-probe fault fails safe to "assume an
    // existing final," but if the final is actually absent the backup move(final→backup) throws notFound. That
    // must degrade to the direct temp→final publish (nothing to back up), not abort a fresh-canonical flush.
    func testBackupExistingFinalFallsThroughToDirectPublishWhenProbeFaultsAndFinalAbsent() async throws {
        let client = InMemoryRemoteStorageClient()
        let newBytes = Data([0xCC, 0xDD])
        await client.seedFile(path: tempPath, data: newBytes)            // fresh: no existing final
        await client.enqueueExistsError(RemoteErrorFixtures.retryable)   // up-front existence probe faults

        try await RemoteMoveReplace.moveReplacing(
            client: client,
            tempPath: tempPath,
            finalPath: finalPath,
            backupPath: backupPath,
            ignoreCancellation: false,
            assertOwnership: {},
            backupExistingFinal: true
        )

        let finalData = await client.fileData(path: finalPath)
        let backupData = await client.fileData(path: backupPath)
        XCTAssertEqual(finalData, newBytes, "a transient probe fault on a fresh canonical must still publish directly, not abort")
        XCTAssertNil(backupData, "no backup is created when there is no existing final to protect")
    }

    // A fresh month (no prior canonical) published on an S3-style copy+delete backend whose temp-source delete
    // faults must NOT report a prior-canonical backup: the dst now holds our own fresh bytes, so reporting
    // `true` would let a later read-back failure "restore" that fresh copy from `.bak` instead of removing it.
    func testCopyDeleteBackendPartialPublishOnFreshMonthReportsNoPriorBackup() async throws {
        let client = InMemoryRemoteStorageClient()
        let newBytes = Data([0xCC, 0xDD])
        await client.seedFile(path: tempPath, data: newBytes)   // fresh: no existing final
        await client.setMoveAsCopyDelete(true)                  // move = copy + delete (S3-style)
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)   // the temp-source delete faults

        let backedUpPriorFinal = try await RemoteMoveReplace.moveReplacing(
            client: client,
            tempPath: tempPath,
            finalPath: finalPath,
            backupPath: backupPath,
            ignoreCancellation: false,
            assertOwnership: {},
            backupExistingFinal: true
        )

        let finalData = await client.fileData(path: finalPath)
        let backupData = await client.fileData(path: backupPath)
        let tempData = await client.fileData(path: tempPath)
        XCTAssertFalse(
            backedUpPriorFinal,
            "a fresh publish whose copy landed but temp-delete faulted backed up no prior canonical — it must report false"
        )
        XCTAssertEqual(finalData, newBytes, "the freshly published bytes remain canonical")
        XCTAssertNil(backupData, "no `.bak` masquerading as a prior canonical may be stranded for a fresh month")
        XCTAssertNil(tempData, "the leftover temp scratch is cleared")
    }

    private final class PassThenLostOwnership: @unchecked Sendable {
        private var remaining: Int
        init(passCount: Int) { remaining = passCount }
        func assertWhileOwned() throws {
            if remaining > 0 { remaining -= 1; return }
            throw LiteRepoError.ownershipLost
        }
    }
}

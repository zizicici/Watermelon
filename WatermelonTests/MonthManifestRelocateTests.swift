import XCTest
import GRDB
@testable import Watermelon

// Step 5 (P05-MonthManifestRelocate): explicit manifest layout, dormant Lite relocation,
// layout-gated discovery, and hardened flush (export + quick_check + read-back verify).
final class MonthManifestRelocateTests: XCTestCase {
    private let basePath = "/photos"
    private let year = 2024
    private let month = 3

    private var v1Layout: MonthManifestStore.ManifestLayout { .v1 }
    private var liteLayout: MonthManifestStore.ManifestLayout { .lite }

    // MARK: - Layout paths

    func testV1ManifestPathMatchesLegacyLayout() {
        XCTAssertEqual(
            v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month),
            "/photos/2024/03/.watermelon_manifest.sqlite"
        )
        XCTAssertEqual(
            v1Layout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month),
            "/photos/2024/03"
        )
    }

    func testLiteManifestPathUnderWatermelonMonths() {
        XCTAssertEqual(
            liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month),
            "/photos/.watermelon/months/2024-03.sqlite"
        )
        XCTAssertEqual(
            liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month),
            "/photos/.watermelon/months"
        )
    }

    func testResourceRemotePathStaysYearMonthFilenameForBothLayouts() {
        let resource = TestFixtures.remoteResource(
            year: year, month: month, contentHash: Data([0x01]), fileName: "IMG_0001.JPG"
        )
        // Data/resource path is layout-independent and must stay YYYY/MM/filename.
        XCTAssertEqual(resource.remoteRelativePath, "2024/03/IMG_0001.JPG")
    }

    // MARK: - Directory-valued manifest slot fails closed

    // A directory occupying the canonical Lite month-manifest path is damaged/foreign control state:
    // loadOrCreate must fail closed (existingLiteManifestConflict), not treat it as absent and mint a
    // fresh manifest that flushToRemote would then move over the directory.
    func testLiteLoadOrCreateFailsClosedWhenManifestPathIsDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        let manifestPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedDirectory(manifestPath)

        do {
            _ = try await MonthManifestStore.loadOrCreate(
                client: client,
                basePath: basePath,
                year: year,
                month: month,
                layout: .lite,
                assertOwnership: {}
            )
            XCTFail("a directory at the Lite manifest path must fail closed, not mint fresh")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .existingLiteManifestConflict(month: "2024-03"))
        }

        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "no manifest upload may be attempted over the directory-valued slot")
    }

    // A directory introduced at the canonical month path AFTER the store loaded (out-of-band/foreign
    // mutation) must fail the dirty flush closed before any move/delete — the load-time guard cannot see a
    // post-load mutation, and RemoteMoveReplace would otherwise move the directory aside and delete it.
    func testLiteFlushFailsClosedWhenCanonicalPathBecomesDirectoryAfterLoad() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        XCTAssertTrue(store.dirty)

        let manifestPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedDirectory(manifestPath)   // foreign directory appears at the canonical slot post-load

        do {
            _ = try await store.flushToRemote()
            XCTFail("flush must fail closed when the canonical month path is a directory")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .existingLiteManifestConflict(month: "2024-03"))
        }

        // No publish or delete touched the directory, and the month stays dirty for triage.
        let uploaded = await client.uploadedPaths
        let deleted = await client.deletedPaths
        let stillDirectory = try await client.metadata(path: manifestPath)?.isDirectory
        XCTAssertTrue(uploaded.isEmpty, "no temp manifest may be uploaded toward the directory-valued slot")
        XCTAssertTrue(deleted.isEmpty, "the foreign directory must not be moved/deleted")
        XCTAssertEqual(stillDirectory, true, "the directory at the canonical path is left intact")
        XCTAssertTrue(store.dirty, "the unflushed month stays dirty")
    }

    // When the flush-time type probe itself faults, the slot type is unresolved — it is NOT proof the slot
    // is a safe file/absent. The flush must fail closed (propagate the fault) rather than swallow it and let
    // the type-blind publish move/delete a directory it could not rule out.
    func testLiteFlushFailsClosedWhenCanonicalPathTypeProbeFaults() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        XCTAssertTrue(store.dirty)

        let manifestPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedDirectory(manifestPath)   // foreign directory at the canonical slot
        // The pre-publish type probe for that slot faults (one-shot), modelling a transient/WebDAV-style fault.
        await client.failMetadata(forPathSuffix: "2024-03.sqlite", error: RemoteErrorFixtures.retryable)

        do {
            _ = try await store.flushToRemote()
            XCTFail("flush must fail closed when the canonical-path type probe cannot be resolved")
        } catch {
            // An unresolved probe must surface (fail closed), not be treated as a safe slot.
        }

        let uploaded = await client.uploadedPaths
        let deleted = await client.deletedPaths
        let stillDirectory = try await client.metadata(path: manifestPath)?.isDirectory
        XCTAssertTrue(uploaded.isEmpty, "no temp manifest may be uploaded when the slot type is unresolved")
        XCTAssertTrue(deleted.isEmpty, "the foreign directory must not be moved/deleted")
        XCTAssertEqual(stillDirectory, true, "the directory at the canonical path is left intact")
        XCTAssertTrue(store.dirty, "the unflushed month stays dirty")
    }

    // A directory introduced at the canonical month path behind a seed must fail a seeded load closed. The
    // seeded path has no canonical probe of its own, so without this guard a clean/pre-covered month could be
    // certified complete (no-op flush returns before the dirty-flush guard) while its manifest slot is a directory.
    func testLiteLoadSeededFailsClosedWhenCanonicalManifestPathIsDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        let manifestPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedDirectory(manifestPath)
        let seed = MonthManifestStore.Seed(
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")],
            assets: [],
            assetResourceLinks: []
        )

        do {
            _ = try await MonthManifestStore.loadSeeded(
                client: client, basePath: basePath, year: year, month: month, seed: seed, layout: .lite,
                assertOwnership: {}
            )
            XCTFail("a directory at the canonical Lite month path must fail a seeded load closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .existingLiteManifestConflict(month: "2024-03"))
        }

        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "no manifest upload may be attempted over the directory-valued slot")
    }

    // An owned verify on a directory-valued canonical month slot is damaged/foreign control state: it must
    // fail closed (existingLiteManifestConflict, not the continuable missing-manifest signal) so a finalizer
    // can never certify the month completed over the directory.
    func testOwnedVerifyMonthFailsClosedWhenManifestPathIsDirectory() async throws {
        let client = InMemoryRemoteStorageClient()
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedDirectory(litePath)
        let service = RemoteIndexSyncService()

        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: LibraryMonthKey(year: year, month: month),
                layout: .lite, assertOwnership: {}
            )
            XCTFail("an owned verify on a directory-valued canonical slot must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .existingLiteManifestConflict(month: "2024-03"))
        }

        let uploaded = await client.uploadedPaths
        let deleted = await client.deletedPaths
        XCTAssertTrue(uploaded.isEmpty && deleted.isEmpty, "owned verify must not publish/delete over the directory slot")
    }

    // A read-only verify (no lease) treats a directory-valued slot like an absent manifest: evict, never throw.
    func testReadOnlyVerifyMonthEvictsDirectoryValuedSlotWithoutThrowing() async throws {
        let client = InMemoryRemoteStorageClient()
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedDirectory(litePath)
        let service = RemoteIndexSyncService()

        try await service.verifyMonth(
            client: client, basePath: basePath, month: LibraryMonthKey(year: year, month: month),
            layout: .lite, assertOwnership: nil
        )
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "a read-only verify performs no remote mutation over the directory slot")
    }

    // Full verify uses this to surface directory-valued month slots the read-plane digest scan skips, so an
    // owned verify can fail closed on them instead of the sweep silently certifying the repo healthy.
    func testDirectoryValuedLiteMonthSlotsDetectsOnlyMonthSlotDirectories() {
        let monthsDir = liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)
        func entry(_ name: String, isDirectory: Bool) -> RemoteStorageEntry {
            RemoteStorageEntry(path: "\(monthsDir)/\(name)", name: name, isDirectory: isDirectory, size: 0, creationDate: nil, modificationDate: nil)
        }
        let entries = [
            entry("2024-03.sqlite", isDirectory: true),    // directory occupying a month slot
            entry("2024-04.sqlite", isDirectory: false),   // a normal month manifest file
            entry("manifest_x.tmp", isDirectory: true),    // non-month directory (scratch-shaped)
            entry("notes", isDirectory: true)              // non-month directory
        ]

        let slots = RemoteIndexSyncService.directoryValuedLiteMonthSlots(in: entries)
        XCTAssertEqual(
            slots, [LibraryMonthKey(year: 2024, month: 3)],
            "only a directory whose name is a <YYYY-MM>.sqlite month slot counts; files and non-month dirs are ignored"
        )
    }

    // MARK: - Flush hardening

    func testLiteFlushRelocatesManifestAndKeepsDataPaths() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        XCTAssertTrue(store.dirty)

        let flushed = try await store.flushToRemote()
        XCTAssertTrue(flushed)
        XCTAssertFalse(store.dirty)

        // Manifest lives at the Lite path; nothing landed at the V1 path.
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let v1Path = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let liteData = await client.fileData(path: litePath)
        let v1Data = await client.fileData(path: v1Path)
        XCTAssertNotNil(liteData)
        XCTAssertNil(v1Data)

        // The manifest directory was created and the temp upload lived under it.
        let created = await client.createdDirectories
        XCTAssertTrue(created.contains("/photos/.watermelon/months"))
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.allSatisfy { $0.hasPrefix("/photos/.watermelon/months/") })

        try assertPersistedManifestValid(liteData, expectedResourceCount: 1)
        XCTAssertEqual(store.monthRelativePath, "2024/03")
    }

    func testV1FlushKeepsLegacyManifestPath() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )

        _ = try await store.flushToRemote()

        let v1Path = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let v1Data = await client.fileData(path: v1Path)
        XCTAssertNotNil(v1Data)
        let liteData = await client.fileData(path: liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month))
        XCTAssertNil(liteData)
        try assertPersistedManifestValid(v1Data, expectedResourceCount: 1)
    }

    func testFlushReadBackMismatchRetriesAndSucceeds() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))

        let flushed = try await store.flushToRemote()

        XCTAssertTrue(flushed)
        XCTAssertFalse(store.dirty)
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 2)
    }

    func testFlushReadBackMismatchTwiceThrowsAndKeepsDirty() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))

        do {
            _ = try await store.flushToRemote()
            XCTFail("flush should fail when the read-back bytes differ from the uploaded manifest")
        } catch {
            assertReadBackVerificationError(error)
        }
        XCTAssertTrue(store.dirty, "a read-back mismatch must keep the manifest dirty for retry")
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 2)
    }

    // A fresh Lite month (no prior canonical) whose published bytes prove byte-wrong on read-back must not be
    // left as an unverified canonical with no recovery scratch: a later loadOrCreate would wedge on invalid
    // bytes (orphan cleanup cannot repair a canonical with no surviving scratch). The proven-bad fresh
    // canonical is removed under ownership so the month routes recoverable next run, mirroring the version
    // commit path's removeProvenBadCanonical.
    func testFreshLiteFlushReadBackMismatchRemovesProvenBadCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // read-back attempt 1 mismatches
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // read-back attempt 2 mismatches

        do {
            _ = try await store.flushToRemote()
            XCTFail("a byte-mismatched read-back on a fresh month must throw")
        } catch {
            assertReadBackVerificationError(error)
        }

        let canonical = await client.fileData(path: finalPath)
        let deleted = await client.deletedPaths
        XCTAssertNil(canonical, "the proven-bad fresh canonical must be removed so the month routes recoverable")
        XCTAssertTrue(deleted.contains(finalPath), "the fresh-canonical recovery is a delete under ownership")
        XCTAssertTrue(store.dirty, "the month stays dirty so the next run re-mints and re-flushes")
    }

    // A fresh-month flush whose read-back fails closed but lacks the write lease must NOT delete the canonical:
    // ownership gates the recovery delete exactly as it gates the publish, so a lost lease leaves the canonical
    // for a successor rather than letting a stale writer remove it.
    func testFreshLiteFlushReadBackMismatchKeepsCanonicalWhenOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        // Owned through flush-start, the move helper's pre-probe assertion, and its post-probe re-assertion
        // (so the publish lands), then lost exactly when the recovery delete re-proves ownership (the fourth
        // assertion), so the canonical must be left for a successor.
        let gate = OwnershipGate([true, true, true, false])
        let store = try makeStore(client: client, layout: .lite) {
            if await gate.next() == false { throw LiteRepoError.ownershipLost }
        }
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // read-back attempt 1 mismatches
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // read-back attempt 2 mismatches

        do {
            _ = try await store.flushToRemote()
            XCTFail("a byte-mismatched read-back must throw")
        } catch {
            assertReadBackVerificationError(error)
        }

        let canonical = await client.fileData(path: finalPath)
        XCTAssertNotNil(canonical, "a lost lease must not let the recovery delete remove the canonical")
    }

    func testFlushReadBackDownloadErrorRetriesAndSucceeds() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "retry.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        let flushed = try await store.flushToRemote()

        XCTAssertTrue(flushed)
        XCTAssertFalse(store.dirty)
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 2)
    }

    func testFlushReadBackDownloadErrorTwiceThrowsAndKeepsDirty() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAC]), fileName: "retry-fail.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        do {
            _ = try await store.flushToRemote()
            XCTFail("flush should fail when read-back download fails twice")
        } catch {
            assertReadBackVerificationError(error)
        }
        XCTAssertTrue(store.dirty, "a read-back download failure must keep the manifest dirty for retry")
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 2)
    }

    func testIgnoreCancellationVerifyReadBackCancellationHardFailsAndKeepsDirty() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        XCTAssertTrue(store.dirty)

        // The only download a flush performs is its post-commit read-back verification. Force that
        // read-back to observe task cancellation after the canonical manifest has been committed.
        await client.enqueueDownloadError(CancellationError())

        do {
            _ = try await store.flushToRemote(ignoreCancellation: true)
            XCTFail("read-back cancellation must hard-fail because durability was not verified")
        } catch {
            assertReadBackVerificationError(error)
        }

        // The canonical manifest committed before read-back, but the failed verification keeps it dirty.
        let finalPath = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let finalData = await client.fileData(path: finalPath)
        try assertPersistedManifestValid(finalData, expectedResourceCount: 1)
        XCTAssertTrue(store.dirty, "read-back cancellation must keep the manifest dirty for retry")
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 1)
    }

    func testIgnoreCancellationWrappedReadBackCancellationHardFailsAndKeepsDirty() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "wrapped-cancel.jpg")
        )

        let finalPath = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadError(RemoteStorageClientError.underlying(URLError(.cancelled)))

        do {
            _ = try await store.flushToRemote(ignoreCancellation: true)
            XCTFail("wrapped read-back cancellation must hard-fail because durability was not verified")
        } catch {
            assertReadBackVerificationError(error)
        }
        XCTAssertTrue(store.dirty, "wrapped read-back cancellation must keep the manifest dirty")
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 1)
    }

    func testCancelledFlushReadBackCancellationThrowsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        let finalPath = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadError(CancellationError())

        do {
            _ = try await store.flushToRemote()
            XCTFail("non-ignored read-back cancellation must surface as CancellationError")
        } catch is CancellationError {
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
        XCTAssertTrue(store.dirty, "read-back cancellation must keep the manifest dirty")
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 1)
    }

    func testCancelledFlushWrappedReadBackCancellationThrowsCancellationError() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "wrapped-cancel.jpg")
        )

        let finalPath = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadError(RemoteStorageClientError.underlying(URLError(.cancelled)))

        do {
            _ = try await store.flushToRemote()
            XCTFail("wrapped read-back cancellation must surface as CancellationError")
        } catch is CancellationError {
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }
        XCTAssertTrue(store.dirty, "wrapped read-back cancellation must keep the manifest dirty")
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 1)
    }

    func testIgnoreCancellationNonCancellationReadBackErrorRetriesDespiteAmbientCancellation() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        // Post-commit verify read-back fails with a non-cancellation transport error (timeout).
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        // Cancel the task as the canonical temp→final move commits, so retry proves non-cancellation
        // read-back errors are still recoverable when ignoreCancellation is set.
        let finalPath = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        final class CancelHandle { var cancel: (() -> Void)? }
        let handle = CancelHandle()
        await client.setOnMove { _, to in
            if to == finalPath { handle.cancel?() }
        }

        let task = Task { try await store.flushToRemote(ignoreCancellation: true) }
        handle.cancel = { task.cancel() }

        let flushed = try await task.value

        // Manifest committed before the transient read-back error; retry verifies the committed bytes.
        let finalData = await client.fileData(path: finalPath)
        XCTAssertNotNil(finalData, "the canonical manifest was committed before the read-back failed")
        XCTAssertTrue(flushed)
        XCTAssertFalse(store.dirty)
        let attempts = await client.downloadAttemptPaths
        XCTAssertEqual(attempts.filter { $0 == finalPath }.count, 2)
    }

    func testIgnoreCancellationReadBackRunsOutsideCancelledTask() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setRespectTaskCancellation(true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAC]), fileName: "shielded.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        final class CancelHandle { var cancel: (() -> Void)? }
        let handle = CancelHandle()
        await client.setOnMove { _, to in
            if to == finalPath { handle.cancel?() }
        }

        let task = Task { try await store.flushToRemote(ignoreCancellation: true) }
        handle.cancel = { task.cancel() }

        let flushed = try await task.value

        XCTAssertTrue(flushed)
        XCTAssertFalse(store.dirty, "ignoreCancellation read-back must verify durability even after ambient cancellation")
        let finalData = await client.fileData(path: finalPath)
        try assertPersistedManifestValid(finalData, expectedResourceCount: 1)
    }

    // MARK: - Phase 1: store-owned Lite write ownership gate (primitive flush)

    // A dirty Lite store with no write lease must fail closed and perform NO remote mutation.
    func testLiteFlushWithoutOwnershipFailsClosedAndPerformsNoRemoteMutation() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite)   // no ownership assertion
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        XCTAssertTrue(store.dirty)

        do {
            _ = try await store.flushToRemote()
            XCTFail("a dirty Lite flush with no ownership assertion must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
        XCTAssertTrue(store.dirty, "a fail-closed flush must leave the manifest dirty for retry")

        let created = await client.createdDirectories
        let uploaded = await client.uploadedPaths
        let moved = await client.movedPaths
        XCTAssertTrue(
            created.isEmpty && uploaded.isEmpty && moved.isEmpty,
            "no directory creation / upload / move may occur when the Lite write lease is absent"
        )
    }

    // A stored assertion returning false throws ownershipLost before directory creation/upload/move.
    func testLiteFlushWithOwnershipFalseThrowsBeforeAnyRemoteMutation() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: { throw LiteRepoError.ownershipLost })
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )

        do {
            _ = try await store.flushToRemote()
            XCTFail("a false ownership assertion must throw ownershipLost")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let created = await client.createdDirectories
        let uploaded = await client.uploadedPaths
        let moved = await client.movedPaths
        XCTAssertTrue(
            created.isEmpty && uploaded.isEmpty && moved.isEmpty,
            "ownershipLost must precede directory creation / upload / move"
        )
    }

    // A stored assertion returning true writes successfully through the gated primitive.
    func testLiteFlushWithOwnershipTrueWritesSuccessfully() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let flushed = try await store.flushToRemote()
        XCTAssertTrue(flushed)
        XCTAssertFalse(store.dirty)
        let liteData = await client.fileData(
            path: liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        )
        XCTAssertNotNil(liteData, "an owned Lite flush must persist the manifest")
    }

    func testLiteFlushDoesNotReassertOwnershipBetweenTempUploadAndMoveHelper() async throws {
        let client = InMemoryRemoteStorageClient()
        let recorder = MarkerRecorder()
        let store = try makeStore(client: client, layout: .lite) {
            await recorder.record()
        }
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAA]), fileName: "d.jpg")
        )

        let flushed = try await store.flushToRemote()

        XCTAssertTrue(flushed)
        let count = await recorder.count
        XCTAssertEqual(
            count, 3,
            "flush asserts once before mutation, once inside the move helper, and once more after the helper's final-existence probe before the direct publish"
        )
    }

    // A fresh Lite publish must re-prove ownership after the move helper's awaited final-existence probe: a
    // lease lost during that probe must fail closed before the direct temp→final move, never overwrite a
    // successor's freshly published canonical. The fresh flush's ownership calls are, in order: flush-start,
    // move-helper pre-probe, move-helper post-probe (the assertion this fix adds).
    func testFreshFlushReassertsOwnershipAfterExistenceProbeBeforeDirectPublish() async throws {
        let client = InMemoryRemoteStorageClient()
        let gate = OwnershipGate([true, true, false])
        let store = try makeStore(client: client, layout: .lite) {
            if await gate.next() == false { throw LiteRepoError.ownershipLost }
        }
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        do {
            _ = try await store.flushToRemote()
            XCTFail("losing ownership after the existence probe must fail closed before the direct publish")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let moves = await client.movedPaths
        XCTAssertFalse(
            moves.contains { $0.to == finalPath },
            "a stale-writer fresh publish must not move the temp manifest into the canonical path"
        )
        let finalData = await client.fileData(path: finalPath)
        XCTAssertNil(finalData, "no canonical manifest may be published after the post-probe ownership loss")
    }

    // V1 flush is never gated by the Lite ownership assertion, even if one is somehow present.
    func testV1FlushIgnoresLiteOwnershipGate() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1, liteWriteOwnership: { throw LiteRepoError.ownershipLost })
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        let flushed = try await store.flushToRemote()
        XCTAssertTrue(flushed, "V1 flush must not consult the Lite ownership gate")
        XCTAssertFalse(store.dirty)
    }

    // MARK: - Phase 1: loadManifestDirect schema-push ownership gating

    // A read-only Lite loadManifestDirect (default, no ownership) must not push the schema upgrade.
    func testLoadManifestDirectLiteReadDefaultDoesNotSchemaPushWithoutOwnership() async throws {
        let client = InMemoryRemoteStorageClient()
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: litePath, data: try makeLegacyManifestData())

        let store = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: basePath, year: year, month: month, layout: .lite
        )
        let unwrapped = try XCTUnwrap(store)
        XCTAssertTrue(unwrapped.dirty, "the pending schema upgrade is held in memory, not pushed")
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(
            uploaded.isEmpty,
            "a read-only Lite loadManifestDirect must not schema-push without ownership"
        )
    }

    // An owned Lite loadManifestDirect schema-push gates through the store and flushes when owned.
    func testLoadManifestDirectLiteOwnedSchemaPushGatesThroughStore() async throws {
        let client = InMemoryRemoteStorageClient()
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: litePath, data: try makeLegacyManifestData())

        let store = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: basePath, year: year, month: month, layout: .lite,
            assertOwnership: {}
        )
        let unwrapped = try XCTUnwrap(store)
        XCTAssertFalse(unwrapped.dirty, "an owned Lite schema-push flushes the upgrade")
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(
            uploaded.contains { $0.hasPrefix("/photos/.watermelon/months/") },
            "an owned Lite schema-push writes the upgraded manifest under the Lite months directory"
        )
    }

    // An owned Lite loadManifestDirect schema-push fails closed when ownership is lost.
    func testLoadManifestDirectLiteOwnedSchemaPushFailsClosedWhenOwnershipLost() async throws {
        let client = InMemoryRemoteStorageClient()
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: litePath, data: try makeLegacyManifestData())

        do {
            _ = try await MonthManifestStore.loadManifestDirect(
                client: client, basePath: basePath, year: year, month: month, layout: .lite,
                assertOwnership: { throw LiteRepoError.ownershipLost }
            )
            XCTFail("an owned Lite schema-push must fail closed when ownership is lost")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }
    }

    func testFlushReassertsOwnershipAfterTempUploadBeforePublish() async throws {
        let client = InMemoryRemoteStorageClient()
        let gate = OwnershipGate([true, false])
        let store = try makeStore(client: client, layout: .lite) {
            if await gate.next() == false { throw LiteRepoError.ownershipLost }
        }
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        do {
            _ = try await store.flushToRemote()
            XCTFail("losing ownership before publish must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let moves = await client.movedPaths
        XCTAssertFalse(moves.contains { $0.to == finalPath }, "ownership must be rechecked before publishing the canonical Lite manifest")
        let finalData = await client.fileData(path: finalPath)
        XCTAssertNil(finalData, "losing ownership after temp upload must not publish the canonical manifest")
    }

    func testFallbackReplaceReassertsOwnershipBeforeMovingCanonicalToBackup() async throws {
        let client = InMemoryRemoteStorageClient()
        let gate = OwnershipGate([true, true, false])
        let store = try makeStore(client: client, layout: .lite) {
            if await gate.next() == false { throw LiteRepoError.ownershipLost }
        }
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        let originalData = Data([0x01, 0x02, 0x03, 0x04])
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: finalPath, data: originalData)
        await client.seedDirectory(
            liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)
        )
        await client.enqueueMoveError(NSError(domain: "TestMove", code: 1))

        do {
            _ = try await store.flushToRemote()
            XCTFail("losing ownership inside fallback replace must fail closed")
        } catch let error as LiteRepoError {
            XCTAssertEqual(error, .ownershipLost)
        }

        let moves = await client.movedPaths
        XCTAssertFalse(moves.contains { $0.from == finalPath && $0.to.hasSuffix(".bak") })
        let finalData = await client.fileData(path: finalPath)
        XCTAssertEqual(finalData, originalData, "canonical manifest must not be moved after ownership is lost")
    }

    // V1 loadManifestDirect keeps its default schema-push behavior (ungated).
    func testLoadManifestDirectV1DefaultSchemaPushUnchanged() async throws {
        let client = InMemoryRemoteStorageClient()
        let v1Path = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: v1Path, data: try makeLegacyManifestData())

        let store = try await MonthManifestStore.loadManifestDirect(
            client: client, basePath: basePath, year: year, month: month, layout: .v1
        )
        let unwrapped = try XCTUnwrap(store)
        XCTAssertFalse(unwrapped.dirty, "V1 default schema-push still flushes the upgrade")
        let uploaded = await client.uploadedPaths
        XCTAssertTrue(
            uploaded.contains { $0.hasPrefix("/photos/2024/03/") },
            "V1 schema-push writes to the legacy in-place manifest directory"
        )
    }

    // MARK: - Re-flush over an existing canonical retains a backup (R04 F1)

    // A re-flush over an existing canonical month manifest must back up the prior manifest before overwrite
    // (even on the overwrite-permitting in-memory backend) so a failed read-back can recover it — then drop
    // that backup inline once the read-back proves the replacement durable, so a surviving month `.bak`
    // always signals an unverified replacement rather than a normal successful re-flush.
    func testLiteFlushBacksUpExistingCanonicalBeforeOverwriteThenDropsItOnSuccess() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthsDir = liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        let store1 = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store1.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await store1.flushToRemote()

        let store2 = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store2.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )
        _ = try await store2.flushToRemote()

        let moves = await client.movedPaths
        XCTAssertTrue(
            moves.contains { $0.from == finalPath && $0.to.hasSuffix(".bak") },
            "a re-flush must back up the prior canonical before overwriting it"
        )
        let entries = try await client.list(path: monthsDir)
        XCTAssertFalse(
            entries.contains { !$0.isDirectory && $0.name.hasSuffix(".bak") },
            "a verified re-flush must drop its now-redundant prior-canonical backup inline"
        )
    }

    // Finding 3 (R05): a re-flush whose read-back is byte-mismatched (but the canonical is still SQLite-valid)
    // must revert the canonical to the prior verified-good manifest, not leave the unverified replacement for
    // cleanup to keep while reclaiming the prior-good .bak as redundant scratch.
    func testLiteFlushRestoresPriorCanonicalOnReadBackMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let monthsDir = liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)

        // First flush establishes the prior verified-good canonical A.
        let storeA = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeA.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await storeA.flushToRemote()
        let canonicalA = await client.fileData(path: finalPath)

        // Second flush (manifest B) reads back mismatched bytes on both attempts → verify throws.
        let storeB = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeB.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // read-back attempt 1
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // read-back attempt 2

        do {
            _ = try await storeB.flushToRemote()
            XCTFail("a byte-mismatched read-back must throw")
        } catch {
            assertReadBackVerificationError(error)
        }

        let canonicalAfter = await client.fileData(path: finalPath)
        XCTAssertEqual(
            canonicalAfter, canonicalA,
            "a read-back mismatch must revert the canonical to the prior verified-good manifest"
        )
        let entries = try await client.list(path: monthsDir)
        XCTAssertFalse(
            entries.contains { !$0.isDirectory && $0.name.hasSuffix(".bak") },
            "the backup is consumed by the restore, leaving no stale scratch"
        )
        XCTAssertTrue(storeB.dirty, "a read-back mismatch keeps the store dirty for retry")
    }

    // The read-back-mismatch revert must re-prove ownership after its final-path existence probe: a lease lost
    // during that probe (e.g. a long background suspension) must not let the stale writer delete the canonical,
    // which could be a successor's freshly published month manifest.
    func testRevertReassertsOwnershipAfterFinalProbeBeforeDeletingCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        // Establish a prior canonical so the second flush takes the backup-first overwrite path.
        let storeA = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeA.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await storeA.flushToRemote()

        // storeB owns through publish and the revert's first ownership proof, then the gate runs out of `true`s
        // exactly at the revert's post-probe re-proof — the call this fix adds before deleting the canonical.
        let gate = OwnershipGate([true, true, true, true, true])
        let storeB = try makeStore(client: client, layout: .lite) {
            if await gate.next() == false { throw LiteRepoError.ownershipLost }
        }
        try storeB.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // read-back attempt 1
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // read-back attempt 2

        do {
            _ = try await storeB.flushToRemote()
            XCTFail("a byte-mismatched read-back must throw")
        } catch {
            assertReadBackVerificationError(error)
        }

        let canonicalAfter = await client.fileData(path: finalPath)
        XCTAssertNotNil(
            canonicalAfter,
            "a lease lost after the final-path probe must not delete the canonical during the revert"
        )
    }

    // MARK: - Owned verify persists schema-only upgrades (R01 F4)

    // An owned Lite maintenance verify of a legacy-ns month with no reconcile prune (touched == 0) must
    // still persist the schema migration; it must not return before the upgrade is flushed to remote.
    func testVerifyMonthOwnedLiteFlushesSchemaOnlyUpgrade() async throws {
        let client = InMemoryRemoteStorageClient()
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: litePath, data: try makeLegacyManifestData())

        let service = RemoteIndexSyncService()
        try await service.verifyMonth(
            client: client, basePath: basePath, month: LibraryMonthKey(year: year, month: month),
            layout: .lite, assertOwnership: {}
        )

        let flushedData = await client.fileData(path: litePath)
        let flushed = try XCTUnwrap(
            flushedData,
            "an owned Lite verify must leave the canonical manifest in place"
        )
        try assertManifestSchemaIsCurrent(flushed)
    }

    // A read-only Lite verify (no ownership) of a legacy-ns month must not write the remote manifest.
    func testVerifyMonthReadOnlyLiteDoesNotFlushSchemaUpgrade() async throws {
        let client = InMemoryRemoteStorageClient()
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: litePath, data: try makeLegacyManifestData())

        let service = RemoteIndexSyncService()
        try await service.verifyMonth(
            client: client, basePath: basePath, month: LibraryMonthKey(year: year, month: month),
            layout: .lite, assertOwnership: nil
        )

        let uploaded = await client.uploadedPaths
        XCTAssertTrue(uploaded.isEmpty, "a read-only Lite verify must never push a schema upgrade")
    }

    private func assertManifestSchemaIsCurrent(_ data: Data) throws {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("schema_\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: url) }
        try data.write(to: url)
        let queue = try DatabaseQueue(path: url.path)
        defer { try? queue.close() }
        let columns = try queue.read { db -> Set<String> in
            Set(try Row.fetchAll(db, sql: "PRAGMA table_info(resources)").compactMap { $0["name"] as String? })
        }
        XCTAssertTrue(columns.contains("creationDateMs"), "verify must persist the creationDateMs upgrade")
        XCTAssertFalse(columns.contains("creationDateNs"), "the legacy creationDateNs column must be gone after verify")
    }

    // MARK: - Relocation via loadOrCreate

    func testLoadOrCreateLiteRelocatesFreshManifest() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, layout: .lite,
            assertOwnership: {}
        )

        XCTAssertEqual(store.monthRelativePath, "2024/03")
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let v1Path = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let liteData = await client.fileData(path: litePath)
        let v1Data = await client.fileData(path: v1Path)
        XCTAssertNotNil(liteData, "fresh Lite month should flush its empty manifest to the Lite path")
        XCTAssertNil(v1Data)

        let created = await client.createdDirectories
        XCTAssertTrue(created.contains("/photos/2024/03"), "data dir is still created/listed")
        XCTAssertTrue(created.contains("/photos/.watermelon/months"))
    }

    // MARK: - Layout-gated discovery

    func testV1DiscoveryFindsLegacyManifestsAndIgnoresLite() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "/photos/2024/03/.watermelon_manifest.sqlite", data: Data([0x01]))
        await client.seedFile(path: "/photos/2024/05/.watermelon_manifest.sqlite", data: Data([0x01]))
        let service = RemoteIndexSyncService()

        let v1 = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .v1)
        XCTAssertEqual(Set(v1.keys), [LibraryMonthKey(year: 2024, month: 3), LibraryMonthKey(year: 2024, month: 5)])

        // No .watermelon/months directory exists → Lite discovery sees nothing.
        let lite = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
        XCTAssertTrue(lite.isEmpty)
    }

    func testLiteDiscoveryFindsRelocatedManifestsAndIgnoresV1() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: "/photos/.watermelon/months/2024-03.sqlite", data: Data([0x01]))
        await client.seedFile(path: "/photos/.watermelon/months/2024-11.sqlite", data: Data([0x01]))
        let service = RemoteIndexSyncService()

        let lite = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
        XCTAssertEqual(Set(lite.keys), [LibraryMonthKey(year: 2024, month: 3), LibraryMonthKey(year: 2024, month: 11)])

        let v1 = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .v1)
        XCTAssertTrue(v1.isEmpty)
    }

    func testLiteDiscoveryMissingMonthsDirectoryMeansNoMonths() async throws {
        let client = InMemoryRemoteStorageClient()   // nothing seeded
        let service = RemoteIndexSyncService()

        let lite = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
        XCTAssertTrue(lite.isEmpty)
    }

    func testLiteDiscoveryListErrorSurfaces() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.enqueueListError(RemoteErrorFixtures.retryable)   // non-notFound transport fault
        let service = RemoteIndexSyncService()

        do {
            _ = try await service.scanManifestDigests(client: client, basePath: basePath, layout: .lite)
            XCTFail("a non-notFound Lite list error must surface, not read as zero months")
        } catch {
            XCTAssertNotEqual(RemoteFaultLite.classify(error), .notFound)
        }
    }

    // MARK: - Fallback replace cancellation

    func testFallbackReplaceCancellationRestoresCanonicalMonth() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        let originalData = Data([0x01, 0x02, 0x03, 0x04])
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: finalPath, data: originalData)
        await client.seedDirectory(
            liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)
        )

        // An existing canonical routes through the backup-first replace; cancel when the backup move fires.
        final class CancelHandle { var cancel: (() -> Void)? }
        let handle = CancelHandle()
        await client.setOnMove { _, to in
            if to.hasSuffix(".bak") { handle.cancel?() }
        }

        let task = Task { try await store.flushToRemote() }
        handle.cancel = { task.cancel() }

        do {
            _ = try await task.value
        } catch is CancellationError {
            // Expected path: cancelled after backup move, before temp→final.
        }

        let finalData = await client.fileData(path: finalPath)
        XCTAssertNotNil(finalData, "canonical Lite month sqlite must not be absent after cancelled fallback replace")
        XCTAssertEqual(finalData, originalData, "restored canonical month sqlite must contain original bytes")
    }

    func testCancellationRestoreRunsInNonCancelledContext() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setRespectTaskCancellation(true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        let originalData = Data([0x01, 0x02, 0x03, 0x04])
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: finalPath, data: originalData)
        await client.seedDirectory(
            liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)
        )

        final class CancelHandle { var cancel: (() -> Void)? }
        let handle = CancelHandle()
        await client.setOnMove { _, to in
            if to.hasSuffix(".bak") { handle.cancel?() }
        }

        let task = Task { try await store.flushToRemote() }
        handle.cancel = { task.cancel() }

        do {
            _ = try await task.value
        } catch is CancellationError {
            // Expected: cancelled after backup move, before temp→final.
        }

        let finalData = await client.fileData(path: finalPath)
        XCTAssertNotNil(finalData, "canonical month sqlite must not be absent when restore runs in non-cancelled context")
        XCTAssertEqual(finalData, originalData, "restored month sqlite must contain original bytes")
    }

    func testIgnoreCancellationFallbackPublishRunsInNonCancelledContext() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setRespectTaskCancellation(true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        let originalData = Data([0x01, 0x02, 0x03, 0x04])
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: finalPath, data: originalData)
        await client.seedDirectory(
            liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)
        )

        final class CancelHandle { var cancel: (() -> Void)? }
        let handle = CancelHandle()
        await client.setOnMove { _, to in
            if to.hasSuffix(".bak") { handle.cancel?() }
        }

        let task = Task { try await store.flushToRemote(ignoreCancellation: true) }
        handle.cancel = { task.cancel() }

        let flushed = try await task.value

        let finalData = await client.fileData(path: finalPath)
        XCTAssertTrue(flushed)
        XCTAssertFalse(store.dirty)
        XCTAssertNotEqual(finalData, originalData, "ignoreCancellation should finish the fallback publish instead of rolling back")
        try assertPersistedManifestValid(finalData, expectedResourceCount: 1)
    }

    func testBackupMoveFailureRestoresCanonicalMonth() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        let originalData = Data([0x01, 0x02, 0x03, 0x04])
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: finalPath, data: originalData)
        await client.seedDirectory(
            liteLayout.manifestDirectoryAbsolutePath(basePath: basePath, year: year, month: month)
        )

        // First direct move fails → fallback branch.
        await client.enqueueMoveError(NSError(domain: "TestMove", code: 1))
        // Backup move applies server-side but throws to the client.
        await client.enqueueMovePostError(CancellationError())

        do {
            _ = try await store.flushToRemote()
        } catch is CancellationError {
            // Expected: backup move "succeeded" on server but threw to client.
        } catch {
            // Any error is acceptable as long as the invariant holds.
        }

        let finalData = await client.fileData(path: finalPath)
        XCTAssertNotNil(finalData, "canonical month sqlite must be restored when backup move throws after server-side effect")
        XCTAssertEqual(finalData, originalData, "restored month sqlite must contain original bytes")
    }

    // MARK: - Lite scratch naming (parseable / final-derived; V1 unchanged)

    func testLiteFlushTempScratchNameIsFinalDerivedAndParseable() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await store.flushToRemote()

        let uploaded = await client.uploadedPaths
        let tempName = try XCTUnwrap(uploaded.last.flatMap { $0.split(separator: "/").last.map(String.init) })
        XCTAssertTrue(tempName.hasPrefix("2024-03.sqlite."), "Lite temp scratch is final-derived")
        XCTAssertTrue(tempName.hasSuffix(".tmp"))
        XCTAssertEqual(
            RepoLayoutLite.month(fromScratchFilename: tempName),
            LibraryMonthKey(year: year, month: month),
            "the Lite temp scratch name must parse back to its canonical month"
        )
    }

    func testV1FlushTempScratchNameStaysOpaqueAndUnparseable() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )
        _ = try await store.flushToRemote()

        let uploaded = await client.uploadedPaths
        let tempName = try XCTUnwrap(uploaded.last.flatMap { $0.split(separator: "/").last.map(String.init) })
        XCTAssertTrue(tempName.hasPrefix("manifest_"), "V1 scratch name behavior is preserved")
        XCTAssertTrue(tempName.hasSuffix(".tmp"))
        XCTAssertNil(
            RepoLayoutLite.month(fromScratchFilename: tempName),
            "V1 opaque scratch names are intentionally not month-parseable"
        )
    }

    func testLiteScratchFilenameParsing() {
        XCTAssertEqual(
            RepoLayoutLite.month(fromScratchFilename: "2024-03.sqlite.\(UUID().uuidString).tmp"),
            LibraryMonthKey(year: 2024, month: 3)
        )
        XCTAssertEqual(
            RepoLayoutLite.month(fromScratchFilename: "2024-11.sqlite.\(UUID().uuidString).bak"),
            LibraryMonthKey(year: 2024, month: 11)
        )
        // Canonical (no scratch suffix), opaque legacy, empty-token, and out-of-range shapes do not parse.
        XCTAssertNil(RepoLayoutLite.month(fromScratchFilename: "2024-03.sqlite"))
        XCTAssertNil(RepoLayoutLite.month(fromScratchFilename: "manifest_abc.tmp"))
        XCTAssertNil(RepoLayoutLite.month(fromScratchFilename: "2024-03.sqlite..tmp"))
        XCTAssertNil(RepoLayoutLite.month(fromScratchFilename: "2024-13.sqlite.\(UUID().uuidString).tmp"))
    }

    // MARK: - Helpers

    private func makeStore(
        client: RemoteStorageClientProtocol,
        layout: MonthManifestStore.ManifestLayout,
        liteWriteOwnership: MonthManifestOwnershipAssertion? = nil
    ) throws -> MonthManifestStore {
        let localURL = MonthManifestStore.makeLocalManifestURL(year: year, month: month)
        let queue = try DatabaseQueue(path: localURL.path)
        try MonthManifestStore.migrate(queue)
        return MonthManifestStore(
            client: client,
            basePath: basePath,
            year: year,
            month: month,
            localManifestURL: localURL,
            dbQueue: queue,
            remoteFilesByName: [:],
            dirty: false,
            layout: layout,
            liteWriteOwnership: liteWriteOwnership
        )
    }

    private func assertReadBackVerificationError(
        _ error: Error,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        let ns = error as NSError
        XCTAssertEqual(ns.domain, "MonthManifestStore", file: file, line: line)
        XCTAssertEqual(ns.code, -36, file: file, line: line)
        XCTAssertTrue(MonthManifestStore.isReadBackVerificationError(error), file: file, line: line)
    }

    func testManifestIntegrityErrorsUseLocalizedDescriptionsAndStableCodes() {
        let quickCheck = MonthManifestStore.makeManifestQuickCheckError(results: ["row 1 failed"])
        XCTAssertEqual(quickCheck.domain, "MonthManifestStore")
        XCTAssertEqual(quickCheck.code, -37)
        XCTAssertTrue(quickCheck.localizedDescription.contains("row 1 failed"))
        XCTAssertFalse(quickCheck.localizedDescription.contains("quick_check"))
        XCTAssertFalse(quickCheck.localizedDescription.contains("MonthManifestStore"))
        XCTAssertFalse(quickCheck.localizedDescription.contains("Manifest integrity check failed before upload"))

        let underlying = NSError(
            domain: "ManifestReadBackTest",
            code: 7,
            userInfo: [NSLocalizedDescriptionKey: "wire failed"]
        )
        let readBack = MonthManifestStore.makeReadBackDownloadError(
            manifestPath: "2024/03",
            underlying: underlying
        )
        XCTAssertEqual(readBack.domain, "MonthManifestStore")
        XCTAssertEqual(readBack.code, -36)
        XCTAssertTrue(MonthManifestStore.isReadBackVerificationError(readBack))
        XCTAssertTrue(readBack.localizedDescription.contains("2024/03"))
        XCTAssertTrue(readBack.localizedDescription.contains("wire failed"))
        XCTAssertEqual((readBack.userInfo[NSUnderlyingErrorKey] as? NSError)?.domain, "ManifestReadBackTest")
        XCTAssertFalse(readBack.localizedDescription.contains("Failed to read back manifest for verification"))

        let mismatch = MonthManifestStore.makeReadBackMismatchError(
            manifestPath: "2024/03",
            expectedByteCount: 12,
            actualByteCount: 3
        )
        XCTAssertEqual(mismatch.domain, "MonthManifestStore")
        XCTAssertEqual(mismatch.code, -36)
        XCTAssertTrue(mismatch.localizedDescription.contains("2024/03"))
        XCTAssertTrue(mismatch.localizedDescription.contains("12"))
        XCTAssertTrue(mismatch.localizedDescription.contains("3"))
        XCTAssertFalse(mismatch.localizedDescription.contains("Manifest read-back mismatch"))

        let fallback = MonthManifestStore.makeReadBackVerificationError(manifestPath: "2024/03")
        XCTAssertEqual(fallback.domain, "MonthManifestStore")
        XCTAssertEqual(fallback.code, -36)
        XCTAssertTrue(fallback.localizedDescription.contains("2024/03"))
        XCTAssertFalse(fallback.localizedDescription.contains("Manifest read-back verification failed"))

        let refusal = MonthManifestStore.freshManifestRefusalError(year: 2024, month: 3)
        XCTAssertEqual(refusal.domain, "MonthManifestStore")
        XCTAssertEqual(refusal.code, -38)
        XCTAssertTrue(refusal.localizedDescription.contains("2024-03"))
        XCTAssertFalse(refusal.localizedDescription.contains("not confirmed absent"))
        XCTAssertFalse(refusal.localizedDescription.contains("refusing to create a fresh manifest"))
    }

    private actor OwnershipGate {
        private var results: [Bool]

        init(_ results: [Bool]) {
            self.results = results
        }

        func next() -> Bool {
            if results.isEmpty { return false }
            return results.removeFirst()
        }
    }

    // A downloaded manifest carrying legacy `creationDateNs`/`backedUpAtNs` columns. Opening it triggers
    // the in-place rename migration, so `requiresRemoteSync` is true — the schema-push that Phase 1 gates.
    private func makeLegacyManifestData() throws -> Data {
        let tmpDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("WT-legacy-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tmpDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tmpDir) }
        let dbURL = tmpDir.appendingPathComponent("legacy.sqlite")
        let queue = try DatabaseQueue(path: dbURL.path)
        try queue.write { db in
            try db.execute(sql: """
                CREATE TABLE resources (
                  fileName TEXT PRIMARY KEY NOT NULL,
                  contentHash BLOB NOT NULL,
                  fileSize INTEGER NOT NULL,
                  resourceType INTEGER NOT NULL,
                  creationDateNs INTEGER,
                  backedUpAtNs INTEGER NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE TABLE assets (
                  assetFingerprint BLOB PRIMARY KEY NOT NULL,
                  creationDateNs INTEGER,
                  backedUpAtNs INTEGER NOT NULL,
                  resourceCount INTEGER NOT NULL,
                  totalFileSizeBytes INTEGER NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE TABLE asset_resources (
                  assetFingerprint BLOB NOT NULL,
                  resourceHash BLOB NOT NULL,
                  role INTEGER NOT NULL,
                  slot INTEGER NOT NULL,
                  PRIMARY KEY(assetFingerprint, role, slot)
                )
                """)
        }
        try queue.close()
        return try Data(contentsOf: dbURL)
    }

    private func assertPersistedManifestValid(_ data: Data?, expectedResourceCount: Int) throws {
        let data = try XCTUnwrap(data)
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("verify_\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: url) }
        try data.write(to: url)
        let queue = try DatabaseQueue(path: url.path)
        defer { try? queue.close() }
        let check = try queue.read { try String.fetchAll($0, sql: "PRAGMA quick_check") }
        XCTAssertEqual(check, ["ok"])
        let count = try queue.read { try Int.fetchOne($0, sql: "SELECT COUNT(*) FROM resources") }
        XCTAssertEqual(count, expectedResourceCount)
    }
}

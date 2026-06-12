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

    func testFlushReadBackMismatchThrowsAndKeepsDirty() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        // The only download flush performs is its read-back verification: force it to return
        // bytes that differ from what was uploaded.
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))

        do {
            _ = try await store.flushToRemote()
            XCTFail("flush should fail when the read-back bytes differ from the uploaded manifest")
        } catch {
            let ns = error as NSError
            XCTAssertEqual(ns.domain, "MonthManifestStore")
            XCTAssertEqual(ns.code, -36)
        }
        XCTAssertTrue(store.dirty, "a read-back mismatch must keep the manifest dirty for retry")
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
            let ns = error as NSError
            XCTAssertEqual(ns.domain, "MonthManifestStore")
            XCTAssertEqual(ns.code, -36)
        }

        // The canonical manifest committed before read-back, but the failed verification keeps it dirty.
        let finalPath = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let finalData = await client.fileData(path: finalPath)
        try assertPersistedManifestValid(finalData, expectedResourceCount: 1)
        XCTAssertTrue(store.dirty, "read-back cancellation must keep the manifest dirty for retry")
    }

    func testIgnoreCancellationNonCancellationReadBackErrorStillHardFails() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .v1)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )

        // Post-commit verify read-back fails with a non-cancellation transport error (timeout).
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)

        // Cancel the task as the canonical temp→final move commits, so the read-back runs with
        // ambient Task.isCancelled == true — the non-cancellation error must still hard-fail.
        let finalPath = v1Layout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        final class CancelHandle { var cancel: (() -> Void)? }
        let handle = CancelHandle()
        await client.setOnMove { _, to in
            if to == finalPath { handle.cancel?() }
        }

        let task = Task { try await store.flushToRemote(ignoreCancellation: true) }
        handle.cancel = { task.cancel() }

        do {
            _ = try await task.value
            XCTFail("a non-cancellation read-back error must remain a hard -36 even when the task is cancelled")
        } catch {
            let ns = error as NSError
            XCTAssertEqual(ns.domain, "MonthManifestStore")
            XCTAssertEqual(ns.code, -36)
        }

        // Manifest committed before the failed read-back; the hard failure keeps it dirty for retry.
        let finalData = await client.fileData(path: finalPath)
        XCTAssertNotNil(finalData, "the canonical manifest was committed before the read-back failed")
        XCTAssertTrue(store.dirty, "a non-cancellation read-back failure must keep the manifest dirty")
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
        XCTAssertEqual(count, 2, "flush should assert once before mutation and once inside the move helper")
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

        // First direct move (temp→final) fails → enters fallback branch.
        await client.enqueueMoveError(NSError(domain: "TestMove", code: 1))

        // Cancel the task when the backup move (final→.bak) fires.
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

        await client.enqueueMoveError(NSError(domain: "TestMove", code: 1))

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

        await client.enqueueMoveError(NSError(domain: "TestMove", code: 1))

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

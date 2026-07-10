import XCTest
import GRDB
@testable import Watermelon

// Step 5 (P05-MonthManifestRelocate): explicit manifest layout, Lite relocation,
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

    // A definitive byte mismatch on the first read-back attempt followed by a transient download fault on the
    // second must still classify the fresh canonical proven-bad: the mismatch outranks the later transient
    // fault, so removeProvenBadFreshCanonical still runs rather than the failure being misread as transient.
    func testFreshLiteFlushReadBackMismatchThenDownloadFaultRemovesProvenBadCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // attempt 1: proven byte mismatch
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)   // attempt 2: transient download fault

        do {
            _ = try await store.flushToRemote()
            XCTFail("a proven read-back mismatch shadowed by a later transient fault must still throw")
        } catch {
            assertReadBackVerificationError(error)
            XCTAssertTrue(
                MonthManifestStore.isReadBackMismatchError(error),
                "the proven mismatch must outrank the later transient download fault"
            )
        }

        let canonical = await client.fileData(path: finalPath)
        let deleted = await client.deletedPaths
        XCTAssertNil(canonical, "the proven-bad fresh canonical must be removed even when attempt 2 faults")
        XCTAssertTrue(deleted.contains(finalPath), "the fresh-canonical recovery is a delete under ownership")
        XCTAssertTrue(store.dirty, "the month stays dirty so the next run re-mints and re-flushes")
    }

    // The reverse ordering — a transient download fault on the first attempt then a proven mismatch on the
    // second — must also classify the canonical proven-bad. Locks in that the proven-bad recovery is
    // independent of which attempt observed the definitive mismatch.
    func testFreshLiteFlushReadBackDownloadFaultThenMismatchRemovesProvenBadCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)   // attempt 1: transient download fault
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // attempt 2: proven byte mismatch

        do {
            _ = try await store.flushToRemote()
            XCTFail("a proven read-back mismatch on the second attempt must throw")
        } catch {
            assertReadBackVerificationError(error)
            XCTAssertTrue(MonthManifestStore.isReadBackMismatchError(error))
        }

        let canonical = await client.fileData(path: finalPath)
        let deleted = await client.deletedPaths
        XCTAssertNil(canonical, "the proven-bad fresh canonical must be removed when the mismatch lands second")
        XCTAssertTrue(deleted.contains(finalPath), "the fresh-canonical recovery is a delete under ownership")
        XCTAssertTrue(store.dirty, "the month stays dirty so the next run re-mints and re-flushes")
    }

    // On a backend whose MOVE isn't independent, a read-back not-found (the temp delete destroyed the moved final)
    // must fall back to a direct publish and memoize it, rather than failing the flush.
    // A non-independent MOVE backend publishes straight to the canonical: no temp, no MOVE, durable on the first
    // flush (the temp→MOVE→delete path — whose delete would alias-destroy the canonical — is never taken).
    func testLiteFlushOnNonIndependentMovePublishesDirectWithoutMove() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let published = try await store.flushToRemote()

        XCTAssertTrue(published)
        XCTAssertFalse(store.dirty)
        let canonical = await client.fileData(path: finalPath)
        XCTAssertNotNil(canonical, "the direct publish leaves a durable canonical")
        let moved = await client.movedPaths
        XCTAssertTrue(moved.isEmpty, "a non-independent MOVE backend must never publish via MOVE")
        // A fresh publish stages a recovery scratch then drops it on success — nothing lingers besides the canonical.
        let monthsDir = RepoLayoutLite.monthsDirectoryPath(basePath: basePath)
        let lingering = ((try? await client.list(path: monthsDir)) ?? []).map(\.name).filter { $0.hasSuffix(".tmp") || $0.hasSuffix(".bak") }
        XCTAssertEqual(lingering, [], "the recovery scratch is cleaned up after a verified publish")
    }

    // The proven-mismatch flag is store-level state; a prior failed publish that set it must not make a later
    // publish's catch skip re-verification and delete a valid canonical that landed during a post-effect failure.
    func testDirectPublishResetsStaleMismatchFlagBetweenAttempts() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        // Attempt 1: a corrupt canonical PUT sets readBackProvedCanonicalByteWrong and removes the bad canonical.
        await client.failUploadWritingCorruptBytes(Data([0x00, 0x01]), forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)
        do { _ = try await store.flushToRemote(); XCTFail("corrupt publish must surface") } catch {}
        let afterAttempt1 = await client.fileData(path: finalPath)
        XCTAssertNil(afterAttempt1, "attempt 1 removes the proven-bad canonical")

        // Attempt 2 (same store): a valid canonical lands but the response fails. The stale flag must not delete it.
        await client.failUploadAfterWrite(forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)
        do { _ = try await store.flushToRemote(); XCTFail("post-effect failure must surface") } catch {}
        let afterAttempt2 = await client.fileData(path: finalPath)
        XCTAssertNotNil(afterAttempt2, "a valid landed canonical must survive a stale-flag catch")
    }

    // The durable recovery scratch must be reflected in the session listing cache the moment it lands: if the
    // canonical PUT then fails, the fresh-over-scratch load guard and cleanup must see this sole recovery copy
    // rather than trust a stale cached listing (mirrors the temp→MOVE path's noteScratchCreated).
    func testDirectPublishNotesRecoveryScratchInSessionListingCache() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let listing = LiteMonthsListingSnapshot()
        await listing.seed(basePath: basePath, entries: [])   // activate the cache so noteScratchCreated updates it
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {}, liteMonthsListing: listing)
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        // The recovery scratch uploads first; fail the canonical PUT so the flush fails with the scratch as sole copy.
        await client.failUpload(forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)

        do { _ = try await store.flushToRemote(); XCTFail("canonical PUT failure must surface") } catch {}

        let entries = try await listing.entries(client: client, basePath: basePath)
        XCTAssertTrue(entries.contains { $0.name.hasSuffix(".tmp") }, "the recovery scratch is reflected in the session listing cache")
    }

    // Isolates the noteScratchCreated call: ownership fails at the pre-canonical re-assert (right after the scratch
    // upload), so the canonical PUT — and its catch-side invalidate — never runs. The scratch must still be in the
    // cache, which only noteScratchCreated can have done here.
    func testDirectPublishScratchStaysInCacheWhenPreCanonicalOwnershipReassertFails() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let listing = LiteMonthsListingSnapshot()
        await listing.seed(basePath: basePath, entries: [])
        // Ownership succeeds until the recovery scratch has uploaded (the first upload), then fails — hitting the
        // pre-canonical re-assert. No canonical PUT ⇒ no invalidate, so the cache reflects only noteScratchCreated.
        let store = try makeStore(
            client: client,
            layout: .lite,
            liteWriteOwnership: { if await client.uploadedPaths.isEmpty == false { throw LiteRepoError.ownershipLost } },
            liteMonthsListing: listing
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        do { _ = try await store.flushToRemote(); XCTFail("pre-canonical ownership re-assert failure must surface") } catch {}

        let entries = try await listing.entries(client: client, basePath: basePath)
        XCTAssertTrue(entries.contains { $0.name.hasSuffix(".tmp") }, "noteScratchCreated must reflect the uploaded recovery scratch")
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let canonical = await client.fileData(path: finalPath)
        XCTAssertNil(canonical, "the canonical PUT must not have run")
    }

    // Fresh direct PUT that lands bad bytes then throws: the success-path verify never ran, so the catch must
    // re-verify and remove the proven-byte-wrong fresh canonical rather than leave an invalid manifest for load.
    func testDirectPublishFreshCorruptUploadRemovesProvenBadCanonical() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.failUploadWritingCorruptBytes(Data([0x00, 0x01, 0x02]), forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)

        do {
            _ = try await store.flushToRemote()
            XCTFail("a corrupt fresh canonical PUT must surface")
        } catch {}

        let canonical = await client.fileData(path: finalPath)
        XCTAssertNil(canonical, "a proven-byte-wrong fresh canonical must be removed so the next run re-mints")
        XCTAssertTrue(store.dirty)
    }

    // Fresh direct PUT whose response fails but whose bytes landed valid: the verify matches, so the canonical is
    // durable and must NOT be removed.
    func testDirectPublishFreshKeepsValidCanonicalOnPostEffectUploadFailure() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.failUploadAfterWrite(forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)

        do {
            _ = try await store.flushToRemote()
            XCTFail("a post-effect upload failure must surface")
        } catch {}

        let canonical = await client.fileData(path: finalPath)
        XCTAssertNotNil(canonical, "a valid landed fresh canonical must not be removed")
        XCTAssertTrue(store.dirty)
    }

    // An independent-MOVE backend still uses temp→MOVE and surfaces a read-back not-found as a failure (dirty stays
    // for retry). There is no direct-publish fallback — that is reserved for non-independent-MOVE backends up front.
    func testFreshLiteFlushReadBackNotFoundOnIndependentMoveBackendSurfaces() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)

        do {
            _ = try await store.flushToRemote()
            XCTFail("an atomic-move backend must surface a read-back not-found, not fall back")
        } catch {
            assertReadBackVerificationError(error)
        }
        XCTAssertTrue(store.dirty)
    }

    // Direct overwrite of an existing canonical on a non-independent backend: even if both the overwrite and the
    // local rollback fail, the prior canonical must survive AND a durable remote `.bak` must be left for recovery.
    func testDirectPublishOverwriteFailureKeepsCanonicalAndDurableBackup() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let oldCanonical = Data([0x01, 0x02, 0x03, 0x04])
        await client.seedFile(path: finalPath, data: oldCanonical)
        await client.enqueueDownloadData(oldCanonical)                          // snapshot download → durable .bak
        await client.failUpload(forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)  // overwrite fails
        await client.failUpload(forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)  // rollback fails

        do {
            _ = try await store.flushToRemote()
            XCTFail("a failed direct overwrite must surface")
        } catch {
            // expected
        }

        let canonical = await client.fileData(path: finalPath)
        XCTAssertEqual(canonical, oldCanonical, "a failed overwrite must not lose the existing canonical")
        let uploaded = await client.uploadedPaths
        let deleted = await client.deletedPaths
        let bak = uploaded.first { $0.hasSuffix(".bak") }
        XCTAssertNotNil(bak, "a durable remote .bak of the prior canonical must be published before overwrite")
        XCTAssertFalse(deleted.contains(bak ?? ""), "the durable .bak must survive a failed publish for recovery")
        XCTAssertTrue(store.dirty)
    }

    // The overwrite lands server-side but the client sees a failure (and the rollback also fails): the durable
    // `.bak` holding the prior canonical must survive so a later run can recover.
    func testDirectPublishLandsServerSideButClientFailsKeepsDurableBackup() async throws {
        let client = InMemoryRemoteStorageClient(moveMayNotBeIndependent: true)
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        let oldCanonical = Data([0x01, 0x02, 0x03, 0x04])
        await client.seedFile(path: finalPath, data: oldCanonical)
        await client.enqueueDownloadData(oldCanonical)                                                   // snapshot → .bak
        await client.failUploadAfterWrite(forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)  // overwrite lands then fails
        await client.failUpload(forPathSuffix: finalPath, error: RemoteErrorFixtures.retryable)            // rollback fails

        do {
            _ = try await store.flushToRemote()
            XCTFail("a failed direct overwrite must surface")
        } catch {
            // expected
        }

        let uploaded = await client.uploadedPaths
        let deleted = await client.deletedPaths
        guard let bak = uploaded.first(where: { $0.hasSuffix(".bak") }) else {
            return XCTFail("a durable remote .bak must be published before overwrite")
        }
        XCTAssertFalse(deleted.contains(bak), "the durable .bak must survive when the overwrite lands but the client fails")
        let bakData = await client.fileData(path: bak)
        XCTAssertEqual(bakData, oldCanonical, "the .bak holds the recoverable prior canonical")
        XCTAssertTrue(store.dirty)
    }

    // A proven byte mismatch on attempt 0 followed by a genuine foreground cancellation on attempt 1 must still
    // remove the proven-bad fresh canonical: the surfaced error stays CancellationError (so the worker's
    // pause/stop handling is unchanged), but the proven-mismatch flag — not the error — drives the cleanup.
    func testFreshLiteFlushReadBackMismatchThenForegroundCancellationRemovesProvenBadCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // attempt 1: proven byte mismatch
        await client.enqueueDownloadError(CancellationError())             // attempt 2: cancellation during read-back

        do {
            _ = try await store.flushToRemote(ignoreCancellation: false)
            XCTFail("a cancellation during the read-back retry must surface as CancellationError")
        } catch is CancellationError {
            // Expected: teardown semantics preserved for the worker's pause/stop handling.
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }

        let canonical = await client.fileData(path: finalPath)
        let deleted = await client.deletedPaths
        XCTAssertNil(canonical, "a proven-bad fresh canonical must be removed even when the read-back retry is cancelled")
        XCTAssertTrue(deleted.contains(finalPath), "the fresh-canonical recovery is a delete under ownership")
        XCTAssertTrue(store.dirty, "the month stays dirty so the next run re-mints and re-flushes")
    }

    // Same proven-mismatch-then-cancellation shadowing, but via a wrapped `URLError(.cancelled)` on attempt 1
    // (URLSession-backed backends) rather than a bare `CancellationError`.
    func testFreshLiteFlushReadBackMismatchThenWrappedForegroundCancellationRemovesProvenBadCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))                              // attempt 1: mismatch
        await client.enqueueDownloadError(RemoteStorageClientError.underlying(URLError(.cancelled)))  // attempt 2: cancelled

        do {
            _ = try await store.flushToRemote(ignoreCancellation: false)
            XCTFail("a wrapped read-back cancellation must surface as CancellationError")
        } catch is CancellationError {
        } catch {
            XCTFail("expected CancellationError, got \(error)")
        }

        let canonical = await client.fileData(path: finalPath)
        let deleted = await client.deletedPaths
        XCTAssertNil(canonical, "a proven-bad fresh canonical must be removed even when the retry is wrapped-cancelled")
        XCTAssertTrue(deleted.contains(finalPath))
        XCTAssertTrue(store.dirty)
    }

    // A proven byte mismatch whose subsequent `.bak` existence probe transiently faults (so
    // restorePriorCanonicalFromBackup returns `.unresolved`, not `.noBackup`) must still remove the proven-bad
    // fresh canonical: the cleanup gates on the proven-mismatch flag and `restore != .restored`, not on `.noBackup`.
    func testFreshLiteFlushReadBackMismatchRemovesProvenBadCanonicalWhenBackupProbeFaults() async throws {
        let client = InMemoryRemoteStorageClient()
        let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xEF]), fileName: "c.jpg")
        )

        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // attempt 1: proven byte mismatch
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // attempt 2: proven byte mismatch
        // The restore path's existence probe for the (non-existent) fresh-month `.bak` transiently faults,
        // so restorePriorCanonicalFromBackup returns `.unresolved` rather than `.noBackup`.
        await client.failExists(forPathSuffix: ".bak", error: RemoteErrorFixtures.retryable)

        do {
            _ = try await store.flushToRemote()
            XCTFail("a byte-mismatched read-back must throw")
        } catch {
            assertReadBackVerificationError(error)
        }

        let canonical = await client.fileData(path: finalPath)
        let deleted = await client.deletedPaths
        XCTAssertNil(canonical, "an unresolved `.bak` probe must not suppress the proven-bad fresh-canonical removal")
        XCTAssertTrue(deleted.contains(finalPath), "the fresh-canonical recovery is a delete under ownership")
        XCTAssertTrue(store.dirty)
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

    // The read-back-mismatch revert re-proves ownership immediately before it deletes the bad replacement: a lease
    // lost there (e.g. a long background suspension) must not let the stale writer delete the canonical, which
    // could be a successor's freshly published month manifest.
    func testRevertReassertsOwnershipBeforeDeletingCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        // Establish a prior canonical so the second flush takes the backup-first overwrite path.
        let storeA = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeA.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await storeA.flushToRemote()

        // storeB owns through publish (flush-start + the three moveReplacing proofs), then the gate runs out of
        // `true`s exactly at the revert's pre-delete ownership re-proof — so the destructive delete is skipped.
        let gate = OwnershipGate([true, true, true, true])
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
            "a lease lost before the revert's delete must leave the existing canonical for a successor"
        )
    }

    // An EXISTING Lite month whose read-back mismatches and whose revert move (`.bak` → final) lands server-side
    // but then faults to the client (S3 copy+delete, server-side rename) returns `.unresolved` with the prior-good
    // already restored. The fresh-canonical cleanup must NOT delete it: the gate keys on whether a prior canonical
    // was backed up by the publish, never on the ambiguous restore-probe outcome.
    func testExistingLiteFlushKeepsRestoredCanonicalWhenRevertMoveLandsThenFaults() async throws {
        let client = InMemoryRemoteStorageClient()
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        // Establish prior verified-good canonical A.
        let storeA = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeA.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await storeA.flushToRemote()
        let canonicalA = await client.fileData(path: finalPath)
        XCTAssertNotNil(canonicalA)

        // storeB publishes B over A (backup-first), read-back proves B byte-wrong, then the revert's `.bak`→final
        // move lands on the server and faults to the client (so restore returns `.unresolved`, with A restored).
        let storeB = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeB.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // read-back attempt 1 mismatch
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // read-back attempt 2 mismatch
        await client.failMovePostEffect(fromPathSuffix: ".bak", error: RemoteErrorFixtures.retryable)

        do {
            _ = try await storeB.flushToRemote()
            XCTFail("a byte-mismatched read-back must throw")
        } catch {
            assertReadBackVerificationError(error)
        }

        let canonicalAfter = await client.fileData(path: finalPath)
        XCTAssertEqual(
            canonicalAfter, canonicalA,
            "an existing month's restored prior-good canonical must not be deleted by the fresh-canonical cleanup after an applied-but-faulted revert move"
        )
        XCTAssertTrue(storeB.dirty, "a read-back mismatch keeps the store dirty for retry")
    }

    // On a no-overwrite backend (SFTP/SMB), a transient fault on the restore's final-path existence probe must not
    // strand the prior-good `.bak` behind a byte-wrong canonical: the restore must still clear the bad replacement
    // and rename `.bak`→final. (The probe is no longer consulted; the bad final is deleted unconditionally under
    // ownership.) Without the fix, the `try?` probe collapses the fault to false, the delete is skipped, and the
    // `.bak`→final rename fails because the destination is occupied, leaving byte-wrong B canonical.
    func testExistingLiteFlushRestoresPriorCanonicalOnNoOverwriteBackendDespiteFinalProbeFault() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setRejectMoveOntoExistingDestination(true)   // SFTP/SMB: rename onto an occupied path fails
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        // Establish prior verified-good canonical A.
        let storeA = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeA.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await storeA.flushToRemote()
        let canonicalA = await client.fileData(path: finalPath)
        XCTAssertNotNil(canonicalA)

        // storeB publishes B over A (backup-first), read-back proves B byte-wrong. Both final-path existence probes
        // fault: the first is moveReplacing's up-front backup probe (fail-safe → still backup-first); the second is
        // the restore's probe, which the pre-fix code used a `try?` on to skip the delete.
        let storeB = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeB.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // read-back attempt 1 mismatch
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // read-back attempt 2 mismatch
        await client.failExists(forPathSuffix: ".sqlite", error: RemoteErrorFixtures.retryable)   // moveReplacing probe
        await client.failExists(forPathSuffix: ".sqlite", error: RemoteErrorFixtures.retryable)   // restore probe (pre-fix)

        do {
            _ = try await storeB.flushToRemote()
            XCTFail("a byte-mismatched read-back must throw")
        } catch {
            assertReadBackVerificationError(error)
        }

        let canonicalAfter = await client.fileData(path: finalPath)
        XCTAssertEqual(
            canonicalAfter, canonicalA,
            "on a no-overwrite backend, the restore must clear the byte-wrong canonical and rename the prior-good .bak back to final"
        )
        XCTAssertTrue(storeB.dirty, "a read-back mismatch keeps the store dirty for retry")
    }

    // On a no-overwrite backend, a *transient fault* on the restore's clear-final delete must be retried (not
    // swallowed) so the `.bak`→final rename can still land. Without the retry, the bad final stays occupied, the
    // rename fails, and cleanup keeps the SQLite-valid byte-wrong canonical with prior-good A stranded only as `.bak`.
    func testExistingLiteFlushRestoresPriorCanonicalOnNoOverwriteBackendWhenClearFinalDeleteFaultsTransiently() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.setRejectMoveOntoExistingDestination(true)   // SFTP/SMB: rename onto an occupied path fails
        let finalPath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)

        // Establish prior verified-good canonical A.
        let storeA = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeA.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
        )
        _ = try await storeA.flushToRemote()
        let canonicalA = await client.fileData(path: finalPath)
        XCTAssertNotNil(canonicalA)

        // storeB publishes B over A (backup-first), read-back proves B byte-wrong. The restore's first clear-final
        // delete faults transiently (no effect); the bounded retry must re-attempt it so the destination is cleared.
        let storeB = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
        try storeB.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xCD]), fileName: "b.jpg")
        )
        await client.enqueueDownloadData(Data([0xDE, 0xAD, 0xBE, 0xEF]))   // read-back attempt 1 mismatch
        await client.enqueueDownloadData(Data([0xBA, 0xAD, 0xF0, 0x0D]))   // read-back attempt 2 mismatch
        await client.enqueueDeleteError(RemoteErrorFixtures.retryable)     // restore's first clear-final delete faults

        do {
            _ = try await storeB.flushToRemote()
            XCTFail("a byte-mismatched read-back must throw")
        } catch {
            assertReadBackVerificationError(error)
        }

        let canonicalAfter = await client.fileData(path: finalPath)
        XCTAssertEqual(
            canonicalAfter, canonicalA,
            "the restore must retry a transient clear-final delete so the prior-good canonical is restored on no-overwrite backends"
        )
        XCTAssertTrue(storeB.dirty, "a read-back mismatch keeps the store dirty for retry")
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

    // A verify that prunes nothing (touched == 0) must still publish the freshly loaded current manifest to the
    // snapshot cache, so a download/restore consumer reads the verified month, not a staler cached one behind it.
    func testVerifyMonthPublishesCurrentManifestWhenNothingPruned() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthKey = LibraryMonthKey(year: year, month: month)
        let monthRel = String(format: "%04d/%02d", year, month)
        await client.seedDirectory("\(basePath)/\(monthRel)")
        await client.seedFile(path: "\(basePath)/\(monthRel)/bB.jpg", data: Data([0xBB]))
        // Current remote canonical (B): one resource whose data file is present ⇒ verify reconciles nothing.
        let store = try await MonthManifestStore.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, layout: .lite, assertOwnership: {}
        )
        try store.upsertResource(
            TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xBB]), fileName: "bB.jpg")
        )
        _ = try await store.flushToRemote()

        let service = RemoteIndexSyncService()
        // Seed a STALE cache (A) for the month holding a different resource than the current manifest.
        service.replaceCachedMonth(
            monthKey,
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xA1]), fileName: "stale.jpg")],
            assets: [],
            links: [],
            expectedProfileKey: nil
        )

        try await service.verifyMonth(
            client: client, basePath: basePath, month: monthKey, layout: .lite, assertOwnership: {}
        )

        let resources = service.fullSnapshot().resources
        XCTAssertTrue(
            resources.contains { $0.fileName == "bB.jpg" },
            "a verify that prunes nothing must still publish the current manifest to the cache"
        )
        XCTAssertFalse(
            resources.contains { $0.fileName == "stale.jpg" },
            "the stale cached month must be replaced by the freshly verified current manifest"
        )
    }

    // An owned verify that proves the canonical month sqlite absent must evict the stale cached month and surface
    // the confirmed-absent signal (-2), which the download path fails closed on (never restoring evicted cache).
    func testOwnedVerifyMissingCanonicalEvictsStaleCache() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthKey = LibraryMonthKey(year: year, month: month)
        let service = RemoteIndexSyncService()
        let fp = Data([0xFA])
        service.replaceCachedMonth(
            monthKey,
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAA]), fileName: "a.jpg")],
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            links: [TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: Data([0xAA]))],
            expectedProfileKey: nil
        )
        XCTAssertNotNil(service.remoteMonthRawData(for: monthKey), "precondition: the stale cache holds the month")

        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: monthKey, layout: .lite, assertOwnership: {}
            )
            XCTFail("an owned verify of an absent canonical must fail closed")
        } catch {
            let ns = error as NSError
            XCTAssertTrue(ns.domain == "RemoteIndexSyncService" && ns.code == -2, "expected the confirmed-absent (fail-closed) signal")
        }

        XCTAssertNil(
            service.remoteMonthRawData(for: monthKey),
            "owned verify proving the canonical absent must evict the stale cached month"
        )
    }

    // An owned verify whose canonical is present but loads as invalid SQLite (-34/-35) must evict the stale cached
    // month too, so a download cannot restore from a cache the current canonical can no longer substantiate.
    func testOwnedVerifyInvalidCanonicalEvictsStaleCache() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthKey = LibraryMonthKey(year: year, month: month)
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        // Canonical present as a regular file but holding non-SQLite bytes ⇒ loadManifestDirect throws -34.
        await client.seedFile(path: litePath, data: Data([0x00, 0x01, 0x02, 0x03]))
        let service = RemoteIndexSyncService()
        let fp = Data([0xFB])
        service.replaceCachedMonth(
            monthKey,
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xBB]), fileName: "b.jpg")],
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            links: [TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: Data([0xBB]))],
            expectedProfileKey: nil
        )
        XCTAssertNotNil(service.remoteMonthRawData(for: monthKey), "precondition: the stale cache holds the month")

        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: monthKey, layout: .lite, assertOwnership: {}
            )
            XCTFail("an owned verify of an invalid canonical must fail closed")
        } catch {
            let ns = error as NSError
            XCTAssertEqual(ns.domain, "MonthManifestStore")
            XCTAssertTrue(ns.code == -34 || ns.code == -35, "expected the invalid-downloaded-manifest error")
        }

        XCTAssertNil(
            service.remoteMonthRawData(for: monthKey),
            "owned verify proving the canonical invalid must evict the stale cached month"
        )
    }

    // metadata says the canonical is present, but the GET races a deletion and returns a clear backend
    // not-found. That is confirmed absence (not a transient fetch fault), so verify must evict the stale cache.
    func testOwnedVerifyDownloadNotFoundEvictsStaleCache() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthKey = LibraryMonthKey(year: year, month: month)
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: litePath, data: Data([0x01]))
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)
        let service = RemoteIndexSyncService()
        let fp = Data([0xFC])
        service.replaceCachedMonth(
            monthKey,
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xBB]), fileName: "b.jpg")],
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            links: [TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: Data([0xBB]))],
            expectedProfileKey: nil
        )
        XCTAssertNotNil(service.remoteMonthRawData(for: monthKey), "precondition: the stale cache holds the month")

        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: monthKey, layout: .lite, assertOwnership: {}
            )
            XCTFail("an owned verify whose canonical download not-founds must fail closed")
        } catch {
            let ns = error as NSError
            XCTAssertTrue(ns.domain == "RemoteIndexSyncService" && ns.code == -2, "expected the confirmed-absent (fail-closed) signal")
        }

        XCTAssertNil(
            service.remoteMonthRawData(for: monthKey),
            "a clear download not-found must evict the stale cached month like a metadata-nil canonical"
        )
    }

    // A transient (retryable) download fault is not proof the canonical is gone: verify must keep last-known-good
    // cache so the continuable download can still serve it, unlike the confirmed not-found case above.
    func testOwnedVerifyTransientDownloadFaultKeepsStaleCache() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthKey = LibraryMonthKey(year: year, month: month)
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: litePath, data: Data([0x01]))
        await client.enqueueDownloadError(RemoteErrorFixtures.retryable)
        let service = RemoteIndexSyncService()
        let fp = Data([0xFD])
        service.replaceCachedMonth(
            monthKey,
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xBB]), fileName: "b.jpg")],
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            links: [TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: Data([0xBB]))],
            expectedProfileKey: nil
        )
        XCTAssertNotNil(service.remoteMonthRawData(for: monthKey), "precondition: the stale cache holds the month")

        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: monthKey, layout: .lite, assertOwnership: {}
            )
            XCTFail("a transient download fault still throws the missing-manifest signal")
        } catch {
            let ns = error as NSError
            XCTAssertTrue(ns.domain == "RemoteIndexSyncService" && ns.code == -1, "expected the continuable missing-manifest signal")
        }

        XCTAssertNotNil(
            service.remoteMonthRawData(for: monthKey),
            "a transient download fault must keep last-known-good cache, not evict it"
        )
    }

    // An owned verify that prunes a MEANINGLESS asset (config-only — its only link is an adjustment sidecar, so
    // there's no real media to restore) but whose corrective flush trips a retryable transport fault must fail
    // the month closed (-3), not surface the raw retryable fault — which the download path treats as continuable
    // and would then restore from the still-stale cache the verify already proved must be corrected.
    func testOwnedVerifyReconcilePruneWithFailedFlushFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthKey = LibraryMonthKey(year: year, month: month)
        let monthRel = String(format: "%04d/%02d", year, month)
        // Resource file present on remote so the listing reconcile is a no-op; the only prune is the config-only asset.
        await client.seedDirectory("\(basePath)/\(monthRel)")
        await client.seedFile(path: "\(basePath)/\(monthRel)/a.jpg", data: Data([0xAB]))

        // Seed a canonical manifest holding a config-only asset (its only link is an adjustment sidecar, role 7),
        // so verify's reconcile prunes it as meaningless (no real media). The store is released at the end of this
        // scope so verify's download isn't racing an open SQLite queue.
        let badFingerprint = Data([0x99])
        do {
            let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
            try store.upsertResource(
                TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
            )
            try store.upsertAsset(
                TestFixtures.remoteAsset(year: year, month: month, fingerprint: badFingerprint),
                links: [TestFixtures.remoteLink(
                    year: year, month: month, assetFingerprint: badFingerprint, resourceHash: Data([0xAB]), role: ResourceTypeCode.adjustmentData
                )]
            )
            _ = try await store.flushToRemote()
        }

        // The corrective flush during verify trips a retryable transport fault before it can persist the prune.
        await client.enqueueUploadError(RemoteErrorFixtures.retryable)

        let service = RemoteIndexSyncService()
        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: monthKey, layout: .lite, assertOwnership: {}
            )
            XCTFail("a reconcile prune whose corrective flush failed must fail the month closed")
        } catch {
            let ns = error as NSError
            XCTAssertTrue(
                ns.domain == "RemoteIndexSyncService" && ns.code == -3,
                "expected the reconcile-flush-failed fail-closed signal, got \(ns.domain)/\(ns.code)"
            )
            XCTAssertFalse(
                HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(error),
                "a download must not continue over a manifest the verify proved invalid but could not durably correct"
            )
        }
    }

    // Sibling of the flush-failure case: after verify prunes a MEANINGLESS asset (config-only, no real media) in
    // memory, a retryable data-directory LIST fault in the same prove→publish window — before the corrective
    // flush is even reached — must also fail the month closed (-3), not surface a raw retryable error the download
    // path treats as continuable (restoring the un-pruned row from the still-stale cache).
    func testOwnedVerifyReconcilePruneWithFailedListFailsClosed() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthKey = LibraryMonthKey(year: year, month: month)
        let monthRel = String(format: "%04d/%02d", year, month)
        await client.seedDirectory("\(basePath)/\(monthRel)")
        await client.seedFile(path: "\(basePath)/\(monthRel)/a.jpg", data: Data([0xAB]))

        // Same config-only seed as the flush-failure sibling; released at the end of this scope so verify's
        // download isn't racing an open SQLite queue.
        let badFingerprint = Data([0x99])
        do {
            let store = try makeStore(client: client, layout: .lite, liteWriteOwnership: {})
            try store.upsertResource(
                TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xAB]), fileName: "a.jpg")
            )
            try store.upsertAsset(
                TestFixtures.remoteAsset(year: year, month: month, fingerprint: badFingerprint),
                links: [TestFixtures.remoteLink(
                    year: year, month: month, assetFingerprint: badFingerprint, resourceHash: Data([0xAB]), role: ResourceTypeCode.adjustmentData
                )]
            )
            _ = try await store.flushToRemote()
        }

        // The data-directory probe LIST during verify trips a retryable transport fault *after* reconcileMonth()
        // has already pruned the invalid row in memory (store dirty), before the correction can be published.
        await client.enqueueListError(RemoteErrorFixtures.retryable)

        let service = RemoteIndexSyncService()
        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: monthKey, layout: .lite, assertOwnership: {}
            )
            XCTFail("a reconcile prune whose data-directory listing then faulted must fail the month closed")
        } catch {
            let ns = error as NSError
            XCTAssertTrue(
                ns.domain == "RemoteIndexSyncService" && ns.code == -3,
                "expected the reconcile-flush-failed fail-closed signal, got \(ns.domain)/\(ns.code)"
            )
            XCTAssertFalse(
                HomeExecutionCoordinator.shouldContinueDownloadAfterVerifyFailure(error),
                "a download must not continue over a manifest the verify proved invalid but could not durably correct"
            )
        }
    }

    // The initial canonical download succeeds (legacy schema), but the owned schema-upgrade flush's read-back GET
    // returns a not-found that the -36 read-back error wraps. That later write/read-back boundary failure must
    // surface (and keep the valid cache) — it must NOT be classified as the initial-download absence and converted
    // into the continuable missing-manifest path.
    func testOwnedVerifySchemaFlushReadBackNotFoundSurfacesAndKeepsCache() async throws {
        let client = InMemoryRemoteStorageClient()
        let monthKey = LibraryMonthKey(year: year, month: month)
        let litePath = liteLayout.manifestAbsolutePath(basePath: basePath, year: year, month: month)
        await client.seedFile(path: litePath, data: try makeLegacyManifestData())
        await client.enqueueDownloadData(try makeLegacyManifestData())   // canonical download (loadManifestDirect)
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)  // schema-flush read-back attempt 1
        await client.enqueueDownloadError(RemoteErrorFixtures.notFound)  // schema-flush read-back attempt 2
        let service = RemoteIndexSyncService()
        let fp = Data([0xFE])
        service.replaceCachedMonth(
            monthKey,
            resources: [TestFixtures.remoteResource(year: year, month: month, contentHash: Data([0xBB]), fileName: "b.jpg")],
            assets: [TestFixtures.remoteAsset(year: year, month: month, fingerprint: fp)],
            links: [TestFixtures.remoteLink(year: year, month: month, assetFingerprint: fp, resourceHash: Data([0xBB]))],
            expectedProfileKey: nil
        )
        XCTAssertNotNil(service.remoteMonthRawData(for: monthKey), "precondition: the cache holds the month")

        do {
            try await service.verifyMonth(
                client: client, basePath: basePath, month: monthKey, layout: .lite, assertOwnership: {}
            )
            XCTFail("a schema-flush read-back failure must surface, not be masked as continuable missing")
        } catch {
            XCTAssertTrue(
                MonthManifestStore.isReadBackVerificationError(error),
                "the schema-flush read-back failure (-36) must surface, not be converted to the continuable -1"
            )
            let ns = error as NSError
            XCTAssertFalse(
                ns.domain == "RemoteIndexSyncService" && ns.code == -1,
                "a read-back failure wrapping a not-found must not become the continuable missing signal"
            )
        }

        XCTAssertNotNil(
            service.remoteMonthRawData(for: monthKey),
            "a later schema-flush/read-back failure must not evict the valid month's cache"
        )
    }

    func testDownloadedManifestWithoutEncryptionColumnsMigratesAndRequiresRemoteSync() throws {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("missing_encryption_columns_\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: url) }
        try writeManifestWithoutResourceEncryptionColumns(to: url)

        let prepared = try MonthManifestStore.prepareLocalManifest(localURL: url, origin: .downloadedFromRemote)
        defer { try? prepared.queue.close() }

        XCTAssertTrue(prepared.requiresRemoteSync)
        let migrated = try prepared.queue.read { db -> (resourceColumns: Set<String>, linkColumns: Set<String>, storageCodec: Int?, storedFileSize: Int64?, encryptionKeyID: String?) in
            let columns = Set(try Row.fetchAll(db, sql: "PRAGMA table_info(resources)").compactMap { $0["name"] as String? })
            let linkColumns = Set(try Row.fetchAll(db, sql: "PRAGMA table_info(asset_resources)").compactMap { $0["name"] as String? })
            let storageCodec = try Int.fetchOne(db, sql: "SELECT storageCodec FROM resources WHERE fileName = ?", arguments: ["legacy.jpg"])
            let storedFileSize = try Int64.fetchOne(db, sql: "SELECT storedFileSize FROM resources WHERE fileName = ?", arguments: ["legacy.jpg"])
            let encryptionKeyID = try String.fetchOne(db, sql: "SELECT encryptionKeyID FROM resources WHERE fileName = ?", arguments: ["legacy.jpg"])
            return (columns, linkColumns, storageCodec, storedFileSize, encryptionKeyID)
        }

        XCTAssertTrue(migrated.resourceColumns.contains("storageCodec"))
        XCTAssertTrue(migrated.resourceColumns.contains("storedFileSize"))
        XCTAssertTrue(migrated.resourceColumns.contains("encryptionKeyID"))
        XCTAssertTrue(migrated.linkColumns.contains("resourceFileName"))
        XCTAssertEqual(migrated.storageCodec, RemoteManifestResource.plaintextStorageCodec)
        XCTAssertNil(migrated.storedFileSize)
        XCTAssertNil(migrated.encryptionKeyID)
    }

    func testResourceEncryptionFieldsPersistThroughReload() throws {
        let store = try makeStore(client: InMemoryRemoteStorageClient(), layout: .lite, liteWriteOwnership: {})
        let resource = RemoteManifestResource(
            year: year,
            month: month,
            fileName: "opaque.wmenc",
            contentHash: Data([0xAB, 0xCD]),
            fileSize: 123,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: 456,
            encryptionKeyID: "repo-key-1"
        )

        try store.upsertResource(resource)
        try store.reloadCache()

        let persisted = try XCTUnwrap(store.unsortedSnapshot().resources.first)
        XCTAssertTrue(persisted.isEncrypted)
        XCTAssertEqual(persisted.storageCodec, RemoteManifestResource.encryptedStorageCodec)
        XCTAssertEqual(persisted.storedFileSize, 456)
        XCTAssertEqual(persisted.encryptionKeyID, "repo-key-1")
    }

    func testSameContentHashCanPersistPlaintextAndEncryptedResources() throws {
        let store = try makeStore(client: InMemoryRemoteStorageClient(), layout: .lite, liteWriteOwnership: {})
        let hash = Data([0xCC, 0xDD])
        let plaintext = RemoteManifestResource(
            year: year,
            month: month,
            fileName: "IMG_0001.JPG",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1,
            storageCodec: RemoteManifestResource.plaintextStorageCodec
        )
        let encrypted = RemoteManifestResource(
            year: year,
            month: month,
            fileName: "opaque.wmenc",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 2,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: 160,
            encryptionKeyID: "repo-key-1"
        )

        _ = try store.upsertResource(plaintext)
        _ = try store.upsertResource(encrypted)
        let plainFingerprint = Data([0x01])
        let encryptedFingerprint = Data([0x02])
        let plainLink = RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: plainFingerprint,
            resourceHash: hash,
            resourceFileName: plaintext.fileName,
            role: ResourceTypeCode.photo,
            slot: 0
        )
        let encryptedLink = RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: encryptedFingerprint,
            resourceHash: hash,
            resourceFileName: encrypted.fileName,
            role: ResourceTypeCode.photo,
            slot: 0
        )
        try store.upsertAsset(
            RemoteManifestAsset(year: year, month: month, assetFingerprint: plainFingerprint, creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100),
            links: [plainLink]
        )
        try store.upsertAsset(
            RemoteManifestAsset(year: year, month: month, assetFingerprint: encryptedFingerprint, creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 100),
            links: [encryptedLink]
        )
        try store.reloadCache()

        XCTAssertEqual(store.unsortedSnapshot().resources.count, 2)
        XCTAssertEqual(store.resource(for: plainLink)?.fileName, plaintext.fileName)
        XCTAssertEqual(store.resource(for: encryptedLink)?.fileName, encrypted.fileName)
        XCTAssertNil(store.findResourceByHash(hash), "ambiguous legacy hash-only lookup must fail closed")
        XCTAssertEqual(
            store.findResourceByHash(hash, storageCodec: RemoteManifestResource.encryptedStorageCodec, encryptionKeyID: "repo-key-1")?.fileName,
            encrypted.fileName
        )
    }

    func testExplicitResourceFileNameDoesNotFallbackToSameHashSibling() throws {
        let store = try makeStore(client: InMemoryRemoteStorageClient(), layout: .lite, liteWriteOwnership: {})
        let hash = Data([0xE0, 0xE1])
        let plaintext = RemoteManifestResource(
            year: year,
            month: month,
            fileName: "IMG_0001.JPG",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1,
            storageCodec: RemoteManifestResource.plaintextStorageCodec
        )
        let link = RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: Data([0xE2]),
            resourceHash: hash,
            resourceFileName: "missing.wmenc",
            role: ResourceTypeCode.photo,
            slot: 0
        )

        _ = try store.upsertResource(plaintext)
        let lookup = RemoteResourceLookup([plaintext])

        XCTAssertNil(store.resource(for: link))
        XCTAssertNil(lookup.resource(for: link))
    }

    func testReplacingAssetReclaimsSupersededSameHashResource() throws {
        let store = try makeStore(client: InMemoryRemoteStorageClient(), layout: .lite, liteWriteOwnership: {})
        let hash = Data([0xE3, 0xE4])
        let fingerprint = Data([0xE5])
        let plaintext = RemoteManifestResource(
            year: year,
            month: month,
            fileName: "IMG_0001.JPG",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1,
            storageCodec: RemoteManifestResource.plaintextStorageCodec
        )
        let encrypted = RemoteManifestResource(
            year: year,
            month: month,
            fileName: "opaque.wmenc",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 2,
            storageCodec: RemoteManifestResource.encryptedStorageCodec,
            storedFileSize: 160,
            encryptionKeyID: "repo-key-1"
        )
        let plainLink = RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: fingerprint,
            resourceHash: hash,
            resourceFileName: plaintext.fileName,
            role: ResourceTypeCode.photo,
            slot: 0
        )
        let encryptedLink = RemoteAssetResourceLink(
            year: year,
            month: month,
            assetFingerprint: fingerprint,
            resourceHash: hash,
            resourceFileName: encrypted.fileName,
            role: ResourceTypeCode.photo,
            slot: 0
        )

        _ = try store.upsertResource(plaintext)
        try store.upsertAsset(
            RemoteManifestAsset(year: year, month: month, assetFingerprint: fingerprint, creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 100),
            links: [plainLink]
        )
        _ = try store.upsertResource(encrypted)
        try store.upsertAsset(
            RemoteManifestAsset(year: year, month: month, assetFingerprint: fingerprint, creationDateMs: nil, backedUpAtMs: 2, resourceCount: 1, totalFileSizeBytes: 100),
            links: [encryptedLink]
        )

        let snapshot = store.unsortedSnapshot()
        XCTAssertFalse(snapshot.resources.contains { $0.fileName == plaintext.fileName })
        XCTAssertTrue(snapshot.resources.contains { $0.fileName == encrypted.fileName })
        XCTAssertNil(store.resource(for: plainLink))
        XCTAssertEqual(store.resource(for: encryptedLink)?.fileName, encrypted.fileName)
        XCTAssertEqual(store.findResourceByHash(hash)?.fileName, encrypted.fileName)
    }

    private func assertManifestSchemaIsCurrent(_ data: Data) throws {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("schema_\(UUID().uuidString).sqlite")
        defer { try? FileManager.default.removeItem(at: url) }
        try data.write(to: url)
        let queue = try DatabaseQueue(path: url.path)
        defer { try? queue.close() }
        let columns = try queue.read { db -> (resources: Set<String>, links: Set<String>) in
            (
                Set(try Row.fetchAll(db, sql: "PRAGMA table_info(resources)").compactMap { $0["name"] as String? }),
                Set(try Row.fetchAll(db, sql: "PRAGMA table_info(asset_resources)").compactMap { $0["name"] as String? })
            )
        }
        XCTAssertTrue(columns.resources.contains("creationDateMs"), "verify must persist the creationDateMs upgrade")
        XCTAssertFalse(columns.resources.contains("creationDateNs"), "the legacy creationDateNs column must be gone after verify")
        XCTAssertTrue(columns.resources.contains("storageCodec"), "verify must persist the resource encryption schema upgrade")
        XCTAssertTrue(columns.resources.contains("storedFileSize"), "verify must persist the resource stored-size column")
        XCTAssertTrue(columns.resources.contains("encryptionKeyID"), "verify must persist the resource key-id column")
        XCTAssertTrue(columns.links.contains("resourceFileName"), "verify must persist the link resource identity column")
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
        liteWriteOwnership: MonthManifestOwnershipAssertion? = nil,
        liteMonthsListing: LiteMonthsListingSnapshot? = nil
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
            liteWriteOwnership: liteWriteOwnership,
            liteMonthsListing: liteMonthsListing
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

    private func writeManifestWithoutResourceEncryptionColumns(to dbURL: URL) throws {
        let queue = try DatabaseQueue(path: dbURL.path)
        try queue.write { db in
            try db.execute(sql: """
                CREATE TABLE resources (
                  fileName TEXT PRIMARY KEY NOT NULL,
                  contentHash BLOB NOT NULL,
                  fileSize INTEGER NOT NULL,
                  resourceType INTEGER NOT NULL,
                  creationDateMs INTEGER,
                  backedUpAtMs INTEGER NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE TABLE assets (
                  assetFingerprint BLOB PRIMARY KEY NOT NULL,
                  creationDateMs INTEGER,
                  backedUpAtMs INTEGER NOT NULL,
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
            try db.execute(
                sql: """
                INSERT INTO resources (
                    fileName,
                    contentHash,
                    fileSize,
                    resourceType,
                    creationDateMs,
                    backedUpAtMs
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                arguments: [
                    "legacy.jpg",
                    Data([0xAA]),
                    100,
                    ResourceTypeCode.photo,
                    nil,
                    1
                ]
            )
        }
        try queue.close()
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

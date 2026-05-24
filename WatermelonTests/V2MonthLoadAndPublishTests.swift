import XCTest
@testable import Watermelon

final class V2MonthLoadAndPublishTests: XCTestCase {
    private let basePath = "/repo"
    private let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let runID = "run-test-uuid"
    private let year = 2026
    private let month = 1
    private var monthKey: LibraryMonthKey { LibraryMonthKey(year: year, month: month) }
    private var tempDBURL: URL!
    private var databaseManager: DatabaseManager!

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        tempDBURL = dir.appendingPathComponent("test.sqlite")
        databaseManager = try DatabaseManager(databaseURL: tempDBURL)
    }

    override func tearDownWithError() throws {
        databaseManager = nil
        if let url = tempDBURL {
            try? FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }

    // MARK: - Empty / Non-empty session

    func testLoadAndPublish_emptyMonth_loadsAndReturnsEmptyStore() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let ris = RemoteIndexSyncService()

        let store = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
            client: client,
            basePath: basePath,
            month: monthKey,
            v2Services: v2,
            remoteIndexService: ris,
            stepLogger: { _ in }
        )

        let snapshot = store.unsortedSnapshot()
        XCTAssertTrue(snapshot.resources.isEmpty)
        XCTAssertTrue(snapshot.assets.isEmpty)
        XCTAssertTrue(snapshot.links.isEmpty)
        XCTAssertFalse(store.physicallyMissingHashesAreAuthoritative,
                       "overlayIsAuthoritative=false (no overlay seeded) must propagate to the loaded session")

        // No overlay was set; verifiedPhysicallyMissingHashes returns nil since the helper publishes nil
        // missing-hashes for a non-authoritative session, clearing/leaving the freshness flag unset.
        let post = await ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(post, "no overlay was set, freshness flag must remain unset post-publish")
    }

    func testLoadAndPublish_committedAssets_publishesResourcesAssetsLinks() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Seed one committed asset via a real V2MonthSession + flush.
        let seedStore = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        let hash = TestFixtures.fingerprint(0xAA)
        let assetFP = TestFixtures.fingerprint(0xBB)
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: assetFP,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1,
            totalFileSizeBytes: 100
        )
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: "2026/01/photo.jpg",
            contentHash: hash,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: assetFP,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "photo.jpg"
        )
        _ = try seedStore.upsertResource(resource)
        try seedStore.upsertAsset(asset, links: [link])
        _ = try await seedStore.flushToRemote()

        // Fresh RIS for the helper call — exercises the cache publish on cold reload.
        let ris = RemoteIndexSyncService()

        _ = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
            client: client,
            basePath: basePath,
            month: monthKey,
            v2Services: v2,
            remoteIndexService: ris,
            stepLogger: { _ in }
        )

        let cached = try XCTUnwrap(ris.remoteMonthRawData(for: monthKey),
                                    "cache must contain the month after publish")
        XCTAssertEqual(cached.resources.count, 1)
        XCTAssertEqual(cached.assets.count, 1)
        XCTAssertEqual(cached.assetResourceLinks.count, 1)
        XCTAssertEqual(cached.assets.first?.assetFingerprint, assetFP)
        XCTAssertEqual(cached.resources.first?.contentHash, hash)
        XCTAssertEqual(cached.assetResourceLinks.first?.logicalName, "photo.jpg")
    }

    // MARK: - Overlay authoritative vs absent

    func testLoadAndPublish_overlayFresh_publishesAuthoritativeAndKeepsFreshFlag() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let ris = RemoteIndexSyncService()

        // Seed the freshness flag by calling replaceCachedMonth with a non-nil missing set.
        // Resources are empty so any hash in physicallyMissingHashes would intersect away;
        // an empty non-nil set keeps the freshness flag set without polluting the missing-set.
        ris.replaceCachedMonth(
            monthKey,
            resources: [],
            assets: [],
            links: [],
            physicallyMissingHashes: Set<Data>()
        )

        let preFresh = await ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertEqual(preFresh, Set<Data>(),
                       "setup precondition: freshness flag must be set (verified returns the empty committed-view set)")

        let store = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
            client: client,
            basePath: basePath,
            month: monthKey,
            v2Services: v2,
            remoteIndexService: ris,
            stepLogger: { _ in }
        )

        XCTAssertTrue(store.physicallyMissingHashesAreAuthoritative,
                      "freshHashes was non-nil → overlayIsAuthoritative=true must propagate")

        // The helper publishes physicallyMissingHashes: non-nil (empty set), so freshness flag stays set.
        let post = await ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertEqual(post, Set<Data>(),
                       "overlay-fresh path must keep freshness flag set after the helper's republish")
    }

    func testLoadAndPublish_overlayAbsent_keepsFreshFlagUnset() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let ris = RemoteIndexSyncService()

        // No overlay seeded -> verifiedPhysicallyMissingHashes returns nil; freshness flag is NOT set.
        let preFresh = await ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(preFresh, "setup precondition: overlay must be absent")

        let store = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
            client: client,
            basePath: basePath,
            month: monthKey,
            v2Services: v2,
            remoteIndexService: ris,
            stepLogger: { _ in }
        )

        XCTAssertFalse(store.physicallyMissingHashesAreAuthoritative,
                       "freshHashes was nil → overlayIsAuthoritative=false must propagate")

        // overlayIsAuthoritative was false -> physicallyMissingHashesAreAuthoritative false;
        // helper publishes physicallyMissingHashes: nil; freshness flag must remain unset.
        let post = await ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(post, "non-authoritative publish must leave freshness flag unset")
    }

    // MARK: - Error propagation

    func testLoadAndPublish_loadOrCreateThrows_propagatesError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let ris = RemoteIndexSyncService()

        // Inject a transport error on the month directory list path so V2MonthSession.loadOrCreate
        // re-throws after the createDirectory step (V2MonthSession.swift:127).
        let monthAbsolutePath = "/repo/2026/01"
        await client.injectListError(.transport, for: monthAbsolutePath)

        var observedLogMessages: [String] = []
        do {
            _ = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
                client: client,
                basePath: basePath,
                month: monthKey,
                v2Services: v2,
                remoteIndexService: ris,
                stepLogger: { observedLogMessages.append($0) }
            )
            XCTFail("expected helper to re-throw the injected list error")
        } catch let storageError as RemoteStorageClientError {
            guard case .underlying(let underlying as NSError) = storageError else {
                XCTFail("expected RemoteStorageClientError.underlying(NSError), got \(storageError)")
                return
            }
            XCTAssertEqual(underlying.domain, NSURLErrorDomain,
                           "helper must re-throw the translated transport error verbatim")
        } catch {
            XCTFail("unexpected error type \(type(of: error)): \(error)")
        }

        // Helper must NOT have mutated the freshness flag when load threw before publish.
        let postFresh = await ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(postFresh,
                     "replaceCachedMonth must not run when V2MonthSession.loadOrCreate throws")

        // stepLogger must observe the diagnostic for the failing list step (confirms the closure was forwarded).
        XCTAssertFalse(observedLogMessages.isEmpty,
                       "stepLogger must observe the localized diagnostic emitted from V2MonthSession.loadOrCreate")
    }

    // MARK: - Fail-closed precedence

    func testLoadAndPublish_failClosedFromCommittedView_clearsStaleMissingWhenOverlayAbsent() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let ris = RemoteIndexSyncService()

        // Seed committed-view physicallyMissing without freshness flag:
        // markPhysicallyMissingV2 sets the missing set inside committedView WITHOUT inserting
        // into physicalPresenceOverlayFreshMonths, so verifiedPhysicallyMissingHashes returns nil
        // while physicallyMissingHashes(for:) returns the non-empty set.
        let staleMissing = TestFixtures.fingerprint(0xDD)
        ris.markPhysicallyMissingV2(month: monthKey, hashes: [staleMissing])

        let preFresh = await ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(preFresh, "setup precondition: freshness flag must NOT be set")
        let preCommitted = ris.physicallyMissingHashes(for: monthKey)
        XCTAssertEqual(preCommitted, [staleMissing],
                       "setup precondition: committed-view must hold the stale missing set")

        let store = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
            client: client,
            basePath: basePath,
            month: monthKey,
            v2Services: v2,
            remoteIndexService: ris,
            stepLogger: { _ in }
        )

        XCTAssertFalse(store.physicallyMissingHashesAreAuthoritative,
                       "freshHashes was nil → overlayIsAuthoritative=false must propagate (committed-view fallback is informational, not authoritative)")

        // Helper publishes physicallyMissingHashes: nil. replaceMonth then intersects the previous
        // stale set with the now-empty resources → clears the stale missing set.
        let postFresh = await ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(postFresh, "freshness flag must remain unset post-publish (helper passed nil)")
        let postCommitted = ris.physicallyMissingHashes(for: monthKey)
        XCTAssertTrue(postCommitted.isEmpty,
                      "publish step with physicallyMissingHashes:nil must clear stale committed-view missing set when no resources reference it")
    }

    // MARK: - Helpers

    private func makeV2Services(client: InMemoryRemoteStorageClient) async throws -> BackupV2RuntimeServices {
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager, writerID: writerID, basePath: basePath, storageType: .webdav
        )
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let commitWriter = CommitLogWriter(client: client, basePath: basePath)
        let snapshotWriter = SnapshotWriter(client: client, basePath: basePath)
        let liveness = LivenessTracker(client: client, basePath: basePath, writerID: writerID, isLocalVolume: true)
        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: repoID,
            runID: runID,
            basePath: basePath,
            postOpenSyncInspection: .v2(formatVersion: RepoLayout.currentSupportedFormatVersion),
            database: databaseManager,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: commitWriter,
            snapshotWriter: snapshotWriter,
            liveness: liveness,
            compactionPolicy: .default,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }
}

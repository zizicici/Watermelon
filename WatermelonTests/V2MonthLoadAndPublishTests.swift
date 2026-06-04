import CryptoKit
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
        XCTAssertFalse(store.presence.isAuthoritative,
                       "overlayIsAuthoritative=false (no overlay seeded) must propagate to the loaded session")

        // No overlay was set; verifiedPhysicallyMissingHashes returns nil since the helper publishes nil
        // missing-hashes for a non-authoritative session, clearing/leaving the freshness flag unset.
        let post = ris.verifiedPhysicallyMissingHashes(for: monthKey)
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
        let assetFP = TestFixtures.assetFingerprint(0xBB)
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

        let preFresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
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

        XCTAssertTrue(store.presence.isAuthoritative,
                      "freshHashes was non-nil → overlayIsAuthoritative=true must propagate")

        // The helper publishes physicallyMissingHashes: non-nil (empty set), so freshness flag stays set.
        let post = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertEqual(post, Set<Data>(),
                       "overlay-fresh path must keep freshness flag set after the helper's republish")
    }

    func testLoadAndPublish_overlayAbsent_keepsFreshFlagUnset() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)
        let ris = RemoteIndexSyncService()

        // No overlay seeded -> verifiedPhysicallyMissingHashes returns nil; freshness flag is NOT set.
        let preFresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(preFresh, "setup precondition: overlay must be absent")

        let store = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
            client: client,
            basePath: basePath,
            month: monthKey,
            v2Services: v2,
            remoteIndexService: ris,
            stepLogger: { _ in }
        )

        XCTAssertFalse(store.presence.isAuthoritative,
                       "freshHashes was nil → overlayIsAuthoritative=false must propagate")

        // overlayIsAuthoritative was false -> presence.isAuthoritative false;
        // helper publishes physicallyMissingHashes: nil; freshness flag must remain unset.
        let post = ris.verifiedPhysicallyMissingHashes(for: monthKey)
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
        let postFresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
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

        let preFresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
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

        XCTAssertFalse(store.presence.isAuthoritative,
                       "freshHashes was nil → overlayIsAuthoritative=false must propagate (committed-view fallback is informational, not authoritative)")

        // Helper publishes physicallyMissingHashes: nil. replaceMonth then intersects the previous
        // stale set with the now-empty resources → clears the stale missing set.
        let postFresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(postFresh, "freshness flag must remain unset post-publish (helper passed nil)")
        let postCommitted = ris.physicallyMissingHashes(for: monthKey)
        XCTAssertTrue(postCommitted.isEmpty,
                      "publish step with physicallyMissingHashes:nil must clear stale committed-view missing set when no resources reference it")
    }

    // MARK: - Gate A regression — non-authoritative non-empty fail-closed input must reach V2MonthIndexes

    func testLoadAndPublish_nonAuthoritativeNonEmptyMissing_preservesFailClosedInputToIndexes() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await makeV2Services(client: client)

        // Seed a committed resource for hash h via a separate seedStore + flush so the materializer
        // returns a non-empty month state with a resource row whose contentHash == h.
        let h = TestFixtures.fingerprint(0xCD)
        let assetFP = TestFixtures.assetFingerprint(0xCE)
        let physicalPath = "\(year)/\(String(format: "%02d", month))/cd-photo.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: physicalPath,
            contentHash: h,
            fileSize: 100,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: assetFP,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1,
            totalFileSizeBytes: 100
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: assetFP,
            resourceHash: h,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "cd-photo.jpg"
        )
        let seedStore = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )
        _ = try seedStore.upsertResource(resource)
        try seedStore.upsertAsset(asset, links: [link])
        _ = try await seedStore.flushToRemote()

        // Inject the physical resource file so the second loadOrCreate's listing populates
        // remoteFilesByName["cd-photo.jpg"] = .init(size: 100). That forces
        // V2MonthIndexes.listedSizeMatches == true for the materialized row. Without this,
        // listedSizeMatches stays false and V2MonthIndexes falls back to .missing under BOTH the
        // correct decomposition and the rejected authority-gated decomposition — the assertion
        // below would pass for the wrong reason.
        await client.injectFile(
            path: "\(basePath)/\(physicalPath)",
            data: Data(repeating: 0xCD, count: 100)
        )

        // Fresh RIS for the load+publish phase. Seed the committed view's missing set WITHOUT
        // freshness (markPhysicallyMissingV2 sets the missing set inside committedView without
        // inserting into physicalPresenceOverlayFreshMonths). presenceSnapshot(for:) will return
        // (missingHashes: [h], isAuthoritative: false) — the non-authoritative non-empty quadrant.
        let ris = RemoteIndexSyncService()
        ris.markPhysicallyMissingV2(month: monthKey, hashes: [h])

        let preFresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(preFresh, "setup precondition: freshness flag must NOT be set")
        let prePresence = ris.presenceSnapshot(for: monthKey)
        XCTAssertEqual(prePresence.missingHashes, [h])
        XCTAssertFalse(prePresence.isAuthoritative,
                       "setup precondition: committed view holds non-authoritative non-empty missing")

        let store = try await V2MonthLoadAndPublish.loadAndPublishSnapshot(
            client: client,
            basePath: basePath,
            month: monthKey,
            v2Services: v2,
            remoteIndexService: ris,
            stepLogger: { _ in }
        )

        // (1) Authority bit propagates as false to the loaded session — Gate B unchanged.
        XCTAssertFalse(store.presence.isAuthoritative,
                       "non-authoritative presence input must propagate authority=false to the loaded session")

        // (2) Fail-closed input preserved end-to-end: V2MonthIndexes.presenceMap marked h as
        //     .missing because verifiedMissingHashes: [h] was forwarded (Gate A — non-empty
        //     forwarding, authority-independent). The injected physical file forces
        //     listedSizeMatches == true; a regression that gates Gate A on authority would let
        //     V2MonthIndexes fall back to .listedSizeMatched here, yielding missingHashes == [].
        XCTAssertEqual(store.presence.missingHashes, [h],
                       "fail-closed missing hash MUST flow into V2MonthIndexes.presenceMap regardless of authority — Gate A")

        // (3) Publish step did NOT seed the freshness flag (publishMonthSnapshot passes
        //     physicallyMissingHashes: nil when presence.isAuthoritative is false — Gate B).
        let postFresh = ris.verifiedPhysicallyMissingHashes(for: monthKey)
        XCTAssertNil(postFresh,
                     "non-authoritative publish must leave freshness flag unset")

        // (4) Committed view still carries h as physically-missing (intersection with stillPresent
        //     keeps [h] because the published snapshot includes the resource at hash h).
        let postCommitted = ris.physicallyMissingHashes(for: monthKey)
        XCTAssertEqual(postCommitted, [h],
                       "committed view's non-authoritative missing set survives publish")
    }

    // MARK: - Grace backend: data-dir LIST omits a durable resource (dedup fast path must survive)

    func testLoadOrCreate_graceBackend_dataListOmitsDurableResource_findResourceByHashRecovers() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let v2 = try await makeV2Services(client: inner)

        // Seed a committed resource so the materializer returns a non-empty month state.
        let bytes = Data("bf2-grace-omit-durable".utf8)
        let hash = Data(SHA256.hash(data: bytes))
        let assetFP = TestFixtures.assetFingerprint(0x7A)
        let physicalPath = "2026/01/photo.jpg"
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: physicalPath,
            contentHash: hash,
            fileSize: Int64(bytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: assetFP,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1,
            totalFileSizeBytes: Int64(bytes.count)
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: assetFP,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "photo.jpg"
        )
        let seedStore = try await V2MonthSession.loadOrCreate(
            client: inner, basePath: basePath, year: year, month: month, v2Services: v2
        )
        _ = try seedStore.upsertResource(resource)
        try seedStore.upsertAsset(asset, links: [link])
        _ = try await seedStore.flushToRemote()

        // The data file is durable on remote, but a stale grace-backend month-dir LIST omits it
        // while the commit is already materialized.
        await inner.injectFile(path: "\(basePath)/\(physicalPath)", data: bytes)
        let client = DataListOmitGraceWrapper(
            inner: inner,
            omittedPaths: ["\(basePath)/\(physicalPath)"],
            grace: 30
        )

        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        XCTAssertNotNil(store.findResourceByHash(hash),
                        "grace backend: a durable resource omitted from one stale data-dir LIST must be recovered via direct probe so the existing-hash dedup fast path is not disabled")
    }

    /// Zero-grace, exact-match (case-sensitive) normalizing backend: the recorded NFC leaf is listed
    /// back as NFD, so the byte-exact presence key misses and V2MonthIndexes would mark the resource
    /// `.missing` — disabling the dedup fast path and driving a redundant re-upload of bytes already
    /// present. loadOrCreate must hash-probe the recorded path on exact-match backends (not only grace
    /// backends) so findResourceByHash / findByFileName still recover the committed resource.
    func testLoadOrCreate_zeroGraceExactMatch_listNFDvsRecordedNFC_findRecovers() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let v2 = try await makeV2Services(client: inner)

        let bytes = Data("bf2-nfc-nfd-dedup".utf8)
        let hash = Data(SHA256.hash(data: bytes))
        let assetFP = TestFixtures.assetFingerprint(0x7B)
        let baseLeaf = "cafe\u{0301}.jpg"
        let nfcLeaf = baseLeaf.precomposedStringWithCanonicalMapping
        let nfdLeaf = baseLeaf.decomposedStringWithCanonicalMapping
        XCTAssertNotEqual(Data(nfcLeaf.utf8), Data(nfdLeaf.utf8), "test premise: NFC and NFD bytes differ")
        let physicalPath = "2026/01/\(nfcLeaf)"
        let resource = RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: physicalPath,
            contentHash: hash,
            fileSize: Int64(bytes.count),
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 0
        )
        let asset = RemoteManifestAsset(
            year: year, month: month,
            assetFingerprint: assetFP,
            creationDateMs: 1_700_000_000_000,
            backedUpAtMs: 1_700_000_001_000,
            resourceCount: 1,
            totalFileSizeBytes: Int64(bytes.count)
        )
        let link = RemoteAssetResourceLink(
            year: year, month: month,
            assetFingerprint: assetFP,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: nfcLeaf
        )
        let seedStore = try await V2MonthSession.loadOrCreate(
            client: inner, basePath: basePath, year: year, month: month, v2Services: v2
        )
        _ = try seedStore.upsertResource(resource)
        try seedStore.upsertAsset(asset, links: [link])
        _ = try await seedStore.flushToRemote()

        // Bytes are durable under the recorded NFC path; the case-sensitive backend lists the same
        // file under its NFD leaf, so the byte-exact presence key no longer matches.
        await inner.injectFile(path: "\(basePath)/\(physicalPath)", data: bytes)
        let client = DataListLeafNormalizationWrapper(inner: inner, recordedLeafToListedLeaf: [nfcLeaf: nfdLeaf])

        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        XCTAssertNotNil(store.findResourceByHash(hash),
                        "zero-grace exact-match backend: a durable resource listed under a canonically-equivalent NFD leaf must be recovered via direct probe so the dedup fast path is not disabled")
        XCTAssertNotNil(store.findByFileName(nfcLeaf),
                        "findByFileName must also recover the divergent-normalization resource")
    }

    // MARK: - Grace backend: an over-budget omitted resource must not abort reconcile for the rest

    /// loadOrCreate's reconcile budget-guards each probed resource by cumulative bytes. A single
    /// LIST-omitted resource larger than the per-month byte cap must be skipped individually, not
    /// abort the whole reconcile — otherwise (with `break`) when the oversized row is visited first
    /// the in-budget omitted resources stay `.missing`, disabling the dedup fast path and forcing
    /// duplicate re-uploads. Asserts the order-independent outcome the fix guarantees: every small
    /// omitted resource is recovered and the oversized one is not (it always trips the byte cap).
    func testLoadOrCreate_graceBackend_oversizedOmittedResourceDoesNotAbortReconcileForSmallOnes() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let v2 = try await makeV2Services(client: inner)

        let seedStore = try await V2MonthSession.loadOrCreate(
            client: inner, basePath: basePath, year: year, month: month, v2Services: v2
        )

        // One oversized resource (> 32 MiB byte cap) with no injected bytes — the reconcile byte guard
        // short-circuits it before any download, so its data file is never needed.
        let oversizedHash = TestFixtures.fingerprint(0xF0)
        let oversizedPath = "2026/01/oversized.bin"
        _ = try seedStore.upsertResource(RemoteManifestResource(
            year: year, month: month,
            physicalRemotePath: oversizedPath,
            contentHash: oversizedHash,
            fileSize: 33 * 1024 * 1024,
            resourceType: ResourceTypeCode.video,
            creationDateMs: nil, backedUpAtMs: 0
        ))
        let oversizedFP = TestFixtures.assetFingerprint(0xF1)
        try seedStore.upsertAsset(
            RemoteManifestAsset(
                year: year, month: month, assetFingerprint: oversizedFP,
                creationDateMs: 1_700_000_000_000, backedUpAtMs: 1_700_000_001_000,
                resourceCount: 1, totalFileSizeBytes: 33 * 1024 * 1024
            ),
            links: [RemoteAssetResourceLink(
                year: year, month: month, assetFingerprint: oversizedFP,
                resourceHash: oversizedHash, role: ResourceTypeCode.video, slot: 0,
                logicalName: "oversized.bin"
            )]
        )

        // Several small content-confirmable resources, each committed and durable on the remote.
        var smallHashes: [Data] = []
        var omittedDataPaths: Set<String> = ["\(basePath)/\(oversizedPath)"]
        for i in 0..<4 {
            let bytes = Data("bf1-small-\(i)".utf8)
            let hash = Data(SHA256.hash(data: bytes))
            smallHashes.append(hash)
            let leaf = "small\(i).jpg"
            let path = "2026/01/\(leaf)"
            _ = try seedStore.upsertResource(RemoteManifestResource(
                year: year, month: month,
                physicalRemotePath: path,
                contentHash: hash,
                fileSize: Int64(bytes.count),
                resourceType: ResourceTypeCode.photo,
                creationDateMs: nil, backedUpAtMs: 0
            ))
            let fp = TestFixtures.assetFingerprint(UInt8(0xA0 + i))
            try seedStore.upsertAsset(
                RemoteManifestAsset(
                    year: year, month: month, assetFingerprint: fp,
                    creationDateMs: 1_700_000_000_000, backedUpAtMs: 1_700_000_001_000,
                    resourceCount: 1, totalFileSizeBytes: Int64(bytes.count)
                ),
                links: [RemoteAssetResourceLink(
                    year: year, month: month, assetFingerprint: fp,
                    resourceHash: hash, role: ResourceTypeCode.photo, slot: 0,
                    logicalName: leaf
                )]
            )
            await inner.injectFile(path: "\(basePath)/\(path)", data: bytes)
            omittedDataPaths.insert("\(basePath)/\(path)")
        }
        _ = try await seedStore.flushToRemote()

        // A stale grace-backend month-dir LIST omits every data file; the commits are materialized.
        let client = DataListOmitGraceWrapper(inner: inner, omittedPaths: omittedDataPaths, grace: 30)

        let store = try await V2MonthSession.loadOrCreate(
            client: client, basePath: basePath, year: year, month: month, v2Services: v2
        )

        for (i, hash) in smallHashes.enumerated() {
            XCTAssertNotNil(store.findResourceByHash(hash),
                            "small omitted resource #\(i) must be recovered regardless of iteration order; an oversized sibling must not abort its reconcile")
        }
        XCTAssertNil(store.findResourceByHash(oversizedHash),
                     "the over-budget resource is conservatively left missing (skipped by the byte cap)")
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
            compactionPolicy: .default,
            isLocalVolume: true,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
        )
    }
}

/// Wraps an InMemory client and drops specific paths from list() while still serving them
/// via metadata/download, simulating a grace backend whose data-dir listing omits a durable file.
private struct DataListOmitGraceWrapper: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let omittedPaths: Set<String>
    let grace: TimeInterval

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    var readAfterWriteGraceSeconds: TimeInterval { grace }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await inner.list(path: path).filter { !omittedPaths.contains($0.path) }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

/// Zero-grace, case-sensitive backend whose listing returns specific leaves under a canonically-equivalent
/// but byte-different Unicode normalization, while metadata/download still serve the recorded path.
/// Simulates an HFS+/SFTP endpoint that stores NFD while the manifest recorded NFC.
private struct DataListLeafNormalizationWrapper: RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let recordedLeafToListedLeaf: [String: String]

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { false }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var readAfterWriteGraceSeconds: TimeInterval { 0 }

    func list(path: String) async throws -> [RemoteStorageEntry] {
        try await inner.list(path: path).map { entry in
            guard let listed = recordedLeafToListedLeaf[entry.name] else { return entry }
            let parent = (entry.path as NSString).deletingLastPathComponent
            return RemoteStorageEntry(
                path: parent.isEmpty ? listed : "\(parent)/\(listed)",
                name: listed,
                isDirectory: entry.isDirectory,
                size: entry.size,
                creationDate: entry.creationDate,
                modificationDate: entry.modificationDate
            )
        }
    }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func download(remotePath: String, localURL: URL) async throws { try await inner.download(remotePath: remotePath, localURL: localURL) }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws { try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult { try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws { try await inner.move(from: sourcePath, to: destinationPath) }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult { try await inner.moveIfAbsent(from: sourcePath, to: destinationPath) }
    func copy(from sourcePath: String, to destinationPath: String) async throws { try await inner.copy(from: sourcePath, to: destinationPath) }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func setModificationDate(_ date: Date, forPath path: String) async throws { try await inner.setModificationDate(date, forPath: path) }
    func supportsExclusiveMoveIfAbsent(forDestinationPath path: String) async throws -> Bool { try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: path) }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee { .overwritePossible }
}

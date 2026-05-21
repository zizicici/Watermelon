import XCTest
import CryptoKit
@testable import Watermelon

/// `detectRemoteContentRace` decides whether the bytes we just uploaded via a
/// best-effort backend (SMB exists+upload TOCTOU) are actually ours. Semantics are
/// inverted from "trusting": failure to verify → race assumed (caller does collision
/// rename). The wrong default would bind our hash record to bytes another writer wrote.
final class AssetProcessorRaceDetectionTests: XCTestCase {
    private let basePath = "/repo"
    private let remotePath = "/repo/2026/01/photo.jpg"

    func testNoRace_sizeAndHashMatch_returnsFalse() async throws {
        let bytes = Data(repeating: 0xAB, count: 256)
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectFile(path: remotePath, data: bytes)

        let race = try await AssetProcessor.detectRemoteContentRace(
            client: client,
            remotePath: remotePath,
            expectedSize: Int64(bytes.count),
            expectedHash: Self.sha256(bytes),
            cancellationController: nil
        )
        XCTAssertFalse(race, "matching size+hash means our bytes landed; no race")
    }

    func testRace_sizeMismatch_returnsTrue() async throws {
        let theirBytes = Data(repeating: 0xCD, count: 200)
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectFile(path: remotePath, data: theirBytes)

        let race = try await AssetProcessor.detectRemoteContentRace(
            client: client,
            remotePath: remotePath,
            expectedSize: 256,
            expectedHash: Self.sha256(Data(repeating: 0xAB, count: 256)),
            cancellationController: nil
        )
        XCTAssertTrue(race, "size mismatch is fast-path race detection")
    }

    func testRace_sizeMatchesButHashDiffers_returnsTrue() async throws {
        // Same size, different content — the catastrophic case where size-only check
        // would falsely pass and bind our hash record to a peer's bytes.
        let theirBytes = Data(repeating: 0xCD, count: 256)
        let ourBytes = Data(repeating: 0xAB, count: 256)
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectFile(path: remotePath, data: theirBytes)

        let race = try await AssetProcessor.detectRemoteContentRace(
            client: client,
            remotePath: remotePath,
            expectedSize: Int64(ourBytes.count),
            expectedHash: Self.sha256(ourBytes),
            cancellationController: nil
        )
        XCTAssertTrue(race, "same-size-different-content must be flagged as race")
    }

    func testRace_metadataFails_returnsTrue() async throws {
        let bytes = Data(repeating: 0xAB, count: 256)
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectFile(path: remotePath, data: bytes)
        await client.injectMetadataError(.transport, for: remotePath)

        let race = try await AssetProcessor.detectRemoteContentRace(
            client: client,
            remotePath: remotePath,
            expectedSize: Int64(bytes.count),
            expectedHash: Self.sha256(bytes),
            cancellationController: nil
        )
        XCTAssertTrue(race, "metadata failure must be assumed-race, not assumed-ours")
    }

    func testRace_remoteAbsent_returnsTrue() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Nothing injected at remotePath — metadata returns nil.

        let race = try await AssetProcessor.detectRemoteContentRace(
            client: client,
            remotePath: remotePath,
            expectedSize: 256,
            expectedHash: Self.sha256(Data(repeating: 0xAB, count: 256)),
            cancellationController: nil
        )
        XCTAssertTrue(race, "absent remote = bytes never landed = treat as race so caller retries")
    }

    func testRace_downloadFails_returnsTrue() async throws {
        let bytes = Data(repeating: 0xAB, count: 256)
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.injectFile(path: remotePath, data: bytes)
        await client.injectDownloadError(.transport, for: remotePath)

        let race = try await AssetProcessor.detectRemoteContentRace(
            client: client,
            remotePath: remotePath,
            expectedSize: Int64(bytes.count),
            expectedHash: Self.sha256(bytes),
            cancellationController: nil
        )
        XCTAssertTrue(race, "download failure during hash verify must trigger collision rename")
    }

    func testFinalizeRowWritingAsset_commitFailureBlocksPublishAndHashWrite() async throws {
        let databaseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathComponent("test.sqlite")
        try FileManager.default.createDirectory(
            at: databaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: databaseURL.deletingLastPathComponent()) }

        let remoteIndexService = RemoteIndexSyncService()
        let processor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: databaseURL)),
            remoteIndexService: remoteIndexService
        )
        let month = LibraryMonthKey(year: 2026, month: 1)
        let hash = TestFixtures.fingerprint(0xE2)
        let assetFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let asset = RemoteManifestAsset(
            year: month.year,
            month: month.month,
            assetFingerprint: assetFingerprint,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 1,
            totalFileSizeBytes: 1
        )
        let link = RemoteAssetResourceLink(
            year: month.year,
            month: month.month,
            assetFingerprint: assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "x.jpg"
        )
        let store = ThrowingCommitMonthStore(year: month.year, month: month.month)
        var timing = AssetProcessTiming()
        var hashWriteCalled = false

        do {
            try await processor.finalizeRowWritingAsset(
                monthStore: store,
                manifestAsset: asset,
                links: [link],
                timing: &timing
            ) {
                hashWriteCalled = true
            }
            XCTFail("expected commit failure")
        } catch ThrowingCommitMonthStore.TestError.commitFailed {
        }

        XCTAssertEqual(store.ignoreCancellationValues, [false])
        XCTAssertFalse(hashWriteCalled)
        XCTAssertNil(remoteIndexService.committedAssetFingerprintsByMonth()[month])
    }

    func testFinalizeRowWritingAsset_commitPrecedesHashWriteAndPublishes() async throws {
        let databaseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathComponent("test.sqlite")
        try FileManager.default.createDirectory(
            at: databaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: databaseURL.deletingLastPathComponent()) }

        let remoteIndexService = RemoteIndexSyncService()
        let processor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: databaseURL)),
            remoteIndexService: remoteIndexService
        )
        let month = LibraryMonthKey(year: 2026, month: 1)
        let hash = TestFixtures.fingerprint(0xE4)
        let assetFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let asset = RemoteManifestAsset(
            year: month.year,
            month: month.month,
            assetFingerprint: assetFingerprint,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 1,
            totalFileSizeBytes: 1
        )
        let resource = RemoteManifestResource(
            year: month.year,
            month: month.month,
            physicalRemotePath: "2026/01/x.jpg",
            contentHash: hash,
            fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1
        )
        let link = RemoteAssetResourceLink(
            year: month.year,
            month: month.month,
            assetFingerprint: assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "x.jpg"
        )
        let recorder = EventRecorder()
        let store = ThrowingCommitMonthStore(year: month.year, month: month.month, eventRecorder: recorder)
        store.shouldThrowCommit = false
        let writer = remoteIndexService.makeOptimisticAssetWriter()
        writer.appendResource(resource)
        var timing = AssetProcessTiming()

        try await processor.finalizeRowWritingAsset(
            monthStore: store,
            manifestAsset: asset,
            links: [link],
            timing: &timing
        ) {
            XCTAssertEqual(recorder.events, ["commit"])
            XCTAssertEqual(remoteIndexService.committedAssetFingerprintsByMonth()[month], [assetFingerprint])
            recorder.append("hash")
        }

        XCTAssertEqual(recorder.events, ["commit", "hash"])
        XCTAssertEqual(store.ignoreCancellationValues, [false])
        XCTAssertEqual(remoteIndexService.committedAssetFingerprintsByMonth()[month], [assetFingerprint])
    }

    func testFinalizeRowWritingAsset_publishesCommittedSweepFromDelta() async throws {
        let databaseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathComponent("test.sqlite")
        try FileManager.default.createDirectory(
            at: databaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: databaseURL.deletingLastPathComponent()) }

        let remoteIndexService = RemoteIndexSyncService()
        let processor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: databaseURL)),
            remoteIndexService: remoteIndexService
        )
        let month = LibraryMonthKey(year: 2026, month: 1)
        let carriedHash = TestFixtures.fingerprint(0xE6)
        let carriedFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: carriedHash)]
        )
        let currentHash = TestFixtures.fingerprint(0xE7)
        let currentFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: currentHash)]
        )
        let carriedAsset = RemoteManifestAsset(
            year: month.year,
            month: month.month,
            assetFingerprint: carriedFingerprint,
            creationDateMs: nil,
            backedUpAtMs: 1,
            resourceCount: 1,
            totalFileSizeBytes: 1
        )
        let currentAsset = RemoteManifestAsset(
            year: month.year,
            month: month.month,
            assetFingerprint: currentFingerprint,
            creationDateMs: nil,
            backedUpAtMs: 2,
            resourceCount: 1,
            totalFileSizeBytes: 1
        )
        let carriedResource = RemoteManifestResource(
            year: month.year,
            month: month.month,
            physicalRemotePath: "2026/01/carried.jpg",
            contentHash: carriedHash,
            fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 1
        )
        let currentResource = RemoteManifestResource(
            year: month.year,
            month: month.month,
            physicalRemotePath: "2026/01/current.jpg",
            contentHash: currentHash,
            fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: 2
        )
        let carriedLink = RemoteAssetResourceLink(
            year: month.year,
            month: month.month,
            assetFingerprint: carriedFingerprint,
            resourceHash: carriedHash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "carried.jpg"
        )
        let currentLink = RemoteAssetResourceLink(
            year: month.year,
            month: month.month,
            assetFingerprint: currentFingerprint,
            resourceHash: currentHash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: "current.jpg"
        )
        let store = ThrowingCommitMonthStore(year: month.year, month: month.month)
        store.shouldThrowCommit = false
        store.commitDelta = MonthManifestStore.FlushDelta(
            didFlush: true,
            committedV2AssetFingerprints: [carriedFingerprint, currentFingerprint],
            committedV2TombstoneFingerprints: []
        )
        store.snapshotResources = [carriedResource, currentResource]
        store.snapshotAssets = [carriedAsset, currentAsset]
        store.snapshotLinks = [carriedLink, currentLink]
        var timing = AssetProcessTiming()

        try await processor.finalizeRowWritingAsset(
            monthStore: store,
            manifestAsset: currentAsset,
            links: [currentLink],
            timing: &timing
        ) {}

        XCTAssertEqual(
            remoteIndexService.committedAssetFingerprintsByMonth()[month],
            [carriedFingerprint, currentFingerprint]
        )
    }

    func testFinalizeRowWritingAsset_realV2PendingSweepPublishesBeforeHashWrite() async throws {
        let databaseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathComponent("test.sqlite")
        try FileManager.default.createDirectory(
            at: databaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: databaseURL.deletingLastPathComponent()) }

        let databaseManager = try DatabaseManager(databaseURL: databaseURL)
        let remoteIndexService = RemoteIndexSyncService()
        let processor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: databaseManager),
            remoteIndexService: remoteIndexService
        )
        let basePath = "/repo"
        let writerID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        let runID = "run-test-uuid"
        let month = LibraryMonthKey(year: 2026, month: 1)
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let v2 = try await Self.makeV2Services(
            client: client,
            databaseManager: databaseManager,
            basePath: basePath,
            writerID: writerID,
            repoID: repoID,
            runID: runID
        )
        let store = try await V2MonthSession.loadOrCreate(
            client: client,
            basePath: basePath,
            year: month.year,
            month: month.month,
            v2Services: v2
        )

        let carried = Self.makeSingleAssetRows(month: month, assetByte: 0xF1, hashByte: 0xF2, name: "carried.jpg")
        _ = try store.upsertResource(carried.resource)
        try store.upsertAsset(carried.asset, links: [carried.link])

        let current = Self.makeSingleAssetRows(month: month, assetByte: 0xF3, hashByte: 0xF4, name: "current.jpg")
        _ = try store.upsertResource(current.resource)
        try store.upsertAsset(current.asset, links: [current.link])
        var timing = AssetProcessTiming()
        var hashWriteCalled = false

        try await processor.finalizeRowWritingAsset(
            monthStore: store,
            manifestAsset: current.asset,
            links: [current.link],
            timing: &timing
        ) {
            XCTAssertEqual(
                remoteIndexService.committedAssetFingerprintsByMonth()[month],
                [carried.asset.assetFingerprint, current.asset.assetFingerprint]
            )
            hashWriteCalled = true
        }

        XCTAssertTrue(hashWriteCalled)
        XCTAssertEqual(
            remoteIndexService.committedAssetFingerprintsByMonth()[month],
            [carried.asset.assetFingerprint, current.asset.assetFingerprint]
        )
        let seqValue = await v2.seqAllocator.value()
        XCTAssertEqual(seqValue, 1)
    }

    /// V1 commits eagerly inside upsertAsset, so commitDelta is always `.none`. A
    /// subset-tombstoning upsert still has to evict the superseded fingerprint from
    /// the in-memory cache, or Home keeps offering A as a restorable remote-only
    /// duplicate even though the manifest no longer contains it.
    func testFinalizeRowWritingAsset_v1SubsetTombstoneEvictsCacheEvenWithEmptyDelta() async throws {
        let databaseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathComponent("test.sqlite")
        try FileManager.default.createDirectory(
            at: databaseURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: databaseURL.deletingLastPathComponent()) }

        let remoteIndexService = RemoteIndexSyncService()
        let processor = AssetProcessor(
            photoLibraryService: PhotoLibraryService(),
            hashIndexRepository: ContentHashIndexRepository(databaseManager: try DatabaseManager(databaseURL: databaseURL)),
            remoteIndexService: remoteIndexService
        )
        let month = LibraryMonthKey(year: 2026, month: 1)
        let photoHash = TestFixtures.fingerprint(0xA2)
        let videoHash = TestFixtures.fingerprint(0xA3)
        let supersededFP = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: photoHash)]
        )
        let photoResource = RemoteManifestResource(
            year: month.year, month: month.month,
            physicalRemotePath: "2026/01/x.jpg",
            contentHash: photoHash, fileSize: 1,
            resourceType: ResourceTypeCode.photo, creationDateMs: nil, backedUpAtMs: 0
        )
        let videoResource = RemoteManifestResource(
            year: month.year, month: month.month,
            physicalRemotePath: "2026/01/x.mov",
            contentHash: videoHash, fileSize: 1,
            resourceType: ResourceTypeCode.pairedVideo, creationDateMs: nil, backedUpAtMs: 0
        )
        let supersededAsset = RemoteManifestAsset(
            year: month.year, month: month.month, assetFingerprint: supersededFP,
            creationDateMs: nil, backedUpAtMs: 1, resourceCount: 1, totalFileSizeBytes: 1
        )
        let supersededLink = RemoteAssetResourceLink(
            year: month.year, month: month.month, assetFingerprint: supersededFP,
            resourceHash: photoHash,
            role: ResourceTypeCode.photo, slot: 0, logicalName: "x.jpg"
        )

        // Pre-populate the optimistic cache with the photo resource + the superseded
        // asset A so eviction is observable.
        let primingWriter = remoteIndexService.makeOptimisticAssetWriter()
        primingWriter.appendResource(photoResource)
        primingWriter.appendAsset(supersededAsset, links: [supersededLink])
        XCTAssertEqual(remoteIndexService.committedAssetFingerprintsByMonth()[month], [supersededFP])

        // The superseding asset B (photo + paired video). A is a strict subset of B's
        // links — what AssetProcessor would detect via findStrictSubsetAssetFingerprints.
        let supersedingFP = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [
                (role: ResourceTypeCode.photo, slot: 0, contentHash: photoHash),
                (role: ResourceTypeCode.pairedVideo, slot: 0, contentHash: videoHash)
            ]
        )
        let supersedingAsset = RemoteManifestAsset(
            year: month.year, month: month.month, assetFingerprint: supersedingFP,
            creationDateMs: nil, backedUpAtMs: 2, resourceCount: 2, totalFileSizeBytes: 2
        )
        let supersedingLinks = [
            RemoteAssetResourceLink(
                year: month.year, month: month.month, assetFingerprint: supersedingFP,
                resourceHash: photoHash,
                role: ResourceTypeCode.photo, slot: 0, logicalName: "x.jpg"
            ),
            RemoteAssetResourceLink(
                year: month.year, month: month.month, assetFingerprint: supersedingFP,
                resourceHash: videoHash,
                role: ResourceTypeCode.pairedVideo, slot: 0, logicalName: "x.mov"
            )
        ]

        // V1-shaped store: commit returns `.none`, snapshot already reflects post-tombstone
        // state (B only, with both resources present so B is healthy).
        let store = ThrowingCommitMonthStore(year: month.year, month: month.month)
        store.shouldThrowCommit = false
        store.commitDelta = .none
        store.snapshotResources = [photoResource, videoResource]
        store.snapshotAssets = [supersedingAsset]
        store.snapshotLinks = supersedingLinks
        var timing = AssetProcessTiming()

        try await processor.finalizeRowWritingAsset(
            monthStore: store,
            manifestAsset: supersedingAsset,
            links: supersedingLinks,
            timing: &timing,
            tombstonedSubsetFingerprints: [supersededFP]
        ) {}

        let committed = remoteIndexService.committedAssetFingerprintsByMonth()[month] ?? []
        XCTAssertFalse(committed.contains(supersededFP),
                       "V1 subset-tombstone path must evict the superseded fingerprint from the optimistic cache so remote-only views stop offering it")
        XCTAssertTrue(committed.contains(supersedingFP),
                      "the superseding fingerprint must remain in the cache after the upsert")
    }

    private static func sha256(_ data: Data) -> Data {
        Data(SHA256.hash(data: data))
    }

    private static func makeV2Services(
        client: InMemoryRemoteStorageClient,
        databaseManager: DatabaseManager,
        basePath: String,
        writerID: String,
        repoID: String,
        runID: String
    ) async throws -> BackupV2RuntimeServices {
        let profileID = try TestFixtures.insertServerProfile(
            in: databaseManager,
            writerID: writerID,
            basePath: basePath,
            storageType: .webdav
        )
        let identity = RepoIdentity(database: databaseManager)
        _ = try await identity.lazyEnsureRepoState(profileID: profileID, repoID: repoID, writerID: writerID)
        let allocator = SeqAllocator(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        let lamport = PersistedLamportClock(database: databaseManager, profileID: profileID, repoID: repoID, initial: 0)
        return BackupV2RuntimeServices(
            writerID: writerID,
            repoID: repoID,
            runID: runID,
            basePath: basePath,
            database: databaseManager,
            identity: identity,
            seqAllocator: allocator,
            lamport: lamport,
            commitWriter: CommitLogWriter(client: client, basePath: basePath),
            snapshotWriter: SnapshotWriter(client: client, basePath: basePath),
            liveness: LivenessTracker(client: client, basePath: basePath, writerID: writerID, isLocalVolume: true),
            retentionRuntimeMode: .disabled,
            metadataClient: client,
            ownsMetadataClient: true,
            initialMaterializeOutput: InitialMaterializeOutputBox(nil),
            sweepTask: nil
        )
    }

    private static func makeSingleAssetRows(
        month: LibraryMonthKey,
        assetByte: UInt8,
        hashByte: UInt8,
        name: String
    ) -> (
        asset: RemoteManifestAsset,
        resource: RemoteManifestResource,
        link: RemoteAssetResourceLink
    ) {
        let hash = TestFixtures.fingerprint(hashByte)
        let assetFingerprint = BackupAssetResourcePlanner.assetFingerprint(
            resourceRoleSlotHashes: [(role: ResourceTypeCode.photo, slot: 0, contentHash: hash)]
        )
        let physicalPath = String(format: "%04d/%02d/%@", month.year, month.month, name)
        let asset = RemoteManifestAsset(
            year: month.year,
            month: month.month,
            assetFingerprint: assetFingerprint,
            creationDateMs: nil,
            backedUpAtMs: Int64(assetByte),
            resourceCount: 1,
            totalFileSizeBytes: 1
        )
        let resource = RemoteManifestResource(
            year: month.year,
            month: month.month,
            physicalRemotePath: physicalPath,
            contentHash: hash,
            fileSize: 1,
            resourceType: ResourceTypeCode.photo,
            creationDateMs: nil,
            backedUpAtMs: Int64(hashByte)
        )
        let link = RemoteAssetResourceLink(
            year: month.year,
            month: month.month,
            assetFingerprint: assetFingerprint,
            resourceHash: hash,
            role: ResourceTypeCode.photo,
            slot: 0,
            logicalName: name
        )
        return (asset, resource, link)
    }
}

private final class ThrowingCommitMonthStore: BackupMonthStore {
    enum TestError: Error {
        case commitFailed
    }

    let year: Int
    let month: Int
    var monthRelativePath: String { String(format: "%04d/%02d", year, month) }
    var monthAbsolutePath: String { "/repo/\(monthRelativePath)" }
    var v2Services: BackupV2RuntimeServices? { nil }
    var dirty: Bool { false }
    var hasAnyAsset: Bool { false }
    var shouldThrowCommit = true
    var ignoreCancellationValues: [Bool] = []
    var commitDelta = MonthManifestStore.FlushDelta.none
    var snapshotResources: [RemoteManifestResource] = []
    var snapshotAssets: [RemoteManifestAsset] = []
    var snapshotLinks: [RemoteAssetResourceLink] = []
    private let eventRecorder: EventRecorder?

    init(year: Int, month: Int, eventRecorder: EventRecorder? = nil) {
        self.year = year
        self.month = month
        self.eventRecorder = eventRecorder
    }

    func containsAssetFingerprint(_ fingerprint: Data) -> Bool { false }
    func isAssetIncomplete(_ fingerprint: Data) -> Bool { false }
    func findResourceByHash(_ contentHash: Data) -> RemoteManifestResource? { nil }
    func findByFileName(_ logicalName: String) -> RemoteManifestResource? { nil }
    func existingFileNames() -> Set<String> { [] }
    func existingCollisionKeys() -> Set<String> { [] }
    func remoteFileSize(named logicalName: String) -> Int64? { nil }
    func upsertAsset(
        _ asset: RemoteManifestAsset,
        links: [RemoteAssetResourceLink],
        replacingSubsetFingerprints: Set<Data>
    ) throws {}
    func upsertResource(_ resource: RemoteManifestResource) throws -> RemoteManifestResource { resource }
    func markRemoteFile(name: String, size: Int64) {}
    func unsortedSnapshot() -> (resources: [RemoteManifestResource], assets: [RemoteManifestAsset], links: [RemoteAssetResourceLink]) {
        (snapshotResources, snapshotAssets, snapshotLinks)
    }
    func physicallyMissingHashesSnapshot() -> Set<Data> { [] }
    var physicallyMissingHashesAreAuthoritative: Bool { false }
    func commitPendingAssetToRemote(ignoreCancellation: Bool) async throws -> MonthManifestStore.FlushDelta {
        ignoreCancellationValues.append(ignoreCancellation)
        eventRecorder?.append("commit")
        if shouldThrowCommit {
            throw TestError.commitFailed
        }
        return commitDelta
    }
    func flushToRemote(ignoreCancellation: Bool) async throws -> MonthManifestStore.FlushDelta { .none }
}

private final class EventRecorder {
    private(set) var events: [String] = []

    func append(_ event: String) {
        events.append(event)
    }
}

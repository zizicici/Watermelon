import Foundation
import XCTest
@testable import Watermelon

final class RetentionManifestRemoteStoreTests: XCTestCase {
    func testWriteLoadRoundTripCreatesRetentionOnlyAndPreservesRepoKeyspace() async throws {
        let client = try await makeClient()
        await client.injectFile(path: "\(basePath)/.watermelon/commits/sentinel.jsonl", data: Data("commit".utf8))
        await client.injectFile(path: "\(basePath)/.watermelon/snapshots/sentinel.jsonl", data: Data("snapshot".utf8))
        let before = await protectedKeyspace(client)
        let manifest = makeManifest()

        let result = try await store(client).writeVerified(manifest, respectTaskCancellation: true)
        let loaded = try await store(client).loadManifests(expectedRepoID: repoID, month: month)

        XCTAssertEqual(result.outcome, .wroteVerified)
        XCTAssertEqual(result.filename, RetentionManifestStore.filename(for: manifest.ref))
        XCTAssertEqual(result.path, RepoLayout.retentionManifestPath(base: basePath, ref: manifest.ref))
        XCTAssertEqual(loaded.valid, [manifest])
        XCTAssertEqual(loaded.invalid, [])
        XCTAssertEqual(loaded.ignoredFilenameCount, 0)
        let after = await protectedKeyspace(client)
        XCTAssertEqual(after, before)
    }

    func testWriteDistinguishesIdempotentSameBytesFromDifferentByteCollision() async throws {
        let client = try await makeClient()
        let manifest = makeManifest()
        let path = RepoLayout.retentionManifestPath(base: basePath, ref: manifest.ref)
        await client.injectFile(path: path, data: try RetentionManifestStore.encode(manifest))

        let same = try await store(client).writeVerified(manifest, respectTaskCancellation: true)
        XCTAssertEqual(same.outcome, .alreadyExistedSameBytes)

        let collisionClient = try await makeClient()
        await collisionClient.injectFile(path: path, data: Data("different".utf8))
        do {
            _ = try await store(collisionClient).writeVerified(manifest, respectTaskCancellation: true)
            XCTFail("expected collisionDifferentBytes")
        } catch RetentionManifestStoreError.collisionDifferentBytes(let filename) {
            XCTAssertEqual(filename, RetentionManifestStore.filename(for: manifest.ref))
        }
    }

    func testBestEffortWriteVerifiesAndReadbackMismatchFails() async throws {
        let client = try await makeClient()
        await client.setAtomicCreateMode(.bestEffort)
        client.setAtomicCreateGuarantee(.overwritePossible)
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setExclusiveMoveProbeOverride(false)
        let manifest = makeManifest(lamport: 43)

        let result = try await store(client).writeVerified(manifest, respectTaskCancellation: true)
        XCTAssertEqual(result.outcome, .wroteVerified)

        let mismatchClient = try await makeClient()
        await mismatchClient.setAtomicCreateMode(.bestEffort)
        mismatchClient.setAtomicCreateGuarantee(.overwritePossible)
        let mismatch = PostWriteCorruptingClient(
            inner: mismatchClient,
            corruptPath: RepoLayout.retentionManifestPath(base: basePath, ref: manifest.ref)
        )
        do {
            _ = try await store(mismatch).writeVerified(manifest, respectTaskCancellation: true)
            XCTFail("expected readback failure")
        } catch RetentionManifestStoreError.decodeRoundtripMismatch {
        } catch RetentionManifestStoreError.readbackMismatch {
        }
    }

    func testLoadSurfacesInvalidManifestEntriesSeparately() async throws {
        let client = try await makeClient()
        let retentionDir = RepoLayout.retentionDirectoryPath(base: basePath)
        try await client.createDirectory(path: retentionDir)
        let valid = makeManifest()
        let validPath = RepoLayout.retentionManifestPath(base: basePath, ref: valid.ref)
        await client.injectFile(path: validPath, data: try RetentionManifestStore.encode(valid))
        await client.injectFile(path: "\(retentionDir)/notes.txt", data: Data())
        await client.injectFile(path: "\(retentionDir)/foo--bar.json", data: Data("{}".utf8))

        let badBody = makeManifest(lamport: 44)
        await client.injectFile(
            path: RepoLayout.retentionManifestPath(base: basePath, ref: badBody.ref),
            data: Data("not json".utf8)
        )

        let mismatchBody = makeManifest(lamport: 45, createdByWriterID: writerB)
        let mismatchFilename = RetentionManifestStore.filename(for: RetentionManifestRef(
            month: month,
            lamport: 45,
            writerID: writerA,
            runIDPrefix: RepoLayout.runIDPrefix(runID)
        ))
        await client.injectFile(path: "\(retentionDir)/\(mismatchFilename)", data: try RetentionManifestStore.encode(mismatchBody))

        let foreign = makeManifest(lamport: 46, repoID: foreignRepoID)
        await client.injectFile(path: RepoLayout.retentionManifestPath(base: basePath, ref: foreign.ref), data: try RetentionManifestStore.encode(foreign))

        let otherMonth = LibraryMonthKey(year: 2026, month: 6)
        let otherMonthBody = makeManifest(lamport: 47, month: otherMonth)
        let targetMonthName = RetentionManifestStore.filename(for: RetentionManifestRef(
            month: month,
            lamport: 47,
            writerID: writerA,
            runIDPrefix: RepoLayout.runIDPrefix(runID)
        ))
        await client.injectFile(path: "\(retentionDir)/\(targetMonthName)", data: try RetentionManifestStore.encode(otherMonthBody))

        let vanished = makeManifest(lamport: 48)
        let vanishedPath = RepoLayout.retentionManifestPath(base: basePath, ref: vanished.ref)
        await client.injectFile(path: vanishedPath, data: try RetentionManifestStore.encode(vanished))
        await client.injectPersistentDownloadError(.notFound, for: vanishedPath)

        let result = try await store(client).loadManifests(expectedRepoID: repoID, month: month)

        XCTAssertEqual(result.valid, [valid])
        XCTAssertEqual(result.ignoredFilenameCount, 1)
        XCTAssertEqual(Set(result.invalid.map(\.reason)), [
            .filenameMalformed,
            .bodyDecodeFailed,
            .filenameBodyMismatch,
            .foreignRepoID(foreignRepoID),
            .monthMismatch,
            .vanishedDuringRead
        ])

        let barrier = try await store(client).loadBarrierSet(expectedRepoID: repoID, month: month)
        XCTAssertFalse(barrier.isComplete)
        XCTAssertEqual(barrier.valid, [valid])
    }

    func testDirectoryShapedRetentionManifest_isInvalid() async throws {
        let client = try await makeClient()
        let retentionDir = RepoLayout.retentionDirectoryPath(base: basePath)
        try await client.createDirectory(path: retentionDir)
        // Write a valid manifest for the target month.
        let valid = makeManifest()
        await client.injectFile(path: RepoLayout.retentionManifestPath(base: basePath, ref: valid.ref),
                                data: try RetentionManifestStore.encode(valid))
        // Create a directory at a retention manifest path for the same month.
        let dirManifest = makeManifest(lamport: 99)
        let dirPath = RepoLayout.retentionManifestPath(base: basePath, ref: dirManifest.ref)
        try await client.createDirectory(path: dirPath)

        let loaded = try await store(client).loadManifests(expectedRepoID: repoID, month: month)
        XCTAssertEqual(loaded.valid, [valid])
        XCTAssertTrue(loaded.invalid.contains(where: { $0.reason == .bodyDecodeFailed }),
                       "directory-shaped manifest for target month must be invalid")

        let barrier = try await store(client).loadBarrierSet(expectedRepoID: repoID, month: month)
        XCTAssertFalse(barrier.isComplete, "directory-shaped manifest must block barrier completeness")
    }

    func testDirectoryShapedRetentionManifest_differentMonth_isNotInvalid() async throws {
        let client = try await makeClient()
        let retentionDir = RepoLayout.retentionDirectoryPath(base: basePath)
        try await client.createDirectory(path: retentionDir)
        let valid = makeManifest()
        await client.injectFile(path: RepoLayout.retentionManifestPath(base: basePath, ref: valid.ref),
                                data: try RetentionManifestStore.encode(valid))
        // Create a directory at a retention manifest path for a different month.
        let otherMonth = LibraryMonthKey(year: 2026, month: 6)
        let otherManifest = makeManifest(lamport: 77, month: otherMonth)
        let otherPath = RepoLayout.retentionManifestPath(base: basePath, ref: otherManifest.ref)
        try await client.createDirectory(path: otherPath)

        let barrier = try await store(client).loadBarrierSet(expectedRepoID: repoID, month: month)
        XCTAssertTrue(barrier.isComplete,
                       "directory-shaped manifest for different month must not block barrier")
    }

    func testDirectoryShapedMalformedJsonName_isInvalid() async throws {
        let client = try await makeClient()
        let retentionDir = RepoLayout.retentionDirectoryPath(base: basePath)
        try await client.createDirectory(path: retentionDir)
        let valid = makeManifest()
        await client.injectFile(path: RepoLayout.retentionManifestPath(base: basePath, ref: valid.ref),
                                data: try RetentionManifestStore.encode(valid))
        // Create a directory with a malformed .json name that parseFilename rejects.
        try await client.createDirectory(path: "\(retentionDir)/not-a-manifest.json")

        let loaded = try await store(client).loadManifests(expectedRepoID: repoID, month: month)
        XCTAssertEqual(loaded.valid, [valid])
        XCTAssertTrue(loaded.invalid.contains(where: { $0.reason == .filenameMalformed && $0.filename == "not-a-manifest.json" }),
                       "malformed .json directory must be invalid as filenameMalformed")

        let barrier = try await store(client).loadBarrierSet(expectedRepoID: repoID, month: month)
        XCTAssertFalse(barrier.isComplete, "malformed .json directory must block barrier completeness")
    }

    func testLoadBarrierSetMatchesPureSupersedingSemantics() async throws {
        let client = try await makeClient()
        let manifests = [
            makeManifest(lamport: 1, covered: covered([writerA: [(1, 10)]])),
            makeManifest(lamport: 2, covered: covered([writerA: [(1, 20)]]), deletePrefix: [writerA: 20]),
            makeManifest(lamport: 3, covered: covered([writerB: [(1, 5)]]), deletePrefix: [writerB: 5]),
            makeManifest(lamport: 4, covered: covered([writerA: [(30, 40)]]), deletePrefix: [:])
        ]
        for manifest in manifests {
            _ = try await store(client).writeVerified(manifest, respectTaskCancellation: true)
        }

        let loaded = try await store(client).loadBarrierSet(expectedRepoID: repoID, month: month)
        let expected = RetentionBarrierSet.unsuperseded(manifests: manifests)

        XCTAssertTrue(loaded.isComplete)
        XCTAssertEqual(
            Set(loaded.valid.map { RetentionManifestStore.filename(for: $0.ref) }),
            Set(manifests.map { RetentionManifestStore.filename(for: $0.ref) })
        )
        XCTAssertEqual(
            Set(loaded.barrierSet.unsuperseded.map { RetentionManifestStore.filename(for: $0.ref) }),
            Set(expected.unsuperseded.map { RetentionManifestStore.filename(for: $0.ref) })
        )
        XCTAssertEqual(loaded.barrierSet.unionCovered, expected.unionCovered)
    }

    func testNoRuntimeCallSiteAndStoreDoesNotDelete() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let sourceFiles = try FileManager.default.subpathsOfDirectory(atPath: root.path)
            .filter { $0.hasSuffix(".swift") }
        let allowed = Set([
            "Shared/Services/Backup/V2RetentionBarrierRefresh.swift",
            "Shared/Services/Repo/RepoCheckpointBarrierHook.swift",
            "Shared/Services/Repo/RepoMaintenanceCoordinator.swift",
            "Shared/Services/Repo/RepoRetentionDeletePreflightService.swift",
            "Shared/Services/Repo/RepoSnapshotDeletePreflightService.swift",
            "Shared/Services/Repo/RetentionMaintenanceOrchestrator.swift",
            "Shared/Services/Repo/RetentionManifestRemoteStore.swift",
            "Shared/Services/Repo/RepoRetentionBarrierService.swift",
            "WatermelonTests/RetentionManifestRemoteStoreTests.swift",
            "WatermelonTests/RepoRetentionBarrierServiceTests.swift"
        ])
        for path in sourceFiles where !path.hasPrefix("WatermelonTests/") {
            let text = try String(contentsOf: root.appendingPathComponent(path), encoding: .utf8)
            if text.contains("RetentionManifestRemoteStore") || text.contains("RepoRetentionBarrierService") {
                XCTAssertTrue(allowed.contains(path), "unexpected production reference in \(path)")
            }
        }
        let storeSource = try String(
            contentsOf: root.appendingPathComponent("Shared/Services/Repo/RetentionManifestRemoteStore.swift"),
            encoding: .utf8
        )
        XCTAssertFalse(storeSource.contains("client.delete("))
    }

    private func makeClient() async throws -> InMemoryRemoteStorageClient {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        try await TestFixtures.injectRepoJSON(client, basePath: basePath, repoID: repoID, writerID: writerA)
        try await TestFixtures.injectVersionJSON(client, basePath: basePath, writerID: writerA)
        try await client.createDirectory(path: RepoLayout.commitsDirectoryPath(base: basePath))
        try await client.createDirectory(path: RepoLayout.snapshotsDirectoryPath(base: basePath))
        return client
    }

    private func store(_ client: any RemoteStorageClientProtocol) -> RetentionManifestRemoteStore {
        RetentionManifestRemoteStore(client: client, basePath: basePath)
    }

    private func protectedKeyspace(_ client: InMemoryRemoteStorageClient) async -> [String: Data] {
        await client.snapshotFiles().filter {
            $0.key.contains("/.watermelon/commits/")
                || $0.key.contains("/.watermelon/snapshots/")
                || $0.key.contains("/.watermelon/liveness/")
                || $0.key.contains("/.watermelon/identity/")
                || $0.key.contains("/.watermelon/migrations/")
        }
    }

    private func makeManifest(
        lamport: UInt64 = 42,
        repoID: String = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        month: LibraryMonthKey = LibraryMonthKey(year: 2026, month: 5),
        createdByWriterID: String = "11111111-1111-1111-1111-aaaaaaaaaaaa",
        covered: CoveredRanges? = nil,
        deletePrefix: [String: UInt64]? = nil
    ) -> RetentionManifest {
        let coveredRanges = covered ?? self.covered([createdByWriterID: [(1, 5)]])
        return RetentionManifest(
            version: RetentionManifest.currentVersion,
            repoID: repoID,
            month: month,
            createdByWriterID: createdByWriterID,
            runID: UUID(uuidString: runID)!,
            createdAtMs: 1_700_000_000_000 + Int64(lamport),
            barrierLamport: lamport,
            checkpointSnapshotName: RepoLayout.snapshotFileName(month: month, lamport: lamport, writerID: createdByWriterID, runID: runID),
            checkpointSHA256Hex: String(repeating: "a", count: 64),
            coveredRanges: coveredRanges,
            deletePrefixByWriter: deletePrefix ?? coveredRanges.conservativeContiguousPrefixByWriter(),
            observedSeqHighByWriter: coveredRanges.rangesByWriter.mapValues { $0.map(\.high).max() ?? 0 },
            policy: RetentionManifestPolicy(
                keepUncoveredCommits: true,
                keepCorruptOrUntrustedCommits: true,
                keepTombstones: true,
                snapshotKeepCount: 2
            ),
            livenessGate: RetentionLivenessGate(
                requiredCompleteView: true,
                requiredNoActiveNonSelfWriters: true,
                legacyClientGraceMs: 604_800_000
            )
        )
    }

    private func covered(_ ranges: [String: [(UInt64, UInt64)]]) -> CoveredRanges {
        CoveredRanges(rangesByWriter: ranges.mapValues { $0.map { ClosedSeqRange(low: $0.0, high: $0.1) } })
    }

    private let basePath = "/unit4-store"
    private let repoID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    private let foreignRepoID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
    private let writerA = "11111111-1111-1111-1111-aaaaaaaaaaaa"
    private let writerB = "22222222-2222-2222-2222-bbbbbbbbbbbb"
    private let runID = "33333333-3333-3333-3333-333333333333"
    private let month = LibraryMonthKey(year: 2026, month: 5)
}

private final class PostWriteCorruptingClient: @unchecked Sendable, RemoteStorageClientProtocol {
    let inner: InMemoryRemoteStorageClient
    let corruptPath: String
    private let lock = NSLock()
    private var downloadCount = 0

    init(inner: InMemoryRemoteStorageClient, corruptPath: String) {
        self.inner = inner
        self.corruptPath = Self.normalize(corruptPath)
    }

    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    nonisolated var moveIfAbsentGuarantee: CreateGuarantee { .exclusive }
    nonisolated var readAfterWriteGraceSeconds: TimeInterval { 0 }
    var dataPathOverwriteRisk: DataPathOverwriteRisk { .perKey }
    var supportsLivenessSafeOverwriteUpload: Bool { false }
    var backendNameCaseSensitivity: BackendNameCaseSensitivity { .caseSensitive }
    var isSerialized: Bool { false }

    func shouldSetModificationDate() -> Bool { true }
    func shouldLimitUploadRetries(for error: Error) -> Bool { false }
    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func verifyWriteAccess() async throws {}
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }
    func supportsExclusiveMoveIfAbsent(forDestinationPath destinationPath: String) async throws -> Bool {
        try await inner.supportsExclusiveMoveIfAbsent(forDestinationPath: destinationPath)
    }
    func upload(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(localURL: URL, remotePath: String, respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        try await inner.download(remotePath: remotePath, localURL: localURL)
        let path = Self.normalize(remotePath)
        guard path == corruptPath else { return }
        let shouldCorrupt = lock.withLock {
            downloadCount += 1
            return downloadCount > 1
        }
        if shouldCorrupt {
            try Data("corrupt".utf8).write(to: localURL, options: .atomic)
        }
    }
    func exists(path: String) async throws -> Bool { try await inner.exists(path: path) }
    func delete(path: String) async throws { try await inner.delete(path: path) }
    func createDirectory(path: String) async throws { try await inner.createDirectory(path: path) }
    func move(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.move(from: sourcePath, to: destinationPath)
    }
    func moveIfAbsent(from sourcePath: String, to destinationPath: String) async throws -> AtomicCreateResult {
        try await inner.moveIfAbsent(from: sourcePath, to: destinationPath)
    }
    func copy(from sourcePath: String, to destinationPath: String) async throws {
        try await inner.copy(from: sourcePath, to: destinationPath)
    }

    private static func normalize(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty else { return "/" }
        return "/" + trimmed.split(separator: "/", omittingEmptySubsequences: true).joined(separator: "/")
    }
}

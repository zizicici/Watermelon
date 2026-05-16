import XCTest
@testable import Watermelon

final class MigrationMarkerStoreTests: XCTestCase {
    private let basePath = "/repo"
    private let validWriterID = "11111111-1111-1111-1111-111111111111"
    private let otherWriterID = "22222222-2222-2222-2222-222222222222"

    // MARK: - writePhase: canonical phase1 fast path

    func testWritePhase1_freshWrite_usesCanonicalPath() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        try await store.writePhase(writerID: validWriterID, phase: .phase1, runID: "run-1")

        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        let snapshot = await client.snapshotFiles()
        XCTAssertNotNil(snapshot[canonical], "phase1 fresh write must land at canonical path")
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        let suffixed = snapshot.keys.filter { $0.hasPrefix(dir + "/") && $0 != canonical }
        XCTAssertTrue(suffixed.isEmpty, "no phase-suffixed sibling allowed on fresh phase1: \(suffixed)")
    }

    func testWritePhase1_collidesWithExistingCanonical_writesPhaseSuffixedMarker() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await client.injectFile(path: canonical, data: Data("{}".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        try await store.writePhase(writerID: validWriterID, phase: .phase1, runID: "run-2")

        let snapshot = await client.snapshotFiles()
        XCTAssertEqual(snapshot[canonical], Data("{}".utf8), "canonical must be untouched when it already exists")
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        let suffixed = snapshot.keys
            .filter { $0.hasPrefix(dir + "/") && $0 != canonical }
            .filter { ($0 as NSString).lastPathComponent.contains("--phase1--") }
        XCTAssertEqual(suffixed.count, 1, "collision must fall through to exactly one phase-suffixed marker: \(suffixed)")
    }

    func testWritePhase2_alwaysUnique_neverWritesCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        try await store.writePhase(writerID: validWriterID, phase: .phase2, runID: "run-3")

        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        let snapshot = await client.snapshotFiles()
        XCTAssertNil(snapshot[canonical], "phase2 must never land at canonical path")
        let suffixed = snapshot.keys.filter { ($0 as NSString).lastPathComponent.contains("--phase2--") }
        XCTAssertEqual(suffixed.count, 1)
    }

    func testWritePhase3_alwaysUnique_neverWritesCanonical() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        try await store.writePhase(writerID: validWriterID, phase: .phase3, runID: "run-4")

        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        let snapshot = await client.snapshotFiles()
        XCTAssertNil(snapshot[canonical])
        let suffixed = snapshot.keys.filter { ($0 as NSString).lastPathComponent.contains("--phase3--") }
        XCTAssertEqual(suffixed.count, 1)
    }

    func testWritePhase1_reusesExistingParseableStartedAtMsOnCollision() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        try await injectValidMarker(
            client,
            path: canonical,
            writerID: validWriterID,
            phase: 1,
            startedAtMs: 1_000
        )
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        try await store.writePhase(writerID: validWriterID, phase: .phase1, runID: "retry-run")

        let snapshot = await client.snapshotFiles()
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        let suffixed = snapshot.keys
            .filter { $0.hasPrefix(dir + "/") && $0 != canonical }
            .filter { ($0 as NSString).lastPathComponent.contains("--phase1--") }
        XCTAssertEqual(suffixed.count, 1, "collision must produce exactly one phase1-suffixed marker")
        guard let suffixedPath = suffixed.first, let bytes = snapshot[suffixedPath] else {
            XCTFail("expected suffixed marker payload")
            return
        }
        let parsed = try MigrationMarker.parse(
            filename: (suffixedPath as NSString).lastPathComponent,
            bytes: bytes
        )
        XCTAssertEqual(
            parsed.startedAtMs,
            1_000,
            "retried phase1 must reuse existing startedAtMs, not stamp now"
        )
    }

    // MARK: - existsFor / deleteAll on malformed markers

    func testExistsFor_returnsTrueForMalformedCanonicalMarker() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await client.injectFile(path: canonical, data: Data("not-json".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let exists = try await store.existsFor(writerID: validWriterID)
        XCTAssertTrue(exists, "metadata-only presence must survive unparseable bytes")
    }

    /// Short writerIDs are not accepted by `RepoLayout.parseMigrationMarkerFilename`,
    /// so the canonical seed in `pathsFor` must be unconditional or `existsFor`
    /// returns false for fixtures the production code path correctly recognizes.
    func testExistsFor_returnsTrueForShortNonUUIDWriterID() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: "peer")
        await client.injectFile(path: canonical, data: Data("not-json".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let exists = try await store.existsFor(writerID: "peer")
        XCTAssertTrue(exists)
    }

    func testDeleteAll_removesMalformedCanonicalAndPhaseSuffixed() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        let phasePath = RepoLayout.migrationPhaseMarkerPath(
            base: basePath, writerID: validWriterID, phase: 2, markerID: "abcd"
        )
        await client.injectFile(path: canonical, data: Data("garbage-1".utf8))
        await client.injectFile(path: phasePath, data: Data("garbage-2".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        try await store.deleteAll(writerID: validWriterID)

        let canonicalGone = await client.hasFile(canonical) == false
        let phaseGone = await client.hasFile(phasePath) == false
        XCTAssertTrue(canonicalGone)
        XCTAssertTrue(phaseGone)
    }

    func testExistsFor_treatsDirectoryAtCanonicalPathAsPresence() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        try await client.createDirectory(path: canonical)
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let exists = try await store.existsFor(writerID: validWriterID)
        XCTAssertTrue(exists, "metadata-only presence includes directories — no isDirectory filter")
    }

    func testDeleteAll_removesCanonicalForNonUUIDWriterID() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: "peer")
        await client.injectFile(path: canonical, data: Data("garbage".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        try await store.deleteAll(writerID: "peer")

        let canonicalGone = await client.hasFile(canonical) == false
        XCTAssertTrue(canonicalGone, "deleteAll must remove canonical even when writerID isn't UUID-shaped")
    }

    func testDeleteAll_doesNotTouchOtherWriterMarkers() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let ours = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        let theirs = RepoLayout.migrationMarkerPath(base: basePath, writerID: otherWriterID)
        await client.injectFile(path: ours, data: Data("a".utf8))
        await client.injectFile(path: theirs, data: Data("b".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        try await store.deleteAll(writerID: validWriterID)

        let oursExists = await client.hasFile(ours)
        let theirsExists = await client.hasFile(theirs)
        XCTAssertFalse(oursExists)
        XCTAssertTrue(theirsExists, "deleteAll must scope to requested writer")
    }

    // MARK: - currentPhase / startedAt tolerant parse

    func testCurrentPhase_fallsBackToPhase1WhenCanonicalBytesUnparseable() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await client.injectFile(path: canonical, data: Data("not-json".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let phase = try await store.currentPhase(writerID: validWriterID)
        XCTAssertEqual(phase, .phase1)
    }

    /// A directory squatting at the canonical marker path is a `sawMarker = true`
    /// signal so callers (e.g. phase1 idempotence) treat it the same way
    /// `existsFor` already does. Filtering it out of `currentPhase` would let two
    /// APIs answering the same question disagree for the same path.
    func testCurrentPhase_directoryAtCanonicalPath_treatedAsPhase1() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        try await client.createDirectory(path: canonical)
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let phase = try await store.currentPhase(writerID: validWriterID)
        XCTAssertEqual(phase, .phase1, "directory at canonical path must count as sawMarker, matching existsFor")
    }

    func testCurrentPhase_returnsNilWhenNoMarkers() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let phase = try await store.currentPhase(writerID: validWriterID)
        XCTAssertNil(phase)
    }

    func testCurrentPhase_foldsMaxPhaseAcrossMarkers() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let phase2Path = RepoLayout.migrationPhaseMarkerPath(
            base: basePath, writerID: validWriterID, phase: 2, markerID: "aaaa1111-aaaa-1111-aaaa-111111111111"
        )
        let phase3Path = RepoLayout.migrationPhaseMarkerPath(
            base: basePath, writerID: validWriterID, phase: 3, markerID: "bbbb2222-bbbb-2222-bbbb-222222222222"
        )
        try await injectValidMarker(client, path: phase2Path, writerID: validWriterID, phase: 2)
        try await injectValidMarker(client, path: phase3Path, writerID: validWriterID, phase: 3)
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let phase = try await store.currentPhase(writerID: validWriterID)
        XCTAssertEqual(phase, .phase3)
    }

    func testStartedAt_returnsValueWhenParseableMarkerExists() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        try await injectValidMarker(client, path: canonical, writerID: validWriterID, phase: 1, startedAtMs: 1_700_000_000_000)
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let startedAt = try await store.startedAt(writerID: validWriterID)
        XCTAssertEqual(startedAt, 1_700_000_000_000)
    }

    func testStartedAt_nilWhenNoParseableMarkers() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await client.injectFile(path: canonical, data: Data("garbage".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let startedAt = try await store.startedAt(writerID: validWriterID)
        XCTAssertNil(startedAt)
    }

    func testCurrentPhase_swallowsCancellationFromDownloadAndReturnsPhase1WhenMetadataPresent() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await inner.injectFile(path: canonical, data: Data("{}".utf8))
        let client = CancellingDownloadClient(inner: inner)
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        let phase = try await store.currentPhase(writerID: validWriterID)
        XCTAssertEqual(phase, .phase1, "tolerant parser must swallow CancellationError and fall back to sawMarker → phase1")
    }

    // MARK: - parseEntries inspection policy

    func testParseEntries_skipsParseFailuresAndReturnsValidOnly() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        try await client.createDirectory(path: dir)

        // filename unparseable
        await client.injectFile(path: "\(dir)/not-a-writer.json", data: Data("{}".utf8))
        // writerID mismatch (filename WriterID-A, JSON writer_id=B)
        let mismatchFilename = "\(validWriterID).json"
        let mismatchPath = "\(dir)/\(mismatchFilename)"
        await client.injectFile(
            path: mismatchPath,
            data: Data(#"{"v":2,"writer_id":"\#(otherWriterID)","phase":2}"#.utf8)
        )
        // phase wrong type (boolean)
        let booleanWriter = "33333333-3333-3333-3333-333333333333"
        let booleanPath = "\(dir)/\(booleanWriter).json"
        await client.injectFile(
            path: booleanPath,
            data: Data(#"{"v":2,"writer_id":"\#(booleanWriter)","phase":true}"#.utf8)
        )
        // valid marker
        let validPath = RepoLayout.migrationPhaseMarkerPath(
            base: basePath, writerID: otherWriterID, phase: 2, markerID: "cccc3333-cccc-3333-cccc-333333333333"
        )
        try await injectValidMarker(client, path: validPath, writerID: otherWriterID, phase: 2)

        let store = MigrationMarkerStore(client: client, basePath: basePath)
        let entries = try await store.migrationsDirectoryEntries()
        let parsed = try await store.parseEntries(entries)

        XCTAssertEqual(parsed.count, 1)
        XCTAssertEqual(parsed.first?.writerID, otherWriterID)
        XCTAssertEqual(parsed.first?.phase, .phase2)
    }

    func testParseEntries_skipsNotFoundDownload() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        try await client.createDirectory(path: dir)
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await client.injectFile(path: markerPath, data: Data("{}".utf8))
        await client.injectDownloadError(.notFound, for: markerPath)

        let store = MigrationMarkerStore(client: client, basePath: basePath)
        let entries = try await store.migrationsDirectoryEntries()
        let parsed = try await store.parseEntries(entries)

        XCTAssertTrue(parsed.isEmpty, "file deleted between list and download must skip, not throw")
    }

    func testParseEntries_rethrowsNonNotFoundDownloadErrors() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        try await client.createDirectory(path: dir)
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await client.injectFile(path: markerPath, data: Data("{}".utf8))
        await client.injectPersistentDownloadError(.transport, for: markerPath)

        let store = MigrationMarkerStore(client: client, basePath: basePath)
        let entries = try await store.migrationsDirectoryEntries()
        do {
            _ = try await store.parseEntries(entries)
            XCTFail("expected transport error to propagate")
        } catch is CancellationError {
            XCTFail("transport error must not be coerced to CancellationError")
        } catch {
            // expected: non-not-found IO surfaces so a network blip can't be misread as "no markers"
        }
    }

    func testParseEntries_propagatesCancellation() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        try await inner.createDirectory(path: dir)
        let markerPath = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await inner.injectFile(path: markerPath, data: Data("{}".utf8))

        let client = CancellingDownloadClient(inner: inner)
        let store = MigrationMarkerStore(client: client, basePath: basePath)
        let entries = try await store.migrationsDirectoryEntries()
        do {
            _ = try await store.parseEntries(entries)
            XCTFail("expected CancellationError to propagate")
        } catch is CancellationError {
            // expected
        }
    }

    // MARK: - existsAny

    func testExistsAny_falseWhenDirectoryMissing() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let store = MigrationMarkerStore(client: client, basePath: basePath)
        let any = try await store.existsAny()
        XCTAssertFalse(any)
    }

    func testExistsAny_falseWhenDirectoryHasOnlyNonJSONFiles() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        await client.injectFile(path: "\(dir)/.DS_Store", data: Data())
        let store = MigrationMarkerStore(client: client, basePath: basePath)
        let any = try await store.existsAny()
        XCTAssertFalse(any)
    }

    func testExistsAny_trueWhenAnyJSONPresent() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let dir = RepoLayout.migrationsDirectoryPath(base: basePath)
        await client.injectFile(path: "\(dir)/something.json", data: Data("{}".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)
        let any = try await store.existsAny()
        XCTAssertTrue(any)
    }

    // MARK: - NSError shapes

    func testWritePhase_unique4AttemptsExhausted_throws43() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // `.exclusive` makes the gate hit atomicCreate directly; `.alwaysAlreadyExists`
        // forces every attempt to collide so the 4-retry budget exhausts.
        client.setAtomicCreateGuarantee(.exclusive)
        await client.setAtomicCreateMode(.alwaysAlreadyExists)
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        do {
            try await store.writePhase(writerID: validWriterID, phase: .phase2, runID: "run-x")
            XCTFail("expected -43 unique-allocation exhaustion")
        } catch let nsError as NSError {
            XCTAssertEqual(nsError.domain, "V1MigrationService")
            XCTAssertEqual(nsError.code, -43)
        }
    }

    func testWritePhase_verifyMismatch_throws41() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // `.exclusive` + `.bestEffort` → gate returns .bestEffortRetry with
        // verifiedAgainstLocalContent=false, so the store invokes verify().
        // `stageBestEffortRace` ensures the persisted bytes diverge from local.
        client.setAtomicCreateGuarantee(.exclusive)
        await client.setAtomicCreateMode(.bestEffort)
        let canonical = RepoLayout.migrationMarkerPath(base: basePath, writerID: validWriterID)
        await client.stageBestEffortRace(at: canonical, with: Data("hijacked".utf8))
        let store = MigrationMarkerStore(client: client, basePath: basePath)

        do {
            try await store.writePhase(writerID: validWriterID, phase: .phase1, runID: "run-y")
            XCTFail("expected -41 readback mismatch")
        } catch let nsError as NSError {
            XCTAssertEqual(nsError.domain, "V1MigrationService")
            XCTAssertEqual(nsError.code, -41)
        }
    }

    // MARK: - Helpers

    private func injectValidMarker(
        _ client: InMemoryRemoteStorageClient,
        path: String,
        writerID: String,
        phase: Int,
        startedAtMs: Int64? = nil
    ) async throws {
        var dict: [String: Any] = [
            "v": 2,
            "writer_id": writerID,
            "phase": phase
        ]
        if let startedAtMs { dict["started_at_ms"] = startedAtMs }
        let data = try JSONSerialization.data(withJSONObject: dict, options: [.sortedKeys])
        await client.injectFile(path: path, data: data)
    }
}

/// Wraps the in-memory client; forces every `download` to throw `CancellationError`
/// so we can assert `parseEntries` propagation without modifying production code.
private actor CancellingDownloadClient: RemoteStorageClientProtocol {
    private let inner: InMemoryRemoteStorageClient
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }

    init(inner: InMemoryRemoteStorageClient) {
        self.inner = inner
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? { try await inner.metadata(path: path) }
    func upload(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws {
        try await inner.upload(
            localURL: localURL, remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation, onProgress: onProgress
        )
    }
    func atomicCreate(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(
            localURL: localURL, remotePath: remotePath,
            respectTaskCancellation: respectTaskCancellation, onProgress: onProgress
        )
    }
    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        throw CancellationError()
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
}

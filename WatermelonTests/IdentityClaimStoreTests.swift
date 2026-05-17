import XCTest
@testable import Watermelon

/// Locks the invariants `RepoBootstrap.ensureRepoJSON` depends on. Each test
/// drives `IdentityClaimStore` directly through `InMemoryRemoteStorageClient`,
/// so ambiguity that needs a particular `AtomicCreateResult` shape is staged
/// through `setAtomicCreateMode` / `stageBestEffortRace` or a tiny wrapper.
final class IdentityClaimStoreTests: XCTestCase {
    private let basePath = "/repo"
    private let selfWriter = "11111111-1111-1111-1111-111111111111"
    private let otherWriter = "22222222-2222-2222-2222-222222222222"
    private let thirdWriter = "33333333-3333-3333-3333-333333333333"

    // MARK: - Election

    func testCanonicalElection_emptyIdentityDirectory_returnsNilNoCorrupt() async throws {
        let (client, store) = await makeStore()
        let result = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
        XCTAssertNil(result.repoID)
        XCTAssertFalse(result.ignoredSelfCorrupt)
        _ = client
    }

    func testCanonicalElection_listNonNotFoundError_propagates() async throws {
        let (client, store) = await makeStore()
        await client.injectListError(.transport, for: RepoLayout.identityDirectoryPath(base: basePath))
        do {
            _ = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
            XCTFail("expected transport error to propagate")
        } catch is CancellationError {
            XCTFail("transport error must not be coerced to CancellationError")
        } catch {
            // expected — fail-closed list error
        }
    }

    func testCanonicalElection_directoryShapedLikeClaim_throwsMalformed() async throws {
        let (client, store) = await makeStore()
        let identityDir = RepoLayout.identityDirectoryPath(base: basePath)
        // Plant a child directory at `<identity>/<selfWriter>.json` to model a
        // half-bootstrapped remote where a stray directory occupies the claim path.
        try await client.createDirectory(path: "\(identityDir)/\(selfWriter).json/child")
        do {
            _ = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
            XCTFail("expected malformed-directory throw")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 17)
        }
    }

    func testCanonicalElection_lexMinByCreatedAtMs() async throws {
        let (client, store) = await makeStore()
        await injectValidClaim(client, writerID: selfWriter, repoID: "repo-A", createdAtMs: 2_000)
        await injectValidClaim(client, writerID: otherWriter, repoID: "repo-B", createdAtMs: 1_000)
        await injectValidClaim(client, writerID: thirdWriter, repoID: "repo-C", createdAtMs: 3_000)
        let result = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
        XCTAssertEqual(result.repoID, "repo-B", "oldest claim must win lex-min")
    }

    func testCanonicalElection_tieBreakByWriterID() async throws {
        let (client, store) = await makeStore()
        await injectValidClaim(client, writerID: otherWriter, repoID: "repo-B", createdAtMs: 1_000)
        await injectValidClaim(client, writerID: selfWriter, repoID: "repo-A", createdAtMs: 1_000)
        let result = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
        // selfWriter < otherWriter lexicographically ("111..." < "222...").
        XCTAssertEqual(result.repoID, "repo-A", "ms-tie must break on lex-smaller writerID")
    }

    func testCanonicalElection_serialOnlyClient_returnsLexMin() async throws {
        // SMB/SFTP advertise .serialOnly; this exercises the sequential branch
        // (no TaskGroup fan-out) with the same expected lex-min outcome.
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        await injectValidClaim(inner, writerID: selfWriter, repoID: "repo-A", createdAtMs: 2_000)
        await injectValidClaim(inner, writerID: otherWriter, repoID: "repo-B", createdAtMs: 1_000)
        await injectValidClaim(inner, writerID: thirdWriter, repoID: "repo-C", createdAtMs: 3_000)
        let serial = SerialOnlyWrapperClient(inner: inner)
        let store = IdentityClaimStore(client: serial, basePath: basePath)
        let result = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
        XCTAssertEqual(result.repoID, "repo-B", "serialOnly branch must match concurrent lex-min outcome")
    }

    /// A foreign claim with `"created_at_ms": true` must not bridge through `as? Int`
    /// to timestamp 1 and silently win lex-min election. `strictInt64` rejects
    /// CFBoolean ahead of any numeric cast, forcing the corrupt-foreign path.
    func testCanonicalElection_foreignBooleanTimestamp_throwsCorrupt() async throws {
        let (client, store) = await makeStore()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: otherWriter)
        let dict: [String: Any] = [
            "v": 1,
            "repo_id": "hijack",
            "created_at_ms": true,
            "writer_id": otherWriter
        ]
        let data = try JSONSerialization.data(withJSONObject: dict)
        await client.injectFile(path: path, data: data)
        do {
            _ = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
            XCTFail("expected throw on boolean timestamp claim")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 9)
        }
    }

    /// Same defense applied to a self-corrupt claim: it must surface as the soft
    /// `ignoredSelfCorrupt` signal, not adopt timestamp 1.
    func testCanonicalElection_selfBooleanTimestamp_softSelfCorrupt() async throws {
        let (client, store) = await makeStore()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        let dict: [String: Any] = [
            "v": 1,
            "repo_id": "ours",
            "created_at_ms": false,
            "writer_id": selfWriter
        ]
        let data = try JSONSerialization.data(withJSONObject: dict)
        await client.injectFile(path: path, data: data)
        let result = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
        XCTAssertNil(result.repoID, "boolean timestamp must not be accepted as a valid claim")
        XCTAssertTrue(result.ignoredSelfCorrupt, "self boolean timestamp must surface as soft self-corrupt")
    }

    func testCanonicalElection_filenameMismatchedPayloadWriterID_throws() async throws {
        let (client, store) = await makeStore()
        // Filename writerID-A but payload writer_id=B (forged-timestamp defense).
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: otherWriter)
        let dict: [String: Any] = [
            "v": 1,
            "repo_id": "hijack",
            "created_at_ms": 0,
            "writer_id": thirdWriter
        ]
        let data = try JSONSerialization.data(withJSONObject: dict)
        await client.injectFile(path: path, data: data)
        do {
            _ = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
            XCTFail("expected throw on filename/payload writer mismatch")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 9)
        }
    }

    func testCanonicalElection_foreignCorruptClaim_throws() async throws {
        let (client, store) = await makeStore()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: otherWriter)
        await client.injectFile(path: path, data: Data("not-json".utf8))
        do {
            _ = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
            XCTFail("expected throw on foreign corrupt claim")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 9)
        }
    }

    func testCanonicalElection_ownCorruptClaim_noOtherClaim_returnsSoftSignal() async throws {
        let (client, store) = await makeStore()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        await client.injectFile(path: path, data: Data("not-json".utf8))
        let result = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
        XCTAssertNil(result.repoID, "own corrupt with no other claim must yield nil repoID")
        XCTAssertTrue(result.ignoredSelfCorrupt, "self-corrupt must surface as a soft signal")
    }

    func testCanonicalElection_ownCorruptClaim_withValidOther_returnsOther() async throws {
        let (client, store) = await makeStore()
        let selfPath = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        await client.injectFile(path: selfPath, data: Data("not-json".utf8))
        await injectValidClaim(client, writerID: otherWriter, repoID: "repo-B", createdAtMs: 1_000)
        let result = try await store.canonicalElection(ignoringCorruptSelfClaimFor: selfWriter)
        XCTAssertEqual(result.repoID, "repo-B")
        XCTAssertTrue(result.ignoredSelfCorrupt, "soft signal still surfaces alongside a valid peer claim")
    }

    // MARK: - Heal

    func testHeal_zeroByteWithEmptyDownload_deletes() async throws {
        let (client, store) = await makeStore()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        await client.injectFile(path: path, data: Data())
        try await store.healZeroByteSelfClaim(writerID: selfWriter)
        let gone = await client.hasFile(path) == false
        XCTAssertTrue(gone, "zero-byte heal must delete the half-written claim")
    }

    func testHeal_zeroByteMetadataButNonEmptyDownload_doesNotDelete() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        await inner.injectFile(path: path, data: Data())  // metadata size=0
        let nonEmpty = Data(#"{"v":1,"repo_id":"r","writer_id":"\#(selfWriter)","created_at_ms":1}"#.utf8)
        let client = ZeroSizeMetadataNonEmptyDownloadClient(inner: inner, racePath: path, raceBytes: nonEmpty)
        let store = IdentityClaimStore(client: client, basePath: basePath)

        try await store.healZeroByteSelfClaim(writerID: selfWriter)

        // The wrapper makes `download` return non-empty bytes despite metadata size=0.
        // Heal must trust the downloaded payload, not metadata, to avoid deleting a healthy claim under disk pressure.
        let stillThere = await inner.hasFile(path)
        XCTAssertTrue(stillThere, "non-empty download must NOT trigger delete even when metadata reports size 0")
    }

    func testHeal_nonZeroByteClaim_untouched() async throws {
        let (client, store) = await makeStore()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        await injectValidClaim(client, writerID: selfWriter, repoID: "repo-A", createdAtMs: 100)
        try await store.healZeroByteSelfClaim(writerID: selfWriter)
        let stillThere = await client.hasFile(path)
        XCTAssertTrue(stillThere)
    }

    func testHeal_claimAbsent_isNoOp() async throws {
        let (_, store) = await makeStore()
        try await store.healZeroByteSelfClaim(writerID: selfWriter)
        // no throw, nothing to assert beyond absence of a file
    }

    // MARK: - Write own claim

    func testWriteOwn_noPriorClaim_writesValidPayload() async throws {
        let (client, store) = await makeStore()
        try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 1_234)
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        guard let bytes = await client.snapshotFiles()[path] else {
            XCTFail("expected claim bytes at \(path)")
            return
        }
        let dict = try JSONSerialization.jsonObject(with: bytes) as? [String: Any]
        XCTAssertEqual(dict?["repo_id"] as? String, "repo-A")
        XCTAssertEqual(dict?["writer_id"] as? String, selfWriter)
        XCTAssertEqual((dict?["created_at_ms"] as? Int64) ?? Int64(dict?["created_at_ms"] as? Int ?? 0), 1_234)
    }

    func testWriteOwn_priorClaimIsOurs_doesNotRewrite() async throws {
        // Wrapper throws on any atomicCreate so the .ours short-circuit is the only way the call succeeds.
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        await injectValidClaim(inner, writerID: selfWriter, repoID: "repo-A", createdAtMs: 1_000)
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        let before = await inner.snapshotFiles()[path]
        let client = ForbidAtomicCreateClient(inner: inner)
        let store = IdentityClaimStore(client: client, basePath: basePath)
        try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 5_000)
        let after = await inner.snapshotFiles()[path]
        XCTAssertEqual(before, after, ".ours must short-circuit before atomicCreate; bytes untouched")
        let attempts = await client.atomicCreateCount
        XCTAssertEqual(attempts, 0, ".ours must short-circuit; zero atomicCreate attempts allowed")
    }

    func testWriteOwn_priorClaimZeroByte_deletesAndWrites() async throws {
        let (client, store) = await makeStore()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        await client.injectFile(path: path, data: Data())
        try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 1_000)
        let bytes = await client.snapshotFiles()[path] ?? Data()
        let dict = try JSONSerialization.jsonObject(with: bytes) as? [String: Any]
        XCTAssertEqual(dict?["repo_id"] as? String, "repo-A")
        XCTAssertEqual(dict?["writer_id"] as? String, selfWriter)
    }

    func testWriteOwn_priorClaimStaleRepoID_deletesAndWritesNewID() async throws {
        let (client, store) = await makeStore()
        await injectValidClaim(client, writerID: selfWriter, repoID: "old-repo", createdAtMs: 999)
        try await store.writeOwnClaim(repoID: "new-repo", writerID: selfWriter, createdAtMs: 2_000)
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        let snapshot = await client.snapshotFiles()
        let bytes = try XCTUnwrap(snapshot[path])
        let dict = try JSONSerialization.jsonObject(with: bytes) as? [String: Any]
        XCTAssertEqual(dict?["repo_id"] as? String, "new-repo")
    }

    func testWriteOwn_priorClaimCorrupt_deletesAndWrites() async throws {
        let (client, store) = await makeStore()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        await client.injectFile(path: path, data: Data("garbage".utf8))
        try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 1_000)
        let snapshot = await client.snapshotFiles()
        let bytes = try XCTUnwrap(snapshot[path])
        let dict = try JSONSerialization.jsonObject(with: bytes) as? [String: Any]
        XCTAssertEqual(dict?["repo_id"] as? String, "repo-A")
    }

    // MARK: - VerifyOwn behavior (driven through writeOwnClaim)

    func testWriteOwn_bestEffortMatchingReadback_succeeds() async throws {
        let (client, store) = await makeStore()
        await client.setAtomicCreateMode(.bestEffort)
        try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 7_000)
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        let snapshot = await client.snapshotFiles()
        let bytes = try XCTUnwrap(snapshot[path])
        let dict = try JSONSerialization.jsonObject(with: bytes) as? [String: Any]
        XCTAssertEqual(dict?["repo_id"] as? String, "repo-A")
    }

    func testWriteOwn_bestEffortDrift_throws() async throws {
        let (client, store) = await makeStore()
        await client.setAtomicCreateMode(.bestEffort)
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        // Stage a hijack: atomicCreate stores these bytes (not ours) and reports .bestEffortRetry.
        let hijack: [String: Any] = [
            "v": 1,
            "repo_id": "hijacked-repo",
            "created_at_ms": 9_999,
            "writer_id": selfWriter
        ]
        let hijackBytes = try JSONSerialization.data(withJSONObject: hijack)
        await client.stageBestEffortRace(at: path, with: hijackBytes)
        do {
            try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 7_000)
            XCTFail("expected post-write drift to throw")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 5)
        }
    }

    func testWriteOwn_bestEffortUnparseableReadback_throws() async throws {
        let (client, store) = await makeStore()
        await client.setAtomicCreateMode(.bestEffort)
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        let incomplete = try JSONSerialization.data(withJSONObject: ["v": 1])
        await client.stageBestEffortRace(at: path, with: incomplete)
        do {
            try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 7_000)
            XCTFail("expected unparseable post-write to throw")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 4)
        }
    }

    /// Truly non-JSON readback bytes must take the same `code: 4` "unparseable
    /// after write" path as the missing-keys readback. Guards against regressing
    /// `try? JSONSerialization` back to `try`, which would leak raw NSCocoaError
    /// instead of the typed bootstrap diagnosis.
    func testWriteOwn_bestEffortNonJSONReadback_throws() async throws {
        let (client, store) = await makeStore()
        await client.setAtomicCreateMode(.bestEffort)
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        await client.stageBestEffortRace(at: path, with: Data("not-json".utf8))
        do {
            try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 7_000)
            XCTFail("expected non-JSON post-write to throw")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 4)
        }
    }

    func testWriteOwn_alreadyExistsMatchingReadback_succeeds() async throws {
        // Simulate a peer racing in between our metadata pre-check and atomicCreate:
        // metadata reports "no file", atomicCreate reports .alreadyExists, the file
        // on disk happens to contain our exact triple (a prior-run self-write adopted).
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        let matching: [String: Any] = [
            "v": 1,
            "repo_id": "repo-A",
            "created_at_ms": 7_000,
            "writer_id": selfWriter
        ]
        let matchingBytes = try JSONSerialization.data(withJSONObject: matching)
        let client = AlreadyExistsRaceClient(inner: inner, racePath: path, raceBytes: matchingBytes)
        let store = IdentityClaimStore(client: client, basePath: basePath)

        // createdAtMs ignored by .alreadyExists branch; writer + repoID must match.
        try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 7_000)
        let stillThere = await inner.hasFile(path)
        XCTAssertTrue(stillThere)
    }

    func testWriteOwn_alreadyExistsDriftedWriterID_throws() async throws {
        let inner = InMemoryRemoteStorageClient()
        try await inner.connect()
        let path = RepoLayout.identityClaimPath(base: basePath, writerID: selfWriter)
        let drifted: [String: Any] = [
            "v": 1,
            "repo_id": "repo-A",
            "created_at_ms": 7_000,
            "writer_id": otherWriter
        ]
        let driftedBytes = try JSONSerialization.data(withJSONObject: drifted)
        let client = AlreadyExistsRaceClient(inner: inner, racePath: path, raceBytes: driftedBytes)
        let store = IdentityClaimStore(client: client, basePath: basePath)

        do {
            try await store.writeOwnClaim(repoID: "repo-A", writerID: selfWriter, createdAtMs: 7_000)
            XCTFail("expected drift throw")
        } catch let RepoBootstrap.BootstrapError.ioFailure(error as NSError) {
            XCTAssertEqual(error.domain, "RepoBootstrap")
            XCTAssertEqual(error.code, 5)
        }
    }

    // MARK: - Stabilize fresh election

    func testStabilize_noPeerLands_returnsInitial() async throws {
        let (_, store) = await makeStore()
        // Tight rounds keep the test sub-second while still exercising the full loop.
        let result = try await store.stabilizeFreshElection(
            initial: "fallback",
            maxRounds: 3,
            interval: .milliseconds(20)
        )
        XCTAssertEqual(result, "fallback", "no peer → return initial after rounds exhaust")
    }

    func testStabilize_peerWithEarlierTimestamp_returnsPeerID() async throws {
        let (client, store) = await makeStore()
        await injectValidClaim(client, writerID: otherWriter, repoID: "peer-repo", createdAtMs: 1)
        let result = try await store.stabilizeFreshElection(
            initial: "fallback",
            maxRounds: 6,
            interval: .milliseconds(20)
        )
        XCTAssertEqual(result, "peer-repo")
    }

    func testStabilize_transientListErrorMidWindow_clearsStableThenReturns() async throws {
        let (client, store) = await makeStore()
        await injectValidClaim(client, writerID: otherWriter, repoID: "peer-repo", createdAtMs: 1)
        // One-shot list error must reset stable-read counter without aborting the function.
        await client.injectListError(.transport, for: RepoLayout.identityDirectoryPath(base: basePath))
        let result = try await store.stabilizeFreshElection(
            initial: "fallback",
            maxRounds: 6,
            interval: .milliseconds(20)
        )
        XCTAssertEqual(result, "peer-repo", "transient error mid-window must not abort")
    }

    // MARK: - Helpers

    private func makeStore() async -> (InMemoryRemoteStorageClient, IdentityClaimStore) {
        let client = InMemoryRemoteStorageClient()
        try? await client.connect()
        let store = IdentityClaimStore(client: client, basePath: basePath)
        return (client, store)
    }

    private func injectValidClaim(
        _ client: InMemoryRemoteStorageClient,
        writerID: String,
        repoID: String,
        createdAtMs: Int64
    ) async {
        let dict: [String: Any] = [
            "v": 1,
            "repo_id": repoID,
            "created_at_ms": createdAtMs,
            "writer_id": writerID
        ]
        guard let data = try? JSONSerialization.data(withJSONObject: dict) else { return }
        await client.injectFile(path: RepoLayout.identityClaimPath(base: basePath, writerID: writerID), data: data)
    }
}

/// Returns metadata size=0 for `racePath` while serving non-empty `raceBytes` from `download`.
/// Models the disk-pressure / stale-list scenario healZeroByteSelfClaim must defend against.
private actor ZeroSizeMetadataNonEmptyDownloadClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    private let inner: InMemoryRemoteStorageClient
    private let racePath: String
    private let raceBytes: Data

    init(inner: InMemoryRemoteStorageClient, racePath: String, raceBytes: Data) {
        self.inner = inner
        self.racePath = racePath
        self.raceBytes = raceBytes
    }

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
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
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        if normalize(remotePath) == normalize(racePath) {
            try raceBytes.write(to: localURL, options: .atomic)
            return
        }
        try await inner.download(remotePath: remotePath, localURL: localURL)
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
    nonisolated private func normalize(_ p: String) -> String {
        let trimmed = p.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        let collapsed = trimmed
            .split(separator: "/", omittingEmptySubsequences: true)
            .joined(separator: "/")
        return "/" + collapsed
    }
}

/// Simulates a peer racing between metadata-precheck and atomicCreate: the first
/// `metadata(racePath)` returns nil, then `atomicCreate(racePath)` injects `raceBytes`
/// and returns `.alreadyExists`. After that point, metadata/download see the file.
private actor AlreadyExistsRaceClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    private let inner: InMemoryRemoteStorageClient
    private let racePath: String
    private let raceBytes: Data
    private var raceFired = false

    init(inner: InMemoryRemoteStorageClient, racePath: String, raceBytes: Data) {
        self.inner = inner
        self.racePath = racePath
        self.raceBytes = raceBytes
    }

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
    }

    func connect() async throws { try await inner.connect() }
    func disconnect() async { await inner.disconnect() }
    func storageCapacity() async throws -> RemoteStorageCapacity? { try await inner.storageCapacity() }
    func list(path: String) async throws -> [RemoteStorageEntry] { try await inner.list(path: path) }
    func metadata(path: String) async throws -> RemoteStorageEntry? {
        if normalize(path) == normalize(racePath) && !raceFired {
            return nil
        }
        return try await inner.metadata(path: path)
    }
    func upload(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws {
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        if normalize(remotePath) == normalize(racePath) {
            await inner.injectFile(path: racePath, data: raceBytes)
            raceFired = true
            return .alreadyExists
        }
        return try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        try await inner.download(remotePath: remotePath, localURL: localURL)
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
    nonisolated private func normalize(_ p: String) -> String {
        let trimmed = p.trimmingCharacters(in: CharacterSet(charactersIn: "/ "))
        guard !trimmed.isEmpty, trimmed != "." else { return "/" }
        let collapsed = trimmed
            .split(separator: "/", omittingEmptySubsequences: true)
            .joined(separator: "/")
        return "/" + collapsed
    }
}

/// Throws on every `atomicCreate`. Use to prove a code path short-circuits before atomicCreate is reached.
private actor ForbidAtomicCreateClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .concurrent }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    private let inner: InMemoryRemoteStorageClient
    private(set) var atomicCreateCount = 0

    init(inner: InMemoryRemoteStorageClient) { self.inner = inner }

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
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
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        atomicCreateCount += 1
        throw NSError(
            domain: "ForbidAtomicCreateClient",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "atomicCreate forbidden in this test"]
        )
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        try await inner.download(remotePath: remotePath, localURL: localURL)
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

/// Pure passthrough that advertises `.serialOnly`, forcing IdentityClaimStore down its sequential branch.
private actor SerialOnlyWrapperClient: RemoteStorageClientProtocol {
    nonisolated var concurrencyMode: ClientConcurrencyMode { .serialOnly }
    nonisolated var supportsLivenessSafeOverwriteMove: Bool { true }
    private let inner: InMemoryRemoteStorageClient

    init(inner: InMemoryRemoteStorageClient) { self.inner = inner }

    nonisolated func atomicCreateGuarantee(forFileSize size: Int64, remotePath: String) -> CreateGuarantee {
        inner.atomicCreateGuarantee(forFileSize: size, remotePath: remotePath)
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
        try await inner.upload(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func atomicCreate(
        localURL: URL, remotePath: String,
        respectTaskCancellation: Bool, onProgress: ((Double) -> Void)?
    ) async throws -> AtomicCreateResult {
        try await inner.atomicCreate(localURL: localURL, remotePath: remotePath, respectTaskCancellation: respectTaskCancellation, onProgress: onProgress)
    }
    func setModificationDate(_ date: Date, forPath path: String) async throws {
        try await inner.setModificationDate(date, forPath: path)
    }
    func download(remotePath: String, localURL: URL) async throws {
        try await inner.download(remotePath: remotePath, localURL: localURL)
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

import XCTest
@testable import Watermelon

/// Per-operation guarantee reporting. Backend-by-backend matrix of "what does
/// the storage layer ACTUALLY promise for this size at this path?" — pinned
/// down so future changes (S3 multipart If-None-Match, WebDAV server quirks)
/// are forced to update this matrix and explain why.
final class CreateGuaranteeTests: XCTestCase {
    /// Default protocol impl is exists+upload — peer can race in between, so
    /// `.overwritePossible` is the conservative-but-correct answer.
    func testInMemoryClient_defaultGuarantee_isOverwritePossible() {
        let client = InMemoryRemoteStorageClient()
        let guarantee = client.atomicCreateGuarantee(forFileSize: 1024, remotePath: "/x")
        XCTAssertEqual(guarantee, .overwritePossible)
    }

    /// `AtomicCreateResult.defaultGuarantee` infers from the result. `.bestEffortRetry`
    /// implies exists+upload happened → overwrite was possible. `.created` implies
    /// the backend told us the file definitely didn't exist before our write.
    func testAtomicCreateResultDefaultGuarantee_mappingMatrix() {
        XCTAssertEqual(AtomicCreateResult.created.defaultGuarantee, .exclusive)
        XCTAssertEqual(AtomicCreateResult.alreadyExists.defaultGuarantee, .exclusive)
        XCTAssertEqual(AtomicCreateResult.bestEffortRetry.defaultGuarantee, .overwritePossible)
    }

    /// `.overwritePossible` backends route through a UUID-staged path + verify +
    /// move. The final path receives our integrity-checked bytes; a peer racing
    /// during the move surfaces as `.alreadyExists` so the writer re-allocates.
    func testCreateWithStagingFallback_overwritePossible_succeedsViaStaging() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.bestEffort)
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let result = try await MetadataCreateGate.createWithStagingFallback(
            client: client,
            localURL: tmp,
            remotePath: "/repo/.watermelon/commits/x.jsonl",
            respectTaskCancellation: false
        )
        XCTAssertEqual(result, .created)
        // Final path holds our bytes.
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: downloadURL) }
        try await client.download(remotePath: "/repo/.watermelon/commits/x.jsonl", localURL: downloadURL)
        let bytes = try Data(contentsOf: downloadURL)
        XCTAssertEqual(bytes, Data("payload".utf8))
    }

    /// `.exclusive` backend (default in-memory mode): direct `atomicCreate` to the
    /// final path, no staging.
    func testCreateWithStagingFallback_exclusive_directCreate() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let result = try await MetadataCreateGate.createWithStagingFallback(
            client: client,
            localURL: tmp,
            remotePath: "/repo/.watermelon/commits/y.jsonl",
            respectTaskCancellation: false
        )
        XCTAssertEqual(result, .created)
    }

    /// Probe says yes → finalization uses `moveIfAbsent`, not the copy fallback. Even
    /// with `.overwritePossible` static guarantee, the runtime probe can promote it
    /// (S3 endpoint with conditional CopyObject support, WebDAV server honoring `Overwrite: F`).
    func testCreateWithStagingFallback_probeYes_usesMoveIfAbsent() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setExclusiveMoveProbeOverride(true)
        await client.setAtomicCreateMode(.bestEffort)

        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let result = try await MetadataCreateGate.createWithStagingFallback(
            client: client,
            localURL: tmp,
            remotePath: "/repo/.watermelon/commits/probe-yes.jsonl",
            respectTaskCancellation: false,
            finalizationPolicy: .allowBestEffort
        )
        XCTAssertEqual(result, .created)
        // Staging side-path got cleaned up.
        let stagingExists = await client.snapshotFiles().keys.contains(where: { $0.contains(".staging-") })
        XCTAssertFalse(stagingExists, "staging path must be cleaned up after success")
    }

    /// Probe says no + `.allowBestEffort` → falls through to `bestEffortCopyIfAbsent` instead
    /// of throwing. The previous design used a post-throw S3-specific error classifier; the
    /// probe-first design routes directly without try/catch on a vendor error string.
    func testCreateWithStagingFallback_probeNo_allowBestEffort_fallsThroughToCopy() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setExclusiveMoveProbeOverride(false)
        await client.setAtomicCreateMode(.bestEffort)

        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let result = try await MetadataCreateGate.createWithStagingFallback(
            client: client,
            localURL: tmp,
            remotePath: "/repo/.watermelon/commits/probe-no-bestEffort.jsonl",
            respectTaskCancellation: false,
            finalizationPolicy: .allowBestEffort
        )
        XCTAssertEqual(result, .created)
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: downloadURL) }
        try await client.download(remotePath: "/repo/.watermelon/commits/probe-no-bestEffort.jsonl", localURL: downloadURL)
        XCTAssertEqual(try Data(contentsOf: downloadURL), Data("payload".utf8))
    }

    /// Probe says no + `.requireExclusiveMove` → throws upfront, before `moveIfAbsent` is
    /// even attempted. Identity finalization (repo-identity.json, version.json) refuses
    /// non-exclusive backends to keep peer overwrites from corrupting repo identity.
    func testCreateWithStagingFallback_probeNo_requireExclusive_throws() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setExclusiveMoveProbeOverride(false)
        await client.setAtomicCreateMode(.bestEffort)

        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        do {
            _ = try await MetadataCreateGate.createWithStagingFallback(
                client: client,
                localURL: tmp,
                remotePath: "/repo/.watermelon/repo-identity.json",
                respectTaskCancellation: false,
                finalizationPolicy: .requireExclusiveMove
            )
            XCTFail("expected nonExclusiveFinalization")
        } catch MetadataCreateGate.Error.nonExclusiveFinalization(let path) {
            XCTAssertEqual(path, "/repo/.watermelon/repo-identity.json")
        }
        // Staging cleanup ran on the throw path.
        let stagingExists = await client.snapshotFiles().keys.contains(where: { $0.contains(".staging-") })
        XCTAssertFalse(stagingExists, "staging path must be cleaned up after throw")
    }
}

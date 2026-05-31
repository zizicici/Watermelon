import XCTest
@testable import Watermelon

final class CreateGuaranteeTests: XCTestCase {
    func testInMemoryClient_defaultGuarantee_isOverwritePossible() {
        let client = InMemoryRemoteStorageClient()
        let guarantee = client.atomicCreateGuarantee(forFileSize: 1024, remotePath: "/x")
        XCTAssertEqual(guarantee, .overwritePossible)
    }

    func testAtomicCreateResultDefaultGuarantee_mappingMatrix() {
        XCTAssertEqual(AtomicCreateResult.created.defaultGuarantee, .exclusive)
        XCTAssertEqual(AtomicCreateResult.alreadyExists.defaultGuarantee, .exclusive)
        XCTAssertEqual(AtomicCreateResult.bestEffortRetry.defaultGuarantee, .overwritePossible)
    }

    func testRebuildable_overwritePossible_succeedsViaStaging() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        await client.setAtomicCreateMode(.bestEffort)
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let result = try await MetadataCreateGate.createRebuildable(
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

    func testRebuildable_exclusive_directCreate() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setAtomicCreateGuarantee(.exclusive)
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let finalPath = "/repo/.watermelon/commits/y.jsonl"
        let result = try await MetadataCreateGate.createRebuildable(
            client: client,
            localURL: tmp,
            remotePath: finalPath,
            respectTaskCancellation: false
        )
        XCTAssertEqual(result, .created)
        // Direct atomicCreate path must NOT have left any staging side files.
        let stagingExists = await client.snapshotFiles().keys.contains(where: { $0.contains(".staging-") })
        XCTAssertFalse(stagingExists, "exclusive direct path must skip staging entirely")
        // Bytes must land at the final path.
        let landed = await client.hasFile(finalPath)
        XCTAssertTrue(landed)
    }

    func testRebuildable_probeYes_usesMoveIfAbsent() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setExclusiveMoveProbeOverride(true)
        await client.setAtomicCreateMode(.bestEffort)

        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let result = try await MetadataCreateGate.createRebuildable(
            client: client,
            localURL: tmp,
            remotePath: "/repo/.watermelon/commits/probe-yes.jsonl",
            respectTaskCancellation: false
        )
        XCTAssertEqual(result, .created)
        // Staging side-path got cleaned up.
        let stagingExists = await client.snapshotFiles().keys.contains(where: { $0.contains(".staging-") })
        XCTAssertFalse(stagingExists, "staging path must be cleaned up after success")
    }

    func testRebuildable_probeNo_bestEffortCopy_returnsBestEffortRetry() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setExclusiveMoveProbeOverride(false)
        await client.setAtomicCreateMode(.bestEffort)

        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let result = try await MetadataCreateGate.createRebuildable(
            client: client,
            localURL: tmp,
            remotePath: "/repo/.watermelon/commits/probe-no-bestEffort.jsonl",
            respectTaskCancellation: false
        )
        // Rebuildable path does not verify, so bestEffortCopyIfAbsent returns .bestEffortRetry.
        XCTAssertEqual(result, .bestEffortRetry)
        let downloadURL = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: downloadURL) }
        try await client.download(remotePath: "/repo/.watermelon/commits/probe-no-bestEffort.jsonl", localURL: downloadURL)
        XCTAssertEqual(try Data(contentsOf: downloadURL), Data("payload".utf8))
    }

    func testAuthoritative_probeNo_requireExclusive_throws() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setMoveIfAbsentGuarantee(.overwritePossible)
        await client.setExclusiveMoveProbeOverride(false)
        await client.setAtomicCreateMode(.bestEffort)

        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data("payload".utf8).write(to: tmp)
        defer { try? FileManager.default.removeItem(at: tmp) }

        do {
            _ = try await MetadataCreateGate.createAuthoritative(
                client: client,
                localURL: tmp,
                remotePath: "/repo/.watermelon/repo-identity.json",
                respectTaskCancellation: false
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

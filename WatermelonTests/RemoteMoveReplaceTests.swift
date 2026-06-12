import XCTest
@testable import Watermelon

final class RemoteMoveReplaceTests: XCTestCase {
    private let finalPath = "/repo/item"
    private let tempPath = "/repo/item.tmp"
    private let backupPath = "/repo/item.bak"

    private let oldBytes = Data([0xAA, 0xBB])

    func testLostOwnershipAtRestoreProbeBlocksBackupOverFinal() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: finalPath, data: oldBytes)

        let ownership = PassThenLostOwnership(passCount: 3)
        do {
            try await RemoteMoveReplace.moveReplacing(
                client: client,
                tempPath: tempPath,
                finalPath: finalPath,
                backupPath: backupPath,
                ignoreCancellation: false,
                assertOwnership: { try ownership.assertWhileOwned() }
            )
            XCTFail("moveReplacing should have thrown once ownership was lost at the restore re-prove")
        } catch {
            // Expected: the post-restore assert propagates the lost-ownership error.
        }

        let finalData = await client.fileData(path: finalPath)
        let backupData = await client.fileData(path: backupPath)
        XCTAssertNil(finalData, "a lease lost before the restore move must not promote the backup to the canonical final")
        XCTAssertEqual(backupData, oldBytes, "the stranded backup must remain for a later owned recovery")
    }

    func testHeldOwnershipRestoresBackupToFinal() async throws {
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: finalPath, data: oldBytes)

        do {
            try await RemoteMoveReplace.moveReplacing(
                client: client,
                tempPath: tempPath,
                finalPath: finalPath,
                backupPath: backupPath,
                ignoreCancellation: false,
                assertOwnership: {}
            )
            XCTFail("moveReplacing should still throw the temp→final move fault even on a successful restore")
        } catch {
            // Expected: the temp→final retry fault propagates after the owned restore.
        }

        let finalData = await client.fileData(path: finalPath)
        XCTAssertEqual(finalData, oldBytes, "an owned restore must promote the backup back to the canonical final")
    }

    private final class PassThenLostOwnership: @unchecked Sendable {
        private var remaining: Int
        init(passCount: Int) { remaining = passCount }
        func assertWhileOwned() throws {
            if remaining > 0 { remaining -= 1; return }
            throw LiteRepoError.ownershipLost
        }
    }
}

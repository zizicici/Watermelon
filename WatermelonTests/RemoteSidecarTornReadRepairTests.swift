import UIKit
import XCTest
@testable import Watermelon

// Pins the torn-sidecar self-heal: a partial at the canonical path (a writer interrupted by a dead
// session, or a pre-shield build) passes every writer's exists/collision probe, so the read side must
// detect and delete it — otherwise it permanently poisons the shared L2 and the L1 of every browsing
// device, and only a full purge repairs it.
final class RemoteSidecarTornReadRepairTests: XCTestCase {
    private let thumbPath = "/base/.watermelon/thumbs/de/deadbeef.jpg"

    private func completeJPEG() throws -> Data {
        let renderer = UIGraphicsImageRenderer(size: CGSize(width: 8, height: 8))
        let image = renderer.image { context in
            UIColor.red.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 8, height: 8))
        }
        return try XCTUnwrap(image.jpegData(compressionQuality: 0.9))
    }

    func testTornSidecarIsDeletedSoWritersCanRegenerate() async throws {
        let jpeg = try completeJPEG()
        // Decodable-but-truncated: SOI/EOI framing must catch what UIImage(data:) would still decode.
        let torn = Data(jpeg.dropLast(16))
        XCTAssertFalse(RemoteThumbnailService.isCompleteJPEG(torn))
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: thumbPath, data: torn)

        do {
            _ = try await RemoteThumbnailService.readSidecar(remotePath: thumbPath, client: client)
            XCTFail("torn sidecar bytes must never be returned")
        } catch {}

        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted, [thumbPath], "the torn canonical must be deleted so writeback/backfill can regenerate it")
    }

    func testCompleteSidecarIsReturnedAndKept() async throws {
        let jpeg = try completeJPEG()
        XCTAssertTrue(RemoteThumbnailService.isCompleteJPEG(jpeg))
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: thumbPath, data: jpeg)

        let result = try await RemoteThumbnailService.readSidecar(remotePath: thumbPath, client: client)
        XCTAssertEqual(result.data, jpeg)
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
    }

    func testCancelledDownloadNeverDeletesTheRemoteSidecar() async throws {
        // A cancelled download can hand back a truncated local file (client-dependent) — judging the
        // remote from it would delete a good sidecar on every scrolled-away cell.
        let jpeg = try completeJPEG()
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: thumbPath, data: jpeg)
        await client.enqueueDownloadData(Data(jpeg.dropLast(16)))

        let task = Task { () -> Bool in
            while !Task.isCancelled { try? await Task.sleep(nanoseconds: 1_000_000) }
            do {
                _ = try await RemoteThumbnailService.readSidecar(remotePath: thumbPath, client: client)
                return true
            } catch {
                return false
            }
        }
        task.cancel()
        _ = await task.value
        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty, "a cancelled read must not delete the canonical sidecar")
    }
}

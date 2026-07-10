import UIKit
import XCTest
@testable import Watermelon

// Pins torn-sidecar handling: reads are pure and only reject bad bytes; writes that already hold the
// repo write gate delete-and-replace invalid sidecars so they do not permanently poison shared L2.
final class RemoteSidecarTornReadRepairTests: XCTestCase {
    private let thumbPath = "/base/.watermelon/thumbs/de/deadbeef.jpg"
    private struct OwnershipDenied: Error {}

    private func completeJPEG() throws -> Data {
        let renderer = UIGraphicsImageRenderer(size: CGSize(width: 8, height: 8))
        let image = renderer.image { context in
            UIColor.red.setFill()
            context.fill(CGRect(x: 0, y: 0, width: 8, height: 8))
        }
        return try XCTUnwrap(image.jpegData(compressionQuality: 0.9))
    }

    func testTornSidecarReadFailsWithoutDeletingRemote() async throws {
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
        XCTAssertTrue(deleted.isEmpty, "read-only thumbnail decode must not mutate the remote")
    }

    func testPlaintextWriteReplacesTornSidecar() async throws {
        let jpeg = try completeJPEG()
        let torn = Data(jpeg.dropLast(16))
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: thumbPath, data: torn)

        let written = try await RemoteThumbnailService.writeSidecar(
            jpeg,
            fingerprintHex: "deadbeef",
            thumbPath: thumbPath,
            shardDir: "/base/.watermelon/thumbs/de",
            client: client
        )

        XCTAssertTrue(written)
        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted, [thumbPath])
        let landedData = await client.fileData(path: thumbPath)
        let landed = try XCTUnwrap(landedData)
        XCTAssertEqual(landed, jpeg)
    }

    func testWriteDoesNotRepairTornSidecarWhenOwnershipAssertionFails() async throws {
        let jpeg = try completeJPEG()
        let torn = Data(jpeg.dropLast(16))
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: thumbPath, data: torn)

        do {
            _ = try await RemoteThumbnailService.writeSidecar(
                jpeg,
                fingerprintHex: "deadbeef",
                thumbPath: thumbPath,
                shardDir: "/base/.watermelon/thumbs/de",
                client: client,
                assertOwnership: { throw OwnershipDenied() }
            )
            XCTFail("sidecar repair must fail closed when the write lease cannot be proven")
        } catch is OwnershipDenied {
            // expected
        }

        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
        let remoteData = await client.fileData(path: thumbPath)
        XCTAssertEqual(remoteData, torn)
    }

    func testWriteDoesNotCreateOrUploadWhenOwnershipAssertionFailsBeforePublish() async throws {
        let jpeg = try completeJPEG()
        let client = InMemoryRemoteStorageClient()

        do {
            _ = try await RemoteThumbnailService.writeSidecar(
                jpeg,
                fingerprintHex: "deadbeef",
                thumbPath: thumbPath,
                shardDir: "/base/.watermelon/thumbs/de",
                client: client,
                assertOwnership: { throw OwnershipDenied() }
            )
            XCTFail("sidecar publish must fail closed when the write lease cannot be proven")
        } catch is OwnershipDenied {
            // expected
        }

        let createdDirectories = await client.createdDirectories
        let uploadedPaths = await client.uploadedPaths
        XCTAssertTrue(createdDirectories.isEmpty)
        XCTAssertTrue(uploadedPaths.isEmpty)
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

    func testEncryptedSidecarWriteAndReadRoundTrip() async throws {
        let jpeg = try completeJPEG()
        let client = InMemoryRemoteStorageClient()
        let context = RepoEncryptionContext(
            repoID: "repo-thumb",
            activeKeyID: "key-thumb",
            contentKey: Data(repeating: 0x44, count: RepoEncryptionKeyMaterial.byteCount)
        )
        let shardDir = "/base/.watermelon/thumbs/de"

        let written = try await RemoteThumbnailService.writeSidecar(
            jpeg,
            fingerprintHex: "deadbeef",
            thumbPath: thumbPath,
            shardDir: shardDir,
            client: client,
            encryptionContext: context
        )
        XCTAssertTrue(written)
        let landedData = await client.fileData(path: thumbPath)
        let landed = try XCTUnwrap(landedData)
        XCTAssertFalse(RemoteThumbnailService.isCompleteJPEG(landed), "encrypted sidecar must not land as plaintext JPEG")

        let result = try await RemoteThumbnailService.readSidecar(
            remotePath: thumbPath,
            client: client,
            encryptionContext: context,
            fingerprintHex: "deadbeef"
        )
        XCTAssertEqual(result.data, jpeg)
        XCTAssertTrue(RemoteThumbnailService.isCompleteJPEG(result.data))
    }

    func testEncryptedReadRejectsPlaintextSidecarWithoutDeletingRemote() async throws {
        let jpeg = try completeJPEG()
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: thumbPath, data: jpeg)
        let context = RepoEncryptionContext(
            repoID: "repo-thumb",
            activeKeyID: "key-thumb",
            contentKey: Data(repeating: 0x44, count: RepoEncryptionKeyMaterial.byteCount)
        )

        do {
            _ = try await RemoteThumbnailService.readSidecar(
                remotePath: thumbPath,
                client: client,
                encryptionContext: context,
                fingerprintHex: "deadbeef"
            )
            XCTFail("encrypted repo must not accept plaintext sidecar bytes")
        } catch {}

        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
    }

    func testEncryptedWriteReplacesExistingPlaintextSidecar() async throws {
        let jpeg = try completeJPEG()
        let client = InMemoryRemoteStorageClient()
        await client.seedFile(path: thumbPath, data: jpeg)
        let context = RepoEncryptionContext(
            repoID: "repo-thumb",
            activeKeyID: "key-thumb",
            contentKey: Data(repeating: 0x44, count: RepoEncryptionKeyMaterial.byteCount)
        )

        let written = try await RemoteThumbnailService.writeSidecar(
            jpeg,
            fingerprintHex: "deadbeef",
            thumbPath: thumbPath,
            shardDir: "/base/.watermelon/thumbs/de",
            client: client,
            encryptionContext: context
        )

        XCTAssertTrue(written)
        let landedData = await client.fileData(path: thumbPath)
        let landed = try XCTUnwrap(landedData)
        XCTAssertFalse(RemoteThumbnailService.isCompleteJPEG(landed))
        let result = try await RemoteThumbnailService.readSidecar(
            remotePath: thumbPath,
            client: client,
            encryptionContext: context,
            fingerprintHex: "deadbeef"
        )
        XCTAssertEqual(result.data, jpeg)
    }

    func testCorruptEncryptedSidecarReadFailsWithoutDeletingRemote() async throws {
        let jpeg = try completeJPEG()
        let client = InMemoryRemoteStorageClient()
        let context = RepoEncryptionContext(
            repoID: "repo-thumb",
            activeKeyID: "key-thumb",
            contentKey: Data(repeating: 0x44, count: RepoEncryptionKeyMaterial.byteCount)
        )
        _ = try await RemoteThumbnailService.writeSidecar(
            jpeg,
            fingerprintHex: "deadbeef",
            thumbPath: thumbPath,
            shardDir: "/base/.watermelon/thumbs/de",
            client: client,
            encryptionContext: context
        )
        let encryptedData = await client.fileData(path: thumbPath)
        var encrypted = try XCTUnwrap(encryptedData)
        encrypted[encrypted.count - 1] ^= 0xff
        await client.seedFile(path: thumbPath, data: encrypted)

        do {
            _ = try await RemoteThumbnailService.readSidecar(
                remotePath: thumbPath,
                client: client,
                encryptionContext: context,
                fingerprintHex: "deadbeef"
            )
            XCTFail("corrupt encrypted sidecar must not be returned")
        } catch {}

        let deleted = await client.deletedPaths
        XCTAssertTrue(deleted.isEmpty)
    }

    func testEncryptedWriteReplacesCorruptEncryptedSidecar() async throws {
        let jpeg = try completeJPEG()
        let client = InMemoryRemoteStorageClient()
        let context = RepoEncryptionContext(
            repoID: "repo-thumb",
            activeKeyID: "key-thumb",
            contentKey: Data(repeating: 0x44, count: RepoEncryptionKeyMaterial.byteCount)
        )
        _ = try await RemoteThumbnailService.writeSidecar(
            jpeg,
            fingerprintHex: "deadbeef",
            thumbPath: thumbPath,
            shardDir: "/base/.watermelon/thumbs/de",
            client: client,
            encryptionContext: context
        )
        let encryptedData = await client.fileData(path: thumbPath)
        var encrypted = try XCTUnwrap(encryptedData)
        encrypted[encrypted.count - 1] ^= 0xff
        await client.seedFile(path: thumbPath, data: encrypted)

        let written = try await RemoteThumbnailService.writeSidecar(
            jpeg,
            fingerprintHex: "deadbeef",
            thumbPath: thumbPath,
            shardDir: "/base/.watermelon/thumbs/de",
            client: client,
            encryptionContext: context
        )

        XCTAssertTrue(written)
        let deleted = await client.deletedPaths
        XCTAssertEqual(deleted, [thumbPath])
        let result = try await RemoteThumbnailService.readSidecar(
            remotePath: thumbPath,
            client: client,
            encryptionContext: context,
            fingerprintHex: "deadbeef"
        )
        XCTAssertEqual(result.data, jpeg)
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

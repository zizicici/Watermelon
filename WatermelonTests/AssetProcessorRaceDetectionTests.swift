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

    private static func sha256(_ data: Data) -> Data {
        Data(SHA256.hash(data: data))
    }
}

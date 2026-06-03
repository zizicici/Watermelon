import CryptoKit
import XCTest
@testable import Watermelon

/// Verifies `RestoreService.downloadWithFallback` tries each known path for a content
/// hash. Multi-writer V2 publishes alternates under different physical paths; deleting
/// from one writer's path must not break restore when content is still on another.
final class RestoreServiceFallbackTests: XCTestCase {
    private let basePath = "/repo"

    func testPrimaryPathSucceeds_alternatesNotConsulted() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let primaryBytes = "primary-bytes"
        await client.injectFile(path: "\(basePath)/2026/01/photo.jpg", contents: primaryBytes)

        let instance = makeInstance(
            primary: "2026/01/photo.jpg",
            alternates: ["2026/01/photo~widB.jpg"],
            fileSize: Int64(primaryBytes.utf8.count),
            resourceHash: Self.sha256(of: primaryBytes)
        )

        let tempURL = makeTempURL()
        defer { try? FileManager.default.removeItem(at: tempURL) }

        try await RestoreService.downloadWithFallback(
            instance: instance,
            profile: makeProfile(),
            storageClient: client,
            localURL: tempURL
        )

        let downloaded = try Data(contentsOf: tempURL)
        XCTAssertEqual(String(data: downloaded, encoding: .utf8), "primary-bytes")
    }

    func testPrimaryMissing_alternateSucceeds() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Only the alternate exists on remote
        let alternateBytes = "alternate-bytes"
        await client.injectFile(path: "\(basePath)/2026/01/photo~widB.jpg", contents: alternateBytes)

        let instance = makeInstance(
            primary: "2026/01/photo.jpg",
            alternates: ["2026/01/photo~widB.jpg"],
            fileSize: Int64(alternateBytes.utf8.count),
            resourceHash: Self.sha256(of: alternateBytes)
        )

        let tempURL = makeTempURL()
        defer { try? FileManager.default.removeItem(at: tempURL) }

        try await RestoreService.downloadWithFallback(
            instance: instance,
            profile: makeProfile(),
            storageClient: client,
            localURL: tempURL
        )

        let downloaded = try Data(contentsOf: tempURL)
        XCTAssertEqual(String(data: downloaded, encoding: .utf8), "alternate-bytes")
    }

    /// Primary path returns the wrong content (e.g., manually overwritten or stale).
    /// Size mismatch must trigger fallback to the next candidate path so restore lands
    /// the manifest's claimed bytes — accepting wrong content silently corrupts the
    /// user's library.
    func testPrimarySizeMismatch_fallsBackToAlternate() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let goodBytes = "correct-bytes"
        // Primary file is wrong size (any bytes that don't match `fileSize`).
        await client.injectFile(path: "\(basePath)/2026/01/photo.jpg", contents: "tampered")
        await client.injectFile(path: "\(basePath)/2026/01/photo~widB.jpg", contents: goodBytes)

        let instance = makeInstance(
            primary: "2026/01/photo.jpg",
            alternates: ["2026/01/photo~widB.jpg"],
            fileSize: Int64(goodBytes.utf8.count),
            resourceHash: Self.sha256(of: goodBytes)
        )

        let tempURL = makeTempURL()
        defer { try? FileManager.default.removeItem(at: tempURL) }

        try await RestoreService.downloadWithFallback(
            instance: instance,
            profile: makeProfile(),
            storageClient: client,
            localURL: tempURL
        )

        let downloaded = try Data(contentsOf: tempURL)
        XCTAssertEqual(String(data: downloaded, encoding: .utf8), goodBytes,
                       "size mismatch on primary must trigger fallback to the alternate")
    }

    /// Primary returns matching size but wrong content (e.g., same-size tamper or peer
    /// race uploaded different bytes). Hash mismatch must trigger fallback so we don't
    /// import the wrong photo into the user's library.
    func testPrimaryHashMismatchSameSize_fallsBackToAlternate() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        let goodBytes = "correct-byte" // 12 bytes
        let badBytes = "wrong-bytes!"  // 12 bytes — same length, different content
        await client.injectFile(path: "\(basePath)/2026/01/photo.jpg", contents: badBytes)
        await client.injectFile(path: "\(basePath)/2026/01/photo~widB.jpg", contents: goodBytes)

        let instance = makeInstance(
            primary: "2026/01/photo.jpg",
            alternates: ["2026/01/photo~widB.jpg"],
            fileSize: Int64(goodBytes.utf8.count),
            resourceHash: Self.sha256(of: goodBytes)
        )

        let tempURL = makeTempURL()
        defer { try? FileManager.default.removeItem(at: tempURL) }

        try await RestoreService.downloadWithFallback(
            instance: instance,
            profile: makeProfile(),
            storageClient: client,
            localURL: tempURL
        )

        let downloaded = try Data(contentsOf: tempURL)
        XCTAssertEqual(String(data: downloaded, encoding: .utf8), goodBytes,
                       "hash mismatch on primary (same size) must fall back to alternate")
    }

    /// On a grace backend a just-committed object can be durable yet briefly return a data-path 404.
    /// Restore must retry within the read-after-write window rather than failing the item from one 404.
    func testGraceBackend_transientNotFound_retriesWithinGraceAndSucceeds() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setReadAfterWriteGrace(5)
        let bytes = "durable-bytes"
        await client.injectFile(path: "\(basePath)/2026/01/photo.jpg", contents: bytes)
        // First GET 404s inside the visibility window; the injected error clears after one throw.
        await client.injectDownloadError(.notFound, for: "\(basePath)/2026/01/photo.jpg")

        let instance = makeInstance(
            primary: "2026/01/photo.jpg",
            alternates: [],
            fileSize: Int64(bytes.utf8.count),
            resourceHash: Self.sha256(of: bytes)
        )

        let tempURL = makeTempURL()
        defer { try? FileManager.default.removeItem(at: tempURL) }

        try await RestoreService.downloadWithFallback(
            instance: instance,
            profile: makeProfile(),
            storageClient: client,
            localURL: tempURL
        )

        let downloaded = try Data(contentsOf: tempURL)
        XCTAssertEqual(String(data: downloaded, encoding: .utf8), bytes,
                       "transient data-path 404 within grace must be retried, not treated as durable absence")
    }

    /// Primary downloads but is corrupt (same-size hash mismatch) and the only alternate is absent (404)
    /// on a grace backend. Proven wrong bytes cannot heal by waiting, so restore must fail promptly with
    /// the integrity mismatch — not spend the grace window retrying nor mask it as the alternate's 404.
    func testGraceBackend_primaryMismatchAlternate404_failsPromptlyWithMismatch() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        client.setReadAfterWriteGrace(30)
        let goodBytes = "correct-byte" // 12 bytes
        let badBytes = "wrong-bytes!"  // 12 bytes — same size, different content
        await client.injectFile(path: "\(basePath)/2026/01/photo.jpg", contents: badBytes)
        // Alternate path is never injected → genuine 404.

        let instance = makeInstance(
            primary: "2026/01/photo.jpg",
            alternates: ["2026/01/photo~widB.jpg"],
            fileSize: Int64(goodBytes.utf8.count),
            resourceHash: Self.sha256(of: goodBytes)
        )

        let tempURL = makeTempURL()
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let start = Date()
        do {
            try await RestoreService.downloadWithFallback(
                instance: instance,
                profile: makeProfile(),
                storageClient: client,
                localURL: tempURL
            )
            XCTFail("expected integrity mismatch throw")
        } catch {
            let elapsed = Date().timeIntervalSince(start)
            XCTAssertLessThan(elapsed, 5,
                              "proven wrong bytes must not spend the read-after-write grace window")
            let ns = error as NSError
            XCTAssertEqual(ns.domain, "RestoreService",
                           "must surface the deterministic mismatch, not the alternate's 404")
            XCTAssertTrue(ns.localizedDescription.contains("don't match manifest"),
                          "error should be the integrity mismatch: \(ns.localizedDescription)")
        }
    }

    /// Restore creation date must come from the asset row, not resource instances.
    /// Duplicate-content assets share a resource row stamped from whichever asset
    /// committed it, so deriving from instance dates imports a peer's creation date.
    func testRestoreCreationDate_usesAssetRowNotInstanceDates() {
        let peerDateMs: Int64 = 1_000
        let assetDateMs: Int64 = 5_000
        let instance = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: Self.sha256(of: "shared"),
            fileName: "photo.jpg",
            fileSize: 6,
            remoteRelativePath: "2026/01/photo.jpg",
            creationDateMs: peerDateMs
        )
        let descriptor = RestoreService.RestoreItemDescriptor(
            instances: [instance],
            assetFingerprint: TestFixtures.assetFingerprint(0xA1),
            creationDateMs: assetDateMs
        )

        let resolved = RestoreService.restoreCreationDate(for: descriptor)
        XCTAssertEqual(resolved, Date(millisecondsSinceEpoch: assetDateMs),
                       "restore date must be the asset row date, not the shared-resource instance date")
    }

    /// A nil asset-row creation date stays nil (Photos assigns import date) — the fix
    /// preserves the prior nil semantics rather than fabricating an instance-derived date.
    func testRestoreCreationDate_nilAssetDate_staysNil() {
        let instance = RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: Self.sha256(of: "shared"),
            fileName: "photo.jpg",
            fileSize: 6,
            remoteRelativePath: "2026/01/photo.jpg",
            creationDateMs: 1_000
        )
        let descriptor = RestoreService.RestoreItemDescriptor(
            instances: [instance],
            assetFingerprint: TestFixtures.assetFingerprint(0xA2),
            creationDateMs: nil
        )

        XCTAssertNil(RestoreService.restoreCreationDate(for: descriptor))
    }

    func testAllPathsMissing_throwsLastError() async throws {
        let client = InMemoryRemoteStorageClient()
        try await client.connect()
        // Nothing on remote

        let instance = makeInstance(
            primary: "2026/01/photo.jpg",
            alternates: ["2026/01/photo~widB.jpg", "2026/01/photo~widC.jpg"]
        )

        let tempURL = makeTempURL()
        defer { try? FileManager.default.removeItem(at: tempURL) }

        do {
            try await RestoreService.downloadWithFallback(
                instance: instance,
                profile: makeProfile(),
                storageClient: client,
                localURL: tempURL
            )
            XCTFail("expected throw when all paths missing")
        } catch {
            // expected
        }
    }


    private func makeInstance(
        primary: String,
        alternates: [String],
        fileSize: Int64 = 0,
        resourceHash: Data = Data()
    ) -> RemoteAssetResourceInstance {
        RemoteAssetResourceInstance(
            role: ResourceTypeCode.photo,
            slot: 0,
            resourceHash: resourceHash,
            fileName: "photo.jpg",
            fileSize: fileSize,
            remoteRelativePath: primary,
            alternateRemoteRelativePaths: alternates,
            creationDateMs: nil
        )
    }

    private static func sha256(of string: String) -> Data {
        Data(SHA256.hash(data: Data(string.utf8)))
    }

    private func makeProfile() -> ServerProfileRecord {
        TestFixtures.makeServerProfile(
            id: 1, storageType: .webdav,
            host: "h", port: 0, shareName: "", basePath: basePath, username: ""
        )
    }

    private func makeTempURL() -> URL {
        FileManager.default.temporaryDirectory.appendingPathComponent("restore_test_\(UUID().uuidString)")
    }
}
